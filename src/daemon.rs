//! Live agorabus subscribe loop for `wm-tts`.
//!
//! Wires the bus schema from [`crate::bus`] to a real subscribe loop. On
//! a `wm.tts.speak` request, the daemon checks the per-voice cache for an
//! exact (trimmed lowercase) match; cache hits play through `pw-cat`
//! (`PipeWire` native CLI) and emit `wm.tts.start` + `wm.tts.end`. Cache
//! misses render on demand via the Piper subprocess (the blocking
//! [`crate::synth::Synth::render`] call is hoisted onto a
//! `tokio::task::spawn_blocking` worker), then drop into the cache-hit
//! playback path.
//!
//! iter-7 wires voice hot-swap: `wm.tts.reload_voice` builds a new
//! per-voice [`CacheManager`], pre-renders the configured phrase set
//! for the new voice, atomically swaps the daemon's [`ActiveVoice`]
//! under a write lock, and publishes `wm.tts.reload.ack`. Subsequent
//! speak requests resolve against the new voice's cache.
//!
//! Cancel is wired with a `oneshot::Sender<()>` stored in
//! [`DaemonState`]: `wm.tts.cancel` fires the channel, which interrupts
//! the playback `tokio::select!` and `SIGKILL`s the player subprocess.
//! iter-13 approximates `drained_ms` as the wall-clock elapsed since
//! [`install_cancel_slot`] recorded the playback start (i.e. since the
//! player subprocess spawn). This is an upper bound on actual drained
//! audio — the kernel/`PipeWire` buffer hasn't necessarily played
//! everything we wrote yet — but it's strictly more useful than the
//! previous unconditional `0`. iter-17 tightens that bound for
//! file-based plays (cache hit + Piper cache miss) by capping the
//! elapsed-since-spawn value at the WAV's declared duration (parsed
//! from the header via [`crate::wav::parse_duration_ms`]). Cloud
//! streaming retains the elapsed-only bound because MP3 frame length
//! isn't known up-front. A future iter measuring real playback
//! position via a `PipeWire`-rs streaming consumer will tighten this
//! further.
//!
//! iter-11 wires the cloud streaming fast path: when
//! [`DaemonState::cloud`] is active, cache misses skip the file-based
//! Piper render-then-play and instead stream `ElevenLabs` MP3 chunks
//! into a `pw-cat --media-type=audio/mpeg -` subprocess via stdin. AC5
//! (≤400ms first audio) becomes realizable; AC6 is honored via the
//! "clean restart" interpretation — a failure before any frame reaches
//! the player silently falls back to Piper, while a failure after at
//! least one frame publishes `wm.tts.error{kind=stream}` and then
//! restarts the utterance from scratch using Piper.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, RwLock, oneshot};
use tracing::{error, info, warn};

use crate::bus::{
    self, CancelAckEvent, EndEvent, ErrorEvent, ReloadAckEvent, Request, SpeakRequest, StartEvent,
    decode_request, now_unix_ms, outgoing,
};
use crate::cache::CacheManager;
use crate::cloud::{CloudError, CloudSynth, cloud_synth_from_env};
use crate::synth::{PiperSubprocess, Synth, SynthError};
use crate::{TtsConfig, load_cache_yaml};

/// Default playback binary. `pw-cat` ships with `PipeWire` and is the
/// natural sink on this laptop; an override is exposed via `WM_TTS_PLAYER`.
pub const DEFAULT_PLAYER: &str = "pw-cat";

/// Read the playback binary from `WM_TTS_PLAYER`, defaulting to
/// [`DEFAULT_PLAYER`].
#[must_use]
pub fn player_from_env() -> String {
    std::env::var("WM_TTS_PLAYER").unwrap_or_else(|_| DEFAULT_PLAYER.to_string())
}

/// Swappable portion of the daemon: the active voice id paired with
/// the `CacheManager` rooted at the per-voice cache directory.
///
/// Held inside a [`tokio::sync::RwLock`] on [`DaemonState`]: speak
/// requests take a brief read lock to snapshot the cache path; voice
/// hot-swap takes the write lock to install a new [`ActiveVoice`].
pub struct ActiveVoice {
    /// Configured voice id (e.g. `en_US-lessac-medium`).
    pub voice: String,
    /// Per-voice cache lookup (wraps `CacheManager`).
    pub cache: CacheManager,
}

/// Live daemon state passed to per-request handlers.
pub struct DaemonState {
    /// Active voice + its per-voice cache. Swappable atomically via
    /// `wm.tts.reload_voice`.
    pub active: RwLock<ActiveVoice>,
    /// Cancel channel for the active utterance. `Some` only while a
    /// `handle_speak` is awaiting playback; `handle_cancel` `take()`s it
    /// and `send(())`s to interrupt.
    pub cancel_signal: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    /// Wall-clock instant at which the active utterance's player
    /// subprocess was spawned. `Some` while playback is in flight,
    /// `None` otherwise. Used by `handle_cancel` to compute
    /// `drained_ms` as `elapsed_since(spawn)`. See module doc.
    pub playback_started_at: Arc<Mutex<Option<Instant>>>,
    /// Declared audio length of the in-flight utterance, in ms.
    /// `Some` when the playback source has a known length (cache hit
    /// or Piper-rendered WAV), `None` for cloud MP3 streams where the
    /// total length isn't known up-front. `handle_cancel` uses this
    /// to cap `drained_ms` at the audio length (iter-17).
    pub audio_duration_ms: Arc<Mutex<Option<u64>>>,
    /// Player binary name (e.g. `pw-cat`, `paplay`).
    pub player_bin: String,
    /// Synthesis backend used to render cache misses on demand.
    /// Shared by `Arc` so it can be moved into `spawn_blocking` workers
    /// while remaining accessible to the dispatch loop.
    pub synth: Arc<PiperSubprocess>,
    /// Cloud TTS backend. Tried first (iter-8) when
    /// `CloudSynth::is_active` is true; failures fall back to
    /// [`Self::synth`]. Defaults to a disabled stub when the cloud env
    /// vars are unset.
    pub cloud: Arc<dyn CloudSynth>,
    /// Cache root directory; new voices construct per-voice cache
    /// managers under this root on hot-swap.
    pub cache_root: PathBuf,
    /// Phrase set to (re-)prerender on voice swap. Mirrors the YAML
    /// loaded at startup.
    pub cache_phrases: Vec<String>,
}

impl DaemonState {
    /// Construct a daemon state with the given config and the phrase
    /// set used for pre-render on startup AND on voice hot-swap.
    /// Cloud backend is read from environment via
    /// [`cloud_synth_from_env`]; tests can inject one via
    /// [`Self::with_cloud`].
    #[must_use]
    pub fn new(cfg: &TtsConfig, cache_phrases: Vec<String>) -> Self {
        Self::with_cloud(cfg, cache_phrases, cloud_synth_from_env())
    }

    /// Construct a daemon state with an injected cloud backend. The
    /// production path uses [`Self::new`]; tests use this to wire a
    /// stub backend that returns canned bytes or a forced error.
    #[must_use]
    pub fn with_cloud(
        cfg: &TtsConfig,
        cache_phrases: Vec<String>,
        cloud: Arc<dyn CloudSynth>,
    ) -> Self {
        let active = ActiveVoice {
            cache: CacheManager::new(&cfg.cache_root, &cfg.voice),
            voice: cfg.voice.clone(),
        };
        Self {
            active: RwLock::new(active),
            cancel_signal: Arc::new(Mutex::new(None)),
            playback_started_at: Arc::new(Mutex::new(None)),
            audio_duration_ms: Arc::new(Mutex::new(None)),
            player_bin: player_from_env(),
            synth: Arc::new(PiperSubprocess::from_env()),
            cloud,
            cache_root: cfg.cache_root.clone(),
            cache_phrases,
        }
    }
}


/// Decide the cache key for a phrase. Exact lowercase trimmed match
/// against the prerendered set.
fn phrase_key(text: &str) -> String {
    text.trim().to_lowercase()
}

/// Path of a cached WAV for `phrase` under the given cache manager.
/// Returns `Some(path)` if the WAV exists on disk, `None` otherwise.
/// Normalization (trim + lowercase) is done by [`CacheManager::entry_path`].
fn cache_hit_path(cache: &CacheManager, phrase: &str) -> Option<PathBuf> {
    let path = cache.entry_path(phrase);
    if path.exists() { Some(path) } else { None }
}

/// Render `text` with Piper for the daemon's active voice into the
/// per-voice WAV cache entry path, returning that path on success.
/// Used by [`handle_speak`] both for the no-cloud path AND as the
/// fallback after a cloud streaming failure (PRD §4 AC6 "clean
/// restart" semantics).
///
/// iter-11 removed the cloud branch that lived here in iter-8: cloud
/// rendering now always flows through [`try_cloud_stream_play`] so
/// this helper stays a single Piper-only producer.
///
/// The blocking Piper subprocess call is hoisted onto a
/// `spawn_blocking` worker so the dispatch loop keeps draining events.
///
/// On error, returns `(kind, message)` shaped for direct use in
/// [`publish_error`]: `kind="io"` for cache-dir or path I/O failures
/// raised by the synth backend, `kind="render"` for missing-binary,
/// non-zero-exit, or task-panic failures.
async fn render_on_demand(state: &DaemonState, text: &str) -> Result<PathBuf, (String, String)> {
    let (voice, target_wav, voice_dir) = {
        let active = state.active.read().await;
        (
            active.voice.clone(),
            active.cache.entry_path(text),
            active.cache.voice_dir(),
        )
    };
    if let Err(e) = std::fs::create_dir_all(&voice_dir) {
        return Err((
            "io".to_string(),
            format!("cache dir {}: {e}", voice_dir.display()),
        ));
    }

    let synth = Arc::clone(&state.synth);
    let text_owned = text.to_string();
    let target_inner = target_wav.clone();
    let res =
        tokio::task::spawn_blocking(move || synth.render(&voice, &text_owned, &target_inner)).await;
    match res {
        Ok(Ok(())) => Ok(target_wav),
        Ok(Err(SynthError::Io { source, path })) => Err((
            "io".to_string(),
            format!("synth io on {}: {source}", path.display()),
        )),
        Ok(Err(e)) => Err(("render".to_string(), format!("{e}"))),
        Err(join_err) => Err((
            "render".to_string(),
            format!("synth task join failed: {join_err}"),
        )),
    }
}

/// Argv (after the binary name) for the streaming player. Mirrors
/// [`player_args`] except the audio source is stdin (`-`) instead of
/// a file path, and the MP3 media-type is set unconditionally for
/// `pw-cat` (the cloud streaming path always emits MP3 chunks). Other
/// players (paplay, /bin/cat in tests) get no args — they read from
/// stdin by default.
fn streaming_player_args(player_bin: &str) -> Vec<OsString> {
    let is_pw_cat = player_bin == "pw-cat" || player_bin.ends_with("/pw-cat");
    let mut args: Vec<OsString> = Vec::new();
    if is_pw_cat {
        args.push("--playback".into());
        args.push("--media-type".into());
        args.push("audio/mpeg".into());
        args.push("-".into());
    }
    args
}

/// Spawn the streaming playback subprocess. Stdin is piped so the
/// cloud chunk pump can write MP3 frames as they arrive; stdout is
/// discarded; stderr is kept piped so a future iter can surface
/// player diagnostics in `wm.tts.error`.
fn spawn_streaming_player(player_bin: &str) -> Result<Child> {
    let mut cmd = Command::new(player_bin);
    cmd.args(streaming_player_args(player_bin));
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    cmd.spawn()
        .with_context(|| format!("spawn streaming player {player_bin}"))
}

/// Outcome of [`try_cloud_stream_play`]. The dispatch in
/// [`handle_speak`] decides whether to fall back to Piper based on
/// whether any audio reached the player before the failure.
#[derive(Debug)]
pub enum StreamOutcome {
    /// Stream completed (or was cancelled). `duration_ms` is wall-clock
    /// time from player spawn to player exit. `cancelled` is true iff
    /// the cancel channel fired before the stream ended naturally.
    Played {
        /// Wall-clock milliseconds from player spawn to exit.
        duration_ms: u64,
        /// True iff cancellation interrupted the stream.
        cancelled: bool,
    },
    /// Cloud refused to start a stream OR errored before any audio
    /// frame reached the player. Caller should fall back to Piper
    /// silently (no `wm.tts.error` event published).
    FailedBeforeFrame(CloudError),
    /// At least one audio frame was written to the player before the
    /// stream errored. Caller should publish
    /// `wm.tts.error{kind=stream}` and clean-restart the utterance
    /// with Piper.
    FailedMidStream(CloudError),
}

/// Stream cloud audio chunks into a freshly-spawned streaming player
/// subprocess. Races the chunk pump against the supplied cancel
/// receiver: if cancel fires, the player is killed and the outcome is
/// `Played { cancelled: true }`.
///
/// Frame accounting drives the fallback policy in [`handle_speak`]:
/// before any successful stdin write, errors are `FailedBeforeFrame`
/// (silent Piper fallback); after at least one frame, errors are
/// `FailedMidStream` (publish + clean restart).
async fn try_cloud_stream_play(
    state: &DaemonState,
    cancel_rx: oneshot::Receiver<()>,
    text: &str,
) -> StreamOutcome {
    let mut rx = match state.cloud.stream_render(text).await {
        Ok(rx) => rx,
        Err(e) => return StreamOutcome::FailedBeforeFrame(e),
    };

    let mut child = match spawn_streaming_player(&state.player_bin) {
        Ok(c) => c,
        Err(e) => {
            return StreamOutcome::FailedBeforeFrame(CloudError::Http(format!(
                "spawn streaming player: {e}"
            )));
        }
    };
    let Some(mut stdin) = child.stdin.take() else {
        let _ = child.start_kill();
        let _ = child.wait().await;
        return StreamOutcome::FailedBeforeFrame(CloudError::Http(
            "streaming player has no piped stdin".into(),
        ));
    };

    let played_start = Instant::now();
    let mut frame_count: usize = 0;
    let mut last_err: Option<CloudError> = None;
    let mut cancelled = false;
    let mut cancel_rx = cancel_rx;

    loop {
        tokio::select! {
            biased;
            _ = &mut cancel_rx => {
                cancelled = true;
                break;
            }
            item = rx.recv() => {
                match item {
                    Some(Ok(bytes)) => {
                        if let Err(e) = stdin.write_all(&bytes).await {
                            last_err = Some(CloudError::Http(format!(
                                "player stdin write: {e}"
                            )));
                            break;
                        }
                        frame_count = frame_count.saturating_add(1);
                    }
                    Some(Err(e)) => {
                        last_err = Some(e);
                        break;
                    }
                    None => {
                        break;
                    }
                }
            }
        }
    }

    // Close stdin so a healthy player can flush its buffer and exit.
    drop(stdin);

    if cancelled {
        let _ = child.start_kill();
        let _ = child.wait().await;
        let duration_ms = u64::try_from(played_start.elapsed().as_millis()).unwrap_or(u64::MAX);
        return StreamOutcome::Played {
            duration_ms,
            cancelled: true,
        };
    }

    if let Some(e) = last_err {
        let _ = child.start_kill();
        let _ = child.wait().await;
        if frame_count > 0 {
            return StreamOutcome::FailedMidStream(e);
        }
        return StreamOutcome::FailedBeforeFrame(e);
    }

    // Stream ended cleanly — wait for the player to drain & exit.
    let _ = child.wait().await;
    let duration_ms = u64::try_from(played_start.elapsed().as_millis()).unwrap_or(u64::MAX);
    StreamOutcome::Played {
        duration_ms,
        cancelled: false,
    }
}

/// Build the argv (after the binary name) to play `audio` with
/// `player_bin`. Extracted so tests can assert pw-cat gets
/// `--media-type=audio/mpeg` for MP3 cloud renders without forking a
/// process. See [`spawn_player`].
fn player_args(player_bin: &str, audio: &Path) -> Vec<OsString> {
    let is_pw_cat = player_bin == "pw-cat" || player_bin.ends_with("/pw-cat");
    let is_mp3 = audio
        .extension()
        .and_then(|s| s.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("mp3"));
    let mut args: Vec<OsString> = Vec::new();
    if is_pw_cat {
        args.push("--playback".into());
        if is_mp3 {
            args.push("--media-type".into());
            args.push("audio/mpeg".into());
        }
    }
    args.push(audio.as_os_str().to_os_string());
    args
}

/// Spawn a playback subprocess for the audio file at `audio`.
/// `pw-cat` needs the `--playback` flag; `paplay` takes the file as a
/// positional arg. Both shapes are detected from the binary name
/// suffix. iter-8: MP3 files (produced by the cloud backend) require
/// `--media-type=audio/mpeg` for pw-cat — without it pw-cat would
/// treat the MP3 bytes as raw PCM and emit silence or noise.
fn spawn_player(player_bin: &str, audio: &Path) -> Result<Child> {
    let mut cmd = Command::new(player_bin);
    cmd.args(player_args(player_bin, audio));
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    cmd.spawn()
        .with_context(|| format!("spawn {player_bin} {}", audio.display()))
}

/// Handle a `wm.tts.speak` request.
///
/// Flow:
/// 1. Publish `wm.tts.start`.
/// 2. Cache-hit path: play the existing per-voice WAV with
///    [`spawn_player`] + cancel race; publish `wm.tts.end`.
/// 3. Cloud-active cache-miss path (iter-11): stream MP3 chunks
///    straight into `pw-cat --media-type=audio/mpeg -` via
///    [`try_cloud_stream_play`]. On `FailedBeforeFrame`, silently
///    fall back to Piper. On `FailedMidStream`, publish
///    `wm.tts.error{kind=stream}` and clean-restart with Piper (AC6).
/// 4. Piper cache-miss path: [`render_on_demand`] then
///    [`spawn_player`] + cancel race.
///
/// All paths converge on a final `wm.tts.end` publish (except the
/// terminal-error cases, which publish `wm.tts.error` and return).
async fn handle_speak(
    state: &DaemonState,
    publish: &mut dyn EventSink,
    req: &SpeakRequest,
) -> Result<()> {
    let ts = now_unix_ms();
    let start = StartEvent {
        text: req.text.clone(),
        source: req.priority.clone().unwrap_or_default(),
        ts,
    };
    publish
        .publish(outgoing::START, serde_json::to_value(&start)?)
        .await?;

    let played_start = Instant::now();

    let hit = {
        let active = state.active.read().await;
        cache_hit_path(&active.cache, &req.text)
    };

    let cancelled = if let Some(wav) = hit {
        play_file_with_cancel(state, &wav).await
    } else if state.cloud.is_active() {
        // Streaming-first: install cancel BEFORE calling cloud so a
        // cancel arriving mid-handshake interrupts the pump. MP3 frame
        // length isn't known up-front, so no duration cap.
        let cancel_rx = install_cancel_slot(state, None).await;
        let outcome = try_cloud_stream_play(state, cancel_rx, &req.text).await;
        match outcome {
            StreamOutcome::Played { cancelled, .. } => {
                clear_cancel_slot(state).await;
                cancelled
            }
            StreamOutcome::FailedBeforeFrame(e) => {
                clear_cancel_slot(state).await;
                warn!(
                    text = %req.text,
                    error = %e,
                    "wm-tts: cloud stream failed before first frame; falling back to piper"
                );
                match piper_fallback(state, publish, &req.text).await {
                    Ok(c) => c,
                    Err(()) => return Ok(()), // publish_error already fired
                }
            }
            StreamOutcome::FailedMidStream(e) => {
                clear_cancel_slot(state).await;
                warn!(
                    text = %req.text,
                    error = %e,
                    "wm-tts: cloud stream failed mid-utterance; restarting with piper"
                );
                publish_error(publish, "stream", &format!("{e}")).await?;
                match piper_fallback(state, publish, &req.text).await {
                    Ok(c) => c,
                    Err(()) => return Ok(()), // publish_error already fired
                }
            }
        }
    } else {
        match piper_fallback(state, publish, &req.text).await {
            Ok(c) => c,
            Err(()) => return Ok(()),
        }
    };

    let duration_ms = u64::try_from(played_start.elapsed().as_millis()).unwrap_or(u64::MAX);
    let end = EndEvent {
        text: req.text.clone(),
        duration_ms,
        ts: now_unix_ms(),
    };
    publish
        .publish(outgoing::END, serde_json::to_value(&end)?)
        .await?;

    if cancelled {
        info!(text = %req.text, duration_ms, "wm-tts: playback cancelled");
    }
    Ok(())
}

/// Install a fresh cancel channel into [`DaemonState::cancel_signal`]
/// and return the receiver. Overwrites any prior slot (the prior
/// utterance has already raced its cancel and would have cleared on
/// completion). `audio_duration_ms` is the known length of the audio
/// the caller is about to play; `Some` for file-based plays where the
/// WAV header is parsed up-front, `None` for cloud MP3 streams.
async fn install_cancel_slot(
    state: &DaemonState,
    audio_duration_ms: Option<u64>,
) -> oneshot::Receiver<()> {
    let (tx, rx) = oneshot::channel::<()>();
    let mut guard = state.cancel_signal.lock().await;
    *guard = Some(tx);
    drop(guard);
    let mut started = state.playback_started_at.lock().await;
    *started = Some(Instant::now());
    drop(started);
    let mut dur = state.audio_duration_ms.lock().await;
    *dur = audio_duration_ms;
    rx
}

/// Drop the cancel slot (after the racing handler returned). Idempotent.
async fn clear_cancel_slot(state: &DaemonState) {
    let mut guard = state.cancel_signal.lock().await;
    *guard = None;
    drop(guard);
    let mut started = state.playback_started_at.lock().await;
    *started = None;
    drop(started);
    let mut dur = state.audio_duration_ms.lock().await;
    *dur = None;
}

/// Play an existing WAV/MP3 file with the daemon's player binary,
/// racing playback completion against cancel. Returns `true` iff
/// cancellation interrupted playback. WAV duration is parsed from the
/// header (best-effort) and used to cap `drained_ms` on cancel; an
/// unparseable header silently falls back to elapsed-only bound.
async fn play_file_with_cancel(state: &DaemonState, wav: &Path) -> bool {
    let duration_ms = crate::wav::parse_duration_ms(wav).ok();
    let cancel_rx = install_cancel_slot(state, duration_ms).await;
    let mut child = match spawn_player(&state.player_bin, wav) {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "wm-tts: spawn_player failed for cache hit");
            clear_cancel_slot(state).await;
            return false;
        }
    };
    let cancelled;
    tokio::select! {
        wait_res = child.wait() => {
            cancelled = false;
            if let Err(e) = wait_res {
                warn!(error = %e, "wm-tts: playback wait failed");
            }
        }
        _ = cancel_rx => {
            cancelled = true;
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }
    clear_cancel_slot(state).await;
    cancelled
}

/// Piper-only render-then-play for both the no-cloud path and the
/// cloud-fallback path. On Piper render error, publishes
/// `wm.tts.error{kind}` and returns `Err(())` so the caller can short
/// out before the `wm.tts.end` publish. On success, returns `true` iff
/// cancellation interrupted playback.
async fn piper_fallback(
    state: &DaemonState,
    publish: &mut dyn EventSink,
    text: &str,
) -> std::result::Result<bool, ()> {
    let wav = match render_on_demand(state, text).await {
        Ok(p) => {
            info!(
                phrase = %phrase_key(text),
                path = %p.display(),
                "wm-tts: rendered cache miss on demand"
            );
            p
        }
        Err((kind, message)) => {
            if let Err(e) = publish_error(publish, &kind, &message).await {
                error!(error = %e, "wm-tts: failed to publish render error");
            }
            return Err(());
        }
    };
    Ok(play_file_with_cancel(state, &wav).await)
}

/// Handle a `wm.tts.cancel` request. Fires the cancel channel of the
/// active utterance (if any) and publishes `wm.tts.cancel.ack`.
/// `drained_ms` is the wall-clock elapsed since playback start (see
/// module doc on the approximation); `0` when no utterance is active.
async fn handle_cancel(state: &DaemonState, publish: &mut dyn EventSink) -> Result<()> {
    let taken = {
        let mut guard = state.cancel_signal.lock().await;
        guard.take()
    };
    if let Some(tx) = taken {
        let _ = tx.send(());
    }
    let drained_ms = {
        let started = state.playback_started_at.lock().await;
        let started_at = *started;
        drop(started);
        let duration = *state.audio_duration_ms.lock().await;
        let elapsed = started_at
            .map_or(0, |t| u64::try_from(t.elapsed().as_millis()).unwrap_or(u64::MAX));
        duration.map_or(elapsed, |d| elapsed.min(d))
    };
    let ack = CancelAckEvent {
        ts: now_unix_ms(),
        drained_ms,
    };
    publish
        .publish(outgoing::CANCEL_ACK, serde_json::to_value(&ack)?)
        .await?;
    Ok(())
}

/// Handle a `wm.tts.reload_voice` request: build a new per-voice
/// [`CacheManager`], pre-render the configured phrase set for that
/// voice, then atomically swap [`DaemonState::active`] under a write
/// lock and publish `wm.tts.reload.ack`.
///
/// Per-phrase render failures are non-fatal — the swap still completes
/// (matches startup behavior) and the failure count is surfaced in the
/// ack payload. Only a top-level pre-render failure (e.g. cache dir
/// not writable) publishes `error{kind=voice}` and aborts the swap.
async fn handle_reload_voice(
    state: &DaemonState,
    publish: &mut dyn EventSink,
    new_voice: &str,
) -> Result<()> {
    let start_ms = now_unix_ms();
    let new_cache = CacheManager::new(&state.cache_root, new_voice);
    let report = match new_cache.prerender(&state.cache_phrases, state.synth.as_ref()) {
        Ok(r) => r,
        Err(err) => {
            warn!(
                voice = %new_voice,
                error = %err,
                "wm-tts: reload_voice prerender aborted"
            );
            return publish_error(
                publish,
                "voice",
                &format!("prerender failed for {new_voice}: {err}"),
            )
            .await;
        }
    };
    {
        let mut active = state.active.write().await;
        active.voice = new_voice.to_string();
        active.cache = new_cache;
    }
    let elapsed_ms = now_unix_ms().saturating_sub(start_ms);
    info!(
        voice = %new_voice,
        hits = report.hits,
        rendered = report.rendered,
        failures = report.failures.len(),
        elapsed_ms,
        "wm-tts: reload_voice complete"
    );
    let ack = ReloadAckEvent {
        voice: new_voice.to_string(),
        cache_hits: report.hits,
        prerendered: report.rendered,
        failures: report.failures.len(),
        elapsed_ms,
        ts: now_unix_ms(),
    };
    publish
        .publish(outgoing::RELOAD_ACK, serde_json::to_value(&ack)?)
        .await
}

async fn publish_error(publish: &mut dyn EventSink, kind: &str, message: &str) -> Result<()> {
    let ev = ErrorEvent {
        kind: kind.to_string(),
        message: message.to_string(),
        ts: now_unix_ms(),
    };
    publish
        .publish(outgoing::ERROR, serde_json::to_value(&ev)?)
        .await
}

/// Abstraction for the publish side of the bus so handlers can be tested
/// without an actual agorabus daemon. The production impl is
/// [`AgoraSink`]; tests use an in-memory sink.
#[async_trait::async_trait]
pub trait EventSink: Send {
    /// Publish `data` on `topic`. Errors are non-fatal at the dispatch
    /// layer (logged + an `error` event is published instead).
    async fn publish(&mut self, topic: &str, data: Value) -> Result<()>;
}

/// Production sink: publishes through an agorabus [`agorabus::Client`].
pub struct AgoraSink {
    pub(crate) inner: agorabus::Client,
}

#[async_trait::async_trait]
impl EventSink for AgoraSink {
    async fn publish(&mut self, topic: &str, data: Value) -> Result<()> {
        let reply = self.inner.publish(topic, data).await?;
        if !reply.ok {
            warn!(
                topic = %topic,
                err = %reply.error.as_deref().unwrap_or("?"),
                "wm-tts: bus rejected publish"
            );
        }
        Ok(())
    }
}

/// Dispatch one decoded request to the correct handler.
///
/// # Errors
///
/// Propagates any error returned by the per-variant handler (typically
/// publish failures from the underlying [`EventSink`]).
pub async fn dispatch(
    state: &DaemonState,
    publish: &mut dyn EventSink,
    req: Request,
) -> Result<()> {
    match req {
        Request::Speak(s) => handle_speak(state, publish, &s).await,
        Request::Cancel(_) => handle_cancel(state, publish).await,
        Request::ReloadVoice(rv) => handle_reload_voice(state, publish, &rv.voice).await,
    }
}

/// Run the live daemon: load cache, prerender, connect to agorabus,
/// subscribe to `wm.tts.`, dispatch each event until the bus closes or
/// a shutdown signal arrives.
///
/// # Errors
///
/// Propagates I/O failures from config loading or the agorabus client.
/// Per-phrase cache-render failures are logged and do not abort startup.
pub async fn run(cache_config: &Path) -> Result<()> {
    let cfg = TtsConfig::default();
    let cache_phrases = load_cache_yaml(cache_config)
        .with_context(|| format!("loading cache config from {}", cache_config.display()))?;
    let state = DaemonState::new(&cfg, cache_phrases.phrases.clone());

    // Pre-render (idempotent). Failures are non-fatal — cache misses
    // are rendered on demand at request time via `render_on_demand`.
    {
        let active = state.active.read().await;
        match active
            .cache
            .prerender(&cache_phrases.phrases, state.synth.as_ref())
        {
            Ok(report) => info!(
                voice = %cfg.voice,
                phrases = cache_phrases.phrases.len(),
                hits = report.hits,
                rendered = report.rendered,
                failures = report.failures.len(),
                "wm-tts: pre-render complete"
            ),
            Err(err) => warn!(error = %err, "wm-tts: pre-render aborted; continuing"),
        }
    }

    // Connect to agorabus. Fail-open: if the bus isn't running, log and
    // exit so the systemd unit restarts us when it comes back. The
    // `WM_TTS_BUS_SOCKET` override mirrors the `WM_TTS_VOICE`/
    // `WM_TTS_PLAYER`/`WM_TTS_CACHE_ROOT` env idiom and lets
    // `tests/bus_smoke.rs` point the daemon at a per-test temp socket
    // without touching `$HOME`.
    let sock = std::env::var("WM_TTS_BUS_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|_| agorabus::default_socket_path());
    let Some(mut sub_client) = agorabus::Client::try_connect(&sock).await? else {
        warn!(socket = %sock.display(), "wm-tts: agorabus not reachable; exiting");
        return Ok(());
    };
    sub_client
        .announce(
            &format!("wm-tts-{}-sub", std::process::id()),
            std::process::id(),
            "",
            "wm-tts control subscribe",
        )
        .await?;
    sub_client.subscribe(bus::TOPIC_PREFIX).await?;
    info!(prefix = bus::TOPIC_PREFIX, "wm-tts: subscribed");

    // Separate connection for publishing — read/write on a subscribed
    // socket would interleave Reply lines with the broadcast stream.
    let mut pub_client = agorabus::Client::connect(&sock).await?;
    pub_client
        .announce(
            &format!("wm-tts-{}", std::process::id()),
            std::process::id(),
            "",
            "wm-tts publish path",
        )
        .await?;
    let mut sink = AgoraSink { inner: pub_client };

    // Dispatch loop. Each event runs to completion before the next is
    // read — barge-in already works because cancel arrives on a separate
    // connection's broadcast and the cancel handler is a *fast* path
    // (sends through the oneshot, returns). iter-6 will hoist
    // `handle_speak` into a spawned task so the loop can race many.
    while let Some(ev) = sub_client.next_event().await? {
        match decode_request(&ev.topic, &ev.data) {
            Ok(req) => {
                if let Err(err) = dispatch(&state, &mut sink, req).await {
                    error!(topic = %ev.topic, err = %err, "wm-tts: dispatch failed");
                    let _ = publish_error(&mut sink, "bus", &format!("dispatch: {err}")).await;
                }
            }
            Err(err) => {
                warn!(topic = %ev.topic, err = %err, "wm-tts: decode failed");
                let _ = publish_error(&mut sink, "bus", &format!("decode: {err}")).await;
            }
        }
    }
    info!("wm-tts: bus closed; daemon exiting");
    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::indexing_slicing,
    clippy::significant_drop_tightening,
    clippy::redundant_clone
)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    /// In-memory publish sink for unit tests.
    #[derive(Default, Clone)]
    struct MemSink {
        events: Arc<StdMutex<Vec<(String, Value)>>>,
    }

    #[async_trait::async_trait]
    impl EventSink for MemSink {
        async fn publish(&mut self, topic: &str, data: Value) -> Result<()> {
            self.events
                .lock()
                .expect("mem sink poisoned")
                .push((topic.to_string(), data));
            Ok(())
        }
    }

    fn tmp_state() -> (DaemonState, tempfile::TempDir) {
        tmp_state_with_phrases(Vec::new())
    }

    fn tmp_state_with_phrases(phrases: Vec<String>) -> (DaemonState, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = TtsConfig {
            voice: "test-voice".into(),
            cache_root: dir.path().to_path_buf(),
            cloud_quality: false,
        };
        let state = DaemonState::new(&cfg, phrases);
        (state, dir)
    }

    #[tokio::test]
    async fn cancel_publishes_ack_when_no_active_player() {
        let (state, _g) = tmp_state();
        let mut sink = MemSink::default();
        handle_cancel(&state, &mut sink).await.expect("cancel ok");
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, outgoing::CANCEL_ACK);
        let parsed: CancelAckEvent =
            serde_json::from_value(events[0].1.clone()).expect("ack decodes");
        assert_eq!(parsed.drained_ms, 0);
    }

    #[tokio::test]
    async fn cancel_drained_ms_reflects_playback_elapsed() {
        let (state, _g) = tmp_state();
        let _rx = install_cancel_slot(&state, None).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let mut sink = MemSink::default();
        handle_cancel(&state, &mut sink).await.expect("cancel ok");
        let events = sink.events.lock().unwrap();
        let parsed: CancelAckEvent =
            serde_json::from_value(events[0].1.clone()).expect("ack decodes");
        assert!(
            parsed.drained_ms >= 15,
            "drained_ms should reflect ~20ms wait, got {}",
            parsed.drained_ms
        );
        assert!(
            parsed.drained_ms < 5_000,
            "drained_ms should be a sane elapsed, got {}",
            parsed.drained_ms
        );
    }

    #[tokio::test]
    async fn cancel_drained_ms_capped_by_audio_duration() {
        // iter-17: install_cancel_slot with a known 10ms audio length,
        // then sleep 60ms — elapsed >> declared length. drained_ms
        // must be capped at the declared length, not wall-clock.
        let (state, _g) = tmp_state();
        let _rx = install_cancel_slot(&state, Some(10)).await;
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        let mut sink = MemSink::default();
        handle_cancel(&state, &mut sink).await.expect("cancel ok");
        let events = sink.events.lock().unwrap();
        let parsed: CancelAckEvent =
            serde_json::from_value(events[0].1.clone()).expect("ack decodes");
        assert_eq!(
            parsed.drained_ms, 10,
            "drained_ms should be capped at audio length, got {}",
            parsed.drained_ms
        );
    }

    #[tokio::test]
    async fn cancel_drained_ms_uses_elapsed_when_under_duration() {
        // Sleep 20ms with a generous 5_000ms declared length — the cap
        // should NOT clamp to the wall-clock value (i.e. the min picks
        // elapsed, not duration).
        let (state, _g) = tmp_state();
        let _rx = install_cancel_slot(&state, Some(5_000)).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let mut sink = MemSink::default();
        handle_cancel(&state, &mut sink).await.expect("cancel ok");
        let events = sink.events.lock().unwrap();
        let parsed: CancelAckEvent =
            serde_json::from_value(events[0].1.clone()).expect("ack decodes");
        assert!(
            (15..200).contains(&parsed.drained_ms),
            "drained_ms should reflect ~20ms elapsed (not the 5000ms cap), got {}",
            parsed.drained_ms
        );
    }

    #[tokio::test]
    async fn reload_voice_publishes_ack_and_swaps_active() {
        let (state, _g) = tmp_state();
        let mut sink = MemSink::default();
        assert_eq!(state.active.read().await.voice, "test-voice");

        handle_reload_voice(&state, &mut sink, "en_GB-jenny")
            .await
            .expect("reload publishes");

        {
            let events = sink.events.lock().unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].0, outgoing::RELOAD_ACK);
            let ack: ReloadAckEvent =
                serde_json::from_value(events[0].1.clone()).expect("ack decodes");
            assert_eq!(ack.voice, "en_GB-jenny");
            // Empty phrase set → zero rendered, zero hits, zero failures.
            assert_eq!(ack.cache_hits, 0);
            assert_eq!(ack.prerendered, 0);
            assert_eq!(ack.failures, 0);
        }

        // State swapped: active voice + the cache root now points at the
        // new voice's per-voice subdir.
        let active = state.active.read().await;
        assert_eq!(active.voice, "en_GB-jenny");
        assert!(active.cache.voice_dir().ends_with("en_GB-jenny"));
    }

    #[tokio::test]
    async fn reload_voice_with_phrases_reports_failures_but_still_swaps() {
        // piper isn't on PATH in CI → render fails per-phrase, but the
        // swap still completes and the ack carries the failure count.
        let (state, _g) =
            tmp_state_with_phrases(vec!["yes".to_string(), "no".to_string()]);
        let mut sink = MemSink::default();
        handle_reload_voice(&state, &mut sink, "en_US-amy-medium")
            .await
            .expect("reload publishes");
        {
            let events = sink.events.lock().unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].0, outgoing::RELOAD_ACK);
            let ack: ReloadAckEvent =
                serde_json::from_value(events[0].1.clone()).expect("ack decodes");
            assert_eq!(ack.voice, "en_US-amy-medium");
            // No phrases were pre-existing for this fresh voice dir; piper
            // either rendered them (if installed) or failed per-phrase.
            assert_eq!(ack.cache_hits, 0);
            assert_eq!(ack.prerendered + ack.failures, 2);
        }
        assert_eq!(state.active.read().await.voice, "en_US-amy-medium");
    }

    #[tokio::test]
    async fn speak_with_cache_miss_publishes_start_then_error() {
        let (state, _g) = tmp_state();
        let mut sink = MemSink::default();
        let req = SpeakRequest {
            text: "totally uncached phrase".into(),
            priority: Some("normal".into()),
            cancel_previous: false,
        };
        handle_speak(&state, &mut sink, &req)
            .await
            .expect("speak handler ok");
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].0, outgoing::START);
        assert_eq!(events[1].0, outgoing::ERROR);
        let err: ErrorEvent =
            serde_json::from_value(events[1].1.clone()).expect("error decodes");
        assert_eq!(err.kind, "render");
    }

    #[tokio::test]
    async fn dispatch_routes_each_variant() {
        let (state, _g) = tmp_state();
        let mut sink = MemSink::default();
        dispatch(
            &state,
            &mut sink,
            Request::Cancel(crate::bus::CancelRequest::default()),
        )
        .await
        .expect("cancel ok");
        dispatch(
            &state,
            &mut sink,
            Request::ReloadVoice(crate::bus::ReloadVoiceRequest {
                voice: "v".into(),
            }),
        )
        .await
        .expect("reload ok");
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].0, outgoing::CANCEL_ACK);
        assert_eq!(events[1].0, outgoing::RELOAD_ACK);
    }

    #[test]
    fn phrase_key_trims_and_lowercases() {
        assert_eq!(phrase_key("  YES "), "yes");
        assert_eq!(phrase_key("I'M HERE"), "i'm here");
    }

    #[test]
    fn default_player_is_pw_cat() {
        assert_eq!(DEFAULT_PLAYER, "pw-cat");
    }

    #[test]
    fn player_args_wav_pw_cat() {
        let args = player_args("pw-cat", Path::new("/tmp/x.wav"));
        let owned: Vec<String> = args
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();
        assert_eq!(owned, vec!["--playback", "/tmp/x.wav"]);
    }

    #[test]
    fn player_args_mp3_pw_cat_sets_media_type() {
        let args = player_args("pw-cat", Path::new("/tmp/x.mp3"));
        let owned: Vec<String> = args
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            owned,
            vec!["--playback", "--media-type", "audio/mpeg", "/tmp/x.mp3"]
        );
    }

    #[test]
    fn player_args_paplay_unchanged_for_mp3() {
        let args = player_args("paplay", Path::new("/tmp/x.mp3"));
        let owned: Vec<String> = args
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();
        // paplay doesn't speak --media-type; rely on libsndfile/ffmpeg
        // sniffing (paplay handles MP3 via PA in practice). Keep the
        // call simple — positional file only.
        assert_eq!(owned, vec!["/tmp/x.mp3"]);
    }

    /// Stub backend that reports active and always errors. Used to
    /// verify `handle_speak` attempts the cloud streaming path AND
    /// silently falls back to Piper on synchronous failure (AC6
    /// before-frame case).
    #[derive(Default, Clone)]
    struct ErroringCloud {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl crate::cloud::CloudSynth for ErroringCloud {
        async fn render(
            &self,
            _text: &str,
        ) -> std::result::Result<bytes::Bytes, crate::cloud::CloudError> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(crate::cloud::CloudError::Http("forced".into()))
        }
        fn is_active(&self) -> bool {
            true
        }
    }

    /// Stub backend that streams one Ok chunk, then Err. Drives the
    /// AC6 mid-stream restart path: the first frame reaches the
    /// player, the second is an error item on the receiver, and
    /// `try_cloud_stream_play` returns `FailedMidStream`.
    #[derive(Default, Clone)]
    struct MidStreamFailCloud;

    #[async_trait::async_trait]
    impl crate::cloud::CloudSynth for MidStreamFailCloud {
        async fn render(
            &self,
            _text: &str,
        ) -> std::result::Result<bytes::Bytes, crate::cloud::CloudError> {
            Err(crate::cloud::CloudError::NotEnabled)
        }
        async fn stream_render(
            &self,
            _text: &str,
        ) -> std::result::Result<
            tokio::sync::mpsc::Receiver<
                std::result::Result<bytes::Bytes, crate::cloud::CloudError>,
            >,
            crate::cloud::CloudError,
        > {
            let (tx, rx) = tokio::sync::mpsc::channel(2);
            tokio::spawn(async move {
                let _ = tx.send(Ok(bytes::Bytes::from_static(b"frame-1"))).await;
                let _ = tx
                    .send(Err(crate::cloud::CloudError::Http(
                        "forced mid-stream".into(),
                    )))
                    .await;
            });
            Ok(rx)
        }
        fn is_active(&self) -> bool {
            true
        }
    }

    fn tmp_state_with_cloud(
        cloud: Arc<dyn crate::cloud::CloudSynth>,
    ) -> (DaemonState, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = TtsConfig {
            voice: "test-voice".into(),
            cache_root: dir.path().to_path_buf(),
            cloud_quality: true,
        };
        let state = DaemonState::with_cloud(&cfg, Vec::new(), cloud);
        (state, dir)
    }

    #[tokio::test]
    async fn cloud_failure_falls_back_to_piper_then_publishes_error() {
        // Cloud is active but stream_render's default impl errors via
        // render(). Piper subprocess isn't available in the test
        // environment, so the silent fallback ultimately publishes a
        // render error — but the cloud must be tried first.
        let cloud = Arc::new(ErroringCloud::default());
        let counter = Arc::clone(&cloud.calls);
        let (state, _g) = tmp_state_with_cloud(cloud);
        let mut sink = MemSink::default();
        let req = SpeakRequest {
            text: "uncached cloud-fallback phrase".into(),
            priority: None,
            cancel_previous: false,
        };
        handle_speak(&state, &mut sink, &req)
            .await
            .expect("speak handler ok");
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "cloud backend must be invoked exactly once before fallback"
        );
        let events = sink.events.lock().unwrap();
        // FailedBeforeFrame is silent — no stream-error event between
        // START and the Piper render error.
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].0, outgoing::START);
        assert_eq!(events[1].0, outgoing::ERROR);
        let err: ErrorEvent =
            serde_json::from_value(events[1].1.clone()).expect("error decodes");
        // Piper isn't installed in test env, so the fallback failed
        // — but that confirms the fallback path was taken.
        assert_eq!(err.kind, "render");
    }

    #[tokio::test]
    async fn try_cloud_stream_play_reports_failed_before_frame_on_sync_err() {
        // ErroringCloud.stream_render's default impl awaits render(),
        // which returns Err synchronously → no player spawn, no chunk
        // pump, FailedBeforeFrame.
        let cloud = Arc::new(ErroringCloud::default());
        let (mut state, _g) = tmp_state_with_cloud(cloud);
        // Player binary doesn't matter — we never reach the spawn.
        state.player_bin = "/usr/bin/false".into();
        let (_tx, rx) = oneshot::channel::<()>();
        match try_cloud_stream_play(&state, rx, "anything").await {
            StreamOutcome::FailedBeforeFrame(_) => {}
            other => panic!("expected FailedBeforeFrame, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn try_cloud_stream_play_reports_failed_mid_stream_after_first_frame() {
        // /bin/cat is the test stand-in for pw-cat: reads stdin, writes
        // to /dev/null (Stdio::null), exits when stdin closes. The
        // first Ok chunk succeeds; the second item is Err →
        // FailedMidStream.
        let cloud = Arc::new(MidStreamFailCloud);
        let (mut state, _g) = tmp_state_with_cloud(cloud);
        state.player_bin = "/bin/cat".into();
        let (_tx, rx) = oneshot::channel::<()>();
        match try_cloud_stream_play(&state, rx, "anything").await {
            StreamOutcome::FailedMidStream(_) => {}
            other => panic!("expected FailedMidStream, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn handle_speak_publishes_stream_error_on_mid_stream_failure_then_piper_restart() {
        // Mid-stream cloud failure → wm.tts.error{kind=stream} → fall
        // through to Piper (which is missing in CI) → wm.tts.error{
        // kind=render}. Three events: START, ERROR(stream), ERROR(render).
        let cloud = Arc::new(MidStreamFailCloud);
        let (mut state, _g) = tmp_state_with_cloud(cloud);
        state.player_bin = "/bin/cat".into();
        let mut sink = MemSink::default();
        let req = SpeakRequest {
            text: "uncached mid-stream phrase".into(),
            priority: None,
            cancel_previous: false,
        };
        handle_speak(&state, &mut sink, &req)
            .await
            .expect("speak handler ok");
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 3, "expected START, stream ERR, render ERR");
        assert_eq!(events[0].0, outgoing::START);
        assert_eq!(events[1].0, outgoing::ERROR);
        let stream_err: ErrorEvent =
            serde_json::from_value(events[1].1.clone()).expect("stream err decodes");
        assert_eq!(stream_err.kind, "stream");
        assert_eq!(events[2].0, outgoing::ERROR);
        let render_err: ErrorEvent =
            serde_json::from_value(events[2].1.clone()).expect("render err decodes");
        assert_eq!(render_err.kind, "render");
    }

    #[test]
    fn streaming_player_args_pw_cat_pipes_stdin_with_mp3_media_type() {
        let args = streaming_player_args("pw-cat");
        let owned: Vec<String> = args
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            owned,
            vec!["--playback", "--media-type", "audio/mpeg", "-"]
        );
    }

    #[test]
    fn streaming_player_args_non_pw_cat_is_empty() {
        let args = streaming_player_args("/bin/cat");
        assert!(args.is_empty());
        let args = streaming_player_args("paplay");
        assert!(args.is_empty());
    }

    #[tokio::test]
    async fn cache_hit_lookup_finds_existing_wav() {
        // We can't spawn a real `pw-cat` in CI, but cache_hit_path itself
        // is testable: write a fake WAV under the CacheManager layout
        // and confirm the (Trim + lowercase)-normalised lookup finds it.
        let (state, _g) = tmp_state();
        let active = state.active.read().await;
        let wav = active.cache.entry_path("hello world");
        std::fs::create_dir_all(wav.parent().unwrap()).unwrap();
        std::fs::write(&wav, b"RIFF\0\0\0\0WAVEfake").unwrap();
        assert_eq!(
            cache_hit_path(&active.cache, "  Hello World "),
            Some(wav.clone())
        );
        assert_eq!(cache_hit_path(&active.cache, "nope"), None);
    }
}
