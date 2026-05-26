//! Live agorabus subscribe loop for `wm-tts` (iter-6).
//!
//! Wires the bus schema from [`crate::bus`] to a real subscribe loop. On
//! a `wm.tts.speak` request, the daemon checks the per-voice cache for an
//! exact (trimmed lowercase) match; cache hits play through `pw-cat`
//! (`PipeWire` native CLI) and emit `wm.tts.start` + `wm.tts.end`.
//!
//! iter-6 wires cache misses to the Piper subprocess: the blocking
//! [`crate::synth::Synth::render`] call is hoisted onto a
//! `tokio::task::spawn_blocking` worker, the resulting WAV is written
//! straight into the cache entry path, and the cache-hit playback path
//! is then reused. The next request for the same phrase resolves as a
//! hit. Render failures still publish `wm.tts.error{kind="render"}`.
//!
//! Cancel is wired with a `oneshot::Sender<()>` stored in
//! [`DaemonState`]: `wm.tts.cancel` fires the channel, which interrupts
//! the playback `tokio::select!` and `SIGKILL`s the player subprocess.
//! `drained_ms` is reported as `0` until a future iter measures real
//! playback position via a `PipeWire`-rs streaming consumer.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use serde_json::Value;
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, oneshot};
use tracing::{error, info, warn};

use crate::bus::{
    self, CancelAckEvent, EndEvent, ErrorEvent, Request, SpeakRequest, StartEvent, decode_request,
    now_unix_ms, outgoing,
};
use crate::cache::CacheManager;
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

/// Live daemon state passed to per-request handlers.
pub struct DaemonState {
    /// Per-voice cache lookup (wraps `CacheManager`).
    pub cache: CacheManager,
    /// Configured voice id.
    pub voice: String,
    /// Cancel channel for the active utterance. `Some` only while a
    /// `handle_speak` is awaiting playback; `handle_cancel` `take()`s it
    /// and `send(())`s to interrupt.
    pub cancel_signal: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    /// Player binary name (e.g. `pw-cat`, `paplay`).
    pub player_bin: String,
    /// Synthesis backend used to render cache misses on demand.
    /// Shared by `Arc` so it can be moved into `spawn_blocking` workers
    /// while remaining accessible to the dispatch loop.
    pub synth: Arc<PiperSubprocess>,
}

impl DaemonState {
    /// Construct a daemon state with the given config.
    #[must_use]
    pub fn new(cfg: &TtsConfig) -> Self {
        Self {
            cache: CacheManager::new(&cfg.cache_root, &cfg.voice),
            voice: cfg.voice.clone(),
            cancel_signal: Arc::new(Mutex::new(None)),
            player_bin: player_from_env(),
            synth: Arc::new(PiperSubprocess::from_env()),
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

/// Render `text` for the daemon's active voice into the cache entry
/// path, returning that path on success. Used by [`handle_speak`] to
/// turn a cache miss into a renderable WAV. The blocking Piper
/// subprocess call is hoisted onto a `spawn_blocking` worker so the
/// dispatch loop keeps draining events.
///
/// On error, returns `(kind, message)` shaped for direct use in
/// [`publish_error`]: `kind="io"` for cache-dir or path I/O failures
/// raised by the synth backend, `kind="render"` for missing-binary,
/// non-zero-exit, or task-panic failures.
async fn render_on_demand(state: &DaemonState, text: &str) -> Result<PathBuf, (String, String)> {
    let target = state.cache.entry_path(text);
    let voice_dir = state.cache.voice_dir();
    if let Err(e) = std::fs::create_dir_all(&voice_dir) {
        return Err((
            "io".to_string(),
            format!("cache dir {}: {e}", voice_dir.display()),
        ));
    }
    let synth = Arc::clone(&state.synth);
    let voice = state.voice.clone();
    let text_owned = text.to_string();
    let target_inner = target.clone();
    let res =
        tokio::task::spawn_blocking(move || synth.render(&voice, &text_owned, &target_inner)).await;
    match res {
        Ok(Ok(())) => Ok(target),
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

/// Spawn a playback subprocess for the WAV at `wav`. `pw-cat` needs the
/// `--playback` flag; `paplay` takes the file as a positional arg. Both
/// shapes are detected from the binary name suffix.
fn spawn_player(player_bin: &str, wav: &Path) -> Result<Child> {
    let mut cmd = Command::new(player_bin);
    if player_bin == "pw-cat" || player_bin.ends_with("/pw-cat") {
        cmd.arg("--playback").arg(wav);
    } else {
        cmd.arg(wav);
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    cmd.spawn()
        .with_context(|| format!("spawn {player_bin} {}", wav.display()))
}

/// Handle a `wm.tts.speak` request. Publishes `start` + `end` on cache
/// hit; publishes `error{kind=render}` on cache miss (iter-5 limitation).
/// Cancel interrupts via [`DaemonState::cancel_signal`].
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

    let wav = match cache_hit_path(&state.cache, &req.text) {
        Some(p) => p,
        None => match render_on_demand(state, &req.text).await {
            Ok(p) => {
                info!(
                    phrase = %phrase_key(&req.text),
                    path = %p.display(),
                    "wm-tts: rendered cache miss on demand"
                );
                p
            }
            Err((kind, message)) => {
                publish_error(publish, &kind, &message).await?;
                return Ok(());
            }
        },
    };

    let played_start = Instant::now();
    let mut child = match spawn_player(&state.player_bin, &wav) {
        Ok(c) => c,
        Err(e) => {
            publish_error(publish, "io", &format!("spawn player: {e}")).await?;
            return Ok(());
        }
    };

    // Install a one-shot cancel channel for this utterance.
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    {
        let mut guard = state.cancel_signal.lock().await;
        *guard = Some(cancel_tx);
    }

    // Race playback completion against cancel.
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

    // Clear cancel slot if still ours.
    {
        let mut guard = state.cancel_signal.lock().await;
        *guard = None;
    }

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

/// Handle a `wm.tts.cancel` request. Fires the cancel channel of the
/// active utterance (if any) and publishes `wm.tts.cancel.ack`.
/// `drained_ms` is always `0` in iter-5 — real measurement requires
/// iter-6 `PipeWire` streaming.
async fn handle_cancel(state: &DaemonState, publish: &mut dyn EventSink) -> Result<()> {
    let taken = {
        let mut guard = state.cancel_signal.lock().await;
        guard.take()
    };
    if let Some(tx) = taken {
        let _ = tx.send(());
    }
    let ack = CancelAckEvent {
        ts: now_unix_ms(),
        drained_ms: 0,
    };
    publish
        .publish(outgoing::CANCEL_ACK, serde_json::to_value(&ack)?)
        .await?;
    Ok(())
}

/// Handle a `wm.tts.reload_voice` request. iter-5 logs and publishes
/// `error{kind=voice}` — real hot-swap (re-prerender + atomic swap)
/// lands in iter-6.
async fn handle_reload_voice(publish: &mut dyn EventSink, new_voice: &str) -> Result<()> {
    warn!(voice = %new_voice, "wm-tts reload-voice: deferred to iter-6");
    publish_error(
        publish,
        "voice",
        &format!("reload_voice deferred to iter-6 (requested={new_voice})"),
    )
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
        Request::ReloadVoice(rv) => handle_reload_voice(publish, &rv.voice).await,
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
    let state = DaemonState::new(&cfg);

    // Pre-render (idempotent). Failures are non-fatal — cache misses
    // are rendered on demand at request time via `render_on_demand`.
    match state.cache.prerender(&cache_phrases.phrases, state.synth.as_ref()) {
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

    // Connect to agorabus. Fail-open: if the bus isn't running, log and
    // exit so the systemd unit restarts us when it comes back.
    let sock = agorabus::default_socket_path();
    let Some(mut sub_client) = agorabus::Client::try_connect(&sock).await? else {
        warn!(socket = %sock.display(), "wm-tts: agorabus not reachable; exiting");
        return Ok(());
    };
    sub_client.subscribe(bus::TOPIC_PREFIX).await?;
    info!(prefix = bus::TOPIC_PREFIX, "wm-tts: subscribed");

    // Separate connection for publishing — read/write on a subscribed
    // socket would interleave Reply lines with the broadcast stream.
    let pub_client = agorabus::Client::connect(&sock).await?;
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
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = TtsConfig {
            voice: "test-voice".into(),
            cache_root: dir.path().to_path_buf(),
            cloud_quality: false,
        };
        let state = DaemonState::new(&cfg);
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
    async fn reload_voice_publishes_error() {
        let mut sink = MemSink::default();
        handle_reload_voice(&mut sink, "en_GB-jenny")
            .await
            .expect("reload publishes");
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, outgoing::ERROR);
        let parsed: ErrorEvent =
            serde_json::from_value(events[0].1.clone()).expect("error decodes");
        assert_eq!(parsed.kind, "voice");
        assert!(parsed.message.contains("en_GB-jenny"));
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
        assert_eq!(events[1].0, outgoing::ERROR);
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

    #[tokio::test]
    async fn cache_hit_lookup_finds_existing_wav() {
        // We can't spawn a real `pw-cat` in CI, but cache_hit_path itself
        // is testable: write a fake WAV under the CacheManager layout
        // and confirm the (Trim + lowercase)-normalised lookup finds it.
        let (state, _g) = tmp_state();
        let wav = state.cache.entry_path("hello world");
        std::fs::create_dir_all(wav.parent().unwrap()).unwrap();
        std::fs::write(&wav, b"RIFF\0\0\0\0WAVEfake").unwrap();
        assert_eq!(
            cache_hit_path(&state.cache, "  Hello World "),
            Some(wav.clone())
        );
        assert_eq!(cache_hit_path(&state.cache, "nope"), None);
    }
}
