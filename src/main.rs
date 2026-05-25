//! `wm-tts` CLI entrypoint.
//!
//! iter-3 wires the pre-render pass: on `start`, load the cache YAML,
//! construct a `PiperSubprocess` synth, and walk the phrase list,
//! materializing missing WAVs into the per-voice cache directory.
//! `speak`/`cancel`/`reload-voice` still return exit code 2 — the
//! streaming + agorabus loop lands in iter-4.

#![cfg_attr(not(test), forbid(unsafe_code))]

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use wintermute_tts::cache::CacheManager;
use wintermute_tts::synth::PiperSubprocess;
use wintermute_tts::{DEFAULT_CACHE_CONFIG, TtsConfig, load_cache_yaml};

#[derive(Parser, Debug)]
#[command(
    name = "wm-tts",
    version,
    about = "wintermute text-to-speech daemon and CLI"
)]
struct Cli {
    /// Path to the cache-phrases YAML file.
    #[arg(long, default_value = DEFAULT_CACHE_CONFIG)]
    cache_config: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start the daemon (long-running). iter-3 wires Piper + `PipeWire`.
    Start,
    /// Speak an utterance synchronously.
    Speak {
        /// Text to render and play.
        text: String,
    },
    /// Cancel the current utterance (no-op when none active).
    Cancel,
    /// Hot-swap to a different voice.
    ReloadVoice {
        /// Voice identifier (e.g. `en_US-lessac-medium`).
        voice: String,
    },
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    drop(tracing_subscriber::fmt().with_env_filter(filter).try_init());
}

fn main() -> ExitCode {
    init_tracing();
    let cli = Cli::parse();

    match cli.command {
        Command::Start => match load_cache_yaml(&cli.cache_config) {
            Ok(cache) => {
                let cfg = TtsConfig::default();
                let mgr = CacheManager::new(&cfg.cache_root, &cfg.voice);
                let synth = PiperSubprocess::from_env();
                match mgr.prerender(&cache.phrases, &synth) {
                    Ok(report) => {
                        info!(
                            voice = %cfg.voice,
                            phrases = cache.phrases.len(),
                            hits = report.hits,
                            rendered = report.rendered,
                            failures = report.failures.len(),
                            cache_root = %mgr.voice_dir().display(),
                            "wm-tts: pre-render complete; pipewire+agorabus wiring deferred to iter-4"
                        );
                        for (phrase, why) in &report.failures {
                            warn!(phrase = %phrase, error = %why, "wm-tts: phrase render failed");
                        }
                        ExitCode::SUCCESS
                    }
                    Err(err) => {
                        error!(error = %err, "wm-tts: cache pre-render aborted");
                        ExitCode::from(1)
                    }
                }
            }
            Err(err) => {
                error!(error = %err, "wm-tts: failed to load cache config");
                ExitCode::from(1)
            }
        },
        Command::Speak { text } => {
            warn!(text = %text, "wm-tts speak: not yet implemented (iter-3)");
            ExitCode::from(2)
        }
        Command::Cancel => {
            warn!("wm-tts cancel: not yet implemented (iter-3)");
            ExitCode::from(2)
        }
        Command::ReloadVoice { voice } => {
            warn!(voice = %voice, "wm-tts reload-voice: not yet implemented (iter-3)");
            ExitCode::from(2)
        }
    }
}
