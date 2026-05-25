//! `wm-tts` CLI entrypoint.
//!
//! iter-2 ships the clap dispatcher and a `start` action that loads
//! the cache config. The other subcommands return a typed "not yet
//! implemented" exit (2) so wiring tests can already pin the surface.

#![cfg_attr(not(test), forbid(unsafe_code))]

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
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
                info!(
                    voice = %cfg.voice,
                    phrases = cache.phrases.len(),
                    cache_path = %cli.cache_config.display(),
                    "wm-tts: config loaded; piper/pipewire wiring deferred to iter-3"
                );
                ExitCode::SUCCESS
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
