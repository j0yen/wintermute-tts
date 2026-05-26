//! `wm-tts` CLI entrypoint.
//!
//! iter-5 wires `start` to the live daemon loop in [`wintermute_tts::daemon`]:
//! load cache config, prerender missing WAVs, connect to agorabus,
//! subscribe to `wm.tts.`, dispatch each event. `speak`/`cancel`/
//! `reload-voice` remain stubs at exit code 2 — interactive single-shot
//! CLI use lands in iter-6 (today the daemon is the only producer).

#![cfg_attr(not(test), forbid(unsafe_code))]

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tracing::{error, warn};
use tracing_subscriber::EnvFilter;
use wintermute_tts::DEFAULT_CACHE_CONFIG;

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
        Command::Start => match build_runtime() {
            Ok(rt) => match rt.block_on(wintermute_tts::daemon::run(&cli.cache_config)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(err) => {
                    error!(error = %err, "wm-tts: daemon exited with error");
                    ExitCode::from(1)
                }
            },
            Err(err) => {
                error!(error = %err, "wm-tts: failed to build tokio runtime");
                ExitCode::from(1)
            }
        },
        Command::Speak { text } => {
            warn!(text = %text, "wm-tts speak: deferred to iter-6 (use agorabus wm.tts.speak)");
            ExitCode::from(2)
        }
        Command::Cancel => {
            warn!("wm-tts cancel: deferred to iter-6 (use agorabus wm.tts.cancel)");
            ExitCode::from(2)
        }
        Command::ReloadVoice { voice } => {
            warn!(voice = %voice, "wm-tts reload-voice: deferred to iter-6");
            ExitCode::from(2)
        }
    }
}

fn build_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
}
