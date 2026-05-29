//! Synthesis backends — adapt different TTS engines to a single trait.
//!
//! The default backend is the upstream `piper` CLI, invoked as a
//! subprocess: `piper --model <voice>.onnx --output_file <out.wav>`
//! with the text on stdin. `PipeWire` playback of the rendered file
//! lives in [`crate::daemon`].

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Errors raised by a [`Synth`] implementation.
#[derive(Debug, thiserror::Error)]
pub enum SynthError {
    /// The configured backend binary was not found on `$PATH`.
    #[error("backend binary not found: {0}")]
    BackendMissing(String),
    /// The backend ran but returned a non-zero exit code.
    #[error("backend exited with status {status}: {stderr}")]
    BackendFailed {
        /// Decoded exit status (or `-1` if signal-killed).
        status: i32,
        /// First 256 bytes of stderr from the backend.
        stderr: String,
    },
    /// I/O failure spawning the backend or reading/writing the WAV.
    #[error("i/o failure on {path}: {source}")]
    Io {
        /// File or directory path that produced the error.
        path: PathBuf,
        /// Underlying I/O cause.
        #[source]
        source: std::io::Error,
    },
}

/// Render `text` for `voice` into a WAV at `out_path`.
///
/// Implementations are synchronous and blocking. Async streaming will
/// arrive in `iter-4` as a separate trait — for the pre-cache pass we
/// only need "produce a WAV file and exit."
pub trait Synth {
    /// Render `text` for `voice` into a `.wav` at `out_path`.
    ///
    /// # Errors
    /// Propagates any backend-specific failure as [`SynthError`].
    fn render(&self, voice: &str, text: &str, out_path: &Path) -> Result<(), SynthError>;
}

/// Run the upstream `piper` CLI as a subprocess.
///
/// Wire layout (per the upstream tool):
///
/// ```text
/// piper --model <voice>.onnx --output_file <out.wav> < <text on stdin>
/// ```
///
/// The voice argument is mapped to `<models_root>/<voice>.onnx`. If the
/// binary or model is missing, [`PiperSubprocess::render`] returns
/// [`SynthError::BackendMissing`] / [`SynthError::Io`] — callers may
/// fall back to a cached WAV or `wm.tts.error` on agorabus.
#[derive(Debug, Clone)]
pub struct PiperSubprocess {
    /// Path to the `piper` binary. Defaults to `"piper"` (resolved via `$PATH`).
    pub bin: PathBuf,
    /// Root directory containing `<voice>.onnx` files.
    pub models_root: PathBuf,
}

impl PiperSubprocess {
    /// Construct with custom paths to the binary and the models directory.
    #[must_use]
    pub fn new(bin: impl Into<PathBuf>, models_root: impl Into<PathBuf>) -> Self {
        Self {
            bin: bin.into(),
            models_root: models_root.into(),
        }
    }

    /// Default-locate `piper` on `$PATH` with models at
    /// `~/.local/share/wintermute/tts/models/`.
    #[must_use]
    pub fn from_env() -> Self {
        let models_root = std::env::var_os("HOME").map_or_else(
            || PathBuf::from(".local/share/wintermute/tts/models"),
            |home| {
                let mut p = PathBuf::from(home);
                p.push(".local/share/wintermute/tts/models");
                p
            },
        );
        Self::new("piper", models_root)
    }
}

impl Synth for PiperSubprocess {
    fn render(&self, voice: &str, text: &str, out_path: &Path) -> Result<(), SynthError> {
        let mut model_path = self.models_root.clone();
        model_path.push(format!("{voice}.onnx"));

        let mut child = Command::new(&self.bin)
            .arg("--model")
            .arg(&model_path)
            .arg("--output_file")
            .arg(out_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|source| {
                if source.kind() == std::io::ErrorKind::NotFound {
                    SynthError::BackendMissing(self.bin.display().to_string())
                } else {
                    SynthError::Io {
                        path: self.bin.clone(),
                        source,
                    }
                }
            })?;

        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(text.as_bytes())
                .map_err(|source| SynthError::Io {
                    path: self.bin.clone(),
                    source,
                })?;
        }

        let output = child.wait_with_output().map_err(|source| SynthError::Io {
            path: self.bin.clone(),
            source,
        })?;

        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let truncated = if stderr.len() > 256 {
                stderr.chars().take(256).collect()
            } else {
                stderr.into_owned()
            };
            Err(SynthError::BackendFailed {
                status: output.status.code().unwrap_or(-1),
                stderr: truncated,
            })
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc
)]
mod tests {
    use super::*;

    #[test]
    fn from_env_defaults_to_piper_on_path() {
        let s = PiperSubprocess::from_env();
        assert_eq!(s.bin, PathBuf::from("piper"));
        assert!(s.models_root.ends_with(".local/share/wintermute/tts/models"));
    }

    #[test]
    fn missing_binary_yields_backend_missing() {
        let s = PiperSubprocess::new(
            "/definitely/not/a/real/piper-binary-xyz",
            "/tmp/no-models",
        );
        let tmp = tempfile::NamedTempFile::new().expect("temp wav");
        let err = s
            .render("en_US-lessac-medium", "hello", tmp.path())
            .expect_err("missing binary must error");
        assert!(matches!(err, SynthError::BackendMissing(_)));
    }
}
