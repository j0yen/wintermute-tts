//! Voice-pack resolver — maps a voice-pack identifier to a backend.
//!
//! `iter-15` introduces the in-repo `voicepack` module per the
//! intent-card resolution of PRD §7: the resolver lives next to
//! `wm-tts` until peon-ping PRD-003 needs it, at which point it
//! extracts to a shared `wm-voicepack` crate without an API rewrite.
//!
//! The contract is intentionally narrow: callers pass a string name
//! (e.g. `"en_US-lessac-medium"` or `"cloud:rachel"`) and receive a
//! [`Backend`] enum describing which engine to dispatch to. The
//! resolver does NOT load models or open network connections — that
//! stays in the dispatch path so the resolver is cheap and testable.
//!
//! Naming convention:
//! - `piper:<voice>` or bare `<lang>_<region>-<voice>-<quality>` →
//!   [`Backend::Piper`] (bare names use the Piper repository layout).
//! - `cloud:<id>` → [`Backend::ElevenLabs`].
//! - `espeak:<args>` → [`Backend::EspeakNg`] (offline fallback).

use std::path::PathBuf;

/// Which TTS backend a voice-pack maps to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Backend {
    /// Piper neural TTS — runs locally via the `piper` binary or
    /// `piper-rs`. `model_path` points at the ONNX model file under
    /// the voice cache root; the resolver does not check existence.
    Piper {
        /// Relative model path under the voice cache root.
        model_path: PathBuf,
    },
    /// `ElevenLabs` cloud TTS — `voice_id` is the `ElevenLabs` voice
    /// identifier (UUID-like string). Streaming is the daemon's
    /// responsibility; the resolver only carries the id.
    ElevenLabs {
        /// `ElevenLabs` voice identifier.
        voice_id: String,
    },
    /// espeak-ng offline fallback — `args` are CLI flags passed
    /// verbatim (e.g. `["-v", "en-us"]`).
    EspeakNg {
        /// CLI argument list for `espeak-ng`.
        args: Vec<String>,
    },
}

/// Errors returned by the voice-pack resolver.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VoicePackError {
    /// The name is empty or contains only whitespace.
    #[error("voice-pack name is empty")]
    Empty,
    /// The `cloud:<id>` prefix was used with no id after the colon.
    #[error("cloud voice id is empty after `cloud:` prefix")]
    EmptyCloudId,
    /// The `espeak:<args>` prefix was used with no args after the colon.
    #[error("espeak args list is empty after `espeak:` prefix")]
    EmptyEspeakArgs,
}

/// Resolve a voice-pack identifier into a [`Backend`].
///
/// # Errors
/// Returns [`VoicePackError::Empty`] when `name` is empty or
/// whitespace, and [`VoicePackError::EmptyCloudId`] /
/// [`VoicePackError::EmptyEspeakArgs`] when a prefix-form name has no
/// body.
pub fn resolve(name: &str) -> Result<Backend, VoicePackError> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(VoicePackError::Empty);
    }
    if let Some(id) = trimmed.strip_prefix("cloud:") {
        let id = id.trim();
        if id.is_empty() {
            return Err(VoicePackError::EmptyCloudId);
        }
        return Ok(Backend::ElevenLabs {
            voice_id: id.to_string(),
        });
    }
    if let Some(rest) = trimmed.strip_prefix("espeak:") {
        let args: Vec<String> = rest
            .split_whitespace()
            .map(str::to_string)
            .collect();
        if args.is_empty() {
            return Err(VoicePackError::EmptyEspeakArgs);
        }
        return Ok(Backend::EspeakNg { args });
    }
    let bare = trimmed.strip_prefix("piper:").unwrap_or(trimmed);
    Ok(Backend::Piper {
        model_path: PathBuf::from(format!("{bare}.onnx")),
    })
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
    fn bare_name_resolves_to_piper() {
        let backend = resolve("en_US-lessac-medium").expect("bare name resolves");
        assert_eq!(
            backend,
            Backend::Piper {
                model_path: PathBuf::from("en_US-lessac-medium.onnx"),
            }
        );
    }

    #[test]
    fn piper_prefix_is_explicit() {
        let backend = resolve("piper:en_GB-jenny-low").expect("piper prefix resolves");
        assert_eq!(
            backend,
            Backend::Piper {
                model_path: PathBuf::from("en_GB-jenny-low.onnx"),
            }
        );
    }

    #[test]
    fn cloud_prefix_resolves_to_eleven_labs() {
        let backend = resolve("cloud:21m00Tcm4TlvDq8ikWAM").expect("cloud resolves");
        assert_eq!(
            backend,
            Backend::ElevenLabs {
                voice_id: "21m00Tcm4TlvDq8ikWAM".to_string(),
            }
        );
    }

    #[test]
    fn espeak_prefix_resolves_with_args() {
        let backend = resolve("espeak:-v en-us -s 175").expect("espeak resolves");
        assert_eq!(
            backend,
            Backend::EspeakNg {
                args: vec![
                    "-v".to_string(),
                    "en-us".to_string(),
                    "-s".to_string(),
                    "175".to_string(),
                ]
            }
        );
    }

    #[test]
    fn empty_name_is_error() {
        assert_eq!(resolve(""), Err(VoicePackError::Empty));
        assert_eq!(resolve("   "), Err(VoicePackError::Empty));
    }

    #[test]
    fn empty_cloud_id_is_error() {
        assert_eq!(resolve("cloud:"), Err(VoicePackError::EmptyCloudId));
        assert_eq!(resolve("cloud:   "), Err(VoicePackError::EmptyCloudId));
    }

    #[test]
    fn empty_espeak_args_is_error() {
        assert_eq!(resolve("espeak:"), Err(VoicePackError::EmptyEspeakArgs));
        assert_eq!(resolve("espeak:   "), Err(VoicePackError::EmptyEspeakArgs));
    }

    #[test]
    fn whitespace_around_name_is_trimmed() {
        let backend = resolve("  en_US-amy-medium  ").expect("trimmed bare name resolves");
        assert_eq!(
            backend,
            Backend::Piper {
                model_path: PathBuf::from("en_US-amy-medium.onnx"),
            }
        );
    }
}
