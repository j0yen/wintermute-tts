//! wintermute-tts — text-to-speech for the wintermute fleet.
//!
//! iter-4 surface: config + cache YAML parser (iter-2), `Synth`
//! abstraction with a subprocess `piper` backend + per-voice WAV cache
//! manager (iter-3), and the agorabus topic + payload schema
//! ([`bus`]) for the `wm.tts.*` request/event vocabulary. The live
//! agorabus subscribe loop, `PipeWire` streaming, and mid-utterance
//! cancellation arrive in iter-5.

#![cfg_attr(not(test), forbid(unsafe_code))]

pub mod bus;
pub mod cache;
pub mod synth;

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Default Piper voice when `WM_TTS_VOICE` is unset.
pub const DEFAULT_VOICE: &str = "en_US-lessac-medium";

/// Default location of the cache-phrases YAML.
pub const DEFAULT_CACHE_CONFIG: &str = "/etc/wintermute/tts-cache.yaml";

/// Runtime configuration for `wm-tts`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TtsConfig {
    /// Piper voice identifier (e.g. `en_US-lessac-medium`).
    #[serde(default = "default_voice")]
    pub voice: String,
    /// Filesystem root for the pre-rendered WAV cache.
    #[serde(default = "default_cache_root")]
    pub cache_root: PathBuf,
    /// Whether the `ElevenLabs` cloud path is enabled.
    #[serde(default)]
    pub cloud_quality: bool,
}

impl Default for TtsConfig {
    fn default() -> Self {
        Self {
            voice: default_voice(),
            cache_root: default_cache_root(),
            cloud_quality: false,
        }
    }
}

fn default_voice() -> String {
    DEFAULT_VOICE.to_string()
}

fn default_cache_root() -> PathBuf {
    PathBuf::from(".cache/wintermute/tts")
}

/// Phrase list for the pre-rendered WAV cache.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CachePhrases {
    /// Phrases to render on startup. Lower-cased exact match at lookup.
    #[serde(default)]
    pub phrases: Vec<String>,
}

/// Errors raised by config parsing and loading.
#[derive(Debug, thiserror::Error)]
pub enum TtsError {
    /// YAML parse error.
    #[error("yaml parse failure: {0}")]
    Yaml(#[from] serde_yaml::Error),
    /// I/O error reading config from disk.
    #[error("io failure on {path}: {source}")]
    Io {
        /// Path that produced the error.
        path: PathBuf,
        /// Underlying I/O cause.
        #[source]
        source: std::io::Error,
    },
    /// Empty phrase list.
    #[error("cache phrase list is empty")]
    EmptyPhrases,
    /// Duplicate phrase entry.
    #[error("duplicate phrase: {0:?}")]
    DuplicatePhrase(String),
}

/// Parse a cache-phrases YAML blob into a validated `CachePhrases`.
///
/// # Errors
/// Returns `TtsError::Yaml` on parse failure, `TtsError::EmptyPhrases`
/// when the list is empty, and `TtsError::DuplicatePhrase` when the
/// list contains a duplicate entry. Duplicates are rejected because
/// the cache filename is a hash of the phrase — duplicates would race
/// on the same output file at render time.
pub fn parse_cache_yaml(blob: &str) -> Result<CachePhrases, TtsError> {
    let parsed: CachePhrases = serde_yaml::from_str(blob)?;
    if parsed.phrases.is_empty() {
        return Err(TtsError::EmptyPhrases);
    }
    let mut seen: HashSet<&str> = HashSet::new();
    for p in &parsed.phrases {
        if !seen.insert(p.as_str()) {
            return Err(TtsError::DuplicatePhrase(p.clone()));
        }
    }
    Ok(parsed)
}

/// Load and validate a cache-phrases YAML file from disk.
///
/// # Errors
/// Wraps I/O failures (including missing file) and parse failures into
/// `TtsError`.
pub fn load_cache_yaml(path: &Path) -> Result<CachePhrases, TtsError> {
    let blob = std::fs::read_to_string(path).map_err(|source| TtsError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    parse_cache_yaml(&blob)
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
    fn parse_minimal_cache() {
        let yaml = "phrases:\n  - yes\n  - no\n";
        let cache = parse_cache_yaml(yaml).expect("valid yaml parses");
        assert_eq!(cache.phrases, vec!["yes".to_string(), "no".to_string()]);
    }

    #[test]
    fn parse_empty_phrases_is_error() {
        let yaml = "phrases: []\n";
        assert!(matches!(
            parse_cache_yaml(yaml),
            Err(TtsError::EmptyPhrases)
        ));
    }

    #[test]
    fn parse_duplicate_phrase_is_error() {
        let yaml = "phrases:\n  - yes\n  - yes\n";
        let result = parse_cache_yaml(yaml);
        assert!(matches!(result, Err(TtsError::DuplicatePhrase(p)) if p == "yes"));
    }

    #[test]
    fn default_voice_matches_constant() {
        let cfg = TtsConfig::default();
        assert_eq!(cfg.voice, DEFAULT_VOICE);
    }

    #[test]
    fn voice_override_via_yaml() {
        let yaml = "voice: en_GB-jenny\n";
        let cfg: TtsConfig = serde_yaml::from_str(yaml).expect("parses");
        assert_eq!(cfg.voice, "en_GB-jenny");
    }

    #[test]
    fn load_missing_file_is_io_error() {
        let path = PathBuf::from("/nonexistent/wm-tts-cache.yaml");
        let result = load_cache_yaml(&path);
        assert!(matches!(result, Err(TtsError::Io { .. })));
    }
}
