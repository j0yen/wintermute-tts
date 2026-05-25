//! Pre-rendered WAV cache for short phrases.
//!
//! At startup, `wm-tts` walks the configured phrase list, looks up the
//! voice-keyed cache directory, and renders any phrase that does not
//! already have a `.wav` on disk. Subsequent `wm.tts.speak` requests
//! whose text exactly matches a cached phrase resolve in <50 ms (PRD
//! AC3) — just a `PipeWire` enqueue of the file.

use std::collections::BTreeMap;
use std::path::PathBuf;

use sha2::{Digest, Sha256};

use crate::synth::{Synth, SynthError};

/// Cache layout: `<root>/<voice>/<sha256(text).truncated(16)>.wav`.
///
/// The hash is computed from the **lower-cased, trimmed** text so that
/// `"Yes"` and `"yes "` resolve to the same cache entry. Filenames are
/// the lower 16 hex chars (64 bits) of the SHA-256, which is collision-
/// safe for the ~50-entry phrase lists the PRD anticipates.
#[derive(Debug, Clone)]
pub struct CacheManager {
    /// Filesystem root for the WAV cache.
    pub root: PathBuf,
    /// Currently-loaded voice (cache files are per-voice).
    pub voice: String,
}

/// Failure modes for cache operations.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// I/O failure creating the cache directory or writing a WAV.
    #[error("io failure on {path}: {source}")]
    Io {
        /// Path that produced the error.
        path: PathBuf,
        /// Underlying I/O cause.
        #[source]
        source: std::io::Error,
    },
    /// The configured synth backend failed.
    #[error("synth backend failure: {0}")]
    Synth(#[from] SynthError),
}

/// Outcome of a single pre-render pass.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RenderReport {
    /// Phrases that were already on disk (hit).
    pub hits: usize,
    /// Phrases newly rendered this pass.
    pub rendered: usize,
    /// Phrases that failed to render, with the per-phrase error message.
    pub failures: BTreeMap<String, String>,
}

impl CacheManager {
    /// Construct a manager for `voice` rooted at `root`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>, voice: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            voice: voice.into(),
        }
    }

    /// Directory holding cache entries for the active voice.
    #[must_use]
    pub fn voice_dir(&self) -> PathBuf {
        self.root.join(&self.voice)
    }

    /// Path the manager will use for a given phrase.
    #[must_use]
    pub fn entry_path(&self, phrase: &str) -> PathBuf {
        let normalized = phrase.trim().to_lowercase();
        let mut hasher = Sha256::new();
        hasher.update(normalized.as_bytes());
        let digest = hasher.finalize();
        let mut name = String::with_capacity(20);
        for byte in digest.iter().take(8) {
            name.push_str(&format!("{byte:02x}"));
        }
        name.push_str(".wav");
        self.voice_dir().join(name)
    }

    /// True if the manager already has a WAV on disk for `phrase`.
    #[must_use]
    pub fn has(&self, phrase: &str) -> bool {
        self.entry_path(phrase).is_file()
    }

    /// Walk `phrases` and render any missing entries via `synth`.
    ///
    /// Idempotent: pre-existing entries are counted as hits and skipped.
    /// Per-phrase failures are recorded in the report rather than
    /// aborting the whole pass — a single voice missing for one of fifty
    /// phrases must not block startup.
    ///
    /// # Errors
    /// Only surfaces I/O failures that prevent the cache directory from
    /// being created at all. Per-phrase render failures are captured in
    /// `RenderReport::failures` and do NOT short-circuit.
    pub fn prerender<S: Synth>(
        &self,
        phrases: &[String],
        synth: &S,
    ) -> Result<RenderReport, CacheError> {
        let dir = self.voice_dir();
        std::fs::create_dir_all(&dir).map_err(|source| CacheError::Io {
            path: dir.clone(),
            source,
        })?;

        let mut report = RenderReport::default();
        for phrase in phrases {
            let path = self.entry_path(phrase);
            if path.is_file() {
                report.hits += 1;
                continue;
            }
            match synth.render(&self.voice, phrase, &path) {
                Ok(()) => report.rendered += 1,
                Err(err) => {
                    report.failures.insert(phrase.clone(), err.to_string());
                }
            }
        }
        Ok(report)
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
    use std::cell::RefCell;
    use std::path::Path;

    use super::*;
    use crate::synth::{Synth, SynthError};

    struct FakeSynth {
        body: Vec<u8>,
        calls: RefCell<Vec<(String, String)>>,
        fail_for: Option<String>,
    }

    impl FakeSynth {
        fn new() -> Self {
            Self {
                body: b"RIFF....WAVEfmt ".to_vec(),
                calls: RefCell::new(Vec::new()),
                fail_for: None,
            }
        }

        fn fail_for(text: &str) -> Self {
            Self {
                body: b"RIFF....WAVEfmt ".to_vec(),
                calls: RefCell::new(Vec::new()),
                fail_for: Some(text.to_string()),
            }
        }
    }

    impl Synth for FakeSynth {
        fn render(&self, voice: &str, text: &str, out_path: &Path) -> Result<(), SynthError> {
            self.calls
                .borrow_mut()
                .push((voice.to_string(), text.to_string()));
            if self.fail_for.as_deref() == Some(text) {
                return Err(SynthError::BackendFailed {
                    status: 1,
                    stderr: format!("boom on {text}"),
                });
            }
            std::fs::write(out_path, &self.body).map_err(|source| SynthError::Io {
                path: out_path.to_path_buf(),
                source,
            })?;
            Ok(())
        }
    }

    #[test]
    fn entry_path_is_normalized_and_stable() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mgr = CacheManager::new(tmp.path(), "en_US-lessac-medium");
        let a = mgr.entry_path("Yes");
        let b = mgr.entry_path("  yes  ");
        assert_eq!(a, b, "case- and whitespace-equivalent phrases share a path");
        assert!(a.starts_with(tmp.path().join("en_US-lessac-medium")));
        assert!(a.extension().is_some_and(|e| e == "wav"));
    }

    #[test]
    fn prerender_creates_missing_files_and_reports_hits() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mgr = CacheManager::new(tmp.path(), "en_US-lessac-medium");
        let synth = FakeSynth::new();
        let phrases = vec!["yes".to_string(), "no".to_string(), "one moment".to_string()];

        let first = mgr.prerender(&phrases, &synth).expect("first render ok");
        assert_eq!(first.hits, 0);
        assert_eq!(first.rendered, 3);
        assert!(first.failures.is_empty());
        for p in &phrases {
            assert!(mgr.has(p), "{p} should be cached after first render");
        }

        let second = mgr.prerender(&phrases, &synth).expect("second render ok");
        assert_eq!(second.hits, 3, "second pass is all hits");
        assert_eq!(second.rendered, 0);
        // Synth was only called 3 times in the FIRST pass; second pass skipped them all.
        assert_eq!(synth.calls.borrow().len(), 3);
    }

    #[test]
    fn prerender_records_per_phrase_failures_without_aborting() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mgr = CacheManager::new(tmp.path(), "en_US-lessac-medium");
        let synth = FakeSynth::fail_for("one moment");
        let phrases = vec!["yes".to_string(), "one moment".to_string(), "no".to_string()];

        let report = mgr.prerender(&phrases, &synth).expect("io ok");
        assert_eq!(report.rendered, 2);
        assert_eq!(report.hits, 0);
        assert_eq!(report.failures.len(), 1);
        assert!(report.failures.contains_key("one moment"));
        assert!(mgr.has("yes"));
        assert!(mgr.has("no"));
        assert!(!mgr.has("one moment"));
    }
}
