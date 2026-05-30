//! Synthesis backends — adapt different TTS engines to a single trait.
//!
//! The default backend is the upstream `piper` CLI, invoked as a
//! subprocess: `piper --model <voice>.onnx --output_file <out.wav>`
//! with the text on stdin. `PipeWire` playback of the rendered file
//! lives in [`crate::daemon`].
//!
//! ## Speaking rate and gain
//!
//! [`PiperSubprocess`] accepts a [`VoiceConfig`] that controls two
//! accessibility knobs:
//!
//! - **`speaking_rate`** — a multiplier relative to the human-normal
//!   baseline (1.0). Values < 1.0 are slower, > 1.0 are faster. This
//!   maps to piper's `--length_scale` argument:
//!   `length_scale = 1.0 / speaking_rate` (higher `length_scale` =
//!   slower in piper's convention). A `speaking_rate` of 0.8 therefore
//!   yields `--length_scale 1.25` — 25 % slower than the piper default.
//!   Valid range: `[SPEAKING_RATE_MIN, SPEAKING_RATE_MAX]`; values
//!   outside the range are clamped with a logged warning.
//!
//! - **`gain`** — linear amplitude multiplier applied to the rendered
//!   WAV in-place (16-bit signed PCM, little-endian). Samples are
//!   saturating-clamped to `[i16::MIN, i16::MAX]` so full-scale inputs
//!   never wrap. Valid range: `[GAIN_MIN, GAIN_MAX]`; values outside
//!   are clamped with a logged warning.
//!
//! Elder-friendly defaults (`VoiceConfig::default`): `speaking_rate =
//! 0.85` (slightly slower), `gain = 1.20` (modest +20 % amplitude).
//! These are opt-in by default; restoring neutral behaviour requires
//! `speaking_rate = 1.0`, `gain = 1.0`.
//!
//! ## `ElevenLabs` cloud path
//!
//! The cloud path (`cloud.rs` / `cloud_ws.rs`) does not support a
//! speaking-rate knob: the `ElevenLabs` HTTP/WebSocket API exposes voice
//! style/stability parameters but no tempo control comparable to piper's
//! `--length_scale`. Rate control is therefore **Piper-only** in this
//! version. Gain (WAV sample scaling) applies on both paths because gain
//! is applied post-synthesis to the rendered WAV file, regardless of
//! which backend produced it — see [`apply_gain_to_wav`].

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::{Deserialize, Serialize};

// ── bounds ────────────────────────────────────────────────────────────────────

/// Minimum allowed [`VoiceConfig::speaking_rate`].
///
/// Values below this (extreme slow-down, > 10× piper length-scale) are
/// unlikely to produce intelligible speech and are clamped to this floor.
pub const SPEAKING_RATE_MIN: f32 = 0.1;

/// Maximum allowed [`VoiceConfig::speaking_rate`].
///
/// Values above this (> 5× baseline speed) are clamped; piper may fail
/// or produce garbage audio at extreme rates.
pub const SPEAKING_RATE_MAX: f32 = 5.0;

/// Minimum allowed [`VoiceConfig::gain`].
///
/// Below 0 has no physical meaning for a linear amplitude scale; clamped
/// to 0 (silence).
pub const GAIN_MIN: f32 = 0.0;

/// Maximum allowed [`VoiceConfig::gain`].
///
/// Cap at 4.0× to prevent extreme loudness / distortion. Clamped with a
/// warning; the physical ceiling is sample saturation, but blowing out a
/// speaker is not fun.
pub const GAIN_MAX: f32 = 4.0;

// ── VoiceConfig ───────────────────────────────────────────────────────────────

/// Per-voice output settings controlling speaking rate and output gain.
///
/// See the [module-level documentation](self) for the rate/gain convention,
/// elder-friendly defaults, and cloud-path limitations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VoiceConfig {
    /// Speaking-rate multiplier relative to the human-normal baseline (1.0).
    ///
    /// - `0.85` (default) → 15 % slower than piper's own default.
    /// - `1.0` → neutral (recovers today's behaviour exactly).
    /// - `< 1.0` → slower; `> 1.0` → faster.
    ///
    /// Clamped to `[SPEAKING_RATE_MIN, SPEAKING_RATE_MAX]` on use.
    #[serde(default = "VoiceConfig::default_speaking_rate")]
    pub speaking_rate: f32,

    /// Linear amplitude gain applied to the synthesised WAV.
    ///
    /// - `1.20` (default) → +20 % louder than the raw piper output.
    /// - `1.0` → unity (no change).
    ///
    /// Samples are saturating-clamped to `[i16::MIN, i16::MAX]`.
    /// Clamped to `[GAIN_MIN, GAIN_MAX]` on use.
    #[serde(default = "VoiceConfig::default_gain")]
    pub gain: f32,
}

impl VoiceConfig {
    const fn default_speaking_rate() -> f32 {
        0.85
    }
    const fn default_gain() -> f32 {
        1.20
    }

    /// Return the clamped `speaking_rate`, logging a warning when the raw
    /// value was out of range.
    #[must_use]
    pub fn effective_speaking_rate(&self) -> f32 {
        clamp_warn(
            self.speaking_rate,
            SPEAKING_RATE_MIN,
            SPEAKING_RATE_MAX,
            "speaking_rate",
        )
    }

    /// Return the clamped `gain`, logging a warning when the raw value was
    /// out of range.
    #[must_use]
    pub fn effective_gain(&self) -> f32 {
        clamp_warn(self.gain, GAIN_MIN, GAIN_MAX, "gain")
    }

    /// Convert `speaking_rate` to piper's `--length_scale` argument.
    ///
    /// piper convention: `length_scale = 1.0 / speaking_rate`. A rate of
    /// 0.85 → `length_scale ≈ 1.176` (piper slows down by 17.6 %).
    ///
    /// When the result equals piper's documented default (1.0, i.e.
    /// `speaking_rate == 1.0`), the caller may omit `--length_scale`
    /// entirely — [`PiperSubprocess`] does this to preserve today's
    /// behaviour when the knob is neutral.
    #[must_use]
    #[allow(clippy::float_arithmetic)]
    pub fn piper_length_scale(&self) -> f32 {
        let rate = self.effective_speaking_rate();
        1.0_f32 / rate
    }

    /// True when `speaking_rate` is exactly neutral (1.0) after clamping,
    /// i.e. no `--length_scale` flag should be emitted.
    #[must_use]
    #[allow(clippy::float_arithmetic)]
    pub fn is_rate_neutral(&self) -> bool {
        // Use a small epsilon to handle float representation of 1.0.
        (self.effective_speaking_rate() - 1.0_f32).abs() < f32::EPSILON
    }

    /// True when `gain` is exactly unity after clamping.
    #[must_use]
    #[allow(clippy::float_arithmetic)]
    pub fn is_gain_unity(&self) -> bool {
        (self.effective_gain() - 1.0_f32).abs() < f32::EPSILON
    }
}

impl Default for VoiceConfig {
    fn default() -> Self {
        Self {
            speaking_rate: Self::default_speaking_rate(),
            gain: Self::default_gain(),
        }
    }
}

fn clamp_warn(value: f32, min: f32, max: f32, name: &str) -> f32 {
    if value < min {
        tracing::warn!(
            field = name,
            value,
            clamped_to = min,
            "VoiceConfig: {name} below minimum; clamping"
        );
        min
    } else if value > max {
        tracing::warn!(
            field = name,
            value,
            clamped_to = max,
            "VoiceConfig: {name} above maximum; clamping"
        );
        max
    } else {
        value
    }
}

// ── gain application ──────────────────────────────────────────────────────────

/// Apply a linear amplitude gain in-place to a 16-bit signed PCM WAV file.
///
/// The file at `wav_path` is read, the `data` chunk's samples are scaled
/// by `gain` with saturating clamp to `[i16::MIN, i16::MAX]`, and the
/// result is written back atomically (temp-file + rename in the same
/// directory).
///
/// A `gain` of 1.0 is a no-op (the function still reads + re-writes the
/// file; callers may skip the call by checking [`VoiceConfig::is_gain_unity`]).
///
/// # Assumptions
/// The WAV contains 16-bit little-endian PCM samples (the format piper
/// always emits). Non-PCM / 8-bit / 24-bit / 32-bit files are written
/// back unchanged — the function parses only the `data` chunk offset and
/// operates on raw bytes; if the format byte says it's not PCM-16 the
/// function is still safe but the scaled bytes will be meaningless.
///
/// For correctness the caller should verify the format is PCM-16 before
/// calling, e.g. by inspecting the WAV header. Since piper always emits
/// PCM-16 we don't add a format check here to avoid duplicating the WAV
/// parser.
///
/// # Errors
/// Returns `std::io::Error` on read/write failures.
pub fn apply_gain_to_wav(wav_path: &Path, gain: f32) -> std::io::Result<()> {
    let mut bytes = std::fs::read(wav_path)?;
    apply_gain_to_wav_bytes(&mut bytes, gain);

    // Atomic write: temp file in same dir, then rename.
    let dir = wav_path.parent().unwrap_or_else(|| Path::new("."));
    let mut tmp = tempfile::Builder::new()
        .prefix(".wm-tts-gain-")
        .suffix(".wav")
        .tempfile_in(dir)
        .map_err(|e| std::io::Error::other(format!("temp file: {e}")))?;
    tmp.write_all(&bytes)?;
    tmp.flush()?;
    let tmp_path = tmp.into_temp_path();
    tmp_path
        .persist(wav_path)
        .map_err(|e| std::io::Error::other(format!("persist: {e}")))?;
    Ok(())
}

/// Apply gain to raw WAV bytes in-place (exposed for unit tests that avoid
/// the filesystem round-trip). Modifies the `data` chunk samples; the RIFF
/// header and other chunks are untouched.
///
/// Finds the `data` chunk by scanning from byte 12 (after the RIFF/WAVE
/// header), then scales each pair of bytes as a little-endian `i16`,
/// clamping to `[i16::MIN, i16::MAX]`.
///
/// Silently no-ops when the file is not a valid RIFF/WAVE or has no `data`
/// chunk — the bytes are returned unchanged.
pub fn apply_gain_to_wav_bytes(bytes: &mut [u8], gain: f32) {
    // Must have the 12-byte RIFF/WAVE header.
    if bytes.len() < 12 {
        return;
    }
    if bytes.get(..4) != Some(b"RIFF") || bytes.get(8..12) != Some(b"WAVE") {
        return;
    }

    let mut cursor: usize = 12;
    while bytes.len() >= cursor.saturating_add(8) {
        let Some(id) = bytes.get(cursor..cursor + 4) else {
            break;
        };
        let Some(size_bytes) = bytes.get(cursor + 4..cursor + 8) else {
            break;
        };
        let size_arr: [u8; 4] = match size_bytes.try_into() {
            Ok(a) => a,
            Err(_) => break,
        };
        let size = u32::from_le_bytes(size_arr);
        let body_start = cursor + 8;
        let size_usize = usize::try_from(size).unwrap_or(usize::MAX);

        if id == b"data" {
            // Scale each 16-bit LE sample in the data body.
            let body_end = body_start.saturating_add(size_usize);
            let body_end = body_end.min(bytes.len());
            let mut i = body_start;
            while i + 1 < body_end {
                // SAFETY: loop guard `i + 1 < body_end` and
                // `body_end <= bytes.len()` ensure both indices are in bounds.
                #[allow(clippy::indexing_slicing)]
                let sample = i16::from_le_bytes([bytes[i], bytes[i + 1]]);
                #[allow(clippy::cast_possible_truncation, clippy::float_arithmetic, clippy::as_conversions)]
                let scaled = (f32::from(sample) * gain)
                    .clamp(f32::from(i16::MIN), f32::from(i16::MAX))
                    as i16;
                let out = scaled.to_le_bytes();
                // SAFETY: same bounds as above.
                #[allow(clippy::indexing_slicing)]
                {
                    bytes[i] = out[0];
                    bytes[i + 1] = out[1];
                }
                i += 2;
            }
            return;
        }

        // Advance past this chunk (RIFF chunks pad to even size).
        let pad = usize::from(size % 2 != 0);
        let Some(next) = body_start.checked_add(size_usize).and_then(|n| n.checked_add(pad))
        else {
            break;
        };
        cursor = next;
    }
}

// ── SynthError ────────────────────────────────────────────────────────────────

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

// ── Synth trait ───────────────────────────────────────────────────────────────

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

// ── PiperSubprocess ───────────────────────────────────────────────────────────

/// Run the upstream `piper` CLI as a subprocess.
///
/// Wire layout (per the upstream tool):
///
/// ```text
/// piper --model <voice>.onnx [--length_scale <ls>] --output_file <out.wav> < <text on stdin>
/// ```
///
/// The voice argument is mapped to `<models_root>/<voice>.onnx`. If the
/// binary or model is missing, [`PiperSubprocess::render`] returns
/// [`SynthError::BackendMissing`] / [`SynthError::Io`] — callers may
/// fall back to a cached WAV or `wm.tts.error` on agorabus.
///
/// Speaking rate and output gain are controlled via [`VoiceConfig`]. The
/// `--length_scale` flag is omitted when the rate is neutral (1.0) to
/// preserve piper's default behaviour exactly. Gain is applied as a
/// post-synthesis WAV in-place scaling step; unity gain (1.0) still
/// causes the file to be re-written — callers may skip the gain step by
/// checking [`VoiceConfig::is_gain_unity`] before calling
/// [`apply_gain_to_wav`].
#[derive(Debug, Clone)]
pub struct PiperSubprocess {
    /// Path to the `piper` binary. Defaults to `"piper"` (resolved via `$PATH`).
    pub bin: PathBuf,
    /// Root directory containing `<voice>.onnx` files.
    pub models_root: PathBuf,
    /// Voice output config (speaking rate + gain). Defaults are elder-friendly.
    pub voice_config: VoiceConfig,
}

impl PiperSubprocess {
    /// Construct with custom paths and the given [`VoiceConfig`].
    #[must_use]
    pub fn new(
        bin: impl Into<PathBuf>,
        models_root: impl Into<PathBuf>,
        voice_config: VoiceConfig,
    ) -> Self {
        Self {
            bin: bin.into(),
            models_root: models_root.into(),
            voice_config,
        }
    }

    /// Default-locate `piper` on `$PATH` with models at
    /// `~/.local/share/wintermute/tts/models/` and elder-friendly
    /// [`VoiceConfig::default`].
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
        Self::new("piper", models_root, VoiceConfig::default())
    }

    /// Construct the argv that would be passed to `piper` for a given
    /// `voice` and `out_path`, applying the current [`VoiceConfig`].
    ///
    /// Exposed for unit tests that need to assert the CLI arguments without
    /// spawning a real piper binary.
    #[must_use]
    pub fn build_argv(&self, voice: &str, out_path: &Path) -> Vec<String> {
        let mut model_path = self.models_root.clone();
        model_path.push(format!("{voice}.onnx"));

        let mut args = vec![
            self.bin.display().to_string(),
            "--model".to_string(),
            model_path.display().to_string(),
        ];

        if !self.voice_config.is_rate_neutral() {
            let ls = self.voice_config.piper_length_scale();
            args.push("--length_scale".to_string());
            args.push(format!("{ls:.6}"));
        }

        args.push("--output_file".to_string());
        args.push(out_path.display().to_string());
        args
    }
}

impl Synth for PiperSubprocess {
    fn render(&self, voice: &str, text: &str, out_path: &Path) -> Result<(), SynthError> {
        let mut model_path = self.models_root.clone();
        model_path.push(format!("{voice}.onnx"));

        let mut cmd = Command::new(&self.bin);
        cmd.arg("--model").arg(&model_path);

        // Emit --length_scale only when the rate is non-neutral, so that
        // a neutral speaking_rate (1.0) leaves today's behaviour exactly
        // unchanged (piper uses its own default, not a forced 1.0).
        if !self.voice_config.is_rate_neutral() {
            let ls = self.voice_config.piper_length_scale();
            cmd.arg("--length_scale").arg(format!("{ls:.6}"));
        }

        cmd.arg("--output_file")
            .arg(out_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());

        let mut child = cmd.spawn().map_err(|source| {
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
            // Apply gain to the rendered WAV in-place.
            if !self.voice_config.is_gain_unity() {
                let g = self.voice_config.effective_gain();
                apply_gain_to_wav(out_path, g).map_err(|source| SynthError::Io {
                    path: out_path.to_path_buf(),
                    source,
                })?;
            }
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

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::float_arithmetic,
    clippy::as_conversions
)]
mod tests {
    use super::*;

    // ── VoiceConfig ──────────────────────────────────────────────────────────

    #[test]
    fn default_voice_config_elder_friendly() {
        let vc = VoiceConfig::default();
        assert!(
            vc.speaking_rate < 1.0,
            "default speaking_rate should be slower than baseline"
        );
        assert!(
            vc.gain > 1.0,
            "default gain should be louder than unity"
        );
    }

    #[test]
    fn neutral_rate_skips_length_scale() {
        let vc = VoiceConfig {
            speaking_rate: 1.0,
            gain: 1.0,
        };
        assert!(vc.is_rate_neutral());
        assert!(vc.is_gain_unity());
    }

    #[test]
    fn non_neutral_rate_produces_length_scale() {
        let vc = VoiceConfig {
            speaking_rate: 0.5,
            gain: 1.0,
        };
        assert!(!vc.is_rate_neutral());
        let ls = vc.piper_length_scale();
        assert!(
            (ls - 2.0_f32).abs() < 1e-5,
            "0.5 speaking_rate → length_scale 2.0, got {ls}"
        );
    }

    #[test]
    fn argv_includes_length_scale_for_non_neutral_rate() {
        let ps = PiperSubprocess::new(
            "piper",
            "/models",
            VoiceConfig {
                speaking_rate: 0.8,
                gain: 1.0,
            },
        );
        let argv = ps.build_argv("en_US-lessac-medium", Path::new("/tmp/out.wav"));
        let ls_idx = argv.iter().position(|a| a == "--length_scale");
        assert!(
            ls_idx.is_some(),
            "argv must contain --length_scale for non-neutral rate: {argv:?}"
        );
        let ls_val: f32 = argv[ls_idx.unwrap() + 1].parse().unwrap();
        // speaking_rate=0.8 → length_scale = 1/0.8 = 1.25
        assert!(
            (ls_val - 1.25_f32).abs() < 1e-4,
            "expected ~1.25, got {ls_val}"
        );
    }

    #[test]
    fn argv_omits_length_scale_for_neutral_rate() {
        let ps = PiperSubprocess::new(
            "piper",
            "/models",
            VoiceConfig {
                speaking_rate: 1.0,
                gain: 1.0,
            },
        );
        let argv = ps.build_argv("en_US-lessac-medium", Path::new("/tmp/out.wav"));
        assert!(
            !argv.contains(&"--length_scale".to_string()),
            "neutral rate must not emit --length_scale: {argv:?}"
        );
    }

    #[test]
    fn speaking_rate_clamped_at_min() {
        let vc = VoiceConfig {
            speaking_rate: -1.0,
            gain: 1.0,
        };
        assert_eq!(vc.effective_speaking_rate(), SPEAKING_RATE_MIN);
    }

    #[test]
    fn speaking_rate_clamped_at_max() {
        let vc = VoiceConfig {
            speaking_rate: 999.0,
            gain: 1.0,
        };
        assert_eq!(vc.effective_speaking_rate(), SPEAKING_RATE_MAX);
    }

    #[test]
    fn gain_clamped_at_min() {
        let vc = VoiceConfig {
            speaking_rate: 1.0,
            gain: -5.0,
        };
        assert_eq!(vc.effective_gain(), GAIN_MIN);
    }

    #[test]
    fn gain_clamped_at_max() {
        let vc = VoiceConfig {
            speaking_rate: 1.0,
            gain: 100.0,
        };
        assert_eq!(vc.effective_gain(), GAIN_MAX);
    }

    // ── gain application ─────────────────────────────────────────────────────

    /// Build a minimal 16-bit mono PCM RIFF/WAVE with `samples` in the data
    /// chunk. `rate` is the sample rate (Hz); channels = 1.
    fn build_pcm16_wav(sample_rate: u32, samples: &[i16]) -> Vec<u8> {
        let num_channels: u16 = 1;
        let bits_per_sample: u16 = 16;
        let byte_rate: u32 = sample_rate * u32::from(num_channels) * u32::from(bits_per_sample) / 8;
        let block_align: u16 = num_channels * bits_per_sample / 8;
        let data_bytes: u32 = (samples.len() as u32) * 2;
        let riff_size: u32 = 4 + (8 + 16) + (8 + data_bytes);
        let mut buf = Vec::with_capacity(12 + 24 + 8 + data_bytes as usize);
        buf.extend_from_slice(b"RIFF");
        buf.extend_from_slice(&riff_size.to_le_bytes());
        buf.extend_from_slice(b"WAVE");
        buf.extend_from_slice(b"fmt ");
        buf.extend_from_slice(&16u32.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes()); // PCM
        buf.extend_from_slice(&num_channels.to_le_bytes());
        buf.extend_from_slice(&sample_rate.to_le_bytes());
        buf.extend_from_slice(&byte_rate.to_le_bytes());
        buf.extend_from_slice(&block_align.to_le_bytes());
        buf.extend_from_slice(&bits_per_sample.to_le_bytes());
        buf.extend_from_slice(b"data");
        buf.extend_from_slice(&data_bytes.to_le_bytes());
        for s in samples {
            buf.extend_from_slice(&s.to_le_bytes());
        }
        buf
    }

    fn read_samples_from_wav(bytes: &[u8]) -> Vec<i16> {
        // Skip RIFF/WAVE header (12 bytes), find data chunk.
        let mut cursor = 12usize;
        while cursor + 8 <= bytes.len() {
            let id = &bytes[cursor..cursor + 4];
            let size = u32::from_le_bytes([
                bytes[cursor + 4],
                bytes[cursor + 5],
                bytes[cursor + 6],
                bytes[cursor + 7],
            ]) as usize;
            let body_start = cursor + 8;
            if id == b"data" {
                let body_end = (body_start + size).min(bytes.len());
                let mut samples = Vec::new();
                let mut i = body_start;
                while i + 1 < body_end {
                    samples.push(i16::from_le_bytes([bytes[i], bytes[i + 1]]));
                    i += 2;
                }
                return samples;
            }
            let pad = usize::from(size % 2 != 0);
            cursor = body_start + size + pad;
        }
        vec![]
    }

    #[test]
    fn gain_2x_doubles_samples() {
        let input: Vec<i16> = vec![100, -200, 0, 1000];
        let mut bytes = build_pcm16_wav(22050, &input);
        apply_gain_to_wav_bytes(&mut bytes, 2.0);
        let out = read_samples_from_wav(&bytes);
        assert_eq!(out, vec![200, -400, 0, 2000]);
    }

    #[test]
    fn gain_unity_preserves_samples() {
        let input: Vec<i16> = vec![1000, -1000, 0, 32767, -32768];
        let mut bytes = build_pcm16_wav(22050, &input);
        apply_gain_to_wav_bytes(&mut bytes, 1.0);
        let out = read_samples_from_wav(&bytes);
        assert_eq!(out, input);
    }

    #[test]
    fn full_scale_at_max_gain_does_not_clip_beyond_i16_range() {
        // AC4: full-scale input at GAIN_MAX must stay within sample range.
        let input: Vec<i16> = vec![i16::MAX, i16::MIN, i16::MAX / 2, -(i16::MAX / 2)];
        let mut bytes = build_pcm16_wav(22050, &input);
        apply_gain_to_wav_bytes(&mut bytes, GAIN_MAX);
        let out = read_samples_from_wav(&bytes);
        for s in &out {
            assert!(
                *s <= i16::MAX && *s >= i16::MIN,
                "sample {s} out of i16 range at GAIN_MAX={GAIN_MAX}"
            );
        }
        // i16::MAX at GAIN_MAX should saturate at i16::MAX (not wrap).
        assert_eq!(out[0], i16::MAX, "full-scale positive must saturate at i16::MAX");
        // i16::MIN at GAIN_MAX should saturate at i16::MIN.
        assert_eq!(out[1], i16::MIN, "full-scale negative must saturate at i16::MIN");
    }

    #[test]
    fn non_riff_bytes_left_unchanged() {
        let mut bytes = b"NOT-A-WAV-FILE".to_vec();
        let original = bytes.clone();
        apply_gain_to_wav_bytes(&mut bytes, 2.0);
        assert_eq!(bytes, original, "non-RIFF bytes must not be modified");
    }

    // ── PiperSubprocess ──────────────────────────────────────────────────────

    #[test]
    fn from_env_defaults_to_piper_on_path() {
        let s = PiperSubprocess::from_env();
        assert_eq!(s.bin, PathBuf::from("piper"));
        assert!(s.models_root.ends_with(".local/share/wintermute/tts/models"));
    }

    #[test]
    fn from_env_uses_elder_friendly_defaults() {
        let s = PiperSubprocess::from_env();
        assert!(s.voice_config.speaking_rate < 1.0);
        assert!(s.voice_config.gain > 1.0);
    }

    #[test]
    fn missing_binary_yields_backend_missing() {
        let s = PiperSubprocess::new(
            "/definitely/not/a/real/piper-binary-xyz",
            "/tmp/no-models",
            VoiceConfig {
                speaking_rate: 1.0,
                gain: 1.0,
            },
        );
        let tmp = tempfile::NamedTempFile::new().expect("temp wav");
        let err = s
            .render("en_US-lessac-medium", "hello", tmp.path())
            .expect_err("missing binary must error");
        assert!(matches!(err, SynthError::BackendMissing(_)));
    }
}
