//! Minimal RIFF/WAVE header parser.
//!
//! The daemon uses [`parse_duration_ms`] to compute an upper-bound cap
//! on `drained_ms` from a cache-hit or Piper-render WAV (iter-17). Not
//! a general WAV decoder — only the `fmt ` + `data` chunks are
//! inspected; unknown chunks (LIST, bext, ...) are skipped per RIFF
//! spec.

use std::path::Path;

use thiserror::Error;

/// Errors produced by the WAV header parser.
#[derive(Debug, Error)]
pub enum WavError {
    /// I/O failure reading the file.
    #[error("io reading wav: {0}")]
    Io(#[from] std::io::Error),
    /// Header isn't `RIFF....WAVE`.
    #[error("not a RIFF/WAVE container")]
    NotRiff,
    /// Container ended before a `fmt ` chunk was seen.
    #[error("fmt chunk missing")]
    MissingFmt,
    /// Container ended before a `data` chunk was seen.
    #[error("data chunk missing")]
    MissingData,
    /// `fmt ` chunk is smaller than the 16-byte PCM minimum.
    #[error("fmt chunk truncated")]
    TruncatedFmt,
    /// `byte_rate` is zero — cannot divide by it.
    #[error("byte_rate is zero")]
    ZeroByteRate,
    /// Chunk size arithmetic overflowed.
    #[error("chunk size overflow")]
    SizeOverflow,
}

/// Parse the WAV header at `path` and return the playback duration in
/// milliseconds (`data_size * 1000 / byte_rate`).
///
/// # Errors
/// See [`WavError`]. Non-WAV files and truncated headers produce
/// `NotRiff` / `MissingFmt` / `MissingData` respectively.
pub fn parse_duration_ms(path: &Path) -> Result<u64, WavError> {
    let bytes = std::fs::read(path)?;
    parse_duration_ms_bytes(&bytes)
}

/// Same as [`parse_duration_ms`] but takes an in-memory slice. Used by
/// tests to avoid a filesystem round-trip.
///
/// # Errors
/// See [`parse_duration_ms`].
pub fn parse_duration_ms_bytes(bytes: &[u8]) -> Result<u64, WavError> {
    let header = bytes.get(..12).ok_or(WavError::NotRiff)?;
    if header.get(..4) != Some(b"RIFF") || header.get(8..12) != Some(b"WAVE") {
        return Err(WavError::NotRiff);
    }

    let mut cursor: usize = 12;
    let mut byte_rate: Option<u32> = None;
    let mut data_size: Option<u32> = None;

    while let Some(chunk_header) = bytes.get(cursor..cursor.saturating_add(8)) {
        let id: [u8; 4] = chunk_header
            .get(..4)
            .and_then(|s| <[u8; 4]>::try_from(s).ok())
            .ok_or(WavError::TruncatedFmt)?;
        let size_le: [u8; 4] = chunk_header
            .get(4..8)
            .and_then(|s| <[u8; 4]>::try_from(s).ok())
            .ok_or(WavError::TruncatedFmt)?;
        let size = u32::from_le_bytes(size_le);
        let body_start = cursor.checked_add(8).ok_or(WavError::SizeOverflow)?;
        let size_usize = usize::try_from(size).map_err(|_| WavError::SizeOverflow)?;
        let body_end = body_start
            .checked_add(size_usize)
            .ok_or(WavError::SizeOverflow)?;
        if body_end > bytes.len() {
            break;
        }
        match &id {
            b"fmt " => {
                if size < 16 {
                    return Err(WavError::TruncatedFmt);
                }
                let body = bytes
                    .get(body_start..body_end)
                    .ok_or(WavError::TruncatedFmt)?;
                let br: [u8; 4] = body
                    .get(8..12)
                    .and_then(|s| <[u8; 4]>::try_from(s).ok())
                    .ok_or(WavError::TruncatedFmt)?;
                byte_rate = Some(u32::from_le_bytes(br));
            }
            b"data" => {
                data_size = Some(size);
            }
            _ => {}
        }
        // RIFF chunks are padded to even byte alignment; the pad byte
        // is not counted in `size` but must be stepped over.
        let pad = u32::from(size % 2 == 1);
        let pad_usize = usize::try_from(pad).map_err(|_| WavError::SizeOverflow)?;
        let advance = size_usize
            .checked_add(pad_usize)
            .ok_or(WavError::SizeOverflow)?;
        cursor = body_start.checked_add(advance).ok_or(WavError::SizeOverflow)?;
    }

    let byte_rate = byte_rate.ok_or(WavError::MissingFmt)?;
    let data_size = data_size.ok_or(WavError::MissingData)?;
    if byte_rate == 0 {
        return Err(WavError::ZeroByteRate);
    }
    Ok(u64::from(data_size).saturating_mul(1000) / u64::from(byte_rate))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    /// Build a minimal RIFF/WAVE byte sequence with the supplied
    /// `sample_rate`, 16-bit mono PCM, and `data_bytes` of audio body.
    fn build_wav(sample_rate: u32, data_bytes: usize) -> Vec<u8> {
        let num_channels: u16 = 1;
        let bits_per_sample: u16 = 16;
        let byte_rate: u32 =
            sample_rate * u32::from(num_channels) * u32::from(bits_per_sample) / 8;
        let block_align: u16 = num_channels * bits_per_sample / 8;
        let data_size: u32 = u32::try_from(data_bytes).unwrap();
        // fmt(8+16) + data(8+N) + RIFF(4)
        let riff_size: u32 = 4 + (8 + 16) + (8 + data_size);
        let mut buf = Vec::with_capacity(12 + 24 + 8 + data_bytes);
        buf.extend_from_slice(b"RIFF");
        buf.extend_from_slice(&riff_size.to_le_bytes());
        buf.extend_from_slice(b"WAVE");
        // fmt chunk
        buf.extend_from_slice(b"fmt ");
        buf.extend_from_slice(&16u32.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes()); // PCM
        buf.extend_from_slice(&num_channels.to_le_bytes());
        buf.extend_from_slice(&sample_rate.to_le_bytes());
        buf.extend_from_slice(&byte_rate.to_le_bytes());
        buf.extend_from_slice(&block_align.to_le_bytes());
        buf.extend_from_slice(&bits_per_sample.to_le_bytes());
        // data chunk
        buf.extend_from_slice(b"data");
        buf.extend_from_slice(&data_size.to_le_bytes());
        buf.extend(std::iter::repeat_n(0u8, data_bytes));
        buf
    }

    #[test]
    fn one_second_22050hz_mono_16bit_is_1000ms() {
        // byte_rate = 22050 * 1 * 16 / 8 = 44100
        // 1s of audio = 44100 bytes
        let bytes = build_wav(22050, 44100);
        let ms = parse_duration_ms_bytes(&bytes).unwrap();
        assert_eq!(ms, 1000);
    }

    #[test]
    fn half_second_44100hz_mono_16bit_is_500ms() {
        // byte_rate = 88200; 0.5s = 44100 bytes
        let bytes = build_wav(44100, 44100);
        let ms = parse_duration_ms_bytes(&bytes).unwrap();
        assert_eq!(ms, 500);
    }

    #[test]
    fn empty_body_is_zero_ms() {
        let bytes = build_wav(22050, 0);
        let ms = parse_duration_ms_bytes(&bytes).unwrap();
        assert_eq!(ms, 0);
    }

    #[test]
    fn non_riff_is_rejected() {
        let bytes = b"NOTAWAVEFILE";
        let err = parse_duration_ms_bytes(bytes).unwrap_err();
        assert!(matches!(err, WavError::NotRiff));
    }

    #[test]
    fn too_short_is_rejected() {
        let bytes = b"RIFF";
        let err = parse_duration_ms_bytes(bytes).unwrap_err();
        assert!(matches!(err, WavError::NotRiff));
    }

    #[test]
    fn missing_data_chunk_is_rejected() {
        // Build RIFF/WAVE with only fmt chunk.
        let mut buf = Vec::new();
        buf.extend_from_slice(b"RIFF");
        buf.extend_from_slice(&(4u32 + 8 + 16).to_le_bytes());
        buf.extend_from_slice(b"WAVE");
        buf.extend_from_slice(b"fmt ");
        buf.extend_from_slice(&16u32.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&22050u32.to_le_bytes());
        buf.extend_from_slice(&44100u32.to_le_bytes());
        buf.extend_from_slice(&2u16.to_le_bytes());
        buf.extend_from_slice(&16u16.to_le_bytes());
        let err = parse_duration_ms_bytes(&buf).unwrap_err();
        assert!(matches!(err, WavError::MissingData));
    }

    #[test]
    fn truncated_fmt_chunk_is_rejected() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"RIFF");
        buf.extend_from_slice(&20u32.to_le_bytes());
        buf.extend_from_slice(b"WAVE");
        buf.extend_from_slice(b"fmt ");
        // Declare size=8 (less than 16-byte PCM minimum).
        buf.extend_from_slice(&8u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 8]);
        let err = parse_duration_ms_bytes(&buf).unwrap_err();
        assert!(matches!(err, WavError::TruncatedFmt));
    }

    #[test]
    fn unknown_chunks_are_skipped() {
        // Build RIFF/WAVE with LIST(extra) before fmt and data.
        let sample_rate: u32 = 22050;
        let data_bytes: usize = 44100; // 1 second
        let byte_rate: u32 = 44100;
        let extra_payload: [u8; 6] = *b"LISTab";
        // LIST chunk is just 6 bytes of opaque body.
        // riff_size = 4 (WAVE) + 8 + 6 + 8 + 16 + 8 + 44100
        let extra_size: u32 = 6;
        let data_u32 = u32::try_from(data_bytes).unwrap();
        let riff_size: u32 = 4 + (8 + extra_size) + (8 + 16) + (8 + data_u32);
        let mut buf = Vec::new();
        buf.extend_from_slice(b"RIFF");
        buf.extend_from_slice(&riff_size.to_le_bytes());
        buf.extend_from_slice(b"WAVE");
        buf.extend_from_slice(b"LIST");
        buf.extend_from_slice(&extra_size.to_le_bytes());
        buf.extend_from_slice(&extra_payload);
        buf.extend_from_slice(b"fmt ");
        buf.extend_from_slice(&16u32.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&sample_rate.to_le_bytes());
        buf.extend_from_slice(&byte_rate.to_le_bytes());
        buf.extend_from_slice(&2u16.to_le_bytes());
        buf.extend_from_slice(&16u16.to_le_bytes());
        buf.extend_from_slice(b"data");
        buf.extend_from_slice(&data_u32.to_le_bytes());
        buf.extend(std::iter::repeat_n(0u8, data_bytes));
        let ms = parse_duration_ms_bytes(&buf).unwrap();
        assert_eq!(ms, 1000);
    }

    #[test]
    fn odd_sized_chunk_is_padded_to_even() {
        // Place an odd-sized LIST chunk (5 bytes) before fmt; the parser
        // must skip the implicit pad byte to land on fmt.
        let sample_rate: u32 = 22050;
        let data_bytes: usize = 22050; // 0.5s
        let byte_rate: u32 = 44100;
        let extra_size: u32 = 5;
        let mut buf = Vec::new();
        buf.extend_from_slice(b"RIFF");
        buf.extend_from_slice(&0u32.to_le_bytes()); // size unimportant for parser
        buf.extend_from_slice(b"WAVE");
        buf.extend_from_slice(b"LIST");
        buf.extend_from_slice(&extra_size.to_le_bytes());
        buf.extend_from_slice(b"abcde");
        buf.push(0); // pad byte
        buf.extend_from_slice(b"fmt ");
        buf.extend_from_slice(&16u32.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&sample_rate.to_le_bytes());
        buf.extend_from_slice(&byte_rate.to_le_bytes());
        buf.extend_from_slice(&2u16.to_le_bytes());
        buf.extend_from_slice(&16u16.to_le_bytes());
        let data_u32 = u32::try_from(data_bytes).unwrap();
        buf.extend_from_slice(b"data");
        buf.extend_from_slice(&data_u32.to_le_bytes());
        buf.extend(std::iter::repeat_n(0u8, data_bytes));
        let ms = parse_duration_ms_bytes(&buf).unwrap();
        assert_eq!(ms, 500);
    }

    #[test]
    fn round_trip_through_tempfile() {
        let bytes = build_wav(16000, 16000); // byte_rate=32000, 16000 bytes = 500ms
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.wav");
        std::fs::write(&path, &bytes).unwrap();
        let ms = parse_duration_ms(&path).unwrap();
        assert_eq!(ms, 500);
    }
}
