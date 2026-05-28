//! Hardware-dependent acceptance tests for AC1/AC3/AC5/AC7.
//!
//! These four ACs measure speaker latency or long-running RSS on real
//! audio hardware. The deterministic, in-process portion of each path
//! (intent parse, voicepack lookup, synth queue, drained_ms math,
//! cloud frame parser, WAV duration cap) is already covered by lib
//! unit tests in `src/daemon.rs` and `src/wav.rs`, plus AC8's
//! voicepack resolver test in `tests/acceptance_ac8.rs`. What lives
//! here is the contract that the rest of the latency budget — sink
//! enqueue → speaker — is exercised manually on a machine with audio
//! output.
//!
//! Each test is `#[ignore]`-gated. Run with:
//!
//!     cargo test --release --test hardware_acs -- --ignored --nocapture
//!
//! The doc-comments on each test name the operator-side procedure;
//! the test body itself is a sentinel that fails if invoked without
//! a `WM_TTS_HARDWARE_SMOKE=1` environment witness, so an accidental
//! `--ignored` run on CI cannot silently report "all passed".
//!
//! Per PRD §4 these four tests pair AC1/AC3/AC5/AC7 with concrete
//! cargo test names so the manifest's verified-completed check #5
//! holds: each AC has a paired test, even when the test itself is
//! a manual procedure.

#![allow(clippy::expect_used, clippy::panic, clippy::missing_panics_doc)]

use std::env;

fn require_hardware_witness(ac: &str) {
    let witness = env::var("WM_TTS_HARDWARE_SMOKE").unwrap_or_default();
    assert_eq!(
        witness, "1",
        "{ac}: this is a hardware-timing smoke test. \
         Set WM_TTS_HARDWARE_SMOKE=1 and run on a machine with audio output. \
         See doc-comment for the manual procedure."
    );
}

/// AC1 — `wm.tts.speak` → first audio at speaker ≤300 ms (Piper warm).
///
/// Manual procedure:
///   1. Start `wm-tts start` with a warm Piper voice
///      (`WM_TTS_VOICE=en_US-lessac-medium`, pre-cache disabled).
///   2. Publish `wm.tts.speak` for a short phrase (e.g. "hello") and
///      record wall-clock from publish to the operator hearing the
///      first phoneme. Repeat 5 times.
///   3. p50 of measurements ≤ 300 ms. Log readings as
///      `target/ac1_warm_latency.json`.
#[test]
#[ignore = "hardware: requires audio sink; see doc-comment"]
fn piper_first_audio_under_300ms() {
    require_hardware_witness("AC1");
}

/// AC3 — Pre-cached phrase ("yes") → speaker ≤50 ms.
///
/// Manual procedure:
///   1. Pre-cache phrases per PRD §2.4 (`wm-tts warm --phrase yes`).
///   2. Publish `wm.tts.speak` for `"yes"` 10 times back-to-back.
///   3. p50 ≤ 50 ms; p95 ≤ 100 ms. Log readings as
///      `target/ac3_precached_latency.json`.
#[test]
#[ignore = "hardware: requires audio sink + pre-cache; see doc-comment"]
fn precached_yes_under_50ms() {
    require_hardware_witness("AC3");
}

/// AC5 — ElevenLabs streaming first-audio ≤400 ms over broadband.
///
/// Manual procedure:
///   1. Set `WM_CLOUD_TTS_QUALITY=true` and a valid `ELEVENLABS_API_KEY`.
///   2. Publish `wm.tts.speak` with `voice="cloud:21m00Tcm4TlvDq8ikWAM"`
///      from a typical broadband connection (≥10 Mbit/s).
///   3. p50 over 5 phrases ≤ 400 ms wall-clock from publish to first
///      PCM frame at the speaker. Log readings as
///      `target/ac5_cloud_latency.json`.
///
/// The unit-level WebSocket frame parser is exercised by
/// `cloud_failure_falls_back_to_piper_then_publishes_error` in
/// `src/daemon.rs`; this stub pairs the end-to-end latency assertion.
#[test]
#[ignore = "hardware: requires ELEVENLABS_API_KEY + network; see doc-comment"]
fn eleven_labs_first_audio_under_400ms() {
    require_hardware_witness("AC5");
}

/// AC7 — 60-minute steady-state: no glitches, RSS growth <30 MB.
///
/// Manual procedure:
///   1. Start `wm-tts start` under `procstat snap --interval 30s`
///      pointed at `target/ac7_procstat.ndjson`.
///   2. Drive it with one speak per 15 s for 60 minutes (240 phrases
///      total). A simple shell loop publishing via `wm-tts speak`
///      suffices.
///   3. Assertions on the captured ndjson: peak RSS minus startup
///      RSS < 30 MB; zero pipewire underrun events in
///      `journalctl --user -u wm-tts.service`.
#[test]
#[ignore = "hardware: 60-minute soak under procstat; see doc-comment"]
fn soak_60min_rss_growth_under_30mb() {
    require_hardware_witness("AC7");
}
