# wintermute-tts

Text in, audio out, fast. **Piper** (CPU-only, MIT, ~10× real-time on a
desktop) is the primary backend; ElevenLabs is opt-in for natural quality.
Streaming PCM to the default sink so the first syllable lands within
300 ms; cancellable mid-utterance for barge-in; pre-cached ~50 common
short phrases serve in <50 ms.

Part of the [wintermute](https://github.com/j0yen) fleet — Fleet 1
provides the voice the rest of the assistants speak through.

## What it does

Long-running daemon `wm-tts` that subscribes to agorabus and renders
speech to the default PipeWire sink:

| Subscribes | Payload |
|---|---|
| `wm.tts.speak` | `{text, priority, cancel_previous}` |
| `wm.tts.cancel` | `{}` |
| `wm.tts.reload_voice` | `{voice}` |

| Publishes | Payload |
|---|---|
| `wm.tts.start` | `{text, source, ts}` |
| `wm.tts.cancel.ack` | `{ts, drained_ms}` |
| `wm.tts.end` | `{text, duration_ms, outcome, played_bytes, ts}` |
| `wm.tts.error` | `{kind, message, ts}` |

`wm.tts.end.outcome` is one of `"ok"`, `"cancelled"`, `"error"`;
`played_bytes` is the WAV `data` chunk size that flowed to the sink
(`0` on cancel/error). `wm.tts.error.kind` includes
`"pw_cat_missing"` when the configured player binary isn't on
`$PATH` (see [Configuration](#configuration)).

Voicepack module (`wm-voicepack`) lives in this crate as `voicepack::`
until peon-ping PRD-003 begins consuming it, at which point it factors
out to a shared crate. See `docs/PEON_PING_INTEGRATION.md`.

## Acceptance criteria

1. `wm.tts.speak` → first audio at speaker latency: ≤300 ms for a short
   phrase using Piper warm.
2. `wm.tts.cancel` → audio silenced at speaker: ≤100 ms; ack includes
   correct `drained_ms`.
3. Pre-cached phrase ("yes") → speaker: ≤50 ms.
4. Voice hot-swap via `wm.tts.reload_voice` completes in <5 s and does
   not interrupt any non-cancellable in-flight speech.
5. ElevenLabs path (when enabled) first-audio latency ≤400 ms over a
   typical broadband connection.
6. Cloud failure during ElevenLabs streaming falls back to Piper
   mid-sentence or restarts the utterance cleanly.
7. 60-minute steady-state run: no audio glitches, RSS growth <30 MB.
8. The `wm-voicepack` crate is published (in-crate today) and peon-ping
   PRD-003 has an integration ticket.

ACs 1, 3, 5, 7 are hardware-timing tests gated under `#[ignore]` —
run on a machine with audio output. ACs 2, 4, 6, 8 are unit/integration
tests in the `tests/` and `src/` trees and run on every `cargo test`.

## Install

```sh
git clone https://github.com/j0yen/wintermute-tts
cd wintermute-tts
cargo install --path . --root ~/.local
```

Requires `piper` on `$PATH` (Piper TTS engine) for the local backend;
set `WM_CLOUD_TTS_QUALITY=true` to enable the ElevenLabs cloud path
(requires `ELEVENLABS_API_KEY` and `WM_TTS_VOICE_ID_CLOUD`).

`pw-cat` from PipeWire is the playback consumer.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `WM_TTS_VOICE` | `en_US-lessac-medium` | Piper voice name |
| `WM_TTS_VOICE_DIR` | `~/.cache/wintermute/tts/voices/` | Piper ONNX models |
| `WM_TTS_CACHE_DIR` | `~/.cache/wintermute/tts/` | Per-voice WAV cache |
| `WM_TTS_CACHE_CONFIG` | `/etc/wintermute/tts-cache.yaml` | Pre-cache phrases |
| `WM_CLOUD_TTS_QUALITY` | `false` | Opt-in ElevenLabs streaming |
| `WM_TTS_VOICE_ID_CLOUD` | (unset) | ElevenLabs voice id |
| `ELEVENLABS_API_KEY` | (unset) | ElevenLabs API key |
| `WM_SINK_NODE` | (unset → PipeWire default) | `pw-cat --target <node>` (PipeWire sink) |
| `WM_PW_CAT_BIN` | `pw-cat` | Override the playback binary |

## Recent

- **v0.3.0 (2026-05-29) — Elder legibility: speaking-rate and gain knobs.**
  `VoiceConfig` (speaking_rate=0.85, gain=1.20 by default) wires
  `--length_scale` into Piper invocations and scales WAV samples
  post-synthesis. Exposed in YAML as `[voice_settings]`. Neutral
  rate/gain recovers today's behaviour exactly. See `CHANGELOG.md`.
- **v0.2.0 (2026-05-28) — PipeWire output ships.** `wm-tts` finally
  routes rendered audio to the configured `WM_SINK_NODE` via
  `pw-cat --target <node>`, emits `wm.tts.end{outcome, played_bytes}`
  envelopes, fail-soft `wm.tts.error{kind:"pw_cat_missing"}` when the
  player isn't on `$PATH`, and fail-open to PipeWire's default sink
  when `WM_SINK_NODE` is empty. See [PRD-wintermute-tts-pipewire-output][prd]
  and the `v0.2.0` entry in `CHANGELOG.md`.

[prd]: https://github.com/j0yen/autobuilder/blob/main/PRDs-archive/PRD-wintermute-tts-pipewire-output.md

## License

Dual MIT OR Apache-2.0.
