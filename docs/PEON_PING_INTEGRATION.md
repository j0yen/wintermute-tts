# peon-ping PRD-003 integration

PRD-wintermute-tts §2.5 and peon-ping PRD-003 both want a TTS engine
on Linux. The two PRDs agreed: whichever ships first defines the
voice-pack resolver, and the other consumes it.

`wm-tts` shipped first. The resolver lives in this repo at
`src/voicepack.rs` per the intent-card resolution of PRD §7 (in-repo
until a second consumer appears, then extract to a shared crate).

## Contract

The resolver is `wintermute_tts::voicepack::resolve(name: &str)
-> Result<Backend, VoicePackError>` with the [`Backend`] enum
covering three engines:

| Variant       | Identifier prefix      | Carries            |
|---------------|------------------------|--------------------|
| `Piper`       | bare or `piper:<name>` | ONNX model path    |
| `ElevenLabs`  | `cloud:<voice_id>`     | ElevenLabs id      |
| `EspeakNg`    | `espeak:<args>`        | CLI argument list  |

Bare names use the Piper repository layout (e.g.
`en_US-lessac-medium` → `en_US-lessac-medium.onnx`). The resolver
does not load models or open network connections — that stays in
the dispatch path.

## peon-ping integration ticket

Tracked at peon-ping PRD-003 §"Voice-pack resolver" — peon-ping
consumes this crate once it gains a Linux TTS path. When that
happens, this module extracts to a sibling crate
`~/wintermute/wm-voicepack/` with the same public API; both `wm-tts`
and peon-ping depend on it via `path = "../wm-voicepack"` (eventually
the crates.io publish, once both repos are public).

Until then, peon-ping's TTS-enabled notifications either:
- shell out to `wm-tts speak` over agorabus (`wm.tts.speak` topic), or
- carry the resolver inline by depending on this crate via path.

The agorabus route is the recommended integration — keeps peon-ping
free of audio backend deps and uses the existing wm-tts daemon.

## Stability promise

Backend variants are additive-only until extraction; renaming or
removing a variant requires bumping `wintermute-tts` minor version
and updating peon-ping in lockstep.
