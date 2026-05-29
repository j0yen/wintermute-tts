# Changelog

## v0.2.0 — 2026-05-28

PipeWire output: audio actually plays
(PRD-wintermute-tts-pipewire-output).

Through v0.1.3, `wm-tts` rendered Piper WAVs correctly but never sent
the bytes to a sink — the cache-hit path spawned `pw-cat` without
`--target`, so on a multi-sink machine the daemon was effectively
silent unless PipeWire's default sink happened to match
`WM_SINK_NODE`. Four source comments (`synth.rs:4`, `cache.rs:7`,
`lib.rs:8`, `bus.rs:111`) all pointed at a planned iter-4/iter-6
PipeWire enqueue that never landed.

v0.2.0 ships the enqueue:

- `WM_SINK_NODE` is now read at startup and threaded through
  `player_args` / `streaming_player_args` as `pw-cat --target
  <node>`. Empty / unset → omit the flag and let PipeWire route to
  the default sink (AC9 fail-open).
- New `WM_PW_CAT_BIN` env var (PRD spec) overrides the player
  binary; the older `WM_TTS_PLAYER` alias is still honored.
- `wm.tts.end` carries two new fields: `outcome` (one of `"ok"`,
  `"cancelled"`, `"error"`) and `played_bytes` (the WAV `data`
  chunk size on `ok`, `0` otherwise). The `bus.rs:111` "always
  reports 0" comment is gone.
- Missing `pw-cat` (or whatever `WM_PW_CAT_BIN` points at) now
  publishes `wm.tts.error{kind:"pw_cat_missing"}` followed by
  `wm.tts.end{outcome:"error"}` instead of silently warn-logging
  (AC10 fail-soft).
- Cancel mid-play now reports `wm.tts.end{outcome:"cancelled"}`
  alongside the existing `wm.tts.cancel.ack`.
- `play: started path=…` and `play: ended outcome=… dur_ms=…
  played_bytes=…` log lines on every play attempt for AC3 / AC6
  smoke verification.
- Four stale iter-N planning comments removed: `synth.rs:4`,
  `cache.rs:7`, `lib.rs:8`, `bus.rs:111`.

Queue / drop policy for overlapping `wm.tts.speak` events: the
dispatch loop awaits each play to completion before draining the
next event, so two `speak`s arriving while one is playing serialize
naturally — there is no parallel-play branch and no explicit drop
policy. A future PRD (PRD-wintermute-tts-queue-policy) will surface
an explicit knob.

Non-goal: the `pipewire-rs` streaming consumer described in the
former `lib.rs:8` comment is deferred to
PRD-wintermute-tts-pipewire-streaming. Subprocess `pw-cat` gets
audio to the speaker today.

## v0.1.3 — 2026-05-28

Break the wm.tts.error feedback loop (PRD-wintermute-tts-error-loop-suppress).

wm-tts subscribes to the `wm.tts.` prefix so it can see incoming requests
(`wm.tts.speak`, `wm.tts.cancel`, `wm.tts.reload_voice`) AND publishes its own
events (`wm.tts.error`, `wm.tts.start`, `wm.tts.end`, `wm.tts.cancel.ack`,
`wm.tts.reload.ack`) onto the same prefix. The agorabus broadcast layer echoes
every publish back to the subscribing socket, so every `wm.tts.error` we
publish landed back at our dispatch loop, failed `decode_request` as
`UnknownTopic`, and triggered another `wm.tts.error` describing the decode
failure. Production hit 37,488 log lines in 30s (~1,250 events/s) before
systemd intervention; the bus's broadcast-channel slot pressure masked the
true rate via `RecvError::Lagged` swallows.

Fix is the PRD's recommended option #1: explicit self-emitted-topic allow-list
on the subscribe side. Added `bus::outgoing::ALL` (every outbound topic) and
`bus::is_self_emitted_topic(&str) -> bool`; the dispatch loop in `run()` now
silently `continue`s on any inbound event whose topic matches one of our own
publishes. The filter is exhaustively unit-tested (4 new tests in `bus.rs`,
1 regression-locking test in `daemon.rs`) so a future outbound topic added to
`outgoing` without extending `ALL` makes the assertion fail.

Inbound legit requests still flow; legitimate decode failures on truly unknown
topics still publish exactly one `wm.tts.error` (which is then silently filtered
on echo — no recursion).

## v0.1.1 — 2026-05-28

Fix post-announce bus-startup defect (PRD-wintermute-fleet-bus-startup-defect).

The announce-before-subscribe fix that shipped overnight was install-stale, not
source-buggy: the binaries under ~/.local/bin/ predated the fix, while the source
already had the dual-Client + announce-first pattern. Tightened the agorabus
path-dependency pin from a wildcard/^0.1 to ^0.3 (agorabus 0.3.0's let_chains
need system cargo 1.95), rebuilt, and reinstalled so the systemd-launched daemons
run post-fix bytes. Daemons now survive a 60s soak (NRestarts=0) and round-trip
their subscribed topics. Note: AC3-strict (peer presence after the 60s window)
is deferred to PRD-wintermute-fleet-bus-heartbeat-keepalive — these daemons still
lack a post-announce heartbeat, so the bus prunes them from the peer snapshot.
