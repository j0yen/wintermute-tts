//! Agorabus topic + payload schema for `wm-tts`.
//!
//! Subscribed prefix is `wm.tts.`; concrete topics are listed in [`Topic`].
//! Payloads round-trip through `serde_json::Value` because the agorabus
//! [`ServerEvent::data`](agorabus::ServerEvent) is a `Value` — the daemon
//! [`crate::daemon`] decodes per-topic into the request enums below.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};

/// Subscribe prefix that captures all incoming `wm-tts` topics.
pub const TOPIC_PREFIX: &str = "wm.tts.";

/// A correlation id that threads one spoken turn across every daemon.
///
/// Minted at wake by `wm-audio` as `<unix_ms_hex>-<seq_hex>` and copied
/// onto every downstream event in the same turn. `wm-tts` is the terminal
/// daemon: it copies the inbound `turn_id` from the `wm.tts.speak` request
/// onto its `wm.tts.start` / `wm.tts.end` events so a consumer can join the
/// whole turn (wake → stt → dialog → brain → tts) by id.
///
/// The format is `<hex>-<hex>` but `wm-tts` treats the value as an opaque
/// token — it only copies it through, never re-mints. The field is
/// **optional** in every envelope: events without a `turn_id` (legacy, or
/// from an upstream that hasn't adopted this PRD) remain valid and consumers
/// MUST handle `None` gracefully.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TurnId(pub String);

/// Global monotone counter for same-millisecond collision avoidance when
/// `wm-tts` must mint a fallback id (a `speak` request that carried none).
static TURN_SEQ: AtomicU32 = AtomicU32::new(0);

impl TurnId {
    /// Mint a fresh, collision-resistant `TurnId`. `wm-tts` only mints when
    /// an inbound `speak` request carried no id (e.g. a system-injected
    /// utterance); the normal path copies the upstream id through.
    #[must_use]
    pub fn mint() -> Self {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        let seq = TURN_SEQ.fetch_add(1, Ordering::Relaxed);
        Self(format!("{ms:013x}-{seq:04x}"))
    }

    /// Parse a `TurnId` from a string slice, returning `None` if the format
    /// does not match `<hex>-<hex>`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let mut parts = s.splitn(2, '-');
        let ms_part = parts.next()?;
        let seq_part = parts.next()?;
        if ms_part.is_empty()
            || seq_part.is_empty()
            || !ms_part.chars().all(|c| c.is_ascii_hexdigit())
            || !seq_part.chars().all(|c| c.is_ascii_hexdigit())
        {
            return None;
        }
        Some(Self(s.to_owned()))
    }

    /// Borrow the underlying string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TurnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Incoming topics handled by the daemon.
pub mod incoming {
    /// Render and play an utterance.
    pub const SPEAK: &str = "wm.tts.speak";
    /// Cancel the current utterance (no-op if none active).
    pub const CANCEL: &str = "wm.tts.cancel";
    /// Hot-swap the active voice.
    pub const RELOAD_VOICE: &str = "wm.tts.reload_voice";
}

/// Outgoing topics published by the daemon.
pub mod outgoing {
    /// First-syllable-ready marker for an utterance.
    pub const START: &str = "wm.tts.start";
    /// Acknowledgement of a cancel request.
    pub const CANCEL_ACK: &str = "wm.tts.cancel.ack";
    /// End-of-utterance marker with duration.
    pub const END: &str = "wm.tts.end";
    /// Failure marker; payload carries `kind` + `message`.
    pub const ERROR: &str = "wm.tts.error";
    /// Acknowledgement of a successful voice hot-swap.
    pub const RELOAD_ACK: &str = "wm.tts.reload.ack";

    /// All outbound topics this daemon publishes.
    ///
    /// Used by the dispatch loop to silently skip events whose topic
    /// matches one of our own publishes. Without this filter, the
    /// broadcast bus echoes our `wm.tts.error` back to us,
    /// `decode_request` rejects it as `UnknownTopic`, we publish
    /// another `wm.tts.error` describing the rejection, the bus echoes
    /// it back, and the cycle saturates the daemon. See
    /// PRD-wintermute-tts-error-loop-suppress.
    pub const ALL: &[&str] = &[START, CANCEL_ACK, END, ERROR, RELOAD_ACK];
}

/// Returns `true` iff `topic` is one of the daemon's own outbound topics.
///
/// Backed by [`outgoing::ALL`]. The dispatch loop uses this to silently
/// skip echoes of its own publishes. See [`outgoing::ALL`] for the loop
/// pathology this guards against.
#[must_use]
pub fn is_self_emitted_topic(topic: &str) -> bool {
    outgoing::ALL.contains(&topic)
}

/// Decoded request payloads. Returned by [`decode_request`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Request {
    /// `wm.tts.speak` payload.
    Speak(SpeakRequest),
    /// `wm.tts.reload_voice` payload.
    ReloadVoice(ReloadVoiceRequest),
    /// `wm.tts.cancel` payload — body is `{}` or absent.
    Cancel(CancelRequest),
}

/// `wm.tts.speak` payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeakRequest {
    /// Text to render.
    pub text: String,
    /// Optional priority hint (`"low" | "normal" | "high"`). Free-form; iter-4 logs it.
    #[serde(default)]
    pub priority: Option<String>,
    /// When `true`, cancel any in-flight utterance before starting this one.
    #[serde(default)]
    pub cancel_previous: bool,
    /// Correlation id for the spoken turn this utterance answers. Copied
    /// from the upstream `wm.brain.reply{turn_id}` that produced this
    /// speak request, and re-emitted on `wm.tts.start` / `wm.tts.end`.
    /// Optional/additive for backward compat (legacy requests carry none).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<TurnId>,
}

/// `wm.tts.reload_voice` payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReloadVoiceRequest {
    /// New voice identifier (e.g. `en_US-lessac-medium`).
    pub voice: String,
}

/// `wm.tts.cancel` payload (no fields).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CancelRequest {}

/// Outbound `wm.tts.start` payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StartEvent {
    /// The utterance text.
    pub text: String,
    /// Originating session id (from the speak request).
    pub source: String,
    /// Unix milliseconds when the speak request was accepted.
    pub ts: u64,
    /// Turn correlation id copied from the inbound speak request. Shared
    /// with `wm.tts.end` for the same utterance. Optional for backward
    /// compat (absent when the speak request carried no id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<TurnId>,
}

/// Outbound `wm.tts.cancel.ack` payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CancelAckEvent {
    /// Unix milliseconds when cancel was processed.
    pub ts: u64,
    /// Milliseconds of speech already played before cancel landed. The
    /// daemon approximates this as `min(elapsed_since_spawn,
    /// wav_declared_ms)` for file-based plays and `elapsed_since_spawn`
    /// alone for cloud MP3 streams (length unknown up-front). A future
    /// `PipeWire`-rs streaming consumer will report exact frame counts.
    pub drained_ms: u64,
}

/// Playback outcome values for [`EndEvent::outcome`].
pub mod outcome {
    /// Player exited cleanly; the utterance completed.
    pub const OK: &str = "ok";
    /// `wm.tts.cancel` interrupted playback mid-stream.
    pub const CANCELLED: &str = "cancelled";
    /// Playback failed (player non-zero exit, spawn failure, render
    /// error). A paired `wm.tts.error` envelope carries the kind +
    /// message.
    pub const ERROR: &str = "error";
}

/// Outbound `wm.tts.end` payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EndEvent {
    /// The utterance text.
    pub text: String,
    /// Wall-clock playback span in milliseconds (player spawn → exit).
    pub duration_ms: u64,
    /// Playback outcome: `"ok"`, `"cancelled"`, or `"error"`. See
    /// [`outcome`].
    pub outcome: String,
    /// Bytes of audio sent to the sink. For WAV plays this is the
    /// `data` chunk size from the file header; for cloud MP3 streams
    /// it's the byte count actually written to the player's stdin
    /// before the stream ended. `0` for `outcome != "ok"`.
    pub played_bytes: u64,
    /// Unix milliseconds when the utterance completed.
    pub ts: u64,
    /// Turn correlation id copied from the inbound speak request, shared
    /// with the paired `wm.tts.start`. This is the terminal event of a
    /// turn, so a consumer joining by `turn_id` sees the whole span
    /// close here. Optional for backward compat.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<TurnId>,
}

/// Outbound `wm.tts.reload.ack` payload.
///
/// Emitted on successful `wm.tts.reload_voice` hot-swap. `prerendered`
/// and `cache_hits` are the rendered/skipped counts from the per-voice
/// pre-render pass; `failures` is the count of phrases the synth
/// backend could not render for the new voice (still counted as a
/// successful swap — startup tolerates per-phrase failures the same way).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReloadAckEvent {
    /// Newly active voice id.
    pub voice: String,
    /// Phrases already on disk for the new voice (hits).
    pub cache_hits: usize,
    /// Phrases newly rendered as part of the swap.
    pub prerendered: usize,
    /// Phrases whose render failed; consumers may want to know.
    pub failures: usize,
    /// Wall-clock milliseconds spent in the swap.
    pub elapsed_ms: u64,
    /// Unix milliseconds when the swap completed.
    pub ts: u64,
}

/// Outbound `wm.tts.error` payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorEvent {
    /// Short kind tag (`"render" | "voice" | "io" | "bus"`).
    pub kind: String,
    /// Human-readable detail.
    pub message: String,
    /// Unix milliseconds when the error fired.
    pub ts: u64,
}

/// Errors raised while decoding an inbound payload.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// Topic was not one of the known incoming names.
    #[error("unknown topic: {0}")]
    UnknownTopic(String),
    /// JSON decode of the payload failed.
    #[error("payload decode failed: {0}")]
    Json(#[from] serde_json::Error),
}

/// Decode a raw `(topic, data)` pair into a strongly-typed [`Request`].
///
/// # Errors
/// Returns [`DecodeError::UnknownTopic`] for topics outside the
/// `wm.tts.{speak,cancel,reload_voice}` set, or [`DecodeError::Json`]
/// when the payload shape doesn't match.
pub fn decode_request(topic: &str, data: &serde_json::Value) -> Result<Request, DecodeError> {
    match topic {
        incoming::SPEAK => Ok(Request::Speak(serde_json::from_value(data.clone())?)),
        incoming::RELOAD_VOICE => {
            Ok(Request::ReloadVoice(serde_json::from_value(data.clone())?))
        }
        incoming::CANCEL => {
            // Cancel payload is `{}`; tolerate `null` and missing too.
            if data.is_null() {
                Ok(Request::Cancel(CancelRequest::default()))
            } else {
                Ok(Request::Cancel(serde_json::from_value(data.clone())?))
            }
        }
        other => Err(DecodeError::UnknownTopic(other.to_string())),
    }
}

/// Wall-clock milliseconds since the Unix epoch. Saturates to `u64::MAX`
/// if the clock is set before 1970 (shouldn't happen).
#[must_use]
pub fn now_unix_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(u64::MAX, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
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
    use serde_json::json;

    #[test]
    fn decode_speak_minimal() {
        let req =
            decode_request(incoming::SPEAK, &json!({ "text": "hello" })).expect("speak parses");
        assert_eq!(
            req,
            Request::Speak(SpeakRequest {
                text: "hello".into(),
                priority: None,
                cancel_previous: false,
                turn_id: None,
            })
        );
    }

    #[test]
    fn decode_speak_full() {
        let req = decode_request(
            incoming::SPEAK,
            &json!({ "text": "x", "priority": "high", "cancel_previous": true }),
        )
        .expect("speak parses");
        assert_eq!(
            req,
            Request::Speak(SpeakRequest {
                text: "x".into(),
                priority: Some("high".into()),
                cancel_previous: true,
                turn_id: None,
            })
        );
    }

    // ---- TurnId: AC1 (mint/parse, same-ms collision avoidance) ----

    #[test]
    fn turn_id_mint_is_parseable() {
        let id = TurnId::mint();
        assert_eq!(
            TurnId::parse(id.as_str()).as_ref().map(TurnId::as_str),
            Some(id.as_str()),
            "a minted id must parse-round-trip"
        );
    }

    #[test]
    fn turn_id_two_mints_differ() {
        // Same-millisecond mints must still differ (the seq counter).
        let a = TurnId::mint();
        let b = TurnId::mint();
        assert_ne!(a, b, "two minted TurnIds must differ");
    }

    #[test]
    fn turn_id_parse_rejects_garbage() {
        assert!(TurnId::parse("").is_none());
        assert!(TurnId::parse("no-dash-hex").is_none());
        assert!(TurnId::parse("-abc").is_none());
        assert!(TurnId::parse("abc-").is_none());
    }

    // ---- AC3: wm-tts copies the inbound turn_id onto start + end ----

    #[test]
    fn speak_request_decodes_inbound_turn_id() {
        // A brain.reply-derived speak request carries a turn_id; decode
        // must surface it so handle_speak can copy it onto start/end.
        let req = decode_request(
            incoming::SPEAK,
            &json!({ "text": "ok", "turn_id": "0000000abc12-000f" }),
        )
        .expect("speak parses");
        let Request::Speak(s) = req else {
            panic!("expected Speak")
        };
        assert_eq!(
            s.turn_id.as_ref().map(TurnId::as_str),
            Some("0000000abc12-000f"),
            "inbound turn_id must be carried on the decoded request"
        );
    }

    #[test]
    fn start_and_end_carry_same_turn_id() {
        // AC3/AC6: start and end events emitted for one utterance share
        // the inbound id (the field handle_speak copies through).
        let id = TurnId::mint();
        let start = StartEvent {
            text: "hi".into(),
            source: String::new(),
            ts: 1,
            turn_id: Some(id.clone()),
        };
        let end = EndEvent {
            text: "hi".into(),
            duration_ms: 10,
            outcome: outcome::OK.to_string(),
            played_bytes: 4,
            ts: 2,
            turn_id: Some(id.clone()),
        };
        assert_eq!(start.turn_id, end.turn_id, "start/end ids must match");
        let sv = serde_json::to_value(&start).unwrap();
        let ev = serde_json::to_value(&end).unwrap();
        assert_eq!(sv["turn_id"], id.as_str());
        assert_eq!(ev["turn_id"], id.as_str());
    }

    // ---- AC5: turn_id is optional/additive (backward compat) ----

    #[test]
    fn legacy_speak_request_without_turn_id() {
        // A pre-PRD speak payload (no turn_id) must still decode, with None.
        let req = decode_request(incoming::SPEAK, &json!({ "text": "legacy" }))
            .expect("legacy speak must decode");
        let Request::Speak(s) = req else {
            panic!("expected Speak")
        };
        assert!(s.turn_id.is_none(), "absent turn_id must map to None");
    }

    #[test]
    fn start_end_omit_turn_id_when_absent() {
        // With no inbound id, the serialized event must not carry the
        // field at all (skip_serializing_if) — legacy consumers unaffected.
        let start = StartEvent {
            text: "x".into(),
            source: String::new(),
            ts: 1,
            turn_id: None,
        };
        let v = serde_json::to_value(&start).unwrap();
        assert!(
            v.get("turn_id").is_none(),
            "absent turn_id must not appear in serialized start event"
        );
        // And a legacy start payload (no turn_id key) round-trips to None.
        let back: StartEvent =
            serde_json::from_value(json!({ "text": "x", "source": "", "ts": 1 })).unwrap();
        assert!(back.turn_id.is_none());
    }

    #[test]
    fn decode_cancel_empty_object() {
        let req = decode_request(incoming::CANCEL, &json!({})).expect("cancel parses");
        assert!(matches!(req, Request::Cancel(_)));
    }

    #[test]
    fn decode_cancel_null() {
        let req =
            decode_request(incoming::CANCEL, &serde_json::Value::Null).expect("cancel parses");
        assert!(matches!(req, Request::Cancel(_)));
    }

    #[test]
    fn decode_reload_voice() {
        let req = decode_request(
            incoming::RELOAD_VOICE,
            &json!({ "voice": "en_GB-jenny" }),
        )
        .expect("reload_voice parses");
        assert_eq!(
            req,
            Request::ReloadVoice(ReloadVoiceRequest {
                voice: "en_GB-jenny".into()
            })
        );
    }

    #[test]
    fn decode_unknown_topic() {
        let result = decode_request("wm.tts.bogus", &json!({}));
        assert!(matches!(result, Err(DecodeError::UnknownTopic(_))));
    }

    #[test]
    fn decode_bad_speak_payload() {
        let result = decode_request(incoming::SPEAK, &json!({ "no_text": true }));
        assert!(matches!(result, Err(DecodeError::Json(_))));
    }

    #[test]
    fn outbound_events_round_trip() {
        let ev = StartEvent {
            text: "hi".into(),
            source: "claude-1-jsy".into(),
            ts: 42,
            turn_id: None,
        };
        let v = serde_json::to_value(&ev).expect("serializes");
        let back: StartEvent = serde_json::from_value(v).expect("round trips");
        assert_eq!(ev, back);

        let ack = CancelAckEvent { ts: 7, drained_ms: 0 };
        let v = serde_json::to_value(&ack).expect("serializes");
        let back: CancelAckEvent = serde_json::from_value(v).expect("round trips");
        assert_eq!(ack, back);

        let reload = ReloadAckEvent {
            voice: "en_GB-jenny".into(),
            cache_hits: 3,
            prerendered: 2,
            failures: 1,
            elapsed_ms: 1280,
            ts: 99,
        };
        let v = serde_json::to_value(&reload).expect("serializes");
        let back: ReloadAckEvent = serde_json::from_value(v).expect("round trips");
        assert_eq!(reload, back);
    }

    #[test]
    fn now_unix_ms_increases() {
        let a = now_unix_ms();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = now_unix_ms();
        assert!(b >= a);
    }

    #[test]
    fn outgoing_all_lists_every_outbound_topic() {
        // Every const in `outgoing` must appear in `ALL`; the
        // self-emitted filter relies on that exhaustiveness to break
        // the wm.tts.error feedback loop. If you add a new outbound
        // topic, add it to `outgoing::ALL` and extend this assertion.
        let expected = [
            outgoing::START,
            outgoing::CANCEL_ACK,
            outgoing::END,
            outgoing::ERROR,
            outgoing::RELOAD_ACK,
        ];
        for t in expected {
            assert!(
                outgoing::ALL.contains(&t),
                "outgoing::ALL is missing {t}"
            );
        }
        assert_eq!(outgoing::ALL.len(), expected.len());
    }

    #[test]
    fn is_self_emitted_topic_flags_every_outbound() {
        // The dispatch-side filter relies on this for every topic the
        // daemon publishes — especially `wm.tts.error`, which is the
        // recursive amplifier.
        assert!(is_self_emitted_topic(outgoing::ERROR));
        assert!(is_self_emitted_topic(outgoing::START));
        assert!(is_self_emitted_topic(outgoing::END));
        assert!(is_self_emitted_topic(outgoing::CANCEL_ACK));
        assert!(is_self_emitted_topic(outgoing::RELOAD_ACK));
    }

    #[test]
    fn is_self_emitted_topic_does_not_flag_inbound() {
        // Inbound requests must NOT be filtered — the filter is only
        // for echoes of our own publishes.
        assert!(!is_self_emitted_topic(incoming::SPEAK));
        assert!(!is_self_emitted_topic(incoming::CANCEL));
        assert!(!is_self_emitted_topic(incoming::RELOAD_VOICE));
    }

    #[test]
    fn is_self_emitted_topic_does_not_flag_unrelated() {
        // Other wm.* topics aren't ours and shouldn't be filtered.
        assert!(!is_self_emitted_topic("wm.audio.speech.start"));
        assert!(!is_self_emitted_topic("wm.dialog.turn"));
        assert!(!is_self_emitted_topic(""));
        // Substring near-misses must NOT match — we filter on equality.
        assert!(!is_self_emitted_topic("wm.tts.error.detail"));
        assert!(!is_self_emitted_topic("wm.tts.err"));
    }
}
