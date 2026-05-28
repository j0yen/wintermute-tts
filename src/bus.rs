//! Agorabus topic + payload schema for `wm-tts`.
//!
//! Subscribed prefix is `wm.tts.`; concrete topics are listed in [`Topic`].
//! Payloads round-trip through `serde_json::Value` because the agorabus
//! [`ServerEvent::data`](agorabus::ServerEvent) is a `Value` — the daemon
//! [`crate::daemon`] decodes per-topic into the request enums below.

use serde::{Deserialize, Serialize};

/// Subscribe prefix that captures all incoming `wm-tts` topics.
pub const TOPIC_PREFIX: &str = "wm.tts.";

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
}

/// Outbound `wm.tts.cancel.ack` payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CancelAckEvent {
    /// Unix milliseconds when cancel was processed.
    pub ts: u64,
    /// Milliseconds of speech already played before cancel landed. iter-4
    /// always reports `0` because no `PipeWire` output is wired yet.
    pub drained_ms: u64,
}

/// Outbound `wm.tts.end` payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EndEvent {
    /// The utterance text.
    pub text: String,
    /// Duration of the rendered audio in milliseconds. iter-4 estimates
    /// from the WAV file size; iter-5 will report measured playback time.
    pub duration_ms: u64,
    /// Unix milliseconds when the utterance completed.
    pub ts: u64,
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
            })
        );
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
