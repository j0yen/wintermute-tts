//! `ElevenLabs` WebSocket streaming primitives. iter-9.
//!
//! Provides the streaming variant of the cloud TTS path: a single text
//! input → an [`mpsc::Receiver`] of audio [`Bytes`] chunks as the WS
//! peer emits them. Connection, send, and recv all run in a spawned
//! task; the caller gets the receiver back synchronously (modulo the
//! handshake's async wait) and consumes chunks as they arrive.
//!
//! Protocol (`ElevenLabs` v1 streaming):
//! 1. Connect `wss://api.elevenlabs.io/v1/text-to-speech/{voice_id}/stream-input?model_id=...&output_format=mp3_44100_128`
//! 2. Send `bos`  `{ "text": " ", "xi_api_key": "...", "voice_settings": {..} }`
//! 3. Send `text` `{ "text": "Hello world. ", "try_trigger_generation": true }`
//! 4. Send `eos`  `{ "text": "" }`
//! 5. Receive audio frames `{ "audio": "<base64-mp3>", "isFinal": <bool> }`
//!    until `isFinal: true` or the peer closes.
//!
//! iter-9 ships the primitives only: connection driver,
//! [`eleven_labs_stream`] entry point, and frame types. iter-10 adds a
//! `CloudSynth::stream_render` trait method in [`crate::cloud`] and
//! wires the receiver to `pw-cat --media-type=audio/mpeg` stdin to
//! realize AC5's ≤400 ms first-audio target. The non-streaming POST
//! path in [`crate::cloud`] remains the default.

#![allow(
    clippy::doc_markdown,
    clippy::doc_lazy_continuation,
    clippy::too_long_first_doc_paragraph,
    clippy::option_if_let_else,
    clippy::too_many_lines,
    clippy::unused_async
)]

use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

use crate::cloud::{CloudConfig, CloudError, DEFAULT_OUTPUT_FORMAT};

/// Buffer depth for the audio-chunk receiver. ElevenLabs sends small
/// MP3 frames (~50-200 bytes every ~30 ms for a typical phrase); 32
/// gives a comfortable headroom against scheduling jitter on the
/// pw-cat-stdin consumer that iter-10 wires.
pub const CHANNEL_DEPTH: usize = 32;

/// WebSocket handshake timeout (DNS + TCP + TLS + HTTP upgrade).
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-frame read timeout. If no frame arrives in this window the task
/// aborts and reports `Http("ws read timeout")` on the receiver.
pub const READ_TIMEOUT: Duration = Duration::from_secs(15);

/// Beginning-of-stream frame. Sent once immediately after the WS
/// handshake completes; carries the API key and voice settings so the
/// upstream backend can authorize and initialize the synth.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct WsBosFrame {
    /// Always a single space per the upstream spec — a real text frame
    /// follows.
    pub text: String,
    /// `xi-api-key` value (the WS protocol carries the key in-band
    /// rather than as a header).
    pub xi_api_key: String,
    /// Voice settings sent with the bos frame.
    pub voice_settings: VoiceSettings,
}

/// Voice settings the bos frame carries. Default values match the
/// `ElevenLabs` "balanced" preset.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct VoiceSettings {
    /// Stability (0-1). Higher = more consistent, less expressive.
    pub stability: f32,
    /// Similarity boost (0-1). Higher = closer to the reference voice.
    pub similarity_boost: f32,
}

impl Default for VoiceSettings {
    fn default() -> Self {
        Self {
            stability: 0.5,
            similarity_boost: 0.75,
        }
    }
}

/// Text content frame. Sent after `bos`; `try_trigger_generation: true`
/// asks the backend to start emitting audio as soon as it has enough
/// input rather than waiting for `eos`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WsTextFrame {
    /// The text to synthesize.
    pub text: String,
    /// Whether the backend may start emitting audio before `eos`.
    pub try_trigger_generation: bool,
}

/// End-of-stream frame. An empty `text` field signals completion.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WsEosFrame {
    /// Always empty per the upstream spec.
    pub text: String,
}

/// Audio response frame. `audio` is optional because the backend can
/// send keepalive/metadata frames with no audio body. `is_final` is
/// optional for the same reason; missing == false.
#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct WsAudioFrame {
    /// Base64-encoded MP3 chunk. `None` on keepalive frames.
    pub audio: Option<String>,
    /// `true` on the last audio frame for the request.
    #[serde(rename = "isFinal", default)]
    pub is_final: Option<bool>,
}

/// Decode a base64 audio payload into raw MP3 bytes.
///
/// # Errors
///
/// Returns [`CloudError::Http`] (with `base64` in the message) when the
/// payload is not valid base64.
pub fn decode_audio(payload: &str) -> Result<Bytes, CloudError> {
    BASE64_STANDARD
        .decode(payload.as_bytes())
        .map(Bytes::from)
        .map_err(|e| CloudError::Http(format!("base64 decode: {e}")))
}

/// Build the WebSocket URL for `voice_id` under `config`. Replaces the
/// `http(s)://` base-URL prefix with `ws(s)://` so test/staging
/// overrides (e.g. `http://localhost:8080`) work without a separate env
/// var for the WS endpoint.
#[must_use]
pub fn streaming_url(config: &CloudConfig, voice_id: &str) -> String {
    let base = if let Some(rest) = config.base_url.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = config.base_url.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        config.base_url.clone()
    };
    format!(
        "{base}/v1/text-to-speech/{voice_id}/stream-input?model_id={model}&output_format={fmt}",
        model = config.model_id,
        fmt = DEFAULT_OUTPUT_FORMAT,
    )
}

/// Open the WebSocket, send `text`, and stream audio chunks back via an
/// [`mpsc::Receiver`]. The receiver is returned immediately; a spawned
/// task drives the WebSocket until `is_final: true` is observed, the
/// peer closes, the read timeout elapses, or the receiver is dropped.
///
/// Channel items are `Result<Bytes, CloudError>`:
/// - `Ok(bytes)` for each non-empty audio chunk in arrival order.
/// - `Err(_)` once, when the connection or a frame fails; no further
///   items follow.
/// After the stream ends the sender is dropped, so the receiver's next
/// `recv().await` returns `None`.
///
/// # Errors
///
/// Returns synchronously only when `config.api_key` or `config.voice_id`
/// is unset (no point opening the connection). All other failures are
/// reported through the receiver as `Err` items so the caller's
/// consumer loop owns one error-handling site.
pub async fn eleven_labs_stream(
    config: &CloudConfig,
    text: &str,
) -> Result<mpsc::Receiver<Result<Bytes, CloudError>>, CloudError> {
    let api_key = config.api_key.clone().ok_or(CloudError::MissingApiKey)?;
    let voice_id = config.voice_id.clone().ok_or(CloudError::MissingVoiceId)?;
    let url = streaming_url(config, &voice_id);
    let text_owned = text.to_string();
    let (tx, rx) = mpsc::channel::<Result<Bytes, CloudError>>(CHANNEL_DEPTH);

    tokio::spawn(async move {
        drive_stream(url, api_key, text_owned, tx).await;
    });

    Ok(rx)
}

/// Drive one WebSocket exchange to completion. Factored out of
/// [`eleven_labs_stream`] so the spawned task body is a single
/// statement and a future iter (testing with a mock socket) can call
/// the logic directly.
async fn drive_stream(
    url: String,
    api_key: String,
    text: String,
    tx: mpsc::Sender<Result<Bytes, CloudError>>,
) {
    let request = match url.into_client_request() {
        Ok(r) => r,
        Err(e) => {
            report(&tx, CloudError::Http(format!("ws request: {e}"))).await;
            return;
        }
    };

    let connect = tokio::time::timeout(HANDSHAKE_TIMEOUT, connect_async(request)).await;
    let (mut ws, _resp) = match connect {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            report(&tx, CloudError::Http(format!("ws connect: {e}"))).await;
            return;
        }
        Err(_) => {
            report(&tx, CloudError::Http("ws handshake timeout".to_string())).await;
            return;
        }
    };

    let bos = WsBosFrame {
        text: " ".to_string(),
        xi_api_key: api_key,
        voice_settings: VoiceSettings::default(),
    };
    let text_frame = WsTextFrame {
        text,
        try_trigger_generation: true,
    };
    let eos = WsEosFrame {
        text: String::new(),
    };

    let outbound = [
        serde_json::to_string(&bos),
        serde_json::to_string(&text_frame),
        serde_json::to_string(&eos),
    ];
    for encoded in outbound {
        let body = match encoded {
            Ok(s) => s,
            Err(e) => {
                report(&tx, CloudError::Http(format!("ws frame encode: {e}"))).await;
                let _ = ws.close(None).await;
                return;
            }
        };
        if let Err(e) = ws.send(Message::Text(body)).await {
            report(&tx, CloudError::Http(format!("ws send: {e}"))).await;
            let _ = ws.close(None).await;
            return;
        }
    }

    loop {
        let next = tokio::time::timeout(READ_TIMEOUT, ws.next()).await;
        let msg = match next {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(e))) => {
                report(&tx, CloudError::Http(format!("ws recv: {e}"))).await;
                return;
            }
            Ok(None) => break,
            Err(_) => {
                report(&tx, CloudError::Http("ws read timeout".to_string())).await;
                return;
            }
        };
        match msg {
            Message::Text(body) => {
                let frame: WsAudioFrame = match serde_json::from_str(body.as_str()) {
                    Ok(f) => f,
                    Err(e) => {
                        report(&tx, CloudError::Http(format!("ws frame decode: {e}"))).await;
                        return;
                    }
                };
                if let Some(b64) = frame.audio.as_deref() {
                    match decode_audio(b64) {
                        Ok(bytes) if !bytes.is_empty() => {
                            if tx.send(Ok(bytes)).await.is_err() {
                                return;
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            report(&tx, e).await;
                            return;
                        }
                    }
                }
                if frame.is_final.unwrap_or(false) {
                    break;
                }
            }
            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
            Message::Close(_) => break,
        }
    }

    let _ = ws.close(None).await;
}

/// Send an error on `tx`, ignoring failure (receiver may be dropped).
async fn report(tx: &mpsc::Sender<Result<Bytes, CloudError>>, err: CloudError) {
    let _ = tx.send(Err(err)).await;
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::float_cmp,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    #[test]
    fn bos_frame_serializes_with_required_fields() {
        let f = WsBosFrame {
            text: " ".to_string(),
            xi_api_key: "sk-test".to_string(),
            voice_settings: VoiceSettings::default(),
        };
        let s = serde_json::to_string(&f).unwrap();
        assert!(s.contains("\"text\":\" \""), "bos text field: {s}");
        assert!(
            s.contains("\"xi_api_key\":\"sk-test\""),
            "bos api key field: {s}"
        );
        assert!(s.contains("\"voice_settings\""), "voice_settings: {s}");
        assert!(s.contains("\"stability\":0.5"), "stability: {s}");
    }

    #[test]
    fn voice_settings_default_matches_balanced_preset() {
        let v = VoiceSettings::default();
        assert_eq!(v.stability, 0.5);
        assert_eq!(v.similarity_boost, 0.75);
    }

    #[test]
    fn text_frame_serializes_trigger() {
        let f = WsTextFrame {
            text: "Hello world.".to_string(),
            try_trigger_generation: true,
        };
        let s = serde_json::to_string(&f).unwrap();
        assert!(s.contains("\"try_trigger_generation\":true"), "{s}");
        assert!(s.contains("\"text\":\"Hello world.\""), "{s}");
    }

    #[test]
    fn eos_frame_is_empty_text() {
        let s = serde_json::to_string(&WsEosFrame {
            text: String::new(),
        })
        .unwrap();
        assert_eq!(s, "{\"text\":\"\"}");
    }

    #[test]
    fn audio_frame_decodes_full_message() {
        let body = r#"{"audio":"aGVsbG8=","isFinal":true}"#;
        let f: WsAudioFrame = serde_json::from_str(body).unwrap();
        assert_eq!(f.audio.as_deref(), Some("aGVsbG8="));
        assert_eq!(f.is_final, Some(true));
        let payload = f.audio.unwrap();
        let bytes = decode_audio(&payload).unwrap();
        assert_eq!(bytes.as_ref(), b"hello");
    }

    #[test]
    fn audio_frame_tolerates_missing_is_final() {
        let body = r#"{"audio":"AAA="}"#;
        let f: WsAudioFrame = serde_json::from_str(body).unwrap();
        assert!(f.audio.is_some());
        assert_eq!(f.is_final, None);
    }

    #[test]
    fn audio_frame_tolerates_keepalive_with_no_audio() {
        let body = r#"{"isFinal":false}"#;
        let f: WsAudioFrame = serde_json::from_str(body).unwrap();
        assert!(f.audio.is_none());
        assert_eq!(f.is_final, Some(false));
    }

    #[test]
    fn decode_audio_rejects_invalid_base64() {
        let err = decode_audio("not!valid!base64!").unwrap_err();
        match err {
            CloudError::Http(msg) => assert!(msg.contains("base64"), "msg: {msg}"),
            other => panic!("expected Http, got {other:?}"),
        }
    }

    fn cfg_with(base: &str, model: &str, key: Option<&str>, voice: Option<&str>) -> CloudConfig {
        CloudConfig {
            enabled: true,
            api_key: key.map(str::to_string),
            voice_id: voice.map(str::to_string),
            model_id: model.to_string(),
            base_url: base.to_string(),
        }
    }

    #[test]
    fn streaming_url_replaces_https_with_wss() {
        let cfg = cfg_with(
            "https://api.elevenlabs.io",
            "eleven_turbo_v2",
            Some("k"),
            Some("v-abc"),
        );
        let url = streaming_url(&cfg, "v-abc");
        assert!(
            url.starts_with("wss://api.elevenlabs.io/v1/text-to-speech/v-abc/stream-input"),
            "url: {url}"
        );
        assert!(url.contains("model_id=eleven_turbo_v2"), "url: {url}");
        assert!(url.contains("output_format=mp3_44100_128"), "url: {url}");
    }

    #[test]
    fn streaming_url_replaces_http_with_ws() {
        let cfg = cfg_with("http://localhost:8080", "m", Some("k"), Some("v-abc"));
        let url = streaming_url(&cfg, "v-abc");
        assert!(
            url.starts_with("ws://localhost:8080/v1/text-to-speech/v-abc/stream-input"),
            "url: {url}"
        );
    }

    #[test]
    fn streaming_url_passes_through_already_ws_base() {
        let cfg = cfg_with("wss://stage.example", "m", Some("k"), Some("v"));
        let url = streaming_url(&cfg, "v");
        assert!(url.starts_with("wss://stage.example/v1/"), "url: {url}");
    }

    #[tokio::test]
    async fn stream_returns_missing_api_key_synchronously() {
        let cfg = cfg_with("https://api.elevenlabs.io", "m", None, Some("v"));
        let err = eleven_labs_stream(&cfg, "hi").await.unwrap_err();
        assert!(matches!(err, CloudError::MissingApiKey));
    }

    #[tokio::test]
    async fn stream_returns_missing_voice_id_synchronously() {
        let cfg = cfg_with("https://api.elevenlabs.io", "m", Some("k"), None);
        let err = eleven_labs_stream(&cfg, "hi").await.unwrap_err();
        assert!(matches!(err, CloudError::MissingVoiceId));
    }

    #[tokio::test]
    async fn stream_reports_connect_failure_through_receiver() {
        // 127.0.0.1:1 — IANA-reserved port that nothing listens on.
        // The handshake should fail fast and surface CloudError::Http
        // on the receiver rather than synchronously.
        let cfg = cfg_with("http://127.0.0.1:1", "m", Some("k"), Some("v"));
        let mut rx = eleven_labs_stream(&cfg, "hi")
            .await
            .expect("synchronous setup ok");
        let item = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("receiver should yield within 2s")
            .expect("an item is sent before drop");
        let err = item.expect_err("connect to dead port must fail");
        match err {
            CloudError::Http(msg) => assert!(
                msg.contains("connect") || msg.contains("timeout"),
                "msg: {msg}"
            ),
            other => panic!("expected Http, got {other:?}"),
        }
    }
}
