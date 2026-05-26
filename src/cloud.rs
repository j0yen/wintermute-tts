//! `ElevenLabs` cloud TTS opt-in.
//!
//! `wm-tts` defaults to local `Piper` synthesis. When the caregiver opts
//! in to higher-quality cloud TTS at bootstrap (`WM_CLOUD_TTS_QUALITY=true`
//! plus an API key + voice id), the daemon tries the cloud first and
//! falls back to `Piper` on any failure (network, auth, rate limit, etc.).
//!
//! The cloud backend in this iter (iter-8) is a non-streaming HTTP call:
//! POST `/v1/text-to-speech/{voice_id}?output_format=mp3_44100_128`
//! with `xi-api-key` header and `{ "text", "model_id" }` body, returning
//! MP3 bytes. Streaming via WebSocket lands in iter-9; that's where
//! AC5's 400 ms first-audio target becomes realistic over broadband.
//! The fallback wiring satisfies AC6.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

/// Env var: enable the cloud path. Accepts `1`, `true`, `yes`, `on`
/// (case-insensitive). Anything else (including unset) leaves cloud off.
pub const ENV_ENABLED: &str = "WM_CLOUD_TTS_QUALITY";
/// Env var: `ElevenLabs` API key (`xi-api-key` header value).
pub const ENV_API_KEY: &str = "WM_TTS_CLOUD_API_KEY";
/// Env var: cloud voice id (distinct from the local `Piper` voice).
pub const ENV_VOICE_ID: &str = "WM_TTS_VOICE_ID_CLOUD";
/// Env var: model id passed to the cloud backend.
pub const ENV_MODEL_ID: &str = "WM_TTS_CLOUD_MODEL";
/// Env var: override the `ElevenLabs` base URL (test/staging endpoints).
pub const ENV_BASE_URL: &str = "WM_TTS_CLOUD_BASE_URL";

/// Default `ElevenLabs` model when `WM_TTS_CLOUD_MODEL` is unset.
pub const DEFAULT_MODEL_ID: &str = "eleven_monolingual_v1";
/// Default base URL when `WM_TTS_CLOUD_BASE_URL` is unset.
pub const DEFAULT_BASE_URL: &str = "https://api.elevenlabs.io";
/// Output format requested from the cloud API. MP3 128 kbps at 44.1 kHz
/// is the most broadly compatible non-streaming format the API exposes;
/// `pw-cat` plays it with `--media-type=audio/mpeg`.
pub const DEFAULT_OUTPUT_FORMAT: &str = "mp3_44100_128";

/// Per-request HTTP timeout. `ElevenLabs` typically returns short
/// utterances in <2 s; a 10 s ceiling protects the daemon if the
/// network stalls (Piper fallback takes over on timeout).
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Cloud TTS configuration. Loaded from environment at daemon startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudConfig {
    /// `WM_CLOUD_TTS_QUALITY` was set to a truthy value.
    pub enabled: bool,
    /// `xi-api-key` value. `None` means the cloud path is unusable
    /// regardless of `enabled`.
    pub api_key: Option<String>,
    /// Cloud voice id. `None` means the cloud path is unusable.
    pub voice_id: Option<String>,
    /// Model id sent to the API.
    pub model_id: String,
    /// Base URL (no trailing slash).
    pub base_url: String,
}

impl Default for CloudConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_key: None,
            voice_id: None,
            model_id: DEFAULT_MODEL_ID.to_string(),
            base_url: DEFAULT_BASE_URL.to_string(),
        }
    }
}

impl CloudConfig {
    /// Read configuration from environment. Missing or empty vars fall
    /// to defaults; trailing slashes on the base URL are trimmed.
    #[must_use]
    pub fn from_env() -> Self {
        let enabled = std::env::var(ENV_ENABLED).is_ok_and(|v| is_truthy(&v));
        let api_key = nonempty_env(ENV_API_KEY);
        let voice_id = nonempty_env(ENV_VOICE_ID);
        let model_id = nonempty_env(ENV_MODEL_ID).unwrap_or_else(|| DEFAULT_MODEL_ID.to_string());
        let base_url = nonempty_env(ENV_BASE_URL)
            .map_or_else(|| DEFAULT_BASE_URL.to_string(), |raw| trim_trailing_slash(&raw).to_string());
        Self {
            enabled,
            api_key,
            voice_id,
            model_id,
            base_url,
        }
    }

    /// True if the cloud path is enabled AND fully credentialed.
    /// `DaemonState` uses this gate to decide whether to attempt cloud
    /// before falling back to Piper.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.enabled && self.api_key.is_some() && self.voice_id.is_some()
    }
}

fn nonempty_env(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => Some(v),
        _ => None,
    }
}

fn is_truthy(v: &str) -> bool {
    matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
}

fn trim_trailing_slash(s: &str) -> &str {
    s.trim_end_matches('/')
}

/// Errors raised by a [`CloudSynth`] implementation.
#[derive(Debug, thiserror::Error)]
pub enum CloudError {
    /// Cloud path is not enabled (or not configured).
    #[error("cloud TTS not enabled or unconfigured")]
    NotEnabled,
    /// Required api key missing — the factory generally short-circuits
    /// to [`DisabledCloudSynth`], but a backend can still raise this
    /// (e.g., key removed at runtime).
    #[error("missing ElevenLabs api key")]
    MissingApiKey,
    /// Required voice id missing.
    #[error("missing cloud voice id")]
    MissingVoiceId,
    /// Transport-layer failure (DNS, TLS, connect, timeout).
    #[error("cloud http transport: {0}")]
    Http(String),
    /// HTTP response with a non-success status.
    #[error("cloud http status {status}: {body}")]
    Status {
        /// HTTP status code.
        status: u16,
        /// First 256 chars of the response body for diagnostics.
        body: String,
    },
    /// Server returned 200 but the body was empty.
    #[error("cloud returned empty body")]
    Empty,
}

/// Abstraction over a cloud TTS backend. Async + Send so the daemon can
/// await it from a tokio runtime.
#[async_trait]
pub trait CloudSynth: Send + Sync {
    /// Render `text` to audio bytes (format defined by the backend; for
    /// `ElevenLabsCloudSynth` this is MP3 per `DEFAULT_OUTPUT_FORMAT`).
    ///
    /// # Errors
    ///
    /// Returns [`CloudError::NotEnabled`] when the backend is the
    /// disabled stub; otherwise transport, status, or empty-body errors.
    async fn render(&self, text: &str) -> Result<Bytes, CloudError>;

    /// True if this backend is actually configured to make calls.
    /// `DaemonState::render_on_demand` checks this before attempting
    /// cloud render.
    fn is_active(&self) -> bool;
}

/// Always-fails backend used when cloud is disabled or unconfigured.
/// `render_on_demand` short-circuits via [`CloudSynth::is_active`]
/// before ever calling this, but the stub remains correct on its own.
#[derive(Debug, Default, Clone, Copy)]
pub struct DisabledCloudSynth;

#[async_trait]
impl CloudSynth for DisabledCloudSynth {
    async fn render(&self, _text: &str) -> Result<Bytes, CloudError> {
        Err(CloudError::NotEnabled)
    }

    fn is_active(&self) -> bool {
        false
    }
}

/// `ElevenLabs` HTTP backend. Holds a shared reqwest client (connection
/// pool) and the resolved config.
#[derive(Debug, Clone)]
pub struct ElevenLabsCloudSynth {
    config: CloudConfig,
    client: reqwest::Client,
}

impl ElevenLabsCloudSynth {
    /// Build a new backend with a pre-configured reqwest client.
    ///
    /// # Errors
    /// Returns the underlying reqwest error if the client cannot be
    /// constructed (e.g., TLS backend init failure).
    pub fn new(config: CloudConfig) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .user_agent("wm-tts/0.1")
            .build()?;
        Ok(Self { config, client })
    }

    fn endpoint(&self, voice_id: &str) -> String {
        format!(
            "{}/v1/text-to-speech/{}?output_format={}",
            self.config.base_url, voice_id, DEFAULT_OUTPUT_FORMAT
        )
    }
}

#[async_trait]
impl CloudSynth for ElevenLabsCloudSynth {
    async fn render(&self, text: &str) -> Result<Bytes, CloudError> {
        let api_key = self
            .config
            .api_key
            .as_deref()
            .ok_or(CloudError::MissingApiKey)?;
        let voice_id = self
            .config
            .voice_id
            .as_deref()
            .ok_or(CloudError::MissingVoiceId)?;

        let url = self.endpoint(voice_id);
        let body = serde_json::json!({
            "text": text,
            "model_id": self.config.model_id,
        });
        let res = self
            .client
            .post(&url)
            .header("xi-api-key", api_key)
            .header("accept", "audio/mpeg")
            .json(&body)
            .send()
            .await
            .map_err(|e| CloudError::Http(e.to_string()))?;

        let status = res.status();
        if !status.is_success() {
            let body_text = res.text().await.unwrap_or_default();
            return Err(CloudError::Status {
                status: status.as_u16(),
                body: truncate(&body_text, 256),
            });
        }
        let bytes = res
            .bytes()
            .await
            .map_err(|e| CloudError::Http(e.to_string()))?;
        if bytes.is_empty() {
            return Err(CloudError::Empty);
        }
        Ok(bytes)
    }

    fn is_active(&self) -> bool {
        self.config.is_active()
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        s.chars().take(max).collect()
    }
}

/// Build the cloud backend the daemon should use.
///
/// Driven by [`CloudConfig::from_env`]: returns a [`DisabledCloudSynth`]
/// when cloud is off or any required env var is missing; otherwise wraps
/// [`ElevenLabsCloudSynth`].
///
/// A failure to construct the reqwest client (very rare) also yields a
/// disabled backend with a warning logged via `tracing::warn`.
#[must_use]
pub fn cloud_synth_from_env() -> Arc<dyn CloudSynth> {
    let cfg = CloudConfig::from_env();
    if !cfg.is_active() {
        return Arc::new(DisabledCloudSynth);
    }
    match ElevenLabsCloudSynth::new(cfg) {
        Ok(backend) => Arc::new(backend),
        Err(e) => {
            tracing::warn!(error = %e, "wm-tts: cloud client init failed; falling back to disabled");
            Arc::new(DisabledCloudSynth)
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    unsafe_code
)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Env vars are process-global; serialize tests that touch them.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        keys: Vec<String>,
    }

    impl EnvGuard {
        fn set(pairs: &[(&str, &str)]) -> Self {
            let keys: Vec<String> = pairs.iter().map(|(k, _)| (*k).to_string()).collect();
            // Clear all related vars first to avoid leakage from a
            // previous test.
            for k in [ENV_ENABLED, ENV_API_KEY, ENV_VOICE_ID, ENV_MODEL_ID, ENV_BASE_URL] {
                // SAFETY: tests serialize through ENV_LOCK; no other
                // thread mutates env in parallel.
                unsafe { std::env::remove_var(k) };
            }
            for (k, v) in pairs {
                // SAFETY: see above.
                unsafe { std::env::set_var(k, v) };
            }
            Self { keys }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for k in &self.keys {
                // SAFETY: tests serialize through ENV_LOCK.
                unsafe { std::env::remove_var(k) };
            }
        }
    }

    #[test]
    fn config_from_env_defaults_to_disabled() {
        let _l = ENV_LOCK.lock().unwrap();
        let _g = EnvGuard::set(&[]);
        let cfg = CloudConfig::from_env();
        assert!(!cfg.enabled);
        assert!(cfg.api_key.is_none());
        assert!(cfg.voice_id.is_none());
        assert_eq!(cfg.model_id, DEFAULT_MODEL_ID);
        assert_eq!(cfg.base_url, DEFAULT_BASE_URL);
        assert!(!cfg.is_active());
    }

    #[test]
    fn config_from_env_parses_all_fields() {
        let _l = ENV_LOCK.lock().unwrap();
        let _g = EnvGuard::set(&[
            (ENV_ENABLED, "TRUE"),
            (ENV_API_KEY, "sk-test"),
            (ENV_VOICE_ID, "v-abc"),
            (ENV_MODEL_ID, "eleven_turbo_v2"),
            (ENV_BASE_URL, "https://stage.example/"),
        ]);
        let cfg = CloudConfig::from_env();
        assert!(cfg.enabled);
        assert_eq!(cfg.api_key.as_deref(), Some("sk-test"));
        assert_eq!(cfg.voice_id.as_deref(), Some("v-abc"));
        assert_eq!(cfg.model_id, "eleven_turbo_v2");
        // Trailing slash trimmed.
        assert_eq!(cfg.base_url, "https://stage.example");
        assert!(cfg.is_active());
    }

    #[test]
    fn config_enabled_without_credentials_is_not_active() {
        let _l = ENV_LOCK.lock().unwrap();
        let _g = EnvGuard::set(&[(ENV_ENABLED, "1")]);
        let cfg = CloudConfig::from_env();
        assert!(cfg.enabled);
        assert!(!cfg.is_active());
    }

    #[test]
    fn truthy_variants_recognized() {
        for v in ["1", "true", "TRUE", "True", "yes", "YES", "on"] {
            assert!(is_truthy(v), "expected truthy: {v:?}");
        }
        for v in ["", "0", "false", "no", "off", "maybe"] {
            assert!(!is_truthy(v), "expected falsy: {v:?}");
        }
    }

    #[tokio::test]
    async fn disabled_returns_not_enabled() {
        let backend = DisabledCloudSynth;
        let err = backend.render("hello").await.expect_err("must error");
        assert!(matches!(err, CloudError::NotEnabled));
        assert!(!backend.is_active());
    }

    #[test]
    fn elevenlabs_constructs_and_reports_active() {
        let cfg = CloudConfig {
            enabled: true,
            api_key: Some("sk-test".into()),
            voice_id: Some("v-abc".into()),
            model_id: DEFAULT_MODEL_ID.to_string(),
            base_url: DEFAULT_BASE_URL.to_string(),
        };
        let backend = ElevenLabsCloudSynth::new(cfg).expect("client builds");
        assert!(backend.is_active());
        let url = backend.endpoint("v-abc");
        assert!(url.starts_with("https://api.elevenlabs.io/v1/text-to-speech/v-abc"));
        assert!(url.contains("output_format=mp3_44100_128"));
    }

    #[test]
    fn factory_returns_disabled_when_env_missing() {
        let _l = ENV_LOCK.lock().unwrap();
        let _g = EnvGuard::set(&[]);
        let backend = cloud_synth_from_env();
        assert!(!backend.is_active());
    }
}
