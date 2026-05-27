//! Acceptance AC8 — voicepack resolver is present in-repo, peon-ping
//! integration doc exists as fixture.
//!
//! Per PRD §2.5 + intent-card resolution: the resolver lives in this
//! crate until peon-ping PRD-003 needs it; this test pins both halves
//! of that contract so a future refactor can't silently drop either.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc
)]

use std::path::{Path, PathBuf};

use wintermute_tts::voicepack::{resolve, Backend};

#[test]
fn voicepack_resolver_present() {
    let backend = resolve("en_US-lessac-medium").expect("bare name must resolve to Piper");
    assert_eq!(
        backend,
        Backend::Piper {
            model_path: PathBuf::from("en_US-lessac-medium.onnx"),
        },
        "AC8: resolver maps the default Piper voice to a Piper backend"
    );

    let cloud = resolve("cloud:21m00Tcm4TlvDq8ikWAM").expect("cloud: prefix must resolve");
    assert!(
        matches!(cloud, Backend::ElevenLabs { ref voice_id } if voice_id == "21m00Tcm4TlvDq8ikWAM"),
        "AC8: resolver maps cloud: prefix to ElevenLabs backend"
    );

    let espeak = resolve("espeak:-v en-us").expect("espeak: prefix must resolve");
    assert!(
        matches!(espeak, Backend::EspeakNg { ref args } if args == &["-v".to_string(), "en-us".to_string()]),
        "AC8: resolver maps espeak: prefix to EspeakNg backend"
    );
}

#[test]
fn peon_ping_integration_doc_present() {
    let doc = Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/PEON_PING_INTEGRATION.md");
    let body = std::fs::read_to_string(&doc).unwrap_or_else(|err| {
        panic!(
            "AC8 fixture {} must exist (peon-ping coordination note): {err}",
            doc.display()
        )
    });
    assert!(
        body.contains("voice-pack resolver"),
        "AC8: integration doc must reference the voice-pack resolver contract"
    );
    assert!(
        body.contains("peon-ping"),
        "AC8: integration doc must name peon-ping as the sibling consumer"
    );
}
