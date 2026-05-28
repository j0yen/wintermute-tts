//! Bus-smoke regression test for the announce-before-subscribe protocol
//! bug class (PRD-wintermute-fleet-bus-smoke-convention.md).
//!
//! Spawns an in-process `agorabus` daemon on a temp socket, points the
//! `wm-tts` daemon at it via the `WM_TTS_BUS_SOCKET` env override,
//! publishes a `wm.tts.cancel`, and asserts the daemon stays up and
//! emits a `wm.tts.cancel.ack` event back through the real bus. A
//! daemon that connected without announcing would have been torn down
//! by agorabus with `announce_required` before it ever saw the cancel,
//! so a received ack is positive evidence that the
//! `connect()` → `announce()` → `subscribe()` ordering is correct.
//!
//! Cancel is used as the smoke driver because it doesn't require Piper
//! or `pw-cat` (cache-hit / render paths are skipped), so the test
//! exercises pure bus wire-up in a clean-room environment.

#![allow(
    unsafe_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::missing_panics_doc,
    clippy::missing_assert_message,
    clippy::missing_errors_doc
)]

use std::path::PathBuf;
use std::time::Duration;

use agorabus::{Client, DaemonConfig, run_daemon};
use tokio::time::timeout;

fn tmp_path(tag: &str, ext: &str) -> PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    // agorabus chmods the socket parent to 0700 on bind; pointing at
    // /tmp directly silently goes wrong. Use a fresh pid+nanos subdir.
    let dir = std::env::temp_dir().join(format!("wm-tts-test-{pid}-{nanos}"));
    let _ = std::fs::create_dir_all(&dir);
    dir.join(format!("{tag}.{ext}"))
}

async fn run_bus_smoke() -> Result<(), String> {
    // 1. Spawn an in-process agorabus on a unique temp socket.
    let bus_sock = tmp_path("bus", "sock");
    let _ = std::fs::remove_file(&bus_sock);
    let bus_cfg = DaemonConfig {
        socket_path: bus_sock.clone(),
        heartbeat_timeout: Duration::from_secs(60),
        broadcast_capacity: 1024,
    };
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
    let (bus_shutdown_tx, bus_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let bus_task = tokio::spawn(async move {
        let _ = run_daemon(bus_cfg, Some(ready_tx), bus_shutdown_rx).await;
    });
    timeout(Duration::from_secs(2), ready_rx)
        .await
        .map_err(|_| "bus never signalled ready".to_string())?
        .map_err(|e| format!("bus ready_tx dropped: {e}"))?;

    // 2. Subscribe BEFORE the wm-tts daemon starts so the broadcast
    //    channel can't race past us. Announce first — positive
    //    evidence the test author understood the ordering (AC7
    //    anti-cargo-cult gate).
    let mut subscriber = Client::connect(&bus_sock)
        .await
        .map_err(|e| format!("subscriber connect: {e:#}"))?;
    subscriber
        .announce(
            "wm-tts-bus-smoke-sub",
            std::process::id(),
            "",
            "test-subscriber",
        )
        .await
        .map_err(|e| format!("subscriber announce: {e:#}"))?;
    subscriber
        .subscribe("wm.tts.")
        .await
        .map_err(|e| format!("subscriber subscribe: {e:#}"))?;

    // 3. Minimal cache YAML — one phrase to satisfy the non-empty
    //    validator in `parse_cache_yaml`. Pre-render will attempt to
    //    spawn Piper for this phrase; in CI Piper isn't installed and
    //    `synth.render` returns `SynthError` per-phrase, which
    //    `prerender` records in `report.failures` rather than
    //    bubbling. The dispatch loop then runs as normal.
    let yaml_path = tmp_path("tts-cache", "yaml");
    std::fs::write(&yaml_path, "phrases:\n  - bus-smoke-probe\n")
        .map_err(|e| format!("write cache yaml: {e}"))?;

    // 4. Point the wm-tts daemon at our temp bus socket. The env var
    //    override matches the `WM_TTS_VOICE`/`WM_TTS_PLAYER` idiom
    //    already in the crate. SAFETY: tests in this file are the
    //    only consumer of this var; cargo runs separate test
    //    binaries in separate processes so cross-file env races are
    //    impossible. Intra-file there's only this one test fn.
    let bus_sock_for_env = bus_sock.clone();
    // SAFETY: see comment above.
    unsafe {
        std::env::set_var("WM_TTS_BUS_SOCKET", &bus_sock_for_env);
    }

    // 5. Spawn the wm-tts daemon. It will announce + subscribe to
    //    `wm.tts.` on the temp bus. The daemon exits cleanly when the
    //    bus closes (next_event returns None).
    let yaml_for_daemon = yaml_path.clone();
    let daemon_task = tokio::spawn(async move {
        wintermute_tts::daemon::run(&yaml_for_daemon).await
    });

    // 6. Give the daemon time to connect + announce + subscribe.
    //    Polling the bus's peer list would be cleaner; agorabus
    //    doesn't expose it through the Client API, so we use a
    //    bounded sleep matching wm-audio's pattern.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 7. Publish a `wm.tts.cancel` from a separate connection.
    //    Announce-first, as always. Empty payload — `decode_request`
    //    accepts `{}` or `null` for cancel.
    let mut publisher = Client::connect(&bus_sock)
        .await
        .map_err(|e| format!("publisher connect: {e:#}"))?;
    publisher
        .announce(
            "wm-tts-bus-smoke-pub",
            std::process::id(),
            "",
            "test-publisher",
        )
        .await
        .map_err(|e| format!("publisher announce: {e:#}"))?;
    publisher
        .publish("wm.tts.cancel", serde_json::json!({}))
        .await
        .map_err(|e| format!("publisher publish: {e:#}"))?;

    // 8. Drain subscriber for the cancel.ack. AC2 requires at least
    //    one publish-through; cancel.ack is the cheap one to obtain
    //    because handle_cancel has no Piper / pw-cat dependency.
    let collect_deadline = Duration::from_secs(10);
    let per_event_quiet = Duration::from_secs(2);
    let mut saw_cancel_ack = false;
    let collect_result = timeout(collect_deadline, async {
        loop {
            match timeout(per_event_quiet, subscriber.next_event()).await {
                Ok(Ok(Some(ev))) => {
                    // The daemon may also publish wm.tts.cancel
                    // itself when it broadcasts its own subscription
                    // back through us — we only assert on cancel.ack.
                    if ev.topic == "wm.tts.cancel.ack" {
                        saw_cancel_ack = true;
                        break;
                    }
                }
                Ok(Ok(None)) => return Err("bus closed before cancel.ack".to_string()),
                Ok(Err(e)) => return Err(format!("next_event: {e:#}")),
                Err(_) => break, // quiet long enough; bail out and let the assertion fire
            }
        }
        Ok(saw_cancel_ack)
    })
    .await;

    // 9. Tear down regardless of outcome — never leak the daemon task
    //    or the bus task. Order: drop the publisher (closes its UDS),
    //    shut down the bus (daemon's next_event returns None, daemon
    //    exits), await both tasks with a deadline.
    drop(publisher);
    drop(subscriber);
    let _ = bus_shutdown_tx.send(());
    let _ = timeout(Duration::from_secs(3), bus_task).await;
    let daemon_outcome = timeout(Duration::from_secs(3), daemon_task).await;
    let _ = std::fs::remove_file(&bus_sock);
    let _ = std::fs::remove_file(&yaml_path);
    // SAFETY: same single-test-consumer reasoning as the set_var
    // above. Removing the var so any later test in the same binary
    // sees a clean env.
    unsafe {
        std::env::remove_var("WM_TTS_BUS_SOCKET");
    }

    // 10. The implicit anti-announce_required check: if the daemon
    //     had failed at announce, it would have exited within ~1s
    //     of contacting the bus and the cancel.ack would never have
    //     arrived. Verify the daemon's task actually completed (or
    //     timed out cleanly) and surface its anyhow chain if so.
    if let Ok(Ok(Err(daemon_err))) = daemon_outcome {
        let chain = format!("{daemon_err:#}");
        if chain.contains("announce_required") {
            return Err(format!(
                "daemon hit announce_required — bus wire-up regression: {chain}"
            ));
        }
        return Err(format!("daemon exited with error: {chain}"));
    }

    let got_ack = collect_result
        .map_err(|_| "timed out collecting cancel.ack".to_string())??;
    if !got_ack {
        return Err("no wm.tts.cancel.ack event observed within deadline".to_string());
    }
    Ok(())
}

#[test]
fn wm_tts_bus_smoke_announces_before_subscribe() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .expect("build tokio runtime");
    rt.block_on(async {
        run_bus_smoke().await.expect("wm-tts bus smoke lifecycle");
    });
}
