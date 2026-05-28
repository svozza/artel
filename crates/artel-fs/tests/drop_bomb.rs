//! `Workspace::Drop` must scream when the workspace is dropped
//! without an `await`ed [`artel_fs::Workspace::shutdown`] first.
//! See the `Workspace` struct's "Shutdown contract" section for why
//! the contract matters (n0 relay rejects same-`EndpointId`
//! reconnect; next host hangs in `Endpoint::online`).
//!
//! Two assertions, exercised via the `drop_bomb_child` helper bin:
//!
//! - **Bomb fires when caller misuses the API.** `--mode ungraceful`
//!   → child stderr contains the `[artel-fs] Workspace dropped
//!   without calling shutdown()` marker.
//! - **Bomb stays quiet on the happy path.** `--mode graceful` →
//!   child stderr does not contain the marker.
//!
//! We pin the eprintln channel rather than the tracing channel
//! because the eprintln is the belt-and-braces fallback that runs
//! regardless of whether a tracing subscriber is installed in the
//! embedding binary.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_protocol::PeerId;
use tempfile::TempDir;
use tokio::time::timeout;

const MARKER: &str = "[artel-fs] Workspace dropped without calling shutdown()";

struct DaemonHarness {
    _tempdir: TempDir,
    socket: PathBuf,
    shutdown: Arc<Shutdown>,
    join: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl DaemonHarness {
    async fn spawn() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("daemon.sock");
        let pid = tempdir.path().join("daemon.pid");
        let daemon = Daemon::start(DaemonConfig {
            socket_path: socket.clone(),
            pid_path: pid,
            sessions_dir: tempdir.path().join("sessions"),
            daemon_peer_id: PeerId::from_bytes([0xee; 32]),
            iroh_key_path: None,
            endpoint_setup: artel_daemon::EndpointSetup::Production,
        })
        .await
        .expect("daemon start");
        let shutdown = daemon.shutdown_handle();
        let join = tokio::spawn(daemon.run());
        Self {
            _tempdir: tempdir,
            socket,
            shutdown,
            join,
        }
    }

    async fn stop(self) {
        self.shutdown.trigger();
        timeout(Duration::from_secs(5), self.join)
            .await
            .expect("daemon did not exit within 5s")
            .expect("daemon panicked")
            .expect("daemon io");
    }
}

async fn run_child_against(harness: &DaemonHarness, mode: &str) -> String {
    let exe = env!("CARGO_BIN_EXE_drop_bomb_child");
    let ws_root = tempfile::tempdir().unwrap();
    let ws_state = tempfile::tempdir().unwrap();
    let exe = exe.to_string();
    let socket = harness.socket.clone();
    let mode_owned = mode.to_string();
    let ws_root_path = ws_root.path().to_path_buf();
    let ws_state_path = ws_state.path().to_path_buf();
    let mode_for_child = mode_owned.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(&exe)
            .args([
                "--socket",
                socket.to_str().expect("socket utf8"),
                "--root",
                ws_root_path.to_str().expect("root utf8"),
                "--state-dir",
                ws_state_path.to_str().expect("state-dir utf8"),
                "--mode",
                &mode_for_child,
            ])
            .output()
            .expect("spawn drop_bomb_child")
    })
    .await
    .expect("spawn_blocking join");
    assert!(
        output.status.success(),
        "drop_bomb_child --mode {mode_owned} exited non-zero: status={:?}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );
    drop(ws_root);
    drop(ws_state);
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn bomb_fires_when_workspace_dropped_without_shutdown() {
    let harness = DaemonHarness::spawn().await;
    let stderr = run_child_against(&harness, "ungraceful").await;
    harness.stop().await;
    assert!(
        stderr.contains(MARKER),
        "expected drop bomb marker in stderr, got:\n{stderr}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bomb_quiet_after_graceful_shutdown() {
    let harness = DaemonHarness::spawn().await;
    let stderr = run_child_against(&harness, "graceful").await;
    harness.stop().await;
    assert!(
        !stderr.contains(MARKER),
        "drop bomb fired on graceful path, stderr:\n{stderr}",
    );
}
