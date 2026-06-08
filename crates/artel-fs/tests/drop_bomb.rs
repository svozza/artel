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

mod common;

use std::path::PathBuf;
use std::process::Command;

use iroh_relay::server::Server as RelayServer;
use tokio::sync::OnceCell;

const MARKER: &str = "[artel-fs] Workspace dropped without calling shutdown()";

static SHARED_RELAY: OnceCell<(RelayServer, String)> = OnceCell::const_new();

async fn shared_relay_url() -> &'static str {
    &SHARED_RELAY
        .get_or_init(|| async {
            let (_relay_map, relay_url, server) = iroh::test_utils::run_relay_server()
                .await
                .expect("run_relay_server for drop-bomb tests");
            (server, relay_url.to_string())
        })
        .await
        .1
}

/// Bin-local thin wrapper over [`common::LocalDaemon`]. Pre-A2 the
/// harness ran the daemon with the iroh runtime disabled; post-A2
/// every daemon binds an `Endpoint`, so this hands off to the
/// shared in-process [`iroh::test_utils::DnsPkarrServer`] fixture.
struct DaemonHarness {
    inner: common::LocalDaemon,
    socket: PathBuf,
}

impl DaemonHarness {
    async fn spawn() -> Self {
        let inner = common::LocalDaemon::spawn().await;
        let socket = inner.socket.clone();
        Self { inner, socket }
    }

    async fn stop(self) {
        self.inner.stop().await;
    }
}

async fn run_child_against(harness: &DaemonHarness, mode: &str) -> String {
    let relay_url = shared_relay_url().await.to_string();
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
            .env("ARTEL_RELAY_URL", &relay_url)
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
