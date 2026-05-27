//! `Workspace::host` honours its [`AttachPolicy`].
//!
//! Three properties pinned here:
//!
//! 1. `RequireEmpty` against a non-empty workspace root rejects with
//!    [`WorkspaceError::Policy`] **before** any iroh state lands on
//!    disk. We assert by checking the would-be `state_dir` is absent
//!    after the failed call — a regression that spawned the iroh
//!    node before the policy check would leave `iroh.key` and
//!    `doc-id` behind.
//! 2. `AllowExisting` against the same dir succeeds and adopts the
//!    pre-existing files into the doc.
//! 3. `RequireEmpty` against a freshly-empty dir succeeds — proves
//!    the rejection is precisely scoped to non-emptiness.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_fs::{AttachPolicy, PolicyViolation, Workspace, WorkspaceConfig, WorkspaceError};
use artel_protocol::{PeerId, PeerInfo};
use iroh_docs::store::Query;
use tempfile::TempDir;

/// Single-daemon harness — these tests don't need cross-daemon
/// addressing because they either error before any iroh work or only
/// exercise the host's local doc.
struct DaemonHarness {
    _tempdir: TempDir,
    socket: PathBuf,
    shutdown: Arc<Shutdown>,
    join: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl DaemonHarness {
    async fn spawn() -> Self {
        let tempdir = TempDir::new().unwrap();
        let socket = tempdir.path().join("daemon.sock");
        let pid = tempdir.path().join("daemon.pid");
        let sessions = tempdir.path().join("sessions");
        let daemon = Daemon::start(DaemonConfig {
            socket_path: socket.clone(),
            pid_path: pid,
            sessions_dir: sessions,
            daemon_peer_id: PeerId::from_bytes([0xee; 32]),
            iroh_key_path: None,
            address_lookup: None,
        })
        .await
        .expect("daemon start");
        let shutdown = daemon.shutdown_handle();
        let socket = daemon.socket_path().to_path_buf();
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
        let _ = tokio::time::timeout(Duration::from_secs(10), self.join).await;
    }
}

fn host_peer() -> PeerInfo {
    PeerInfo::new(PeerId::from_bytes([1; 32]), "host")
}

#[tokio::test(flavor = "multi_thread")]
async fn host_require_empty_rejects_non_empty_dir_without_creating_state() {
    let harness = DaemonHarness::spawn().await;
    let client = Client::connect(&harness.socket).await.unwrap();

    let ws_dir = TempDir::new().unwrap();
    tokio::fs::write(ws_dir.path().join("user-data.txt"), b"surprise!")
        .await
        .unwrap();

    let err = Workspace::host(
        &client,
        host_peer(),
        ws_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
    )
    .await
    .expect_err("RequireEmpty must reject a non-empty dir");

    match err {
        WorkspaceError::Policy(PolicyViolation::DirNotEmpty {
            offending_entries, ..
        }) => {
            assert!(
                offending_entries
                    .iter()
                    .any(|p| p.ends_with("user-data.txt")),
                "offending_entries should name user-data.txt: {offending_entries:?}",
            );
        }
        other => panic!("expected Policy(DirNotEmpty), got {other:?}"),
    }

    // Critical: the iroh state dir must not have been created. A
    // regression that spawned the iroh node before the policy check
    // would leave `iroh.key` / `doc-id` behind under `.artel-fs/`.
    let state_dir = ws_dir.path().join(".artel-fs");
    assert!(
        !state_dir.exists(),
        "policy rejection must leave no iroh state behind, but {} exists",
        state_dir.display(),
    );

    drop(client);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn host_allow_existing_publishes_pre_seeded_contents() {
    let harness = DaemonHarness::spawn().await;
    let client = Client::connect(&harness.socket).await.unwrap();

    let ws_dir = TempDir::new().unwrap();
    tokio::fs::write(ws_dir.path().join("README.md"), b"hello")
        .await
        .unwrap();

    let (ws, _events) = Workspace::host(
        &client,
        host_peer(),
        ws_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
    )
    .await
    .expect("AllowExisting should succeed against pre-seeded dir");

    // Sanity: the pre-existing file made it into the doc.
    let stream = ws
        .doc()
        .get_many(Query::single_latest_per_key())
        .await
        .expect("get_many");
    tokio::pin!(stream);
    let mut found = false;
    while let Some(res) = futures_util::StreamExt::next(&mut stream).await {
        let entry = res.expect("entry");
        if String::from_utf8_lossy(entry.key()).contains("README.md") {
            found = true;
            break;
        }
    }
    assert!(found, "README.md should be published into the doc");

    ws.shutdown().await;
    drop(client);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn host_require_empty_accepts_truly_empty_dir() {
    let harness = DaemonHarness::spawn().await;
    let client = Client::connect(&harness.socket).await.unwrap();

    let ws_dir = TempDir::new().unwrap();
    let (ws, _events) = Workspace::host_with(
        &client,
        host_peer(),
        ws_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default(),
    )
    .await
    .expect("RequireEmpty should accept fresh empty dir");

    ws.shutdown().await;
    drop(client);
    harness.stop().await;
}
