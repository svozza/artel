//! `RequireEmpty` accepts a workspace root whose only inhabitant is
//! the workspace's own `.artel-fs/` state directory.
//!
//! This is the returning-host / returning-joiner case: state survives
//! across restarts under `<root>/.artel-fs/`, and a strict
//! `RequireEmpty` that didn't exempt the state dir would refuse to
//! resume — defeating the point of persistence.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_fs::{AttachPolicy, Workspace};
use artel_protocol::{PeerId, PeerInfo};
use tempfile::TempDir;

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
            endpoint_setup: artel_daemon::EndpointSetup::Production,
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

#[tokio::test(flavor = "multi_thread")]
async fn require_empty_accepts_dir_with_only_artel_fs_state() {
    let harness = DaemonHarness::spawn().await;
    let client = Client::connect(&harness.socket).await.unwrap();
    let peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "host");

    // Pre-create the state dir with the layout a previous lifetime
    // would have left behind. Real `iroh.key` content isn't needed —
    // the workspace will recreate or load it as appropriate. The
    // test point is purely "the policy check exempts .artel-fs".
    let ws_dir = TempDir::new().unwrap();
    let state_dir = ws_dir.path().join(".artel-fs");
    tokio::fs::create_dir_all(&state_dir).await.unwrap();

    let (ws, _events) = Workspace::host(
        &client,
        peer,
        ws_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
    )
    .await
    .expect("RequireEmpty should accept dir with only .artel-fs/");

    ws.shutdown().await;
    drop(client);
    harness.stop().await;
}
