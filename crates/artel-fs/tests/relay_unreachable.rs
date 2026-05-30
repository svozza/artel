//! Pins the timeout-and-typed-error contract for handoff finding
//! #7 (`endpoint.online().await` has no timeout).
//!
//! `Workspace::host_with` against [`EndpointSetup::TestingUnreachableRelay`]
//! must return [`WorkspaceError::RelayUnreachable`] within a budget,
//! NOT hang forever in [`iroh::Endpoint::online`]. Pre-fix the call
//! at `crates/artel-fs/src/node.rs:125` is a bare
//! `endpoint.online().await` with no wrapper — this test hangs in
//! that state until the harness-side timeout panics with the phase
//! name (per `docs/diagnosing-flaky-tests.md`).
//!
//! The fixture uses RFC 5737 TEST-NET-1 (`192.0.2.1`), guaranteed
//! unrouteable on the public internet. No external network access
//! is required for the test to run.

#![cfg(feature = "test-utils")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_fs::{AttachPolicy, EndpointSetup, Workspace, WorkspaceConfig, WorkspaceError};
use artel_protocol::{PeerId, PeerInfo};
use tempfile::TempDir;
use tokio::time::timeout;

/// How long to wait for `Workspace::host_with` to surface the typed
/// error. The substrate's internal budget for `endpoint.online()`
/// is 30s; the harness gives a small margin so a slow CI doesn't
/// flake on scheduling alone.
const HARNESS_BUDGET: Duration = Duration::from_secs(40);

struct DaemonHarness {
    _tempdir: TempDir,
    socket: PathBuf,
    shutdown: Arc<Shutdown>,
    join: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl DaemonHarness {
    /// Daemon stays local-only (no `iroh_key_path`) — only the
    /// workspace endpoint exercises the relay path.
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

#[tokio::test(flavor = "multi_thread")]
async fn host_with_unreachable_relay_returns_typed_error() {
    let harness = DaemonHarness::spawn().await;
    let client = Client::connect(&harness.socket).await.unwrap();

    let ws_dir = tempfile::tempdir().unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");

    let config =
        WorkspaceConfig::default().with_endpoint_setup(EndpointSetup::TestingUnreachableRelay);

    // `Workspace::host_with` must return Err within the harness
    // budget. Pre-fix this hangs in `endpoint.online()` and the
    // harness panics with the phase name.
    let result = timeout(
        HARNESS_BUDGET,
        Workspace::host_with(
            &client,
            alice_peer,
            ws_dir.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            config,
        ),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "phase hung past {HARNESS_BUDGET:?}: \
             Workspace::host_with did not return Err within budget — \
             the timeout wrapper around endpoint.online() is missing"
        )
    });

    match result {
        Err(WorkspaceError::RelayUnreachable(budget)) => {
            assert!(
                budget <= HARNESS_BUDGET,
                "internal budget {budget:?} should be at most the harness budget {HARNESS_BUDGET:?}"
            );
        }
        Ok(_) => panic!(
            "expected WorkspaceError::RelayUnreachable, but host_with succeeded — \
             that's impossible against a TEST-NET-1 relay"
        ),
        Err(other) => panic!("expected WorkspaceError::RelayUnreachable, got {other:?}"),
    }

    drop(client);
    harness.stop().await;
}
