//! Pins the timeout-and-typed-error contract for handoff finding
//! #6 (daemon-side `endpoint.online()` asymmetry).
//!
//! `Daemon::start` against [`EndpointSetup::TestingUnreachableRelay`]
//! with an `iroh_key_path` (so the iroh runtime is stood up) must
//! return [`StartError::RelayUnreachable`] within a budget. Pre-fix
//! the daemon's `resolve_iroh_runtime` never calls
//! `endpoint.online()` at all — so this test fails by returning
//! `Ok` instead of the typed error. Post-fix the daemon mirrors
//! `WorkspaceNode::spawn`: it gates `online()` on
//! `EndpointSetup::awaits_relay()` and wraps it in
//! `tokio::time::timeout`.

#![cfg(feature = "test-utils")]

use std::path::PathBuf;
use std::time::Duration;

use artel_daemon::{Daemon, DaemonConfig, EndpointSetup, StartError};
use artel_protocol::PeerId;
use tempfile::TempDir;
use tokio::time::timeout;

const HARNESS_BUDGET: Duration = Duration::from_secs(40);

struct State {
    _root: TempDir,
    socket: PathBuf,
    pid: PathBuf,
    sessions: PathBuf,
    iroh_key: PathBuf,
}

fn fresh_state() -> State {
    let root = TempDir::new().unwrap();
    State {
        socket: root.path().join("daemon.sock"),
        pid: root.path().join("daemon.pid"),
        sessions: root.path().join("sessions"),
        iroh_key: root.path().join("iroh.key"),
        _root: root,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn daemon_start_with_unreachable_relay_returns_typed_error() {
    let state = fresh_state();
    let config = DaemonConfig {
        socket_path: state.socket.clone(),
        pid_path: state.pid.clone(),
        sessions_dir: state.sessions.clone(),
        daemon_peer_id: PeerId::from_bytes([0xee; 32]),
        // iroh_key_path = Some triggers the iroh runtime, which
        // is the codepath under test (#6).
        iroh_key_path: Some(state.iroh_key.clone()),
        endpoint_setup: EndpointSetup::TestingUnreachableRelay,
    };

    // `Daemon::start` must return Err within the harness budget.
    // Pre-fix it returns Ok almost immediately because the daemon
    // never awaits `endpoint.online()` at all — that's the
    // asymmetry #6 documents.
    let result = timeout(HARNESS_BUDGET, Daemon::start(config))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "phase hung past {HARNESS_BUDGET:?}: \
                 Daemon::start did not return Err within budget — \
                 the timeout wrapper around endpoint.online() is missing"
            )
        });

    match result {
        Err(StartError::RelayUnreachable(budget)) => {
            assert!(
                budget <= HARNESS_BUDGET,
                "internal budget {budget:?} should be at most the harness budget {HARNESS_BUDGET:?}"
            );
        }
        Ok(daemon) => {
            // Pre-fix path: the daemon stood up because `online()`
            // was never awaited. Tear it down so the test process
            // exits cleanly, then fail with the diagnosis.
            daemon.trigger_shutdown();
            let _ = timeout(Duration::from_secs(5), daemon.run()).await;
            panic!(
                "expected StartError::RelayUnreachable, but Daemon::start succeeded — \
                 the daemon never awaits endpoint.online() (#6 asymmetry)"
            );
        }
        Err(other) => panic!(
            "expected StartError::RelayUnreachable, got {other:?}"
        ),
    }

    // Hold the temp dir for the duration of the test; nothing to
    // shut down because Daemon::start returned Err on the path
    // we want.
    drop(state);
}
