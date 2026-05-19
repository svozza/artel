//! End-to-end test: the daemon's iroh-derived peer id is stable
//! across restarts when the iroh secret key file persists.
//!
//! Gated on the `iroh` feature. Without it, the daemon falls back to
//! the synthetic peer id and these assertions don't apply.

#![cfg(feature = "iroh")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_protocol::ticket;
use artel_protocol::{PeerId, PeerInfo, Request, Response};
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::timeout;

/// Synthetic id used as the `daemon_peer_id` fallback. With an iroh
/// key supplied, the daemon must derive its real id from the key and
/// ignore this value, so we pick something obviously fake.
const FALLBACK_PEER: PeerId = PeerId::from_bytes([0xee; 32]);

struct StateDir {
    _root: TempDir,
    socket: PathBuf,
    pid: PathBuf,
    sessions: PathBuf,
    iroh_key: PathBuf,
}

fn fresh_state_dir() -> StateDir {
    let root = TempDir::new().unwrap();
    StateDir {
        socket: root.path().join("daemon.sock"),
        pid: root.path().join("daemon.pid"),
        sessions: root.path().join("sessions"),
        iroh_key: root.path().join("iroh.key"),
        _root: root,
    }
}

struct RunningDaemon {
    shutdown: Arc<Shutdown>,
    join: tokio::task::JoinHandle<std::io::Result<()>>,
}

async fn spawn_at(state: &StateDir) -> RunningDaemon {
    let daemon = Daemon::start(DaemonConfig {
        socket_path: state.socket.clone(),
        pid_path: state.pid.clone(),
        sessions_dir: state.sessions.clone(),
        daemon_peer_id: FALLBACK_PEER,
        iroh_key_path: Some(state.iroh_key.clone()),
        address_lookup: None,
    })
    .await
    .expect("daemon start");
    let shutdown = daemon.shutdown_handle();
    let join = tokio::spawn(daemon.run());
    RunningDaemon { shutdown, join }
}

impl RunningDaemon {
    async fn stop(self) {
        self.shutdown.trigger();
        timeout(Duration::from_secs(10), self.join)
            .await
            .expect("daemon did not exit within 10s")
            .expect("daemon panicked")
            .expect("daemon io");
    }
}

#[tokio::test]
async fn endpoint_id_is_stable_across_daemon_restarts() {
    let state = fresh_state_dir();

    // First boot generates and persists the key.
    let daemon = spawn_at(&state).await;
    let client = Client::connect(&state.socket).await.unwrap();
    let first_id = client.daemon_peer_id();
    assert_ne!(
        first_id, FALLBACK_PEER,
        "iroh-derived id must not equal the synthetic fallback",
    );
    assert!(state.iroh_key.exists(), "iroh.key should be persisted");
    drop(client);
    daemon.stop().await;

    // Second boot reuses the persisted key.
    let daemon = spawn_at(&state).await;
    let client = Client::connect(&state.socket).await.unwrap();
    let second_id = client.daemon_peer_id();
    assert_eq!(
        first_id, second_id,
        "EndpointId must be stable across restarts",
    );
    drop(client);
    daemon.stop().await;
}

#[tokio::test]
async fn host_ticket_carries_a_real_endpoint_addr() {
    // When iroh is wired up, the ticket the daemon emits via
    // HostSession should carry the daemon's actual EndpointId in
    // host_addr.peer_id. We don't assert anything stronger about
    // direct addrs / relay url because those are environment-
    // dependent — but the addr must be self-consistent and match
    // the live peer id.
    let state = fresh_state_dir();
    let daemon = spawn_at(&state).await;
    let client = Client::connect(&state.socket).await.unwrap();
    let daemon_id = client.daemon_peer_id();

    let resp = client
        .request(Request::HostSession {
            peer: PeerInfo::new(PeerId::from_bytes([1; 32]), "alice"),
        })
        .await
        .unwrap();
    let raw = match resp {
        Response::HostSession { ticket, .. } => ticket,
        other => panic!("expected HostSession, got {other:?}"),
    };
    let decoded = ticket::decode(raw.as_str()).expect("ticket decodes");
    assert_eq!(decoded.host_peer_id, daemon_id);
    assert_eq!(
        decoded.host_addr.peer_id, daemon_id,
        "host_addr.peer_id must match the daemon's live id",
    );

    drop(client);
    daemon.stop().await;
}

#[tokio::test]
async fn iroh_key_file_is_chmod_0600() {
    use std::os::unix::fs::MetadataExt;

    let state = fresh_state_dir();
    let daemon = spawn_at(&state).await;
    let mode = std::fs::metadata(&state.iroh_key).unwrap().mode() & 0o777;
    assert_eq!(mode, 0o600, "iroh.key must be owner-only");
    daemon.stop().await;
}

#[tokio::test]
async fn no_iroh_key_path_keeps_synthetic_peer_id() {
    // Sanity: when the caller doesn't supply iroh_key_path, the
    // daemon stays local-only and the wire peer id matches the
    // synthetic fallback.
    let root = TempDir::new().unwrap();
    let daemon = Daemon::start(DaemonConfig {
        socket_path: root.path().join("daemon.sock"),
        pid_path: root.path().join("daemon.pid"),
        sessions_dir: root.path().join("sessions"),
        daemon_peer_id: FALLBACK_PEER,
        iroh_key_path: None,
        address_lookup: None,
    })
    .await
    .expect("daemon start");
    let socket = daemon.socket_path().to_path_buf();
    let shutdown = daemon.shutdown_handle();
    let join = tokio::spawn(daemon.run());

    let client = Client::connect(&socket).await.unwrap();
    assert_eq!(client.daemon_peer_id(), FALLBACK_PEER);
    drop(client);
    shutdown.trigger();
    timeout(Duration::from_secs(5), join)
        .await
        .expect("daemon did not exit")
        .expect("daemon panicked")
        .expect("daemon io");
}
