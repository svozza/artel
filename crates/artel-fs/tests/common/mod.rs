//! Shared fixtures for `artel-fs` integration tests.
//!
//! Two flavours of harness:
//! - [`spawn_local_daemon`] — single iroh-disabled daemon, used by
//!   tests that only exercise client IPC paths (e.g.
//!   `host_publishes_ticket.rs`).
//! - [`spawn_pair`] — two iroh-enabled daemons with cross-seeded
//!   `MemoryLookup`s. Mirrors `artel-daemon`'s test fixture so the
//!   artel session traffic between the two daemons doesn't depend
//!   on the n0 relay infrastructure being reachable. The
//!   workspace's *own* iroh nodes still use n0 default discovery —
//!   that's the "`DocTicket` carries enough addressing" path 3a-1
//!   verified.

#![allow(dead_code, unreachable_pub, clippy::redundant_pub_crate)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use artel_daemon::shutdown::Shutdown;
use artel_daemon::{AddressLookupOverride, Daemon, DaemonConfig};
use artel_protocol::PeerId;
use iroh::address_lookup::memory::MemoryLookup;
use tempfile::TempDir;
use tokio::time::timeout;

pub const FALLBACK_PEER: PeerId = PeerId::from_bytes([0xee; 32]);

pub struct DaemonState {
    pub root: TempDir,
    pub socket: PathBuf,
    pub pid: PathBuf,
    pub sessions: PathBuf,
    pub iroh_key: PathBuf,
}

pub fn fresh_state() -> DaemonState {
    let root = TempDir::new().unwrap();
    DaemonState {
        socket: root.path().join("daemon.sock"),
        pid: root.path().join("daemon.pid"),
        sessions: root.path().join("sessions"),
        iroh_key: root.path().join("iroh.key"),
        root,
    }
}

pub struct RunningDaemon {
    pub socket: PathBuf,
    pub iroh_addr: Option<iroh::EndpointAddr>,
    pub shutdown: Arc<Shutdown>,
    pub join: tokio::task::JoinHandle<std::io::Result<()>>,
    pub _state: DaemonState,
}

impl RunningDaemon {
    pub async fn stop(self) {
        self.shutdown.trigger();
        timeout(Duration::from_secs(10), self.join)
            .await
            .expect("daemon did not exit within 10s")
            .expect("daemon panicked")
            .expect("daemon io");
    }
}

/// Bring an iroh-enabled daemon up against the supplied
/// `MemoryLookup` (used for cross-daemon addressing in tests).
pub async fn spawn_daemon_with_lookup(state: DaemonState, lookup: MemoryLookup) -> RunningDaemon {
    let daemon = Daemon::start(DaemonConfig {
        socket_path: state.socket.clone(),
        pid_path: state.pid.clone(),
        sessions_dir: state.sessions.clone(),
        daemon_peer_id: FALLBACK_PEER,
        iroh_key_path: Some(state.iroh_key.clone()),
        address_lookup: Some(AddressLookupOverride(lookup)),
    })
    .await
    .expect("daemon start");
    let iroh_addr = daemon.iroh().map(|rt| rt.endpoint.addr());
    let shutdown = daemon.shutdown_handle();
    let socket = daemon.socket_path().to_path_buf();
    let join = tokio::spawn(daemon.run());
    RunningDaemon {
        socket,
        iroh_addr,
        shutdown,
        join,
        _state: state,
    }
}

/// Spin two daemons up and cross-seed their address books so the
/// artel session traffic flows between them. The workspace's own
/// iroh nodes are independent and use n0 discovery.
pub async fn spawn_pair() -> (RunningDaemon, RunningDaemon) {
    let lookup_a = MemoryLookup::new();
    let lookup_b = MemoryLookup::new();

    let daemon_a = spawn_daemon_with_lookup(fresh_state(), lookup_a.clone()).await;
    let daemon_b = spawn_daemon_with_lookup(fresh_state(), lookup_b.clone()).await;
    lookup_a.add_endpoint_info(daemon_b.iroh_addr.clone().expect("daemon_b iroh addr"));
    lookup_b.add_endpoint_info(daemon_a.iroh_addr.clone().expect("daemon_a iroh addr"));

    (daemon_a, daemon_b)
}
