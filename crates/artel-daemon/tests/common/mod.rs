//! Shared fixture for iroh integration tests.
//!
//! Each consumer is its own integration-test binary. Cargo runs
//! those binaries in separate processes, which is exactly what we
//! want: iroh tests bring up real `Endpoint`s and they don't play
//! well when several pairs run in the same process under load.
//!
//! This module is referenced via `mod common;` from each
//! `tests/iroh_*.rs` consumer; lives under `tests/common/` so
//! cargo doesn't treat it as a test target itself.

#![cfg(feature = "iroh")]
// Each consumer pulls a different subset of these helpers; tolerate
// per-binary "unused" warnings rather than cfg-forking the helpers.
#![allow(dead_code)]
// Each integration-test binary that pulls this in is its own crate
// root, so `pub` here is reachable even though clippy thinks
// otherwise. See `feedback_clippy_lint_conflict` in memory.
#![allow(unreachable_pub, clippy::redundant_pub_crate)]

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

pub struct State {
    pub root: TempDir,
    pub socket: PathBuf,
    pub pid: PathBuf,
    pub sessions: PathBuf,
    pub iroh_key: PathBuf,
}

pub fn fresh_state() -> State {
    let root = TempDir::new().unwrap();
    State {
        socket: root.path().join("daemon.sock"),
        pid: root.path().join("daemon.pid"),
        sessions: root.path().join("sessions"),
        iroh_key: root.path().join("iroh.key"),
        root,
    }
}

pub struct RunningDaemon {
    pub socket: PathBuf,
    pub iroh_addr: iroh::EndpointAddr,
    pub shutdown: Arc<Shutdown>,
    pub join: tokio::task::JoinHandle<std::io::Result<()>>,
    /// State dir kept alive for the daemon's lifetime; dropped on
    /// `stop()` so cleanup runs only after the daemon has exited.
    pub _state: State,
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

pub async fn spawn_daemon(state: State, lookup: MemoryLookup) -> RunningDaemon {
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
    let iroh_addr = daemon.iroh().expect("iroh runtime").endpoint.addr();
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

/// Spin two daemons up and cross-seed their address books. Returns
/// `(daemon_a, daemon_b)` ready for one of them to host and the
/// other to join.
pub async fn spawn_pair() -> (RunningDaemon, RunningDaemon) {
    let lookup_a = MemoryLookup::new();
    let lookup_b = MemoryLookup::new();

    let daemon_a = spawn_daemon(fresh_state(), lookup_a.clone()).await;
    let daemon_b = spawn_daemon(fresh_state(), lookup_b.clone()).await;
    lookup_a.add_endpoint_info(daemon_b.iroh_addr.clone());
    lookup_b.add_endpoint_info(daemon_a.iroh_addr.clone());

    (daemon_a, daemon_b)
}

/// Wait for `events` to deliver an `Event::Message` whose payload
/// matches `expected_payload`. Skips other events; panics on
/// timeout, channel close, or no message in 20 seconds.
pub async fn expect_message_with_payload(
    events: &mut artel_client::EventStream,
    expected_payload: &[u8],
    who: &str,
) -> artel_protocol::SessionMessage {
    use artel_protocol::Event;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "{who}: message with payload {expected_payload:?} never arrived",
        );
        let event = match timeout(remaining, events.recv()).await {
            Ok(Some(ev)) => ev,
            Ok(None) => panic!("{who}: events channel closed"),
            Err(_) => continue,
        };
        if let Event::Message { message, .. } = event
            && message.payload == expected_payload
        {
            return message;
        }
    }
}
