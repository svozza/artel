//! Shared fixtures for `artel-fs` integration tests.
//!
//! Two flavours of harness:
//! - [`spawn_local_daemon`] — single iroh-disabled daemon, used by
//!   tests that only exercise client IPC paths (e.g.
//!   `host_publishes_ticket.rs`).
//! - [`spawn_pair`] — two iroh-enabled daemons with cross-seeded
//!   [`MemoryLookup`]s. Mirrors `artel-daemon`'s test fixture so the
//!   artel session traffic between the two daemons doesn't depend
//!   on the n0 relay infrastructure being reachable.
//!
//! [`spawn_pair`] returns the same [`MemoryLookup`] handles back to
//! the caller as [`Pair::workspace_lookup_a`] /
//! [`Pair::workspace_lookup_b`] so callers can plumb them into the
//! `address_lookup_override` field of [`artel_fs::WorkspaceConfig`].
//! Doing so takes the per-workspace iroh nodes off n0 too, which
//! eliminates the rapid-iteration flakiness traced in
//! `docs/handoff-stale-daemon.md` (n0 DNS publish/resolve has
//! external rate limits and TTLs that fresh-tempdir tests have no
//! reason to be paying).
//!
//! Tests that exercise the production discovery path (e.g.
//! `iroh_docs_smoke.rs`) deliberately *don't* use this fixture —
//! they keep `presets::N0` so the discovery contract stays under
//! regression coverage somewhere.

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

/// Two cross-seeded daemons plus the [`MemoryLookup`] handles the
/// caller hands to each side's [`artel_fs::WorkspaceConfig`].
///
/// Returned as a struct rather than a long tuple so adding a new
/// handle (e.g. a third daemon, a fourth lookup) doesn't ripple
/// through every test's `let (a, b) = spawn_pair().await;`.
pub struct Pair {
    pub daemon_a: RunningDaemon,
    pub daemon_b: RunningDaemon,
    /// Hand to daemon A's workspace via
    /// `WorkspaceConfig::with_address_lookup_override`.
    pub workspace_lookup_a: MemoryLookup,
    /// Hand to daemon B's workspace via
    /// `WorkspaceConfig::with_address_lookup_override`.
    pub workspace_lookup_b: MemoryLookup,
}

/// Spin two daemons up and cross-seed their address books so the
/// artel session traffic flows between them. The same lookups are
/// returned via [`Pair::workspace_lookup_a`] /
/// [`Pair::workspace_lookup_b`] so the per-workspace iroh nodes
/// can share the same discovery substrate — both daemon and
/// workspace endpoints participate in one in-process address
/// book and n0's externally-rate-limited DNS layer is fully off
/// the test's critical path.
///
/// All four nodes (daemon A, daemon B, workspace A, workspace B)
/// share **one** underlying [`MemoryLookup`] map (cloned across
/// participants — the inner state is `Arc<RwLock<BTreeMap>>`, so
/// clones share storage). Any node that registers itself becomes
/// resolvable to every other node. Two-lookup cross-seeding can't
/// work for workspaces: workspace A's id isn't known when daemon B
/// is constructed and vice versa, so a clone-on-creation pattern
/// is the only setup that's race-free at fixture time.
pub async fn spawn_pair() -> Pair {
    let shared = MemoryLookup::new();

    let daemon_a = spawn_daemon_with_lookup(fresh_state(), shared.clone()).await;
    let daemon_b = spawn_daemon_with_lookup(fresh_state(), shared.clone()).await;
    shared.add_endpoint_info(daemon_a.iroh_addr.clone().expect("daemon_a iroh addr"));
    shared.add_endpoint_info(daemon_b.iroh_addr.clone().expect("daemon_b iroh addr"));

    Pair {
        daemon_a,
        daemon_b,
        workspace_lookup_a: shared.clone(),
        workspace_lookup_b: shared,
    }
}
