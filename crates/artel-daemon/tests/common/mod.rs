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
use artel_daemon::{Daemon, DaemonConfig, EndpointSetup};
use artel_protocol::PeerId;
use iroh::test_utils::DnsPkarrServer;
use iroh_relay::server::Server as RelayServer;
use tempfile::TempDir;
use tokio::sync::OnceCell;
use tokio::time::timeout;

/// How long to wait for a freshly-bound endpoint to publish its
/// pkarr record to the localhost server.
pub const PKARR_READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Derive a curve-valid [`PeerId`] from a single seed byte. Plain
/// `[seed; 32]` byte arrays don't satisfy iroh's Ed25519 curve
/// check, so tests that need a "novel but valid" peer-id (e.g. the
/// ghost peer in a spoofing test) go through this helper.
pub fn valid_peer_id(seed: u8) -> PeerId {
    PeerId::from_bytes(*iroh::SecretKey::from_bytes(&[seed; 32]).public().as_bytes())
}

/// Per-test-bin shared `DnsPkarrServer`. Bins that only need to
/// stand up an iroh-enabled daemon for IPC tests (no peer-to-peer
/// gossip) reuse this so they don't pay the ~200ms server startup
/// cost per test. Each test process gets its own server.
static SHARED_DNS_PKARR: OnceCell<Arc<DnsPkarrServer>> = OnceCell::const_new();

async fn shared_dns_pkarr() -> Arc<DnsPkarrServer> {
    SHARED_DNS_PKARR
        .get_or_init(|| async {
            Arc::new(
                DnsPkarrServer::run()
                    .await
                    .expect("DnsPkarrServer::run for shared local-daemon fixture"),
            )
        })
        .await
        .clone()
}

/// Per-binary shared localhost relay server. Keeps the `RelayServer`
/// alive for the test binary's lifetime; all binary-spawn tests reuse
/// the same relay URL so the spawned daemon subprocess can reach a
/// relay without touching n0's public TLS-fronted infra.
static SHARED_RELAY: OnceCell<(RelayServer, String)> = OnceCell::const_new();

pub async fn shared_relay_url() -> &'static str {
    &SHARED_RELAY
        .get_or_init(|| async {
            let (_relay_map, relay_url, server) = iroh::test_utils::run_relay_server()
                .await
                .expect("run_relay_server for binary-spawn tests");
            (server, relay_url.to_string())
        })
        .await
        .1
}

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

/// Path-only state for tests that need a daemon to restart at the
/// SAME paths across multiple incarnations. The caller owns the temp
/// dir and keeps it alive for the whole test, so files (iroh.key,
/// cache files, etc.) persist across `stop()`/respawn.
pub struct RestartState {
    pub socket: PathBuf,
    pub pid: PathBuf,
    pub sessions: PathBuf,
    pub iroh_key: PathBuf,
}

impl RestartState {
    pub fn under(root: &std::path::Path) -> Self {
        Self {
            socket: root.join("daemon.sock"),
            pid: root.join("daemon.pid"),
            sessions: root.join("sessions"),
            iroh_key: root.join("iroh.key"),
        }
    }
}

pub struct RunningDaemon {
    pub socket: PathBuf,
    pub iroh_addr: iroh::EndpointAddr,
    /// Cloned out of the daemon's [`IrohRuntime`] before `run()`
    /// consumes the `Daemon`. Cheap (the inner state is `Arc`-shared)
    /// and identical to the instance the daemon's resolver chain
    /// holds, so direct calls here are visible to iroh's lookups.
    pub addr_hint: iroh::address_lookup::memory::MemoryLookup,
    /// Cloned out of the daemon's [`IrohRuntime`] before `run()`
    /// consumes it. Lets tests subscribe to a session's gossip topic
    /// and broadcast hand-crafted frames (e.g. spoofed `peer.id`s
    /// for the auth-L1 regression suite).
    pub gossip: iroh_gossip::net::Gossip,
    /// Cloned out of the daemon's [`IrohRuntime`] before `run()`
    /// consumes it. Lets tests assert what's been recorded for the
    /// shutdown-snapshot path — e.g. that a peer that ever-only sent
    /// spoofed frames is NOT captured.
    pub tracked_peer_ids: Arc<std::sync::Mutex<std::collections::BTreeSet<iroh::EndpointId>>>,
    pub shutdown: Arc<Shutdown>,
    pub join: tokio::task::JoinHandle<std::io::Result<()>>,
    /// Optional caller-owned state dir (kept alive for the daemon's
    /// lifetime when present; dropped on `stop()`). `None` for
    /// restart-style tests where the dir outlives the daemon.
    pub _state: Option<State>,
}

impl RunningDaemon {
    /// The daemon's authenticated [`PeerId`] — its iroh
    /// `EndpointId` bytes wrapped as the protocol-side type.
    pub fn peer_id(&self) -> PeerId {
        PeerId::from_bytes(*self.iroh_addr.id.as_bytes())
    }

    /// Snapshot the bridge's `tracked_peer_ids` set. Returns a fresh
    /// `BTreeSet` so the caller can assert membership without holding
    /// the daemon's internal lock.
    pub fn tracked_peer_ids_snapshot(&self) -> std::collections::BTreeSet<iroh::EndpointId> {
        self.tracked_peer_ids.lock().expect("poisoned").clone()
    }

    pub async fn stop(self) {
        self.shutdown.trigger();
        timeout(Duration::from_secs(10), self.join)
            .await
            .expect("daemon did not exit within 10s")
            .expect("daemon panicked")
            .expect("daemon io");
    }
}

/// Build the [`EndpointSetup::Testing`] variant for `dns_pkarr`.
pub fn testing_setup(dns_pkarr: &Arc<DnsPkarrServer>) -> EndpointSetup {
    EndpointSetup::Testing {
        dns_pkarr: Arc::clone(dns_pkarr),
    }
}

pub async fn spawn_daemon(state: State, setup: EndpointSetup) -> RunningDaemon {
    let config = DaemonConfig {
        socket_path: state.socket.clone(),
        pid_path: state.pid.clone(),
        sessions_dir: state.sessions.clone(),
        iroh_key_path: Some(state.iroh_key.clone()),
        endpoint_setup: setup,
    };
    spawn_with_state(config, Some(state)).await
}

/// Spawn at fixed paths owned by the caller. Used by restart-style
/// tests that need the state dir (iroh.key, cache file, etc.) to
/// persist across daemon stop/respawn.
pub async fn spawn_daemon_at(paths: &RestartState, setup: EndpointSetup) -> RunningDaemon {
    let config = DaemonConfig {
        socket_path: paths.socket.clone(),
        pid_path: paths.pid.clone(),
        sessions_dir: paths.sessions.clone(),
        iroh_key_path: Some(paths.iroh_key.clone()),
        endpoint_setup: setup,
    };
    spawn_with_state(config, None).await
}

/// Bring a single iroh-enabled daemon up against the shared
/// in-process [`DnsPkarrServer`]. For tests that don't need a
/// second daemon to gossip with — they just want an iroh-enabled
/// daemon to drive IPC commands against — this is the right
/// helper. Pre-A2 the same surface was provided via the now-retired
/// synthetic-peer-id path; post-A2, every daemon binds an iroh
/// `Endpoint`.
///
/// Used by bins that own a [`TempDir`] directly so the daemon's
/// on-disk state outlives the spawn — e.g. restart-style tests,
/// attachment tests that keep the session dir live across
/// `RunningDaemon::stop()`.
pub async fn spawn_local_daemon_at(paths: &RestartState) -> RunningDaemon {
    let dns_pkarr = shared_dns_pkarr().await;
    spawn_daemon_at(paths, testing_setup(&dns_pkarr)).await
}

async fn spawn_with_state(config: DaemonConfig, state: Option<State>) -> RunningDaemon {
    let daemon = Daemon::start(config).await.expect("daemon start");
    let iroh_runtime = daemon.iroh();
    let iroh_addr = iroh_runtime.endpoint.addr();
    let addr_hint = iroh_runtime.addr_hint.clone();
    let gossip = iroh_runtime.gossip.clone();
    let tracked_peer_ids = Arc::clone(&iroh_runtime.tracked_peer_ids);
    let shutdown = daemon.shutdown_handle();
    let socket = daemon.socket_path().to_path_buf();
    let join = tokio::spawn(daemon.run());
    RunningDaemon {
        socket,
        iroh_addr,
        addr_hint,
        gossip,
        tracked_peer_ids,
        shutdown,
        join,
        _state: state,
    }
}

/// Spin two daemons up sharing one localhost
/// [`DnsPkarrServer`]. Returns
/// `(daemon_a, daemon_b, dns_pkarr)` ready for one of them to host
/// and the other to join. Waits until both daemons' pkarr records
/// are queryable before returning so the first dial doesn't race
/// the publish.
pub async fn spawn_pair() -> (RunningDaemon, RunningDaemon, Arc<DnsPkarrServer>) {
    let dns_pkarr = Arc::new(DnsPkarrServer::run().await.expect("DnsPkarrServer::run"));

    // Box::pin the spawn futures so `tokio::join!` doesn't stack two
    // copies of the (large) `Daemon::start` state machine into one
    // future and trip `clippy::large_futures` at every caller.
    let fut_a = Box::pin(spawn_daemon(fresh_state(), testing_setup(&dns_pkarr)));
    let fut_b = Box::pin(spawn_daemon(fresh_state(), testing_setup(&dns_pkarr)));
    let (daemon_a, daemon_b) = tokio::join!(fut_a, fut_b);

    let pkarr_a = dns_pkarr.on_endpoint(&daemon_a.iroh_addr.id, PKARR_READY_TIMEOUT);
    let pkarr_b = dns_pkarr.on_endpoint(&daemon_b.iroh_addr.id, PKARR_READY_TIMEOUT);
    let (ready_a, ready_b) = tokio::join!(pkarr_a, pkarr_b);
    ready_a.expect("daemon endpoint pkarr-published");
    ready_b.expect("daemon endpoint pkarr-published");

    (daemon_a, daemon_b, dns_pkarr)
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
