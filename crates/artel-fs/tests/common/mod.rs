//! Shared fixtures for `artel-fs` integration tests.
//!
//! Two flavours of harness:
//! - [`spawn_local_daemon`] / [`DaemonHarness`]-style — single
//!   iroh-disabled daemon, used by tests that only exercise client
//!   IPC paths (e.g. `host_publishes_ticket.rs`).
//! - [`spawn_pair`] — two iroh-enabled daemons sharing one
//!   localhost [`DnsPkarrServer`]. Mirrors `artel-daemon`'s test
//!   fixture so the artel session traffic between the two daemons
//!   doesn't depend on n0's pkarr/DNS infrastructure being
//!   reachable.
//!
//! [`spawn_pair`] hands the same [`Arc<DnsPkarrServer>`] back to
//! the caller as [`Pair::dns_pkarr`] so tests can plumb it into
//! per-workspace [`artel_fs::WorkspaceConfig`]s via
//! [`WorkspaceConfig::with_endpoint_setup`]. All four endpoints
//! (daemon A, daemon B, workspace A, workspace B) sharing the same
//! pkarr+DNS pair is what makes cross-peer tests deterministic —
//! every endpoint that holds a clone of the same Arc resolves to
//! the same shared state, exactly the same code path as production
//! except the localhost transport.

#![allow(dead_code, unreachable_pub, clippy::redundant_pub_crate)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig, EndpointSetup as DaemonEndpointSetup};
use artel_fs::EndpointSetup as FsEndpointSetup;
use artel_protocol::PeerId;
use futures_util::StreamExt;
use iroh::test_utils::DnsPkarrServer;
use iroh_docs::api::Doc;
use iroh_docs::store::Query;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

/// Default poll interval used by [`wait_for_file`] / [`wait_for_missing`].
pub const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Default deadline budget. 15s covers notify debounce (300ms) +
/// doc `set_bytes` + sync + applier + `tokio::fs::write` under load.
pub const FILE_BUDGET: Duration = Duration::from_secs(15);

/// How long to wait for a freshly-bound endpoint to publish its
/// pkarr record to the localhost server. Generous because endpoint
/// startup includes reading the on-disk secret + binding QUIC + the
/// pkarr publish loop kicking in. A failure here means the test
/// would have raced regardless; surfacing it as a fixture-side
/// timeout makes the cause clear.
pub const PKARR_READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Poll `path` until it contains `expected` exactly, or panic with
/// the path on timeout.
pub async fn wait_for_file(path: &Path, expected: &[u8]) {
    let deadline = Instant::now() + FILE_BUDGET;
    loop {
        if let Ok(bytes) = tokio::fs::read(path).await
            && bytes == expected
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "never saw expected bytes at {}",
            path.display(),
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Poll until `path` no longer exists, or panic on timeout.
pub async fn wait_for_missing(path: &Path) {
    let deadline = Instant::now() + FILE_BUDGET;
    loop {
        if !path.exists() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} never disappeared",
            path.display(),
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Whether `doc` has any (non-tombstone) entry for `key`. Used by
/// rule tests to confirm a `ReadOnly` write never landed in the doc.
pub async fn doc_has_key(doc: &Doc, key: &[u8]) -> bool {
    let stream = doc.get_many(Query::key_exact(key)).await.expect("get_many");
    tokio::pin!(stream);
    stream.next().await.is_some()
}

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

/// Build the daemon-side `EndpointSetup::Testing` variant from
/// the shared fixture. `Arc::clone` is cheap (refcount bump); the
/// `dns_pkarr` field stays the same across every endpoint that
/// uses this fixture, so all of them resolve via the same
/// localhost server.
pub fn daemon_testing_setup(dns_pkarr: &Arc<DnsPkarrServer>) -> DaemonEndpointSetup {
    DaemonEndpointSetup::Testing {
        dns_pkarr: Arc::clone(dns_pkarr),
    }
}

/// Workspace-side companion to [`daemon_testing_setup`]. The
/// daemon and workspace each define their own `EndpointSetup`
/// enum (peer crates, neither depending on the other) so
/// cross-crate setup values can't be shared by type. Both wrap
/// the same `Arc<DnsPkarrServer>`. `WorkspaceConfig` callers
/// pass this in; `DaemonConfig` callers pass [`daemon_testing_setup`].
pub fn testing_setup(dns_pkarr: &Arc<DnsPkarrServer>) -> FsEndpointSetup {
    FsEndpointSetup::Testing {
        dns_pkarr: Arc::clone(dns_pkarr),
    }
}

/// Bring an iroh-enabled daemon up against the supplied
/// [`EndpointSetup`]. Used by [`spawn_pair`] for the shared
/// `Testing` setup; tests that need a single daemon (e.g. the
/// single-daemon attachment cases in `workspace_attachment.rs`)
/// can call this directly with their own per-test fixture.
pub async fn spawn_daemon_with_setup(
    state: DaemonState,
    setup: DaemonEndpointSetup,
) -> RunningDaemon {
    let daemon = Daemon::start(DaemonConfig {
        socket_path: state.socket.clone(),
        pid_path: state.pid.clone(),
        sessions_dir: state.sessions.clone(),
        daemon_peer_id: FALLBACK_PEER,
        iroh_key_path: Some(state.iroh_key.clone()),
        endpoint_setup: setup,
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

/// Daemon state paths whose on-disk directory is owned by the caller.
/// Use this when a test needs to stop a daemon and respawn another at
/// the same paths (e.g. mid-session restart scenarios). Compare with
/// [`DaemonState`], which bundles a [`TempDir`] and is wiped on stop.
pub struct DaemonPaths {
    pub socket: PathBuf,
    pub pid: PathBuf,
    pub sessions: PathBuf,
    pub iroh_key: PathBuf,
}

impl DaemonPaths {
    /// Build paths that all live under `root`. The caller owns `root`
    /// (typically a [`TempDir`]) so it survives across daemon restarts.
    pub fn at(root: &Path) -> Self {
        Self {
            socket: root.join("daemon.sock"),
            pid: root.join("daemon.pid"),
            sessions: root.join("sessions"),
            iroh_key: root.join("iroh.key"),
        }
    }
}

/// A running daemon whose on-disk state is owned by the caller. Cf.
/// [`RunningDaemon`], which owns its [`TempDir`] and wipes state on
/// `stop()`. Designed for restart-scenario tests that need to stand a
/// fresh daemon up at the same paths after the previous one exited.
pub struct DaemonHandle {
    pub socket: PathBuf,
    pub iroh_addr: Option<iroh::EndpointAddr>,
    pub shutdown: Arc<Shutdown>,
    pub join: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl DaemonHandle {
    pub async fn stop(self) {
        self.shutdown.trigger();
        timeout(Duration::from_secs(10), self.join)
            .await
            .expect("daemon did not exit within 10s")
            .expect("daemon panicked")
            .expect("daemon io");
    }
}

/// Bring an iroh-enabled daemon up at fixed paths.
///
/// Pass [`EndpointSetup::Testing`] (built via [`testing_setup`]) for
/// fast deterministic tests; pass [`EndpointSetup::Production`] (the
/// default) for real-n0 tests. The directory containing `paths` is
/// the caller's responsibility — it must outlive the daemon and any
/// planned restarts.
pub async fn spawn_daemon_at(paths: &DaemonPaths, setup: DaemonEndpointSetup) -> DaemonHandle {
    let daemon = Daemon::start(DaemonConfig {
        socket_path: paths.socket.clone(),
        pid_path: paths.pid.clone(),
        sessions_dir: paths.sessions.clone(),
        daemon_peer_id: FALLBACK_PEER,
        iroh_key_path: Some(paths.iroh_key.clone()),
        endpoint_setup: setup,
    })
    .await
    .expect("daemon start");
    let iroh_addr = daemon.iroh().map(|rt| rt.endpoint.addr());
    let shutdown = daemon.shutdown_handle();
    let socket = daemon.socket_path().to_path_buf();
    let join = tokio::spawn(daemon.run());
    DaemonHandle {
        socket,
        iroh_addr,
        shutdown,
        join,
    }
}

/// Two daemons sharing one [`DnsPkarrServer`]. Tests that call
/// [`spawn_pair`] hand `dns_pkarr` clones into every workspace
/// they create so the workspace endpoints resolve via the same
/// localhost server.
pub struct Pair {
    pub daemon_a: RunningDaemon,
    pub daemon_b: RunningDaemon,
    /// The shared localhost DNS+pkarr fixture. Cloned into both
    /// daemons (already done) and into each workspace's
    /// `EndpointSetup::Testing` (caller's responsibility, via
    /// [`testing_setup`]). The fixture's localhost servers stay
    /// alive as long as any clone of this Arc exists.
    pub dns_pkarr: Arc<DnsPkarrServer>,
}

/// Spin two daemons up sharing a single localhost
/// [`DnsPkarrServer`].
///
/// Both daemons publish their pkarr records to the shared server
/// during [`Daemon::start`]'s endpoint bind; this helper waits via
/// [`DnsPkarrServer::on_endpoint`] until each one is queryable
/// before returning, so the first cross-peer dial in the test
/// can't race the publish. Eliminates the propagation-window
/// flakes that the old `MemoryLookup` fixture papered over.
pub async fn spawn_pair() -> Pair {
    let dns_pkarr = Arc::new(DnsPkarrServer::run().await.expect("DnsPkarrServer::run"));

    // Box::pin the spawn futures so `tokio::join!` doesn't stack two
    // copies of the (large) `Daemon::start` state machine into one
    // future and trip `clippy::large_futures` at every caller.
    let fut_a = Box::pin(spawn_daemon_with_setup(
        fresh_state(),
        daemon_testing_setup(&dns_pkarr),
    ));
    let fut_b = Box::pin(spawn_daemon_with_setup(
        fresh_state(),
        daemon_testing_setup(&dns_pkarr),
    ));
    let (daemon_a, daemon_b) = tokio::join!(fut_a, fut_b);

    let id_a = daemon_a.iroh_addr.as_ref().expect("daemon iroh addr").id;
    let id_b = daemon_b.iroh_addr.as_ref().expect("daemon iroh addr").id;
    tokio::join!(
        wait_for_endpoint(&dns_pkarr, &id_a),
        wait_for_endpoint(&dns_pkarr, &id_b),
    );

    Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    }
}

/// Wait until `endpoint_id` has published its pkarr record to the
/// shared localhost server, or panic on timeout.
///
/// Use this on the *workspace* endpoints too: a workspace's iroh
/// node binds during [`artel_fs::Workspace::host_with`] /
/// [`artel_fs::Workspace::join_with`] and pkarr-publishes shortly
/// after; tests that immediately dial the other side from a
/// freshly-constructed workspace can race the publish. The
/// daemon-side `spawn_pair` already gates on the daemon endpoints,
/// but tests that observe workspace-to-workspace traffic
/// (everything in the cross-peer suite) want this gate too.
pub async fn wait_for_endpoint(dns_pkarr: &DnsPkarrServer, endpoint_id: &iroh::EndpointId) {
    dns_pkarr
        .on_endpoint(endpoint_id, PKARR_READY_TIMEOUT)
        .await
        .expect("endpoint pkarr-published in time");
}
