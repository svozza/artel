//! `Workspace::run().await` is the workspace's readiness barrier.
//!
//! Before commit `69bb860` it was a synchronous `fn run` that
//! returned a `JoinHandle` immediately, leaving callers to guess at
//! the timing of two independent races:
//!
//! 1. The watcher's debouncer hadn't yet attached its OS-level
//!    filesystem watch (`FSEvents` on macOS, inotify on Linux), so a
//!    write that landed under [`Workspace::root`] right after
//!    `run()` could silently miss the watcher.
//! 2. The applier's `doc.subscribe()` hadn't yet returned, so a
//!    remote `InsertRemote` / `ContentReady` fired in the same
//!    window was lost — iroh-docs subscribers are push-to-vec, no
//!    replay.
//!
//! Now `Workspace::run` is `async`, awaits both halves, and only
//! resolves once each is observably ready. These two tests pin
//! that contract:
//!
//! - `watcher_attached_when_run_resolves`: write a file
//!   immediately on return and assert the doc picks it up via the
//!   watcher → `set_bytes` path. No settling sleep; without the
//!   gate this race-flakes.
//! - `applier_subscribed_when_run_resolves`: assert
//!   `doc.status().subscribers` has grown by 1 on return. iroh-docs
//!   exposes the live actor's subscriber count, so we don't need to
//!   wait for an event to confirm the subscription is live.
//!
//! Caveat on the applier test: under tokio's scheduler, even
//! without the gate the test's next `.await` (calling
//! `doc.status()`) typically yields long enough for the applier
//! task to be polled and subscribe. So the test is **non-strict**
//! against regressions — a regressed `run()` may still pass it on
//! a fast scheduler. It pins the post-condition (the contract) but
//! relies on the watcher test and the `round_trip` integration test
//! to catch real timing regressions. Without a peer it's hard to do
//! better; the applier only handles `InsertRemote` events.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use artel_client::Client;
use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_fs::{AttachPolicy, Workspace, path_to_key};
use artel_protocol::{PeerId, PeerInfo, Request, Response, SessionId};
use futures_util::StreamExt;
use iroh_docs::store::Query;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

const POLL: Duration = Duration::from_millis(20);
const BUDGET: Duration = Duration::from_secs(10);

struct DaemonHarness {
    _tempdir: TempDir,
    socket: PathBuf,
    shutdown: Arc<Shutdown>,
    join: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl DaemonHarness {
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
            address_lookup: None,
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

async fn host_session(client: &Client) -> SessionId {
    let peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    match client
        .request(Request::HostSession {
            peer,
            session: None,
        })
        .await
        .unwrap()
    {
        Response::HostSession { session, .. } => session,
        other => panic!("HostSession: got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn watcher_attached_when_run_resolves() {
    let harness = DaemonHarness::spawn().await;
    let client = Client::connect(&harness.socket).await.unwrap();
    let session = host_session(&client).await;

    let ws_dir = tempfile::tempdir().unwrap();
    let (ws, _ws_events) = Workspace::host(
        &client,
        session,
        ws_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
    )
    .await
    .expect("Workspace::host");
    let ws = Arc::new(ws);
    let handle = Arc::clone(&ws).run().await;

    // No settling sleep — `run().await` is the barrier. Write
    // immediately and expect the watcher to pick it up.
    let target = ws.root.join("hello.txt");
    let payload = b"watcher-ready";
    tokio::fs::write(&target, payload).await.unwrap();

    let key = path_to_key(ws.root.as_path(), &target).expect("path_to_key");
    let deadline = Instant::now() + BUDGET;
    let mut found = false;
    while Instant::now() < deadline {
        let stream = ws
            .doc()
            .get_many(Query::key_exact(key.clone()))
            .await
            .expect("get_many");
        tokio::pin!(stream);
        if stream.next().await.is_some() {
            found = true;
            break;
        }
        sleep(POLL).await;
    }
    assert!(
        found,
        "watcher must have published hello.txt to the doc within {BUDGET:?} \
         — if it didn't, run().await returned before the watcher attached",
    );

    ws.shutdown().await;
    let _ = timeout(Duration::from_secs(5), handle).await;
    drop(client);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn applier_subscribed_when_run_resolves() {
    let harness = DaemonHarness::spawn().await;
    let client = Client::connect(&harness.socket).await.unwrap();
    let session = host_session(&client).await;

    let ws_dir = tempfile::tempdir().unwrap();
    let (ws, _ws_events) = Workspace::host(
        &client,
        session,
        ws_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
    )
    .await
    .expect("Workspace::host");
    let ws = Arc::new(ws);

    // Snapshot the subscriber count before run — `Workspace::host`
    // does some internal subscribing of its own (e.g. iroh-docs's
    // start_sync path), so we can't assume zero. What we want to
    // assert is that the applier *adds one*.
    let pre = ws.doc().status().await.expect("status").subscribers;

    let handle = Arc::clone(&ws).run().await;

    // Immediately on return, the applier's subscriber must already
    // be registered. No polling, no sleep — this is the contract.
    let post = ws.doc().status().await.expect("status").subscribers;
    assert!(
        post > pre,
        "expected applier to add a subscriber by the time \
         run().await resolves: pre={pre}, post={post} \
         — if equal, run().await returned before the applier subscribed",
    );

    ws.shutdown().await;
    let _ = timeout(Duration::from_secs(5), handle).await;
    drop(client);
    harness.stop().await;
}
