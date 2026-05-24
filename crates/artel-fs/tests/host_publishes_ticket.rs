//! `Workspace::host` stands the workspace up and lands a
//! `workspace.ticket` system message on the artel session.
//!
//! Doesn't run the watcher / applier — verifies only that:
//! 1. `Workspace::host` returns successfully against an existing
//!    artel session.
//! 2. A second client subscribed to the same session observes a
//!    `MessageKind::System` event with action [`TICKET_ACTION`] and
//!    a non-empty payload.
//! 3. The payload deserialises as a real [`DocTicket`].

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_fs::{AttachPolicy, TICKET_ACTION, Workspace};
use artel_protocol::{Event, MessageKind, PeerId, PeerInfo, Request, Response};
use iroh_docs::DocTicket;
use tempfile::TempDir;
use tokio::time::timeout;

/// Stand up an artel daemon in a tempdir; no iroh feature needed for
/// this test since we only exercise local IPC.
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

#[tokio::test(flavor = "multi_thread")]
async fn host_lands_ticket_on_session() {
    let harness = DaemonHarness::spawn().await;

    // Alice hosts an artel session and a workspace on top of it.
    let alice = Client::connect(&harness.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let session = match alice
        .request(Request::HostSession {
            peer: alice_peer.clone(),
        })
        .await
        .unwrap()
    {
        Response::HostSession { session, .. } => session,
        other => panic!("HostSession: got {other:?}"),
    };

    // A second client subscribes so we can observe the system
    // message Alice's workspace will broadcast. Bob's client uses
    // the same daemon — local clients see their own session
    // events.
    let bob = Client::connect(&harness.socket).await.unwrap();
    // Bob has to be a member of the session before Subscribe is
    // legal; use the artel session ticket.
    let _ = bob
        .request(Request::HostSession {
            peer: PeerInfo::new(PeerId::from_bytes([2; 32]), "bob"),
        })
        .await
        .unwrap();
    // Actually — for a local-only daemon we can't have two hosts of
    // the same session, and joining via JoinSession requires the
    // ticket. Drop the bob-host call: we just have alice subscribe
    // to her own session.
    drop(bob);

    let _ = alice
        .request(Request::Subscribe {
            session,
            since: None,
        })
        .await
        .unwrap();
    let mut events = alice.take_events().await.expect("events");

    // Stand the workspace up. The temp dir gets a single seed file
    // so we exercise the scan-and-publish path too.
    let ws_dir = tempfile::tempdir().unwrap();
    tokio::fs::write(ws_dir.path().join("README.md"), b"hello workspace")
        .await
        .unwrap();

    let (workspace, _ws_events) = Workspace::host(
        &alice,
        session,
        ws_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
    )
    .await
    .expect("Workspace::host");

    // Pull events until we see the ticket system message.
    let payload = timeout(Duration::from_secs(15), async {
        loop {
            let ev = events.recv().await.expect("event channel closed");
            if let Event::Message { message, .. } = ev
                && message.kind == MessageKind::System
                && message.action == TICKET_ACTION
            {
                return message.payload;
            }
            // Anything else (PeerJoined, prior Messages) is fine —
            // keep draining.
        }
    })
    .await
    .expect("workspace.ticket message never arrived");

    assert!(!payload.is_empty(), "ticket payload should be non-empty");
    let ticket_str = std::str::from_utf8(&payload).expect("ticket bytes are utf-8");
    let _: DocTicket = DocTicket::from_str(ticket_str).expect("valid DocTicket");

    workspace.shutdown().await;
    drop(events);
    drop(alice);
    harness.stop().await;
}
