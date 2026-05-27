//! Re-hosting the same workspace dir produces a structurally
//! identical ticket: same `NamespaceId`, same host `NodeId(s)`. This
//! is what lets existing joiners' tickets keep working across host
//! restart (3b-1 persistence guarantees the on-disk substrate; this
//! test pins the consumer-visible property).
//!
//! `disk_resume.rs` already asserts the same properties inside a
//! larger two-daemon end-to-end scenario. This test scopes them to
//! the host side only — no joiner, no daemon swap, no live sync —
//! so a regression in just the resume-ticket-stability property
//! surfaces fast (~2s) with an unambiguous failure mode.
//!
//! NOT asserted: byte-identity of the whole ticket. Address-discovery
//! info inside a ticket can drift legitimately (e.g. relay URL list
//! ordering); structural identity is what consumers actually depend
//! on. See `disk_resume.rs::workspace_state_survives_graceful_restart`
//! for the same reasoning.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_fs::{AttachPolicy, TICKET_ACTION, Workspace, WorkspaceConfig, ticket as fs_ticket};
use artel_protocol::{Event, MessageKind, PeerId, PeerInfo, Request, SessionId};
use iroh_docs::DocTicket;
use tempfile::TempDir;
use tokio::time::timeout;

/// Stand up an iroh-disabled daemon in a tempdir. Mirrors
/// `host_publishes_ticket.rs`'s harness — the resume-stability
/// property is about per-workspace iroh state, not the daemon's
/// iroh layer, so the daemon doesn't need to be iroh-enabled.
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
async fn re_hosting_same_dir_yields_structurally_identical_ticket() {
    let harness = DaemonHarness::spawn().await;

    // Workspace dir outlives both phases. The state dir
    // (`<root>/.artel-fs/`) is what carries `iroh.key` + `doc-id`
    // across the host/re-host boundary.
    let ws_dir = tempfile::tempdir().unwrap();

    let phase1 = host_once_and_capture_ticket(&harness, ws_dir.path().to_path_buf()).await;
    let phase2 = host_once_and_capture_ticket(&harness, ws_dir.path().to_path_buf()).await;

    // NamespaceId stable: existing joiners' tickets keep referring to
    // the same doc.
    assert_eq!(
        phase1.capability.id(),
        phase2.capability.id(),
        "NamespaceId must be stable across host restart",
    );

    // Host NodeId(s) stable: joiners can still dial the host.
    let nodes_1: Vec<_> = phase1.nodes.iter().map(|n| n.id).collect();
    let nodes_2: Vec<_> = phase2.nodes.iter().map(|n| n.id).collect();
    assert_eq!(
        nodes_1, nodes_2,
        "host NodeId(s) must be stable across host restart",
    );

    harness.stop().await;
}

/// Stand up a workspace, capture the published ticket, shut down.
/// Returns the inner `DocTicket` decoded from the
/// `WorkspaceTicketEnvelope`.
async fn host_once_and_capture_ticket(harness: &DaemonHarness, root: PathBuf) -> DocTicket {
    let alice = Client::connect(&harness.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");

    let (workspace, _ws_events) = Workspace::host_with(
        &alice,
        alice_peer,
        root,
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default(),
    )
    .await
    .expect("Workspace::host_with");
    let session = workspace.session_id();

    // Subscribe *after* host returns. The daemon's replay path
    // surfaces the workspace.ticket system message published during
    // host even for late subscribers.
    let _ = alice
        .request(Request::Subscribe {
            session,
            since: None,
        })
        .await
        .unwrap();
    let mut events = alice.take_events().await.expect("events");

    let ticket = drain_until_ticket(&mut events, session).await;

    workspace.shutdown().await;
    drop(events);
    drop(alice);
    ticket
}

/// Drain `events` until the workspace ticket lands; decode the
/// envelope and parse the inner `DocTicket`.
async fn drain_until_ticket(
    events: &mut artel_client::EventStream,
    session: SessionId,
) -> DocTicket {
    let payload = timeout(Duration::from_secs(15), async {
        loop {
            let ev = events.recv().await.expect("event channel closed");
            if let Event::Message {
                session: ev_session,
                message,
            } = ev
                && ev_session == session
                && message.kind == MessageKind::System
                && message.action == TICKET_ACTION
            {
                return message.payload;
            }
        }
    })
    .await
    .expect("workspace.ticket never arrived");

    let envelope = fs_ticket::decode(&payload).expect("envelope decode");
    DocTicket::from_str(&envelope.doc_ticket).expect("DocTicket parse")
}
