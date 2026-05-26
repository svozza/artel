//! Workspace state survives a process-graceful restart on both
//! host and joiner.
//!
//! Alice hosts a workspace, Bob joins, both shut down cleanly,
//! both come back later (against fresh artel daemons) and pick up
//! where they left off — without losing files, without the
//! workspace ticket invalidating, and without breaking delete
//! propagation.
//!
//! Load-bearing pieces:
//! - `iroh.key` keeps the host's `EndpointId` / `NodeId` stable.
//! - `doc-id` keeps the host's `NamespaceId` stable.
//! - `Docs::persistent` + `FsStore` retain doc + blob state on the
//!   joiner side, so a returning joiner doesn't lose its synced
//!   files even if the host hasn't started yet.
//! - `reconcile_doc_against_disk` propagates a delete that
//!   happened *while the host was down* to peers on next start.

mod common;

use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use artel_client::Client;
use artel_fs::{AttachPolicy, TICKET_ACTION, Workspace, WorkspaceConfig, ticket as fs_ticket};
use artel_protocol::{Event, MessageKind, PeerId, PeerInfo, Request, Response, SessionId};
use iroh_docs::DocTicket;
use tokio::time::{sleep, timeout};

const POLL: Duration = Duration::from_millis(100);
const FILE_BUDGET: Duration = Duration::from_secs(20);
const TICKET_BUDGET: Duration = Duration::from_secs(15);

// Long, deliberately linear two-phase scenario: extracting per-phase
// helpers obscured the order more than the length hurts.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread")]
async fn workspace_state_survives_graceful_restart() {
    // Workspace state dirs and content roots live in tempdirs that
    // outlive the daemons — they get torn down between phases, the
    // workspace state must not.
    let alice_root = tempfile::tempdir().unwrap();
    let alice_wstate = tempfile::tempdir().unwrap();
    let bob_root = tempfile::tempdir().unwrap();
    let bob_wstate = tempfile::tempdir().unwrap();

    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");

    // -----------------------------------------------------------
    // Phase 1: first lifetime of the workspaces.
    // -----------------------------------------------------------
    tokio::fs::write(alice_root.path().join("a.txt"), b"alpha")
        .await
        .unwrap();

    let phase1_ticket = {
        let common::Pair {
            daemon_a,
            daemon_b,
            workspace_lookup_a,
            workspace_lookup_b,
        } = common::spawn_pair().await;

        let alice = Client::connect(&daemon_a.socket).await.unwrap();
        let (session, artel_ticket) = match alice
            .request(Request::HostSession {
                peer: alice_peer.clone(),
                session: None,
            })
            .await
            .unwrap()
        {
            Response::HostSession { session, ticket } => (session, ticket),
            other => panic!("HostSession: got {other:?}"),
        };

        // Subscribe before standing the workspace up so we don't
        // miss the broadcast.
        let _ = alice
            .request(Request::Subscribe {
                session,
                since: None,
            })
            .await
            .unwrap();
        let mut alice_events = alice.take_events().await.expect("alice events");

        let alice_cfg = WorkspaceConfig::default()
            .with_state_dir(alice_wstate.path().to_path_buf())
            .with_address_lookup_override(workspace_lookup_a);
        let (alice_ws, _alice_ws_events) = Workspace::host_with(
            &alice,
            session,
            alice_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            alice_cfg,
        )
        .await
        .expect("Workspace::host_with");
        let alice_ws = Arc::new(alice_ws);
        let alice_handle = Arc::clone(&alice_ws).run().await;

        let phase1_ticket = capture_ticket(&mut alice_events, session).await;

        // Bob joins.
        let bob = Client::connect(&daemon_b.socket).await.unwrap();
        let resp = bob
            .request(Request::JoinSession {
                peer: bob_peer.clone(),
                ticket: artel_ticket,
            })
            .await
            .unwrap();
        assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

        let bob_cfg = WorkspaceConfig::default()
            .with_state_dir(bob_wstate.path().to_path_buf())
            .with_address_lookup_override(workspace_lookup_b);
        let (bob_ws, _bob_ws_events) = Workspace::join_with(
            &bob,
            session,
            bob_root.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            bob_cfg,
        )
        .await
        .expect("Workspace::join_with");
        let bob_ws = Arc::new(bob_ws);
        let bob_handle = Arc::clone(&bob_ws).run().await;

        // Sanity: a.txt makes it to bob.
        wait_for_file(&bob_root.path().join("a.txt"), b"alpha").await;

        alice_ws.shutdown().await;
        bob_ws.shutdown().await;
        let _ = tokio::time::timeout(Duration::from_secs(5), alice_handle).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), bob_handle).await;
        drop(alice_events);
        drop(alice);
        drop(bob);
        daemon_a.stop().await;
        daemon_b.stop().await;

        phase1_ticket
    };

    // Workspace state survived the shutdown.
    assert!(
        alice_wstate.path().join("iroh.key").exists(),
        "alice iroh.key should persist"
    );
    assert!(
        alice_wstate.path().join("doc-id").exists(),
        "alice doc-id should persist"
    );
    assert!(
        bob_wstate.path().join("iroh.key").exists(),
        "bob iroh.key should persist"
    );
    assert!(
        !bob_wstate.path().join("doc-id").exists(),
        "joiners must not write doc-id (host owns the namespace)",
    );

    // Between-lifetimes mutation: delete a.txt from alice's disk
    // while alice is offline. The reconcile pass on the next host
    // restart should propagate the delete to bob.
    tokio::fs::remove_file(alice_root.path().join("a.txt"))
        .await
        .unwrap();

    // -----------------------------------------------------------
    // Phase 2: fresh daemons, same workspace state dirs.
    // -----------------------------------------------------------
    let common::Pair {
        daemon_a,
        daemon_b,
        workspace_lookup_a,
        workspace_lookup_b,
    } = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let (session, artel_ticket) = match alice
        .request(Request::HostSession {
            peer: alice_peer,
            session: None,
        })
        .await
        .unwrap()
    {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("HostSession (phase 2): got {other:?}"),
    };

    let _ = alice
        .request(Request::Subscribe {
            session,
            since: None,
        })
        .await
        .unwrap();
    let mut alice_events = alice.take_events().await.expect("alice events");

    let alice_cfg = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_address_lookup_override(workspace_lookup_a);
    let (alice_ws, _alice_ws_events) = Workspace::host_with(
        &alice,
        session,
        alice_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        alice_cfg,
    )
    .await
    .expect("Workspace::host_with phase 2");
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let phase2_ticket = capture_ticket(&mut alice_events, session).await;

    // Identity stability: NamespaceId stable across restarts, host
    // NodeId stable across restarts. Address-discovery info inside a
    // ticket can drift legitimately (e.g. relay URL list ordering),
    // so we don't assert byte-identity of the whole ticket — only
    // the structural identity that consumers actually depend on.
    assert_eq!(
        phase1_ticket.capability.id(),
        phase2_ticket.capability.id(),
        "NamespaceId must be stable across host restart",
    );
    let nodes_1: Vec<_> = phase1_ticket.nodes.iter().map(|n| n.id).collect();
    let nodes_2: Vec<_> = phase2_ticket.nodes.iter().map(|n| n.id).collect();
    assert_eq!(
        nodes_1, nodes_2,
        "host NodeId(s) must be stable across host restart",
    );

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            peer: bob_peer,
            ticket: artel_ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_cfg = WorkspaceConfig::default()
        .with_state_dir(bob_wstate.path().to_path_buf())
        .with_address_lookup_override(workspace_lookup_b);
    let (bob_ws, _bob_ws_events) = Workspace::join_with(
        &bob,
        session,
        bob_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        bob_cfg,
    )
    .await
    .expect("Workspace::join_with phase 2");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Reconcile-driven delete propagates to bob.
    wait_for_missing(&bob_root.path().join("a.txt")).await;

    // Live sync resumed both ways. No settling sleep needed —
    // `Workspace::run().await` only resolves once the watcher is
    // attached.
    tokio::fs::write(alice_root.path().join("b.txt"), b"beta")
        .await
        .unwrap();
    wait_for_file(&bob_root.path().join("b.txt"), b"beta").await;

    tokio::fs::write(bob_root.path().join("c.txt"), b"charlie")
        .await
        .unwrap();
    wait_for_file(&alice_root.path().join("c.txt"), b"charlie").await;

    // Delete after restart still propagates.
    tokio::fs::remove_file(alice_root.path().join("b.txt"))
        .await
        .unwrap();
    wait_for_missing(&bob_root.path().join("b.txt")).await;

    alice_ws.shutdown().await;
    bob_ws.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), alice_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice_events);
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

/// Drain `events` until the workspace ticket lands; decode the
/// envelope and return the embedded `DocTicket`. The rules half of
/// the envelope is dropped — this test only cares about
/// `NamespaceId` / `NodeId` stability.
async fn capture_ticket(events: &mut artel_client::EventStream, session: SessionId) -> DocTicket {
    let payload = timeout(TICKET_BUDGET, async {
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
    DocTicket::from_str(&envelope.doc_ticket).expect("ticket parse")
}

async fn wait_for_file(path: &Path, expected: &[u8]) {
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
        sleep(POLL).await;
    }
}

async fn wait_for_missing(path: &Path) {
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
        sleep(POLL).await;
    }
}
