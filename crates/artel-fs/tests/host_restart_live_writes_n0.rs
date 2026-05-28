//! Real-n0 variant of [`host_restart_live_writes`].
//!
//! The MemoryLookup-based sibling test passes — meaning whatever's
//! breaking in the chat harness either (a) is specific to the n0
//! production discovery / relay path, or (b) is a harness-side
//! issue that the substrate doesn't have. This test runs the same
//! shape against full n0 to distinguish those two: if it passes too,
//! the harness is the culprit; if it fails, it's a substrate bug
//! tied to n0-specific transport state.
//!
//! Why this is its own file: the `iroh_docs_smoke` flake noted in
//! `docs/handoff-post-workspace-registry.md` § "What's pinned by
//! tests" is a real concern — n0's pkarr+DNS rate-limits under
//! back-to-back test load. Keeping this test isolated from the
//! deterministic `MemoryLookup` path means a flake here doesn't
//! mask genuine regressions on the dev-time path. The
//! `MemoryLookup` test covers the substrate property in CI; this
//! one is the production-path canary.

#![allow(clippy::too_many_lines)]

mod common;

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, TICKET_ACTION, Workspace, WorkspaceConfig, ticket as fs_ticket};
use artel_protocol::{Event, MessageKind, PeerId, PeerInfo, Request, Response};
use iroh_docs::DocTicket;
use tempfile::TempDir;
use tokio::time::timeout;

use common::{DaemonPaths, fresh_state, spawn_daemon_at, wait_for_file};

const TICKET_BUDGET: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread")]
async fn alice_post_restart_writes_reach_bob_real_n0() {
    // Caller-owned dir for alice's daemon state so it survives the
    // restart. Bob's daemon dir lives inside a `RunningDaemon` /
    // `DaemonState` from the helper because bob doesn't restart.
    let alice_daemon_root = TempDir::new().unwrap();
    let alice_paths = DaemonPaths::at(alice_daemon_root.path());
    let alice_root = TempDir::new().unwrap();
    let alice_wstate = TempDir::new().unwrap();
    let bob_root = TempDir::new().unwrap();
    let bob_wstate = TempDir::new().unwrap();

    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");

    // Real n0 discovery — both daemons get `None` for address
    // lookup, so they go through pkarr + DNS like production.
    let alice_daemon = spawn_daemon_at(&alice_paths, None).await;
    let bob_daemon_state = fresh_state();
    let bob_paths = DaemonPaths::at(bob_daemon_state.root.path());
    let bob_daemon = spawn_daemon_at(&bob_paths, None).await;

    // Phase 1: alice hosts, bob joins, exchange one file each way.
    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default().with_state_dir(alice_wstate.path().to_path_buf());
    let (alice_ws, _alice_ws_events) = Workspace::host_with(
        &alice,
        alice_peer.clone(),
        alice_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        alice_cfg,
    )
    .await
    .expect("Workspace::host_with phase 1");
    let session = alice_ws.session_id();
    let artel_ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();

    let _ = alice
        .request(Request::Subscribe {
            session,
            since: None,
        })
        .await
        .unwrap();
    let mut alice_events = alice.take_events().await.expect("alice events");
    let _phase1_ticket = capture_ticket(&mut alice_events, session).await;

    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&bob_daemon.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            peer: bob_peer.clone(),
            ticket: artel_ticket,
        })
        .await
        .unwrap();
    let bob_session = match resp {
        Response::JoinSession { session, .. } => session,
        other => panic!("JoinSession: got {other:?}"),
    };
    assert_eq!(bob_session, session, "joiner must land on same session id");

    let bob_cfg = WorkspaceConfig::default().with_state_dir(bob_wstate.path().to_path_buf());
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

    tokio::fs::write(alice_root.path().join("pre_alice.txt"), b"alpha")
        .await
        .unwrap();
    wait_for_file(&bob_root.path().join("pre_alice.txt"), b"alpha").await;

    tokio::fs::write(bob_root.path().join("pre_bob.txt"), b"bravo")
        .await
        .unwrap();
    wait_for_file(&alice_root.path().join("pre_bob.txt"), b"bravo").await;

    // Phase 2: alice's side goes down. Bob stays alive throughout.
    alice_ws.shutdown().await;
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    drop(alice_events);
    drop(alice);
    alice_daemon.stop().await;

    // Phase 3: alice respawns at the same daemon-state and
    // workspace-state dirs. iroh.key + doc-id keep her identity
    // stable; n0 discovery has to find bob via pkarr/DNS again.
    let alice_daemon = spawn_daemon_at(&alice_paths, None).await;
    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default().with_state_dir(alice_wstate.path().to_path_buf());
    let (alice_ws, _alice_ws_events) = Workspace::host_with(
        &alice,
        alice_peer,
        alice_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        alice_cfg,
    )
    .await
    .expect("Workspace::host_with phase 2");
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // Phase 4: bob → alice. Observed working in the chat harness;
    // pinning here so a regression that breaks both directions
    // surfaces cleanly.
    tokio::fs::write(bob_root.path().join("post_restart_bob.txt"), b"charlie")
        .await
        .unwrap();
    wait_for_file(&alice_root.path().join("post_restart_bob.txt"), b"charlie").await;

    // Phase 5: alice → bob. THE LOAD-BEARING ASSERTION.
    tokio::fs::write(alice_root.path().join("post_restart_alice.txt"), b"delta")
        .await
        .unwrap();
    wait_for_file(&bob_root.path().join("post_restart_alice.txt"), b"delta").await;

    // Cleanup.
    alice_ws.shutdown().await;
    bob_ws.shutdown().await;
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    alice_daemon.stop().await;
    bob_daemon.stop().await;
    drop(alice_daemon_root);
    drop(alice_root);
    drop(alice_wstate);
    drop(bob_root);
    drop(bob_wstate);
    drop(bob_daemon_state);
}

async fn capture_ticket(
    events: &mut artel_client::EventStream,
    session: artel_protocol::SessionId,
) -> DocTicket {
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
    DocTicket::from_str(&envelope.doc_ticket).expect("DocTicket parse")
}
