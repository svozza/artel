//! Regression trap for the chat-harness "alice's messages stop
//! reaching bob after alice's restart" bug observed during the
//! lost-message investigation (`docs/handoff-post-workspace-registry.md`,
//! § "Open investigation: lost messages on fast Ctrl-C").
//!
//! Shape:
//!
//! 1. Alice (host) and bob (joiner) come up. Alice writes a file →
//!    bob receives it. Pre-restart sanity.
//! 2. Bob writes a file → alice receives it. Bidirectional pre-restart.
//! 3. Alice's workspace shuts down. Alice's daemon stops. Bob's
//!    workspace and daemon stay alive throughout.
//! 4. Alice's daemon respawns at the same paths. Alice's workspace
//!    re-attaches via `host_with` (returning host).
//! 5. Bob writes a file → alice receives it. (Bob's outbound is the
//!    cheap-to-pin direction; we expect this to work because the
//!    iroh-docs reconcile-on-connect surfaces it.)
//! 6. Alice writes a file → **bob must receive it**. This is the
//!    failing case observed in the chat harness: alice's `set_bytes`
//!    succeeds, alice's local replica has the entry, but bob's
//!    iroh-gossip never delivers an `InsertRemote`.
//!
//! Step 6 is the load-bearing assertion. With the bug present, the
//! test should fail at `wait_for_file(bob/post_restart_alice.txt)`.
//!
//! NOT in scope:
//! - The harness's per-launch fresh-ulid filename. The substrate
//!   bug also reproduces with same-key follow-up writes, so this
//!   test pins the simpler "any post-restart write from alice"
//!   shape. Once that's fixed, a follow-up test for repeated writes
//!   to the same key would be additive.
//! - Joiner-side restart symmetry. Worth a separate trap once we
//!   know what the alice-side fix shape is.

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

use common::{DaemonPaths, FILE_BUDGET, Pair, spawn_daemon_at, spawn_pair, wait_for_file};

const TICKET_BUDGET: Duration = Duration::from_secs(15);

#[tokio::test(flavor = "multi_thread")]
async fn alice_post_restart_writes_reach_bob() {
    // Alice's daemon-state dir survives restart; the `RunningDaemon`
    // in `Pair` owns its own tempdir which gets wiped on stop, so we
    // don't use the convenience harness for alice. Bob keeps the
    // standard harness because his daemon doesn't restart.
    let alice_daemon_root = TempDir::new().unwrap();
    let alice_paths = DaemonPaths::at(alice_daemon_root.path());
    let alice_root = TempDir::new().unwrap();
    let alice_wstate = TempDir::new().unwrap();
    let bob_root = TempDir::new().unwrap();
    let bob_wstate = TempDir::new().unwrap();

    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");

    // Phase 1: bring alice's daemon up alongside bob's via spawn_pair,
    // but immediately stop alice's so we can respawn at our own
    // caller-owned paths. The reason we still go through spawn_pair
    // is to seed the shared MemoryLookup with bob's iroh addr — we
    // need the same lookup handle to survive across alice's daemon
    // respawns so bob remains addressable from alice's perspective.
    let Pair {
        daemon_a: alice_throwaway,
        daemon_b: bob_daemon,
        workspace_lookup_a: alice_workspace_lookup,
        workspace_lookup_b: bob_workspace_lookup,
    } = spawn_pair().await;
    // We don't use the throwaway alice daemon — drop the handle (which
    // stops it) and respawn one we own at the right paths.
    alice_throwaway.stop().await;

    let alice_daemon = spawn_daemon_at(&alice_paths, Some(alice_workspace_lookup.clone())).await;
    // Re-seed alice's daemon iroh-addr into the shared lookup so bob
    // can dial alice when alice's workspace tries to reconcile.
    if let Some(addr) = alice_daemon.iroh_addr.clone() {
        alice_workspace_lookup.add_endpoint_info(addr);
    }

    // Phase 1: alice hosts, bob joins, exchange one file each way to
    // confirm baseline propagation before any restarts.
    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_address_lookup_override(alice_workspace_lookup.clone());
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

    let bob_cfg = WorkspaceConfig::default()
        .with_state_dir(bob_wstate.path().to_path_buf())
        .with_address_lookup_override(bob_workspace_lookup.clone());
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

    // Pre-restart bidirectional sanity. If either of these fails we
    // know the harness itself is broken and the post-restart
    // assertions further down are uninterpretable.
    tokio::fs::write(alice_root.path().join("pre_alice.txt"), b"alpha")
        .await
        .unwrap();
    wait_for_file(&bob_root.path().join("pre_alice.txt"), b"alpha").await;

    tokio::fs::write(bob_root.path().join("pre_bob.txt"), b"bravo")
        .await
        .unwrap();
    wait_for_file(&alice_root.path().join("pre_bob.txt"), b"bravo").await;

    // Phase 2: alice's side goes down. Bob stays alive — his
    // workspace and daemon keep running. This mirrors the chat-
    // harness scenario where alice Ctrl-Cs and re-launches while
    // bob's window is untouched.
    alice_ws.shutdown().await;
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    drop(alice_events);
    drop(alice);
    alice_daemon.stop().await;

    // Phase 3: alice respawns. Same daemon-state dir, same workspace
    // state dir, same root.
    let alice_daemon = spawn_daemon_at(&alice_paths, Some(alice_workspace_lookup.clone())).await;
    if let Some(addr) = alice_daemon.iroh_addr.clone() {
        alice_workspace_lookup.add_endpoint_info(addr);
    }
    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_address_lookup_override(alice_workspace_lookup.clone());
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

    // Phase 4: bob → alice direction first. The chat-harness
    // observation was that bob → alice still worked after alice's
    // restart; pin that here so a regression that breaks *both*
    // directions is distinguishable from one that only breaks
    // alice → bob.
    tokio::fs::write(bob_root.path().join("post_restart_bob.txt"), b"charlie")
        .await
        .unwrap();
    wait_for_file(&alice_root.path().join("post_restart_bob.txt"), b"charlie").await;

    // Phase 5: alice → bob. This is the load-bearing assertion. With
    // the bug, alice's `set_bytes` succeeds locally but bob's
    // gossip-side never delivers an `InsertRemote`. The test fails
    // here at the `FILE_BUDGET` deadline.
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

// Pull `FILE_BUDGET` into scope so a future tightening of test
// budgets in `common` flows through here too.
const _: Duration = FILE_BUDGET;
