//! Integration test: outbound dial filter blocks host's iroh-docs
//! engine from dialing revoked peers.
//!
//! Exercises the full outbound-block flow:
//! 1. Host + joiner set up workspaces with `daemon_socket` wired.
//! 2. Joiner writes a file → host sees it (baseline: sync works).
//! 3. Host revokes joiner via a `Capability/Revoke` session message.
//! 4. Host restarts its workspace (forces fresh outbound dial attempts).
//! 5. Joiner writes post-revoke → host never receives it.
//!
//! Combined with the existing `DocsGate` (inbound rejection), this
//! test proves the bidirectional sync gap is closed: neither inbound
//! nor outbound paths can leak revoked-peer data to the host.

mod common;

use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, Direction, Workspace, WorkspaceConfig, WorkspaceEvent};
use artel_protocol::capability::CapabilityAction;
use artel_protocol::{MessageKind, Request, Response, SendPayload};
use tempfile::TempDir;
use tokio::time::timeout;

use common::{Pair, spawn_pair, testing_setup, wait_for_event};

/// After revocation + host restart, the host's outbound dial filter
/// prevents iroh-docs from syncing with the revoked joiner.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn host_outbound_dial_blocked_after_revocation() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    // Alice hosts with daemon_socket set so the cap-listener is live.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_dir = TempDir::new().unwrap();

    let (alice_ws, mut alice_events) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_a.socket.clone()),
    )
    .await
    .expect("Workspace::host");

    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // Bob joins.
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let bob_peer_id = bob.daemon_peer_id();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = TempDir::new().unwrap();
    let (bob_ws, _) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_b.socket.clone()),
    )
    .await
    .expect("Workspace::join");

    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Grant Bob RW so he receives the NamespaceSecret upgrade and can
    // produce valid signed entries.
    common::grant_rw_and_wait(
        &alice,
        session,
        bob_peer_id,
        bob_dir.path(),
        alice_dir.path(),
    )
    .await;

    // --- Phase 1: baseline — Bob writes, Alice sees it. ---
    let baseline_path = bob_dir.path().join("before_revoke.txt");
    tokio::fs::write(&baseline_path, b"allowed write")
        .await
        .unwrap();

    let alice_baseline = alice_dir.path().join("before_revoke.txt");
    common::wait_for_file(&alice_baseline, b"allowed write").await;

    // --- Phase 2: revoke Bob ---
    let revoke = CapabilityAction::Revoke { peer: bob_peer_id };
    let resp = alice
        .request(Request::Send {
            session,
            payload: SendPayload {
                kind: MessageKind::Capability,
                action: revoke.action_str().to_string(),
                payload: revoke.encode(),
            },
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::Sent { .. }), "{resp:?}");

    // Wait until Alice's cap-listener has applied the revoke: the
    // moment `PeerRevoked` fires, her PeerMap marks Bob revoked, and
    // the restarted workspace below re-derives that same state from
    // session-log replay.
    wait_for_event(
        &mut alice_events,
        common::FILE_BUDGET,
        "PeerRevoked(bob)",
        |ev| matches!(ev, WorkspaceEvent::PeerRevoked { peer } if *peer == bob_peer_id),
    )
    .await;

    // --- Phase 3: restart Alice's workspace. ---
    // The restart forces Alice's iroh-docs engine to establish fresh
    // outbound connections. The PeerFilter should block dials
    // to Bob's workspace endpoint because the PeerMap (rebuilt from
    // the cap-listener's session-log replay) marks Bob as revoked.
    alice_ws.shutdown().await.expect("alice phase-1 shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;

    let alice2 = Client::connect(&daemon_a.socket).await.unwrap();
    let (alice_ws2, mut alice_events2) = Workspace::host_with(
        &alice2,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_a.socket.clone()),
    )
    .await
    .expect("Workspace::host phase 2");
    let alice_ws2 = Arc::new(alice_ws2);
    let alice_handle2 = Arc::clone(&alice_ws2).run().await;

    // --- Phase 4: Bob writes post-revoke — must NOT arrive at Alice. ---
    let blocked_path = bob_dir.path().join("after_revoke.txt");
    tokio::fs::write(&blocked_path, b"should be blocked")
        .await
        .unwrap();

    // Force the outbound dial rather than waiting for the engine to
    // decide to make one: after a restart, iroh-docs only dials peers
    // recovered from its persisted useful-peers table, on its own
    // schedule (observed flaky — some runs never dialed within the
    // budget). `Doc::start_sync(peers)` drives the same code path a
    // spontaneous dial would (`join_peers` → `sync_with_peer` →
    // `Endpoint::connect`), which must pass
    // `PeerFilter::before_connect` — the subject under test.
    let bob_workspace_id = iroh::EndpointId::from_bytes(
        &bob_ws
            .test_endpoint_id_bytes()
            .await
            .expect("bob node live"),
    )
    .expect("valid endpoint id");
    alice_ws2
        .doc()
        .start_sync(vec![iroh::EndpointAddr::new(bob_workspace_id)])
        .await
        .expect("start_sync toward bob");

    // Deterministic wait: the dial attempt to Bob is refused by
    // `PeerFilter::before_connect`, surfaced as a
    // `RevokedPeerBlocked { Outgoing }` event. (Bob's own inbound
    // attempts at Alice fire `Incoming` blocks too — the predicate
    // pins the direction under test.)
    wait_for_event(
        &mut alice_events2,
        common::FILE_BUDGET,
        "RevokedPeerBlocked { bob, Outgoing }",
        |ev| {
            matches!(
                ev,
                WorkspaceEvent::RevokedPeerBlocked {
                    peer,
                    direction: Direction::Outgoing,
                } if *peer == bob_peer_id
            )
        },
    )
    .await;

    // With the dial provably blocked, assert the payload never landed.
    // (No settling window needed: the blocked dial IS the only path
    // this data could have taken to Alice — her inbound gate rejects
    // Bob symmetrically.)
    let alice_blocked = alice_dir.path().join("after_revoke.txt");
    assert!(
        !alice_blocked.exists(),
        "post-revoke write leaked to host via outbound sync — PeerFilter did not block",
    );

    // Cleanup
    alice_ws2.shutdown().await.expect("alice shutdown");
    bob_ws.shutdown().await.expect("bob shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle2).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(alice2);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
