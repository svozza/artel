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
use artel_fs::{AttachPolicy, Workspace, WorkspaceConfig};
use artel_protocol::capability::CapabilityAction;
use artel_protocol::{MessageKind, Request, Response, SendPayload};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

use common::{Pair, spawn_pair, testing_setup};

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

    let (alice_ws, _) = Workspace::host_with(
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
    common::grant_rw(&alice, session, bob_peer_id).await;

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

    // Wait for the revoke to propagate to Alice's cap-listener.
    sleep(Duration::from_secs(2)).await;

    // --- Phase 3: restart Alice's workspace. ---
    // The restart forces Alice's iroh-docs engine to establish fresh
    // outbound connections. The OutboundDialFilter should block dials
    // to Bob's workspace endpoint because the PeerMap (rebuilt from
    // the cap-listener's session-log replay) marks Bob as revoked.
    alice_ws.shutdown().await.expect("alice phase-1 shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;

    let alice2 = Client::connect(&daemon_a.socket).await.unwrap();
    let (alice_ws2, _) = Workspace::host_with(
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

    // Give generous time for sync to propagate (if it could).
    sleep(Duration::from_secs(8)).await;

    let alice_blocked = alice_dir.path().join("after_revoke.txt");
    assert!(
        !alice_blocked.exists(),
        "post-revoke write leaked to host via outbound sync — OutboundDialFilter did not block",
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
