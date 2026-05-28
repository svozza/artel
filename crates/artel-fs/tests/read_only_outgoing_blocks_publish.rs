//! Watcher-side rule check: a `ReadOnly` path written *after*
//! `Workspace::run` must not reach the doc, must not reach Bob, and
//! must surface as `WorkspaceEvent::SkippedReadOnly { Outgoing }`.
//!
//! Defence in depth: we inspect Alice's doc directly to confirm the
//! watcher dropped the change at source, not just that Bob's applier
//! filtered it. A leaked publish would still propagate to a third
//! joiner.

mod common;

use common::testing_setup;

use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{
    AttachPolicy, Direction, Mode, PathRule, PathRules, Workspace, WorkspaceConfig, WorkspaceEvent,
    path_to_key,
};
use artel_protocol::{PeerId, PeerInfo, Request, Response};

use common::{doc_has_key, wait_for_file};

#[tokio::test(flavor = "multi_thread")]
async fn watcher_blocks_outgoing_read_only_write() {
    let common::Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let rules = PathRules {
        default: Mode::ReadWrite,
        rules: vec![PathRule {
            glob: "secret/**".into(),
            mode: Mode::ReadOnly,
        }],
    };

    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, mut alice_events) = Workspace::host_with(
        &alice,
        alice_peer,
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_rules(rules),
    )
    .await
    .expect("Workspace::host_with");
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
    let resp = bob
        .request(Request::JoinSession {
            peer: bob_peer,
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, _bob_events) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join_with");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Write a secret + a sentinel marker. The marker propagates
    // (default ReadWrite); the secret must not.
    tokio::fs::create_dir_all(alice_dir.path().join("secret"))
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("secret/key.txt"), b"top-secret")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("marker.txt"), b"go")
        .await
        .unwrap();

    // Wait for marker on Bob; afterwards we know the secret event
    // (which preceded it) has been processed by Alice's watcher.
    wait_for_file(&bob_dir.path().join("marker.txt"), b"go").await;

    // Bob never sees the secret.
    assert!(
        !bob_dir.path().join("secret/key.txt").exists(),
        "secret/key.txt leaked to bob",
    );

    // Defence in depth: Alice's doc has no entry for the secret key.
    let secret_key = path_to_key(
        alice_ws.root.as_path(),
        &alice_ws.root.join("secret/key.txt"),
    )
    .expect("path_to_key");
    assert!(
        !doc_has_key(alice_ws.doc(), &secret_key).await,
        "alice's watcher regression: secret/key.txt landed in the doc",
    );

    // The watcher emitted `SkippedReadOnly { Outgoing }` for the
    // secret. Drain Alice's events and assert at least one matched.
    let mut saw_skip = false;
    while let Ok(ev) = alice_events.try_recv() {
        if let WorkspaceEvent::SkippedReadOnly { path, direction } = ev
            && direction == Direction::Outgoing
            && path.ends_with("secret/key.txt")
        {
            saw_skip = true;
            break;
        }
    }
    assert!(
        saw_skip,
        "expected SkippedReadOnly{{Outgoing}} event for secret/key.txt",
    );

    alice_ws.shutdown().await;
    bob_ws.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), alice_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
