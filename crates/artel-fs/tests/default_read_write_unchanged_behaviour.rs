//! Default-permissive `PathRules` (the implicit case for every
//! `WorkspaceConfig::default()` consumer) gives exactly the
//! pre-rules behaviour.
//!
//! The watcher, applier, scan, and bulk-export each consult
//! `Workspace::compiled_rules` per event. This test guards against
//! accidental behavioural drift on the 100% case where rules are
//! absent — if it ever fails, the rule check has unintentionally
//! changed the default-permissive path.

mod common;

use common::testing_setup;

use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, Workspace, WorkspaceConfig};
use artel_protocol::{PeerId, PeerInfo, Request, Response};

use common::{wait_for_file, wait_for_missing};

#[tokio::test(flavor = "multi_thread")]
async fn default_rules_give_unchanged_round_trip() {
    let common::Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");

    let alice_dir = tempfile::tempdir().unwrap();
    // Pre-existing file exercises `scan_and_publish_existing` on the
    // default-permissive path.
    tokio::fs::write(alice_dir.path().join("preseed.txt"), b"hello")
        .await
        .unwrap();

    let (alice_ws, _) = Workspace::host_with(
        &alice,
        alice_peer,
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
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
    let (bob_ws, _) = Workspace::join_with(
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

    // Bulk-export: pre-seed reaches Bob.
    wait_for_file(&bob_dir.path().join("preseed.txt"), b"hello").await;

    // Live edit: outgoing watcher path.
    tokio::fs::write(alice_dir.path().join("live.txt"), b"world")
        .await
        .unwrap();
    wait_for_file(&bob_dir.path().join("live.txt"), b"world").await;

    // Live delete.
    tokio::fs::remove_file(alice_dir.path().join("live.txt"))
        .await
        .unwrap();
    wait_for_missing(&bob_dir.path().join("live.txt")).await;

    alice_ws.shutdown().await;
    bob_ws.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), alice_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
