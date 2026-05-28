//! Post-join live writes to a `ReadOnly` zone are blocked by the
//! watcher and never propagate. Same idea as
//! `read_only_outgoing_blocks_publish.rs` but specifically with the
//! sentinel write happening *after* both sides have joined and run
//! their watchers — guards against an "only the cold path is gated"
//! regression.

mod common;

use common::testing_setup;

use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, Mode, PathRule, PathRules, Workspace, WorkspaceConfig, path_to_key};
use artel_protocol::{PeerId, PeerInfo, Request, Response};

use common::{doc_has_key, wait_for_file};

#[tokio::test(flavor = "multi_thread")]
async fn post_join_live_write_to_read_only_zone_is_blocked() {
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
            glob: "locked/**".into(),
            mode: Mode::ReadOnly,
        }],
    };

    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _) = Workspace::host_with(
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

    // Now, post-join, post-run, write into the locked zone.
    tokio::fs::create_dir_all(alice_dir.path().join("locked"))
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("locked/x.txt"), b"locked-data")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("marker.txt"), b"go")
        .await
        .unwrap();

    wait_for_file(&bob_dir.path().join("marker.txt"), b"go").await;

    assert!(
        !bob_dir.path().join("locked/x.txt").exists(),
        "locked/x.txt leaked to bob (post-join)",
    );

    let locked_key = path_to_key(alice_ws.root.as_path(), &alice_ws.root.join("locked/x.txt"))
        .expect("path_to_key");
    assert!(
        !doc_has_key(alice_ws.doc(), &locked_key).await,
        "alice's post-join watcher regression: locked/x.txt landed in the doc",
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
