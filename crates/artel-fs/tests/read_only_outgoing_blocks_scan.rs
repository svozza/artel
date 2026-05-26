//! Scan-side rule check: a `ReadOnly` file pre-existing on disk
//! when the host attaches must not be published by
//! `scan_and_publish_existing`. Distinct from
//! `read_only_outgoing_blocks_publish.rs` which exercises the live
//! watcher path.

mod common;

use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, Mode, PathRule, PathRules, Workspace, WorkspaceConfig, path_to_key};
use artel_protocol::{PeerId, PeerInfo, Request, Response};

use common::{doc_has_key, wait_for_file};

#[tokio::test(flavor = "multi_thread")]
async fn scan_blocks_outgoing_read_only_preexisting_file() {
    let common::Pair {
        daemon_a,
        daemon_b,
        workspace_lookup_a,
        workspace_lookup_b,
    } = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let (session, ticket) = match alice
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

    // Pre-seed Alice's dir BEFORE the workspace is constructed, so
    // the secret goes through `scan_and_publish_existing` rather
    // than the live watcher path.
    let alice_dir = tempfile::tempdir().unwrap();
    tokio::fs::create_dir_all(alice_dir.path().join("secret"))
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("secret/key.txt"), b"top-secret")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("marker.txt"), b"go")
        .await
        .unwrap();

    let rules = PathRules {
        default: Mode::ReadWrite,
        rules: vec![PathRule {
            glob: "secret/**".into(),
            mode: Mode::ReadOnly,
        }],
    };

    let (alice_ws, _alice_events) = Workspace::host_with(
        &alice,
        session,
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default()
            .with_address_lookup_override(workspace_lookup_a)
            .with_rules(rules),
    )
    .await
    .expect("Workspace::host_with");
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
        WorkspaceConfig::default().with_address_lookup_override(workspace_lookup_b),
    )
    .await
    .expect("Workspace::join_with");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Marker propagated → scan completed → secret was either
    // published or skipped by now.
    wait_for_file(&bob_dir.path().join("marker.txt"), b"go").await;

    assert!(
        !bob_dir.path().join("secret/key.txt").exists(),
        "secret/key.txt leaked to bob via bulk-export",
    );

    let secret_key = path_to_key(
        alice_ws.root.as_path(),
        &alice_ws.root.join("secret/key.txt"),
    )
    .expect("path_to_key");
    assert!(
        !doc_has_key(alice_ws.doc(), &secret_key).await,
        "alice's scan regression: secret/key.txt landed in the doc",
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
