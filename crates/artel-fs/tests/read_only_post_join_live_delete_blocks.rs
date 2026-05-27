//! `on_removed` rule check: a delete on a `ReadOnly` path must NOT
//! publish a tombstone. Specifically tests the watcher's `on_removed`
//! gate (Linux `Remove` events arrive there directly).

mod common;

use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, Mode, PathRule, PathRules, Workspace, WorkspaceConfig, path_to_key};
use artel_protocol::{PeerId, PeerInfo, Request, Response};
use futures_util::StreamExt;
use iroh_docs::store::Query;

use common::wait_for_file;

#[tokio::test(flavor = "multi_thread")]
async fn on_removed_does_not_tombstone_read_only_path() {
    let common::Pair {
        daemon_a,
        daemon_b,
        workspace_lookup_a,
        workspace_lookup_b,
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
            .with_address_lookup_override(workspace_lookup_a)
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
        WorkspaceConfig::default().with_address_lookup_override(workspace_lookup_b),
    )
    .await
    .expect("Workspace::join_with");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Write a locked file (publish blocked by `on_modified` rule
    // check), then delete it (delete must be blocked by `on_removed`
    // rule check, otherwise a tombstone propagates).
    tokio::fs::create_dir_all(alice_dir.path().join("locked"))
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("locked/y.txt"), b"some-data")
        .await
        .unwrap();

    // Drive a marker through the watcher path AFTER the locked write
    // — the marker landing on Bob proves the locked write event was
    // observed and processed.
    tokio::fs::write(alice_dir.path().join("pre-marker.txt"), b"phase-1")
        .await
        .unwrap();
    wait_for_file(&bob_dir.path().join("pre-marker.txt"), b"phase-1").await;

    // Now delete the locked file.
    tokio::fs::remove_file(alice_dir.path().join("locked/y.txt"))
        .await
        .unwrap();

    // Drive a second marker — once it lands on Bob, the delete event
    // for `locked/y.txt` has been observed by Alice's watcher.
    tokio::fs::write(alice_dir.path().join("post-marker.txt"), b"phase-2")
        .await
        .unwrap();
    wait_for_file(&bob_dir.path().join("post-marker.txt"), b"phase-2").await;

    // Inspect Alice's doc with `include_empty()` — a tombstone is a
    // zero-length entry that `single_latest_per_key()` filters out
    // by default. We want to catch tombstones explicitly.
    let locked_key = path_to_key(alice_ws.root.as_path(), &alice_ws.root.join("locked/y.txt"))
        .expect("path_to_key");
    let stream = alice_ws
        .doc()
        .get_many(Query::key_exact(locked_key).include_empty())
        .await
        .expect("get_many");
    tokio::pin!(stream);
    let mut had_any_entry = false;
    while let Some(res) = stream.next().await {
        let _ = res.expect("entry ok");
        had_any_entry = true;
    }
    assert!(
        !had_any_entry,
        "alice's on_removed regression: locked/y.txt has an entry/tombstone in the doc",
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
