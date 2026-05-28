//! Tombstones must not bypass the workspace filter on either the
//! applier or the bulk-export side.
//!
//! The historic ordering had `ReadOnly → tombstone → filter`, so a
//! peer's tombstone for a path the local filter would skip
//! (asymmetric ignore globs, version drift, attacker-crafted key
//! targeting a hardcoded-skip path like `.git/HEAD`) reached
//! `tokio::fs::remove_file` regardless. That deleted local state the
//! workspace was never supposed to touch.
//!
//! Both call sites (`applier::handle_entry` and
//! `workspace::bulk_export`) are covered here; they share the same
//! intended ordering, and the fix moves the filter check ABOVE the
//! tombstone branch in both. The hardcoded-skip path is the
//! cheapest way to exercise the bug — `WorkspaceFilter` already
//! refuses to let `.git/HEAD` through, no asymmetric-glob plumbing
//! needed.

mod common;

use common::{spawn_pair, testing_setup, wait_for_file, Pair};

use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{path_to_key, AttachPolicy, Workspace, WorkspaceConfig};
use artel_protocol::{PeerId, PeerInfo, Request, Response};
use bytes::Bytes;
use tokio::time::sleep;

/// Settling window after a tombstone propagates and the marker has
/// been observed. The marker idiom guarantees FIFO arrival, so a
/// short extra sleep is only insurance against the
/// remove-then-write reordering the macOS notify backend has
/// historically produced. 200ms is plenty.
const TOMBSTONE_SETTLE: Duration = Duration::from_millis(200);

#[tokio::test(flavor = "multi_thread")]
async fn applier_filter_check_gates_tombstone_for_hardcoded_skip() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");

    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _alice_events) = Workspace::host_with(
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

    // Pre-create `.git/HEAD` on Bob's side. This file lives outside
    // the workspace's filter (hardcoded skip) — Bob's watcher will
    // never publish it, and Alice's tombstone for the same key
    // therefore must not delete it.
    let bob_git_head = bob_ws.root.join(".git").join("HEAD");
    tokio::fs::create_dir_all(bob_git_head.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&bob_git_head, b"ref: refs/heads/main\n")
        .await
        .unwrap();

    // Inject a tombstone for `.git/HEAD` directly into Alice's doc.
    // First seed an entry so `del` produces a recognisable
    // zero-length tombstone (iroh-docs doesn't tombstone a key it's
    // never seen).
    let git_head_key = path_to_key(
        alice_ws.root.as_path(),
        &alice_ws.root.join(".git").join("HEAD"),
    )
    .expect("path_to_key for .git/HEAD");
    alice_ws
        .doc()
        .set_bytes(
            alice_ws.author(),
            git_head_key.clone(),
            Bytes::from_static(b"attacker-write"),
        )
        .await
        .expect("doc.set_bytes seeding");
    alice_ws
        .doc()
        .del(alice_ws.author(), git_head_key)
        .await
        .expect("doc.del tombstone");

    // Marker idiom: a non-skipped path lets us observe when the
    // applier has chewed through the tombstone above (FIFO).
    let marker_key = path_to_key(alice_ws.root.as_path(), &alice_ws.root.join("marker.txt"))
        .expect("path_to_key for marker");
    alice_ws
        .doc()
        .set_bytes(alice_ws.author(), marker_key, Bytes::from_static(b"go"))
        .await
        .expect("doc.set_bytes marker");

    wait_for_file(&bob_dir.path().join("marker.txt"), b"go").await;
    sleep(TOMBSTONE_SETTLE).await;

    // The bug: applier's filter check sits AFTER the tombstone
    // branch, so `.git/HEAD` was unlinked despite being a hardcoded
    // skip. The fix moves filter ABOVE the tombstone branch.
    assert!(
        bob_git_head.exists(),
        "applier deleted bob's .git/HEAD via tombstone bypass — \
         filter check must gate the remove_file branch",
    );
    let surviving = tokio::fs::read(&bob_git_head)
        .await
        .expect(".git/HEAD readable");
    assert_eq!(
        surviving, b"ref: refs/heads/main\n",
        ".git/HEAD contents must be untouched",
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

#[tokio::test(flavor = "multi_thread")]
async fn bulk_export_filter_check_gates_tombstone_for_hardcoded_skip() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");

    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _alice_events) = Workspace::host_with(
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

    // Seed-then-tombstone `.git/HEAD` in the host's doc so a
    // joiner who runs `bulk_export` against this doc sees the
    // zero-length entry. `include_empty()` on the bulk_export side
    // is what surfaces it.
    let git_head_key = path_to_key(
        alice_ws.root.as_path(),
        &alice_ws.root.join(".git").join("HEAD"),
    )
    .expect("path_to_key for .git/HEAD");
    alice_ws
        .doc()
        .set_bytes(
            alice_ws.author(),
            git_head_key.clone(),
            Bytes::from_static(b"attacker-write"),
        )
        .await
        .expect("doc.set_bytes seeding");
    alice_ws
        .doc()
        .del(alice_ws.author(), git_head_key)
        .await
        .expect("doc.del tombstone");

    // Bob's dir is non-empty: he already has `.git/HEAD`. The
    // attach-policy emptiness check exempts hardcoded-skip paths,
    // so `RequireEmpty` still passes. The point: bulk_export must
    // not delete this file when it sees the tombstone.
    let bob_dir = tempfile::tempdir().unwrap();
    let bob_git_head = bob_dir.path().join(".git").join("HEAD");
    tokio::fs::create_dir_all(bob_git_head.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&bob_git_head, b"ref: refs/heads/main\n")
        .await
        .unwrap();

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

    let (bob_ws, _bob_events) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join_with");

    // After bulk_export has run inside `join_with`, Bob's
    // `.git/HEAD` must still be on disk.
    assert!(
        bob_git_head.exists(),
        "bulk_export deleted bob's .git/HEAD via tombstone bypass — \
         filter check must gate the remove_file branch",
    );
    let surviving = tokio::fs::read(&bob_git_head)
        .await
        .expect(".git/HEAD readable");
    assert_eq!(
        surviving, b"ref: refs/heads/main\n",
        ".git/HEAD contents must be untouched",
    );

    bob_ws.shutdown().await;
    alice_ws.shutdown().await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
