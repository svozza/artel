//! Applier-side defence-in-depth: even if a peer publishes a
//! `ReadOnly` path into the doc (because they're misbehaving, run an
//! older version, or in this test bypass their own watcher), the
//! receiving applier drops the `InsertRemote` and surfaces
//! `WorkspaceEvent::SkippedReadOnly { Incoming }`.
//!
//! Mechanism: Alice hosts with `secret/**: ReadOnly`. Alice then
//! injects `secret/foo.txt` directly via `alice_ws.doc().set_bytes`,
//! which bypasses Alice's own watcher (the rule check lives in the
//! watcher, not in `doc.set_bytes`). The doc write propagates to
//! Bob; Bob's applier sees the rule and drops it.

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
use bytes::Bytes;
use tokio::time::sleep;

use common::wait_for_file;

#[tokio::test(flavor = "multi_thread")]
async fn applier_drops_incoming_read_only_insert() {
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
    let (alice_ws, _alice_events) = Workspace::host_with(
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
    let (bob_ws, mut bob_events) = Workspace::join_with(
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

    // Inject `secret/foo.txt` directly into Alice's doc, bypassing
    // her own watcher's rule check. Use Alice's author so the doc
    // entry is well-formed.
    let secret_key = path_to_key(
        alice_ws.root.as_path(),
        &alice_ws.root.join("secret/foo.txt"),
    )
    .expect("path_to_key for secret");
    alice_ws
        .doc()
        .set_bytes(
            alice_ws.author(),
            secret_key.clone(),
            Bytes::from_static(b"injected-secret"),
        )
        .await
        .expect("doc.set_bytes");

    // Inject a marker too so we know the InsertRemote train has
    // arrived at Bob — the marker isn't ReadOnly so it lands.
    let marker_key = path_to_key(alice_ws.root.as_path(), &alice_ws.root.join("marker.txt"))
        .expect("path_to_key for marker");
    alice_ws
        .doc()
        .set_bytes(alice_ws.author(), marker_key, Bytes::from_static(b"go"))
        .await
        .expect("doc.set_bytes marker");

    // Wait for marker on Bob's disk → guarantees the secret
    // InsertRemote has been processed by Bob's applier (FIFO).
    wait_for_file(&bob_dir.path().join("marker.txt"), b"go").await;

    // Bob's applier dropped the secret.
    assert!(
        !bob_dir.path().join("secret/foo.txt").exists(),
        "applier regression: secret/foo.txt landed on bob despite ReadOnly rule",
    );

    // Bob's event stream has at least one SkippedReadOnly{Incoming}
    // for the secret.
    let mut saw_skip = false;
    while let Ok(ev) = bob_events.try_recv() {
        if let WorkspaceEvent::SkippedReadOnly { path, direction } = ev
            && direction == Direction::Incoming
            && path.ends_with("secret/foo.txt")
        {
            saw_skip = true;
            break;
        }
    }
    assert!(
        saw_skip,
        "expected SkippedReadOnly{{Incoming}} on bob for secret/foo.txt",
    );

    // Now drive a tombstone for the same key. The applier's rule
    // check sits BEFORE the tombstone branch, so the delete must
    // also be dropped.
    alice_ws
        .doc()
        .del(alice_ws.author(), secret_key)
        .await
        .expect("doc.del");

    // Sleep just enough for the tombstone InsertRemote to traverse
    // — there's no positive-disk-state to poll on, so a short
    // settle window is the cheapest cross-platform approach.
    sleep(Duration::from_millis(500)).await;

    // No file existed to begin with on Bob, so the assertion is
    // about the event stream: a second SkippedReadOnly{Incoming}
    // for the same path means the tombstone branch was gated.
    let mut saw_tomb_skip = false;
    while let Ok(ev) = bob_events.try_recv() {
        if let WorkspaceEvent::SkippedReadOnly { path, direction } = ev
            && direction == Direction::Incoming
            && path.ends_with("secret/foo.txt")
        {
            saw_tomb_skip = true;
            break;
        }
    }
    assert!(
        saw_tomb_skip,
        "expected SkippedReadOnly{{Incoming}} for tombstone on secret/foo.txt — \
         applier rule check is sitting AFTER the tombstone branch",
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
