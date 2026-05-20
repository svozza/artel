//! Deletion round-trip: Alice deletes a file → tombstone in the
//! doc → Bob's applier removes it from disk.
//!
//! Exercises the watcher's `Removed` branch and the applier's
//! `content_len() == 0` branch, neither of which is covered by
//! `round_trip.rs` (which only tests writes).

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use artel_client::Client;
use artel_fs::Workspace;
use artel_protocol::{PeerId, PeerInfo, Request, Response};
use tokio::time::sleep;

const WAIT_BUDGET: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[tokio::test(flavor = "multi_thread")]
async fn alice_delete_propagates_to_bob() {
    let (daemon_a, daemon_b) = common::spawn_pair().await;

    // Alice hosts; her workspace starts with one seed file so the
    // file is present on Bob's side after `Workspace::join`.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let (session, ticket) = match alice
        .request(Request::HostSession { peer: alice_peer })
        .await
        .unwrap()
    {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("HostSession: got {other:?}"),
    };

    let alice_dir = tempfile::tempdir().unwrap();
    tokio::fs::write(alice_dir.path().join("doomed.txt"), b"to be deleted")
        .await
        .unwrap();

    let (alice_ws, _) = Workspace::host(&alice, session, alice_dir.path().to_path_buf())
        .await
        .expect("Workspace::host");
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run();

    // Bob joins. After bulk_export his dir should already contain
    // `doomed.txt` (sanity-checked before we delete).
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
    let (bob_ws, _) = Workspace::join(&bob, session, bob_dir.path().to_path_buf())
        .await
        .expect("Workspace::join");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run();

    let bob_path = bob_ws.root.join("doomed.txt");
    let bob_bytes = tokio::fs::read(&bob_path)
        .await
        .expect("bulk export should have populated doomed.txt");
    assert_eq!(bob_bytes, b"to be deleted");

    // Settling delay so the watcher attaches inotify before we
    // start mutating Alice's dir.
    sleep(Duration::from_millis(150)).await;

    // Delete on Alice. The watcher emits a `Removed` event after
    // the 300ms debounce, which becomes a `Doc::del` (zero-length
    // entry). Bob's applier sees an `InsertRemote` with
    // `content_len() == 0` and calls `remove_file`.
    //
    // Cross-platform note: macOS FSEvents reports the unlink as
    // post-hoc `Modify(Metadata)` / `Modify(Data)` rather than a
    // clean `Remove`. The watcher's `on_modified` path handles
    // that case by tombstoning when the read fails with NotFound.
    tokio::fs::remove_file(alice_dir.path().join("doomed.txt"))
        .await
        .unwrap();

    let deadline = Instant::now() + WAIT_BUDGET;
    loop {
        if !tokio::fs::try_exists(&bob_path).await.unwrap_or(false) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "bob still has doomed.txt after {WAIT_BUDGET:?}",
        );
        sleep(POLL_INTERVAL).await;
    }

    alice_ws.shutdown().await;
    bob_ws.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), alice_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
