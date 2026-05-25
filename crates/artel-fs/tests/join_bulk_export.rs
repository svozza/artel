//! A joiner imports the host's `DocTicket` from the artel session
//! and bulk-exports the doc to disk.
//!
//! Two daemons (cross-seeded address books for the artel session
//! traffic). Alice on daemon A hosts and stands a workspace up with
//! two pre-existing files; Bob on daemon B joins the artel session,
//! then calls `Workspace::join` which: subscribes, reads the
//! `workspace.ticket` system message, imports the ticket into its
//! own iroh node, and writes the doc contents into Bob's empty
//! tempdir.
//!
//! No watcher / applier yet — this test only proves the bulk path.

mod common;

use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, Workspace, WorkspaceConfig};
use artel_protocol::{PeerId, PeerInfo, Request, Response};
use pretty_assertions::assert_eq;
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread")]
async fn joiner_bulk_imports_host_files() {
    let common::Pair {
        daemon_a,
        daemon_b,
        workspace_lookup_a,
        workspace_lookup_b,
    } = common::spawn_pair().await;

    // Alice on daemon A hosts the artel session.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let (session, ticket) = match alice
        .request(Request::HostSession {
            peer: alice_peer.clone(),
        })
        .await
        .unwrap()
    {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("HostSession: got {other:?}"),
    };

    // Alice's workspace dir has two seed files.
    let alice_dir = tempfile::tempdir().unwrap();
    tokio::fs::write(alice_dir.path().join("a.txt"), b"alpha")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("b.txt"), b"beta")
        .await
        .unwrap();

    // Stand Alice's workspace up. This publishes the existing files
    // into the doc and broadcasts the ticket on the session.
    let (alice_ws, _alice_ws_events) = Workspace::host_with(
        &alice,
        session,
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default().with_address_lookup_override(workspace_lookup_a),
    )
    .await
    .expect("Workspace::host");

    // Bob on daemon B joins the artel session. We need a separate
    // client because Workspace::join consumes the events stream.
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

    // Bob stands his workspace up. Empty dir to start with.
    let bob_dir = tempfile::tempdir().unwrap();

    let (bob_ws, _bob_ws_events) = timeout(
        Duration::from_secs(45),
        Workspace::join_with(
            &bob,
            session,
            bob_dir.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            WorkspaceConfig::default().with_address_lookup_override(workspace_lookup_b),
        ),
    )
    .await
    .expect("Workspace::join exceeded 45s")
    .expect("Workspace::join");

    // Bob's dir should now contain both seed files with the same
    // contents.
    let a = tokio::fs::read(bob_dir.path().join("a.txt"))
        .await
        .expect("a.txt readable");
    let b = tokio::fs::read(bob_dir.path().join("b.txt"))
        .await
        .expect("b.txt readable");
    assert_eq!(a, b"alpha");
    assert_eq!(b, b"beta");

    bob_ws.shutdown().await;
    alice_ws.shutdown().await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
