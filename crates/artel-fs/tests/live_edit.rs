//! A live edit on the host's filesystem propagates to the joiner
//! via the watcher → doc → applier pipeline.
//!
//! Two daemons, Alice hosts the artel session and a workspace,
//! Bob joins. Both call `Workspace::run` so their watchers +
//! appliers are live. Alice writes `live.txt` *after*
//! `Workspace::host` returned; Bob's filesystem should reflect it
//! within a couple of seconds.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use artel_client::Client;
use artel_fs::{AttachPolicy, Workspace, WorkspaceConfig};
use artel_protocol::{PeerId, PeerInfo, Request, Response};
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread")]
async fn live_edit_propagates_host_to_joiner() {
    let common::Pair {
        daemon_a,
        daemon_b,
        workspace_lookup_a,
        workspace_lookup_b,
    } = common::spawn_pair().await;

    // Alice hosts.
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

    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _) = Workspace::host_with(
        &alice,
        session,
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_address_lookup_override(workspace_lookup_a),
    )
    .await
    .expect("Workspace::host");
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // Bob joins.
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
    .expect("Workspace::join");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    let alice_path = alice_dir.path().join("live.txt");
    let bob_path = bob_dir.path().join("live.txt");
    let payload = b"hello from a live edit";
    tokio::fs::write(&alice_path, payload).await.unwrap();

    // Poll Bob's tempdir for the file. Generous deadline because
    // we're going through:
    //   notify debounce (300ms) -> doc set_bytes -> sync -> applier
    // -> tokio::fs::write. ~5s upper bound is realistic.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(bytes) = tokio::fs::read(&bob_path).await
            && bytes == payload
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Bob never observed live edit at {}",
            bob_path.display(),
        );
        sleep(Duration::from_millis(100)).await;
    }

    // Tear down: stop the workspaces so their tasks exit, then the
    // daemons.
    alice_ws.shutdown().await;
    bob_ws.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), alice_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
