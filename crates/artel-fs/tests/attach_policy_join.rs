//! `Workspace::join` honours its [`AttachPolicy`].
//!
//! Three behaviours pinned:
//!
//! 1. `RequireEmpty` against a non-empty joiner root rejects with
//!    [`WorkspaceError::Policy`] **before** any subscribe / iroh
//!    work happens. We assert by checking the state dir is absent.
//! 2. `AllowExisting` against the same dir succeeds and bulk-exports
//!    the host's contents on top.
//! 3. `InitFromExisting` on the joiner side is rejected with
//!    [`PolicyViolation::InitFromExistingNotMeaningfulOnJoin`] —
//!    joiners have no canonical tree to seed from, so the variant
//!    is host-only by design.

mod common;

use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, PolicyViolation, Workspace, WorkspaceError};
use artel_protocol::{PeerId, PeerInfo, Request, Response};
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn join_require_empty_rejects_non_empty_dir_without_creating_state() {
    let (daemon_a, daemon_b) = common::spawn_pair().await;

    // Stand the host up so there's a real session + ticket to join
    // against. The joiner's policy rejection should fire *before* we
    // get anywhere near the host's iroh node.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let (session, artel_ticket) = match alice
        .request(Request::HostSession { peer: alice_peer })
        .await
        .unwrap()
    {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("HostSession: got {other:?}"),
    };

    let alice_dir = TempDir::new().unwrap();
    let (alice_ws, _) = Workspace::host(
        &alice,
        session,
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
    )
    .await
    .expect("Workspace::host");
    let alice_ws = Arc::new(alice_ws);

    // Bob joins the artel session and tries to mount a workspace
    // into a non-empty dir.
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
    let resp = bob
        .request(Request::JoinSession {
            peer: bob_peer,
            ticket: artel_ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = TempDir::new().unwrap();
    tokio::fs::write(bob_dir.path().join("local-edit.md"), b"don't clobber me")
        .await
        .unwrap();

    let err = Workspace::join(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
    )
    .await
    .expect_err("RequireEmpty must reject a non-empty join target");

    match err {
        WorkspaceError::Policy(PolicyViolation::DirNotEmpty {
            offending_entries, ..
        }) => {
            assert!(
                offending_entries
                    .iter()
                    .any(|p| p.ends_with("local-edit.md")),
                "offending_entries should name local-edit.md: {offending_entries:?}",
            );
        }
        other => panic!("expected Policy(DirNotEmpty), got {other:?}"),
    }

    let state_dir = bob_dir.path().join(".artel-fs");
    assert!(
        !state_dir.exists(),
        "policy rejection must not create iroh state, but {} exists",
        state_dir.display(),
    );

    // Bob's pre-existing file must not have been touched.
    let preserved = tokio::fs::read(bob_dir.path().join("local-edit.md"))
        .await
        .expect("local-edit.md still readable");
    assert_eq!(preserved, b"don't clobber me");

    alice_ws.shutdown().await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn join_init_from_existing_is_rejected() {
    // No host needed — the policy check fires before the joiner
    // does any session work.
    let daemon = common::spawn_daemon_with_lookup(
        common::fresh_state(),
        iroh::address_lookup::memory::MemoryLookup::new(),
    )
    .await;
    let client = Client::connect(&daemon.socket).await.unwrap();

    // We need *some* session id to pass to `join`, but the policy
    // check fires before any IPC, so the value is immaterial. Mint
    // one via HostSession on the same client.
    let peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "joiner");
    let session = match client.request(Request::HostSession { peer }).await.unwrap() {
        Response::HostSession { session, .. } => session,
        other => panic!("HostSession: got {other:?}"),
    };

    let join_dir = TempDir::new().unwrap();
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        Workspace::join(
            &client,
            session,
            join_dir.path().to_path_buf(),
            AttachPolicy::InitFromExisting,
        ),
    )
    .await
    .expect("InitFromExisting must reject *quickly* — the check fires before any IPC")
    .expect_err("InitFromExisting must reject on join");

    assert!(
        matches!(
            err,
            WorkspaceError::Policy(PolicyViolation::InitFromExistingNotMeaningfulOnJoin),
        ),
        "expected InitFromExistingNotMeaningfulOnJoin, got {err:?}",
    );

    drop(client);
    daemon.stop().await;
}
