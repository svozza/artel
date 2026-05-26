//! First-match-wins ordering, end-to-end on the wire.
//!
//! Rule unit tests in `rules.rs` already verify ordering at the
//! `mode_for` level. This integration test confirms the same
//! ordering carries through the watcher → doc → applier pipeline:
//! a `docs/secret/foo.txt` write under
//! `[{ "docs/**" -> ReadWrite }, { "docs/secret/**" -> ReadOnly }]`
//! propagates (first rule wins → `ReadWrite`), and stops propagating
//! when the rule order is reversed.

mod common;

use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, Mode, PathRule, PathRules, Workspace, WorkspaceConfig};
use artel_protocol::{PeerId, PeerInfo, Request, Response};

use common::wait_for_file;

#[tokio::test(flavor = "multi_thread")]
async fn first_match_wins_carries_through_wire() {
    // Phase 1: broad ReadWrite rule precedes narrow ReadOnly. The
    // narrow rule is unreachable; `docs/secret/foo.txt` propagates.
    let propagated = run_with_rules(PathRules {
        default: Mode::ReadWrite,
        rules: vec![
            PathRule {
                glob: "docs/**".into(),
                mode: Mode::ReadWrite,
            },
            PathRule {
                glob: "docs/secret/**".into(),
                mode: Mode::ReadOnly,
            },
        ],
    })
    .await;
    assert!(
        propagated,
        "first-match-wins broken: ReadWrite-first should let docs/secret/foo.txt through",
    );

    // Phase 2: reorder. Narrow ReadOnly precedes broad ReadWrite.
    // Now `docs/secret/foo.txt` is blocked.
    let propagated_reversed = run_with_rules(PathRules {
        default: Mode::ReadWrite,
        rules: vec![
            PathRule {
                glob: "docs/secret/**".into(),
                mode: Mode::ReadOnly,
            },
            PathRule {
                glob: "docs/**".into(),
                mode: Mode::ReadWrite,
            },
        ],
    })
    .await;
    assert!(
        !propagated_reversed,
        "first-match-wins broken: ReadOnly-first should block docs/secret/foo.txt",
    );
}

/// Stand a host/joiner pair up with `rules`; write
/// `docs/secret/foo.txt` and a marker; return whether the secret
/// reached Bob.
async fn run_with_rules(rules: PathRules) -> bool {
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

    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _) = Workspace::host_with(
        &alice,
        session,
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
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

    // Write the secret + marker.
    tokio::fs::create_dir_all(alice_dir.path().join("docs/secret"))
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("docs/secret/foo.txt"), b"data")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("marker.txt"), b"go")
        .await
        .unwrap();

    wait_for_file(&bob_dir.path().join("marker.txt"), b"go").await;

    let propagated = bob_dir.path().join("docs/secret/foo.txt").exists();

    alice_ws.shutdown().await;
    bob_ws.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), alice_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;

    propagated
}
