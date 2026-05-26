//! `PathRules` ride the `workspace.ticket` envelope from host to
//! joiner intact.
//!
//! End-to-end proof that [`artel_fs::PathRules`] survive the wire.
//! Asserts the joiner sees the host's rules deep-equal on
//! `Workspace::rules()` — independent of any enforcement happening
//! at the watcher / applier layer.

mod common;

use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, Mode, PathRule, PathRules, Workspace, WorkspaceConfig};
use artel_protocol::{PeerId, PeerInfo, Request, Response};
use pretty_assertions::assert_eq;
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread")]
async fn rules_round_trip_via_envelope() {
    let common::Pair {
        daemon_a,
        daemon_b,
        workspace_lookup_a,
        workspace_lookup_b,
    } = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let (session, artel_ticket) = match alice
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

    let configured_rules = PathRules {
        default: Mode::ReadOnly,
        rules: vec![
            PathRule {
                glob: "shared/**".into(),
                mode: Mode::ReadWrite,
            },
            PathRule {
                glob: "*.lock".into(),
                mode: Mode::ReadOnly,
            },
        ],
    };

    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _alice_ws_events) = Workspace::host_with(
        &alice,
        session,
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default()
            .with_address_lookup_override(workspace_lookup_a)
            .with_rules(configured_rules.clone()),
    )
    .await
    .expect("Workspace::host_with");

    // Sanity: the host stores its own rules.
    assert_eq!(alice_ws.rules(), &configured_rules);

    // Bob joins. Joiner-side `WorkspaceConfig::rules` is intentionally
    // set to something *different* from the host's rules to confirm
    // the host's rules win on join.
    let bob_distractor_rules = PathRules {
        default: Mode::ReadWrite,
        rules: vec![PathRule {
            glob: "ignored/**".into(),
            mode: Mode::ReadOnly,
        }],
    };

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

    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, _bob_ws_events) = timeout(
        Duration::from_secs(45),
        Workspace::join_with(
            &bob,
            session,
            bob_dir.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            WorkspaceConfig::default()
                .with_address_lookup_override(workspace_lookup_b)
                .with_rules(bob_distractor_rules.clone()),
        ),
    )
    .await
    .expect("Workspace::join_with exceeded 45s")
    .expect("Workspace::join_with");

    // Bob's rules deep-equal the host's, *not* the distractor rules
    // Bob configured. Host wins.
    assert_eq!(bob_ws.rules(), &configured_rules);
    assert_ne!(bob_ws.rules(), &bob_distractor_rules);

    bob_ws.shutdown().await;
    alice_ws.shutdown().await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
