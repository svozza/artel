//! Phase 2c-2b: a joiner can't issue `Send` yet — that's joiner→host
//! routing, deferred to 2c-2c. Verify the daemon refuses cleanly with
//! `ProtocolError::NotHost`.
//!
//! Own integration-test binary so it runs in a distinct process from
//! the gossip-fanout test; see `tests/common/mod.rs` for the
//! rationale.

#![cfg(feature = "iroh")]

mod common;

use artel_client::Client;
use artel_protocol::{MessageKind, PeerId, PeerInfo, Request, Response, SendPayload};

#[tokio::test]
async fn joiner_send_returns_not_host() {
    let (daemon_a, daemon_b) = common::spawn_pair().await;

    let alice_client = Client::connect(&daemon_a.socket).await.unwrap();
    let alice = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let resp = alice_client
        .request(Request::HostSession { peer: alice })
        .await
        .unwrap();
    let (session_id, ticket) = match resp {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    let bob = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
    bob_client
        .request(Request::JoinSession { peer: bob, ticket })
        .await
        .unwrap();

    let send_err = bob_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"i'm not the host".to_vec(),
            },
        })
        .await
        .unwrap_err();
    assert!(
        matches!(
            send_err,
            artel_client::ClientError::Protocol(artel_protocol::ProtocolError::NotHost),
        ),
        "expected NotHost, got {send_err:?}",
    );

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
