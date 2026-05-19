//! Phase 2c-2b end-to-end: a host's `Send` reaches a joiner on a
//! different daemon via gossip.
//!
//! This is its own integration-test binary so cargo runs it in a
//! distinct process from the other iroh tests; concurrent iroh
//! endpoint setup across binaries occasionally takes longer than a
//! single-binary timeout under load. Production exposure is low
//! (one daemon per machine) but the test-side flakiness was real.

#![cfg(feature = "iroh")]

mod common;

use artel_client::Client;
use artel_protocol::{MessageKind, PeerId, PeerInfo, Request, Response, SendPayload};
use pretty_assertions::assert_eq;

#[tokio::test]
async fn host_sends_message_joiner_observes_via_gossip() {
    let (daemon_a, daemon_b) = common::spawn_pair().await;

    // Alice on daemon A hosts.
    let alice_client = Client::connect(&daemon_a.socket).await.unwrap();
    let alice = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let host_resp = alice_client
        .request(Request::HostSession {
            peer: alice.clone(),
        })
        .await
        .unwrap();
    let (session_id, ticket) = match host_resp {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    // Alice subscribes to her own session as a sanity baseline that
    // local fan-out still works.
    alice_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut alice_events = alice_client.take_events().await.expect("alice events");

    // Bob on daemon B joins via the real ticket.
    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    let bob = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
    let join_resp = bob_client
        .request(Request::JoinSession {
            peer: bob.clone(),
            ticket,
        })
        .await
        .unwrap();
    match join_resp {
        Response::JoinSession { session, .. } => assert_eq!(session, session_id),
        other => panic!("expected JoinSession, got {other:?}"),
    }
    bob_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut bob_events = bob_client.take_events().await.expect("bob events");

    // Alice sends. Daemon A persists, fans out locally, and
    // broadcasts on the gossip topic. Daemon B's bridge forwarder
    // decodes and pushes into Bob's session log.
    alice_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"hi from alice".to_vec(),
            },
        })
        .await
        .unwrap();

    // Alice sees her own Message via the local broadcast.
    let alice_msg =
        common::expect_message_with_payload(&mut alice_events, b"hi from alice", "alice").await;
    assert_eq!(alice_msg.peer, alice);

    // Bob sees the same message via gossip.
    let bob_msg =
        common::expect_message_with_payload(&mut bob_events, b"hi from alice", "bob").await;
    assert_eq!(bob_msg.peer, alice);
    assert_eq!(bob_msg.seq, alice_msg.seq);
    assert_eq!(bob_msg.action, "chat.message");

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
