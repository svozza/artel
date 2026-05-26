//! Phase 2c-2c end-to-end: a joiner's `Send` reaches the host (and
//! every other subscriber) via gossip-backed `SendRequest` /
//! `SendAck`.
//!
//! Own integration-test binary so it runs in a distinct process from
//! the other iroh tests; concurrent iroh endpoint setup across
//! binaries occasionally takes longer than a single-binary timeout
//! under load. Production exposure is low (one daemon per machine);
//! the test-side flakiness was real in 2c-2b.

#![cfg(feature = "iroh")]

mod common;

use artel_client::Client;
use artel_protocol::{MessageKind, PeerId, PeerInfo, Request, Response, SendPayload};
use pretty_assertions::assert_eq;

#[tokio::test]
async fn joiner_send_round_trips_through_host() {
    let (daemon_a, daemon_b) = common::spawn_pair().await;

    // Alice on daemon A hosts.
    let alice_client = Client::connect(&daemon_a.socket).await.unwrap();
    let alice = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let host_resp = alice_client
        .request(Request::HostSession {
            peer: alice.clone(),
            session: None,
        })
        .await
        .unwrap();
    let (session_id, ticket) = match host_resp {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    // Alice subscribes to her own session so we can observe the
    // host-side fanout when Bob sends.
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

    // Bob sends. Daemon B's bridge publishes a SendRequest on the
    // gossip topic; daemon A's bridge picks it up, drives
    // Registry::send (lazily admitting Bob to the session), persists,
    // fans out a Message frame, and replies with SendAck. Daemon B's
    // forwarder resolves the pending oneshot, the IPC reply lands.
    let send_resp = bob_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"hi from bob".to_vec(),
            },
        })
        .await
        .unwrap();
    let bob_sent_seq = match send_resp {
        Response::Sent { session, seq } => {
            assert_eq!(session, session_id);
            seq
        }
        other => panic!("expected Sent, got {other:?}"),
    };

    // Alice observes the message via the host's local broadcast.
    let alice_msg =
        common::expect_message_with_payload(&mut alice_events, b"hi from bob", "alice").await;
    assert_eq!(alice_msg.peer, bob);
    assert_eq!(alice_msg.seq, bob_sent_seq);
    assert_eq!(alice_msg.action, "chat.message");

    // Bob observes the same message via gossip — the Message frame
    // his forwarder produces for the local mirror.
    let bob_msg = common::expect_message_with_payload(&mut bob_events, b"hi from bob", "bob").await;
    assert_eq!(bob_msg.peer, bob);
    assert_eq!(bob_msg.seq, bob_sent_seq);
    assert_eq!(bob_msg.action, "chat.message");

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
