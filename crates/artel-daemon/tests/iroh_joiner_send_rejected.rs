//! Phase 2c-2c: a joiner's `Send` is no longer rejected outright —
//! it now round-trips via `SendRequest`/`SendAck`. This test
//! exercises the *error* path of that round-trip: when the host has
//! closed the session, the joiner's send must surface the host's
//! actual rejection (not a flattened generic error or a timeout).
//!
//! Own integration-test binary so it runs in a distinct process from
//! the other iroh tests; see `tests/common/mod.rs` for the
//! rationale.

#![cfg(feature = "iroh")]

mod common;

use artel_client::Client;
use artel_protocol::{MessageKind, PeerId, PeerInfo, Request, Response, SendPayload};

#[tokio::test]
async fn joiner_send_after_host_closes_surfaces_unknown_session() {
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

    // Bob on daemon B joins via the real ticket.
    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    let bob = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
    bob_client
        .request(Request::JoinSession {
            peer: bob.clone(),
            ticket,
        })
        .await
        .unwrap();

    // Alice closes the session before Bob sends. LeaveSession on the
    // host removes the session from Alice's daemon; Bob's daemon
    // doesn't yet know (no leave-broadcast yet — that's a future
    // slice). The bridge keeps Bob's gossip topic alive so the
    // SendRequest still gets out.
    alice_client
        .request(Request::LeaveSession {
            session: session_id,
        })
        .await
        .unwrap();

    let send_err = bob_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"after close".to_vec(),
            },
        })
        .await
        .unwrap_err();

    // Alice's daemon answered with UnknownSession (the session is
    // gone from her registry); the bridge ferried that back as a
    // SendAck.Err, the joiner-side Registry forwarded it as
    // HostRejected, and the IPC dispatch flattened to the host's
    // verdict — we should see UnknownSession on the wire.
    match send_err {
        artel_client::ClientError::Protocol(artel_protocol::ProtocolError::UnknownSession(id)) => {
            assert_eq!(id, session_id);
        }
        other => panic!("expected UnknownSession on the wire, got {other:?}"),
    }

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
