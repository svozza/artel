//! 2c-2c added the joiner→host send round-trip via
//! `SendRequest`/`SendAck`. 2c-2e made `LeaveSession` tear down
//! the host's gossip topic. Follow-up (a) added the
//! `SessionClosed` broadcast so joiners learn about the close
//! proactively rather than via timeouts.
//!
//! This test exercises the resulting error path: after the host
//! leaves and broadcasts `SessionClosed`, the joiner's mirror
//! is gone, so a subsequent `Send` from the joiner's IPC client
//! surfaces a specific `UnknownSession` (registry's verdict on
//! the missing mirror) instead of a generic timeout.
//!
//! Own integration-test binary so it runs in a distinct process from
//! the other iroh tests; see `tests/common/mod.rs` for the
//! rationale.

#![cfg(feature = "iroh")]

mod common;

use std::time::Duration;

use artel_client::Client;
use artel_protocol::{Event, MessageKind, PeerId, PeerInfo, Request, Response, SendPayload};
use pretty_assertions::assert_eq;
use tokio::time::timeout;

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

    // Bob on daemon B joins and subscribes so we can wait for the
    // SessionClosed event before sending — otherwise we race the
    // close-broadcast and the test occasionally sees a timeout.
    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    let bob = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
    bob_client
        .request(Request::JoinSession {
            peer: bob.clone(),
            ticket,
        })
        .await
        .unwrap();
    bob_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut bob_events = bob_client.take_events().await.expect("bob events");

    // Alice closes the session. The host-side bridge broadcasts
    // SessionClosed before tearing down its topic; Bob's
    // forwarder picks it up, drops his local mirror, and emits
    // Event::SessionClosed to his IPC subscribers.
    alice_client
        .request(Request::LeaveSession {
            session: session_id,
        })
        .await
        .unwrap();

    // Wait for Bob to observe the close before sending. Filter past
    // any Message/PeerJoined that may interleave.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "bob never observed SessionClosed for the host's leave",
        );
        let event = match timeout(remaining, bob_events.recv()).await {
            Ok(Some(ev)) => ev,
            Ok(None) => panic!("bob's events channel closed"),
            Err(_) => continue,
        };
        if let Event::SessionClosed { session } = event {
            assert_eq!(session, session_id);
            break;
        }
    }

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

    // Bob's mirror is gone, so the registry rejects the send
    // outright with UnknownSession — no gossip round-trip, no
    // timeout. That's the specific-reason behaviour the
    // SessionClosed frame restored.
    match send_err {
        artel_client::ClientError::Protocol(artel_protocol::ProtocolError::UnknownSession(id)) => {
            assert_eq!(id, session_id);
        }
        other => panic!("expected UnknownSession, got {other:?}"),
    }

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
