//! Open follow-up (a): when the host closes a session, joiners
//! learn about it via a `GossipBody::SessionClosed` broadcast and
//! surface `Event::SessionClosed` to their IPC subscribers
//! immediately, instead of finding out by sending into the void
//! and timing out at the bridge's `SEND_REMOTE_TIMEOUT`.
//!
//! Own integration-test binary so cargo runs it in a distinct
//! process from the other iroh tests; see `tests/common/mod.rs`
//! for the rationale.

#![cfg(feature = "iroh")]

mod common;

use std::time::Duration;

use artel_client::Client;
use artel_protocol::{Event, PeerId, PeerInfo, Request, Response};
use pretty_assertions::assert_eq;
use tokio::time::timeout;

#[tokio::test]
async fn host_close_propagates_session_closed_to_joiner() {
    let (daemon_a, daemon_b, _dns_pkarr) = common::spawn_pair().await;

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

    // Bob on daemon B joins and subscribes so he has an event
    // stream we can read SessionClosed off of.
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

    // Alice closes the session. Daemon A broadcasts SessionClosed
    // before tearing down its bridge entry; daemon B's forwarder
    // decodes and calls Registry::host_closed_session, which
    // emits the event to Bob's IPC subscribers.
    alice_client
        .request(Request::LeaveSession {
            session: session_id,
        })
        .await
        .unwrap();

    // Bob's events stream should surface SessionClosed within a
    // few seconds. Filter past any Message/PeerJoined that may
    // interleave (e.g., Alice's own subscribe-side broadcasts
    // catching up).
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

    // Sanity: Bob's daemon should no longer list the session.
    let listed = match bob_client.request(Request::ListSessions).await.unwrap() {
        Response::ListSessions { sessions } => sessions,
        other => panic!("expected ListSessions, got {other:?}"),
    };
    assert!(
        listed.iter().all(|s| s.id != session_id),
        "bob's daemon should have dropped the mirror after SessionClosed",
    );

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
