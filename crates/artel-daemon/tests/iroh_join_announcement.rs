//! Phase 2c-2d: a joiner announces themselves on the gossip topic
//! once the mesh is up so the host's IPC subscribers see
//! `PeerJoined` proactively — without waiting for the joiner's
//! first `SendRequest` (the lazy-admission path that 2c-2c shipped).
//!
//! Own integration-test binary so cargo runs it in a distinct
//! process from the other iroh tests; see `tests/common/mod.rs` for
//! the rationale.

#![cfg(feature = "iroh")]

mod common;

use std::time::Duration;

use artel_client::Client;
use artel_protocol::{Event, PeerId, PeerInfo, Request, Response};
use pretty_assertions::assert_eq;
use tokio::time::timeout;

#[tokio::test]
async fn joiner_announces_membership_without_sending() {
    let (daemon_a, daemon_b, _dns_pkarr) = common::spawn_pair().await;

    // Alice on daemon A hosts and subscribes to her own session so we
    // can observe events for incoming peers.
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
    alice_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut alice_events = alice_client.take_events().await.expect("alice events");

    // Bob on daemon B joins via the real ticket. He never sends —
    // we want to verify Alice sees him purely from the
    // JoinAnnouncement frame the bridge broadcasts when Bob's
    // gossip mesh comes up.
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

    // Alice should observe `PeerJoined { bob }` — driven by the
    // JoinAnnouncement frame, not by a SendRequest. Filter past any
    // other events that may interleave.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "alice never observed PeerJoined for bob",
        );
        let event = match timeout(remaining, alice_events.recv()).await {
            Ok(Some(ev)) => ev,
            Ok(None) => panic!("alice's events channel closed"),
            Err(_) => continue,
        };
        if let Event::PeerJoined { session, peer } = event {
            assert_eq!(session, session_id);
            assert_eq!(peer, bob);
            break;
        }
    }

    // Sanity: ListSessions on Alice's daemon now reports peer_count = 2.
    let list_resp = alice_client.request(Request::ListSessions).await.unwrap();
    let sessions = match list_resp {
        Response::ListSessions { sessions } => sessions,
        other => panic!("expected ListSessions, got {other:?}"),
    };
    let summary = sessions
        .iter()
        .find(|s| s.id == session_id)
        .expect("session present");
    assert_eq!(summary.peer_count, 2, "alice + bob");

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
