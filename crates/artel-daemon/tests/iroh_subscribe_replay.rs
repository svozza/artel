//! Open follow-up (c): a joiner asks for the host's log on join
//! via `GossipBody::Replay`, so messages sent *before* the joiner
//! existed land in the joiner's mirror as Message events.
//!
//! Concretely: Alice hosts, sends three messages, then Bob joins.
//! Bob's events stream surfaces all three historical messages
//! (via the Replay round-trip) plus any subsequent live ones.
//!
//! Own integration-test binary so cargo runs it in a distinct
//! process from the other iroh tests; see `tests/common/mod.rs`.

#![cfg(feature = "iroh")]

mod common;

use std::time::Duration;

use artel_client::Client;
use artel_protocol::{Event, MessageKind, PeerId, PeerInfo, Request, Response, SendPayload};
use pretty_assertions::assert_eq;
use tokio::time::timeout;

#[tokio::test]
async fn joiner_replays_messages_sent_before_join() {
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

    // Alice sends three messages BEFORE Bob joins. These must
    // surface on Bob's events stream after his join + replay.
    for n in 0..3u32 {
        alice_client
            .request(Request::Send {
                session: session_id,
                payload: SendPayload {
                    kind: MessageKind::Chat,
                    action: format!("pre-{n}"),
                    payload: format!("payload-{n}").into_bytes(),
                },
            })
            .await
            .unwrap();
    }

    // Bob on daemon B joins via the real ticket and subscribes.
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

    // Alice sends a fourth message live, after Bob is on the topic.
    // We expect Bob to see all four in his event stream — the
    // first three from the Replay round-trip, the fourth from the
    // normal live broadcast.
    alice_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "live".into(),
                payload: b"payload-live".to_vec(),
            },
        })
        .await
        .unwrap();

    // Collect Message events until we've seen all four expected
    // payloads. They may arrive in any order (replay backfill
    // can interleave with live broadcasts), so collect into a set.
    let expected: std::collections::HashSet<Vec<u8>> = [
        b"payload-0".to_vec(),
        b"payload-1".to_vec(),
        b"payload-2".to_vec(),
        b"payload-live".to_vec(),
    ]
    .into_iter()
    .collect();
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while seen.len() < expected.len() {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "bob only saw {seen:?}; missing {:?}",
            expected.difference(&seen).collect::<Vec<_>>(),
        );
        let event = match timeout(remaining, bob_events.recv()).await {
            Ok(Some(ev)) => ev,
            Ok(None) => panic!("bob's events channel closed"),
            Err(_) => continue,
        };
        if let Event::Message { message, .. } = event {
            seen.insert(message.payload);
        }
    }
    assert_eq!(seen, expected);

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
