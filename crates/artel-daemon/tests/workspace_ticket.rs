//! Revoked-lurker fix, daemon tier: workspace ticket unicast
//! delivery + the membership-gated `Replay` path.
//!
//! - The host workspace publishes the envelope ONCE over IPC
//!   (`PublishWorkspaceTicket`); the daemon persists it and delivers
//!   host→peer over the direct stream — to current members on
//!   publish, and to each peer at admission.
//! - The joiner daemon surfaces it as a synthetic `TICKET_ACTION`
//!   System message, live and on every `Subscribe` (replayed from
//!   the persisted mirror copy).
//! - A non-member's `GossipBody::Replay` is refused outright.

#![cfg(feature = "iroh")]

mod common;

use std::time::{Duration, Instant};

use artel_client::Client;
use artel_protocol::gossip::{self, GossipBody};
use artel_protocol::{
    Event, MessageKind, Request, Response, Seq, SessionId, TICKET_ACTION,
};
use bytes::Bytes;
use futures_util::StreamExt;
use iroh_gossip::api::Event as GossipEvent;
use iroh_gossip::proto::TopicId;
use tokio::time::timeout;

/// Mirrors `gossip_bridge::topic_for`, which is private (same shape
/// as the auth_l1_spoofing helper).
fn topic_for(session: SessionId) -> TopicId {
    let mut bytes = [0u8; 32];
    bytes[..16].copy_from_slice(session.as_bytes());
    TopicId::from_bytes(bytes)
}

const ENVELOPE_FIXTURE: &[u8] = b"opaque-workspace-ticket-envelope-fixture";

async fn host_session(client: &Client) -> (SessionId, artel_protocol::JoinTicket) {
    match client
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: None,
        })
        .await
        .unwrap()
    {
        Response::HostSession {
            session, ticket, ..
        } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    }
}

async fn publish_envelope(client: &Client, session: SessionId) {
    match client
        .request(Request::PublishWorkspaceTicket {
            session,
            envelope_bytes: ENVELOPE_FIXTURE.to_vec(),
        })
        .await
        .unwrap()
    {
        Response::WorkspaceTicketPublished => {}
        other => panic!("expected WorkspaceTicketPublished, got {other:?}"),
    }
}

/// Drain `events` until the synthetic `TICKET_ACTION` System message
/// arrives; panic after 20 s.
async fn expect_ticket_message(events: &mut artel_client::EventStream, who: &str) {
    timeout(Duration::from_secs(20), async {
        loop {
            let ev = events.recv().await.expect("event stream closed");
            if let Event::Message { message, .. } = ev
                && message.kind == MessageKind::System
                && message.action == TICKET_ACTION
            {
                assert_eq!(message.payload, ENVELOPE_FIXTURE, "{who}");
                return;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{who}: TICKET_ACTION never arrived"));
}

// =============================================================
// Publish BEFORE the joiner exists: the envelope reaches the peer
// at admission (ensure_member delivery), and the joiner's
// Subscribe replays it from the persisted mirror copy.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn envelope_published_before_join_reaches_joiner_at_admission() {
    let (daemon_a, daemon_b, _dns_pkarr) = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let (session, ticket) = host_session(&alice).await;
    publish_envelope(&alice, session).await;

    // Bob joins after the publish — the admission-triggered delivery
    // is the only path that can reach him.
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    bob.request(Request::Subscribe {
        session,
        since: None,
    })
    .await
    .unwrap();
    let mut bob_events = bob.take_events().await.expect("bob events");
    expect_ticket_message(&mut bob_events, "bob (publish-before-join)").await;

    // The mirror persisted it: a second Subscribe replays the
    // envelope without any further host activity.
    let bob2 = Client::connect(&daemon_b.socket).await.unwrap();
    bob2.request(Request::Subscribe {
        session,
        since: None,
    })
    .await
    .unwrap();
    let mut bob2_events = bob2.take_events().await.expect("bob2 events");
    expect_ticket_message(&mut bob2_events, "bob (replayed from mirror)").await;

    drop(bob_events);
    drop(bob2_events);
    drop(alice);
    drop(bob);
    drop(bob2);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Publish AFTER the joiner is admitted: deliver-on-publish fans the
// envelope out to current members.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn envelope_published_after_join_reaches_existing_member() {
    let (daemon_a, daemon_b, _dns_pkarr) = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let (session, ticket) = host_session(&alice).await;
    alice
        .request(Request::Subscribe {
            session,
            since: None,
        })
        .await
        .unwrap();
    let mut alice_events = alice.take_events().await.expect("alice events");

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    bob.request(Request::JoinSession {
        display_name: "bob".into(),
        ticket,
    })
    .await
    .unwrap();
    bob.request(Request::Subscribe {
        session,
        since: None,
    })
    .await
    .unwrap();
    let mut bob_events = bob.take_events().await.expect("bob events");

    // Wait until the host has actually admitted Bob, so the publish
    // below exercises deliver-to-members (not admission delivery).
    timeout(Duration::from_secs(20), async {
        loop {
            let ev = alice_events.recv().await.expect("alice events closed");
            if matches!(ev, Event::PeerJoined { .. }) {
                return;
            }
        }
    })
    .await
    .expect("alice never saw PeerJoined");

    publish_envelope(&alice, session).await;
    expect_ticket_message(&mut bob_events, "bob (publish-after-join)").await;

    drop(alice_events);
    drop(bob_events);
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Replay gate: a non-member's `GossipBody::Replay` is refused — no
// Message frames are served in response. (The member case is pinned
// by gossip.rs::joiner_replays_messages_sent_before_join, which now
// rides the admission-triggered replay + the gated Replay arm.)
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn replay_from_non_member_is_refused() {
    let (daemon_a, daemon_b, _dns_pkarr) = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let (session, _ticket) = host_session(&alice).await;

    // Backlog the lurker would love to read.
    for n in 0..3u32 {
        alice
            .request(Request::Send {
                session,
                payload: artel_protocol::SendPayload {
                    kind: MessageKind::Chat,
                    action: format!("secret-{n}"),
                    payload: format!("secret-payload-{n}").into_bytes(),
                },
            })
            .await
            .unwrap();
    }

    // Daemon B raw-subscribes to the topic WITHOUT joining the
    // session — the unauthenticated-lurker shape. The topic id is
    // derivable from the session id alone.
    let topic = daemon_b
        .gossip
        .subscribe(topic_for(session), vec![daemon_a.iroh_addr.id])
        .await
        .expect("raw subscribe");
    let (sender, mut receiver) = topic.split();
    timeout(Duration::from_secs(15), receiver.joined())
        .await
        .expect("raw subscribe never joined")
        .expect("joined() errored");

    sender
        .broadcast(Bytes::from(gossip::encode(&GossipBody::Replay {
            since: Seq::ZERO,
        })))
        .await
        .expect("broadcast Replay");

    // For 3 s the lurker must see NO Message frames — the gate drops
    // the request before log_since runs.
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, receiver.next()).await {
            Ok(Some(Ok(GossipEvent::Received(msg)))) => {
                let decoded = gossip::decode(&msg.content).expect("decode");
                assert!(
                    !matches!(decoded, GossipBody::Message(_)),
                    "host served the backlog to a non-member: {decoded:?}",
                );
            }
            Ok(None) => break,
            Ok(Some(Ok(_))) | Err(_) => {}
            Ok(Some(Err(err))) => panic!("gossip receiver errored: {err}"),
        }
    }

    drop(receiver);
    drop(sender);
    drop(alice);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// IPC authority: PublishWorkspaceTicket is host-only — a joiner's
// daemon (Remote mirror) refuses with NotHost.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn publish_workspace_ticket_refused_on_mirror() {
    let (daemon_a, daemon_b, _dns_pkarr) = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let (session, ticket) = host_session(&alice).await;

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    bob.request(Request::JoinSession {
        display_name: "bob".into(),
        ticket,
    })
    .await
    .unwrap();

    let err = bob
        .request(Request::PublishWorkspaceTicket {
            session,
            envelope_bytes: ENVELOPE_FIXTURE.to_vec(),
        })
        .await
        .expect_err("mirror must refuse publish");
    assert!(
        matches!(
            err,
            artel_client::ClientError::Protocol(artel_protocol::ProtocolError::NotHost)
        ),
        "expected NotHost, got {err:?}",
    );

    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
