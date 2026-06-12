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
use artel_protocol::{Event, MessageKind, Request, Response, Seq, SessionId, TICKET_ACTION};
use bytes::Bytes;
use futures_util::StreamExt;
use iroh_gossip::api::Event as GossipEvent;
use iroh_gossip::proto::TopicId;
use tokio::time::timeout;

/// Mirrors `gossip_bridge::topic_for`, which is private (same shape
/// as the `auth_l1_spoofing` helper).
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
    let second_client = Client::connect(&daemon_b.socket).await.unwrap();
    second_client
        .request(Request::Subscribe {
            session,
            since: None,
        })
        .await
        .unwrap();
    let mut second_events = second_client.take_events().await.expect("second events");
    expect_ticket_message(&mut second_events, "bob (replayed from mirror)").await;

    drop(bob_events);
    drop(second_events);
    drop(alice);
    drop(bob);
    drop(second_client);
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
// Two frame kinds, one channel: an RW joiner receives BOTH the
// workspace ticket envelope and the NamespaceSecret over the same
// delivery stream.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn rw_joiner_receives_envelope_and_secret_on_one_channel() {
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
    publish_envelope(&alice, session).await;

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

    // Wait for admission, then push the secret to Bob.
    let bob_peer_id = daemon_b.peer_id();
    timeout(Duration::from_secs(20), async {
        loop {
            let ev = alice_events.recv().await.expect("alice events closed");
            if let Event::PeerJoined { peer, .. } = ev
                && peer.id == bob_peer_id
            {
                return;
            }
        }
    })
    .await
    .expect("alice never saw bob join");

    match alice
        .request(Request::DeliverUpgrade {
            session,
            target_peer: bob_peer_id,
            namespace_secret: [0x42; 32],
        })
        .await
        .unwrap()
    {
        Response::UpgradeDelivered => {}
        other => panic!("expected UpgradeDelivered, got {other:?}"),
    }

    // Bob sees both synthetic messages.
    let mut saw_ticket = false;
    let mut saw_upgrade = false;
    timeout(Duration::from_secs(20), async {
        while !(saw_ticket && saw_upgrade) {
            let ev = bob_events.recv().await.expect("bob events closed");
            if let Event::Message { message, .. } = ev
                && message.kind == MessageKind::System
            {
                if message.action == TICKET_ACTION {
                    assert_eq!(message.payload, ENVELOPE_FIXTURE);
                    saw_ticket = true;
                } else if message.action == artel_protocol::UPGRADE_ACTION {
                    saw_upgrade = true;
                }
            }
        }
    })
    .await
    .expect("bob never saw both delivery kinds");

    drop(alice_events);
    drop(bob_events);
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Joiner daemon restart: the envelope persisted on the mirror
// survives, and a fresh Subscribe replays it with no host
// re-publish (extends the
// gossip.rs::joiner_replays_system_message_after_daemon_restart
// shape onto the unicast-sourced envelope).
// =============================================================

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::used_underscore_binding)]
async fn joiner_restart_replays_envelope_without_host_republish() {
    let (daemon_a, mut daemon_b, dns_pkarr) = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let (session, ticket) = host_session(&alice).await;
    publish_envelope(&alice, session).await;

    // Bob joins; the admission delivery lands the envelope in his
    // mirror. Confirm receipt before restarting.
    let bob1 = Client::connect(&daemon_b.socket).await.unwrap();
    bob1.request(Request::JoinSession {
        display_name: "bob".into(),
        ticket: ticket.clone(),
    })
    .await
    .unwrap();
    bob1.request(Request::Subscribe {
        session,
        since: None,
    })
    .await
    .unwrap();
    let mut bob1_events = bob1.take_events().await.expect("bob events");
    expect_ticket_message(&mut bob1_events, "bob (pre-restart)").await;

    // Restart bob's daemon at the same paths.
    drop(bob1_events);
    drop(bob1);
    let bob_state_2 = daemon_b._state.take().expect("state present");
    daemon_b.shutdown.trigger();
    timeout(Duration::from_secs(10), daemon_b.join)
        .await
        .expect("bob daemon stop")
        .expect("bob daemon join")
        .expect("bob daemon io");
    let daemon_b_2 = common::spawn_daemon(bob_state_2, common::testing_setup(&dns_pkarr)).await;

    // A fresh Subscribe replays the envelope from the persisted
    // mirror record — no host re-publish happened.
    let bob2 = Client::connect(&daemon_b_2.socket).await.unwrap();
    bob2.request(Request::Subscribe {
        session,
        since: None,
    })
    .await
    .unwrap();
    let mut bob2_events = bob2.take_events().await.expect("bob2 events");
    expect_ticket_message(&mut bob2_events, "bob (post-restart replay)").await;

    drop(bob2_events);
    drop(bob2);
    daemon_b_2.stop().await;
    drop(alice);
    daemon_a.stop().await;
}

// =============================================================
// Host daemon restart: the envelope reloads with the session
// record; a Subscribe on the resumed session replays the exact
// bytes with no workspace re-publish.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn host_restart_reloads_envelope_byte_stable() {
    let root = tempfile::TempDir::new().unwrap();
    let paths = common::RestartState::under(root.path());

    // Incarnation 1: host + publish.
    let daemon1 = common::spawn_local_daemon_at(&paths).await;
    let client1 = Client::connect(&paths.socket).await.unwrap();
    let (session, _ticket) = host_session(&client1).await;
    publish_envelope(&client1, session).await;
    drop(client1);
    daemon1.stop().await;

    // Incarnation 2: resume; the envelope must come back from disk.
    let daemon2 = common::spawn_local_daemon_at(&paths).await;
    let client2 = Client::connect(&paths.socket).await.unwrap();
    match client2
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: Some(session),
        })
        .await
        .unwrap()
    {
        Response::HostSession { session: id, .. } => assert_eq!(id, session),
        other => panic!("expected HostSession resume, got {other:?}"),
    }
    client2
        .request(Request::Subscribe {
            session,
            since: None,
        })
        .await
        .unwrap();
    let mut events = client2.take_events().await.expect("events");
    expect_ticket_message(&mut events, "host (post-restart, byte-stable)").await;

    drop(events);
    drop(client2);
    daemon2.stop().await;
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
