//! Auth Slice A — L1 peer-id collapse: regression suite.
//!
//! Pins the host-side enforcement (drop frames whose body `peer.id`
//! does not match the gossip-authenticated `delivered_from`) and the
//! joiner-side outbound stamp (the bridge overrides any IPC-supplied
//! `peer.id` with the daemon's authenticated id before publishing).
//! See `docs/plans/2026-05-30-auth-l1-peer-id-collapse-plan.md` and
//! `docs/brainstorms/2026-05-30-auth-story-brainstorm.md`.
//!
//! All tests Tier B (hermetic against `DnsPkarrServer`) and use the
//! `common::spawn_pair` fixture.

#![cfg(feature = "iroh")]

mod common;

use std::time::{Duration, Instant};

use artel_client::Client;
use artel_protocol::gossip::{self, GossipBody};
use artel_protocol::rpc::SendPayload;
use artel_protocol::{Event, MessageKind, PeerInfo, Request, Response, SessionId, SessionMessage};
use bytes::Bytes;
use futures_util::StreamExt;
use iroh_gossip::api::Event as GossipEvent;
use iroh_gossip::proto::TopicId;
use pretty_assertions::assert_eq;
use tokio::time::timeout;
use uuid::Uuid;

/// Mirrors `gossip_bridge::topic_for`, which is private. Same shape:
/// session UUID in the high 16 bytes, zeros in the low 16. If the
/// bridge ever changes the derivation, this helper drifts and the
/// tests fail loudly — that's the desired signal.
fn topic_for(session: SessionId) -> TopicId {
    let mut bytes = [0u8; 32];
    bytes[..16].copy_from_slice(session.as_bytes());
    TopicId::from_bytes(bytes)
}

// =============================================================
// Host drops a spoofed `SendRequest` whose body `peer.id` doesn't
// match the gossip-authenticated `delivered_from`.
// =============================================================

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn host_drops_send_request_with_spoofed_peer_id() {
    let (daemon_a, daemon_b, _dns_pkarr) = common::spawn_pair().await;

    // Alice on daemon A hosts and subscribes so we can observe (or
    // fail to observe) inbound traffic.
    let alice_client = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_id = daemon_a.peer_id();
    let alice = PeerInfo::new(alice_id, "alice");
    let host_resp = alice_client
        .request(Request::HostSession {
            display_name: "alice".into(),
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

    // Bob on daemon B joins legitimately so the gossip mesh between
    // the two daemons is wired up. We then sneak a spoofed frame in
    // alongside the real bridge traffic.
    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    let join_resp = bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(join_resp, Response::JoinSession { .. }));

    // Subscribe to the session's gossip topic from daemon B's
    // `Gossip` instance directly. Same id we'd reach via the bridge
    // (which is already subscribed); iroh-gossip multiplexes
    // multiple subscribers on one topic on a single endpoint.
    let topic_id = topic_for(session_id);
    let topic = daemon_b
        .gossip
        .subscribe(topic_id, vec![daemon_a.iroh_addr.id])
        .await
        .expect("B raw subscribes");
    let (sender, mut receiver) = topic.split();
    timeout(Duration::from_secs(15), receiver.joined())
        .await
        .expect("raw subscribe never joined")
        .expect("joined() errored");

    // Forge: B's endpoint publishes a SendRequest carrying alice's
    // peer-id in the body. The host (alice's daemon) authenticates
    // `delivered_from` as B but reads `peer.id == alice` in the body
    // — must be dropped.
    let spoofed_payload = b"spoofed-from-bob-claiming-to-be-alice".to_vec();
    let body = GossipBody::SendRequest {
        req_id: Uuid::new_v4(),
        peer: alice.clone(),
        payload: SendPayload {
            kind: MessageKind::Chat,
            action: "chat.message".into(),
            payload: spoofed_payload.clone(),
        },
    };
    sender
        .broadcast(Bytes::from(gossip::encode(&body)))
        .await
        .expect("broadcast spoofed SendRequest");

    // For 2s in parallel: (a) the gossip topic must not see a
    // SendAck (the host doesn't ack a dropped frame), and (b)
    // alice's IPC stream must not surface a Message for the
    // spoofed payload.
    let deadline = Instant::now() + Duration::from_secs(2);
    let drain_gossip = async {
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match timeout(remaining, receiver.next()).await {
                Ok(Some(Ok(GossipEvent::Received(msg)))) => {
                    let decoded = gossip::decode(&msg.content).expect("decode round-trip");
                    assert!(
                        !matches!(decoded, GossipBody::SendAck { .. }),
                        "host produced a SendAck for the spoofed SendRequest",
                    );
                }
                Ok(None) => break,
                Ok(Some(Ok(_))) | Err(_) => {}
                Ok(Some(Err(err))) => panic!("gossip receiver errored mid-test: {err}"),
            }
        }
    };
    let drain_alice = async {
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match timeout(remaining, alice_events.recv()).await {
                Ok(Some(event)) => {
                    if let Event::Message { message, .. } = event {
                        assert_ne!(
                            message.payload, spoofed_payload,
                            "host accepted the spoofed SendRequest and fanned it out",
                        );
                    }
                }
                Ok(None) => panic!("alice IPC events channel closed mid-test"),
                Err(_) => {}
            }
        }
    };
    tokio::join!(drain_gossip, drain_alice);

    drop(receiver);
    drop(sender);
    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Host drops a `JoinAnnouncement` whose body `peer.id` doesn't match
// the delivered_from. Asserts the membership snapshot stays at the
// host alone, and no `PeerJoined` for the ghost id ever fires.
// =============================================================

#[tokio::test]
async fn host_drops_join_announcement_with_spoofed_peer_id() {
    let (daemon_a, daemon_b, _dns_pkarr) = common::spawn_pair().await;

    let alice_client = Client::connect(&daemon_a.socket).await.unwrap();
    let host_resp = alice_client
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: None,
        })
        .await
        .unwrap();
    let session_id = match host_resp {
        Response::HostSession { session, .. } => session,
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

    // Subscribe directly via daemon B's gossip — no client-side join.
    // We want B to be the gossip-mesh sender for the spoofed frame
    // without becoming a session member.
    let topic_id = topic_for(session_id);
    let topic = daemon_b
        .gossip
        .subscribe(topic_id, vec![daemon_a.iroh_addr.id])
        .await
        .expect("B raw subscribes");
    let (sender, mut receiver) = topic.split();
    timeout(Duration::from_secs(15), receiver.joined())
        .await
        .expect("raw subscribe never joined")
        .expect("joined() errored");

    // Forge a JoinAnnouncement claiming to be a never-seen peer
    // (curve-valid id derived from seed 0xde).
    let ghost_peer_id = common::valid_peer_id(0xde);
    let ghost = PeerInfo::new(ghost_peer_id, "ghost");
    let body = GossipBody::JoinAnnouncement {
        peer: ghost.clone(),
        timestamp_ms: 0,
    };
    sender
        .broadcast(Bytes::from(gossip::encode(&body)))
        .await
        .expect("broadcast spoofed JoinAnnouncement");

    // For 2s, alice must not see PeerJoined for the ghost id.
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let Ok(Some(event)) = timeout(remaining, alice_events.recv()).await else {
            break;
        };
        if let Event::PeerJoined { peer, .. } = event {
            assert_ne!(
                peer.id, ghost_peer_id,
                "host admitted a ghost peer via spoofed JoinAnnouncement",
            );
        }
    }

    // Membership snapshot: Alice alone (no real joiner ever joined).
    let list_resp = alice_client.request(Request::ListSessions).await.unwrap();
    let summary = match list_resp {
        Response::ListSessions { sessions } => sessions
            .into_iter()
            .find(|s| s.id == session_id)
            .expect("session present"),
        other => panic!("expected ListSessions, got {other:?}"),
    };
    assert_eq!(summary.peer_count, 1, "host alone");

    // The host (daemon_a) must not have captured B's `EndpointId` in
    // `tracked_peer_ids`. B never sent a legitimate frame — the
    // spoofed JoinAnnouncement was the only thing on the wire from
    // them — so the shutdown-snapshot path must not persist B's addr.
    let tracked = daemon_a.tracked_peer_ids_snapshot();
    assert!(
        !tracked.contains(&daemon_b.iroh_addr.id),
        "host cached spoofed-only sender's EndpointId: {tracked:?}",
    );

    drop(receiver);
    drop(sender);
    drop(alice_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Regression guard: a legitimate joiner-side `Send` (via the bridge,
// which stamps the daemon's authenticated id) is admitted normally.
// Pins "the host doesn't over-drop" alongside the spoofing tests.
// =============================================================

#[tokio::test]
async fn host_accepts_send_request_with_matching_peer_id() {
    let (daemon_a, daemon_b, _dns_pkarr) = common::spawn_pair().await;

    let alice_client = Client::connect(&daemon_a.socket).await.unwrap();
    let host_resp = alice_client
        .request(Request::HostSession {
            display_name: "alice".into(),
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

    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    let bob_id = daemon_b.peer_id();
    bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    bob_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"hi from bob (legit)".to_vec(),
            },
        })
        .await
        .unwrap();

    let alice_msg =
        common::expect_message_with_payload(&mut alice_events, b"hi from bob (legit)", "alice")
            .await;
    assert_eq!(alice_msg.peer.id, bob_id);

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Joiner-side outbound stamp: even when Bob's IPC client supplies a
// deliberately-wrong `peer.id`, the bridge overrides it with the
// daemon's authenticated id before publishing. Alice observes the
// real id, not the IPC caller's claim.
// =============================================================

#[tokio::test]
async fn joiner_outbound_stamps_authenticated_peer_id() {
    let (daemon_a, daemon_b, _dns_pkarr) = common::spawn_pair().await;

    let alice_client = Client::connect(&daemon_a.socket).await.unwrap();
    let host_resp = alice_client
        .request(Request::HostSession {
            display_name: "alice".into(),
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

    // Auth L1 fix #3 (PROTOCOL_VERSION 5): the IPC has no `peer.id`
    // field anymore — the daemon stamps its authenticated id
    // server-side. This test pins the property at the wire level: Bob's
    // observed `peer.id` always equals daemon B's authenticated id,
    // regardless of any display_name we pass.
    let bob_real_id = daemon_b.peer_id();
    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    bob_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"signed by my-real-id".to_vec(),
            },
        })
        .await
        .unwrap();

    let observed: SessionMessage =
        common::expect_message_with_payload(&mut alice_events, b"signed by my-real-id", "alice")
            .await;
    // The IPC has no `peer.id` field post-fix-#3, so there's no way
    // for an embedder to lie. Pin the load-bearing property:
    // `observed.peer.id` ALWAYS equals daemon B's authenticated id.
    assert_eq!(
        observed.peer.id, bob_real_id,
        "the daemon must stamp its authenticated id, not whatever the IPC client supplied",
    );

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
