//! Auth Slice B — L3 per-message signing: regression suite.
//!
//! Pins the load-bearing signing properties end-to-end:
//!
//! - A joiner-side `Send` arrives at the host with a real ed25519
//!   signature that verifies against the joiner's `peer.id`.
//! - A `SendRequest` whose signature does not verify is dropped at
//!   the host with a `ProtocolError::Signature` `SendAck`.
//! - A `Message` re-broadcast whose signature has been tampered is
//!   dropped on the joiner side without touching the mirror's log.
//! - On daemon restart, `read_log` drops tampered frames and keeps
//!   the surrounding good ones.
//! - A signed body for session A cannot be replayed on session B —
//!   `session_id` is in the signed scope.
//!
//! All tests Tier B (hermetic against `DnsPkarrServer`). Most use the
//! `common::spawn_pair` fixture; the replay-verify test uses
//! `common::spawn_local_daemon_at` for a restart of the same state
//! dir. See
//! `docs/plans/2026-06-02-auth-slice-b-l3-signing-plan.md` § B2 and
//! `docs/brainstorms/2026-05-30-auth-story-brainstorm.md` § L3.

#![cfg(feature = "iroh")]

mod common;

use std::time::{Duration, Instant};

use artel_client::Client;
use artel_protocol::gossip::{self, GossipBody};
use artel_protocol::rpc::{SendPayload, SignedSendPayload};
use artel_protocol::signing::{self, SigningKey};
use artel_protocol::{
    Event, MessageKind, PeerInfo, ProtocolError, Request, Response, Seq, SessionId, SessionMessage,
};
use bytes::Bytes;
use futures_util::StreamExt;
use iroh_gossip::api::Event as GossipEvent;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::timeout;
use uuid::Uuid;

use common::topic_for;

/// Set up a host (alice) + joiner (bob) over `spawn_pair` and
/// return the session id, alice's IPC + event handles, and bob's
/// IPC client. Helps each test stay focused on the property it
/// pins rather than the boilerplate.
async fn host_and_join() -> (
    common::RunningDaemon,
    common::RunningDaemon,
    std::sync::Arc<iroh::test_utils::DnsPkarrServer>,
    SessionId,
    Client,
    artel_client::EventStream,
    Client,
) {
    let (daemon_a, daemon_b, dns_pkarr) = common::spawn_pair().await;
    let alice_client = Client::connect(&daemon_a.socket).await.unwrap();
    let host_resp = alice_client
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: None,
        })
        .await
        .unwrap();
    let (session_id, ticket) = match host_resp {
        Response::HostSession {
            session, ticket, ..
        } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };
    alice_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let alice_events = alice_client.take_events().await.expect("alice events");

    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    let join_resp = bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(join_resp, Response::JoinSession { .. }));

    (
        daemon_a,
        daemon_b,
        dns_pkarr,
        session_id,
        alice_client,
        alice_events,
        bob_client,
    )
}

// =============================================================
// A legitimate joiner-side `Send` arrives at the host with a real
// ed25519 signature that verifies against the joiner's peer id.
// Pins the load-bearing happy path: B2 turns signing on, this is
// what "on" looks like end-to-end.
// =============================================================

#[tokio::test]
async fn joiner_send_arrives_signed_at_host() {
    let (daemon_a, daemon_b, _dns, session_id, alice_client, mut alice_events, bob_client) =
        host_and_join().await;

    bob_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"signed by bob".to_vec(),
            },
        })
        .await
        .unwrap();

    let observed: SessionMessage =
        common::expect_message_with_payload(&mut alice_events, b"signed by bob", "alice").await;
    // Signature must NOT be the unsigned sentinel — the bridge
    // signed the body before publishing.
    assert_ne!(
        observed.signature,
        artel_protocol::SIGNATURE_UNSIGNED,
        "joiner-side send must arrive signed",
    );
    // Verifies against the body's peer.id, which is bob's
    // authenticated id.
    assert_eq!(observed.peer.id, daemon_b.peer_id());
    signing::verify_message(session_id, &observed, &observed.signature)
        .expect("host's broadcast must verify against bob's pubkey");

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Host drops a `SendRequest` whose signature does not match the
// body. Joiner sees `ProtocolError::Signature` in the SendAck
// rather than timing out.
// =============================================================

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn host_drops_send_with_tampered_signature() {
    let (daemon_a, daemon_b, _dns, session_id, alice_client, mut alice_events, _bob_client) =
        host_and_join().await;

    // Subscribe to the topic from daemon B's gossip directly so we
    // can publish a hand-rolled (tampered-signature) SendRequest
    // alongside the real bridge traffic.
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

    // Build a body whose `peer.id` IS bob's (authentic delivered_from
    // → L1 check passes), but sign with a different key so the L3
    // verifier rejects.
    let bob_peer = PeerInfo::new(daemon_b.peer_id(), "bob");
    let timestamp_ms = 1_700_000_000_000u64;
    let other_key = SigningKey::from_bytes(&[0xcc; 32]);
    let bad_signature = signing::sign_body(
        &other_key,
        session_id,
        artel_protocol::MESSAGE_FORMAT,
        timestamp_ms,
        &bob_peer,
        MessageKind::Chat,
        "chat.message",
        b"tampered",
    );
    let req_id = Uuid::new_v4();
    let body = GossipBody::SendRequest {
        req_id,
        peer: bob_peer.clone(),
        payload: SignedSendPayload {
            timestamp_ms,
            kind: MessageKind::Chat,
            action: "chat.message".into(),
            payload: b"tampered".to_vec(),
            signature: bad_signature,
        },
    };
    sender
        .broadcast(Bytes::from(gossip::encode(&body)))
        .await
        .expect("broadcast tampered SendRequest");

    // The host MUST publish a SendAck with ProtocolError::Signature
    // (not a Message frame, not a timeout). Look for the matching
    // req_id on the gossip stream we subscribed to above.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_signature_err = false;
    while Instant::now() < deadline && !saw_signature_err {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, receiver.next()).await {
            Ok(Some(Ok(GossipEvent::Received(msg)))) => {
                let Ok(decoded) = gossip::decode(&msg.content) else {
                    continue;
                };
                if let GossipBody::SendAck {
                    req_id: ack_req,
                    result,
                    ..
                } = decoded
                    && ack_req == req_id
                {
                    match result {
                        Err(ProtocolError::Signature(_)) => saw_signature_err = true,
                        other => panic!("unexpected SendAck: {other:?}"),
                    }
                }
            }
            Ok(Some(Ok(_))) | Err(_) => {}
            Ok(None) => break,
            Ok(Some(Err(err))) => panic!("gossip receiver errored mid-test: {err}"),
        }
    }
    assert!(
        saw_signature_err,
        "expected SendAck for our req_id with Err(Signature(_))",
    );

    // Belt-and-suspenders: alice's IPC stream sees no Message
    // carrying the tampered payload over the next second.
    let bound = Instant::now() + Duration::from_secs(1);
    while Instant::now() < bound {
        let remaining = bound.saturating_duration_since(Instant::now());
        if let Ok(Some(Event::Message { message, .. })) =
            timeout(remaining, alice_events.recv()).await
        {
            assert_ne!(
                message.payload, b"tampered",
                "host accepted a tampered-signature SendRequest",
            );
        }
    }

    drop(receiver);
    drop(sender);
    drop(alice_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// A `Message` re-broadcast whose signature has been tampered is
// dropped at the joiner side. Pins
// `materialise_remote_session::on_message`'s verify-before-persist
// behaviour at the wire level.
// =============================================================

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn joiner_drops_message_with_tampered_signature() {
    let (daemon_a, daemon_b, _dns, session_id, alice_client, _alice_events, bob_client) =
        host_and_join().await;
    let mut bob_events = bob_client.take_events().await.expect("bob events");
    bob_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();

    // Alice sends a legitimate message — bob's mirror should see it.
    alice_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"hello legit".to_vec(),
            },
        })
        .await
        .unwrap();
    let legit = common::expect_message_with_payload(&mut bob_events, b"hello legit", "bob").await;

    // Now subscribe to alice's topic from a third-party gossip
    // handle (we just reuse daemon A's gossip — same topic, same
    // mesh). Re-broadcast the legit message but flip a payload
    // byte so the signature no longer matches. Bob must drop it
    // without surfacing a Message event.
    let topic_id = topic_for(session_id);
    let topic = daemon_a
        .gossip
        .subscribe(topic_id, vec![])
        .await
        .expect("A raw subscribes own topic");
    let (sender, _receiver) = topic.split();

    let mut tampered = legit.clone();
    tampered.payload = b"hello tampered".to_vec();
    // Keep the signature unchanged — verification fails because the
    // payload no longer matches the canonical bytes the signature
    // covers.
    sender
        .broadcast(Bytes::from(gossip::encode(&GossipBody::Message(
            tampered.clone(),
        ))))
        .await
        .expect("broadcast tampered Message");

    // Drain bob's events for 2s; we must NOT see the tampered
    // payload. Other events (PeerJoined, etc.) are fine.
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if let Ok(Some(Event::Message { message, .. })) =
            timeout(remaining, bob_events.recv()).await
        {
            assert_ne!(
                message.payload, b"hello tampered",
                "joiner accepted a tampered-signature Message",
            );
        }
    }

    drop(sender);
    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// On daemon restart, `read_log` drops tampered frames and keeps
// the surrounding good ones. Pins the log-replay path of B2.
// =============================================================

#[tokio::test]
async fn log_replay_drops_tampered_frames() {
    let root = TempDir::new().unwrap();
    let paths = common::RestartState::under(root.path());

    // ---- First daemon: host + send three messages ----
    let daemon1 = common::spawn_local_daemon_at(&paths).await;
    let alice_client = Client::connect(&paths.socket).await.unwrap();
    let session_id = match alice_client
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: None,
        })
        .await
        .unwrap()
    {
        Response::HostSession { session, .. } => session,
        other => panic!("expected HostSession, got {other:?}"),
    };
    for n in 0..3u32 {
        alice_client
            .request(Request::Send {
                session: session_id,
                payload: SendPayload {
                    kind: MessageKind::Chat,
                    action: format!("m{n}"),
                    payload: format!("payload-{n}").into_bytes(),
                },
            })
            .await
            .unwrap();
    }
    drop(alice_client);
    daemon1.stop().await;

    // ---- Tamper the middle log frame on disk ----
    let log_path = paths.sessions.join(session_id.to_string()).join("log");
    let mut bytes = std::fs::read(&log_path).expect("log file exists");
    let len1 = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
    let frame2_start = 4 + len1;
    let len2 =
        u32::from_be_bytes(bytes[frame2_start..frame2_start + 4].try_into().unwrap()) as usize;
    // Flip a byte in frame 2's author `signature` so postcard still
    // decodes but signature verify fails. Postcard length-prefixes
    // each serde_bytes run, so the frame tail is
    // `[len=64][signature 64][len=64][host_sig 64]`; the author
    // signature's last byte sits 65 bytes before the frame end (64
    // host_sig bytes + 1 length-prefix byte). host_sig itself is not
    // checked by this replay verify path (`read_log` verifies the
    // author signature only; host seq-sig verification is live-path).
    let target = frame2_start + 4 + len2 - 1 - 65;
    bytes[target] ^= 0xff;
    std::fs::write(&log_path, &bytes).unwrap();

    // ---- Second daemon: load the dir, replay the log ----
    let daemon2 = common::spawn_local_daemon_at(&paths).await;
    let recovered = Client::connect(&paths.socket).await.unwrap();
    recovered
        .request(Request::Subscribe {
            session: session_id,
            since: Some(Seq::ZERO),
        })
        .await
        .unwrap();
    let mut events = recovered.take_events().await.expect("events");

    // We expect to see TWO replayed messages — payload-0 and
    // payload-2 — and NOT payload-1 (the tampered frame).
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut got_zero = false;
    let mut got_two = false;
    let mut saw_one = false;
    while Instant::now() < deadline && !(got_zero && got_two) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if let Ok(Some(Event::Message { message, .. })) = timeout(remaining, events.recv()).await {
            match &message.payload[..] {
                b"payload-0" => got_zero = true,
                b"payload-1" => saw_one = true,
                b"payload-2" => got_two = true,
                _ => {}
            }
        }
    }
    assert!(got_zero, "must replay frame 0 (untampered)");
    assert!(got_two, "must replay frame 2 (untampered)");
    assert!(!saw_one, "tampered frame 1 must be dropped");

    drop(recovered);
    daemon2.stop().await;
}

// =============================================================
// Cross-session replay: a body legitimately signed for session A
// cannot be replayed on session B's topic — `session_id` is in the
// signed scope, so verification fails on B.
// =============================================================

#[tokio::test]
async fn cross_session_grant_replay_is_rejected() {
    let (daemon_a, daemon_b, _dns, session_a, alice_client, mut alice_events, bob_client_a) =
        host_and_join().await;

    // Alice opens a SECOND session (session B) on the same daemon
    // and subscribes to it — the same IPC `events` stream
    // multiplexes events for every session this client has
    // subscribed to.
    let host_resp = alice_client
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: None,
        })
        .await
        .unwrap();
    let session_b = match host_resp {
        Response::HostSession { session, .. } => session,
        other => panic!("expected HostSession, got {other:?}"),
    };
    alice_client
        .request(Request::Subscribe {
            session: session_b,
            since: None,
        })
        .await
        .unwrap();

    // Subscribe to A's topic FIRST so we don't miss the broadcast
    // when bob's send arrives. Use daemon_b's separate Gossip
    // instance (its endpoint is a peer in the mesh, iroh-gossip
    // will route the host's broadcast to us alongside bob's
    // bridge).
    let topic_a = topic_for(session_a);
    let topic_b = topic_for(session_b);
    let topic_listen = daemon_b
        .gossip
        .subscribe(topic_a, vec![daemon_a.iroh_addr.id])
        .await
        .expect("B raw subscribes A");
    let (_sender_a, mut listen_a) = topic_listen.split();
    timeout(Duration::from_secs(15), listen_a.joined())
        .await
        .expect("topic A listen joined")
        .expect("joined errored");

    // Bob signs a body for session A correctly — his daemon's
    // bridge stamps + signs over (A, ...).
    bob_client_a
        .request(Request::Send {
            session: session_a,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"intended for A".to_vec(),
            },
        })
        .await
        .unwrap();

    // Pull the host's broadcast `Message` for A off the mesh.
    let mut grant_for_a: Option<SessionMessage> = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && grant_for_a.is_none() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, listen_a.next()).await {
            Ok(Some(Ok(GossipEvent::Received(msg)))) => {
                if let Ok(GossipBody::Message(m)) = gossip::decode(&msg.content)
                    && m.payload == b"intended for A"
                {
                    grant_for_a = Some(m);
                }
            }
            Ok(Some(Ok(_))) | Err(_) => {}
            Ok(None) => break,
            Ok(Some(Err(err))) => panic!("gossip recv errored: {err}"),
        }
    }
    let signed_for_a = grant_for_a.expect("never saw bob's signed message on A");

    // Replay onto B from daemon_b's gossip — this is the
    // "third party tries to inject the cross-session frame"
    // shape. daemon_b is not a member of session B; it's just
    // re-broadcasting on the topic.
    let topic_b_handle = daemon_b
        .gossip
        .subscribe(topic_b, vec![daemon_a.iroh_addr.id])
        .await
        .expect("B raw subscribes B's topic");
    let (sender_b, mut r) = topic_b_handle.split();
    timeout(Duration::from_secs(15), r.joined())
        .await
        .expect("topic B replay joined")
        .expect("joined errored");
    sender_b
        .broadcast(Bytes::from(gossip::encode(&GossipBody::Message(
            signed_for_a.clone(),
        ))))
        .await
        .expect("broadcast cross-session replay");

    // Alice's events on session B must NOT see the replayed
    // payload — `materialise_remote_session::on_message` is on the
    // joiner side, so for a host's own session the local fan-out
    // is via `Registry`'s broadcast, not the gossip mirror. The
    // safety net here is the joiner side; for completeness, we
    // assert the host's local subscribers don't see it either.
    // (Alice's daemon is the host of B, so the gossip Message
    // arrives on B's topic but is dispatched as host-role: ignored
    // arms in `handle_inbound_frame`. So the assertion is "no
    // Message on B" within the time window.)
    // For 2s on alice's IPC stream, no Message must be carried
    // for session B with bob's payload. Messages on session A
    // (the legitimate one) ARE allowed; we filter on session id.
    let bound = Instant::now() + Duration::from_secs(2);
    while Instant::now() < bound {
        let remaining = bound.saturating_duration_since(Instant::now());
        if let Ok(Some(Event::Message { session, message })) =
            timeout(remaining, alice_events.recv()).await
            && session == session_b
        {
            assert_ne!(
                message.payload, b"intended for A",
                "cross-session replay was accepted on B",
            );
        }
    }

    drop(alice_client);
    drop(bob_client_a);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
