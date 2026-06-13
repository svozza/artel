//! Auth Slice B.5 — control-frame & sequence authentication:
//! regression suite.
//!
//! Pins the load-bearing properties end-to-end, over real iroh-gossip
//! transport (Tier B against `DnsPkarrServer`):
//!
//! - A forged `SessionClosed` (wrong-key `host_sig`) is dropped — the
//!   victim joiner's mirror survives (finding #2).
//! - A `SessionClosed` replayed across a host epoch bump is dropped:
//!   the resume `EpochBeacon` advances the joiner's watermark past the
//!   captured close's epoch (D3, the load-bearing mechanism).
//! - A forged `SendAck` (Ok or Err) with a bad `host_sig` does not
//!   resolve the joiner's in-flight send — the IPC client never sees
//!   the spoofed result (finding #3).
//! - A genuine host `Message` replayed under a fresh seq is dropped by
//!   the host seq-sig; the joiner appends exactly one (finding #1).
//! - The happy path: host send → mirror appends; host close tears the
//!   mirror down. All valid frames accepted.
//!
//! Host-origin authentication is by signature against the host pubkey
//! the joiner persists as `session.host` (= the ticket's
//! `host_peer_id`), so it is topology-independent. See
//! `docs/plans/2026-06-03-auth-slice-b5-control-frame-auth-plan.md` and
//! `docs/brainstorms/2026-06-02-control-frame-auth-slice-b5-brainstorm.md`.

#![cfg(feature = "iroh")]

mod common;

use std::time::{Duration, Instant};

use artel_client::Client;
use artel_daemon::EndpointSetup;
use artel_protocol::gossip::{self, GossipBody};
use artel_protocol::rpc::SendPayload;
use artel_protocol::signing::{self, SigningKey};
use artel_protocol::{
    Event, MessageKind, ProtocolError, Request, Response, SessionId, SessionMessage,
};
use bytes::Bytes;
use futures_util::StreamExt;
use iroh_gossip::api::Event as GossipEvent;
use pretty_assertions::assert_eq;
use tokio::time::timeout;

// INTERIM (iroh 0.98.2): the `_n0` tests below dial through n0's
// public relay (`Production`) instead of the localhost shared relay.
// noq-proto 0.17.0 has a handshake path-poisoning bug: when a dialer
// knows both a relay URL and same-machine direct addrs, the relay
// copy of the Initial packet wins the race to a localhost relay and
// wedges the acceptor's handshake (gossip JOIN_READY timeouts).
// Against the public relay the direct packet always wins, so the bug
// stays dormant. Fixed upstream in noq-proto 1.0.0-rc. When the iroh
// 1.0 upgrade lands, revert to `common::shared_relay_url()` +
// `ProductionCustomRelay` so Tier C stops depending on n0's relay.
// Full writeup: docs/diagnosing-flaky-tests.md case study 2026-06-11.
const fn n0_relay_setup() -> EndpointSetup {
    EndpointSetup::Production
}

use common::topic_for;

/// Re-host the same id (resume → host-epoch bump + signed beacon).
async fn resume_host(client: &Client, session_id: SessionId) {
    let resp = client
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: Some(session_id),
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::HostSession { .. }));
}

/// Capture the `host_sig` of an `EpochBeacon` at `target_epoch` off the
/// wire. Panics on timeout.
async fn capture_beacon_at(
    receiver: &mut iroh_gossip::api::GossipReceiver,
    target_epoch: u64,
) -> [u8; 64] {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, receiver.next()).await {
            Ok(Some(Ok(GossipEvent::Received(msg)))) => {
                if let Ok(GossipBody::EpochBeacon {
                    host_epoch,
                    host_sig,
                }) = gossip::decode(&msg.content)
                    && host_epoch == target_epoch
                {
                    return host_sig;
                }
            }
            Ok(Some(Ok(_))) | Err(_) => {}
            Ok(None) => break,
            Ok(Some(Err(err))) => panic!("receiver errored: {err}"),
        }
    }
    panic!("never captured an EpochBeacon at epoch {target_epoch}");
}

/// Wire alice-hosts / bob-joins over two already-spawned daemons
/// (works for either the hermetic pair or a real-n0 pair). Returns the
/// session id, both IPC clients, and bob's event stream.
async fn wire_host_and_join(
    daemon_a: &common::RunningDaemon,
    daemon_b: &common::RunningDaemon,
) -> (SessionId, Client, Client, artel_client::EventStream) {
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

    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    let join_resp = bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(join_resp, Response::JoinSession { .. }));
    let bob_events = bob_client.take_events().await.expect("bob events");
    bob_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    (session_id, alice_client, bob_client, bob_events)
}

/// Host (alice) + joiner (bob) over `spawn_pair`, with bob subscribed
/// for events. Returns the daemons, dns handle, session id, both IPC
/// clients, and bob's event stream.
#[allow(clippy::type_complexity)]
async fn host_and_join() -> (
    common::RunningDaemon,
    common::RunningDaemon,
    std::sync::Arc<iroh::test_utils::DnsPkarrServer>,
    SessionId,
    Client,
    Client,
    artel_client::EventStream,
) {
    let (daemon_a, daemon_b, dns_pkarr) = common::spawn_pair().await;
    let (session_id, alice_client, bob_client, bob_events) =
        wire_host_and_join(&daemon_a, &daemon_b).await;
    (
        daemon_a,
        daemon_b,
        dns_pkarr,
        session_id,
        alice_client,
        bob_client,
        bob_events,
    )
}

/// Drain `events` for `dur`, asserting no `Event::SessionClosed`
/// arrives. Used to prove a forged/replayed close was dropped.
async fn assert_no_session_closed(
    events: &mut artel_client::EventStream,
    dur: Duration,
    who: &str,
) {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, events.recv()).await {
            Ok(Some(Event::SessionClosed { .. })) => {
                panic!("{who}: saw SessionClosed — a forged/replayed close was accepted");
            }
            Ok(Some(_)) | Err(_) => {}
            Ok(None) => break,
        }
    }
}

// =============================================================
// A forged `SessionClosed` (wrong-key host_sig) is dropped; the
// victim joiner's mirror survives. Finding #2.
// =============================================================

async fn forged_session_closed_dropped_impl(
    daemon_a: &common::RunningDaemon,
    daemon_b: &common::RunningDaemon,
) {
    let (session_id, alice_client, _bob_client, mut bob_events) =
        wire_host_and_join(daemon_a, daemon_b).await;

    // A non-host topic member broadcasts a SessionClosed signed with a
    // key that is NOT the host's. verify_ctrl must reject it.
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

    let attacker = SigningKey::from_bytes(&[0xee; 32]);
    let host_epoch = 0u64;
    let host_sig = signing::sign_ctrl(&attacker, session_id, host_epoch);
    sender
        .broadcast(Bytes::from(gossip::encode(&GossipBody::SessionClosed {
            host_epoch,
            host_sig,
        })))
        .await
        .expect("broadcast forged SessionClosed");

    // Bob must NOT see a SessionClosed event — the forged close is
    // dropped at verify_ctrl.
    assert_no_session_closed(&mut bob_events, Duration::from_secs(2), "bob").await;

    // And bob's session is still live: a host send still reaches him.
    alice_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"still alive".to_vec(),
            },
        })
        .await
        .unwrap();
    let _ = common::expect_message_with_payload(&mut bob_events, b"still alive", "bob").await;

    drop(sender);
}

#[tokio::test]
async fn forged_session_closed_dropped() {
    let (daemon_a, daemon_b, _dns) = common::spawn_pair().await;
    forged_session_closed_dropped_impl(&daemon_a, &daemon_b).await;
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// A forged `SendAck` (Ok with a bogus message) with a bad host_sig
// does not resolve the joiner's in-flight send. Finding #3.
// =============================================================

/// Drive a forged-ack scenario for one `result` shape. Returns once
/// the assertions hold. Deterministic: the forged ack is the ONLY ack
/// bob can resolve against, because we inject it for a `req_id` we
/// own — bob's genuine send (a separate, real IPC send) runs in
/// parallel and must resolve on the host's *genuine* ack, never on our
/// forged frame.
///
/// Design (per `docs/diagnosing-flaky-tests.md`: gate on events, never
/// sleep). iroh-gossip does not surface a node's *inbound* frames to a
/// sibling raw subscription on the same endpoint, so we cannot observe
/// bob's real `req_id` from a third-party sub. Instead we assert the
/// load-bearing primitive directly at the wire boundary: the host's
/// genuine `SendAck` (captured off daemon B's mesh-joined receiver)
/// passes `verify_ack` under the host pubkey, while an attacker-signed
/// ack for the same `req_id` and `result` does NOT — which is exactly
/// the check the joiner's `SendAck` arm runs before resolving. The
/// bridge's resolve-only-after-verify behaviour is unit-tested in
/// `session.rs`; this pins it end-to-end over real transport.
async fn forged_ack_rejected_for(payload: &[u8], forged_result_is_err: bool) {
    let (daemon_a, daemon_b, _dns, session_id, alice_client, bob_client, _bob_events) =
        host_and_join().await;

    // Raw-subscribe on daemon B (the joiner side), bootstrapped to the
    // host, so we observe the host's genuine SendAck (host → bob).
    let topic_id = topic_for(session_id);
    let topic = daemon_b
        .gossip
        .subscribe(topic_id, vec![daemon_a.iroh_addr.id])
        .await
        .expect("raw subscribes");
    let (_sender, mut receiver) = topic.split();
    timeout(Duration::from_secs(15), receiver.joined())
        .await
        .expect("raw subscribe never joined")
        .expect("joined() errored");

    // Bob's real IPC send. Membership is per-connection state
    // populated by JoinSession, so the send must ride bob's original
    // (joined) connection. Share it via Arc with the spawned task.
    let bob_arc = std::sync::Arc::new(bob_client);
    let bob_send = {
        let bob = std::sync::Arc::clone(&bob_arc);
        let payload = payload.to_vec();
        tokio::spawn(async move {
            bob.request(Request::Send {
                session: session_id,
                payload: SendPayload {
                    kind: MessageKind::Chat,
                    action: "chat.message".into(),
                    payload,
                },
            })
            .await
        })
    };

    // Bob's send must resolve via the host's GENUINE ack — a real
    // host-sequenced `Sent`, never a forged result.
    let sent = timeout(Duration::from_secs(20), bob_send)
        .await
        .expect("bob send did not resolve")
        .expect("send task panicked");
    assert!(
        matches!(sent, Ok(Response::Sent { .. })),
        "bob send must resolve via the host's genuine ack, got {sent:?}",
    );

    // Capture the host's genuine SendAck off the wire and pin the
    // verify_ack primitive: genuine passes under the host pubkey; an
    // attacker-signed ack for the same (req_id, result) does NOT.
    let host_pubkey = daemon_a.peer_id();
    let attacker = SigningKey::from_bytes(&[0xee; 32]);
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut checked = false;
    while Instant::now() < deadline && !checked {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, receiver.next()).await {
            Ok(Some(Ok(GossipEvent::Received(msg)))) => {
                let Ok(GossipBody::SendAck {
                    req_id,
                    result,
                    host_sig,
                }) = gossip::decode(&msg.content)
                else {
                    continue;
                };
                // Genuine host ack verifies under the host pubkey.
                signing::verify_ack(&host_pubkey, session_id, req_id, &result, &host_sig)
                    .expect("genuine host SendAck must verify under host pubkey");

                // A forged ack for the same req_id with an
                // attacker-flipped result must NOT verify — this is the
                // exact gate the joiner runs before resolving, so a
                // forged ack can never resolve bob's send.
                let forged_result: Result<SessionMessage, ProtocolError> = if forged_result_is_err {
                    Err(ProtocolError::Internal("FORGED rejection".into()))
                } else {
                    let bogus = SessionMessage::new(
                        artel_protocol::Seq::new(999),
                        1,
                        artel_protocol::PeerInfo::new(daemon_b.peer_id(), "bob"),
                        MessageKind::Chat,
                        "chat.message",
                        b"FORGED RESULT".to_vec(),
                        artel_protocol::SIGNATURE_UNSIGNED,
                        artel_protocol::SIGNATURE_UNSIGNED,
                    );
                    Ok(bogus)
                };
                let forged_sig = signing::sign_ack(&attacker, session_id, req_id, &forged_result);
                assert!(
                    signing::verify_ack(
                        &host_pubkey,
                        session_id,
                        req_id,
                        &forged_result,
                        &forged_sig,
                    )
                    .is_err(),
                    "attacker-signed ack must fail verify_ack under the host pubkey",
                );
                checked = true;
            }
            Ok(Some(Ok(_))) | Err(_) => {}
            Ok(None) => break,
            Ok(Some(Err(err))) => panic!("receiver errored: {err}"),
        }
    }
    assert!(
        checked,
        "never observed the host's genuine SendAck to verify"
    );

    drop(alice_client);
    drop(bob_arc);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// A forged `SendAck` (Ok with a bogus message) with a bad host_sig
// does not resolve the joiner's in-flight send. Finding #3.
// =============================================================

#[tokio::test]
async fn forged_send_ack_ok_dropped() {
    forged_ack_rejected_for(b"bob real send ok", false).await;
}

// =============================================================
// A forged `SendAck` Err with a bad host_sig does not resolve bob's
// send with the spoofed error. Finding #3 (Err shape).
// =============================================================

#[tokio::test]
async fn forged_send_ack_err_dropped() {
    forged_ack_rejected_for(b"bob real send err", true).await;
}

// =============================================================
// A genuine host `Message` replayed under a fresh seq is dropped by
// the host seq-sig; the joiner appends exactly one. Finding #1.
// =============================================================

#[tokio::test]
async fn replayed_message_under_new_seq_dropped() {
    let (daemon_a, daemon_b, _dns, session_id, alice_client, bob_client, mut bob_events) =
        host_and_join().await;

    // Raw-subscribe to capture the host's genuine broadcast.
    let topic_id = topic_for(session_id);
    let topic = daemon_b
        .gossip
        .subscribe(topic_id, vec![daemon_a.iroh_addr.id])
        .await
        .expect("raw subscribes");
    let (sender, mut receiver) = topic.split();
    timeout(Duration::from_secs(15), receiver.joined())
        .await
        .expect("raw subscribe never joined")
        .expect("joined() errored");

    // Host sends a real message; capture the genuine Message frame off
    // the wire (it carries the host's seq-sig).
    alice_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"genuine host msg".to_vec(),
            },
        })
        .await
        .unwrap();

    let mut captured: Option<SessionMessage> = None;
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && captured.is_none() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, receiver.next()).await {
            Ok(Some(Ok(GossipEvent::Received(msg)))) => {
                if let Ok(GossipBody::Message(m)) = gossip::decode(&msg.content)
                    && m.payload == b"genuine host msg"
                {
                    captured = Some(m);
                }
            }
            Ok(Some(Ok(_))) | Err(_) => {}
            Ok(None) => break,
            Ok(Some(Err(err))) => panic!("receiver errored: {err}"),
        }
    }
    let captured = captured.expect("never captured the genuine host Message");

    // Bob's mirror should have appended the genuine message once.
    let _ = common::expect_message_with_payload(&mut bob_events, b"genuine host msg", "bob").await;

    // Replay the genuine bytes under a fresh seq (a gap bob hasn't
    // seen). The host_sig is bound to the original seq, so verify_seq
    // rejects the replay — bob must not emit a second Message event.
    let mut replayed = captured.clone();
    replayed.seq = artel_protocol::Seq::new(captured.seq.get() + 100);
    sender
        .broadcast(Bytes::from(gossip::encode(&GossipBody::Message(replayed))))
        .await
        .expect("broadcast replayed Message");

    // Drain 2s: bob must NOT see the replayed payload a second time.
    let mut count = 0u32;
    let bound = Instant::now() + Duration::from_secs(2);
    while Instant::now() < bound {
        let remaining = bound.saturating_duration_since(Instant::now());
        if let Ok(Some(Event::Message { message, .. })) =
            timeout(remaining, bob_events.recv()).await
            && message.payload == b"genuine host msg"
        {
            count += 1;
        }
    }
    assert_eq!(count, 0, "replayed message under a new seq must be dropped");

    drop(sender);
    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// A `SessionClosed` replayed across a host epoch bump is dropped.
// Drives the D3 mechanism end to end: the host resume EpochBeacon
// advances the joiner's watermark past the captured close's epoch.
//
// The host close and the epoch beacon share canonical bytes
// (`verify_ctrl`), so a genuine epoch-N ctrl signature captured off
// the wire is a valid close signature at epoch N. We capture a real
// epoch-1 beacon signature, let a later epoch-2 beacon advance bob's
// watermark to 2, then replay a SessionClosed{epoch:1} carrying the
// captured (genuine) signature. It passes verify_ctrl but fails
// `host_epoch >= watermark` (1 < 2), so it is dropped.
// =============================================================

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn replayed_session_closed_across_epoch_bump_dropped() {
    let (daemon_a, daemon_b, _dns, session_id, alice_client, bob_client, mut bob_events) =
        host_and_join().await;

    let topic_id = topic_for(session_id);
    let topic = daemon_b
        .gossip
        .subscribe(topic_id, vec![daemon_a.iroh_addr.id])
        .await
        .expect("raw subscribes");
    let (sender, mut receiver) = topic.split();
    timeout(Duration::from_secs(15), receiver.joined())
        .await
        .expect("raw subscribe never joined")
        .expect("joined() errored");

    // Resume #1: epoch 0 → 1, beacon broadcast. Capture the genuine
    // epoch-1 ctrl signature.
    resume_host(&alice_client, session_id).await;
    let epoch1_sig = capture_beacon_at(&mut receiver, 1).await;

    // Resume #2: epoch 1 → 2, beacon broadcast. This advances bob's
    // watermark to 2. Capture the epoch-2 beacon off our receiver to
    // confirm the host emitted it.
    resume_host(&alice_client, session_id).await;
    let _epoch2_sig = capture_beacon_at(&mut receiver, 2).await;

    // Deterministic happens-before gate (no sleep): the host sends a
    // Message *after* the epoch-2 beacon. Bob's forwarder processes
    // topic frames in delivery order on a single task, so once bob's
    // IPC delivers this Message, bob has already processed the
    // epoch-2 beacon — i.e. its watermark is durably 2. (The watermark
    // is monotonic, so it can only stay >= 2 afterwards.)
    alice_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"after epoch2 beacon".to_vec(),
            },
        })
        .await
        .unwrap();
    let _ =
        common::expect_message_with_payload(&mut bob_events, b"after epoch2 beacon", "bob").await;

    // Replay a SessionClosed at epoch 1 with the genuine captured
    // signature. verify_ctrl passes (real host sig at epoch 1), but
    // 1 < watermark(2) → dropped. Bob has provably processed the
    // epoch-2 beacon already, so no ordering race remains.
    sender
        .broadcast(Bytes::from(gossip::encode(&GossipBody::SessionClosed {
            host_epoch: 1,
            host_sig: epoch1_sig,
        })))
        .await
        .expect("broadcast replayed close");

    // Bob must NOT see a SessionClosed.
    assert_no_session_closed(&mut bob_events, Duration::from_secs(2), "bob").await;

    // And bob is still live — a host send reaches him.
    alice_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"alive after replay".to_vec(),
            },
        })
        .await
        .unwrap();
    let _ =
        common::expect_message_with_payload(&mut bob_events, b"alive after replay", "bob").await;

    drop(sender);
    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Happy path: a genuine host send reaches the mirror; a genuine host
// close (via the host's IPC leave) takes effect for the joiner. All
// valid frames accepted.
//
// The close half asserts the *documented contract*, not the
// proactive-delivery optimization. `publish_session_closed` is
// explicitly best-effort: "joiners fall back to discovering the close
// via their next SendRequest timing out." So a genuine host close is
// "effective" for bob iff EITHER:
//   (a) bob's forwarder receives the proactive `Event::SessionClosed`
//       (the fast path — host→bob link still eager at close time), OR
//   (b) bob never gets the proactive event because plumtree had pruned
//       the host→bob link (eager→lazy) under the prior chatty exchange
//       and `forget_session` severed the topic before the lazy
//       IHave/IWant round-trip — in which case bob discovers the close
//       on his next send: the host topic is gone, so `bob.send()`
//       errors (UnknownSession once the mirror is dropped, or a
//       send-timeout if the mirror outlives the topic).
//
// Asserting (a)-OR-(b) tests what the system actually guarantees. The
// pure proactive-delivery assertion was flaky (~20%) because (b) is a
// real, latent, best-effort-by-design race (the host broadcasts
// `SessionClosed` eager-only, then tears the topic down in the same
// breath — iroh-gossip 0.98 has no graceful-leave/flush primitive).
// Slice C's host-private auto-grant `send` runs on the host's gossip
// forwarder task (JoinAnnouncement → ensure_member → send: a disk
// append + ed25519 sign, serial on that task), which shifts when the
// host services bob's traffic and nudges the prune to land — exposing
// the race more often. It does NOT add gossip traffic. See
// docs/brainstorms/2026-06-04-auth-slice-c-l2-delivery-rethink-brainstorm.md
// (D4: close-vs-teardown is a separate latent bug, tracked outside L2).
//
// The genuine-close *acceptance* security property (verify_ctrl passes,
// epoch >= watermark → close applied; forged/replayed → dropped) is
// covered deterministically by the `host_closed_session_drops_remote_
// mirror_and_emits_event` + `verify_ctrl` unit tests and by
// `host_close_propagates_session_closed_to_joiner` (no prior exchange →
// no prune → 0-flake proactive delivery). This test pins the e2e
// *effect*, not the wire timing.
async fn legit_host_frames_accepted_impl(
    daemon_a: &common::RunningDaemon,
    daemon_b: &common::RunningDaemon,
) {
    let (session_id, alice_client, bob_client, mut bob_events) =
        wire_host_and_join(daemon_a, daemon_b).await;

    // Genuine host send → bob's mirror appends.
    alice_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"legit host frame".to_vec(),
            },
        })
        .await
        .unwrap();
    let _ = common::expect_message_with_payload(&mut bob_events, b"legit host frame", "bob").await;

    // Genuine host close: alice leaves (host-of-Local → close-for-all).
    // The host signs the SessionClosed at its current epoch; bob's
    // verify_ctrl passes and host_epoch >= watermark, so the close is
    // accepted whenever it reaches bob.
    alice_client
        .request(Request::LeaveSession {
            session: session_id,
        })
        .await
        .unwrap();

    // Path (a): wait a bounded window for the proactive close event.
    // A short budget — if the eager link survived, the event arrives
    // promptly; if it was pruned, no amount of waiting delivers it
    // (the topic is already gone), so we fall through to path (b)
    // rather than burning the full budget on a doomed wait.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_close = false;
    while Instant::now() < deadline && !saw_close {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, bob_events.recv()).await {
            Ok(Some(Event::SessionClosed { session })) => {
                assert_eq!(session, session_id);
                saw_close = true;
            }
            Ok(Some(_)) | Err(_) => {}
            Ok(None) => break,
        }
    }

    if saw_close {
        return;
    }

    // Path (b): no proactive event — the close raced topic teardown.
    // The documented fallback says bob learns of the close on his next
    // send. The host topic is gone, so bob's send must error (it cannot
    // succeed against a closed session). Either the mirror was already
    // dropped (UnknownSession) or it outlived the topic and the send
    // times out at the bridge — both prove the close is effective.
    let send_result = bob_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"after host close".to_vec(),
            },
        })
        .await;
    assert!(
        send_result.is_err(),
        "genuine host close must take effect for bob: with no proactive \
         SessionClosed, his next send must fail (host topic gone), got \
         {send_result:?}",
    );
}

#[tokio::test]
async fn legit_host_frames_accepted() {
    let (daemon_a, daemon_b, _dns) = common::spawn_pair().await;
    legit_host_frames_accepted_impl(&daemon_a, &daemon_b).await;
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Tier C (real n0): the host-pubkey-from-ticket verification holds
// over real n0 transport, not just the hermetic DnsPkarrServer mesh.
// Functions suffixed `_n0` run only under the `n0` nextest profile.
// =============================================================

/// Spawn two real-n0 daemons and wait until both pkarr records are
/// queryable, mirroring `common::spawn_pair` but on
/// `EndpointSetup::Production`.
async fn spawn_pair_n0() -> (common::RunningDaemon, common::RunningDaemon) {
    let a = common::spawn_daemon(common::fresh_state(), n0_relay_setup()).await;
    let b = common::spawn_daemon(common::fresh_state(), n0_relay_setup()).await;
    (a, b)
}

#[tokio::test]
async fn legit_host_frames_accepted_n0() {
    let (daemon_a, daemon_b) = spawn_pair_n0().await;
    legit_host_frames_accepted_impl(&daemon_a, &daemon_b).await;
    daemon_a.stop().await;
    daemon_b.stop().await;
}

#[tokio::test]
async fn forged_session_closed_dropped_n0() {
    let (daemon_a, daemon_b) = spawn_pair_n0().await;
    forged_session_closed_dropped_impl(&daemon_a, &daemon_b).await;
    daemon_a.stop().await;
    daemon_b.stop().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn capability_survives_host_restart_n0() {
    use artel_client::ClientError;
    use artel_protocol::ProtocolError;
    use artel_protocol::capability::CapabilityAction;
    use artel_protocol::rpc::SendPayload;
    use tempfile::TempDir;

    // Persistent state for the host (survives across stop/respawn).
    let host_root = TempDir::new().unwrap();
    let host_paths = common::RestartState::under(host_root.path());

    // 1. Spawn host + joiner (both Production/n0).
    let daemon_a = common::spawn_daemon_at(&host_paths, n0_relay_setup()).await;
    let daemon_b = common::spawn_daemon(common::fresh_state(), n0_relay_setup()).await;

    // Alice hosts, bob joins → auto-granted RW.
    let (session_id, alice_client, bob_client, _bob_events) =
        wire_host_and_join(&daemon_a, &daemon_b).await;

    // Bob writes successfully (auto-grant in effect).
    let sent = bob_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"bob writes before restart".to_vec(),
            },
        })
        .await
        .unwrap();
    assert!(
        matches!(sent, Response::Sent { .. }),
        "bob can write pre-restart: {sent:?}",
    );

    // 2. Stop host gracefully (publishes SessionClosed, tears bob's mirror).
    drop(alice_client);
    daemon_a.stop().await;

    // 3. Respawn host at the SAME paths (cold start from disk).
    let daemon_a = common::spawn_daemon_at(&host_paths, n0_relay_setup()).await;

    // 4. Alice resumes the session (host(Some(id))).
    let alice_client = Client::connect(&daemon_a.socket).await.unwrap();
    let host_resp = alice_client
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: Some(session_id),
        })
        .await
        .unwrap();
    let fresh_ticket = match host_resp {
        Response::HostSession { ticket, .. } => ticket,
        other => panic!("expected HostSession, got {other:?}"),
    };

    // 5. Bob re-joins via a fresh ticket (his mirror was torn down).
    drop(bob_client);
    daemon_b.stop().await;
    let daemon_b = common::spawn_daemon(common::fresh_state(), n0_relay_setup()).await;
    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    let join_resp = bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: fresh_ticket,
        })
        .await
        .unwrap();
    assert!(matches!(join_resp, Response::JoinSession { .. }));
    bob_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();

    // 6. Bob writes → succeeds (session + caps survived restart,
    //    re-join triggered a fresh auto-grant).
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(
            Instant::now() < deadline,
            "bob's post-restart write never succeeded",
        );
        let resp = bob_client
            .request(Request::Send {
                session: session_id,
                payload: SendPayload {
                    kind: MessageKind::Chat,
                    action: "chat.message".into(),
                    payload: b"bob writes after restart".to_vec(),
                },
            })
            .await;
        match resp {
            Ok(Response::Sent { .. }) => break,
            // Transient: bob's join may not have propagated to the host yet.
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
            other => panic!("unexpected response: {other:?}"),
        }
    }

    // 7. Alice revokes bob on the reloaded session.
    let revoke = CapabilityAction::Revoke {
        peer: daemon_b.peer_id(),
    };
    alice_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Capability,
                action: revoke.action_str().into(),
                payload: revoke.encode(),
            },
        })
        .await
        .unwrap();

    // 8. Bob's next send is rejected (enforcement works post-resume).
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(
            Instant::now() < deadline,
            "bob's post-revoke send was never rejected after restart",
        );
        let resp = bob_client
            .request(Request::Send {
                session: session_id,
                payload: SendPayload {
                    kind: MessageKind::Chat,
                    action: "chat.message".into(),
                    payload: b"bob tries after revoke".to_vec(),
                },
            })
            .await;
        match resp {
            Err(ClientError::Protocol(ProtocolError::Capability(_))) => break,
            Ok(Response::Sent { .. }) => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            other => panic!("expected Capability error or transient Sent, got {other:?}"),
        }
    }

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
