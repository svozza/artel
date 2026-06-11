//! Iroh-gossip wiring + bridge end-to-end: raw payload exchange,
//! host-to-joiner fanout, joiner-to-host send round-trip, joiner
//! send-rejected after host close, joiner-side mirror persistence,
//! pre-join replay, host-close `SessionClosed` propagation, and the
//! join-announcement frame.
//!
//! Consolidated from eight per-file bins (`iroh_gossip_smoke`,
//! `iroh_gossip_fanout`, `iroh_join_announcement`,
//! `iroh_joiner_send_fanout`, `iroh_joiner_send_rejected`,
//! `iroh_session_closed`, `iroh_subscribe_replay`,
//! `iroh_remote_mirror_persists_log`) per
//! `docs/plans/2026-05-29-faster-cargo-test-plan.md` slice 3b. Each
//! original file's docstring is retained verbatim in section banners.
//!
//! All Tier B: hermetic against `DnsPkarrServer`. The smoke test
//! goes straight at `IrohRuntime` (no `Client`); everything else
//! uses `common::spawn_pair` + `Client`.

#![cfg(feature = "iroh")]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use artel_client::{Client, ClientError, EventStream};
use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig, EndpointSetup};
use artel_protocol::capability::CapabilityAction;
use artel_protocol::{Event, MessageKind, PeerInfo, ProtocolError, Request, Response, SendPayload};
use bytes::Bytes;
use futures_util::StreamExt;
use iroh::test_utils::DnsPkarrServer;
use iroh_gossip::api::Event as GossipEvent;
use iroh_gossip::proto::TopicId;
use pretty_assertions::assert_eq;
use tokio::time::timeout;

// =============================================================
// Phase 2c-1 smoke test: two in-process daemons exchange a payload
// over iroh-gossip on a shared topic.
//
// Goal: prove the gossip wiring inside `artel_daemon::Daemon` is
// correctly stitched together — `Endpoint`, `Gossip`, and `Router`
// share state, an inbound connection on the gossip ALPN reaches the
// `Gossip` instance, and broadcasts on a topic round-trip between
// two endpoints. **No `Registry` traffic** is involved here.
//
// Discovery runs against a localhost `DnsPkarrServer` shared by both
// daemons.
// =============================================================

/// Bin-local iroh-runtime harness for the smoke test. Reuses
/// `common::State` for the on-disk paths but goes around the IPC
/// boundary entirely (the smoke test needs `IrohRuntime` directly,
/// which `common::RunningDaemon` doesn't expose).
struct SmokeDaemon {
    runtime: artel_daemon::IrohRuntime,
    shutdown: Arc<Shutdown>,
    join: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl SmokeDaemon {
    async fn spawn(state: &common::State, dns_pkarr: &Arc<DnsPkarrServer>) -> Self {
        let daemon = Daemon::start(DaemonConfig {
            socket_path: state.socket.clone(),
            pid_path: state.pid.clone(),
            sessions_dir: state.sessions.clone(),
            iroh_key_path: Some(state.iroh_key.clone()),
            endpoint_setup: EndpointSetup::Testing {
                dns_pkarr: Arc::clone(dns_pkarr),
            },
        })
        .await
        .expect("daemon start");

        let runtime = daemon.iroh().clone();
        let shutdown = daemon.shutdown_handle();
        let join = tokio::spawn(daemon.run());
        Self {
            runtime,
            shutdown,
            join,
        }
    }

    async fn stop(self) {
        self.shutdown.trigger();
        timeout(Duration::from_secs(10), self.join)
            .await
            .expect("daemon did not exit within 10s")
            .expect("daemon panicked")
            .expect("daemon io");
    }
}

#[tokio::test]
async fn two_daemons_exchange_a_payload_over_gossip() {
    let dns_pkarr = Arc::new(DnsPkarrServer::run().await.expect("dns_pkarr"));
    let state_a = common::fresh_state();
    let state_b = common::fresh_state();

    let daemon_a = SmokeDaemon::spawn(&state_a, &dns_pkarr).await;
    let daemon_b = SmokeDaemon::spawn(&state_b, &dns_pkarr).await;

    let addr_a = daemon_a.runtime.endpoint.addr();
    let addr_b = daemon_b.runtime.endpoint.addr();
    let id_a = addr_a.id;
    let id_b = addr_b.id;
    assert_ne!(id_a, id_b, "both daemons must have distinct identities");

    // Wait until both daemons' pkarr records are queryable on the
    // shared localhost server. Without this, the joiner's first dial
    // can race the publisher's announce loop. The timeout budget is
    // generous; a real failure here is a publish loop bug, not a
    // network race.
    dns_pkarr
        .on_endpoint(&id_a, Duration::from_secs(10))
        .await
        .expect("daemon A pkarr-published");
    dns_pkarr
        .on_endpoint(&id_b, Duration::from_secs(10))
        .await
        .expect("daemon B pkarr-published");

    // Both subscribe to the same topic. A is the "host" (empty
    // bootstrap); B bootstraps from A.
    let topic_id = TopicId::from_bytes([0x42; 32]);

    let topic_a = daemon_a
        .runtime
        .gossip
        .subscribe(topic_id, vec![])
        .await
        .expect("A subscribes");
    let topic_b = daemon_b
        .runtime
        .gossip
        .subscribe(topic_id, vec![id_a])
        .await
        .expect("B subscribes");

    let (sender_a, mut receiver_a) = topic_a.split();
    let (sender_b, mut receiver_b) = topic_b.split();
    drop(sender_a); // unused — A only receives in this test

    // Wait for the join to settle on B's side. `joined()` resolves
    // once at least one neighbor has sent a NeighborUp.
    timeout(Duration::from_secs(15), receiver_b.joined())
        .await
        .expect("B never joined the topic")
        .expect("B joined() errored");

    // B broadcasts; A should observe it as Event::Received.
    let payload = Bytes::from_static(b"hello from B");
    sender_b
        .broadcast(payload.clone())
        .await
        .expect("B broadcast");

    let observed = timeout(Duration::from_secs(15), async {
        loop {
            match receiver_a.next().await {
                Some(Ok(GossipEvent::Received(msg))) => return msg.content,
                Some(Ok(
                    GossipEvent::NeighborUp(_) | GossipEvent::NeighborDown(_) | GossipEvent::Lagged,
                )) => {}
                Some(Err(err)) => panic!("A receiver error: {err}"),
                None => panic!("A stream ended before payload"),
            }
        }
    })
    .await
    .expect("A never observed B's broadcast");

    assert_eq!(observed.as_ref(), payload.as_ref());

    // Drop the topic handles before shutting the daemons down so the
    // gossip task doesn't fight us during Router::shutdown.
    drop(receiver_a);
    drop(sender_b);
    drop(receiver_b);

    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Phase 2c-2b: a host's `Send` reaches a joiner on a different
// daemon via gossip.
// =============================================================

#[tokio::test]
async fn host_sends_message_joiner_observes_via_gossip() {
    let (daemon_a, daemon_b, _dns_pkarr) = common::spawn_pair().await;

    // Alice on daemon A hosts.
    let alice_client = Client::connect(&daemon_a.socket).await.unwrap();
    let alice = PeerInfo::new(daemon_a.peer_id(), "alice");
    let host_resp = alice_client
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: None,
        })
        .await
        .unwrap();
    let (session_id, ticket) = match host_resp {
        Response::HostSession { session, ticket, .. } => (session, ticket),
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

    // Bob on daemon B joins via the real ticket.
    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    let join_resp = bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    match join_resp {
        Response::JoinSession { session, .. } => assert_eq!(session, session_id),
        other => panic!("expected JoinSession, got {other:?}"),
    }
    bob_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut bob_events = bob_client.take_events().await.expect("bob events");

    // Alice sends. Daemon A persists, fans out locally, and broadcasts
    // on the gossip topic. Daemon B's bridge forwarder decodes and
    // pushes into Bob's session log.
    alice_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"hi from alice".to_vec(),
            },
        })
        .await
        .unwrap();

    // Alice sees her own Message via the local broadcast.
    let alice_msg =
        common::expect_message_with_payload(&mut alice_events, b"hi from alice", "alice").await;
    assert_eq!(alice_msg.peer, alice);

    // Bob sees the same message via gossip.
    let bob_msg =
        common::expect_message_with_payload(&mut bob_events, b"hi from alice", "bob").await;
    assert_eq!(bob_msg.peer, alice);
    assert_eq!(bob_msg.seq, alice_msg.seq);
    assert_eq!(bob_msg.action, "chat.message");

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Phase 2c-2d: a joiner announces themselves on the gossip topic
// once the mesh is up so the host's IPC subscribers see `PeerJoined`
// proactively — without waiting for the joiner's first `SendRequest`
// (the lazy-admission path that 2c-2c shipped).
// =============================================================

#[tokio::test]
async fn joiner_announces_membership_without_sending() {
    let (daemon_a, daemon_b, _dns_pkarr) = common::spawn_pair().await;

    // Alice on daemon A hosts and subscribes to her own session so we
    // can observe events for incoming peers.
    let alice_client = Client::connect(&daemon_a.socket).await.unwrap();
    let host_resp = alice_client
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: None,
        })
        .await
        .unwrap();
    let (session_id, ticket) = match host_resp {
        Response::HostSession { session, ticket, .. } => (session, ticket),
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

    // Bob on daemon B joins via the real ticket. He never sends — we
    // want to verify Alice sees him purely from the JoinAnnouncement
    // frame the bridge broadcasts when Bob's gossip mesh comes up.
    //
    // Auth L1: Bob's outbound `peer.id` is stamped to daemon B's
    // authenticated `EndpointId` by the bridge regardless of what the
    // IPC client claims. Source the real id from `daemon_b.peer_id()`
    // so the `PeerJoined` assertion below matches what's on the wire.
    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    let bob = PeerInfo::new(daemon_b.peer_id(), "bob");
    let join_resp = bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
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
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
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

// =============================================================
// Phase 2c-2c: a joiner's `Send` reaches the host (and every other
// subscriber) via gossip-backed `SendRequest` / `SendAck`.
// =============================================================

#[tokio::test]
async fn joiner_send_round_trips_through_host() {
    let (daemon_a, daemon_b, _dns_pkarr) = common::spawn_pair().await;

    // Alice on daemon A hosts.
    let alice_client = Client::connect(&daemon_a.socket).await.unwrap();
    let host_resp = alice_client
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: None,
        })
        .await
        .unwrap();
    let (session_id, ticket) = match host_resp {
        Response::HostSession { session, ticket, .. } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    // Alice subscribes to her own session so we can observe the
    // host-side fanout when Bob sends.
    alice_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut alice_events = alice_client.take_events().await.expect("alice events");

    // Bob on daemon B joins via the real ticket. Auth L1: Bob's
    // outbound `peer.id` is stamped to daemon B's authenticated id at
    // the bridge — the IPC client's claim is ignored on the wire. Use
    // the real id so the assertions on `peer == bob` below match.
    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    let bob = PeerInfo::new(daemon_b.peer_id(), "bob");
    let join_resp = bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    match join_resp {
        Response::JoinSession { session, .. } => assert_eq!(session, session_id),
        other => panic!("expected JoinSession, got {other:?}"),
    }
    bob_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut bob_events = bob_client.take_events().await.expect("bob events");

    // Bob sends. Daemon B's bridge publishes a SendRequest on the
    // gossip topic; daemon A's bridge picks it up, drives
    // Registry::send (lazily admitting Bob to the session), persists,
    // fans out a Message frame, and replies with SendAck. Daemon B's
    // forwarder resolves the pending oneshot, the IPC reply lands.
    let send_resp = bob_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"hi from bob".to_vec(),
            },
        })
        .await
        .unwrap();
    let bob_sent_seq = match send_resp {
        Response::Sent { session, seq } => {
            assert_eq!(session, session_id);
            seq
        }
        other => panic!("expected Sent, got {other:?}"),
    };

    // Alice observes the message via the host's local broadcast.
    let alice_msg =
        common::expect_message_with_payload(&mut alice_events, b"hi from bob", "alice").await;
    assert_eq!(alice_msg.peer, bob);
    assert_eq!(alice_msg.seq, bob_sent_seq);
    assert_eq!(alice_msg.action, "chat.message");

    // Bob observes the same message via gossip — the Message frame
    // his forwarder produces for the local mirror.
    let bob_msg = common::expect_message_with_payload(&mut bob_events, b"hi from bob", "bob").await;
    assert_eq!(bob_msg.peer, bob);
    assert_eq!(bob_msg.seq, bob_sent_seq);
    assert_eq!(bob_msg.action, "chat.message");

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Auth Slice C / L2: capability lifecycle end-to-end. A joiner is
// admitted via ticket → auto-granted ReadWrite → can send → host
// revokes → joiner's next send is rejected with a Capability error.
// Exercises the every-peer enforcement teeth across a real gossip
// mesh: the revoke rides the log to the joiner's mirror, and the
// host authoritatively rejects the post-revoke write at `send`.
// =============================================================

#[tokio::test]
async fn capability_lifecycle_join_grant_write_revoke_reject() {
    let (daemon_a, daemon_b, _dns_pkarr) = common::spawn_pair().await;

    // Alice hosts.
    let alice_client = Client::connect(&daemon_a.socket).await.unwrap();
    let host_resp = alice_client
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: None,
        })
        .await
        .unwrap();
    let (session_id, ticket) = match host_resp {
        Response::HostSession { session, ticket, .. } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    // Bob joins via the real ticket and is auto-granted ReadWrite.
    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    let bob = PeerInfo::new(daemon_b.peer_id(), "bob");
    match bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap()
    {
        Response::JoinSession { session, .. } => assert_eq!(session, session_id),
        other => panic!("expected JoinSession, got {other:?}"),
    }
    bob_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();

    // Bob's first send succeeds — the auto-grant gave him write.
    let sent = bob_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"bob writes".to_vec(),
            },
        })
        .await
        .unwrap();
    assert!(
        matches!(sent, Response::Sent { .. }),
        "auto-granted joiner can write: {sent:?}",
    );

    // Alice (the host) revokes Bob via a Capability message.
    let revoke = CapabilityAction::Revoke { peer: bob.id };
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

    // Bob's next send is now rejected. The revoke is authoritative on
    // the host; the joiner's `send_remote` surfaces the host's verdict
    // as a Capability error. Retry until the rejection lands (the
    // revoke must propagate / be sequenced before Bob's request is
    // evaluated on the host).
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(
            Instant::now() < deadline,
            "bob's post-revoke send was never rejected",
        );
        let resp = bob_client
            .request(Request::Send {
                session: session_id,
                payload: SendPayload {
                    kind: MessageKind::Chat,
                    action: "chat.message".into(),
                    payload: b"bob writes again".to_vec(),
                },
            })
            .await;
        match resp {
            // The host's CapabilityDenied is forwarded verbatim to Bob's
            // IPC client as a `ProtocolError::Capability` (HostRejected
            // wire path), surfacing as a `ClientError::Protocol`.
            Err(ClientError::Protocol(ProtocolError::Capability(_))) => break,
            // The revoke may not have reached the host's projection yet;
            // a transient success means retry until it does.
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

// =============================================================
// 2c-2c added the joiner→host send round-trip via
// `SendRequest`/`SendAck`. 2c-2e made `LeaveSession` tear down the
// host's gossip topic. Follow-up (a) added the `SessionClosed`
// broadcast so joiners learn about the close proactively rather
// than via timeouts.
//
// This test exercises the resulting error path: after the host
// leaves and broadcasts `SessionClosed`, the joiner's mirror is
// gone, so a subsequent `Send` from the joiner's IPC client surfaces
// a specific `UnknownSession` instead of a generic timeout.
// =============================================================

#[tokio::test]
async fn joiner_send_after_host_closes_surfaces_unknown_session() {
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
        Response::HostSession { session, ticket, .. } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    // Bob on daemon B joins and subscribes so we can wait for the
    // SessionClosed event before sending — otherwise we race the
    // close-broadcast.
    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
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

    // Alice closes the session.
    alice_client
        .request(Request::LeaveSession {
            session: session_id,
        })
        .await
        .unwrap();

    // Wait for Bob to observe the close before sending.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
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

    // Bob's mirror is gone, so the registry rejects the send outright
    // with UnknownSession — no gossip round-trip, no timeout.
    match send_err {
        ClientError::Protocol(ProtocolError::UnknownSession(id)) => {
            assert_eq!(id, session_id);
        }
        other => panic!("expected UnknownSession, got {other:?}"),
    }

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Open follow-up (a): when the host closes a session, joiners learn
// about it via a `GossipBody::SessionClosed` broadcast and surface
// `Event::SessionClosed` to their IPC subscribers immediately,
// instead of finding out by sending into the void and timing out at
// the bridge's `SEND_REMOTE_TIMEOUT`.
// =============================================================

#[tokio::test]
async fn host_close_propagates_session_closed_to_joiner() {
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
        Response::HostSession { session, ticket, .. } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    // Bob on daemon B joins and subscribes so he has an event stream
    // we can read SessionClosed off of.
    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
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

    alice_client
        .request(Request::LeaveSession {
            session: session_id,
        })
        .await
        .unwrap();

    // Bob's events stream should surface SessionClosed within a few
    // seconds.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
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

// =============================================================
// Open follow-up (c): a joiner asks for the host's log on join via
// `GossipBody::Replay`, so messages sent *before* the joiner existed
// land in the joiner's mirror as Message events.
// =============================================================

#[tokio::test]
async fn joiner_replays_messages_sent_before_join() {
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
        Response::HostSession { session, ticket, .. } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    // Alice sends three messages BEFORE Bob joins.
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

    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
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
    // payloads. They may arrive in any order (replay backfill can
    // interleave with live broadcasts), so collect into a set.
    let expected: std::collections::HashSet<Vec<u8>> = [
        b"payload-0".to_vec(),
        b"payload-1".to_vec(),
        b"payload-2".to_vec(),
        b"payload-live".to_vec(),
    ]
    .into_iter()
    .collect();
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    while seen.len() < expected.len() {
        let remaining = deadline.saturating_duration_since(Instant::now());
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
            // Ignore the auto-grant Capability message the host emits
            // when it admits Bob (Auth Slice C / L2) — this test pins
            // Chat replay + live fan-out, not the cap log.
            if message.kind == MessageKind::Chat {
                seen.insert(message.payload);
            }
        }
    }
    assert_eq!(seen, expected);

    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Joiner-side log persistence: a remote-mirror session must persist
// its incoming gossip messages to disk so a daemon restart at the
// same `state_dir` can replay them via `Subscribe { since: None }`.
//
// Real-world consequence the test pins:
// `artel-fs::Workspace::join_with` waits for the host's
// `workspace.ticket` System message via `wait_for_ticket`. On a
// joiner-side daemon restart that wait hangs forever without
// persistence; with it, the message replays from disk and the
// workspace stands up cleanly.
// =============================================================

const SYSTEM_PAYLOAD: &[u8] = b"workspace-ticket-fixture-bytes";
const SYSTEM_ACTION: &str = "workspace.ticket";

// `used_underscore_binding`: rebuild a fresh `State` from
// `RunningDaemon._state` to give the second daemon the same on-disk
// paths. Same shape as `host_resume_session_id.rs` in artel-fs.
#[tokio::test]
#[allow(clippy::used_underscore_binding)]
async fn joiner_replays_system_message_after_daemon_restart() {
    let (daemon_a, mut daemon_b, dns_pkarr) = common::spawn_pair().await;

    let alice_client = Client::connect(&daemon_a.socket).await.unwrap();
    let host_resp = alice_client
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: None,
        })
        .await
        .unwrap();
    let (session_id, ticket) = match host_resp {
        Response::HostSession { session, ticket, .. } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    // Bob joins (first run).
    let bob_client_1 = Client::connect(&daemon_b.socket).await.unwrap();
    let join_resp = bob_client_1
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: ticket.clone(),
        })
        .await
        .unwrap();
    assert!(matches!(join_resp, Response::JoinSession { .. }));

    // Subscribe on bob's first run so the live System broadcast lands
    // in his events stream — and the gossip forwarder therefore lands
    // it in his on-disk log via the persistence path under test.
    let _ = bob_client_1
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut bob_events_1 = bob_client_1.take_events().await.expect("events");

    // Alice sends the System message (the fixture for what `artel-fs`
    // would publish as `workspace.ticket`).
    alice_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::System,
                action: SYSTEM_ACTION.into(),
                payload: SYSTEM_PAYLOAD.to_vec(),
            },
        })
        .await
        .unwrap();

    // Confirm bob saw it live on the first run.
    expect_system_message(&mut bob_events_1, SYSTEM_PAYLOAD, "live").await;

    // Tear bob's first daemon down. Recover the on-disk paths so we
    // can reconstruct a fresh `State` for the second daemon.
    drop(bob_events_1);
    drop(bob_client_1);
    let bob_state_2 = daemon_b._state.take().expect("state present");
    daemon_b.shutdown.trigger();
    timeout(Duration::from_secs(10), daemon_b.join)
        .await
        .expect("bob daemon stop")
        .expect("bob daemon join")
        .expect("bob daemon io");

    // Spawn a fresh daemon at bob's same paths. Reuse the shared
    // `Arc<DnsPkarrServer>` so the new daemon's pkarr publish lands
    // on the same localhost server alice is querying against.
    let daemon_b_2 = common::spawn_daemon(bob_state_2, common::testing_setup(&dns_pkarr)).await;

    // Re-subscribe from bob's new daemon, asking for the full history
    // (`since: None`). The System message must surface from the
    // persisted log, NOT a re-broadcast from alice.
    let bob_client_2 = Client::connect(&daemon_b_2.socket).await.unwrap();
    let _ = bob_client_2
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut bob_events_2 = bob_client_2.take_events().await.expect("events");

    expect_system_message(&mut bob_events_2, SYSTEM_PAYLOAD, "replay-after-restart").await;

    drop(bob_events_2);
    drop(bob_client_2);
    daemon_b_2.stop().await;
    drop(alice_client);
    daemon_a.stop().await;
}

/// Drain `events` until a `MessageKind::System` event whose payload
/// equals `expected` arrives, or panic with `who` as context after 20s.
async fn expect_system_message(events: &mut EventStream, expected: &[u8], who: &str) {
    timeout(Duration::from_secs(20), async {
        loop {
            let ev = events.recv().await.expect("event stream closed");
            if let Event::Message { message, .. } = ev
                && message.kind == MessageKind::System
                && message.action == SYSTEM_ACTION
            {
                assert_eq!(message.payload, expected, "{who}");
                return;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{who}: never saw System message"));
}
