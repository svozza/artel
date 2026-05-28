//! Joiner-side log persistence: a remote-mirror session must
//! persist its incoming gossip messages to disk so a daemon
//! restart at the same `state_dir` can replay them via
//! `Subscribe { since: None }`.
//!
//! Concretely: Alice hosts and sends a `MessageKind::System`
//! message (modelled on the `workspace.ticket` payload that
//! `artel-fs` sends). Bob joins; the message lands in his mirror
//! live. Bob's daemon stops. A fresh daemon starts at Bob's same
//! state dir. Bob's new client subscribes and the System message
//! must appear in the events stream — without the persistence
//! shipped in this slice, the message is never written to disk on
//! the joiner side and Subscribe replays nothing.
//!
//! Real-world consequence the test pins: `artel-fs::Workspace::join_with`
//! waits for the host's `workspace.ticket` System message via
//! `wait_for_ticket`. On a joiner-side daemon restart that wait
//! hangs forever without persistence; with it, the message
//! replays from disk and the workspace stands up cleanly.

#![cfg(feature = "iroh")]

mod common;

use std::time::Duration;

use artel_client::{Client, EventStream};
use artel_protocol::{Event, MessageKind, PeerId, PeerInfo, Request, Response, SendPayload};
use pretty_assertions::assert_eq;
use tokio::time::timeout;

const SYSTEM_PAYLOAD: &[u8] = b"workspace-ticket-fixture-bytes";
const SYSTEM_ACTION: &str = "workspace.ticket";

// `used_underscore_binding`: rebuild a fresh `State` from
// `RunningDaemon._state` to give the second daemon the same on-disk
// paths. Same shape as `host_resume_session_id.rs` in artel-fs.
#[tokio::test]
#[allow(clippy::used_underscore_binding)]
async fn joiner_replays_system_message_after_daemon_restart() {
    let (daemon_a, daemon_b) = common::spawn_pair().await;

    // Alice hosts.
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

    // Bob joins (first run).
    let bob_client_1 = Client::connect(&daemon_b.socket).await.unwrap();
    let bob = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
    let join_resp = bob_client_1
        .request(Request::JoinSession {
            peer: bob.clone(),
            ticket: ticket.clone(),
        })
        .await
        .unwrap();
    assert!(matches!(join_resp, Response::JoinSession { .. }));

    // Subscribe on bob's first run so the live System broadcast
    // lands in his events stream — and the gossip forwarder
    // therefore lands it in his on-disk log via the persistence
    // path under test.
    let _ = bob_client_1
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut bob_events_1 = bob_client_1.take_events().await.expect("events");

    // Alice sends the System message (the fixture for what
    // `artel-fs` would publish as `workspace.ticket`).
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

    // Confirm bob saw it live on the first run — sanity that
    // gossip delivery worked at all before we test the restart
    // path.
    expect_system_message(&mut bob_events_1, SYSTEM_PAYLOAD, "live").await;

    // Tear bob's first daemon down. Recover the on-disk paths so
    // we can reconstruct a fresh `State` for the second daemon.
    drop(bob_events_1);
    drop(bob_client_1);
    let bob_state_2 = common::State {
        root: daemon_b._state.root,
        socket: daemon_b._state.socket.clone(),
        pid: daemon_b._state.pid.clone(),
        sessions: daemon_b._state.sessions.clone(),
        iroh_key: daemon_b._state.iroh_key.clone(),
    };
    daemon_b.shutdown.trigger();
    timeout(Duration::from_secs(10), daemon_b.join)
        .await
        .expect("bob daemon stop")
        .expect("bob daemon join")
        .expect("bob daemon io");

    // Spawn a fresh daemon at bob's same paths. Reuse alice's
    // lookup so cross-seeding still works (each daemon's
    // MemoryLookup is shared in-process, so a fresh daemon at
    // the same iroh-key picks up alice's addr automatically). We
    // build a fresh lookup since the first one was moved into
    // the original daemon; the alice-side lookup still has bob's
    // addr in it from the first cross-seed.
    let lookup_b_2 = iroh::address_lookup::memory::MemoryLookup::new();
    let daemon_b_2 = common::spawn_daemon(bob_state_2, lookup_b_2).await;
    // No cross-seeding needed for this test: we're only reading
    // bob's local replay path. Bob's daemon doesn't dial alice
    // again because Subscribe just walks the persisted log.

    // Re-subscribe from bob's new daemon, asking for the full
    // history (`since: None`). The System message must surface
    // from the persisted log, NOT a re-broadcast from alice (we
    // are not poking alice between the two runs).
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

/// Drain `events` until a `MessageKind::System` event whose
/// payload equals `expected` arrives, or panic with `who` as
/// context after 20 s.
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
