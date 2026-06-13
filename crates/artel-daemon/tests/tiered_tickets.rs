//! Phase 4B daemon-level e2e tests for tiered tickets:
//! - Read-only ticket joiners cannot send.
//! - RW ticket joiners (default from `HostSession`) can send.
//! - Expired tickets are rejected at admission.
//! - Direct-stream upgrade delivers secret only to the target peer.
//! - Revoked tickets are rejected at admission; `ListTickets`
//!   reflects status + `used_by`; revocation survives a host
//!   daemon restart.

#![cfg(feature = "iroh")]

mod common;

use std::time::Duration;

use tokio::time::timeout;

use artel_client::{Client, ClientError};
use artel_protocol::capability::Capability;
use artel_protocol::ticket::TicketStatus;
use artel_protocol::{
    Event, MessageKind, ProtocolError, Request, Response, SendPayload, UPGRADE_ACTION,
};

// =============================================================
// A joiner admitted via a Read-only ticket cannot send messages.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn read_ticket_joiner_cannot_send() {
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
    let (session_id, _ticket) = match host_resp {
        Response::HostSession {
            session, ticket, ..
        } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    // Alice subscribes so the session is live.
    alice_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();

    // Alice issues a Read-only ticket.
    let issue_resp = alice_client
        .request(Request::IssueTicket {
            session: session_id,
            granted_cap: Capability::Read,
            expiry_ms: 0,
        })
        .await
        .unwrap();
    let read_ticket = match issue_resp {
        Response::IssuedTicket { ticket, .. } => ticket,
        other => panic!("expected IssuedTicket, got {other:?}"),
    };

    // Bob on daemon B joins with the read-only ticket.
    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    let join_resp = bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: read_ticket,
        })
        .await
        .unwrap();
    match join_resp {
        Response::JoinSession { session, .. } => assert_eq!(session, session_id),
        other => panic!("expected JoinSession, got {other:?}"),
    }

    // Bob tries to send — should be rejected with a Capability error.
    // The rejection may take a round-trip through the host, so retry
    // until the host's cap projection is applied.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "bob's send was never rejected with a Capability error",
        );
        let resp = bob_client
            .request(Request::Send {
                session: session_id,
                payload: SendPayload {
                    kind: MessageKind::Chat,
                    action: "test".into(),
                    payload: b"hello".to_vec(),
                },
            })
            .await;
        match resp {
            Err(ClientError::Protocol(ProtocolError::Capability(_))) => break,
            // Transient success while the cap claim hasn't propagated yet.
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
// A joiner admitted via the default RW ticket can send, and the
// host observes the message.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn rw_ticket_joiner_can_send() {
    let (daemon_a, daemon_b, _dns_pkarr) = common::spawn_pair().await;

    // Alice on daemon A hosts (default ticket is RW).
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

    // Alice subscribes.
    alice_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut alice_events = alice_client.take_events().await.expect("alice events");

    // Carol on daemon B joins with Alice's default (RW) ticket.
    let carol_client = Client::connect(&daemon_b.socket).await.unwrap();
    let join_resp = carol_client
        .request(Request::JoinSession {
            display_name: "carol".into(),
            ticket,
        })
        .await
        .unwrap();
    match join_resp {
        Response::JoinSession { session, .. } => assert_eq!(session, session_id),
        other => panic!("expected JoinSession, got {other:?}"),
    }

    // Carol sends a message.
    let send_resp = carol_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"hello from carol".to_vec(),
            },
        })
        .await
        .unwrap();
    assert!(
        matches!(send_resp, Response::Sent { .. }),
        "RW joiner should be able to send: {send_resp:?}",
    );

    // Alice sees the message via her event stream.
    let alice_msg =
        common::expect_message_with_payload(&mut alice_events, b"hello from carol", "alice").await;
    assert_eq!(alice_msg.action, "chat.message");

    drop(alice_client);
    drop(carol_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// A ticket issued with a very short expiry is rejected at
// admission time: the host's ensure_member fails on the expired
// cap-claim so PeerJoined is never emitted. The joiner's local
// JoinSession still succeeds (materialises a remote mirror), but
// the host never admits the peer.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn expired_ticket_rejected_at_admission() {
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
    let (session_id, _ticket) = match host_resp {
        Response::HostSession {
            session, ticket, ..
        } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    // Alice subscribes so she receives PeerJoined events.
    alice_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut alice_events = alice_client.take_events().await.expect("alice events");

    // Alice issues a ticket with expiry_ms = 1 (immediately expired).
    let issue_resp = alice_client
        .request(Request::IssueTicket {
            session: session_id,
            granted_cap: Capability::ReadWrite,
            expiry_ms: 1,
        })
        .await
        .unwrap();
    let expired_ticket = match issue_resp {
        Response::IssuedTicket { ticket, .. } => ticket,
        other => panic!("expected IssuedTicket, got {other:?}"),
    };

    // Ensure the ticket is definitely expired.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Bob joins with the expired ticket. His local daemon accepts
    // (remote mirror materialised), but the host never admits him.
    let bob_client = Client::connect(&daemon_b.socket).await.unwrap();
    let join_resp = bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: expired_ticket,
        })
        .await
        .unwrap();
    assert!(
        matches!(join_resp, Response::JoinSession { .. }),
        "local join should succeed: {join_resp:?}",
    );

    // Wait long enough for the gossip round-trip (JoinAnnouncement →
    // host ensure_member → reject) to complete. If the host admitted
    // Bob, Alice would see PeerJoined. We assert she doesn't.
    let peer_joined = timeout(Duration::from_secs(5), async {
        loop {
            let Some(ev) = alice_events.recv().await else {
                return false;
            };
            if matches!(ev, Event::PeerJoined { .. }) {
                return true;
            }
        }
    })
    .await;

    assert!(
        peer_joined.is_err() || !peer_joined.unwrap(),
        "host must NOT admit a peer with an expired ticket",
    );

    drop(alice_events);
    drop(alice_client);
    drop(bob_client);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Direct-stream upgrade delivers the namespace secret ONLY to
// the target peer — a third read-only peer does NOT observe it.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn direct_stream_upgrade_delivers_secret() {
    let dns_pkarr = std::sync::Arc::new(
        iroh::test_utils::DnsPkarrServer::run()
            .await
            .expect("DnsPkarrServer::run"),
    );

    let fut_a = Box::pin(common::spawn_daemon(
        common::fresh_state(),
        common::testing_setup(&dns_pkarr),
    ));
    let fut_b = Box::pin(common::spawn_daemon(
        common::fresh_state(),
        common::testing_setup(&dns_pkarr),
    ));
    let fut_c = Box::pin(common::spawn_daemon(
        common::fresh_state(),
        common::testing_setup(&dns_pkarr),
    ));
    let (daemon_a, daemon_b, daemon_c) = tokio::join!(fut_a, fut_b, fut_c);

    // Wait for all three to publish their pkarr records.
    let (ra, rb, rc) = tokio::join!(
        dns_pkarr.on_endpoint(&daemon_a.iroh_addr.id, common::PKARR_READY_TIMEOUT),
        dns_pkarr.on_endpoint(&daemon_b.iroh_addr.id, common::PKARR_READY_TIMEOUT),
        dns_pkarr.on_endpoint(&daemon_c.iroh_addr.id, common::PKARR_READY_TIMEOUT),
    );
    ra.expect("daemon_a pkarr");
    rb.expect("daemon_b pkarr");
    rc.expect("daemon_c pkarr");

    // Alice hosts on daemon A.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let host_resp = alice
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
    alice
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut alice_events = alice.take_events().await.expect("alice events");

    // Bob joins on daemon B with default (RW) ticket.
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    bob.request(Request::JoinSession {
        display_name: "bob".into(),
        ticket: ticket.clone(),
    })
    .await
    .unwrap();
    bob.request(Request::Subscribe {
        session: session_id,
        since: None,
    })
    .await
    .unwrap();
    let mut bob_events = bob.take_events().await.expect("bob events");

    // Carol joins on daemon C with a Read-only ticket.
    let carol_ticket = match alice
        .request(Request::IssueTicket {
            session: session_id,
            granted_cap: Capability::Read,
            expiry_ms: 0,
        })
        .await
        .unwrap()
    {
        Response::IssuedTicket { ticket, .. } => ticket,
        other => panic!("expected IssuedTicket, got {other:?}"),
    };
    let carol = Client::connect(&daemon_c.socket).await.unwrap();
    carol
        .request(Request::JoinSession {
            display_name: "carol".into(),
            ticket: carol_ticket,
        })
        .await
        .unwrap();
    carol
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut carol_events = carol.take_events().await.expect("carol events");

    // Wait for both Bob and Carol to be admitted by the host.
    let bob_peer_id = daemon_b.peer_id();
    let carol_peer_id = daemon_c.peer_id();
    wait_for_peer_joined(&mut alice_events, bob_peer_id, "alice sees bob").await;
    wait_for_peer_joined(&mut alice_events, carol_peer_id, "alice sees carol").await;

    // Alice delivers the upgrade secret to Bob via direct stream.
    let secret = [0x42u8; 32];
    let deliver_resp = alice
        .request(Request::DeliverUpgrade {
            session: session_id,
            target_peer: bob_peer_id,
            namespace_secret: secret,
        })
        .await
        .unwrap();
    assert!(
        matches!(deliver_resp, Response::UpgradeDelivered),
        "expected UpgradeDelivered, got {deliver_resp:?}",
    );

    // Bob should receive the upgrade event.
    let bob_msg = wait_for_upgrade_event(&mut bob_events, "bob").await;
    assert_eq!(bob_msg.action, UPGRADE_ACTION);
    assert!(
        bob_msg.payload.len() >= 32,
        "upgrade payload should contain the secret",
    );

    // Carol must NOT see any upgrade event (she's not the target).
    let carol_upgrade = timeout(Duration::from_secs(3), async {
        loop {
            let Some(ev) = carol_events.recv().await else {
                return false;
            };
            if let Event::Message { message, .. } = ev
                && message.action == UPGRADE_ACTION
            {
                return true;
            }
        }
    })
    .await;
    assert!(
        carol_upgrade.is_err() || !carol_upgrade.unwrap(),
        "Carol (read-only) must NOT receive the upgrade secret",
    );

    drop(alice_events);
    drop(bob_events);
    drop(carol_events);
    drop(alice);
    drop(bob);
    drop(carol);
    daemon_a.stop().await;
    daemon_b.stop().await;
    daemon_c.stop().await;
}

// =============================================================
// Ticket revocation end-to-end: the host revokes one of two
// tickets; a joiner with the live ticket is admitted and synced,
// a joiner with the revoked ticket is never admitted (no
// PeerJoined on the host — same joiner-side UX as expiry). The
// ledger over IPC reflects status and used_by.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn revoked_ticket_rejected_live_ticket_admits() {
    let dns_pkarr = std::sync::Arc::new(
        iroh::test_utils::DnsPkarrServer::run()
            .await
            .expect("DnsPkarrServer::run"),
    );

    let fut_a = Box::pin(common::spawn_daemon(
        common::fresh_state(),
        common::testing_setup(&dns_pkarr),
    ));
    let fut_b = Box::pin(common::spawn_daemon(
        common::fresh_state(),
        common::testing_setup(&dns_pkarr),
    ));
    let fut_c = Box::pin(common::spawn_daemon(
        common::fresh_state(),
        common::testing_setup(&dns_pkarr),
    ));
    let (daemon_a, daemon_b, daemon_c) = tokio::join!(fut_a, fut_b, fut_c);

    let (ra, rb, rc) = tokio::join!(
        dns_pkarr.on_endpoint(&daemon_a.iroh_addr.id, common::PKARR_READY_TIMEOUT),
        dns_pkarr.on_endpoint(&daemon_b.iroh_addr.id, common::PKARR_READY_TIMEOUT),
        dns_pkarr.on_endpoint(&daemon_c.iroh_addr.id, common::PKARR_READY_TIMEOUT),
    );
    ra.expect("daemon_a pkarr");
    rb.expect("daemon_b pkarr");
    rc.expect("daemon_c pkarr");

    // Alice hosts on daemon A.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let session_id = match alice
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
    alice
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut alice_events = alice.take_events().await.expect("alice events");

    // Two extra tickets: one stays live, one gets revoked.
    let issue = |cap| {
        alice.request(Request::IssueTicket {
            session: session_id,
            granted_cap: cap,
            expiry_ms: 0,
        })
    };
    let (live_ticket, live_id) = match issue(Capability::ReadWrite).await.unwrap() {
        Response::IssuedTicket { ticket, ticket_id } => (ticket, ticket_id),
        other => panic!("expected IssuedTicket, got {other:?}"),
    };
    let (dead_ticket, dead_id) = match issue(Capability::ReadWrite).await.unwrap() {
        Response::IssuedTicket { ticket, ticket_id } => (ticket, ticket_id),
        other => panic!("expected IssuedTicket, got {other:?}"),
    };

    // Revoking an id never issued for this session is an explicit
    // error (success would falsely reassure the operator).
    let bogus = artel_protocol::TicketId::from_bytes([0xEE; 16]);
    match alice
        .request(Request::RevokeTicket {
            session: session_id,
            ticket_id: bogus,
        })
        .await
    {
        Err(ClientError::Protocol(ProtocolError::UnknownTicket(t))) => assert_eq!(t, bogus),
        other => panic!("expected UnknownTicket error, got {other:?}"),
    }

    // Revoke the dead ticket for real.
    match alice
        .request(Request::RevokeTicket {
            session: session_id,
            ticket_id: dead_id,
        })
        .await
        .unwrap()
    {
        Response::TicketRevoked => {}
        other => panic!("expected TicketRevoked, got {other:?}"),
    }

    // Bob on daemon B joins with the live ticket: admitted + synced.
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    bob.request(Request::JoinSession {
        display_name: "bob".into(),
        ticket: live_ticket,
    })
    .await
    .unwrap();
    let bob_peer_id = daemon_b.peer_id();
    wait_for_peer_joined(&mut alice_events, bob_peer_id, "alice sees bob").await;

    // Carol on daemon C joins with the revoked ticket. Her local
    // JoinSession succeeds (remote mirror materialised) but the host
    // must never admit her — no PeerJoined, no auto-grant.
    let carol = Client::connect(&daemon_c.socket).await.unwrap();
    let join_resp = carol
        .request(Request::JoinSession {
            display_name: "carol".into(),
            ticket: dead_ticket,
        })
        .await
        .unwrap();
    assert!(
        matches!(join_resp, Response::JoinSession { .. }),
        "local join should succeed: {join_resp:?}",
    );
    let carol_peer_id = daemon_c.peer_id();
    let carol_joined = timeout(Duration::from_secs(5), async {
        loop {
            let Some(ev) = alice_events.recv().await else {
                return false;
            };
            if let Event::PeerJoined { peer, .. } = ev
                && peer.id == carol_peer_id
            {
                return true;
            }
        }
    })
    .await;
    assert!(
        carol_joined.is_err() || !carol_joined.unwrap(),
        "host must NOT admit a peer with a revoked ticket",
    );

    // The ledger over IPC: three entries (HostSession default + two
    // issued), the dead one Revoked and unused, the live one Active
    // with bob in used_by.
    let entries = match alice
        .request(Request::ListTickets {
            session: session_id,
        })
        .await
        .unwrap()
    {
        Response::Tickets { entries } => entries,
        other => panic!("expected Tickets, got {other:?}"),
    };
    assert_eq!(entries.len(), 3, "host + 2 issued: {entries:?}");
    let dead = entries.iter().find(|t| t.ticket_id == dead_id).unwrap();
    assert_eq!(dead.status, TicketStatus::Revoked);
    assert!(
        dead.used_by.is_empty(),
        "nobody was admitted with the revoked ticket: {dead:?}",
    );
    let live = entries.iter().find(|t| t.ticket_id == live_id).unwrap();
    assert_eq!(live.status, TicketStatus::Active);
    assert_eq!(live.used_by, vec![bob_peer_id]);

    drop(alice_events);
    drop(alice);
    drop(bob);
    drop(carol);
    daemon_a.stop().await;
    daemon_b.stop().await;
    daemon_c.stop().await;
}

// =============================================================
// Revocation survives a host daemon restart: the ledger is
// rehydrated from tickets.json, the revoked entry stays Revoked,
// and the resume re-mint appends a fresh Active entry.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn revocation_survives_host_restart() {
    let root = tempfile::TempDir::new().unwrap();
    let paths = common::RestartState::under(root.path());

    // ---- Incarnation 1: host, issue, revoke the initial ticket ----
    let daemon1 = common::spawn_local_daemon_at(&paths).await;
    let client1 = Client::connect(&paths.socket).await.unwrap();

    let (session_id, initial_id) = match client1
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: None,
        })
        .await
        .unwrap()
    {
        Response::HostSession {
            session, ticket_id, ..
        } => (session, ticket_id),
        other => panic!("expected HostSession, got {other:?}"),
    };
    let issued_id = match client1
        .request(Request::IssueTicket {
            session: session_id,
            granted_cap: Capability::Read,
            expiry_ms: 0,
        })
        .await
        .unwrap()
    {
        Response::IssuedTicket { ticket_id, .. } => ticket_id,
        other => panic!("expected IssuedTicket, got {other:?}"),
    };
    match client1
        .request(Request::RevokeTicket {
            session: session_id,
            ticket_id: initial_id,
        })
        .await
        .unwrap()
    {
        Response::TicketRevoked => {}
        other => panic!("expected TicketRevoked, got {other:?}"),
    }

    drop(client1);
    daemon1.stop().await;

    // ---- Incarnation 2: resume at the same paths ----
    let daemon2 = common::spawn_local_daemon_at(&paths).await;
    let client2 = Client::connect(&paths.socket).await.unwrap();

    let resume_id = match client2
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: Some(session_id),
        })
        .await
        .unwrap()
    {
        Response::HostSession {
            session, ticket_id, ..
        } => {
            assert_eq!(session, session_id, "resume must reuse the id");
            ticket_id
        }
        other => panic!("expected HostSession, got {other:?}"),
    };

    let entries = match client2
        .request(Request::ListTickets {
            session: session_id,
        })
        .await
        .unwrap()
    {
        Response::Tickets { entries } => entries,
        other => panic!("expected Tickets, got {other:?}"),
    };
    // initial (Revoked) + issued (Active) + resume re-mint (Active).
    assert_eq!(entries.len(), 3, "{entries:?}");
    let status_of = |id| entries.iter().find(|t| t.ticket_id == id).unwrap().status;
    assert_eq!(
        status_of(initial_id),
        TicketStatus::Revoked,
        "revocation must survive the restart",
    );
    assert_eq!(status_of(issued_id), TicketStatus::Active);
    assert_eq!(status_of(resume_id), TicketStatus::Active);

    drop(client2);
    daemon2.stop().await;
}

use common::wait_for_peer_joined;

async fn wait_for_upgrade_event(
    events: &mut artel_client::EventStream,
    label: &str,
) -> artel_protocol::SessionMessage {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(!remaining.is_zero(), "{label}: upgrade event never arrived");
        let event = match timeout(remaining, events.recv()).await {
            Ok(Some(ev)) => ev,
            Ok(None) => panic!("{label}: events channel closed"),
            Err(_) => continue,
        };
        if let Event::Message { message, .. } = event
            && message.action == UPGRADE_ACTION
        {
            return message;
        }
    }
}
