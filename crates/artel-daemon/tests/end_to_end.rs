//! End-to-end integration test: two clients drive a real daemon
//! through Hello -> Host/Join -> Subscribe -> Send -> Leave.
//!
//! Uses `artel_client::Client` so we exercise the public client API
//! and not raw IPC framing.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_protocol::{Event, MessageKind, PeerId, PeerInfo, Request, Response, SendPayload};
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::timeout;

struct DaemonHarness {
    _tempdir: TempDir,
    socket: PathBuf,
    shutdown: Arc<Shutdown>,
    join: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl DaemonHarness {
    async fn spawn() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("daemon.sock");
        let pid = tempdir.path().join("daemon.pid");
        let daemon = Daemon::start(DaemonConfig {
            socket_path: socket.clone(),
            pid_path: pid,
            sessions_dir: tempdir.path().join("sessions"),
            daemon_peer_id: PeerId::from_bytes([0xee; 32]),
            iroh_key_path: None,
        })
        .await
        .expect("daemon start");
        let shutdown = daemon.shutdown_handle();
        let join = tokio::spawn(daemon.run());
        Self {
            _tempdir: tempdir,
            socket,
            shutdown,
            join,
        }
    }

    async fn shutdown(self) {
        self.shutdown.trigger();
        timeout(Duration::from_secs(5), self.join)
            .await
            .expect("daemon did not exit within 5s")
            .expect("daemon panicked")
            .expect("daemon returned error");
    }
}

async fn next_event(events: &mut artel_client::EventStream) -> Event {
    timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("timed out waiting for event")
        .expect("event channel closed")
}

#[tokio::test]
async fn two_clients_chat_end_to_end() {
    let h = DaemonHarness::spawn().await;

    let alice_client = Client::connect(&h.socket).await.unwrap();
    let bob_client = Client::connect(&h.socket).await.unwrap();

    // Alice hosts.
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let resp = alice_client
        .request(Request::HostSession { peer: alice_peer })
        .await
        .unwrap();
    let (session, ticket) = match resp {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    // Alice subscribes so she observes peer-joined and the message.
    let resp = alice_client
        .request(Request::Subscribe {
            session,
            since: None,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::Subscribed { .. }), "{resp:?}");
    let mut alice_events = alice_client.take_events().await.expect("events");

    // Bob joins.
    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
    let resp = bob_client
        .request(Request::JoinSession {
            peer: bob_peer.clone(),
            ticket,
        })
        .await
        .unwrap();
    match resp {
        Response::JoinSession { session: got, head } => {
            assert_eq!(got, session);
            assert_eq!(head, None);
        }
        other => panic!("expected JoinSession, got {other:?}"),
    }

    // Alice observes Bob joining.
    match next_event(&mut alice_events).await {
        Event::PeerJoined { session: got, peer } => {
            assert_eq!(got, session);
            assert_eq!(peer, bob_peer);
        }
        other => panic!("expected PeerJoined, got {other:?}"),
    }

    // Bob sends; Alice receives.
    let resp = bob_client
        .request(Request::Send {
            session,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"hi alice".to_vec(),
            },
        })
        .await
        .unwrap();
    let bob_seq = match resp {
        Response::Sent { session: got, seq } => {
            assert_eq!(got, session);
            seq
        }
        other => panic!("expected Sent, got {other:?}"),
    };

    match next_event(&mut alice_events).await {
        Event::Message {
            session: got,
            message,
        } => {
            assert_eq!(got, session);
            assert_eq!(message.seq, bob_seq);
            assert_eq!(message.peer, bob_peer);
            assert_eq!(message.payload, b"hi alice");
            assert_eq!(message.action, "chat.message");
        }
        other => panic!("expected Message event, got {other:?}"),
    }

    // Bob leaves; Alice observes PeerLeft.
    let resp = bob_client
        .request(Request::LeaveSession { session })
        .await
        .unwrap();
    assert!(matches!(resp, Response::Left { .. }), "{resp:?}");

    match next_event(&mut alice_events).await {
        Event::PeerLeft { session: got, peer } => {
            assert_eq!(got, session);
            assert_eq!(peer, bob_peer.id);
        }
        other => panic!("expected PeerLeft, got {other:?}"),
    }

    // Alice (host) leaves; session closes.
    let resp = alice_client
        .request(Request::LeaveSession { session })
        .await
        .unwrap();
    assert!(matches!(resp, Response::Left { .. }), "{resp:?}");

    match next_event(&mut alice_events).await {
        Event::SessionClosed { session: got } => assert_eq!(got, session),
        other => panic!("expected SessionClosed, got {other:?}"),
    }

    drop(alice_client);
    drop(bob_client);
    h.shutdown().await;
}

#[tokio::test]
async fn subscribe_replays_history() {
    let h = DaemonHarness::spawn().await;

    let alice_client = Client::connect(&h.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let resp = alice_client
        .request(Request::HostSession { peer: alice_peer })
        .await
        .unwrap();
    let (session, ticket) = match resp {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    let mut sent_seqs = Vec::new();
    for n in 0..3u32 {
        let resp = alice_client
            .request(Request::Send {
                session,
                payload: SendPayload {
                    kind: MessageKind::Chat,
                    action: format!("m{n}"),
                    payload: vec![],
                },
            })
            .await
            .unwrap();
        match resp {
            Response::Sent { seq, .. } => sent_seqs.push(seq),
            other => panic!("expected Sent, got {other:?}"),
        }
    }

    // Bob joins and subscribes since the first seq. He should see m1
    // and m2 replayed.
    let bob_client = Client::connect(&h.socket).await.unwrap();
    let _ = bob_client
        .request(Request::JoinSession {
            peer: PeerInfo::new(PeerId::from_bytes([2; 32]), "bob"),
            ticket,
        })
        .await
        .unwrap();
    let _ = bob_client
        .request(Request::Subscribe {
            session,
            since: Some(sent_seqs[0]),
        })
        .await
        .unwrap();
    let mut bob_events = bob_client.take_events().await.expect("events");

    for expected_action in ["m1", "m2"] {
        match next_event(&mut bob_events).await {
            Event::Message { message, .. } => assert_eq!(message.action, expected_action),
            other => panic!("expected Message {expected_action:?}, got {other:?}"),
        }
    }

    drop(alice_client);
    drop(bob_client);
    h.shutdown().await;
}
