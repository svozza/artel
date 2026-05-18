//! End-to-end integration test: two clients drive a real daemon
//! through Hello -> Host/Join -> Subscribe -> Send -> Leave.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_protocol::transport::client::connect;
use artel_protocol::{
    Event, MessageKind, PROTOCOL_VERSION, PeerId, PeerInfo, Request, RequestId, Response,
    SendPayload, WireMessage,
};
use futures_util::{SinkExt, StreamExt};
use tempfile::TempDir;
use tokio::time::timeout;

type Client = artel_protocol::transport::Framed<tokio::net::UnixStream>;

/// Send a request, receive its response. Panics on unrelated frames.
async fn rpc(client: &mut Client, id: RequestId, request: Request) -> Response {
    client
        .send(WireMessage::Request { id, request })
        .await
        .unwrap();
    let frame = timeout(Duration::from_secs(2), client.next())
        .await
        .expect("timed out waiting for response")
        .expect("stream ended early")
        .unwrap();
    match frame {
        WireMessage::Response { id: rid, response } if rid == id => response,
        other => panic!("unexpected frame waiting for {id:?}: {other:?}"),
    }
}

async fn next_event(client: &mut Client) -> Event {
    let frame = timeout(Duration::from_secs(2), client.next())
        .await
        .expect("timed out waiting for event")
        .expect("stream ended early")
        .unwrap();
    match frame {
        WireMessage::Event { event } => event,
        other => panic!("expected event, got {other:?}"),
    }
}

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
            daemon_peer_id: PeerId::from_bytes([0xee; 32]),
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

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "linear e2e walkthrough is clearer flat than split into helpers"
)]
async fn two_clients_chat_end_to_end() {
    let harness = DaemonHarness::spawn().await;

    // ---- Alice (host) ----
    let mut alice = connect(&harness.socket).await.unwrap();
    let resp = rpc(
        &mut alice,
        RequestId::new(1),
        Request::Hello {
            client_version: PROTOCOL_VERSION,
        },
    )
    .await;
    assert!(matches!(resp, Response::Hello { .. }), "{resp:?}");

    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let resp = rpc(
        &mut alice,
        RequestId::new(2),
        Request::HostSession {
            peer: alice_peer.clone(),
        },
    )
    .await;
    let (session, ticket) = match resp {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    let resp = rpc(
        &mut alice,
        RequestId::new(3),
        Request::Subscribe {
            session,
            since: None,
        },
    )
    .await;
    assert!(matches!(resp, Response::Subscribed { .. }), "{resp:?}");

    // ---- Bob (joiner) ----
    let mut bob = connect(&harness.socket).await.unwrap();
    let resp = rpc(
        &mut bob,
        RequestId::new(1),
        Request::Hello {
            client_version: PROTOCOL_VERSION,
        },
    )
    .await;
    assert!(matches!(resp, Response::Hello { .. }), "{resp:?}");

    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
    let resp = rpc(
        &mut bob,
        RequestId::new(2),
        Request::JoinSession {
            peer: bob_peer.clone(),
            ticket,
        },
    )
    .await;
    match resp {
        Response::JoinSession { session: got, head } => {
            assert_eq!(got, session);
            assert_eq!(head, None);
        }
        other => panic!("expected JoinSession, got {other:?}"),
    }

    // Alice observes Bob joining.
    match next_event(&mut alice).await {
        Event::PeerJoined { session: got, peer } => {
            assert_eq!(got, session);
            assert_eq!(peer, bob_peer);
        }
        other => panic!("expected PeerJoined, got {other:?}"),
    }

    // ---- Bob sends; Alice receives ----
    let resp = rpc(
        &mut bob,
        RequestId::new(3),
        Request::Send {
            session,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"hi alice".to_vec(),
            },
        },
    )
    .await;
    let bob_seq = match resp {
        Response::Sent { session: got, seq } => {
            assert_eq!(got, session);
            seq
        }
        other => panic!("expected Sent, got {other:?}"),
    };

    match next_event(&mut alice).await {
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

    // ---- Bob leaves; Alice observes PeerLeft ----
    let resp = rpc(
        &mut bob,
        RequestId::new(4),
        Request::LeaveSession { session },
    )
    .await;
    assert!(matches!(resp, Response::Left { .. }), "{resp:?}");

    match next_event(&mut alice).await {
        Event::PeerLeft { session: got, peer } => {
            assert_eq!(got, session);
            assert_eq!(peer, bob_peer.id);
        }
        other => panic!("expected PeerLeft, got {other:?}"),
    }

    // ---- Alice (host) leaves; session closes ----
    let resp = rpc(
        &mut alice,
        RequestId::new(4),
        Request::LeaveSession { session },
    )
    .await;
    assert!(matches!(resp, Response::Left { .. }), "{resp:?}");

    match next_event(&mut alice).await {
        Event::SessionClosed { session: got } => assert_eq!(got, session),
        other => panic!("expected SessionClosed, got {other:?}"),
    }

    drop(alice);
    drop(bob);
    harness.shutdown().await;
}

#[tokio::test]
async fn subscribe_replays_history() {
    let harness = DaemonHarness::spawn().await;

    // Alice hosts, sends three messages, then Bob joins and subscribes
    // since the first sent seq. Bob should see the last two replayed.
    let mut alice = connect(&harness.socket).await.unwrap();
    let _ = rpc(
        &mut alice,
        RequestId::new(1),
        Request::Hello {
            client_version: PROTOCOL_VERSION,
        },
    )
    .await;

    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let resp = rpc(
        &mut alice,
        RequestId::new(2),
        Request::HostSession { peer: alice_peer },
    )
    .await;
    let (session, ticket) = match resp {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    let mut sent_seqs = Vec::new();
    for n in 0..3u32 {
        let resp = rpc(
            &mut alice,
            RequestId::new(u64::from(10 + n)),
            Request::Send {
                session,
                payload: SendPayload {
                    kind: MessageKind::Chat,
                    action: format!("m{n}"),
                    payload: vec![],
                },
            },
        )
        .await;
        match resp {
            Response::Sent { seq, .. } => sent_seqs.push(seq),
            other => panic!("expected Sent, got {other:?}"),
        }
    }

    let mut bob = connect(&harness.socket).await.unwrap();
    let _ = rpc(
        &mut bob,
        RequestId::new(1),
        Request::Hello {
            client_version: PROTOCOL_VERSION,
        },
    )
    .await;
    let _ = rpc(
        &mut bob,
        RequestId::new(2),
        Request::JoinSession {
            peer: PeerInfo::new(PeerId::from_bytes([2; 32]), "bob"),
            ticket,
        },
    )
    .await;
    let _ = rpc(
        &mut bob,
        RequestId::new(3),
        Request::Subscribe {
            session,
            since: Some(sent_seqs[0]),
        },
    )
    .await;

    for expected_action in ["m1", "m2"] {
        match next_event(&mut bob).await {
            Event::Message { message, .. } => assert_eq!(message.action, expected_action),
            other => panic!("expected Message {expected_action:?}, got {other:?}"),
        }
    }

    drop(alice);
    drop(bob);
    harness.shutdown().await;
}
