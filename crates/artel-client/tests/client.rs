//! Integration tests for [`artel_client::Client`].
//!
//! These spin up an in-process [`artel_daemon::Daemon`] on a tempdir
//! socket and drive it through the public Client API. That keeps the
//! tests honest about what the API actually exposes (vs what the
//! reader/writer tasks happen to do internally).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use artel_client::{Client, ClientError};
use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig, EndpointSetup};
use artel_protocol::{
    Event, MessageKind, PROTOCOL_VERSION, PeerId, ProtocolError, Request, Response, SendPayload,
};
use iroh::test_utils::DnsPkarrServer;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::sync::OnceCell;
use tokio::time::timeout;

/// Per-process shared `DnsPkarrServer`. Auth-L1 (`PROTOCOL_VERSION` 4)
/// removed the synthetic-peer-id path, so every daemon binds an
/// iroh `Endpoint`. Reuse one localhost server across tests in this
/// bin so we don't pay the ~200ms server startup cost per test.
static SHARED_DNS_PKARR: OnceCell<Arc<DnsPkarrServer>> = OnceCell::const_new();

async fn shared_dns_pkarr() -> Arc<DnsPkarrServer> {
    SHARED_DNS_PKARR
        .get_or_init(|| async {
            Arc::new(
                DnsPkarrServer::run_with_origin(artel_daemon::TEST_DNS_ORIGIN.to_string())
                    .await
                    .expect("DnsPkarrServer::run for shared local-daemon fixture"),
            )
        })
        .await
        .clone()
}

struct DaemonHarness {
    _tempdir: TempDir,
    socket: PathBuf,
    daemon_peer_id: PeerId,
    shutdown: Arc<Shutdown>,
    join: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl DaemonHarness {
    async fn spawn() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("daemon.sock");
        let pid = tempdir.path().join("daemon.pid");
        let dns_pkarr = shared_dns_pkarr().await;
        let daemon = Daemon::start(DaemonConfig {
            socket_path: socket.clone(),
            pid_path: pid,
            sessions_dir: tempdir.path().join("sessions"),
            iroh_key_path: Some(tempdir.path().join("iroh.key")),
            endpoint_setup: EndpointSetup::Testing { dns_pkarr },
        })
        .await
        .expect("daemon start");
        let daemon_peer_id = PeerId::from_bytes(*daemon.iroh().endpoint.id().as_bytes());
        let shutdown = daemon.shutdown_handle();
        let join = tokio::spawn(daemon.run());
        Self {
            _tempdir: tempdir,
            socket,
            daemon_peer_id,
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
async fn connect_does_handshake_and_records_daemon_info() {
    let h = DaemonHarness::spawn().await;
    let client = Client::connect(&h.socket).await.unwrap();
    assert_eq!(client.daemon_peer_id(), h.daemon_peer_id);
    assert_eq!(client.daemon_version(), PROTOCOL_VERSION);
    drop(client);
    h.shutdown().await;
}

#[tokio::test]
async fn connect_records_socket_path_for_reconnect() {
    // The cap-listener's lag-recovery loop reconnects by opening a
    // fresh connection to the same socket; `Client` must therefore
    // remember the path it was connected on so callers don't have to
    // thread it separately.
    let h = DaemonHarness::spawn().await;
    let client = Client::connect(&h.socket).await.unwrap();
    assert_eq!(client.socket_path(), h.socket.as_path());
    drop(client);
    h.shutdown().await;
}

#[tokio::test]
async fn connect_against_missing_socket_errors() {
    let dir = tempfile::tempdir().unwrap();
    let bogus = dir.path().join("absent.sock");
    let err = Client::connect(&bogus).await.unwrap_err();
    assert!(
        matches!(err, ClientError::Transport(_)),
        "expected transport error, got {err:?}"
    );
}

#[tokio::test]
async fn list_sessions_returns_empty_on_fresh_daemon() {
    let h = DaemonHarness::spawn().await;
    let client = Client::connect(&h.socket).await.unwrap();

    let resp = client.request(Request::ListSessions).await.unwrap();
    match resp {
        Response::ListSessions { sessions } => assert!(sessions.is_empty()),
        other => panic!("expected ListSessions, got {other:?}"),
    }

    drop(client);
    h.shutdown().await;
}

#[tokio::test]
async fn host_then_list_sees_the_session() {
    let h = DaemonHarness::spawn().await;
    let client = Client::connect(&h.socket).await.unwrap();

    let resp = client
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: None,
        })
        .await
        .unwrap();
    let session_id = match resp {
        Response::HostSession { session, .. } => session,
        other => panic!("expected HostSession, got {other:?}"),
    };

    let resp = client.request(Request::ListSessions).await.unwrap();
    match resp {
        Response::ListSessions { sessions } => {
            assert_eq!(sessions.len(), 1);
            assert_eq!(sessions[0].id, session_id);
        }
        other => panic!("expected ListSessions, got {other:?}"),
    }

    drop(client);
    h.shutdown().await;
}

#[tokio::test]
async fn concurrent_requests_correlate_independently() {
    // Fire many requests in parallel through the same Client and make
    // sure each future sees its own response, not someone else's.
    let h = DaemonHarness::spawn().await;
    let client = Arc::new(Client::connect(&h.socket).await.unwrap());

    // Prime: host a session so the daemon has something to look at.
    let resp = client
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: None,
        })
        .await
        .unwrap();
    let session_id = match resp {
        Response::HostSession { session, .. } => session,
        other => panic!("got {other:?}"),
    };

    let mut joins = Vec::new();
    for _ in 0..32 {
        let c = Arc::clone(&client);
        joins.push(tokio::spawn(async move {
            c.request(Request::ListSessions).await
        }));
    }
    for j in joins {
        let resp = j.await.unwrap().unwrap();
        match resp {
            Response::ListSessions { sessions } => {
                assert_eq!(sessions.len(), 1);
                assert_eq!(sessions[0].id, session_id);
            }
            other => panic!("expected ListSessions, got {other:?}"),
        }
    }

    drop(client);
    h.shutdown().await;
}

#[tokio::test]
async fn protocol_error_surfaces_as_client_error_protocol() {
    // Trying to JoinSession with a malformed ticket triggers
    // ProtocolError::InvalidTicket from the daemon.
    let h = DaemonHarness::spawn().await;
    let client = Client::connect(&h.socket).await.unwrap();

    let err = client
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: "iroh-fake:abc".into(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, ClientError::Protocol(ProtocolError::InvalidTicket)),
        "got {err:?}"
    );

    drop(client);
    h.shutdown().await;
}

#[tokio::test]
async fn events_stream_delivers_message_events() {
    // Auth L1 fix #3: each "client" is a separate daemon. Two
    // DaemonHarness instances share the localhost DnsPkarr server so
    // they discover each other without per-test n0 startup cost.
    let h_a = DaemonHarness::spawn().await;
    let h_b = DaemonHarness::spawn().await;
    let alice_client = Client::connect(&h_a.socket).await.unwrap();
    let bob_client = Client::connect(&h_b.socket).await.unwrap();

    // Alice hosts and subscribes.
    let resp = alice_client
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: None,
        })
        .await
        .unwrap();
    let (session_id, ticket) = match resp {
        Response::HostSession {
            session, ticket, ..
        } => (session, ticket),
        other => panic!("got {other:?}"),
    };
    let _ = alice_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut alice_events = alice_client.take_events().await.expect("events");

    // Bob joins and sends.
    let _ = bob_client
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    let _ = bob_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "chat.message".into(),
                payload: b"hi".to_vec(),
            },
        })
        .await
        .unwrap();

    // Alice should observe PeerJoined and Message. Across daemons,
    // gossip-mesh setup may interleave events, so drain to each
    // expected one within a generous ceiling.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut saw_join = false;
    let mut saw_message = false;
    while !(saw_join && saw_message) {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "alice never observed PeerJoined+Message (saw_join={saw_join}, saw_message={saw_message})",
        );
        match timeout(remaining, alice_events.recv()).await {
            Ok(Some(Event::PeerJoined { .. })) => saw_join = true,
            // Ignore the auto-grant Capability message the host emits on
            // admitting Bob (Auth Slice C / L2); this test pins Bob's
            // Chat send reaching Alice.
            Ok(Some(Event::Message { message, .. })) if message.kind == MessageKind::Chat => {
                assert_eq!(message.payload, b"hi");
                saw_message = true;
            }
            Ok(Some(_)) | Err(_) => {}
            Ok(None) => panic!("alice events channel closed"),
        }
    }

    drop(alice_client);
    drop(bob_client);
    h_a.shutdown().await;
    h_b.shutdown().await;
}

#[tokio::test]
async fn take_events_is_single_consumer() {
    let h = DaemonHarness::spawn().await;
    let client = Client::connect(&h.socket).await.unwrap();

    let first = client.take_events().await;
    let second = client.take_events().await;
    assert!(first.is_some());
    assert!(second.is_none(), "second take_events should return None");

    drop(client);
    h.shutdown().await;
}

#[tokio::test]
async fn drop_resolves_pending_requests_to_connection_closed() {
    // Shut the daemon while a request is in-flight. The pending
    // future should resolve to ConnectionClosed.
    let h = DaemonHarness::spawn().await;
    let client = Client::connect(&h.socket).await.unwrap();

    // Trigger daemon shutdown immediately. The connection drops out
    // from under us; subsequent requests should fail.
    h.shutdown.trigger();
    timeout(Duration::from_secs(5), h.join)
        .await
        .expect("daemon shutdown")
        .expect("daemon panic")
        .expect("daemon io");

    // Poll until the client's reader task observes EOF and turns
    // every subsequent request into ConnectionClosed. No fixed
    // settling delay — bounded retry instead.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match client.request(Request::ListSessions).await {
            Err(ClientError::ConnectionClosed) => break,
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "ListSessions kept succeeding after daemon shutdown — \
                     reader task never observed EOF",
                );
                tokio::task::yield_now().await;
            }
        }
    }
}

#[tokio::test]
async fn dropping_client_does_not_panic() {
    // Just make sure Drop is well-behaved.
    let h = DaemonHarness::spawn().await;
    let client = Client::connect(&h.socket).await.unwrap();
    drop(client);
    h.shutdown().await;
}
