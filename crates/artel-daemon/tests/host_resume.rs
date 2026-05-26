//! End-to-end resume test: a host that supplies an existing
//! [`SessionId`] reattaches to its previously-persisted session
//! verbatim. A different host trying to claim the same id is
//! rejected with [`ProtocolError::SessionConflict`].
//!
//! Mirrors the layout of `tests/persistence.rs`: two sequential
//! daemon spawns at the same `state_dir`, real client over a Unix
//! socket. iroh-disabled (the resume contract is about the
//! `Registry` + `SessionStore` round trip; the gossip bridge has
//! its own coverage).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_protocol::{
    MessageKind, PeerId, PeerInfo, ProtocolError, Request, Response, SendPayload, Seq,
};
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::timeout;

const DAEMON_PEER: PeerId = PeerId::from_bytes([0xee; 32]);

struct StateDir {
    _root: TempDir,
    socket: PathBuf,
    pid: PathBuf,
    sessions: PathBuf,
}

fn fresh_state_dir() -> StateDir {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("daemon.sock");
    let pid = root.path().join("daemon.pid");
    let sessions = root.path().join("sessions");
    StateDir {
        _root: root,
        socket,
        pid,
        sessions,
    }
}

struct RunningDaemon {
    shutdown: Arc<Shutdown>,
    join: tokio::task::JoinHandle<std::io::Result<()>>,
}

async fn spawn_at(state: &StateDir) -> RunningDaemon {
    let daemon = Daemon::start(DaemonConfig {
        socket_path: state.socket.clone(),
        pid_path: state.pid.clone(),
        sessions_dir: state.sessions.clone(),
        daemon_peer_id: DAEMON_PEER,
        iroh_key_path: None,
        address_lookup: None,
    })
    .await
    .expect("daemon start");
    let shutdown = daemon.shutdown_handle();
    let join = tokio::spawn(daemon.run());
    RunningDaemon { shutdown, join }
}

impl RunningDaemon {
    async fn stop(self) {
        self.shutdown.trigger();
        timeout(Duration::from_secs(5), self.join)
            .await
            .expect("daemon did not exit within 5s")
            .expect("daemon panicked")
            .expect("daemon io");
    }
}

/// Re-hosting with a previously-minted `SessionId` recovers the
/// existing session log and members. The second `HostSession`
/// returns the same id (no random remint) and Subscribe replays
/// the pre-restart messages.
#[tokio::test]
async fn host_with_some_id_resumes_persisted_session() {
    let state = fresh_state_dir();
    let alice = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");

    // ---- Daemon 1: host a fresh session, send three messages ----
    let daemon1 = spawn_at(&state).await;
    let client1 = Client::connect(&state.socket).await.unwrap();

    let session_id = match client1
        .request(Request::HostSession {
            peer: alice.clone(),
            session: None,
        })
        .await
        .unwrap()
    {
        Response::HostSession { session, .. } => session,
        other => panic!("expected HostSession, got {other:?}"),
    };

    for n in 0..3u32 {
        let resp = client1
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
        assert!(matches!(resp, Response::Sent { .. }));
    }

    drop(client1);
    daemon1.stop().await;

    // ---- Daemon 2: resume by supplying the same SessionId ----
    let daemon2 = spawn_at(&state).await;
    let client2 = Client::connect(&state.socket).await.unwrap();

    let resumed = match client2
        .request(Request::HostSession {
            peer: alice.clone(),
            session: Some(session_id),
        })
        .await
        .unwrap()
    {
        Response::HostSession { session, .. } => session,
        Response::Error { error } => panic!("resume should succeed, got {error:?}"),
        other => panic!("expected HostSession, got {other:?}"),
    };
    assert_eq!(resumed, session_id, "resumed id must match the original");

    // Subscribe and confirm the full log replays — proves the
    // resume reused the persisted record verbatim rather than
    // creating a fresh one at the same id.
    let _ = client2
        .request(Request::Subscribe {
            session: session_id,
            since: Some(Seq::ZERO),
        })
        .await
        .unwrap();
    let mut events = client2.take_events().await.expect("events");

    for n in 0..3u32 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timeout")
            .expect("event channel closed");
        match event {
            artel_protocol::Event::Message { message, .. } => {
                assert_eq!(message.action, format!("m{n}"));
                assert_eq!(message.payload, format!("payload-{n}").into_bytes());
            }
            other => panic!("expected Message m{n}, got {other:?}"),
        }
    }

    drop(client2);
    daemon2.stop().await;
}

/// `HostSession { session: Some(id) }` issued by a peer who isn't
/// the recorded host returns `SessionConflict`. The existing session
/// is unchanged.
#[tokio::test]
async fn host_with_some_id_rejects_when_host_differs() {
    let state = fresh_state_dir();
    let alice = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let bob = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");

    let daemon = spawn_at(&state).await;
    let client = Client::connect(&state.socket).await.unwrap();

    // Alice hosts.
    let session_id = match client
        .request(Request::HostSession {
            peer: alice,
            session: None,
        })
        .await
        .unwrap()
    {
        Response::HostSession { session, .. } => session,
        other => panic!("expected HostSession, got {other:?}"),
    };

    // Bob tries to resume Alice's session with the same id.
    let err = client
        .request(Request::HostSession {
            peer: bob,
            session: Some(session_id),
        })
        .await
        .expect_err("conflict should map to client error");
    match err {
        artel_client::ClientError::Protocol(ProtocolError::SessionConflict(id)) => {
            assert_eq!(id, session_id);
        }
        other => panic!("expected SessionConflict, got {other:?}"),
    }

    // Listing still shows Alice's session, untouched.
    match client.request(Request::ListSessions).await.unwrap() {
        Response::ListSessions { sessions } => {
            assert_eq!(sessions.len(), 1);
            assert_eq!(sessions[0].id, session_id);
            assert_eq!(sessions[0].peer_count, 1);
        }
        other => panic!("expected ListSessions, got {other:?}"),
    }

    drop(client);
    daemon.stop().await;
}

/// Same as `host_with_some_id_creates_session_at_that_id` in the
/// session.rs unit tests, but exercised end-to-end through the IPC
/// layer to confirm the protocol field plumbs through.
#[tokio::test]
async fn host_with_some_id_creates_at_that_id_first_time() {
    let state = fresh_state_dir();
    let alice = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let chosen = artel_protocol::SessionId::from_bytes([0xbe; 16]);

    let daemon = spawn_at(&state).await;
    let client = Client::connect(&state.socket).await.unwrap();

    let returned = match client
        .request(Request::HostSession {
            peer: alice,
            session: Some(chosen),
        })
        .await
        .unwrap()
    {
        Response::HostSession { session, .. } => session,
        other => panic!("expected HostSession, got {other:?}"),
    };
    assert_eq!(returned, chosen);

    drop(client);
    daemon.stop().await;
}
