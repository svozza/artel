//! End-to-end tests for the attachment RPCs.
//!
//! Exercises the daemon's persistence story for opaque per-session
//! attachments: register/list/forget round-trips, `kind`-filtering,
//! cascade on host-leave, and survival across a daemon restart.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use artel_client::{Client, ClientError};
use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_protocol::{Attachment, PeerId, PeerInfo, ProtocolError, Request, Response};
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::timeout;

const DAEMON_PEER: PeerId = PeerId::from_bytes([0xee; 32]);
const KIND_V1: &str = "artel-fs/workspace/v1";

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
        endpoint_setup: artel_daemon::EndpointSetup::Production,
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

async fn host_session(client: &Client) -> artel_protocol::SessionId {
    let alice = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    match client
        .request(Request::HostSession {
            peer: alice,
            session: None,
        })
        .await
        .unwrap()
    {
        Response::HostSession { session, .. } => session,
        other => panic!("expected HostSession, got {other:?}"),
    }
}

async fn list(client: &Client, kind: Option<&str>) -> Vec<Attachment> {
    match client
        .request(Request::ListAttachments {
            kind: kind.map(str::to_owned),
        })
        .await
        .unwrap()
    {
        Response::Attachments { entries } => entries,
        other => panic!("expected Attachments, got {other:?}"),
    }
}

#[tokio::test]
async fn register_then_list_round_trips_via_ipc() {
    let state = fresh_state_dir();
    let daemon = spawn_at(&state).await;
    let client = Client::connect(&state.socket).await.unwrap();
    let session = host_session(&client).await;

    let resp = client
        .request(Request::RegisterAttachment {
            session,
            kind: KIND_V1.into(),
            payload: b"opaque".to_vec(),
        })
        .await
        .unwrap();
    assert_eq!(resp, Response::AttachmentRegistered);

    let entries = list(&client, Some(KIND_V1)).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].session, session);
    assert_eq!(entries[0].kind, KIND_V1);
    assert_eq!(entries[0].payload, b"opaque");

    drop(client);
    daemon.stop().await;
}

#[tokio::test]
async fn list_attachments_filters_by_kind_via_ipc() {
    let state = fresh_state_dir();
    let daemon = spawn_at(&state).await;
    let client = Client::connect(&state.socket).await.unwrap();
    let session = host_session(&client).await;

    for (kind, payload) in [(KIND_V1, "a"), ("other/kind/v1", "b")] {
        client
            .request(Request::RegisterAttachment {
                session,
                kind: kind.into(),
                payload: payload.as_bytes().to_vec(),
            })
            .await
            .unwrap();
    }

    let v1 = list(&client, Some(KIND_V1)).await;
    assert_eq!(v1.len(), 1);
    assert_eq!(v1[0].kind, KIND_V1);
    assert_eq!(v1[0].payload, b"a");

    let all = list(&client, None).await;
    assert_eq!(all.len(), 2);

    drop(client);
    daemon.stop().await;
}

#[tokio::test]
async fn register_attachment_unknown_session_surfaces_unknown_session_error() {
    let state = fresh_state_dir();
    let daemon = spawn_at(&state).await;
    let client = Client::connect(&state.socket).await.unwrap();

    let bogus = artel_protocol::SessionId::new_random();
    let err = client
        .request(Request::RegisterAttachment {
            session: bogus,
            kind: KIND_V1.into(),
            payload: b"x".to_vec(),
        })
        .await
        .unwrap_err();
    match err {
        ClientError::Protocol(ProtocolError::UnknownSession(s)) => assert_eq!(s, bogus),
        other => panic!("expected UnknownSession, got {other:?}"),
    }

    drop(client);
    daemon.stop().await;
}

#[tokio::test]
async fn forget_attachment_is_idempotent_via_ipc() {
    let state = fresh_state_dir();
    let daemon = spawn_at(&state).await;
    let client = Client::connect(&state.socket).await.unwrap();
    let session = host_session(&client).await;

    client
        .request(Request::RegisterAttachment {
            session,
            kind: KIND_V1.into(),
            payload: b"x".to_vec(),
        })
        .await
        .unwrap();

    for _ in 0..2 {
        let resp = client
            .request(Request::ForgetAttachment {
                session,
                kind: KIND_V1.into(),
            })
            .await
            .unwrap();
        assert_eq!(resp, Response::AttachmentForgotten);
    }
    assert!(list(&client, None).await.is_empty());

    drop(client);
    daemon.stop().await;
}

#[tokio::test]
async fn attachments_cascade_when_host_leaves_via_ipc() {
    let state = fresh_state_dir();
    let daemon = spawn_at(&state).await;
    let client = Client::connect(&state.socket).await.unwrap();
    let session = host_session(&client).await;

    client
        .request(Request::RegisterAttachment {
            session,
            kind: KIND_V1.into(),
            payload: b"x".to_vec(),
        })
        .await
        .unwrap();
    assert_eq!(list(&client, None).await.len(), 1);

    client
        .request(Request::LeaveSession { session })
        .await
        .unwrap();

    assert!(list(&client, None).await.is_empty());

    drop(client);
    daemon.stop().await;
}

#[tokio::test]
async fn attachments_persist_across_daemon_restart() {
    let state = fresh_state_dir();

    // ---- First daemon: host + register ----
    let daemon1 = spawn_at(&state).await;
    let client1 = Client::connect(&state.socket).await.unwrap();
    let session = host_session(&client1).await;
    client1
        .request(Request::RegisterAttachment {
            session,
            kind: KIND_V1.into(),
            payload: b"persist-me".to_vec(),
        })
        .await
        .unwrap();
    drop(client1);
    daemon1.stop().await;

    // ---- Second daemon: same state dir ----
    let daemon2 = spawn_at(&state).await;
    let client2 = Client::connect(&state.socket).await.unwrap();

    let entries = list(&client2, Some(KIND_V1)).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].session, session);
    assert_eq!(entries[0].kind, KIND_V1);
    assert_eq!(entries[0].payload, b"persist-me");

    drop(client2);
    daemon2.stop().await;
}
