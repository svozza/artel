//! End-to-end persistence test: a daemon's sessions outlive the
//! daemon process. Hosts a session, sends messages, kills the daemon,
//! starts a fresh daemon at the same state directory, and asserts the
//! session and its log are recovered.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_protocol::ticket::{self, SessionTicket, WireEndpointAddr};
use artel_protocol::{Event, MessageKind, PeerId, PeerInfo, Request, Response, SendPayload, Seq};
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

#[tokio::test]
async fn session_and_log_survive_daemon_restart() {
    let state = fresh_state_dir();

    // ---- First daemon: host + send three messages ----
    let daemon1 = spawn_at(&state).await;
    let alice_client = Client::connect(&state.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");

    let session_id = match alice_client
        .request(Request::HostSession {
            peer: alice_peer.clone(),
        })
        .await
        .unwrap()
    {
        Response::HostSession { session, .. } => session,
        other => panic!("expected HostSession, got {other:?}"),
    };

    let mut sent_seqs = Vec::new();
    for n in 0..3u32 {
        let resp = alice_client
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
        match resp {
            Response::Sent { seq, .. } => sent_seqs.push(seq),
            other => panic!("expected Sent, got {other:?}"),
        }
    }

    drop(alice_client);
    daemon1.stop().await;

    // ---- Second daemon: state directory unchanged ----
    let daemon2 = spawn_at(&state).await;
    let recovered = Client::connect(&state.socket).await.unwrap();

    // ListSessions should see the original session.
    match recovered.request(Request::ListSessions).await.unwrap() {
        Response::ListSessions { sessions } => {
            assert_eq!(sessions.len(), 1);
            assert_eq!(sessions[0].id, session_id);
            assert_eq!(sessions[0].last_seq, sent_seqs.last().copied());
            assert_eq!(sessions[0].peer_count, 1);
        }
        other => panic!("expected ListSessions, got {other:?}"),
    }

    // Re-join Alice (her per-connection state was wiped) and subscribe
    // from before the first message; she should see all three replayed.
    let _ = recovered
        .request(Request::JoinSession {
            peer: alice_peer.clone(),
            ticket: ticket::encode(&SessionTicket {
                session_id,
                host_peer_id: DAEMON_PEER,
                host_addr: WireEndpointAddr::id_only(DAEMON_PEER),
            })
            .into(),
        })
        .await;
    // She might already be a member of the session per persisted state
    // — that's expected; AlreadyJoined is fine here. The next call is
    // what we actually test.

    let _ = recovered
        .request(Request::Subscribe {
            session: session_id,
            since: Some(Seq::ZERO),
        })
        .await
        .unwrap();
    let mut events = recovered.take_events().await.expect("events");

    for n in 0..3u32 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timeout")
            .expect("event channel closed");
        match event {
            Event::Message { message, .. } => {
                assert_eq!(message.action, format!("m{n}"));
                assert_eq!(message.payload, format!("payload-{n}").into_bytes());
            }
            other => panic!("expected Message m{n}, got {other:?}"),
        }
    }

    drop(recovered);
    daemon2.stop().await;
}

#[tokio::test]
async fn host_leave_removes_session_dir() {
    let state = fresh_state_dir();
    let daemon = spawn_at(&state).await;
    let client = Client::connect(&state.socket).await.unwrap();
    let alice = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");

    let session_id = match client
        .request(Request::HostSession { peer: alice })
        .await
        .unwrap()
    {
        Response::HostSession { session, .. } => session,
        other => panic!("expected HostSession, got {other:?}"),
    };
    let session_dir = state.sessions.join(session_id.to_string());
    assert!(session_dir.exists(), "session dir should exist after host");

    let _ = client
        .request(Request::LeaveSession {
            session: session_id,
        })
        .await
        .unwrap();

    assert!(
        !session_dir.exists(),
        "session dir should be deleted when host leaves"
    );

    drop(client);
    daemon.stop().await;
}
