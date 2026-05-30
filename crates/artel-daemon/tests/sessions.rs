//! Session-lifecycle integration tests: auto-spawn (PID file
//! recovery, parallel cold-start, missing-binary error mapping),
//! end-to-end Hello → Host/Join → Subscribe → Send → Leave, host
//! resume by `SessionId` (recover persisted log, conflict on a
//! different host, create-at-given-id first time), and the
//! cross-restart persistence guarantee.
//!
//! Consolidated from four per-file bins (`auto_spawn`, `end_to_end`,
//! `host_resume`, `persistence`) per
//! `docs/plans/2026-05-29-faster-cargo-test-plan.md` slice 3a. Each
//! original file's docstring is retained verbatim in section banners.
//!
//! All Tier A: no iroh `Endpoint` is bound. The shared daemon
//! `tests/common/mod.rs` is iroh-gated (`#[cfg(feature = "iroh")]`)
//! so the helpers used here are deliberately bin-local.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use artel_client::{Client, ClientError, EventStream, SpawnError, SpawnOptions};
use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_protocol::ticket::{self, SessionTicket, WireEndpointAddr};
use artel_protocol::{
    Event, JoinTicket, MessageKind, PeerId, PeerInfo, ProtocolError, Request, Response,
    SendPayload, Seq, SessionId,
};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

const DAEMON_PEER: PeerId = PeerId::from_bytes([0xee; 32]);

// =============================================================
// Local daemon helpers
//
// Two flavours used by this bin:
//
// - `LocalDaemon` (Daemon::start at fresh paths) — used by the
//   end-to-end / host-resume / persistence tests. Mirrors the shape
//   each original file's `DaemonHarness` had.
// - The auto-spawn tests build their own throwaway daemons via
//   `Client::connect_or_spawn` and use `sigterm_pidfile` to take
//   them down — no Daemon::start needed there.
// =============================================================

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

struct LocalDaemon {
    shutdown: Arc<Shutdown>,
    join: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl LocalDaemon {
    /// Spawn an iroh-disabled daemon at `state`'s paths.
    async fn spawn_at(state: &StateDir) -> Self {
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
        Self { shutdown, join }
    }

    async fn stop(self) {
        self.shutdown.trigger();
        timeout(Duration::from_secs(5), self.join)
            .await
            .expect("daemon did not exit within 5s")
            .expect("daemon panicked")
            .expect("daemon io");
    }
}

async fn next_event(events: &mut EventStream) -> Event {
    timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("timed out waiting for event")
        .expect("event channel closed")
}

/// Host a session as `peer` and subscribe to its full log,
/// returning the session id, the join ticket, and a live event
/// stream. Used as the test preamble for any scenario where the
/// host wants to observe joiners and messages.
async fn host_and_watch(client: &Client, peer: PeerInfo) -> (SessionId, JoinTicket, EventStream) {
    let resp = client
        .request(Request::HostSession {
            peer,
            session: None,
        })
        .await
        .unwrap();
    let (session, ticket) = match resp {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };
    let resp = client
        .request(Request::Subscribe {
            session,
            since: None,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::Subscribed { .. }), "{resp:?}");
    let events = client.take_events().await.expect("events");
    (session, ticket, events)
}

// =============================================================
// Auto-spawn — `artel_client::Client::connect_or_spawn` integration.
//
// Lives in the `artel-daemon` crate because Cargo only exposes the
// daemon binary path via `CARGO_BIN_EXE_artel-daemon` to integration
// tests within that crate.
//
// Each test spawns its own short-lived daemon under a tempdir, waits
// for it to come up via `connect_or_spawn`, exercises an assertion,
// and SIGTERMs the daemon by reading the PID file. Tempdir is
// preserved as a `TempDir` so its `Drop` cleans up — but only after
// the daemon has exited and released the files.
// =============================================================

/// Path to the `artel-daemon` binary built by Cargo for these tests.
fn daemon_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_artel-daemon"))
}

struct AutoSpawned {
    _tempdir: TempDir,
    pid_path: PathBuf,
}

impl AutoSpawned {
    /// SIGTERM the spawned daemon (looked up via the PID file) and
    /// wait for it to exit.
    async fn shutdown(self) {
        let Self {
            _tempdir, pid_path, ..
        } = self;
        let _ = sigterm_pidfile(&pid_path).await;
    }
}

fn fresh_paths() -> (TempDir, PathBuf, PathBuf) {
    let tempdir = TempDir::new().unwrap();
    let socket = tempdir.path().join("daemon.sock");
    let pid = tempdir.path().join("daemon.pid");
    (tempdir, socket, pid)
}

async fn sigterm_pidfile(pid_path: &Path) -> std::io::Result<()> {
    let raw = std::fs::read_to_string(pid_path)?;
    let pid: i32 = raw.trim().parse().map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, format!("bad pid: {e}"))
    })?;
    let _ = kill(Pid::from_raw(pid), Signal::SIGTERM);
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if !pid_path.exists() {
            return Ok(());
        }
        sleep(Duration::from_millis(25)).await;
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "daemon did not exit within 5s",
    ))
}

#[tokio::test]
async fn happy_path_cold_dir_spawns_daemon() {
    let (tempdir, socket, pid_path) = fresh_paths();
    let opts = SpawnOptions::new(&socket, &pid_path, daemon_binary());
    let client = Client::connect_or_spawn(opts).await.unwrap();
    // Daemon answered Hello.
    assert!(client.daemon_version().get() > 0);
    // PID file now points at a real process.
    assert!(pid_path.exists(), "PID file should exist after spawn");
    drop(client);
    AutoSpawned {
        _tempdir: tempdir,
        pid_path,
    }
    .shutdown()
    .await;
}

#[tokio::test]
async fn second_call_reuses_existing_daemon() {
    let (tempdir, socket, pid_path) = fresh_paths();
    let first = Client::connect_or_spawn(SpawnOptions::new(&socket, &pid_path, daemon_binary()))
        .await
        .unwrap();
    let pid_after_first = std::fs::read_to_string(&pid_path).unwrap();
    let second = Client::connect_or_spawn(SpawnOptions::new(&socket, &pid_path, daemon_binary()))
        .await
        .unwrap();
    let pid_after_second = std::fs::read_to_string(&pid_path).unwrap();
    assert_eq!(
        pid_after_first.trim(),
        pid_after_second.trim(),
        "second connect_or_spawn must not have spawned a new daemon",
    );
    drop(first);
    drop(second);
    AutoSpawned {
        _tempdir: tempdir,
        pid_path,
    }
    .shutdown()
    .await;
}

#[tokio::test]
async fn stale_pid_file_is_recovered() {
    // Simulate a previous daemon that crashed without releasing its
    // PID file. The PID points at a reaped process, so it's stale and
    // a fresh daemon should be spawned.
    let (tempdir, socket, pid_path) = fresh_paths();
    let mut throwaway = Command::new("true").spawn().unwrap();
    let dead_pid = throwaway.id();
    throwaway.wait().unwrap();
    std::fs::write(&pid_path, format!("{dead_pid}\n")).unwrap();

    let client = Client::connect_or_spawn(SpawnOptions::new(&socket, &pid_path, daemon_binary()))
        .await
        .unwrap();
    let new_pid = std::fs::read_to_string(&pid_path).unwrap();
    assert_ne!(
        new_pid.trim(),
        dead_pid.to_string(),
        "PID file should now name the new daemon",
    );
    drop(client);
    AutoSpawned {
        _tempdir: tempdir,
        pid_path,
    }
    .shutdown()
    .await;
}

#[tokio::test]
async fn stale_socket_file_is_recovered() {
    // The daemon side handles this: after winning the PID lock, it
    // unlinks any leftover socket file before binding. Verify the
    // client path doesn't choke on the leftover.
    let (tempdir, socket, pid_path) = fresh_paths();
    std::fs::create_dir_all(socket.parent().unwrap()).unwrap();
    std::fs::write(&socket, b"junk").unwrap();
    let client = Client::connect_or_spawn(SpawnOptions::new(&socket, &pid_path, daemon_binary()))
        .await
        .unwrap();
    drop(client);
    AutoSpawned {
        _tempdir: tempdir,
        pid_path,
    }
    .shutdown()
    .await;
}

#[tokio::test]
async fn parallel_calls_settle_on_one_daemon() {
    // Two parallel cold starts: both spawn a daemon, but PID-file
    // contention means only one survives. Both clients connect to the
    // survivor and see the same peer id.
    let (tempdir, socket, pid_path) = fresh_paths();
    let opts_a = SpawnOptions::new(&socket, &pid_path, daemon_binary());
    let opts_b = SpawnOptions::new(&socket, &pid_path, daemon_binary());

    let (a, b) = tokio::join!(
        Client::connect_or_spawn(opts_a),
        Client::connect_or_spawn(opts_b),
    );
    let a = a.expect("client A");
    let b = b.expect("client B");
    assert_eq!(
        a.daemon_peer_id(),
        b.daemon_peer_id(),
        "both clients should be talking to the same daemon",
    );

    // Smoke: both can issue requests through the survivor.
    let resp = a.request(Request::ListSessions).await.unwrap();
    assert!(matches!(resp, Response::ListSessions { .. }));
    let resp = b.request(Request::ListSessions).await.unwrap();
    assert!(matches!(resp, Response::ListSessions { .. }));

    drop(a);
    drop(b);
    AutoSpawned {
        _tempdir: tempdir,
        pid_path,
    }
    .shutdown()
    .await;
}

#[tokio::test]
async fn missing_daemon_binary_yields_launch_error() {
    let (tempdir, socket, pid_path) = fresh_paths();
    let bogus = tempdir.path().join("does-not-exist");
    let err = Client::connect_or_spawn(SpawnOptions::new(&socket, &pid_path, &bogus))
        .await
        .unwrap_err();
    match err {
        ClientError::Spawn(SpawnError::Launch { path, .. }) => {
            assert_eq!(path, bogus);
        }
        other => panic!("expected Spawn::Launch, got {other:?}"),
    }
    // Nothing should have been written.
    assert!(!socket.exists());
    assert!(!pid_path.exists());
}

#[tokio::test]
async fn live_pid_no_socket_waits_for_socket_then_times_out() {
    // Synthesise the "daemon is mid-boot" state: PID file names a
    // long-running process (this test process), but the socket never
    // materialises. connect_or_spawn should NOT spawn a new daemon
    // (because the PID is alive), and should fail with Timeout.
    let (_tempdir, socket, pid_path) = fresh_paths();
    std::fs::write(&pid_path, format!("{}\n", std::process::id())).unwrap();

    let opts = SpawnOptions::new(&socket, &pid_path, daemon_binary())
        .with_spawn_timeout(Duration::from_millis(200));
    let err = Client::connect_or_spawn(opts).await.unwrap_err();
    match err {
        ClientError::Spawn(SpawnError::Timeout { socket: s, timeout }) => {
            assert_eq!(s, socket);
            assert_eq!(timeout, Duration::from_millis(200));
        }
        other => panic!("expected Spawn::Timeout, got {other:?}"),
    }
}

// =============================================================
// End-to-end: two clients drive a real daemon through Hello ->
// Host/Join -> Subscribe -> Send -> Leave.
//
// Uses `artel_client::Client` so we exercise the public client API
// and not raw IPC framing.
// =============================================================

#[tokio::test]
async fn two_clients_chat_end_to_end() {
    let state = fresh_state_dir();
    let daemon = LocalDaemon::spawn_at(&state).await;

    let alice_client = Client::connect(&state.socket).await.unwrap();
    let bob_client = Client::connect(&state.socket).await.unwrap();

    // Alice hosts and subscribes so she observes peer-joined and
    // the message.
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let (session, ticket, mut alice_events) = host_and_watch(&alice_client, alice_peer).await;

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
    daemon.stop().await;
}

#[tokio::test]
async fn subscribe_replays_history() {
    let state = fresh_state_dir();
    let daemon = LocalDaemon::spawn_at(&state).await;

    let alice_client = Client::connect(&state.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let resp = alice_client
        .request(Request::HostSession {
            peer: alice_peer,
            session: None,
        })
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
    let bob_client = Client::connect(&state.socket).await.unwrap();
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
    daemon.stop().await;
}

// =============================================================
// Host resume: a host that supplies an existing `SessionId`
// reattaches to its previously-persisted session verbatim. A
// different host trying to claim the same id is rejected with
// `ProtocolError::SessionConflict`. iroh-disabled — the resume
// contract is about the `Registry` + `SessionStore` round trip; the
// gossip bridge has its own coverage.
// =============================================================

/// Re-hosting with a previously-minted `SessionId` recovers the
/// existing session log and members. The second `HostSession`
/// returns the same id (no random remint) and Subscribe replays
/// the pre-restart messages.
#[tokio::test]
async fn host_with_some_id_resumes_persisted_session() {
    let state = fresh_state_dir();
    let alice = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");

    // ---- Daemon 1: host a fresh session, send three messages ----
    let daemon1 = LocalDaemon::spawn_at(&state).await;
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
    let daemon2 = LocalDaemon::spawn_at(&state).await;
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
            Event::Message { message, .. } => {
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

    let daemon = LocalDaemon::spawn_at(&state).await;
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
        ClientError::Protocol(ProtocolError::SessionConflict(id)) => {
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
    let chosen = SessionId::from_bytes([0xbe; 16]);

    let daemon = LocalDaemon::spawn_at(&state).await;
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

// =============================================================
// Persistence: a daemon's sessions outlive the daemon process.
// Hosts a session, sends messages, kills the daemon, starts a fresh
// daemon at the same state directory, and asserts the session and
// its log are recovered.
// =============================================================

#[tokio::test]
async fn session_and_log_survive_daemon_restart() {
    let state = fresh_state_dir();

    // ---- First daemon: host + send three messages ----
    let daemon1 = LocalDaemon::spawn_at(&state).await;
    let alice_client = Client::connect(&state.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");

    let session_id = match alice_client
        .request(Request::HostSession {
            peer: alice_peer.clone(),
            session: None,
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
    let daemon2 = LocalDaemon::spawn_at(&state).await;
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
    let daemon = LocalDaemon::spawn_at(&state).await;
    let client = Client::connect(&state.socket).await.unwrap();
    let alice = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");

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
