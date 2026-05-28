//! E2E coverage for the `artel-fs/workspace/v1` attachment slice.
//!
//! `Workspace::host_with` and `Workspace::join_with` register a typed
//! [`WorkspaceAttachmentV1`] with the daemon as part of standing up.
//! These tests pin the user-visible properties of that registration
//! through the IPC boundary:
//!
//! - Host registers with `role: Host` after a successful attach.
//! - Joiner registers with `role: Joiner` against its *own* daemon
//!   (each daemon's attachment view is local).
//! - The typed [`list_known_workspaces`] helper returns the same
//!   data as a raw `Request::ListAttachments` round-trip.
//! - The attachment survives a daemon restart at the same state dir
//!   — combined with the stable-session-id slice this makes a
//!   workspace's discovery entry durable across host crashes.
//! - `Request::LeaveSession` cascades the attachment via the daemon's
//!   2b cascade contract.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_fs::{
    AttachPolicy, KIND_V1, Workspace, WorkspaceAttachmentV1, WorkspaceConfig, WorkspaceRole,
    list_known_workspaces,
};
use artel_protocol::{Attachment, PeerId, PeerInfo, Request, Response};
use tempfile::TempDir;
use tokio::time::timeout;

/// Raw IPC list — used to verify that [`list_known_workspaces`]
/// matches a hand-rolled `ListAttachments` round-trip.
async fn raw_list(client: &Client, kind: Option<&str>) -> Vec<Attachment> {
    let resp = client
        .request(Request::ListAttachments {
            kind: kind.map(str::to_owned),
        })
        .await
        .expect("ListAttachments");
    match resp {
        Response::Attachments { entries } => entries,
        other => panic!("expected Attachments, got {other:?}"),
    }
}

/// Stand up an artel daemon in a tempdir; no iroh feature needed for
/// tests that only exercise the attachment IPC against a local
/// `Workspace::host` (which itself spawns its own iroh node).
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
        let sessions = tempdir.path().join("sessions");
        let daemon = Daemon::start(DaemonConfig {
            socket_path: socket.clone(),
            pid_path: pid,
            sessions_dir: sessions,
            daemon_peer_id: PeerId::from_bytes([0xee; 32]),
            iroh_key_path: None,
            address_lookup: None,
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

    async fn stop(self) {
        self.shutdown.trigger();
        timeout(Duration::from_secs(5), self.join)
            .await
            .expect("daemon did not exit within 5s")
            .expect("daemon panicked")
            .expect("daemon io");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn host_workspace_registers_attachment_via_ipc() {
    let harness = DaemonHarness::spawn().await;

    let alice = Client::connect(&harness.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");

    let ws_root = tempfile::tempdir().unwrap();
    let ws_state = tempfile::tempdir().unwrap();
    let cfg = WorkspaceConfig::default().with_state_dir(ws_state.path().to_path_buf());

    let (workspace, _events) = Workspace::host_with(
        &alice,
        alice_peer,
        ws_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        cfg,
    )
    .await
    .expect("Workspace::host_with");
    let session = workspace.session_id();

    // The host's `host_with` registered an attachment as part of
    // standing up. List via raw IPC and confirm exactly one entry
    // back.
    let entries = raw_list(&alice, Some(KIND_V1)).await;
    assert_eq!(entries.len(), 1, "host should register exactly one entry");
    assert_eq!(entries[0].session, session);
    assert_eq!(entries[0].kind, KIND_V1);

    let decoded = WorkspaceAttachmentV1::decode(&entries[0].payload).expect("decode payload");
    assert_eq!(decoded.role, WorkspaceRole::Host);
    // `host_with` canonicalises `root` and resolves `state_dir` against
    // it; canonicalising the test paths the same way is the only
    // robust comparison (tempfile paths on macOS round-trip through
    // `/private/var/...`).
    assert_eq!(
        decoded.local_path,
        std::fs::canonicalize(ws_root.path()).unwrap_or_else(|_| ws_root.path().to_path_buf()),
    );
    assert_eq!(decoded.state_dir, ws_state.path());

    workspace.shutdown().await;
    drop(alice);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn join_workspace_registers_attachment_via_ipc() {
    // Each daemon's attachment view is local — Alice sees only her
    // own host attachment, Bob sees only his own joiner attachment.
    let pair = common::spawn_pair().await;
    let common::Pair {
        daemon_a,
        daemon_b,
        workspace_lookup_a,
        workspace_lookup_b,
    } = pair;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let alice_root = tempfile::tempdir().unwrap();
    let (alice_ws, _alice_events) = Workspace::host_with(
        &alice,
        alice_peer,
        alice_root.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_address_lookup_override(workspace_lookup_a),
    )
    .await
    .expect("Workspace::host");
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
    let resp = bob
        .request(Request::JoinSession {
            peer: bob_peer,
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_root = tempfile::tempdir().unwrap();
    let (bob_ws, _bob_events) = Workspace::join_with(
        &bob,
        session,
        bob_root.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_address_lookup_override(workspace_lookup_b),
    )
    .await
    .expect("Workspace::join");

    // Alice's daemon: one attachment, role=Host.
    let alice_entries = raw_list(&alice, Some(KIND_V1)).await;
    assert_eq!(alice_entries.len(), 1, "alice's daemon: one attachment");
    let alice_decoded =
        WorkspaceAttachmentV1::decode(&alice_entries[0].payload).expect("alice decode");
    assert_eq!(alice_decoded.role, WorkspaceRole::Host);
    assert_eq!(alice_entries[0].session, session);

    // Bob's daemon: one attachment, role=Joiner.
    let bob_entries = raw_list(&bob, Some(KIND_V1)).await;
    assert_eq!(bob_entries.len(), 1, "bob's daemon: one attachment");
    let bob_decoded = WorkspaceAttachmentV1::decode(&bob_entries[0].payload).expect("bob decode");
    assert_eq!(bob_decoded.role, WorkspaceRole::Joiner);
    assert_eq!(bob_entries[0].session, session);

    alice_ws.shutdown().await;
    bob_ws.shutdown().await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn list_known_workspaces_helper_returns_typed_view() {
    let harness = DaemonHarness::spawn().await;

    let alice = Client::connect(&harness.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([3; 32]), "alice");
    let ws_root = tempfile::tempdir().unwrap();
    let (workspace, _events) = Workspace::host_with(
        &alice,
        alice_peer,
        ws_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default(),
    )
    .await
    .expect("host_with");
    let session = workspace.session_id();

    let known = list_known_workspaces(&alice)
        .await
        .expect("list_known_workspaces");
    assert_eq!(known.len(), 1);
    assert_eq!(known[0].session, session);
    assert_eq!(known[0].attachment.role, WorkspaceRole::Host);
    assert_eq!(
        known[0].attachment.local_path,
        std::fs::canonicalize(ws_root.path()).unwrap_or_else(|_| ws_root.path().to_path_buf()),
    );

    workspace.shutdown().await;
    drop(alice);
    harness.stop().await;
}

// `used_underscore_binding`: this test rebuilds a fresh `DaemonState`
// from `RunningDaemon._state` to give the second daemon the same
// on-disk paths. Renaming the field would ripple through every
// fixture caller; matches `host_resume_session_id.rs`.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines, clippy::used_underscore_binding)]
async fn attachment_persists_across_daemon_restart() {
    // Stable session id (slice 1) + persistent attachment (slice 2)
    // combine to make a workspace's discovery entry durable across
    // its host daemon crashing. Re-host at the same state dir, then
    // assert `list_known_workspaces` reports the same workspace
    // entry — same session, same paths, same role.
    let shared = iroh::address_lookup::memory::MemoryLookup::new();

    let alice_root = TempDir::new().unwrap();
    let alice_wstate = TempDir::new().unwrap();
    let alice_daemon_state = common::fresh_state();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");

    // Phase 1: host once, capture session id and attachment.
    let daemon_a1 = common::spawn_daemon_with_lookup(alice_daemon_state, shared.clone()).await;
    shared.add_endpoint_info(daemon_a1.iroh_addr.clone().expect("daemon_a1 iroh addr"));

    let alice_a1 = Client::connect(&daemon_a1.socket).await.unwrap();
    let cfg_1 = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_address_lookup_override(shared.clone());
    let (alice_ws_1, _alice_events_1) = Workspace::host_with(
        &alice_a1,
        alice_peer.clone(),
        alice_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        cfg_1,
    )
    .await
    .expect("phase 1 host_with");
    let session_id_1 = alice_ws_1.session_id();

    let known_1 = list_known_workspaces(&alice_a1).await.expect("list 1");
    assert_eq!(known_1.len(), 1, "phase 1 should have one entry");
    let phase_1_entry = known_1.into_iter().next().unwrap();
    assert_eq!(phase_1_entry.session, session_id_1);
    assert_eq!(phase_1_entry.attachment.role, WorkspaceRole::Host);

    // Tear down: workspace then daemon. Recover the daemon's
    // on-disk paths so phase 2 can reattach to the same state.
    alice_ws_1.shutdown().await;
    drop(alice_a1);
    let alice_daemon_state_2 = common::DaemonState {
        root: daemon_a1._state.root,
        socket: daemon_a1._state.socket.clone(),
        pid: daemon_a1._state.pid.clone(),
        sessions: daemon_a1._state.sessions.clone(),
        iroh_key: daemon_a1._state.iroh_key.clone(),
    };
    daemon_a1.shutdown.trigger();
    timeout(Duration::from_secs(10), daemon_a1.join)
        .await
        .expect("daemon_a1 stop")
        .expect("daemon_a1 join")
        .expect("daemon_a1 io");

    // Phase 2: fresh daemon, same state dir, same workspace. Stable
    // session id means the attachment from phase 1 is still indexed
    // under the same `(session, kind)` key on disk.
    let daemon_a2 = common::spawn_daemon_with_lookup(alice_daemon_state_2, shared.clone()).await;
    shared.add_endpoint_info(daemon_a2.iroh_addr.clone().expect("daemon_a2 iroh addr"));

    let alice_a2 = Client::connect(&daemon_a2.socket).await.unwrap();

    // The attachment must already be visible *before* phase-2
    // host_with re-registers — that's the durability claim. Phase 2
    // re-register would overwrite (same `(session, kind)`), so reading
    // first proves the on-disk file from phase 1 is what we're
    // observing.
    let known_pre_register = list_known_workspaces(&alice_a2)
        .await
        .expect("list pre re-register");
    assert_eq!(
        known_pre_register.len(),
        1,
        "phase-1 attachment should survive daemon restart",
    );
    assert_eq!(known_pre_register[0].session, session_id_1);
    assert_eq!(
        known_pre_register[0].attachment.local_path,
        phase_1_entry.attachment.local_path,
    );

    // Re-host: idempotent register at the same `(session, KIND_V1)`
    // overwrites with identical bytes. Still exactly one entry,
    // still the same session id.
    let cfg_2 = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_address_lookup_override(shared.clone());
    let (alice_ws_2, _alice_events_2) = Workspace::host_with(
        &alice_a2,
        alice_peer,
        alice_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        cfg_2,
    )
    .await
    .expect("phase 2 host_with");
    assert_eq!(
        alice_ws_2.session_id(),
        session_id_1,
        "stable session id across restart",
    );

    let known_2 = list_known_workspaces(&alice_a2).await.expect("list 2");
    assert_eq!(known_2.len(), 1, "phase 2 still one entry");
    assert_eq!(known_2[0].session, session_id_1);
    assert_eq!(known_2[0].attachment, phase_1_entry.attachment);

    alice_ws_2.shutdown().await;
    drop(alice_a2);
    daemon_a2.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn attachment_removed_on_host_leave_session() {
    let harness = DaemonHarness::spawn().await;

    let alice = Client::connect(&harness.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let ws_root = tempfile::tempdir().unwrap();
    let (workspace, _events) = Workspace::host_with(
        &alice,
        alice_peer,
        ws_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default(),
    )
    .await
    .expect("host_with");
    let session = workspace.session_id();

    assert_eq!(raw_list(&alice, Some(KIND_V1)).await.len(), 1);

    // Cascade: leaving the session removes the attachment via the
    // 2b `delete(session)` cascade.
    alice
        .request(Request::LeaveSession { session })
        .await
        .expect("LeaveSession");

    assert!(
        raw_list(&alice, Some(KIND_V1)).await.is_empty(),
        "LeaveSession should cascade-delete the attachment",
    );

    workspace.shutdown().await;
    drop(alice);
    harness.stop().await;
}
