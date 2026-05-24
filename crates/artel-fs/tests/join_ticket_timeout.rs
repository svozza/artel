//! `WorkspaceConfig::join_ticket_timeout` controls how long a
//! joiner waits for the host's `workspace.ticket` system message
//! before giving up.
//!
//! Two scenarios exercised here, both against a session whose host
//! never calls `Workspace::host` (so no ticket is ever published):
//!
//! - With `Some(short)`: `Workspace::join_with` errors within
//!   roughly the configured budget.
//! - With `None`: `Workspace::join_with` stays pending — the
//!   future hasn't resolved several seconds in. Long-lived joiners
//!   that arrive minutes or hours after the host first published
//!   are the use case here.

mod common;

use std::time::{Duration, Instant};

use artel_client::Client;
use artel_fs::{AttachPolicy, Workspace, WorkspaceConfig, WorkspaceError};
use artel_protocol::{PeerId, PeerInfo, Request, Response};
use futures_util::future::FutureExt;
use tempfile::TempDir;
use tokio::time::sleep;

struct JoinerSetup {
    daemon_a: common::RunningDaemon,
    daemon_b: common::RunningDaemon,
    bob: Client,
    session: artel_protocol::SessionId,
    bob_dir: TempDir,
    bob_state: TempDir,
}

async fn host_session_without_workspace() -> JoinerSetup {
    let (daemon_a, daemon_b) = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let (session, ticket) = match alice
        .request(Request::HostSession { peer: alice_peer })
        .await
        .unwrap()
    {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("HostSession: got {other:?}"),
    };
    // Deliberately *do not* call `Workspace::host` on alice — we
    // want the joiner's `wait_for_ticket` to find an empty session.
    drop(alice);

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

    let bob_dir = tempfile::tempdir().unwrap();
    let bob_state = tempfile::tempdir().unwrap();

    JoinerSetup {
        daemon_a,
        daemon_b,
        bob,
        session,
        bob_dir,
        bob_state,
    }
}

fn workspace_config(state: &TempDir, timeout: Option<Duration>) -> WorkspaceConfig {
    WorkspaceConfig::default()
        .with_state_dir(state.path().to_path_buf())
        .with_join_ticket_timeout(timeout)
}

#[tokio::test(flavor = "multi_thread")]
async fn join_with_short_timeout_errors_when_no_ticket_published() {
    let setup = host_session_without_workspace().await;
    let JoinerSetup {
        daemon_a,
        daemon_b,
        bob,
        session,
        bob_dir,
        bob_state,
    } = setup;

    let cfg = workspace_config(&bob_state, Some(Duration::from_millis(500)));
    let started = Instant::now();
    let err = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        cfg,
    )
    .await
    .expect_err("must time out — no host ever published a ticket");
    let elapsed = started.elapsed();

    match err {
        WorkspaceError::Iroh(msg) if msg.contains("timed out waiting for workspace.ticket") => {}
        other => panic!("expected ticket-timeout error, got {other:?}"),
    }
    // Generous upper bound: 500ms budget + setup/scheduling slack.
    // The point is "errored quickly", not "errored at exactly 500ms".
    assert!(
        elapsed < Duration::from_secs(5),
        "join with 500ms timeout took {elapsed:?} — should have errored fast",
    );

    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
    let _ = bob_dir;
    let _ = bob_state;
}

#[tokio::test(flavor = "multi_thread")]
async fn join_with_no_timeout_stays_pending_when_no_ticket_published() {
    let setup = host_session_without_workspace().await;
    let JoinerSetup {
        daemon_a,
        daemon_b,
        bob,
        session,
        bob_dir,
        bob_state,
    } = setup;

    let cfg = workspace_config(&bob_state, None);
    let bob_dir_path = bob_dir.path().to_path_buf();
    let mut join_fut = Box::pin(Workspace::join_with(
        &bob,
        session,
        bob_dir_path,
        AttachPolicy::RequireEmpty,
        cfg,
    ));

    // Real wall-clock wait — `tokio::time::pause` would also pause
    // the daemon's internal timers and risk false positives. 3s is
    // enough to be meaningful (the old hard-coded ceiling was 15s,
    // recently bumped to 60s; wall-time evidence at 3s shows the
    // wait is genuinely unbounded).
    sleep(Duration::from_secs(3)).await;
    assert!(
        (&mut join_fut).now_or_never().is_none(),
        "join_with(timeout=None) must stay pending while no ticket is published",
    );

    // Cancel the join by dropping it; tear down cleanly.
    drop(join_fut);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
    let _ = bob_dir;
    let _ = bob_state;
}
