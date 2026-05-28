//! Real-n0 + ungraceful-shutdown variant of
//! [`host_restart_live_writes_n0`].
//!
//! The graceful-shutdown sibling (`host_restart_live_writes_n0`)
//! calls [`Workspace::shutdown`] before stopping alice's daemon.
//! The chat harness explicitly does **not** do that — it triggers
//! daemon shutdown and lets the workspace drop. This test
//! reproduces that ungraceful pattern to test the hypothesis that
//! an undrained iroh node teardown leaves orphan gossip-subscription
//! state on bob's side that doesn't recover when alice's iroh node
//! respawns.
//!
//! See `docs/handoff-post-workspace-registry.md` § "Open
//! investigation" item (4): "The harness doesn't even call
//! `Workspace::shutdown()`. It triggers `daemon_shutdown` and lets
//! the workspace drop."

#![allow(clippy::too_many_lines)]

mod common;

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, TICKET_ACTION, Workspace, WorkspaceConfig, ticket as fs_ticket};
use artel_protocol::{Event, MessageKind, PeerId, PeerInfo, Request, Response};
use iroh_docs::DocTicket;
use tempfile::TempDir;
use tokio::time::timeout;

use common::{DaemonPaths, fresh_state, spawn_daemon_at, wait_for_file};

const TICKET_BUDGET: Duration = Duration::from_secs(30);
/// Per-phase timeout — keeps the test self-bounded so a hang fails
/// fast with a "phase X hung" panic rather than the test runner's
/// "running for over 60 seconds" indefinite wait.
const PHASE_BUDGET: Duration = Duration::from_secs(45);

async fn phase<F, T>(name: &'static str, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    eprintln!(">>> phase begin: {name}");
    let res = timeout(PHASE_BUDGET, fut)
        .await
        .unwrap_or_else(|_| panic!("phase hung past {PHASE_BUDGET:?}: {name}"));
    eprintln!("<<< phase end:   {name}");
    res
}

/// Initialise tracing-subscriber once per test process. Honours
/// `RUST_LOG` (defaulting to `info,artel_fs=debug,artel_daemon=debug`
/// so the watcher / applier / lifecycle logs are visible by default
/// when this test fails); writes to test stderr via `--nocapture`.
fn init_tracing() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| {
            "info,artel_fs=debug,artel_daemon=debug,iroh_docs=info,iroh_gossip=info".to_string()
        });
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .with_test_writer()
            .try_init();
    });
}

// `#[ignore]`d because this test deliberately reproduces a known
// failure mode (host workspace dropped without shutdown → next
// `host_with` hangs in `Endpoint::online` because n0's relay
// rejects same-`EndpointId` reconnect). The substrate's contract
// is that callers must call `Workspace::shutdown` before drop;
// the Drop bomb in `drop_bomb.rs` enforces detection. This test
// exists as documentation of the failure mode and as a regression
// trap the day someone changes the substrate to handle ungraceful
// shutdown — at which point flip the `#[ignore]` and the
// assertions become live.
//
// Run explicitly with:
//   cargo test -p artel-fs --test host_restart_ungraceful_n0 -- --ignored
#[ignore = "deliberately reproduces a known failure mode (no workspace shutdown)"]
#[tokio::test(flavor = "multi_thread")]
async fn alice_post_ungraceful_restart_writes_reach_bob_real_n0() {
    init_tracing();
    let alice_daemon_root = TempDir::new().unwrap();
    let alice_paths = DaemonPaths::at(alice_daemon_root.path());
    let alice_root = TempDir::new().unwrap();
    let alice_wstate = TempDir::new().unwrap();
    let bob_root = TempDir::new().unwrap();
    let bob_wstate = TempDir::new().unwrap();

    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");

    let alice_daemon = phase(
        "spawn alice daemon (real n0)",
        spawn_daemon_at(&alice_paths, None),
    )
    .await;
    let bob_daemon_state = fresh_state();
    let bob_paths = DaemonPaths::at(bob_daemon_state.root.path());
    let bob_daemon = phase(
        "spawn bob daemon (real n0)",
        spawn_daemon_at(&bob_paths, None),
    )
    .await;

    // Phase 1: bring alice + bob up, exchange one file each way.
    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default().with_state_dir(alice_wstate.path().to_path_buf());
    let (alice_ws, _alice_ws_events) = Box::pin(phase(
        "alice host_with phase 1",
        Workspace::host_with(
            &alice,
            alice_peer.clone(),
            alice_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            alice_cfg,
        ),
    ))
    .await
    .expect("Workspace::host_with phase 1");
    let session = alice_ws.session_id();
    let artel_ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();

    let _ = alice
        .request(Request::Subscribe {
            session,
            since: None,
        })
        .await
        .unwrap();
    let mut alice_events = alice.take_events().await.expect("alice events");
    let _phase1_ticket = capture_ticket(&mut alice_events, session).await;

    let alice_ws = Arc::new(alice_ws);
    let _alice_handle_phase1 = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&bob_daemon.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            peer: bob_peer.clone(),
            ticket: artel_ticket,
        })
        .await
        .unwrap();
    let bob_session = match resp {
        Response::JoinSession { session, .. } => session,
        other => panic!("JoinSession: got {other:?}"),
    };
    assert_eq!(bob_session, session);

    let bob_cfg = WorkspaceConfig::default().with_state_dir(bob_wstate.path().to_path_buf());
    let (bob_ws, _bob_ws_events) = Box::pin(phase(
        "bob join_with phase 1",
        Workspace::join_with(
            &bob,
            session,
            bob_root.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            bob_cfg,
        ),
    ))
    .await
    .expect("Workspace::join_with");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    tokio::fs::write(alice_root.path().join("pre_alice.txt"), b"alpha")
        .await
        .unwrap();
    phase(
        "pre-restart alice → bob",
        wait_for_file(&bob_root.path().join("pre_alice.txt"), b"alpha"),
    )
    .await;

    tokio::fs::write(bob_root.path().join("pre_bob.txt"), b"bravo")
        .await
        .unwrap();
    phase(
        "pre-restart bob → alice",
        wait_for_file(&alice_root.path().join("pre_bob.txt"), b"bravo"),
    )
    .await;

    // Phase 2: ungraceful alice shutdown — drop the workspace
    // *without* calling shutdown(), then stop the daemon. This is
    // exactly what the chat harness does:
    //
    //   tracing::info!("main: TUI returned, triggering daemon shutdown");
    //   daemon_shutdown.trigger();
    //   let join_result = tokio::time::timeout(Duration::from_secs(3), daemon_join).await;
    //   ...
    //   res                         // <- workspace dropped here
    //
    // No `Workspace::shutdown()`, no `await` on the run-handle.
    drop(alice_events);
    drop(alice);
    drop(alice_ws); // workspace dropped; watcher + applier still running on a now-dead daemon
    phase("alice daemon stop (phase 2)", alice_daemon.stop()).await;

    // Phase 3: alice respawns at the same paths. Returning host —
    // iroh.key + doc-id keep her identity stable.
    let alice_daemon = phase(
        "spawn alice daemon (phase 3, returning)",
        spawn_daemon_at(&alice_paths, None),
    )
    .await;
    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default().with_state_dir(alice_wstate.path().to_path_buf());
    let (alice_ws, _alice_ws_events) = Box::pin(phase(
        "alice host_with phase 3 (returning)",
        Workspace::host_with(
            &alice,
            alice_peer,
            alice_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            alice_cfg,
        ),
    ))
    .await
    .expect("Workspace::host_with phase 2");
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // Phase 4: bob → alice. Cheap check; expected to work.
    tokio::fs::write(bob_root.path().join("post_restart_bob.txt"), b"charlie")
        .await
        .unwrap();
    phase(
        "post-restart bob → alice",
        wait_for_file(&alice_root.path().join("post_restart_bob.txt"), b"charlie"),
    )
    .await;

    // Phase 5: alice → bob. THE LOAD-BEARING ASSERTION.
    tokio::fs::write(alice_root.path().join("post_restart_alice.txt"), b"delta")
        .await
        .unwrap();
    phase(
        "post-restart alice → bob (load-bearing)",
        wait_for_file(&bob_root.path().join("post_restart_alice.txt"), b"delta"),
    )
    .await;

    alice_ws.shutdown().await;
    bob_ws.shutdown().await;
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    alice_daemon.stop().await;
    bob_daemon.stop().await;
    drop(alice_daemon_root);
    drop(alice_root);
    drop(alice_wstate);
    drop(bob_root);
    drop(bob_wstate);
    drop(bob_daemon_state);
}

async fn capture_ticket(
    events: &mut artel_client::EventStream,
    session: artel_protocol::SessionId,
) -> DocTicket {
    let payload = timeout(TICKET_BUDGET, async {
        loop {
            let ev = events.recv().await.expect("event channel closed");
            if let Event::Message {
                session: ev_session,
                message,
            } = ev
                && ev_session == session
                && message.kind == MessageKind::System
                && message.action == TICKET_ACTION
            {
                return message.payload;
            }
        }
    })
    .await
    .expect("workspace.ticket never arrived");

    let envelope = fs_ticket::decode(&payload).expect("envelope decode");
    DocTicket::from_str(&envelope.doc_ticket).expect("DocTicket parse")
}
