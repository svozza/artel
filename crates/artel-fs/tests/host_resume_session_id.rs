//! Re-hosting the same workspace dir under a fresh host daemon
//! recovers the same [`SessionId`] and lets an existing joiner keep
//! receiving messages from the host across the host's daemon
//! restart.
//!
//! This is the user-visible payoff of `Workspace::host_with` deriving
//! the session id from the local `NamespaceId` (sub-slice 1c) on top
//! of `Registry::host` resuming on `Some(id)` (sub-slice 1b). Without
//! either piece, Alice's restart would mint a fresh session id and
//! Bob's mirror — still subscribed to the *old* gossip topic — would
//! go silent.
//!
//! Test shape:
//! - Alice's daemon A1 + Bob's daemon B (cross-seeded) + Alice's
//!   workspace + Bob's workspace.
//! - Capture Alice's session id and the artel join ticket.
//! - Bob joins; live propagation works (sanity).
//! - Alice's workspace shuts down; daemon A1 stops.
//! - Daemon B and Bob's workspace stay up the whole time.
//! - A fresh daemon A2 spins up against Alice's *same* state dir
//!   (iroh.key + sessions/ persist), and Alice's workspace stands up
//!   again on the *same* root.
//! - Assert: Alice's new session id equals the old one.
//! - Assert: Alice's new local-doc-id equals the old one (the doc
//!   ticket would be byte-stable too, but we don't depend on the
//!   surface here — `NamespaceId` is what derives the session id).
//! - Assert: a fresh write on Alice's side propagates to Bob over
//!   the same gossip topic. **This is the load-bearing assertion.**

mod common;

use common::{daemon_testing_setup, testing_setup};

use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, Workspace, WorkspaceConfig, session_id_for};
use artel_protocol::{PeerId, PeerInfo, Request, Response};
use iroh::test_utils::DnsPkarrServer;
use tempfile::TempDir;

// Long, deliberately linear two-phase scenario — extracting per-phase
// helpers would obscure the order more than the length hurts.
// `used_underscore_binding`: the test pulls paths out of
// `RunningDaemon._state` to rebuild a fresh `DaemonState` for the
// second daemon. Renaming the field would ripple through every
// fixture caller; an allow here is the smallest concession.
#[allow(clippy::too_many_lines, clippy::used_underscore_binding)]
#[tokio::test(flavor = "multi_thread")]
async fn re_hosting_recovers_session_id_and_resumes_message_flow() {
    let dns_pkarr = Arc::new(DnsPkarrServer::run().await.unwrap());

    // Alice's persistent state: workspace root, workspace state dir,
    // and daemon state (iroh.key + sessions). All three outlive the
    // first daemon so the second daemon picks them up.
    let alice_root = TempDir::new().unwrap();
    let alice_wstate = TempDir::new().unwrap();
    let alice_daemon_state = common::fresh_state();

    // Bob's daemon and workspace are alive for the whole test — only
    // Alice's daemon restarts.
    let bob_daemon_state = common::fresh_state();
    let bob_root = TempDir::new().unwrap();
    let bob_wstate = TempDir::new().unwrap();

    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");

    // ---------------------------------------------------------------
    // Phase 1: Alice on daemon A1, Bob on daemon B. Live sync works.
    // ---------------------------------------------------------------
    let daemon_b =
        common::spawn_daemon_with_setup(bob_daemon_state, daemon_testing_setup(&dns_pkarr)).await;
    let daemon_a1 =
        common::spawn_daemon_with_setup(alice_daemon_state, daemon_testing_setup(&dns_pkarr)).await;

    let alice_a1 = Client::connect(&daemon_a1.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_endpoint_setup(testing_setup(&dns_pkarr));
    let (alice_ws_1, _alice_events_1) = Workspace::host_with(
        &alice_a1,
        alice_peer.clone(),
        alice_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        alice_cfg,
    )
    .await
    .expect("Workspace::host_with phase 1");
    let session_id_1 = alice_ws_1.session_id();
    let ticket = alice_ws_1
        .join_ticket()
        .expect("phase 1 host has join_ticket")
        .clone();

    // The session id must be derived from Alice's local namespace.
    let derived_1 = session_id_for(alice_ws_1.doc().id());
    assert_eq!(
        session_id_1, derived_1,
        "host_with must register at the derived session id",
    );

    let alice_ws_1 = Arc::new(alice_ws_1);
    let alice_handle_1 = Arc::clone(&alice_ws_1).run().await;

    // Bob joins via the daemon-issued artel ticket.
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            peer: bob_peer,
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_cfg = WorkspaceConfig::default()
        .with_state_dir(bob_wstate.path().to_path_buf())
        .with_endpoint_setup(testing_setup(&dns_pkarr));
    let (bob_ws, _bob_events) = Workspace::join_with(
        &bob,
        session_id_1,
        bob_root.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        bob_cfg,
    )
    .await
    .expect("Workspace::join_with phase 1");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Sanity: phase-1 propagation works.
    tokio::fs::write(alice_root.path().join("phase1.txt"), b"before-restart")
        .await
        .unwrap();
    common::wait_for_file(&bob_root.path().join("phase1.txt"), b"before-restart").await;

    // ---------------------------------------------------------------
    // Tear down Alice's daemon. Keep Bob's daemon + workspace alive.
    // ---------------------------------------------------------------
    alice_ws_1.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), alice_handle_1).await;
    drop(alice_a1);
    let alice_daemon_state_2 = {
        // Reconstruct Alice's daemon state from the same paths the
        // first daemon was using. `RunningDaemon` consumed
        // `alice_daemon_state` so we have to rebuild it from the
        // socket/iroh-key paths captured before phase 1 started.
        // `daemon_a1.socket`/`daemon_a1.iroh_addr` aren't enough — we
        // need a fresh `DaemonState` pointed at the same on-disk paths.
        // The easiest way is to recover the paths via `_state`, which
        // `RunningDaemon` exposes.
        let socket = daemon_a1._state.socket.clone();
        let pid = daemon_a1._state.pid.clone();
        let sessions = daemon_a1._state.sessions.clone();
        let iroh_key = daemon_a1._state.iroh_key.clone();
        let root = daemon_a1._state.root;
        common::DaemonState {
            root,
            socket,
            pid,
            sessions,
            iroh_key,
        }
    };
    daemon_a1.shutdown.trigger();
    tokio::time::timeout(Duration::from_secs(10), daemon_a1.join)
        .await
        .expect("daemon_a1 stop")
        .expect("daemon_a1 join")
        .expect("daemon_a1 io");

    // ---------------------------------------------------------------
    // Phase 2: fresh daemon A2 against Alice's same state dir.
    // ---------------------------------------------------------------
    let daemon_a2 =
        common::spawn_daemon_with_setup(alice_daemon_state_2, daemon_testing_setup(&dns_pkarr))
            .await;

    let alice_a2 = Client::connect(&daemon_a2.socket).await.unwrap();
    let alice_cfg_2 = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_endpoint_setup(testing_setup(&dns_pkarr));
    let (alice_ws_2, _alice_events_2) = Workspace::host_with(
        &alice_a2,
        alice_peer,
        alice_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        alice_cfg_2,
    )
    .await
    .expect("Workspace::host_with phase 2");

    // The whole point of this slice: same session id across the
    // restart. Stable id → stable gossip topic → Bob's mirror keeps
    // receiving messages.
    assert_eq!(
        alice_ws_2.session_id(),
        session_id_1,
        "re-hosting same workspace dir must recover the same session id",
    );

    // Belt-and-braces: the underlying NamespaceId must also be the
    // same (otherwise the session id could only have matched by
    // coincidence — extremely unlikely under blake3, but worth
    // pinning).
    assert_eq!(
        alice_ws_2.doc().id(),
        alice_ws_1.doc().id(),
        "re-hosting same workspace dir must recover the same NamespaceId",
    );

    let alice_ws_2 = Arc::new(alice_ws_2);
    let alice_handle_2 = Arc::clone(&alice_ws_2).run().await;

    // Load-bearing: Bob's mirror, still subscribed to the gossip
    // topic derived from `session_id_1`, must see a fresh write from
    // Alice's reincarnated workspace. Without 1b's resume path or 1c's
    // derivation, this assertion fails — the host's restart would
    // either mint a fresh session id (and Bob would be on the old
    // topic alone) or stamp out a fresh empty namespace (and the
    // file would never reach Bob's iroh-docs replica at all).
    tokio::fs::write(alice_root.path().join("phase2.txt"), b"after-restart")
        .await
        .unwrap();
    common::wait_for_file(&bob_root.path().join("phase2.txt"), b"after-restart").await;

    alice_ws_2.shutdown().await;
    bob_ws.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), alice_handle_2).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice_a2);
    drop(bob);
    daemon_a2.stop().await;
    daemon_b.stop().await;
}
