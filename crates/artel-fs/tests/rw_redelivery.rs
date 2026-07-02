//! RW-secret re-delivery after a joiner daemon restart, with and
//! without a namespace rotation while the joiner was offline.
//!
//! Real-n0 tests (see `docs/plans/2026-06-18-rw-redelivery.md`):
//!
//! 1. `joiner_daemon_restart_resyncs_both_ways_real_n0` — no rotation.
//!    Proves Part 1: a joiner whose *daemon* restarts re-establishes
//!    gossip presence (lazily, on its first post-restart Send) so live
//!    writes flow both ways again. This path had no prior coverage — the
//!    one "joiner restart" test (`alice_post_restart_writes_reach_bob`)
//!    actually restarts the *host*.
//!
//! 2. `returning_rw_member_offline_across_rotation_regains_write_real_n0`
//!    — the honest regression. bob holds RW, goes offline (daemon down),
//!    the host evicts a *different* peer (carol) which rotates the
//!    namespace, then bob's daemon restarts and reattaches. Bob must
//!    regain write on the rotated namespace with NO manual re-grant
//!    (Part 1 restores presence; Part 2 re-delivers the current secret
//!    on bob's `NODE_ID` re-announce).
//!
//! 3. `returning_rw_member_regains_write_after_host_workspace_restart_real_n0`
//!    — finding-#1 regression. Same as (2), but the HOST's workspace
//!    restarts after the rotation, before bob returns. Proves the host's
//!    NODE_ID-redelivery cell is re-seeded with the epoch recovered from
//!    disk (not a hard-coded 0, which would make the re-delivered rotate
//!    look stale to bob's monotonic-epoch guard).
//!
//! 4. `offline_read_to_write_promotion_delivers_secret_on_return_real_n0`
//!    — a Read member promoted to RW while offline receives the secret
//!    on its return (host-side detection: the returner holds no prior
//!    secret to compare against).
//!
//! All four tests are real-network and suffixed `_n0`: the default
//! nextest profile filters them out; `--profile n0` runs them. Tests
//! 1, 2, and 4 ride the bin-shared localhost relay
//! (`ProductionCustomRelay`); test 3 stays on n0's public relay — see
//! [`prod`] for why its simultaneous-restart topology can't use
//! the localhost relay yet.

mod common;

use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, Workspace, WorkspaceConfig};
use artel_protocol::{Request, Response};
use tempfile::TempDir;
use tokio::time::timeout;

use common::{
    DaemonPaths, drain_ws_events, fresh_state, grant_rw, grant_rw_and_wait, init_tracing,
    phase_budgeted, revoke, spawn_daemon_at, wait_for_file,
};

const PHASE_BUDGET: Duration = Duration::from_secs(45);

async fn phase<F, T>(name: &'static str, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    phase_budgeted(name, PHASE_BUDGET, fut).await
}

/// Public-n0-relay setup, used ONLY by
/// `returning_rw_member_regains_write_after_host_workspace_restart_real_n0`.
/// That test's simultaneous-restart topology (host workspace restarts,
/// then bob's daemon+workspace restart and must re-discover the host's
/// rebound endpoint) deterministically fails to re-establish gossip on
/// the localhost shared relay — bob's dial to the host is closed with
/// `ApplicationClosed { error_code: 62 }` and the host's post-restart
/// workspace never sees a `NeighborUp` — while the same topology
/// converges on n0's public relay. Diagnosed 2026-07-02 (flake-detective;
/// upstream iroh-gossip/iroh-docs peer-rediscovery edge, not a substrate
/// bug). Revisit after the next iroh upgrade. The other three tests use
/// [`common::custom_relay_setup`] (localhost relay).
const fn prod() -> artel_fs::EndpointSetup {
    artel_fs::EndpointSetup::Production
}

/// Part 1 in isolation: a joiner daemon restart re-establishes live
/// gossip sync both ways, no rotation involved.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::large_futures, clippy::too_many_lines)]
async fn joiner_daemon_restart_resyncs_both_ways_real_n0() {
    init_tracing();

    let alice_root_d = TempDir::new().unwrap();
    let alice_paths = DaemonPaths::at(alice_root_d.path());
    let alice_root = TempDir::new().unwrap();
    let alice_wstate = TempDir::new().unwrap();

    // Bob's daemon state must survive the restart, so give it fixed
    // paths under a TempDir we keep alive (not `fresh_state`, which is
    // for daemons that don't restart).
    let bob_root_d = TempDir::new().unwrap();
    let bob_paths = DaemonPaths::at(bob_root_d.path());
    let bob_root = TempDir::new().unwrap();
    let bob_wstate = TempDir::new().unwrap();

    let alice_daemon = phase(
        "spawn alice daemon",
        spawn_daemon_at(&alice_paths, common::custom_relay_setup().await),
    )
    .await;
    let bob_daemon = phase(
        "spawn bob daemon (initial)",
        spawn_daemon_at(&bob_paths, common::custom_relay_setup().await),
    )
    .await;

    // alice hosts.
    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_daemon_socket(alice_daemon.socket.clone())
        .with_endpoint_setup(common::custom_relay_setup().await);
    let (alice_ws, alice_ws_events) = phase(
        "alice Workspace::host_with",
        Workspace::host_with(
            &alice,
            "alice",
            alice_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            alice_cfg,
        ),
    )
    .await
    .expect("host_with");
    drain_ws_events(alice_ws_events);
    let session = alice_ws.session_id();
    let ticket = alice_ws.join_ticket().expect("host ticket").clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // bob joins + gets RW.
    let bob = Client::connect(&bob_daemon.socket).await.unwrap();
    let bob_peer_id = bob.daemon_peer_id();
    let resp = phase(
        "bob JoinSession (initial)",
        bob.request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: ticket.clone(),
        }),
    )
    .await
    .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_cfg = WorkspaceConfig::default()
        .with_state_dir(bob_wstate.path().to_path_buf())
        .with_daemon_socket(bob_daemon.socket.clone())
        .with_endpoint_setup(common::custom_relay_setup().await);
    let (bob_ws, bob_ws_events) = phase(
        "bob Workspace::join_with (initial)",
        Workspace::join_with(
            &bob,
            session,
            bob_root.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            bob_cfg,
        ),
    )
    .await
    .expect("join_with");
    drain_ws_events(bob_ws_events);
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    phase(
        "grant bob RW",
        grant_rw_and_wait(
            &alice,
            session,
            bob_peer_id,
            bob_root.path(),
            alice_root.path(),
        ),
    )
    .await;

    // Baseline: writes flow both ways.
    tokio::fs::write(alice_root.path().join("pre_a.txt"), b"a1")
        .await
        .unwrap();
    phase(
        "pre: alice -> bob",
        wait_for_file(&bob_root.path().join("pre_a.txt"), b"a1"),
    )
    .await;
    tokio::fs::write(bob_root.path().join("pre_b.txt"), b"b1")
        .await
        .unwrap();
    phase(
        "pre: bob -> alice",
        wait_for_file(&alice_root.path().join("pre_b.txt"), b"b1"),
    )
    .await;

    // Bob's daemon goes down (workspace + daemon), then respawns at the
    // same paths. Alice stays up throughout.
    phase("bob_ws.shutdown()", bob_ws.shutdown())
        .await
        .expect("shutdown");
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(bob);
    phase("bob_daemon.stop()", bob_daemon.stop()).await;

    let bob_daemon = phase(
        "spawn bob daemon (post-restart)",
        spawn_daemon_at(&bob_paths, common::custom_relay_setup().await),
    )
    .await;
    let bob = Client::connect(&bob_daemon.socket).await.unwrap();
    // Re-present the ticket: a daemon restart needs JoinSession so the
    // self-rejoin path is taken and membership is re-asserted; the lazy
    // re-subscribe then fires on bob's first post-restart Send.
    let resp = phase(
        "bob JoinSession (post-restart)",
        bob.request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        }),
    )
    .await
    .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_cfg = WorkspaceConfig::default()
        .with_state_dir(bob_wstate.path().to_path_buf())
        .with_daemon_socket(bob_daemon.socket.clone())
        .with_endpoint_setup(common::custom_relay_setup().await);
    let (bob_ws, bob_ws_events) = phase(
        "bob Workspace::join_with (post-restart)",
        Workspace::join_with(
            &bob,
            session,
            bob_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            bob_cfg,
        ),
    )
    .await
    .expect("join_with post-restart");
    drain_ws_events(bob_ws_events);
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // alice -> bob after bob's restart (inbound resumes).
    tokio::fs::write(alice_root.path().join("post_a.txt"), b"a2")
        .await
        .unwrap();
    phase(
        "post: alice -> bob",
        wait_for_file(&bob_root.path().join("post_a.txt"), b"a2"),
    )
    .await;

    // bob -> alice after bob's restart. LOAD-BEARING: this is the send
    // that lazily re-subscribes bob's reloaded mirror's gossip topic.
    tokio::fs::write(bob_root.path().join("post_b.txt"), b"b2")
        .await
        .unwrap();
    phase(
        "post: bob -> alice (LOAD-BEARING, lazy re-subscribe)",
        wait_for_file(&alice_root.path().join("post_b.txt"), b"b2"),
    )
    .await;

    phase("alice_ws.shutdown()", alice_ws.shutdown())
        .await
        .expect("shutdown");
    phase("bob_ws.shutdown() (final)", bob_ws.shutdown())
        .await
        .expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    phase("alice_daemon.stop()", alice_daemon.stop()).await;
    phase("bob_daemon.stop()", bob_daemon.stop()).await;
}

/// The honest regression: bob RW, offline across a rotation triggered by
/// evicting carol, daemon restarts + reattaches, regains write with no
/// re-grant.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::large_futures, clippy::too_many_lines)]
async fn returning_rw_member_offline_across_rotation_regains_write_real_n0() {
    init_tracing();

    let alice_root_d = TempDir::new().unwrap();
    let alice_paths = DaemonPaths::at(alice_root_d.path());
    let alice_root = TempDir::new().unwrap();
    let alice_wstate = TempDir::new().unwrap();

    let bob_root_d = TempDir::new().unwrap();
    let bob_paths = DaemonPaths::at(bob_root_d.path());
    let bob_root = TempDir::new().unwrap();
    let bob_wstate = TempDir::new().unwrap();

    // Carol is the peer alice evicts to force a rotation while bob is
    // offline. Her daemon doesn't restart, so `fresh_state` is fine.
    let carol_daemon_state = fresh_state();
    let carol_paths = DaemonPaths::at(carol_daemon_state.root.path());
    let carol_root = TempDir::new().unwrap();
    let carol_wstate = TempDir::new().unwrap();

    let alice_daemon = phase(
        "spawn alice daemon",
        spawn_daemon_at(&alice_paths, common::custom_relay_setup().await),
    )
    .await;
    let bob_daemon = phase(
        "spawn bob daemon (initial)",
        spawn_daemon_at(&bob_paths, common::custom_relay_setup().await),
    )
    .await;
    let carol_daemon = phase(
        "spawn carol daemon",
        spawn_daemon_at(&carol_paths, common::custom_relay_setup().await),
    )
    .await;

    // alice hosts.
    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_daemon_socket(alice_daemon.socket.clone())
        .with_endpoint_setup(common::custom_relay_setup().await);
    let (alice_ws, alice_ws_events) = phase(
        "alice Workspace::host_with",
        Workspace::host_with(
            &alice,
            "alice",
            alice_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            alice_cfg,
        ),
    )
    .await
    .expect("host_with");
    drain_ws_events(alice_ws_events);
    let session = alice_ws.session_id();
    let ticket = alice_ws.join_ticket().expect("host ticket").clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // bob joins + RW.
    let bob = Client::connect(&bob_daemon.socket).await.unwrap();
    let bob_peer_id = bob.daemon_peer_id();
    let resp = phase(
        "bob JoinSession (initial)",
        bob.request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: ticket.clone(),
        }),
    )
    .await
    .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");
    let bob_cfg = WorkspaceConfig::default()
        .with_state_dir(bob_wstate.path().to_path_buf())
        .with_daemon_socket(bob_daemon.socket.clone())
        .with_endpoint_setup(common::custom_relay_setup().await);
    let (bob_ws, bob_ws_events) = phase(
        "bob Workspace::join_with (initial)",
        Workspace::join_with(
            &bob,
            session,
            bob_root.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            bob_cfg,
        ),
    )
    .await
    .expect("join_with");
    drain_ws_events(bob_ws_events);
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;
    phase(
        "grant bob RW",
        grant_rw_and_wait(
            &alice,
            session,
            bob_peer_id,
            bob_root.path(),
            alice_root.path(),
        ),
    )
    .await;

    // carol joins + RW (she must be RW so eviction triggers a rotation).
    let carol = Client::connect(&carol_daemon.socket).await.unwrap();
    let carol_peer_id = carol.daemon_peer_id();
    let resp = phase(
        "carol JoinSession",
        carol.request(Request::JoinSession {
            display_name: "carol".into(),
            ticket,
        }),
    )
    .await
    .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");
    let carol_cfg = WorkspaceConfig::default()
        .with_state_dir(carol_wstate.path().to_path_buf())
        .with_daemon_socket(carol_daemon.socket.clone())
        .with_endpoint_setup(common::custom_relay_setup().await);
    let (carol_ws, carol_ws_events) = phase(
        "carol Workspace::join_with",
        Workspace::join_with(
            &carol,
            session,
            carol_root.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            carol_cfg,
        ),
    )
    .await
    .expect("join_with carol");
    drain_ws_events(carol_ws_events);
    let carol_ws = Arc::new(carol_ws);
    let carol_handle = Arc::clone(&carol_ws).run().await;
    phase(
        "grant carol RW",
        grant_rw_and_wait(
            &alice,
            session,
            carol_peer_id,
            carol_root.path(),
            alice_root.path(),
        ),
    )
    .await;

    // Baseline: bob can write pre-rotation.
    tokio::fs::write(bob_root.path().join("pre_bob.txt"), b"pre")
        .await
        .unwrap();
    phase(
        "pre: bob -> alice (genesis namespace)",
        wait_for_file(&alice_root.path().join("pre_bob.txt"), b"pre"),
    )
    .await;

    // Bob goes fully offline (workspace + daemon down).
    phase("bob_ws.shutdown()", bob_ws.shutdown())
        .await
        .expect("shutdown");
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(bob);
    phase("bob_daemon.stop()", bob_daemon.stop()).await;

    // While bob is down, alice evicts carol → namespace rotation. Bob's
    // persisted secret is now for the abandoned namespace.
    phase(
        "alice revokes carol (triggers rotation)",
        revoke(&alice, session, carol_peer_id),
    )
    .await;
    // Let the rotation + survivor re-distribution settle on alice's side.
    // (carol is the only other survivor and she's being evicted, so this
    // is really just giving alice's rotation task time to mint + reimport
    // the new namespace before bob returns.)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Bob's daemon restarts and reattaches.
    let bob_daemon = phase(
        "spawn bob daemon (post-restart)",
        spawn_daemon_at(&bob_paths, common::custom_relay_setup().await),
    )
    .await;
    let bob = Client::connect(&bob_daemon.socket).await.unwrap();
    let resp = phase(
        "bob JoinSession (post-restart)",
        bob.request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: alice_ws.join_ticket().expect("host ticket").clone(),
        }),
    )
    .await
    .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");
    let bob_cfg = WorkspaceConfig::default()
        .with_state_dir(bob_wstate.path().to_path_buf())
        .with_daemon_socket(bob_daemon.socket.clone())
        .with_endpoint_setup(common::custom_relay_setup().await);
    let (bob_ws, bob_ws_events) = phase(
        "bob Workspace::join_with (post-restart, rotated namespace)",
        Workspace::join_with(
            &bob,
            session,
            bob_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            bob_cfg,
        ),
    )
    .await
    .expect("join_with post-restart");
    drain_ws_events(bob_ws_events);
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // THE LOAD-BEARING ASSERTION: bob writes on the rotated namespace and
    // it reaches alice, with NO manual re-grant. This works only if:
    //   - Part 1 re-subscribed bob's reloaded mirror's gossip (so his
    //     NODE_ID announce reaches alice), and
    //   - Part 2 re-delivered the *current* (post-rotation) secret on
    //     that NODE_ID, which bob imported onto the rotated namespace.
    // Re-delivery + namespace swap is asynchronous. Two test hazards to
    // dodge:
    //   1. The echo-guard suppresses re-writes of IDENTICAL bytes, so a
    //      fixed-content poll authors only once — and that first write
    //      races the swap, landing on the abandoned genesis doc. Give
    //      each tick UNIQUE content so every tick re-authors, including
    //      after the swap completes.
    //   2. A write needs a round-trip, so reading back the just-written
    //      value in the same tick always loses. Poll a FIXED path and
    //      accept ANY of our writes arriving (content starts with the
    //      known prefix) — whichever post-swap write lands proves bob
    //      regained write on the rotated namespace.
    let deadline = std::time::Instant::now() + Duration::from_secs(40);
    let target = alice_root.path().join("post_bob.txt");
    let mut tick = 0u32;
    loop {
        let content = format!("post-{tick}");
        let _ = tokio::fs::write(bob_root.path().join("post_bob.txt"), content.as_bytes()).await;
        if let Ok(bytes) = tokio::fs::read(&target).await
            && bytes.starts_with(b"post-")
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "returning RW member never regained write on the rotated namespace",
        );
        tick += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    phase("alice_ws.shutdown()", alice_ws.shutdown())
        .await
        .expect("shutdown");
    phase("bob_ws.shutdown() (final)", bob_ws.shutdown())
        .await
        .expect("shutdown");
    phase("carol_ws.shutdown()", carol_ws.shutdown())
        .await
        .expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    let _ = timeout(Duration::from_secs(5), carol_handle).await;
    drop(alice);
    drop(bob);
    drop(carol);
    phase("alice_daemon.stop()", alice_daemon.stop()).await;
    phase("bob_daemon.stop()", bob_daemon.stop()).await;
    phase("carol_daemon.stop()", carol_daemon.stop()).await;
}

/// Finding #1 regression: the rotation re-delivery must still work when
/// the HOST's workspace restarts after the rotation, before the returning
/// member comes back.
///
/// The host's `current_write_ticket` cell (read by the `NODE_ID`
/// re-delivery) is re-seeded on `host_with`. If it were seeded at a
/// hard-coded epoch 0 instead of the epoch recovered from disk, the
/// `publish_rotate` it sends a returning member would carry epoch 0, and
/// the member's monotonic-epoch guard would drop it as stale — leaving the
/// member writing to the abandoned namespace. So: bob RW, offline across a
/// rotation (evict carol), THEN alice's workspace restarts (daemon stays
/// up, so the session log + state dir persist), THEN bob returns. Bob must
/// regain write on the rotated namespace with no re-grant.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::large_futures, clippy::too_many_lines)]
async fn returning_rw_member_regains_write_after_host_workspace_restart_real_n0() {
    init_tracing();

    let alice_root_d = TempDir::new().unwrap();
    let alice_paths = DaemonPaths::at(alice_root_d.path());
    let alice_root = TempDir::new().unwrap();
    let alice_wstate = TempDir::new().unwrap();

    let bob_root_d = TempDir::new().unwrap();
    let bob_paths = DaemonPaths::at(bob_root_d.path());
    let bob_root = TempDir::new().unwrap();
    let bob_wstate = TempDir::new().unwrap();

    // Carol is the peer alice evicts to force a rotation while bob is
    // offline. Her daemon doesn't restart, so `fresh_state` is fine.
    let carol_daemon_state = fresh_state();
    let carol_paths = DaemonPaths::at(carol_daemon_state.root.path());
    let carol_root = TempDir::new().unwrap();
    let carol_wstate = TempDir::new().unwrap();

    let alice_daemon = phase("spawn alice daemon", spawn_daemon_at(&alice_paths, prod())).await;
    let bob_daemon = phase(
        "spawn bob daemon (initial)",
        spawn_daemon_at(&bob_paths, prod()),
    )
    .await;
    let carol_daemon = phase("spawn carol daemon", spawn_daemon_at(&carol_paths, prod())).await;

    // alice hosts.
    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_daemon_socket(alice_daemon.socket.clone())
        .with_endpoint_setup(prod());
    let (alice_ws, alice_ws_events) = phase(
        "alice Workspace::host_with",
        Workspace::host_with(
            &alice,
            "alice",
            alice_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            alice_cfg,
        ),
    )
    .await
    .expect("host_with");
    drain_ws_events(alice_ws_events);
    let session = alice_ws.session_id();
    let ticket = alice_ws.join_ticket().expect("host ticket").clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // bob joins + RW.
    let bob = Client::connect(&bob_daemon.socket).await.unwrap();
    let bob_peer_id = bob.daemon_peer_id();
    let resp = phase(
        "bob JoinSession (initial)",
        bob.request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: ticket.clone(),
        }),
    )
    .await
    .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");
    let bob_cfg = WorkspaceConfig::default()
        .with_state_dir(bob_wstate.path().to_path_buf())
        .with_daemon_socket(bob_daemon.socket.clone())
        .with_endpoint_setup(prod());
    let (bob_ws, bob_ws_events) = phase(
        "bob Workspace::join_with (initial)",
        Workspace::join_with(
            &bob,
            session,
            bob_root.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            bob_cfg,
        ),
    )
    .await
    .expect("join_with");
    drain_ws_events(bob_ws_events);
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;
    phase(
        "grant bob RW",
        grant_rw_and_wait(
            &alice,
            session,
            bob_peer_id,
            bob_root.path(),
            alice_root.path(),
        ),
    )
    .await;

    // carol joins + RW (she must be RW so eviction triggers a rotation).
    let carol = Client::connect(&carol_daemon.socket).await.unwrap();
    let carol_peer_id = carol.daemon_peer_id();
    let resp = phase(
        "carol JoinSession",
        carol.request(Request::JoinSession {
            display_name: "carol".into(),
            ticket: ticket.clone(),
        }),
    )
    .await
    .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");
    let carol_cfg = WorkspaceConfig::default()
        .with_state_dir(carol_wstate.path().to_path_buf())
        .with_daemon_socket(carol_daemon.socket.clone())
        .with_endpoint_setup(prod());
    let (carol_ws, carol_ws_events) = phase(
        "carol Workspace::join_with",
        Workspace::join_with(
            &carol,
            session,
            carol_root.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            carol_cfg,
        ),
    )
    .await
    .expect("join_with carol");
    drain_ws_events(carol_ws_events);
    let carol_ws = Arc::new(carol_ws);
    let carol_handle = Arc::clone(&carol_ws).run().await;
    phase(
        "grant carol RW",
        grant_rw_and_wait(
            &alice,
            session,
            carol_peer_id,
            carol_root.path(),
            alice_root.path(),
        ),
    )
    .await;

    // Baseline: bob can write pre-rotation.
    tokio::fs::write(bob_root.path().join("pre_bob.txt"), b"pre")
        .await
        .unwrap();
    phase(
        "pre: bob -> alice (genesis namespace)",
        wait_for_file(&alice_root.path().join("pre_bob.txt"), b"pre"),
    )
    .await;

    // Bob goes fully offline (workspace + daemon down).
    phase("bob_ws.shutdown()", bob_ws.shutdown())
        .await
        .expect("shutdown");
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(bob);
    phase("bob_daemon.stop()", bob_daemon.stop()).await;

    // While bob is down, alice evicts carol → namespace rotation (epoch 1).
    phase(
        "alice revokes carol (triggers rotation)",
        revoke(&alice, session, carol_peer_id),
    )
    .await;
    // Let the rotation settle before tearing down alice's workspace.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        alice_ws.namespace_epoch(),
        1,
        "evicting carol should have rotated alice to epoch 1",
    );

    // THE TWIST (finding #1): alice's WORKSPACE restarts after the
    // rotation — daemon stays up (session log persists), state dir
    // persists. The re-hosted workspace must recover the rotated epoch and
    // seed its NODE_ID-redelivery cell with it, not a hard-coded 0.
    phase(
        "alice_ws.shutdown() (pre host-restart)",
        alice_ws.shutdown(),
    )
    .await
    .expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let alice_cfg = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_daemon_socket(alice_daemon.socket.clone())
        .with_endpoint_setup(prod());
    let (alice_ws, alice_ws_events) = phase(
        "alice Workspace::host_with (restart, rotated namespace)",
        Workspace::host_with(
            &alice,
            "alice",
            alice_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            alice_cfg,
        ),
    )
    .await
    .expect("host_with restart");
    drain_ws_events(alice_ws_events);
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;
    assert_eq!(
        alice_ws.namespace_epoch(),
        1,
        "re-hosted alice must recover the rotated epoch from disk, not reset to 0",
    );

    // Bob's daemon restarts and reattaches.
    let bob_daemon = phase(
        "spawn bob daemon (post-restart)",
        spawn_daemon_at(&bob_paths, prod()),
    )
    .await;
    let bob = Client::connect(&bob_daemon.socket).await.unwrap();
    let resp = phase(
        "bob JoinSession (post-restart)",
        bob.request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        }),
    )
    .await
    .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");
    let bob_cfg = WorkspaceConfig::default()
        .with_state_dir(bob_wstate.path().to_path_buf())
        .with_daemon_socket(bob_daemon.socket.clone())
        .with_endpoint_setup(prod());
    let (bob_ws, bob_ws_events) = phase(
        "bob Workspace::join_with (post-restart, rotated namespace)",
        Workspace::join_with(
            &bob,
            session,
            bob_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            bob_cfg,
        ),
    )
    .await
    .expect("join_with post-restart");
    drain_ws_events(bob_ws_events);
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // THE LOAD-BEARING ASSERTION: bob regains write on the rotated
    // namespace and it reaches the re-hosted alice — proving the host's
    // re-seeded cell carried the recovered epoch (not 0), so the
    // re-delivered rotate cleared bob's monotonic-epoch guard. Unique
    // content per tick (echo-guard) + fixed path polled to arrival
    // (round-trip), as in the sibling rotation test.
    let deadline = std::time::Instant::now() + Duration::from_secs(40);
    let target = alice_root.path().join("post_bob.txt");
    let mut tick = 0u32;
    loop {
        let content = format!("post-{tick}");
        let _ = tokio::fs::write(bob_root.path().join("post_bob.txt"), content.as_bytes()).await;
        if let Ok(bytes) = tokio::fs::read(&target).await
            && bytes.starts_with(b"post-")
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "returning RW member never regained write after a host workspace restart",
        );
        tick += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    phase("alice_ws.shutdown()", alice_ws.shutdown())
        .await
        .expect("shutdown");
    phase("bob_ws.shutdown() (final)", bob_ws.shutdown())
        .await
        .expect("shutdown");
    phase("carol_ws.shutdown()", carol_ws.shutdown())
        .await
        .expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    let _ = timeout(Duration::from_secs(5), carol_handle).await;
    drop(alice);
    drop(bob);
    drop(carol);
    phase("alice_daemon.stop()", alice_daemon.stop()).await;
    phase("bob_daemon.stop()", bob_daemon.stop()).await;
    phase("carol_daemon.stop()", carol_daemon.stop()).await;
}

/// Offline READ->WRITE promotion: bob joins Read-only, goes offline, the
/// host grants him RW *while he is down* (no rotation), then his daemon
/// restarts and reattaches. Bob must gain write with no second grant.
///
/// This is the case that motivated host-side detection (decision B in the
/// plan): a peer promoted while offline holds NO prior secret, so a
/// joiner-side "is my secret stale?" check has nothing to compare — only
/// the host knows bob is now RW. It is also exactly the `emit_upgrade`
/// INVARIANT's "offline promotion" shoe; this fix subsumes it.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::large_futures, clippy::too_many_lines)]
async fn offline_read_to_write_promotion_delivers_secret_on_return_real_n0() {
    init_tracing();

    let alice_root_d = TempDir::new().unwrap();
    let alice_paths = DaemonPaths::at(alice_root_d.path());
    let alice_root = TempDir::new().unwrap();
    let alice_wstate = TempDir::new().unwrap();

    let bob_root_d = TempDir::new().unwrap();
    let bob_paths = DaemonPaths::at(bob_root_d.path());
    let bob_root = TempDir::new().unwrap();
    let bob_wstate = TempDir::new().unwrap();

    let alice_daemon = phase(
        "spawn alice daemon",
        spawn_daemon_at(&alice_paths, common::custom_relay_setup().await),
    )
    .await;
    let bob_daemon = phase(
        "spawn bob daemon (initial)",
        spawn_daemon_at(&bob_paths, common::custom_relay_setup().await),
    )
    .await;

    // alice hosts.
    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_daemon_socket(alice_daemon.socket.clone())
        .with_endpoint_setup(common::custom_relay_setup().await);
    let (alice_ws, alice_ws_events) = phase(
        "alice Workspace::host_with",
        Workspace::host_with(
            &alice,
            "alice",
            alice_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            alice_cfg,
        ),
    )
    .await
    .expect("host_with");
    drain_ws_events(alice_ws_events);
    let session = alice_ws.session_id();
    let ticket = alice_ws.join_ticket().expect("host ticket").clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // bob joins READ-ONLY (no grant).
    let bob = Client::connect(&bob_daemon.socket).await.unwrap();
    let bob_peer_id = bob.daemon_peer_id();
    let resp = phase(
        "bob JoinSession (read-only, initial)",
        bob.request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: ticket.clone(),
        }),
    )
    .await
    .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");
    let bob_cfg = WorkspaceConfig::default()
        .with_state_dir(bob_wstate.path().to_path_buf())
        .with_daemon_socket(bob_daemon.socket.clone())
        .with_endpoint_setup(common::custom_relay_setup().await);
    let (bob_ws, bob_ws_events) = phase(
        "bob Workspace::join_with (read-only)",
        Workspace::join_with(
            &bob,
            session,
            bob_root.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            bob_cfg,
        ),
    )
    .await
    .expect("join_with");
    drain_ws_events(bob_ws_events);
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Baseline: alice -> bob read sync works (bob holds Read).
    tokio::fs::write(alice_root.path().join("pre_a.txt"), b"a1")
        .await
        .unwrap();
    phase(
        "pre: alice -> bob (bob is Read-only)",
        wait_for_file(&bob_root.path().join("pre_a.txt"), b"a1"),
    )
    .await;

    // Bob goes fully offline BEFORE any RW grant — he holds no secret.
    phase("bob_ws.shutdown()", bob_ws.shutdown())
        .await
        .expect("shutdown");
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(bob);
    phase("bob_daemon.stop()", bob_daemon.stop()).await;

    // Host grants bob RW while he is down. No rotation: the cap-set just
    // gains bob@RW. The live upgrade delivery has no receiver (bob's
    // gone), so the secret is lost — recovery must happen on his return.
    phase(
        "alice grants bob RW (while offline)",
        grant_rw(&alice, session, bob_peer_id),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Bob's daemon restarts and reattaches.
    let bob_daemon = phase(
        "spawn bob daemon (post-restart)",
        spawn_daemon_at(&bob_paths, common::custom_relay_setup().await),
    )
    .await;
    let bob = Client::connect(&bob_daemon.socket).await.unwrap();
    let resp = phase(
        "bob JoinSession (post-restart)",
        bob.request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        }),
    )
    .await
    .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");
    let bob_cfg = WorkspaceConfig::default()
        .with_state_dir(bob_wstate.path().to_path_buf())
        .with_daemon_socket(bob_daemon.socket.clone())
        .with_endpoint_setup(common::custom_relay_setup().await);
    let (bob_ws, bob_ws_events) = phase(
        "bob Workspace::join_with (post-restart, now RW)",
        Workspace::join_with(
            &bob,
            session,
            bob_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            bob_cfg,
        ),
    )
    .await
    .expect("join_with post-restart");
    drain_ws_events(bob_ws_events);
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // LOAD-BEARING: bob writes and it reaches alice, proving the secret
    // was delivered on his NODE_ID re-announce (host-side detection of a
    // peer it promoted while offline). Unique content per tick (echo-guard)
    // + fixed path polled to arrival (round-trip), as in the rotation test.
    let deadline = std::time::Instant::now() + Duration::from_secs(40);
    let target = alice_root.path().join("promo_bob.txt");
    let mut tick = 0u32;
    loop {
        let content = format!("promo-{tick}");
        let _ = tokio::fs::write(bob_root.path().join("promo_bob.txt"), content.as_bytes()).await;
        if let Ok(bytes) = tokio::fs::read(&target).await
            && bytes.starts_with(b"promo-")
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "offline-promoted member never gained write on return",
        );
        tick += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    phase("alice_ws.shutdown()", alice_ws.shutdown())
        .await
        .expect("shutdown");
    phase("bob_ws.shutdown() (final)", bob_ws.shutdown())
        .await
        .expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    phase("alice_daemon.stop()", alice_daemon.stop()).await;
    phase("bob_daemon.stop()", bob_daemon.stop()).await;
}
