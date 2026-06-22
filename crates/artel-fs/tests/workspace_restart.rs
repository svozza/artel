//! Workspace restart properties: graceful disk resume,
//! re-hosting yields a structurally identical ticket, session-id
//! recovery across daemon restart, host-restart live writes
//! (Tier B + Tier C variants).
//!
//! Consolidated from five per-file bins (`disk_resume`,
//! `host_restart_live_writes`, `host_restart_live_writes_n0`,
//! `host_restart_ticket_stable`, `host_resume_session_id`) per
//! `docs/plans/2026-05-29-faster-cargo-test-plan.md` slice 2d. The
//! `*_real_n0` test fn keeps its `_n0` suffix — the default nextest
//! profile filters it out via `not test(/_n0$/)`; the `n0` profile
//! runs it.

mod common;

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use artel_client::{Client, EventStream};
use artel_fs::{
    AttachPolicy, TICKET_ACTION, Workspace, WorkspaceConfig, session_id_for, ticket as fs_ticket,
};
use artel_protocol::{Event, MessageKind, Request, Response, SessionId};
use iroh::test_utils::DnsPkarrServer;
use iroh_docs::DocTicket;
use tempfile::TempDir;
use tokio::time::timeout;

use common::{
    DaemonPaths, LocalDaemon, Pair, daemon_testing_setup, fresh_state, init_tracing,
    spawn_daemon_at, spawn_daemon_with_setup, spawn_pair, testing_setup, wait_for_file,
    wait_for_missing,
};

const TICKET_BUDGET: Duration = Duration::from_secs(15);

/// Drain `events` until the workspace ticket lands; decode the
/// envelope and parse the inner `DocTicket`. Used by the disk-resume
/// + host-restart-ticket-stable + post-restart tests.
async fn capture_ticket(events: &mut EventStream, session: SessionId) -> DocTicket {
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

// =============================================================
// Workspace state survives a process-graceful restart on both host
// and joiner.
//
// Alice hosts a workspace, Bob joins, both shut down cleanly, both
// come back later (against fresh artel daemons) and pick up where
// they left off — without losing files, without the workspace
// ticket invalidating, and without breaking delete propagation.
//
// Load-bearing pieces:
// - `iroh.key` keeps the host's `EndpointId` / `NodeId` stable.
// - `doc-id` keeps the host's `NamespaceId` stable.
// - `Docs::persistent` + `FsStore` retain doc + blob state on the
//   joiner side.
// - `reconcile_doc_against_disk` propagates a delete that happened
//   *while the host was down* to peers on next start.
// =============================================================

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread")]
async fn workspace_state_survives_graceful_restart() {
    init_tracing();

    // Workspace state dirs and content roots live in tempdirs that
    // outlive the daemons.
    let alice_root = tempfile::tempdir().unwrap();
    let alice_wstate = tempfile::tempdir().unwrap();
    let bob_root = tempfile::tempdir().unwrap();
    let bob_wstate = tempfile::tempdir().unwrap();

    // -----------------------------------------------------------
    // Phase 1: first lifetime of the workspaces.
    // -----------------------------------------------------------
    tokio::fs::write(alice_root.path().join("a.txt"), b"alpha")
        .await
        .unwrap();

    let phase1_ticket = {
        let Pair {
            daemon_a,
            daemon_b,
            dns_pkarr,
        } = spawn_pair().await;

        let alice = Client::connect(&daemon_a.socket).await.unwrap();
        let alice_cfg = WorkspaceConfig::default()
            .with_state_dir(alice_wstate.path().to_path_buf())
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_a.socket.clone());
        let (alice_ws, alice_ws_events) = Workspace::host_with(
            &alice,
            "alice",
            alice_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            alice_cfg,
        )
        .await
        .expect("Workspace::host_with");
        common::drain_ws_events(alice_ws_events);
        let session = alice_ws.session_id();
        let artel_ticket = alice_ws
            .join_ticket()
            .expect("host has join_ticket")
            .clone();

        // Subscribe *after* host returns. The daemon's replay path
        // surfaces the `workspace.ticket` system message published
        // during host even for late subscribers.
        let _ = alice
            .request(Request::Subscribe {
                session,
                since: None,
            })
            .await
            .unwrap();
        let mut alice_events = alice.take_events().await.expect("alice events");

        let alice_ws = Arc::new(alice_ws);
        let alice_handle = Arc::clone(&alice_ws).run().await;

        let phase1_ticket = capture_ticket(&mut alice_events, session).await;

        // Bob joins via the daemon-issued artel ticket.
        let bob = Client::connect(&daemon_b.socket).await.unwrap();
        let bob_peer_id = bob.daemon_peer_id();
        let resp = bob
            .request(Request::JoinSession {
                display_name: "bob".into(),
                ticket: artel_ticket,
            })
            .await
            .unwrap();
        let bob_session = match resp {
            Response::JoinSession { session, .. } => session,
            other => panic!("JoinSession: got {other:?}"),
        };
        assert_eq!(bob_session, session, "joiner must land on same session id");

        let bob_cfg = WorkspaceConfig::default()
            .with_state_dir(bob_wstate.path().to_path_buf())
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_b.socket.clone());
        let (bob_ws, bob_ws_events) = Workspace::join_with(
            &bob,
            session,
            bob_root.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            bob_cfg,
        )
        .await
        .expect("Workspace::join_with");
        common::drain_ws_events(bob_ws_events);
        let bob_ws = Arc::new(bob_ws);
        let bob_handle = Arc::clone(&bob_ws).run().await;

        // Grant Bob RW so he receives the NamespaceSecret needed to write.
        common::grant_rw_and_wait(
            &alice,
            session,
            bob_peer_id,
            bob_root.path(),
            alice_root.path(),
        )
        .await;

        // Sanity: a.txt makes it to bob.
        wait_for_file(&bob_root.path().join("a.txt"), b"alpha").await;

        alice_ws.shutdown().await.expect("shutdown");
        bob_ws.shutdown().await.expect("shutdown");
        let _ = timeout(Duration::from_secs(5), alice_handle).await;
        let _ = timeout(Duration::from_secs(5), bob_handle).await;
        drop(alice_events);
        drop(alice);
        drop(bob);
        daemon_a.stop().await;
        daemon_b.stop().await;

        phase1_ticket
    };

    // Workspace state survived the shutdown.
    assert!(
        alice_wstate.path().join("iroh.key").exists(),
        "alice iroh.key should persist"
    );
    assert!(
        alice_wstate.path().join("doc-id").exists(),
        "alice doc-id should persist"
    );
    // Identity decoupling (Slice 2): with no rotation yet, the current
    // namespace IS the genesis, so no `current-namespace` file is
    // written (absent ⇒ equals genesis). A spuriously-written file
    // here would mean the decoupling logic diverged genesis from
    // current without a rotation.
    assert!(
        !alice_wstate.path().join("current-namespace").exists(),
        "no rotation yet ⇒ current-namespace must be absent (equals genesis)",
    );
    // Likewise the epoch file is only written on rotation (C2); at
    // genesis it must be absent (absent ⇒ epoch 0).
    assert!(
        !alice_wstate.path().join("namespace-epoch").exists(),
        "no rotation yet ⇒ namespace-epoch must be absent (equals genesis epoch 0)",
    );
    assert!(
        bob_wstate.path().join("iroh.key").exists(),
        "bob iroh.key should persist"
    );
    assert!(
        !bob_wstate.path().join("doc-id").exists(),
        "joiners must not write doc-id (host owns the namespace)",
    );

    // Between-lifetimes mutation: delete a.txt from alice's disk
    // while alice is offline. The reconcile pass on the next host
    // restart should propagate the delete to bob.
    tokio::fs::remove_file(alice_root.path().join("a.txt"))
        .await
        .unwrap();

    // -----------------------------------------------------------
    // Phase 2: fresh daemons, same workspace state dirs.
    // -----------------------------------------------------------
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_endpoint_setup(testing_setup(&dns_pkarr))
        .with_daemon_socket(daemon_a.socket.clone());
    let (alice_ws, alice_ws_events) = Workspace::host_with(
        &alice,
        "alice",
        alice_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        alice_cfg,
    )
    .await
    .expect("Workspace::host_with phase 2");
    common::drain_ws_events(alice_ws_events);
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

    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let phase2_ticket = capture_ticket(&mut alice_events, session).await;

    // Identity stability: NamespaceId stable across restarts, host
    // NodeId stable across restarts.
    assert_eq!(
        phase1_ticket.capability.id(),
        phase2_ticket.capability.id(),
        "NamespaceId must be stable across host restart",
    );
    let nodes_1: Vec<_> = phase1_ticket.nodes.iter().map(|n| n.id).collect();
    let nodes_2: Vec<_> = phase2_ticket.nodes.iter().map(|n| n.id).collect();
    assert_eq!(
        nodes_1, nodes_2,
        "host NodeId(s) must be stable across host restart",
    );

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: artel_ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_cfg = WorkspaceConfig::default()
        .with_state_dir(bob_wstate.path().to_path_buf())
        .with_endpoint_setup(testing_setup(&dns_pkarr))
        .with_daemon_socket(daemon_b.socket.clone());
    let (bob_ws, bob_ws_events) = Workspace::join_with(
        &bob,
        session,
        bob_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        bob_cfg,
    )
    .await
    .expect("Workspace::join_with phase 2");
    common::drain_ws_events(bob_ws_events);
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Reconcile-driven delete propagates to bob. This proves the
    // initial doc-sync (bob→alice import path) succeeded.
    phase(
        "p2: a.txt delete propagates to bob",
        wait_for_missing(&bob_root.path().join("a.txt")),
    )
    .await;

    // Gate on bidirectional gossip: bob writes a probe that alice
    // must see. This blocks until iroh-docs' gossip neighbors are
    // mutually registered — without it, alice's subsequent writes
    // race the NeighborUp event and may never gossip-broadcast.
    tokio::fs::write(bob_root.path().join(".sync_probe"), b"ok")
        .await
        .unwrap();
    phase(
        "p2: bob→alice gossip probe",
        wait_for_file(&alice_root.path().join(".sync_probe"), b"ok"),
    )
    .await;

    // Live sync resumed both ways.
    tokio::fs::write(alice_root.path().join("b.txt"), b"beta")
        .await
        .unwrap();
    phase(
        "p2: b.txt reaches bob",
        wait_for_file(&bob_root.path().join("b.txt"), b"beta"),
    )
    .await;

    tokio::fs::write(bob_root.path().join("c.txt"), b"charlie")
        .await
        .unwrap();
    phase(
        "p2: c.txt reaches alice",
        wait_for_file(&alice_root.path().join("c.txt"), b"charlie"),
    )
    .await;

    // Delete after restart still propagates.
    tokio::fs::remove_file(alice_root.path().join("b.txt"))
        .await
        .unwrap();
    phase(
        "p2: b.txt delete propagates to bob",
        wait_for_missing(&bob_root.path().join("b.txt")),
    )
    .await;

    alice_ws.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice_events);
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Regression trap for the chat-harness "alice's messages stop
// reaching bob after alice's restart" bug observed during the
// lost-message investigation.
// =============================================================

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread")]
async fn alice_post_restart_writes_reach_bob() {
    // Alice's daemon-state dir survives restart; the `RunningDaemon`
    // in `Pair` owns its own tempdir which gets wiped on stop, so we
    // don't use the convenience harness for alice. Bob keeps the
    // standard harness because his daemon doesn't restart.
    let alice_daemon_root = TempDir::new().unwrap();
    let alice_paths = DaemonPaths::at(alice_daemon_root.path());
    let alice_root = TempDir::new().unwrap();
    let alice_wstate = TempDir::new().unwrap();
    let bob_root = TempDir::new().unwrap();
    let bob_wstate = TempDir::new().unwrap();

    // Phase 1: bring up the shared `DnsPkarrServer` directly, then
    // spawn alice's daemon at caller-owned paths so the same on-disk
    // state (including iroh secret) survives the mid-test restart.
    // Bob's daemon uses `fresh_state()` since it doesn't restart.
    let dns_pkarr = Arc::new(DnsPkarrServer::run_with_origin(artel_fs::TEST_DNS_ORIGIN.to_string()).await.expect("DnsPkarrServer::run"));
    let alice_daemon = spawn_daemon_at(&alice_paths, daemon_testing_setup(&dns_pkarr)).await;
    let bob_daemon = spawn_daemon_with_setup(fresh_state(), daemon_testing_setup(&dns_pkarr)).await;

    // Phase 1: alice hosts, bob joins, exchange one file each way to
    // confirm baseline propagation before any restarts.
    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_endpoint_setup(testing_setup(&dns_pkarr))
        .with_daemon_socket(alice_daemon.socket.clone());
    let (alice_ws, alice_ws_events) = Workspace::host_with(
        &alice,
        "alice",
        alice_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        alice_cfg,
    )
    .await
    .expect("Workspace::host_with phase 1");
    common::drain_ws_events(alice_ws_events);
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
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&bob_daemon.socket).await.unwrap();
    let bob_peer_id = bob.daemon_peer_id();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: artel_ticket,
        })
        .await
        .unwrap();
    let bob_session = match resp {
        Response::JoinSession { session, .. } => session,
        other => panic!("JoinSession: got {other:?}"),
    };
    assert_eq!(bob_session, session, "joiner must land on same session id");

    let bob_cfg = WorkspaceConfig::default()
        .with_state_dir(bob_wstate.path().to_path_buf())
        .with_endpoint_setup(testing_setup(&dns_pkarr))
        .with_daemon_socket(bob_daemon.socket.clone());
    let (bob_ws, bob_ws_events) = Workspace::join_with(
        &bob,
        session,
        bob_root.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        bob_cfg,
    )
    .await
    .expect("Workspace::join_with");
    common::drain_ws_events(bob_ws_events);
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Grant Bob RW so he receives the NamespaceSecret needed to write.
    common::grant_rw_and_wait(
        &alice,
        session,
        bob_peer_id,
        bob_root.path(),
        alice_root.path(),
    )
    .await;

    // Pre-restart bidirectional sanity.
    tokio::fs::write(alice_root.path().join("pre_alice.txt"), b"alpha")
        .await
        .unwrap();
    wait_for_file(&bob_root.path().join("pre_alice.txt"), b"alpha").await;

    tokio::fs::write(bob_root.path().join("pre_bob.txt"), b"bravo")
        .await
        .unwrap();
    wait_for_file(&alice_root.path().join("pre_bob.txt"), b"bravo").await;

    // Phase 2: alice's side goes down. Bob stays alive.
    alice_ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    drop(alice_events);
    drop(alice);
    alice_daemon.stop().await;

    // Phase 3: alice respawns. Same daemon-state dir, same workspace
    // state dir, same root.
    let alice_daemon = spawn_daemon_at(&alice_paths, daemon_testing_setup(&dns_pkarr)).await;
    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_endpoint_setup(testing_setup(&dns_pkarr))
        .with_daemon_socket(alice_daemon.socket.clone());
    let (alice_ws, alice_ws_events) = Workspace::host_with(
        &alice,
        "alice",
        alice_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        alice_cfg,
    )
    .await
    .expect("Workspace::host_with phase 2");
    common::drain_ws_events(alice_ws_events);
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // Phase 4: bob → alice direction first.
    tokio::fs::write(bob_root.path().join("post_restart_bob.txt"), b"charlie")
        .await
        .unwrap();
    wait_for_file(&alice_root.path().join("post_restart_bob.txt"), b"charlie").await;

    // Phase 5: alice → bob. THE LOAD-BEARING ASSERTION.
    tokio::fs::write(alice_root.path().join("post_restart_alice.txt"), b"delta")
        .await
        .unwrap();
    wait_for_file(&bob_root.path().join("post_restart_alice.txt"), b"delta").await;

    // Cleanup.
    alice_ws.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
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
}

// =============================================================
// Real-n0 variant of `alice_post_restart_writes_reach_bob`.
//
// The DnsPkarrServer-backed sibling above passes — meaning whatever's
// breaking in the chat harness either (a) is specific to the n0
// production discovery / relay path, or (b) is a harness-side issue
// that the substrate doesn't have. This test runs the same shape
// against full n0 to distinguish those two.
//
// Why this is its own test fn (not deleted): n0 pkarr+DNS rate-limits
// under back-to-back test load, so this test stays Tier C. Default
// nextest profile filters it out via `not test(/_n0$/)`. The Tier B
// sibling above covers the substrate property; this one is the
// production-path canary.
// =============================================================

const PHASE_BUDGET: Duration = Duration::from_secs(45);

/// Bound one phase of the restart test (see `common::phase_budgeted`).
async fn phase<F, T>(name: &'static str, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    common::phase_budgeted(name, PHASE_BUDGET, fut).await
}

/// Real-n0 capture-ticket: same shape as the top-level helper but with
/// a 30s ticket budget — n0's pkarr+DNS publish has measurably longer
/// tail-latency than the localhost fixture.
async fn capture_ticket_n0(events: &mut EventStream, session: SessionId) -> DocTicket {
    const TICKET_BUDGET_N0: Duration = Duration::from_secs(30);
    let payload = timeout(TICKET_BUDGET_N0, async {
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

// Real-n0 sibling for finding #5c (host-restart peer-addr cache).
// The deterministic counterpart is the `peer_addr_cache_pkarr`
// test fn in `crates/artel-daemon/tests/identity.rs`. Runs under
// the `n0` nextest profile (filter `test(/_n0$/)`); the default
// profile filters it out via `not test(/_n0$/)`.
//
// Dials across the localhost shared relay (`ProductionCustomRelay`):
// n0 DNS/pkarr for discovery, self-signed localhost relay for the
// QUIC transport. Previously this dialed n0's public relay to dodge
// the noq-proto 0.17.0 handshake path-poisoning bug (diagnosed
// 2026-06-11; full writeup in docs/diagnosing-flaky-tests.md), which
// deterministically wedged acceptor-side handshakes against a
// localhost relay. Fixed upstream in noq-proto 1.0.0 (handshake
// check uses is_probably_same_path; local-IP learning gated on
// handshake-confirmed; iroh#4273/#4281 four-tuple rework), shipped
// with the iroh 1.0 upgrade, so Tier C no longer depends on n0's
// public relay.

/// Daemon-side localhost-relay setup for the Tier C test below.
async fn daemon_custom_relay_setup() -> artel_daemon::EndpointSetup {
    let relay_url: iroh::RelayUrl = common::shared_relay_url().await.parse().unwrap();
    artel_daemon::EndpointSetup::ProductionCustomRelay { relay_url }
}

/// Workspace-side companion to [`daemon_custom_relay_setup`].
async fn fs_custom_relay_setup() -> artel_fs::EndpointSetup {
    let relay_url: iroh::RelayUrl = common::shared_relay_url().await.parse().unwrap();
    artel_fs::EndpointSetup::ProductionCustomRelay { relay_url }
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::large_futures, clippy::too_many_lines)]
async fn alice_post_restart_writes_reach_bob_real_n0() {
    init_tracing();

    let alice_daemon_root = TempDir::new().unwrap();
    let alice_paths = DaemonPaths::at(alice_daemon_root.path());
    let alice_root = TempDir::new().unwrap();
    let alice_wstate = TempDir::new().unwrap();
    let bob_root = TempDir::new().unwrap();
    let bob_wstate = TempDir::new().unwrap();

    let alice_daemon = phase(
        "spawn alice daemon (initial)",
        spawn_daemon_at(&alice_paths, daemon_custom_relay_setup().await),
    )
    .await;
    let bob_daemon_state = fresh_state();
    let bob_paths = DaemonPaths::at(bob_daemon_state.root.path());
    let bob_daemon = phase(
        "spawn bob daemon",
        spawn_daemon_at(&bob_paths, daemon_custom_relay_setup().await),
    )
    .await;

    // Phase 1: alice hosts, bob joins, exchange one file each way.
    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_daemon_socket(alice_daemon.socket.clone())
        .with_endpoint_setup(fs_custom_relay_setup().await);
    let (alice_ws, alice_ws_events) = phase(
        "alice Workspace::host_with (phase 1)",
        Workspace::host_with(
            &alice,
            "alice",
            alice_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            alice_cfg,
        ),
    )
    .await
    .expect("Workspace::host_with phase 1");
    common::drain_ws_events(alice_ws_events);
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
    let _phase1_ticket = phase(
        "capture alice's TICKET_ACTION over IPC events",
        capture_ticket_n0(&mut alice_events, session),
    )
    .await;

    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&bob_daemon.socket).await.unwrap();
    let bob_peer_id = bob.daemon_peer_id();
    let resp = phase(
        "bob JoinSession over IPC (gossip subscribe + JOIN_READY)",
        bob.request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: artel_ticket,
        }),
    )
    .await
    .unwrap();
    let bob_session = match resp {
        Response::JoinSession { session, .. } => session,
        other => panic!("JoinSession: got {other:?}"),
    };
    assert_eq!(bob_session, session, "joiner must land on same session id");

    let bob_cfg = WorkspaceConfig::default()
        .with_state_dir(bob_wstate.path().to_path_buf())
        .with_daemon_socket(bob_daemon.socket.clone())
        .with_endpoint_setup(fs_custom_relay_setup().await);
    let (bob_ws, bob_ws_events) = phase(
        "bob Workspace::join_with (doc import + bulk_export)",
        Workspace::join_with(
            &bob,
            session,
            bob_root.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            bob_cfg,
        ),
    )
    .await
    .expect("Workspace::join_with");
    common::drain_ws_events(bob_ws_events);
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Grant Bob RW so he receives the NamespaceSecret via the upgrade
    // path and can produce valid signed entries.
    common::grant_rw_and_wait(
        &alice,
        session,
        bob_peer_id,
        bob_root.path(),
        alice_root.path(),
    )
    .await;

    tokio::fs::write(alice_root.path().join("pre_alice.txt"), b"alpha")
        .await
        .unwrap();
    phase(
        "wait for pre_alice.txt to reach bob",
        wait_for_file(&bob_root.path().join("pre_alice.txt"), b"alpha"),
    )
    .await;

    tokio::fs::write(bob_root.path().join("pre_bob.txt"), b"bravo")
        .await
        .unwrap();
    phase(
        "wait for pre_bob.txt to reach alice",
        wait_for_file(&alice_root.path().join("pre_bob.txt"), b"bravo"),
    )
    .await;

    // Phase 2: alice's side goes down. Bob stays alive throughout.
    phase("alice_ws.shutdown() (phase 1)", alice_ws.shutdown())
        .await
        .expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    drop(alice_events);
    drop(alice);
    phase("alice_daemon.stop() (phase 1)", alice_daemon.stop()).await;

    // Phase 3: alice respawns at the same daemon-state and
    // workspace-state dirs.
    let alice_daemon = phase(
        "spawn alice daemon (post-restart)",
        spawn_daemon_at(&alice_paths, daemon_custom_relay_setup().await),
    )
    .await;
    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_daemon_socket(alice_daemon.socket.clone())
        .with_endpoint_setup(fs_custom_relay_setup().await);
    let (alice_ws, alice_ws_events) = phase(
        "alice Workspace::host_with (phase 2 — post-restart)",
        Workspace::host_with(
            &alice,
            "alice",
            alice_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            alice_cfg,
        ),
    )
    .await
    .expect("Workspace::host_with phase 2");
    common::drain_ws_events(alice_ws_events);
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // Phase 4: bob → alice.
    tokio::fs::write(bob_root.path().join("post_restart_bob.txt"), b"charlie")
        .await
        .unwrap();
    phase(
        "wait for post_restart_bob.txt to reach alice (bob → alice post-restart)",
        wait_for_file(&alice_root.path().join("post_restart_bob.txt"), b"charlie"),
    )
    .await;

    // Phase 5: alice → bob. THE LOAD-BEARING ASSERTION.
    tokio::fs::write(alice_root.path().join("post_restart_alice.txt"), b"delta")
        .await
        .unwrap();
    phase(
        "wait for post_restart_alice.txt to reach bob (alice → bob post-restart, LOAD-BEARING)",
        wait_for_file(&bob_root.path().join("post_restart_alice.txt"), b"delta"),
    )
    .await;

    // Cleanup.
    phase("alice_ws.shutdown() (final)", alice_ws.shutdown())
        .await
        .expect("shutdown");
    phase("bob_ws.shutdown() (final)", bob_ws.shutdown())
        .await
        .expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    phase("alice_daemon.stop() (final)", alice_daemon.stop()).await;
    phase("bob_daemon.stop() (final)", bob_daemon.stop()).await;
    drop(alice_daemon_root);
    drop(alice_root);
    drop(alice_wstate);
    drop(bob_root);
    drop(bob_wstate);
    drop(bob_daemon_state);
}

// =============================================================
// Re-hosting the same workspace dir produces a structurally
// identical ticket: same `NamespaceId`, same host `NodeId(s)`. This
// is what lets existing joiners' tickets keep working across host
// restart.
//
// `workspace_state_survives_graceful_restart` above already asserts
// the same properties inside a larger two-daemon end-to-end
// scenario. This test scopes them to the host side only — no
// joiner, no daemon swap, no live sync — so a regression in just
// the resume-ticket-stability property surfaces fast (~2s) with an
// unambiguous failure mode.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn re_hosting_same_dir_yields_structurally_identical_ticket() {
    // The resume-stability property is about per-workspace iroh
    // state, not the daemon's iroh layer, so a `LocalDaemon`
    // (iroh-disabled) is sufficient.
    let harness = LocalDaemon::spawn().await;

    // Workspace dir outlives both phases. The state dir
    // (`<root>/.artel-fs/`) is what carries `iroh.key` + `doc-id`
    // across the host/re-host boundary.
    let ws_dir = tempfile::tempdir().unwrap();

    let phase1 = host_once_and_capture_ticket(&harness, ws_dir.path().to_path_buf()).await;
    let phase2 = host_once_and_capture_ticket(&harness, ws_dir.path().to_path_buf()).await;

    // NamespaceId stable: existing joiners' tickets keep referring to
    // the same doc.
    assert_eq!(
        phase1.capability.id(),
        phase2.capability.id(),
        "NamespaceId must be stable across host restart",
    );

    // Host NodeId(s) stable: joiners can still dial the host.
    let nodes_1: Vec<_> = phase1.nodes.iter().map(|n| n.id).collect();
    let nodes_2: Vec<_> = phase2.nodes.iter().map(|n| n.id).collect();
    assert_eq!(
        nodes_1, nodes_2,
        "host NodeId(s) must be stable across host restart",
    );

    harness.stop().await;
}

/// Stand up a workspace, capture the published ticket, shut down.
async fn host_once_and_capture_ticket(harness: &LocalDaemon, root: PathBuf) -> DocTicket {
    let alice = Client::connect(&harness.socket).await.unwrap();
    let dns_pkarr = common::shared_dns_pkarr().await;

    let (workspace, _ws_events) = Workspace::host_with(
        &alice,
        "alice",
        root,
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::host_with");
    let session = workspace.session_id();

    // Subscribe *after* host returns. The daemon's replay path
    // surfaces the workspace.ticket system message published during
    // host even for late subscribers.
    let _ = alice
        .request(Request::Subscribe {
            session,
            since: None,
        })
        .await
        .unwrap();
    let mut events = alice.take_events().await.expect("events");

    let ticket = capture_ticket(&mut events, session).await;

    workspace.shutdown().await.expect("shutdown");
    drop(events);
    drop(alice);
    ticket
}

// =============================================================
// Re-hosting the same workspace dir under a fresh host daemon
// recovers the same `SessionId` and lets an existing joiner keep
// receiving messages from the host across the host's daemon restart.
//
// This is the user-visible payoff of `Workspace::host_with` deriving
// the session id from the local `NamespaceId` (sub-slice 1c) on top
// of `Registry::host` resuming on `Some(id)` (sub-slice 1b). Without
// either piece, Alice's restart would mint a fresh session id and
// Bob's mirror — still subscribed to the *old* gossip topic — would
// go silent.
// =============================================================

// `used_underscore_binding`: the test pulls paths out of
// `RunningDaemon._state` to rebuild a fresh `DaemonState` for the
// second daemon. Renaming the field would ripple through every
// fixture caller; an allow here is the smallest concession.
#[allow(clippy::too_many_lines, clippy::used_underscore_binding)]
#[tokio::test(flavor = "multi_thread")]
async fn re_hosting_recovers_session_id_and_resumes_message_flow() {
    let dns_pkarr = Arc::new(DnsPkarrServer::run_with_origin(artel_fs::TEST_DNS_ORIGIN.to_string()).await.unwrap());

    // Alice's persistent state: workspace root, workspace state dir,
    // and daemon state (iroh.key + sessions). All three outlive the
    // first daemon so the second daemon picks them up.
    let alice_root = TempDir::new().unwrap();
    let alice_wstate = TempDir::new().unwrap();
    let alice_daemon_state = fresh_state();

    // Bob's daemon and workspace are alive for the whole test — only
    // Alice's daemon restarts.
    let bob_daemon_state = fresh_state();
    let bob_root = TempDir::new().unwrap();
    let bob_wstate = TempDir::new().unwrap();

    // ---------------------------------------------------------------
    // Phase 1: Alice on daemon A1, Bob on daemon B. Live sync works.
    // ---------------------------------------------------------------
    let daemon_b =
        spawn_daemon_with_setup(bob_daemon_state, daemon_testing_setup(&dns_pkarr)).await;
    let daemon_a1 =
        spawn_daemon_with_setup(alice_daemon_state, daemon_testing_setup(&dns_pkarr)).await;

    let alice_a1 = Client::connect(&daemon_a1.socket).await.unwrap();
    let alice_cfg = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_endpoint_setup(testing_setup(&dns_pkarr));
    let (alice_ws_1, alice_events_1) = Workspace::host_with(
        &alice_a1,
        "alice",
        alice_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        alice_cfg,
    )
    .await
    .expect("Workspace::host_with phase 1");
    common::drain_ws_events(alice_events_1);
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
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_cfg = WorkspaceConfig::default()
        .with_state_dir(bob_wstate.path().to_path_buf())
        .with_endpoint_setup(testing_setup(&dns_pkarr));
    let (bob_ws, bob_events) = Workspace::join_with(
        &bob,
        session_id_1,
        bob_root.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        bob_cfg,
    )
    .await
    .expect("Workspace::join_with phase 1");
    common::drain_ws_events(bob_events);
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Sanity: phase-1 propagation works.
    tokio::fs::write(alice_root.path().join("phase1.txt"), b"before-restart")
        .await
        .unwrap();
    wait_for_file(&bob_root.path().join("phase1.txt"), b"before-restart").await;

    // ---------------------------------------------------------------
    // Tear down Alice's daemon. Keep Bob's daemon + workspace alive.
    // ---------------------------------------------------------------
    alice_ws_1.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle_1).await;
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

    // ---------------------------------------------------------------
    // Phase 2: fresh daemon A2 against Alice's same state dir.
    // ---------------------------------------------------------------
    let daemon_a2 =
        spawn_daemon_with_setup(alice_daemon_state_2, daemon_testing_setup(&dns_pkarr)).await;

    let alice_a2 = Client::connect(&daemon_a2.socket).await.unwrap();
    let alice_cfg_2 = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_endpoint_setup(testing_setup(&dns_pkarr));
    let (alice_ws_2, alice_events_2) = Workspace::host_with(
        &alice_a2,
        "alice",
        alice_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        alice_cfg_2,
    )
    .await
    .expect("Workspace::host_with phase 2");
    common::drain_ws_events(alice_events_2);

    // The whole point of this slice: same session id across the
    // restart.
    assert_eq!(
        alice_ws_2.session_id(),
        session_id_1,
        "re-hosting same workspace dir must recover the same session id",
    );

    // Belt-and-braces: the underlying NamespaceId must also be the
    // same.
    assert_eq!(
        alice_ws_2.doc().id(),
        alice_ws_1.doc().id(),
        "re-hosting same workspace dir must recover the same NamespaceId",
    );

    let alice_ws_2 = Arc::new(alice_ws_2);
    let alice_handle_2 = Arc::clone(&alice_ws_2).run().await;

    // Load-bearing: Bob's mirror, still subscribed to the gossip
    // topic derived from `session_id_1`, must see a fresh write from
    // Alice's reincarnated workspace.
    tokio::fs::write(alice_root.path().join("phase2.txt"), b"after-restart")
        .await
        .unwrap();
    wait_for_file(&bob_root.path().join("phase2.txt"), b"after-restart").await;

    alice_ws_2.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle_2).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice_a2);
    drop(bob);
    daemon_a2.stop().await;
    daemon_b.stop().await;
}
