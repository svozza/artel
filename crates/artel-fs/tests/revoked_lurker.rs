//! Lurker regression suite: a bearer of a revoked (or expired)
//! artel-session ticket is refused admission but must ALSO end up
//! with **no file content and no doc replica** after driving the
//! full `Workspace::join_with` flow.
//!
//! Written FIRST against the broadcast-ticket code (2026-06-12
//! revoked-lurker plan): today the host broadcasts the
//! read-capability `WorkspaceTicketEnvelope` on the session log and
//! `run_host_replay` serves the backlog to any topic subscriber, so
//! both tests FAIL — the lurker materialises a live read replica.
//! The unicast-ticket slices (1–4) turn them green: the envelope is
//! delivered host→peer over direct QUIC at admission only, and the
//! Replay path is membership-gated.

#![cfg(feature = "test-utils")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, Workspace, WorkspaceConfig};
use artel_protocol::{Capability, JoinTicket, Request, Response, SessionId, TicketId};
use iroh::test_utils::DnsPkarrServer;
use tokio::time::timeout;

use common::{Pair, RunningDaemon, spawn_pair, testing_setup};

/// How long the lurker's `join_with` is allowed to wait for the
/// ticket. Long enough that the gossip mesh + replay round-trip
/// (~1–2 s in the hermetic fixture) would have delivered the
/// broadcast envelope many times over if the leak were still open;
/// short enough to keep the red test snappy.
const LURK_TICKET_BUDGET: Duration = Duration::from_secs(8);

/// After `join_with` gives up, how long we keep watching the
/// lurker's dir for late file materialisation (doc sync runs on its
/// own tasks; the leak can land bytes after the join future
/// resolves).
const LATE_MATERIALISE_BUDGET: Duration = Duration::from_secs(3);

const SEED_FILE: &str = "secret-plans.txt";
const SEED_CONTENT: &[u8] = b"the lurker must never read this";

/// Host a workspace with one seed file on daemon A and return
/// everything the lurker scenario needs.
struct LurkerScenario {
    daemon_a: RunningDaemon,
    daemon_c: RunningDaemon,
    dns_pkarr: Arc<DnsPkarrServer>,
    alice: Client,
    alice_ws: Arc<Workspace>,
    alice_handle: tokio::task::JoinHandle<()>,
    _alice_dir: tempfile::TempDir,
    session: SessionId,
}

async fn stand_up_host() -> LurkerScenario {
    common::init_tracing();
    let Pair {
        daemon_a,
        daemon_b: daemon_c,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_dir = tempfile::tempdir().unwrap();
    tokio::fs::write(alice_dir.path().join(SEED_FILE), SEED_CONTENT)
        .await
        .unwrap();

    let (alice_ws, alice_rx) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_a.socket.clone()),
    )
    .await
    .expect("Workspace::host");
    common::drain_ws_events(alice_rx);
    let session = alice_ws.session_id();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    LurkerScenario {
        daemon_a,
        daemon_c,
        dns_pkarr,
        alice,
        alice_ws,
        alice_handle,
        _alice_dir: alice_dir,
        session,
    }
}

impl LurkerScenario {
    /// Issue a ticket on the host with the given cap + expiry.
    async fn issue(&self, cap: Capability, expiry_ms: u64) -> (JoinTicket, TicketId) {
        match self
            .alice
            .request(Request::IssueTicket {
                session: self.session,
                granted_cap: cap,
                expiry_ms,
            })
            .await
            .unwrap()
        {
            Response::IssuedTicket { ticket, ticket_id } => (ticket, ticket_id),
            other => panic!("expected IssuedTicket, got {other:?}"),
        }
    }

    async fn revoke(&self, ticket_id: TicketId) {
        match self
            .alice
            .request(Request::RevokeTicket {
                session: self.session,
                ticket_id,
            })
            .await
            .unwrap()
        {
            Response::TicketRevoked => {}
            other => panic!("expected TicketRevoked, got {other:?}"),
        }
    }

    async fn teardown(self) {
        self.alice_ws.shutdown().await.expect("shutdown");
        let _ = timeout(Duration::from_secs(5), self.alice_handle).await;
        drop(self.alice);
        self.daemon_a.stop().await;
        self.daemon_c.stop().await;
    }
}

/// Drive the full lurk flow on `daemon_c` with `bad_ticket` and
/// assert the bearer ends with nothing: no `workspace.ticket`
/// envelope (`join_with` times out), no file content on disk, and no
/// doc replica in its docs store.
async fn assert_lurker_gets_nothing(scenario: &LurkerScenario, bad_ticket: JoinTicket) {
    // The lurker's local daemon accepts the join (it can't know the
    // ticket is revoked/expired — only the host's ledger does) and
    // subscribes to the session's gossip topic. That subscription is
    // the lurk: the topic id is derived from the session id in the
    // ticket, no admission required.
    let carol = Client::connect(&scenario.daemon_c.socket).await.unwrap();
    let join_resp = carol
        .request(Request::JoinSession {
            display_name: "carol-lurker".into(),
            ticket: bad_ticket,
        })
        .await
        .unwrap();
    assert!(
        matches!(join_resp, Response::JoinSession { .. }),
        "local join (mirror materialisation) should succeed: {join_resp:?}",
    );

    let carol_dir = tempfile::tempdir().unwrap();
    let join_result = Workspace::join_with(
        &carol,
        scenario.session,
        carol_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&scenario.dns_pkarr))
            .with_join_ticket_timeout(Some(LURK_TICKET_BUDGET)),
    )
    .await;

    // The decisive assertion: the lurker must NOT obtain a workspace.
    // Against the broadcast code this join succeeds (the replayed
    // workspace.ticket envelope arrives over the topic) — that is
    // the leak.
    match join_result {
        Err(err) => {
            // Expected post-fix: the ticket never arrives; join_with
            // times out (or the event stream surfaces no envelope).
            tracing::info!(%err, "lurker join refused as expected");
        }
        Ok((ws, _rx)) => {
            // Leak path: clean up the workspace before panicking so
            // the drop bomb doesn't add noise on top of the failure.
            let _ = ws.shutdown().await;
            panic!(
                "LEAK: revoked/expired-ticket lurker obtained a workspace \
                 (the workspace.ticket envelope reached a non-member)",
            );
        }
    }

    // Defence in depth: even with join_with refused, watch for late
    // file materialisation, then assert no user file ever landed.
    tokio::time::sleep(LATE_MATERIALISE_BUDGET).await;
    let mut entries = tokio::fs::read_dir(carol_dir.path()).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let name = entry.file_name();
        let s = name.to_string_lossy().to_string();
        assert_eq!(
            s, ".artel-fs",
            "unexpected entry in lurker dir after refused join: {s}",
        );
    }
    let seed_on_disk = carol_dir.path().join(SEED_FILE);
    assert!(
        !seed_on_disk.exists(),
        "LEAK: seed file materialised in the lurker's dir",
    );

    // And no doc replica: stand a fresh workspace node up over the
    // same state dir and list its namespaces. The failed join_with
    // rolled its node back, so the docs store is free to reopen.
    // (If join_with succeeded above we already panicked.)
    let docs_db = carol_dir
        .path()
        .join(".artel-fs")
        .join("docs")
        .join("docs.redb");
    if docs_db.exists() {
        let mut store =
            iroh_docs::store::Store::persistent(&docs_db).expect("open lurker docs store");
        let namespaces: Vec<_> = store
            .list_namespaces()
            .expect("list_namespaces")
            .collect::<Result<Vec<_>, _>>()
            .expect("namespace entries");
        assert!(
            namespaces.is_empty(),
            "LEAK: lurker's docs store holds a replica: {namespaces:?}",
        );
    }

    drop(carol);
}

// =============================================================
// A revoked-ticket bearer lurks through the full join flow and
// must end with NO file content on disk and NO doc replica.
// Against the pre-fix broadcast code this test FAILS — the
// lurker gets both.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn revoked_ticket_lurker_gets_no_replica() {
    let scenario = stand_up_host().await;

    let (dead_ticket, dead_id) = scenario.issue(Capability::Read, 0).await;
    scenario.revoke(dead_id).await;

    assert_lurker_gets_nothing(&scenario, dead_ticket).await;

    scenario.teardown().await;
}

// =============================================================
// Mirror coverage: an EXPIRED ticket bearer drives the same lurk
// flow and must end with nothing. Identical hole pre-fix.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn expired_ticket_lurker_gets_no_replica() {
    let scenario = stand_up_host().await;

    // expiry_ms = 1 → already expired at admission time.
    let (expired_ticket, _id) = scenario.issue(Capability::Read, 1).await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert_lurker_gets_nothing(&scenario, expired_ticket).await;

    scenario.teardown().await;
}

// =============================================================
// Positive-path mirror: a VALID Read-tier ticket bearer — same
// flow, same daemons, only the ticket differs — receives the
// envelope via unicast (admission delivery + mirror-persisted
// Subscribe replay) and syncs the files. This is also the
// late-attach shape: the workspace attaches strictly AFTER the
// daemon-level join was admitted (we gate on the host observing
// PeerJoined), so the envelope can only come from the persisted
// mirror copy injected into the Subscribe replay — the live
// unicast event fired before any workspace subscriber existed.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn valid_read_ticket_joiner_late_attach_gets_files() {
    let scenario = stand_up_host().await;

    let (read_ticket, _id) = scenario.issue(Capability::Read, 0).await;

    // Observe admissions on the host side.
    let observer = Client::connect(&scenario.daemon_a.socket).await.unwrap();
    observer
        .request(Request::Subscribe {
            session: scenario.session,
            since: None,
        })
        .await
        .unwrap();
    let mut host_events = observer.take_events().await.expect("observer events");

    let carol = Client::connect(&scenario.daemon_c.socket).await.unwrap();
    let resp = carol
        .request(Request::JoinSession {
            display_name: "carol".into(),
            ticket: read_ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    // Deterministic late-attach gate: wait until the host has
    // actually admitted carol (PeerJoined), which is also the moment
    // the admission-triggered unicast delivery fires.
    let carol_peer = carol.daemon_peer_id();
    timeout(Duration::from_secs(20), async {
        loop {
            let ev = host_events.recv().await.expect("host events closed");
            if let artel_protocol::Event::PeerJoined { peer, .. } = ev
                && peer.id == carol_peer
            {
                return;
            }
        }
    })
    .await
    .expect("host never admitted carol");

    // NOW attach the workspace — strictly after the live delivery.
    let carol_dir = tempfile::tempdir().unwrap();
    let (carol_ws, carol_rx) = Workspace::join_with(
        &carol,
        scenario.session,
        carol_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&scenario.dns_pkarr))
            .with_join_ticket_timeout(Some(Duration::from_secs(20))),
    )
    .await
    .expect("valid Read-tier joiner must obtain the workspace");
    common::drain_ws_events(carol_rx);

    // bulk_export ran inside join_with — the seed file is on disk.
    let synced = tokio::fs::read(carol_dir.path().join(SEED_FILE))
        .await
        .expect("seed file synced to the joiner");
    assert_eq!(synced, SEED_CONTENT);

    carol_ws.shutdown().await.expect("carol shutdown");
    drop(carol);
    drop(observer);
    scenario.teardown().await;
}

// =============================================================
// Tier C sibling: unicast workspace-ticket delivery across real
// QUIC / n0 relay infrastructure. The hermetic tests above prove
// the logic; this proves the new direct-stream path (ALPN
// artel/upgrade/2, 64 KiB frames) crosses real network plumbing.
// Precedent: alice_post_restart_writes_reach_bob_real_n0.
//
// INTERIM (iroh 0.98.2): uses EndpointSetup::Production (n0's
// public relay) rather than the localhost shared relay — see the
// noq-proto handshake path-poisoning note on
// workspace_restart.rs::alice_post_restart_writes_reach_bob_real_n0.
// =============================================================

/// Per-phase ceiling for the real-n0 test below; generous because
/// every phase crosses real relay infrastructure.
const N0_PHASE_BUDGET: Duration = Duration::from_mins(1);

/// Label + bound one phase of the n0 test so a hang fails fast with
/// the phase name (per docs/diagnosing-flaky-tests.md).
async fn n0_phase<F, T>(name: &'static str, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    common::phase_budgeted(name, N0_PHASE_BUDGET, fut).await
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::large_futures)]
async fn unicast_workspace_ticket_delivery_real_n0() {
    common::init_tracing();

    let alice_state = common::fresh_state();
    let bob_state = common::fresh_state();
    let alice_daemon = n0_phase(
        "spawn alice daemon (production n0)",
        common::spawn_daemon_with_setup(alice_state, artel_daemon::EndpointSetup::Production),
    )
    .await;
    let bob_daemon = n0_phase(
        "spawn bob daemon (production n0)",
        common::spawn_daemon_with_setup(bob_state, artel_daemon::EndpointSetup::Production),
    )
    .await;

    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_dir = tempfile::tempdir().unwrap();
    tokio::fs::write(alice_dir.path().join(SEED_FILE), SEED_CONTENT)
        .await
        .unwrap();
    let (alice_ws, alice_rx) = n0_phase(
        "alice Workspace::host_with",
        Workspace::host_with(
            &alice,
            "alice",
            alice_dir.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            WorkspaceConfig::default()
                .with_endpoint_setup(artel_fs::EndpointSetup::Production)
                .with_daemon_socket(alice_daemon.socket.clone()),
        ),
    )
    .await
    .expect("host_with");
    common::drain_ws_events(alice_rx);
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&bob_daemon.socket).await.unwrap();
    let resp = n0_phase(
        "bob JoinSession (real QUIC/relay)",
        bob.request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        }),
    )
    .await
    .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    // join_with drains for the synthetic TICKET_ACTION — sourced
    // from the unicast-delivered envelope across real n0 — then
    // bulk-exports.
    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, bob_rx) = n0_phase(
        "bob Workspace::join_with (unicast envelope over real QUIC)",
        Workspace::join_with(
            &bob,
            session,
            bob_dir.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            WorkspaceConfig::default()
                .with_endpoint_setup(artel_fs::EndpointSetup::Production)
                .with_join_ticket_timeout(Some(Duration::from_secs(45))),
        ),
    )
    .await
    .expect("join_with over real n0");
    common::drain_ws_events(bob_rx);

    let synced = tokio::fs::read(bob_dir.path().join(SEED_FILE))
        .await
        .expect("seed file exported on bob's side");
    assert_eq!(synced, SEED_CONTENT);

    bob_ws.shutdown().await.expect("bob shutdown");
    alice_ws.shutdown().await.expect("alice shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    drop(alice);
    drop(bob);
    alice_daemon.stop().await;
    bob_daemon.stop().await;
}
