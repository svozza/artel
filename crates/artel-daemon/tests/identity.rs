//! Identity / address-discovery: stable `EndpointId` across daemon
//! restarts (Tier C, real n0), join-addr-hint regression (Tier C,
//! real n0), peer-addr-cache restart determinism (Tier B,
//! `DnsPkarrServer`), and the daemon-side relay-unreachable typed
//! error contract (Tier B, `TestingUnreachableRelay`).
//!
//! Consolidated from four per-file bins (`iroh_identity`,
//! `iroh_join_addr_hint_n0`, `peer_addr_cache_pkarr`,
//! `relay_unreachable`) per
//! `docs/plans/2026-05-29-faster-cargo-test-plan.md` slice 3d. Each
//! original file's docstring is retained verbatim in section banners.
//!
//! Mixed-tier in one bin: the two `*_n0` test fns from
//! `iroh_identity.rs` (renamed in slice 1) plus
//! `join_succeeds_within_tight_budget_real_n0` are filtered out by
//! the default nextest profile via `not test(/_n0$/)`. The Tier-A
//! `iroh_key_file_is_chmod_0600` and
//! `no_iroh_key_path_keeps_synthetic_peer_id` plus the Tier-B
//! `addr_hint_survives_daemon_restart_via_on_disk_cache` and
//! `daemon_start_with_unreachable_relay_returns_typed_error` run on
//! the default profile.
//!
//! The whole bin is `#[cfg(feature = "test-utils")]` because
//! `relay_unreachable` needs `EndpointSetup::TestingUnreachableRelay`,
//! which is `test-utils`-only. `test-utils` implies `iroh`, so the
//! other tests' `feature = "iroh"` gate is satisfied.

#![cfg(feature = "test-utils")]

mod common;

use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use artel_client::Client;
use artel_daemon::{Daemon, DaemonConfig, EndpointSetup, StartError};
use artel_protocol::{
    Event, MessageKind, PeerId, PeerInfo, Request, Response, SendPayload, ticket,
};
use iroh::test_utils::DnsPkarrServer;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::timeout;

// =============================================================
// `EndpointId` is stable across daemon restarts when the iroh secret
// key file persists.
//
// Gated on the `iroh` feature. Without it, the daemon falls back to
// the synthetic peer id and these assertions don't apply.
// =============================================================

#[tokio::test]
async fn endpoint_id_is_stable_across_daemon_restarts_n0() {
    // Caller-owned root so iroh.key persists across the stop/respawn.
    let root = TempDir::new().unwrap();
    let paths = common::RestartState::under(root.path());

    // First boot generates and persists the key.
    let daemon = common::spawn_daemon_at(&paths, EndpointSetup::Production).await;
    let client = Client::connect(&paths.socket).await.unwrap();
    let first_id = client.daemon_peer_id();
    assert_ne!(
        first_id,
        common::FALLBACK_PEER,
        "iroh-derived id must not equal the synthetic fallback",
    );
    assert!(paths.iroh_key.exists(), "iroh.key should be persisted");
    drop(client);
    daemon.stop().await;

    // Second boot reuses the persisted key.
    let daemon = common::spawn_daemon_at(&paths, EndpointSetup::Production).await;
    let client = Client::connect(&paths.socket).await.unwrap();
    let second_id = client.daemon_peer_id();
    assert_eq!(
        first_id, second_id,
        "EndpointId must be stable across restarts",
    );
    drop(client);
    daemon.stop().await;
}

#[tokio::test]
async fn host_ticket_carries_a_real_endpoint_addr_n0() {
    // When iroh is wired up, the ticket the daemon emits via
    // HostSession should carry the daemon's actual EndpointId in
    // host_addr.peer_id. We don't assert anything stronger about
    // direct addrs / relay url because those are environment-
    // dependent — but the addr must be self-consistent and match
    // the live peer id.
    let daemon = common::spawn_daemon(common::fresh_state(), EndpointSetup::Production).await;
    let client = Client::connect(&daemon.socket).await.unwrap();
    let daemon_id = client.daemon_peer_id();

    let resp = client
        .request(Request::HostSession {
            peer: PeerInfo::new(PeerId::from_bytes([1; 32]), "alice"),
            session: None,
        })
        .await
        .unwrap();
    let raw = match resp {
        Response::HostSession { ticket, .. } => ticket,
        other => panic!("expected HostSession, got {other:?}"),
    };
    let decoded = ticket::decode(raw.as_str()).expect("ticket decodes");
    assert_eq!(decoded.host_peer_id, daemon_id);
    assert_eq!(
        decoded.host_addr.peer_id, daemon_id,
        "host_addr.peer_id must match the daemon's live id",
    );

    drop(client);
    daemon.stop().await;
}

#[tokio::test]
async fn iroh_key_file_is_chmod_0600() {
    use std::os::unix::fs::MetadataExt;

    let state = common::fresh_state();
    let iroh_key = state.iroh_key.clone();
    let daemon = common::spawn_daemon(state, EndpointSetup::Production).await;
    let mode = std::fs::metadata(&iroh_key).unwrap().mode() & 0o777;
    assert_eq!(mode, 0o600, "iroh.key must be owner-only");
    daemon.stop().await;
}

#[tokio::test]
async fn no_iroh_key_path_keeps_synthetic_peer_id() {
    // Sanity: when the caller doesn't supply iroh_key_path, the
    // daemon stays local-only and the wire peer id matches the
    // synthetic fallback. Bypasses the common helper because that
    // helper hard-wires `iroh_key_path: Some(...)`.
    let root = TempDir::new().unwrap();
    let daemon = Daemon::start(DaemonConfig {
        socket_path: root.path().join("daemon.sock"),
        pid_path: root.path().join("daemon.pid"),
        sessions_dir: root.path().join("sessions"),
        daemon_peer_id: common::FALLBACK_PEER,
        iroh_key_path: None,
        endpoint_setup: EndpointSetup::Production,
    })
    .await
    .expect("daemon start");
    let socket = daemon.socket_path().to_path_buf();
    let shutdown = daemon.shutdown_handle();
    let join = tokio::spawn(daemon.run());

    let client = Client::connect(&socket).await.unwrap();
    assert_eq!(client.daemon_peer_id(), common::FALLBACK_PEER);
    drop(client);
    shutdown.trigger();
    timeout(Duration::from_secs(5), join)
        .await
        .expect("daemon did not exit")
        .expect("daemon panicked")
        .expect("daemon io");
}

// =============================================================
// Real-n0 regression test for `Registry::join`'s `host_addr` hint
// (handoff finding #5).
//
// When alice publishes a fresh ticket and bob immediately calls
// `JoinSession`, bob's daemon needs to dial alice's iroh
// `EndpointId`. Without an addr hint from the ticket, that dial
// depends entirely on iroh's pkarr/DNS lookup chain — which has
// ~500ms of propagation lag in production. Pre-fix, bob's join could
// hit `JOIN_READY_TIMEOUT` (15s) before pkarr caught up. Post-fix,
// the wire-form `host_addr` in the ticket is fed synchronously into
// iroh's address-lookup chain (via a `MemoryLookup` the daemon
// installs at startup) so the very first dial has the relay url +
// direct addrs in hand and doesn't wait on pkarr.
//
// Runs under the `n0` nextest profile (filter `test(/_n0$/)`); the
// default profile filters it out via `not test(/_n0$/)`.
// =============================================================

/// Tighter than `gossip_bridge::JOIN_READY_TIMEOUT` (15s) but loose
/// enough to cover normal n0 discovery (~1-2s with a working hint).
const JOIN_BUDGET: Duration = Duration::from_secs(8);

#[tokio::test]
async fn join_succeeds_within_tight_budget_real_n0() {
    let alice_state = common::fresh_state();
    let bob_state = common::fresh_state();

    // Spawn alice first so her endpoint is up. Don't spawn bob until
    // after alice has issued her HostSession — that way bob's daemon
    // is freshly born with an empty DNS cache when it immediately
    // needs to resolve alice.
    let alice = common::spawn_daemon(alice_state, EndpointSetup::Production).await;

    // Alice hosts immediately.
    let alice_client = Client::connect(&alice.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let host_resp = alice_client
        .request(Request::HostSession {
            peer: alice_peer,
            session: None,
        })
        .await
        .unwrap();
    let (session_id, ticket) = match host_resp {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    // Now spawn bob and have him join immediately — no
    // `wait_for_endpoint` gating, no artificial delay. Bob's daemon
    // has an empty DNS cache and must dial alice by EndpointId. That
    // dial races alice's pkarr publish-loop on the n0 network.
    let bob = common::spawn_daemon(bob_state, EndpointSetup::Production).await;
    let bob_client = Client::connect(&bob.socket).await.unwrap();
    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");

    let started = Instant::now();
    let join_outcome = timeout(
        JOIN_BUDGET,
        bob_client.request(Request::JoinSession {
            peer: bob_peer,
            ticket,
        }),
    )
    .await;
    let elapsed = started.elapsed();

    let Ok(reply) = join_outcome else {
        panic!(
            "bob's JoinSession exceeded JOIN_BUDGET={JOIN_BUDGET:?} (elapsed: {elapsed:?}); \
             addr hint not being used — bob is waiting on pkarr/DNS propagation",
        );
    };
    let join_resp = reply.expect("IPC request");
    match join_resp {
        Response::JoinSession { session, .. } => {
            assert_eq!(session, session_id, "session id mismatch");
        }
        Response::Error { error } => {
            panic!("bob's JoinSession returned error after {elapsed:?}: {error:?}")
        }
        other => panic!("expected JoinSession, got {other:?}"),
    }

    drop(alice_client);
    drop(bob_client);
    alice.stop().await;
    bob.stop().await;
}

// =============================================================
// Deterministic regression test for handoff finding #5c —
// daemon-side persistent peer-addr cache.
//
// When a daemon restarts, peer addrs it learned in the previous
// incarnation must survive in `addr_hint` (the daemon's
// `MemoryLookup`) so iroh's address-lookup chain can resolve them
// without depending on pkarr/DNS having a fresh record. Bug shape
// from finding #5c: iroh-docs reads id-only `EndpointAddr`s from its
// persistent doc store on host restart, skips its internal
// `memory_lookup` seeding (`engine/live.rs:472`), and races
// pkarr/DNS to find the peer. The cache fills that gap.
//
// Two `DnsPkarrServer` instances. Phase 1 spins up `dns_phase1`,
// alice and bob's daemons; bob joins alice's session over real
// gossip+pkarr (production join path), which seeds alice's
// `addr_hint` with bob's addr via the gossip-bridge. Phase 2 drops
// `dns_phase1`, drops bob, and brings `dns_phase2` (which has never
// seen bob's pkarr publish) online before alice's restart. Alice's
// phase-2 resolver therefore sees an empty pkarr/DNS for bob — the
// only way `addr_hint` can hold bob's addrs is via the on-disk cache.
// =============================================================

/// Per-phase budget — see `docs/diagnosing-flaky-tests.md` § 1.
/// 30s covers two-daemon spin-up + `DnsPkarrServer` pkarr publish on
/// CI without leaving slack for genuine hangs.
const PEER_CACHE_PHASE_BUDGET: Duration = Duration::from_secs(30);

async fn peer_cache_phase<F, T>(name: &'static str, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    eprintln!(">>> phase begin: {name}");
    let res = timeout(PEER_CACHE_PHASE_BUDGET, fut)
        .await
        .unwrap_or_else(|_| panic!("phase hung past {PEER_CACHE_PHASE_BUDGET:?}: {name}"));
    eprintln!("<<< phase end:   {name}");
    res
}

fn init_peer_cache_tracing() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| {
            concat!(
                "info,",
                "iroh=debug,iroh::discovery=trace,",
                "iroh_docs=debug,iroh_gossip=debug,iroh_blobs=debug,",
                "artel_fs=debug,artel_daemon=debug",
            )
            .to_string()
        });
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .with_test_writer()
            .with_target(true)
            .try_init();
    });
}

#[tokio::test]
#[allow(clippy::too_many_lines, clippy::large_futures)]
async fn addr_hint_survives_daemon_restart_via_on_disk_cache() {
    init_peer_cache_tracing();

    // Caller-owned root for alice so iroh.key + cache file persist
    // across her stop/respawn. Bob gets a fresh tempdir each time
    // (no restart needed for him).
    let alice_root = TempDir::new().expect("alice tempdir");
    let alice_paths = common::RestartState::under(alice_root.path());
    let cache_path = alice_paths
        .iroh_key
        .parent()
        .expect("iroh_key has parent")
        .join("peer_addrs.postcard");

    // ------- Phase 1 — alice + bob run; bob joins alice's session -------
    let dns_phase1 = Arc::new(
        peer_cache_phase(
            "phase1: spin up first DnsPkarrServer",
            DnsPkarrServer::run(),
        )
        .await
        .expect("DnsPkarrServer::run phase1"),
    );

    let alice_phase1 = peer_cache_phase(
        "phase1: spawn alice daemon",
        common::spawn_daemon_at(&alice_paths, common::testing_setup(&dns_phase1)),
    )
    .await;
    let alice_endpoint_id = alice_phase1.iroh_addr.id;
    let bob_phase1 = peer_cache_phase(
        "phase1: spawn bob daemon",
        common::spawn_daemon(common::fresh_state(), common::testing_setup(&dns_phase1)),
    )
    .await;
    let bob_endpoint_id = bob_phase1.iroh_addr.id;

    // Wait for both daemons' pkarr records to publish.
    for daemon in [&alice_phase1, &bob_phase1] {
        peer_cache_phase(
            "phase1: wait for daemon pkarr publish",
            dns_phase1.on_endpoint(&daemon.iroh_addr.id, common::PKARR_READY_TIMEOUT),
        )
        .await
        .expect("pkarr ready");
    }

    // Alice hosts.
    let alice_client = Client::connect(&alice_phase1.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let host_resp = peer_cache_phase(
        "phase1: alice hosts",
        alice_client.request(Request::HostSession {
            peer: alice_peer,
            session: None,
        }),
    )
    .await
    .unwrap();
    let (session_id, ticket) = match host_resp {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("expected HostSession, got {other:?}"),
    };

    // Bob joins. The production seeding flow seeds bob's addr_hint
    // with alice's addr (from the ticket); the gossip handshake
    // populates alice's `endpoint.remote_info(bob_id)` with bob's
    // actual relay+direct addrs.
    let bob_client = Client::connect(&bob_phase1.socket).await.unwrap();
    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
    let _join_resp = peer_cache_phase(
        "phase1: bob joins alice's session",
        bob_client.request(Request::JoinSession {
            peer: bob_peer,
            ticket,
        }),
    )
    .await
    .unwrap();

    // Drive a real message through the session so the gossip mesh is
    // definitively up. Once alice receives bob's message, the
    // handshake is complete and alice's `endpoint.remote_info(bob)`
    // is populated — which is what the shutdown-snapshot reads to
    // fill in `tracked_peer_ids`.
    let _ = alice_client
        .request(Request::Subscribe {
            session: session_id,
            since: None,
        })
        .await
        .unwrap();
    let mut alice_events = alice_client.take_events().await.expect("events");

    bob_client
        .request(Request::Send {
            session: session_id,
            payload: SendPayload {
                kind: MessageKind::Chat,
                action: "ping".into(),
                payload: b"hello-from-bob".to_vec(),
            },
        })
        .await
        .unwrap();

    peer_cache_phase("phase1: alice receives bob's message (mesh up)", async {
        loop {
            match alice_events.recv().await {
                Some(Event::Message { message, .. }) if message.payload == b"hello-from-bob" => {
                    break;
                }
                Some(_) => {}
                None => panic!("alice's event stream closed before bob's message arrived"),
            }
        }
    })
    .await;

    drop(alice_events);
    drop(alice_client);
    drop(bob_client);

    peer_cache_phase("phase1: bob daemon shuts down", bob_phase1.stop()).await;
    peer_cache_phase("phase1: alice daemon shuts down", alice_phase1.stop()).await;
    drop(dns_phase1);

    // After alice's shutdown: the cache file MUST exist.
    assert!(
        cache_path.exists(),
        "phase1 post-shutdown: cache file expected at {} \
         (snapshot-on-shutdown hook did not fire)",
        cache_path.display(),
    );

    let _ = session_id;

    // ------- Phase 2 — fresh DnsPkarrServer, alice restarts -------
    let dns_phase2 = Arc::new(
        peer_cache_phase(
            "phase2: spin up second DnsPkarrServer (fresh, no bob)",
            DnsPkarrServer::run(),
        )
        .await
        .expect("DnsPkarrServer::run phase2"),
    );

    let alice_phase2 = peer_cache_phase(
        "phase2: respawn alice at same paths, fresh pkarr",
        common::spawn_daemon_at(&alice_paths, common::testing_setup(&dns_phase2)),
    )
    .await;

    // EndpointId stability: same iroh_key on disk → same EndpointId.
    assert_eq!(
        alice_phase2.iroh_addr.id, alice_endpoint_id,
        "phase2: alice's EndpointId changed across restart \
         (iroh_key not reloaded from disk?)"
    );

    // ------- Phase 3 — load-bearing assertion -------
    let restored = alice_phase2.addr_hint.get_endpoint_info(bob_endpoint_id);

    let info = restored.unwrap_or_else(|| {
        panic!(
            "phase3: addr_hint does not contain bob after restart. \
             Cache file at {} {} on disk. Either the snapshot-on- \
             shutdown hook didn't run in phase 1, or the load-on- \
             startup hook didn't run in phase 2.",
            cache_path.display(),
            if cache_path.exists() {
                "exists"
            } else {
                "MISSING"
            },
        )
    });

    // Spot-check: at least ONE of relay-url / direct-addrs must be
    // present, otherwise the entry is id-only and provides no value.
    let has_relay = info.data.relay_urls().next().is_some();
    let has_direct = info.data.ip_addrs().next().is_some();
    assert!(
        has_relay || has_direct,
        "phase3: bob's restored entry has neither relay_url nor \
         direct_addrs (id-only seed — cache wrote a useless entry)"
    );

    peer_cache_phase("phase4: alice graceful shutdown", alice_phase2.stop()).await;
    drop(dns_phase2);
    drop(alice_root);
}

// =============================================================
// `Daemon::start` against `EndpointSetup::TestingUnreachableRelay`
// with an `iroh_key_path` (so the iroh runtime is stood up) must
// return `StartError::RelayUnreachable` within a budget. Pre-fix the
// daemon's `resolve_iroh_runtime` never calls `endpoint.online()` at
// all — so this test fails by returning `Ok` instead of the typed
// error. Post-fix the daemon mirrors `WorkspaceNode::spawn`: it
// gates `online()` on `EndpointSetup::awaits_relay()` and wraps it
// in `tokio::time::timeout`.
// =============================================================

const RELAY_HARNESS_BUDGET: Duration = Duration::from_secs(40);

#[tokio::test(flavor = "multi_thread")]
async fn daemon_start_with_unreachable_relay_returns_typed_error() {
    let state = common::fresh_state();
    let config = DaemonConfig {
        socket_path: state.socket.clone(),
        pid_path: state.pid.clone(),
        sessions_dir: state.sessions.clone(),
        daemon_peer_id: common::FALLBACK_PEER,
        // iroh_key_path = Some triggers the iroh runtime, which
        // is the codepath under test (#6).
        iroh_key_path: Some(state.iroh_key.clone()),
        endpoint_setup: EndpointSetup::TestingUnreachableRelay,
    };

    // `Daemon::start` must return Err within the harness budget.
    // Pre-fix it returns Ok almost immediately because the daemon
    // never awaits `endpoint.online()` at all.
    let result = timeout(RELAY_HARNESS_BUDGET, Daemon::start(config))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "phase hung past {RELAY_HARNESS_BUDGET:?}: \
                 Daemon::start did not return Err within budget — \
                 the timeout wrapper around endpoint.online() is missing"
            )
        });

    match result {
        Err(StartError::RelayUnreachable(budget)) => {
            assert!(
                budget <= RELAY_HARNESS_BUDGET,
                "internal budget {budget:?} should be at most the harness budget {RELAY_HARNESS_BUDGET:?}"
            );
        }
        Ok(daemon) => {
            // Pre-fix path: the daemon stood up because `online()`
            // was never awaited. Tear it down so the test process
            // exits cleanly, then fail with the diagnosis.
            daemon.trigger_shutdown();
            let _ = timeout(Duration::from_secs(5), daemon.run()).await;
            panic!(
                "expected StartError::RelayUnreachable, but Daemon::start succeeded — \
                 the daemon never awaits endpoint.online() (#6 asymmetry)"
            );
        }
        Err(other) => panic!("expected StartError::RelayUnreachable, got {other:?}"),
    }

    drop(state);
}
