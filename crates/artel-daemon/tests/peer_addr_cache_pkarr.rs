//! Deterministic regression test for handoff finding #5c —
//! daemon-side persistent peer-addr cache.
//!
//! ## What this pins
//!
//! When a daemon restarts, peer addrs it learned in the previous
//! incarnation must survive in `addr_hint` (the daemon's
//! `MemoryLookup`) so iroh's address-lookup chain can resolve them
//! without depending on pkarr/DNS having a fresh record. Bug shape
//! from finding #5c: iroh-docs reads id-only `EndpointAddr`s from its
//! persistent doc store on host restart, skips its internal
//! `memory_lookup` seeding (`engine/live.rs:472`), and races
//! pkarr/DNS to find the peer. The cache fills that gap.
//!
//! ## Why deterministic localhost reproduction works
//!
//! Two `DnsPkarrServer` instances. Phase 1 spins up `dns_phase1`,
//! alice and bob's daemons; bob joins alice's session over real
//! gossip+pkarr (production join path), which seeds alice's
//! `addr_hint` with bob's addr via the gossip-bridge. Phase 2 drops
//! `dns_phase1`, drops bob, and brings `dns_phase2` (which has never
//! seen bob's pkarr publish) online before alice's restart. Alice's
//! phase-2 resolver therefore sees an empty pkarr/DNS for bob — the
//! only way `addr_hint` can hold bob's addrs is via the on-disk
//! cache.
//!
//! ## Why the test goes through `JoinSession`, not direct `addr_hint`
//!
//! Production code paths populate `addr_hint` AND a sibling
//! `tracked_peer_ids` set via `gossip_bridge::join_session` —
//! that's the invariant the shutdown-snapshot relies on. Exercising
//! the real production seeding step is strictly stronger than a
//! direct insert: it pins the production invariant by going through
//! the same code path the cache is meant to support.

#![cfg(feature = "iroh")]

mod common;

use std::sync::{Arc, Once};
use std::time::Duration;

use artel_client::Client;
use artel_protocol::{
    Event, MessageKind, PeerId, PeerInfo, Request, Response, SendPayload,
};
use iroh::test_utils::DnsPkarrServer;
use tempfile::TempDir;
use tokio::time::timeout;

/// Per-phase budget — see `docs/diagnosing-flaky-tests.md` § 1.
/// 30s covers two-daemon spin-up + DnsPkarrServer pkarr publish on
/// CI without leaving slack for genuine hangs.
const PHASE_BUDGET: Duration = Duration::from_secs(30);

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

fn init_tracing() {
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

/// Phase 1: alice and bob daemons run on `dns_phase1`. Alice hosts
/// a session, bob joins (production seeding path through
/// gossip-bridge). Alice's daemon shuts down gracefully — the cache
/// snapshot must run from that hook, persisting bob's addr to disk.
///
/// Phase 2: a fresh `DnsPkarrServer` with no record of bob comes up.
/// Alice's daemon restarts at the SAME `iroh_key` path so her
/// `EndpointId` is stable, but pointed at the new pkarr fixture.
///
/// Phase 3 (load-bearing): `addr_hint.get_endpoint_info(bob_id)`
/// must be `Some` containing bob's relay url + direct addrs.
///
/// **Pre-fix:** `addr_hint` is freshly constructed at every daemon
/// startup (`server.rs:701`), so the lookup returns `None` and the
/// final assertion fails. The cache file at the expected path also
/// does not exist post-shutdown — a sub-assertion makes that
/// explicit so snapshot-not-firing vs load-not-firing are
/// distinguishable failure modes.
#[tokio::test]
async fn addr_hint_survives_daemon_restart_via_on_disk_cache() {
    init_tracing();

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
        phase("phase1: spin up first DnsPkarrServer", DnsPkarrServer::run())
            .await
            .expect("DnsPkarrServer::run phase1"),
    );

    let alice_phase1 = phase(
        "phase1: spawn alice daemon",
        common::spawn_daemon_at(&alice_paths, common::testing_setup(&dns_phase1)),
    )
    .await;
    let alice_endpoint_id = alice_phase1.iroh_addr.id;
    let bob_phase1 = phase(
        "phase1: spawn bob daemon",
        common::spawn_daemon(common::fresh_state(), common::testing_setup(&dns_phase1)),
    )
    .await;
    let bob_endpoint_id = bob_phase1.iroh_addr.id;

    // Wait for both daemons' pkarr records to publish so the join
    // dial doesn't race.
    for daemon in [&alice_phase1, &bob_phase1] {
        phase(
            "phase1: wait for daemon pkarr publish",
            dns_phase1.on_endpoint(&daemon.iroh_addr.id, common::PKARR_READY_TIMEOUT),
        )
        .await
        .expect("pkarr ready");
    }

    // Alice hosts.
    let alice_client = Client::connect(&alice_phase1.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let host_resp = phase(
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

    // Bob joins. The production seeding flow:
    //   1. Bob's gossip-bridge call seeds bob's addr_hint with
    //      alice's addr (from the ticket).
    //   2. The gossip handshake brings up the mesh; bob broadcasts
    //      a `JoinAnnouncement`. Alice's host-side bridge receives
    //      it and inserts `bob.peer_id` into `tracked_peer_ids`.
    //   3. Iroh's gossip transport, in the course of (2), populates
    //      alice's `endpoint.remote_info(bob_id)` with bob's actual
    //      relay+direct addrs.
    //
    // Alice's `addr_hint` is NOT seeded with bob during phase 1 —
    // bob's address-of-record lives in iroh's per-endpoint
    // `remote_info`, which the cache snapshot will read at
    // shutdown to fill in `tracked_peer_ids`'s entries.
    let bob_client = Client::connect(&bob_phase1.socket).await.unwrap();
    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
    let _join_resp = phase(
        "phase1: bob joins alice's session",
        bob_client.request(Request::JoinSession {
            peer: bob_peer,
            ticket,
        }),
    )
    .await
    .unwrap();

    // Drive a real message through the session so the gossip
    // mesh is definitively up. Once alice receives bob's message,
    // the handshake is complete and alice's
    // `endpoint.remote_info(bob)` is populated — which is what the
    // shutdown-snapshot reads to fill in `tracked_peer_ids`.
    //
    // Subscribe alice first, then have bob send. We wait for the
    // event to land in alice's stream before tearing down.
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

    phase("phase1: alice receives bob's message (mesh up)", async {
        loop {
            match alice_events.recv().await {
                Some(Event::Message { message, .. }) if message.payload == b"hello-from-bob" => {
                    break;
                }
                Some(_) => continue,
                None => panic!("alice's event stream closed before bob's message arrived"),
            }
        }
    })
    .await;

    drop(alice_events);
    drop(alice_client);
    drop(bob_client);

    phase("phase1: bob daemon shuts down", bob_phase1.stop()).await;
    phase("phase1: alice daemon shuts down", alice_phase1.stop()).await;
    drop(dns_phase1);

    // After alice's shutdown: the cache file MUST exist. If this
    // fails the snapshot-on-shutdown hook didn't fire — different
    // bug than load-on-startup.
    assert!(
        cache_path.exists(),
        "phase1 post-shutdown: cache file expected at {} \
         (snapshot-on-shutdown hook did not fire)",
        cache_path.display(),
    );

    // The session_id from phase 1 isn't used post-restart but
    // dropping it explicitly keeps the test scope tidy.
    let _ = session_id;

    // ------- Phase 2 — fresh DnsPkarrServer, alice restarts -------
    let dns_phase2 = Arc::new(
        phase(
            "phase2: spin up second DnsPkarrServer (fresh, no bob)",
            DnsPkarrServer::run(),
        )
        .await
        .expect("DnsPkarrServer::run phase2"),
    );

    let alice_phase2 = phase(
        "phase2: respawn alice at same paths, fresh pkarr",
        common::spawn_daemon_at(&alice_paths, common::testing_setup(&dns_phase2)),
    )
    .await;

    // EndpointId stability: same iroh_key on disk → same EndpointId.
    // If this drifts the test isn't exercising the same identity
    // across restart and the cache lookup is meaningless.
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
            if cache_path.exists() { "exists" } else { "MISSING" },
        )
    });

    // Spot-check: at least ONE of relay-url / direct-addrs must be
    // present, otherwise the entry is id-only and provides no
    // value (pre-cache, fallback path was equivalent). The exact
    // values depend on what iroh learned during the live session.
    let has_relay = info.data.relay_urls().next().is_some();
    let has_direct = info.data.ip_addrs().next().is_some();
    assert!(
        has_relay || has_direct,
        "phase3: bob's restored entry has neither relay_url nor \
         direct_addrs (id-only seed — cache wrote a useless entry)"
    );

    phase("phase4: alice graceful shutdown", alice_phase2.stop()).await;
    drop(dns_phase2);
    drop(alice_root);
}

