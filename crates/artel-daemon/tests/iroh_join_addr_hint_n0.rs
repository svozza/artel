//! Real-n0 regression test for `Registry::join`'s `host_addr` hint
//! (handoff finding #5).
//!
//! ## What this pins
//!
//! When alice publishes a fresh ticket and bob immediately calls
//! `JoinSession`, bob's daemon needs to dial alice's iroh
//! `EndpointId`. Without an addr hint from the ticket, that dial
//! depends entirely on iroh's pkarr/DNS lookup chain — which has
//! ~500ms of propagation lag in production. Pre-fix, bob's join
//! could hit `JOIN_READY_TIMEOUT` (15s) before pkarr caught up.
//!
//! Post-fix, the wire-form `host_addr` in the ticket is fed
//! synchronously into iroh's address-lookup chain (via a
//! `MemoryLookup` the daemon installs at startup) so the very first
//! dial has the relay url + direct addrs in hand and doesn't wait
//! on pkarr.
//!
//! ## Why `#[ignore]`
//!
//! Real-n0 tests are flaky in CI under back-to-back load (n0's
//! pkarr+DNS rate-limits, see `host_restart_live_writes_n0.rs`).
//! Keeping this `#[ignore]`d means a CI flake here doesn't mask a
//! genuine regression elsewhere. Run manually before changes that
//! touch session-join paths:
//!
//! ```bash
//! cargo test -p artel-daemon --test iroh_join_addr_hint_n0 -- \
//!     --ignored --nocapture
//! ```
//!
//! The deterministic `DnsPkarrServer`-based gossip tests
//! (`iroh_gossip_fanout`, etc.) cover the on-the-happy-path
//! regression in CI; this one is the production-discovery canary.

#![cfg(feature = "iroh")]

mod common;

use std::time::{Duration, Instant};

use artel_client::Client;
use artel_protocol::{PeerId, PeerInfo, Request, Response};

/// Tighter than `gossip_bridge::JOIN_READY_TIMEOUT` (15s) but loose
/// enough to cover normal n0 discovery (~1-2s with a working hint).
/// A failure-mode timeout here means the addr hint isn't being used;
/// bob is falling back to pkarr+DNS resolution. In that fallback path
/// even a successful join takes 5-15s in production, so 8s is a
/// fail-fast budget.
const JOIN_BUDGET: Duration = Duration::from_secs(8);

/// Repro: alice's daemon hosts immediately, bob joins immediately —
/// no `wait_for_endpoint` gating, no artificial delay. With real n0
/// pkarr propagation, bob's first dial races alice's publish.
///
/// Pre-fix shape: bob's `JoinSession` call hangs in
/// `bridge.subscribe_inner`'s `joined()` wait, eventually surfacing
/// as `BridgeError::Iroh("timed out waiting for gossip neighbor")`
/// once `JOIN_READY_TIMEOUT` (15s) fires. The test's `JOIN_BUDGET`
/// (8s) trips first, panicking with the timeout message.
///
/// Post-fix shape: bob's daemon installs alice's wire-form addr into
/// its address-lookup chain before subscribing; the first dial
/// resolves synchronously. Bob's join returns `Ok` well within
/// `JOIN_BUDGET`.
#[tokio::test]
#[ignore = "real-n0; run manually with --ignored before changes touching session-join paths"]
async fn join_succeeds_within_tight_budget_real_n0() {
    let alice_state = common::fresh_state();
    let bob_state = common::fresh_state();

    // Spawn alice first so her endpoint is up. Don't spawn bob
    // until after alice has issued her HostSession — that way bob's
    // daemon is freshly born with an empty DNS cache when it
    // immediately needs to resolve alice. Without this ordering, bob
    // can pre-warm caches during the time alice is starting up and
    // the race window collapses.
    let alice = common::spawn_daemon(alice_state, artel_daemon::EndpointSetup::Production).await;

    // Alice hosts immediately. The ticket comes back with whatever
    // addr the endpoint has populated by now (relay url should be
    // there once `endpoint.online()` resolves; the daemon's startup
    // doesn't currently gate on that — see handoff finding #6 — but
    // with real n0 the addr is usually filled in before the host
    // request returns).
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
    let bob = common::spawn_daemon(bob_state, artel_daemon::EndpointSetup::Production).await;
    let bob_client = Client::connect(&bob.socket).await.unwrap();
    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");

    let started = Instant::now();
    let join_outcome = tokio::time::timeout(
        JOIN_BUDGET,
        bob_client.request(Request::JoinSession {
            peer: bob_peer,
            ticket,
        }),
    )
    .await;
    let elapsed = started.elapsed();

    // `Err` here is the outer `tokio::time::timeout` firing — i.e.
    // bob's IPC reply didn't come back inside `JOIN_BUDGET`. That's
    // the failure mode this test pins.
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
        Response::Error { error } => panic!(
            "bob's JoinSession returned error after {elapsed:?}: {error:?}",
        ),
        other => panic!("expected JoinSession, got {other:?}"),
    }

    drop(alice_client);
    drop(bob_client);
    alice.stop().await;
    bob.stop().await;
}
