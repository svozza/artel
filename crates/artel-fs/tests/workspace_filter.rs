//! Workspace filter behaviour: default-permissive round-trip,
//! first-match-wins ordering, `ReadOnly` enforcement at every layer
//! (watcher / scan / applier, both incoming and outgoing,
//! pre-/post-join, plus the `on_removed` tombstone gate), the
//! `WorkspaceTicketEnvelope` round-trip, and the "tombstone bypasses
//! filter" regression trap.
//!
//! Consolidated from ten per-file bins (the
//! `default_read_write_unchanged_behaviour`, `mixed_rules_first_match_wins`,
//! `read_only_*` family, `ticket_envelope_*`, and `tombstone_filter_check`
//! files) per `docs/plans/2026-05-29-faster-cargo-test-plan.md` slice 2b.
//! The original files' docstrings live on (occasionally updated) in the
//! section banners below.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use artel_client::Client;
use artel_fs::error::WorkspaceError;
use artel_fs::{
    AttachPolicy, Direction, Mode, PathRule, PathRules, TICKET_ACTION, Workspace, WorkspaceConfig,
    WorkspaceEvent, path_to_key,
};
use artel_protocol::{MessageKind, Request, Response, SendPayload};
use bytes::Bytes;
use futures_util::StreamExt;
use iroh_docs::store::Query;
use pretty_assertions::assert_eq;
use tokio::time::{sleep, timeout};

use common::{
    Pair, doc_has_key, init_tracing, spawn_pair, testing_setup, wait_for_event, wait_for_file,
    wait_for_missing,
};

// =============================================================
// Default-permissive `PathRules` (the implicit case for every
// `WorkspaceConfig::default()` consumer) gives exactly the
// pre-rules behaviour.
//
// The watcher, applier, scan, and bulk-export each consult
// `Workspace::compiled_rules` per event. This test guards against
// accidental behavioural drift on the 100% case where rules are
// absent — if it ever fails, the rule check has unintentionally
// changed the default-permissive path.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn default_rules_give_unchanged_round_trip() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();

    let alice_dir = tempfile::tempdir().unwrap();
    // Pre-existing file exercises `scan_and_publish_existing` on the
    // default-permissive path.
    tokio::fs::write(alice_dir.path().join("preseed.txt"), b"hello")
        .await
        .unwrap();

    let (alice_ws, _) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::host_with");
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, _) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join_with");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Bulk-export: pre-seed reaches Bob.
    wait_for_file(&bob_dir.path().join("preseed.txt"), b"hello").await;

    // Live edit: outgoing watcher path.
    tokio::fs::write(alice_dir.path().join("live.txt"), b"world")
        .await
        .unwrap();
    wait_for_file(&bob_dir.path().join("live.txt"), b"world").await;

    // Live delete.
    tokio::fs::remove_file(alice_dir.path().join("live.txt"))
        .await
        .unwrap();
    wait_for_missing(&bob_dir.path().join("live.txt")).await;

    alice_ws.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// First-match-wins ordering, end-to-end on the wire.
//
// Rule unit tests in `rules.rs` already verify ordering at the
// `mode_for` level. This integration test confirms the same ordering
// carries through the watcher → doc → applier pipeline:
// a `docs/secret/foo.txt` write under
// `[{ "docs/**" -> ReadWrite }, { "docs/secret/**" -> ReadOnly }]`
// propagates (first rule wins → `ReadWrite`), and stops propagating
// when the rule order is reversed.
// =============================================================

/// What `run_first_match_wins` expects to happen to
/// `docs/secret/foo.txt` on Bob's side. Each variant uses the
/// shape-appropriate signal: `Propagates` polls for the file directly
/// (positive); `Blocked` waits for a sentinel marker that was written
/// *after* the secret and then asserts the secret is still absent.
#[derive(Clone, Copy, Debug)]
enum Expectation {
    Propagates,
    Blocked,
}

/// Budget for the positive path: how long to wait for the secret to
/// reach Bob before failing. Generous because we go through
/// debounce + doc → sync → applier.
const PROPAGATE_BUDGET: Duration = Duration::from_secs(15);
const POLL: Duration = Duration::from_millis(100);

async fn poll_for(path: &Path, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        sleep(POLL).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread")]
async fn first_match_wins_carries_through_wire() {
    init_tracing();
    // Phase 1: broad ReadWrite rule precedes narrow ReadOnly. The
    // narrow rule is unreachable; `docs/secret/foo.txt` propagates.
    // Drive timing positively — poll for the secret on Bob's side.
    run_first_match_wins(
        PathRules {
            default: Mode::ReadWrite,
            rules: vec![
                PathRule {
                    glob: "docs/**".into(),
                    mode: Mode::ReadWrite,
                },
                PathRule {
                    glob: "docs/secret/**".into(),
                    mode: Mode::ReadOnly,
                },
            ],
        },
        Expectation::Propagates,
    )
    .await;

    // Phase 2: reorder. Narrow ReadOnly precedes broad ReadWrite.
    // Now `docs/secret/foo.txt` is blocked. Drive timing via a
    // marker file written *after* the secret — once Bob has the
    // marker, the secret would have arrived too if the rule weren't
    // blocking it.
    run_first_match_wins(
        PathRules {
            default: Mode::ReadWrite,
            rules: vec![
                PathRule {
                    glob: "docs/secret/**".into(),
                    mode: Mode::ReadOnly,
                },
                PathRule {
                    glob: "docs/**".into(),
                    mode: Mode::ReadWrite,
                },
            ],
        },
        Expectation::Blocked,
    )
    .await;
}

/// Stand a host/joiner pair up with `rules`, write
/// `docs/secret/foo.txt` (and a marker), then verify the
/// `expectation` against Bob's filesystem.
async fn run_first_match_wins(rules: PathRules, expectation: Expectation) {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_rules(rules),
    )
    .await
    .expect("Workspace::host_with");
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, _) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join_with");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Write the secret + marker.
    tokio::fs::create_dir_all(alice_dir.path().join("docs/secret"))
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("docs/secret/foo.txt"), b"data")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("marker.txt"), b"go")
        .await
        .unwrap();

    let bob_secret = bob_dir.path().join("docs/secret/foo.txt");
    match expectation {
        Expectation::Propagates => {
            // Positive case: poll the secret directly. The marker
            // signal is *not* a substitute here — top-level marker
            // and nested secret travel through independent
            // debounce/publish paths and the marker can land on
            // Bob first even though the secret was written first.
            assert!(
                poll_for(&bob_secret, PROPAGATE_BUDGET).await,
                "first-match-wins broken: ReadWrite-first should let \
                 docs/secret/foo.txt through within {PROPAGATE_BUDGET:?}",
            );
        }
        Expectation::Blocked => {
            // Negative case: wait for the marker (written *after*
            // the secret) to arrive on Bob. Once it has, the
            // pipeline has had at least one full round-trip — if
            // the secret were going to leak, it would have arrived
            // by now too.
            wait_for_file(&bob_dir.path().join("marker.txt"), b"go").await;
            assert!(
                !bob_secret.exists(),
                "first-match-wins broken: ReadOnly-first should block \
                 docs/secret/foo.txt; it leaked to {}",
                bob_secret.display(),
            );
        }
    }

    alice_ws.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Applier-side defence-in-depth: even if a peer publishes a
// `ReadOnly` path into the doc (because they're misbehaving, run an
// older version, or in this test bypass their own watcher), the
// receiving applier drops the `InsertRemote` and surfaces
// `WorkspaceEvent::SkippedReadOnly { Incoming }`.
//
// Mechanism: Alice hosts with `secret/**: ReadOnly`. Alice then
// injects `secret/foo.txt` directly via `alice_ws.doc().set_bytes`,
// which bypasses Alice's own watcher (the rule check lives in the
// watcher, not in `doc.set_bytes`). The doc write propagates to Bob;
// Bob's applier sees the rule and drops it.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn applier_drops_incoming_read_only_insert() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let rules = PathRules {
        default: Mode::ReadWrite,
        rules: vec![PathRule {
            glob: "secret/**".into(),
            mode: Mode::ReadOnly,
        }],
    };

    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _alice_events) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_rules(rules),
    )
    .await
    .expect("Workspace::host_with");
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, mut bob_events) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join_with");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Inject `secret/foo.txt` directly into Alice's doc, bypassing
    // her own watcher's rule check. Use Alice's author so the doc
    // entry is well-formed.
    let secret_key = path_to_key(
        alice_ws.root.as_path(),
        &alice_ws.root.join("secret/foo.txt"),
    )
    .expect("path_to_key for secret");
    alice_ws
        .doc()
        .set_bytes(
            alice_ws.author(),
            secret_key.clone(),
            Bytes::from_static(b"injected-secret"),
        )
        .await
        .expect("doc.set_bytes");

    // Inject a marker too so we know the InsertRemote train has
    // arrived at Bob — the marker isn't ReadOnly so it lands.
    let marker_key = path_to_key(alice_ws.root.as_path(), &alice_ws.root.join("marker.txt"))
        .expect("path_to_key for marker");
    alice_ws
        .doc()
        .set_bytes(alice_ws.author(), marker_key, Bytes::from_static(b"go"))
        .await
        .expect("doc.set_bytes marker");

    // Wait for marker on Bob's disk → guarantees the secret
    // InsertRemote has been processed by Bob's applier (FIFO).
    wait_for_file(&bob_dir.path().join("marker.txt"), b"go").await;

    // Bob's applier dropped the secret.
    assert!(
        !bob_dir.path().join("secret/foo.txt").exists(),
        "applier regression: secret/foo.txt landed on bob despite ReadOnly rule",
    );

    // Bob's event stream surfaces SkippedReadOnly{Incoming} for the
    // secret. `wait_for_event` awaits the channel rather than
    // draining what happened to have arrived, so this doesn't lean
    // on the marker wait having ordered the event's delivery.
    wait_for_event(
        &mut bob_events,
        common::FILE_BUDGET,
        "SkippedReadOnly{Incoming} for secret/foo.txt",
        |ev| {
            matches!(
                ev,
                WorkspaceEvent::SkippedReadOnly {
                    path,
                    direction: Direction::Incoming,
                } if path.ends_with("secret/foo.txt")
            )
        },
    )
    .await;

    // Now drive a tombstone for the same key. The applier's rule
    // check sits BEFORE the tombstone branch, so the delete must
    // also be dropped.
    alice_ws
        .doc()
        .del(alice_ws.author(), secret_key)
        .await
        .expect("doc.del");

    // No file existed to begin with on Bob, so the assertion is
    // about the event stream: a second SkippedReadOnly{Incoming}
    // for the same path means the tombstone branch was gated.
    // (The first wait consumed the write's skip event, so a match
    // here can only be the tombstone's.)
    wait_for_event(
        &mut bob_events,
        common::FILE_BUDGET,
        "SkippedReadOnly{Incoming} for tombstone on secret/foo.txt",
        |ev| {
            matches!(
                ev,
                WorkspaceEvent::SkippedReadOnly {
                    path,
                    direction: Direction::Incoming,
                } if path.ends_with("secret/foo.txt")
            )
        },
    )
    .await;

    alice_ws.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Watcher-side rule check: a `ReadOnly` path written *after*
// `Workspace::run` must not reach the doc, must not reach Bob, and
// must surface as `WorkspaceEvent::SkippedReadOnly { Outgoing }`.
//
// Defence in depth: we inspect Alice's doc directly to confirm the
// watcher dropped the change at source, not just that Bob's applier
// filtered it. A leaked publish would still propagate to a third
// joiner.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn watcher_blocks_outgoing_read_only_write() {
    init_tracing();
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let rules = PathRules {
        default: Mode::ReadWrite,
        rules: vec![PathRule {
            glob: "secret/**".into(),
            mode: Mode::ReadOnly,
        }],
    };

    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, mut alice_events) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_rules(rules),
    )
    .await
    .expect("Workspace::host_with");
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, _bob_events) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join_with");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Write a secret + a sentinel marker. The marker propagates
    // (default ReadWrite); the secret must not.
    tokio::fs::create_dir_all(alice_dir.path().join("secret"))
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("secret/key.txt"), b"top-secret")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("marker.txt"), b"go")
        .await
        .unwrap();

    // Wait for marker on Bob; afterwards we know the secret event
    // (which preceded it) has been processed by Alice's watcher.
    wait_for_file(&bob_dir.path().join("marker.txt"), b"go").await;

    // Bob never sees the secret.
    assert!(
        !bob_dir.path().join("secret/key.txt").exists(),
        "secret/key.txt leaked to bob",
    );

    // Defence in depth: Alice's doc has no entry for the secret key.
    let secret_key = path_to_key(
        alice_ws.root.as_path(),
        &alice_ws.root.join("secret/key.txt"),
    )
    .expect("path_to_key");
    assert!(
        !doc_has_key(&alice_ws.doc(), &secret_key).await,
        "alice's watcher regression: secret/key.txt landed in the doc",
    );

    // The watcher emitted `SkippedReadOnly { Outgoing }` for the
    // secret. Await it rather than draining what's already queued —
    // the watcher emits after its debounce, which is not ordered
    // against the marker's cross-peer round-trip.
    wait_for_event(
        &mut alice_events,
        common::FILE_BUDGET,
        "SkippedReadOnly{Outgoing} for secret/key.txt",
        |ev| {
            matches!(
                ev,
                WorkspaceEvent::SkippedReadOnly {
                    path,
                    direction: Direction::Outgoing,
                } if path.ends_with("secret/key.txt")
            )
        },
    )
    .await;

    alice_ws.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Scan-side rule check: a `ReadOnly` file pre-existing on disk when
// the host attaches must not be published by
// `scan_and_publish_existing`. Distinct from the watcher path above.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn scan_blocks_outgoing_read_only_preexisting_file() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    // Pre-seed Alice's dir BEFORE the workspace is constructed, so
    // the secret goes through `scan_and_publish_existing` rather
    // than the live watcher path.
    let alice_dir = tempfile::tempdir().unwrap();
    tokio::fs::create_dir_all(alice_dir.path().join("secret"))
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("secret/key.txt"), b"top-secret")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("marker.txt"), b"go")
        .await
        .unwrap();

    let rules = PathRules {
        default: Mode::ReadWrite,
        rules: vec![PathRule {
            glob: "secret/**".into(),
            mode: Mode::ReadOnly,
        }],
    };

    let (alice_ws, _alice_events) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_rules(rules),
    )
    .await
    .expect("Workspace::host_with");
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, _bob_events) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join_with");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Marker propagated → scan completed → secret was either
    // published or skipped by now.
    wait_for_file(&bob_dir.path().join("marker.txt"), b"go").await;

    assert!(
        !bob_dir.path().join("secret/key.txt").exists(),
        "secret/key.txt leaked to bob via bulk-export",
    );

    let secret_key = path_to_key(
        alice_ws.root.as_path(),
        &alice_ws.root.join("secret/key.txt"),
    )
    .expect("path_to_key");
    assert!(
        !doc_has_key(&alice_ws.doc(), &secret_key).await,
        "alice's scan regression: secret/key.txt landed in the doc",
    );

    alice_ws.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Post-join live writes to a `ReadOnly` zone are blocked by the
// watcher and never propagate. Same idea as
// `watcher_blocks_outgoing_read_only_write` above but specifically
// with the sentinel write happening *after* both sides have joined
// and run their watchers — guards against an "only the cold path is
// gated" regression.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn post_join_live_write_to_read_only_zone_is_blocked() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let rules = PathRules {
        default: Mode::ReadWrite,
        rules: vec![PathRule {
            glob: "locked/**".into(),
            mode: Mode::ReadOnly,
        }],
    };

    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_rules(rules),
    )
    .await
    .expect("Workspace::host_with");
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, _) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join_with");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Now, post-join, post-run, write into the locked zone.
    tokio::fs::create_dir_all(alice_dir.path().join("locked"))
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("locked/x.txt"), b"locked-data")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("marker.txt"), b"go")
        .await
        .unwrap();

    wait_for_file(&bob_dir.path().join("marker.txt"), b"go").await;

    assert!(
        !bob_dir.path().join("locked/x.txt").exists(),
        "locked/x.txt leaked to bob (post-join)",
    );

    let locked_key = path_to_key(alice_ws.root.as_path(), &alice_ws.root.join("locked/x.txt"))
        .expect("path_to_key");
    assert!(
        !doc_has_key(&alice_ws.doc(), &locked_key).await,
        "alice's post-join watcher regression: locked/x.txt landed in the doc",
    );

    alice_ws.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// `on_removed` rule check: a delete on a `ReadOnly` path must NOT
// publish a tombstone. Specifically tests the watcher's `on_removed`
// gate (Linux `Remove` events arrive there directly).
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn on_removed_does_not_tombstone_read_only_path() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let rules = PathRules {
        default: Mode::ReadWrite,
        rules: vec![PathRule {
            glob: "locked/**".into(),
            mode: Mode::ReadOnly,
        }],
    };

    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_rules(rules),
    )
    .await
    .expect("Workspace::host_with");
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, _) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join_with");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Write a locked file (publish blocked by `on_modified` rule
    // check), then delete it (delete must be blocked by `on_removed`
    // rule check, otherwise a tombstone propagates).
    tokio::fs::create_dir_all(alice_dir.path().join("locked"))
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("locked/y.txt"), b"some-data")
        .await
        .unwrap();

    // Drive a marker through the watcher path AFTER the locked write
    // — the marker landing on Bob proves the locked write event was
    // observed and processed.
    tokio::fs::write(alice_dir.path().join("pre-marker.txt"), b"phase-1")
        .await
        .unwrap();
    wait_for_file(&bob_dir.path().join("pre-marker.txt"), b"phase-1").await;

    // Now delete the locked file.
    tokio::fs::remove_file(alice_dir.path().join("locked/y.txt"))
        .await
        .unwrap();

    // Drive a second marker — once it lands on Bob, the delete event
    // for `locked/y.txt` has been observed by Alice's watcher.
    tokio::fs::write(alice_dir.path().join("post-marker.txt"), b"phase-2")
        .await
        .unwrap();
    wait_for_file(&bob_dir.path().join("post-marker.txt"), b"phase-2").await;

    // Inspect Alice's doc with `include_empty()` — a tombstone is a
    // zero-length entry that `single_latest_per_key()` filters out
    // by default. We want to catch tombstones explicitly.
    let locked_key = path_to_key(alice_ws.root.as_path(), &alice_ws.root.join("locked/y.txt"))
        .expect("path_to_key");
    let stream = alice_ws
        .doc()
        .get_many(Query::key_exact(locked_key).include_empty())
        .await
        .expect("get_many");
    tokio::pin!(stream);
    let mut had_any_entry = false;
    while let Some(res) = stream.next().await {
        let _ = res.expect("entry ok");
        had_any_entry = true;
    }
    assert!(
        !had_any_entry,
        "alice's on_removed regression: locked/y.txt has an entry/tombstone in the doc",
    );

    alice_ws.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Forged broadcast `workspace.ticket` sends are rejected at ingress
// and never reach a joiner.
//
// Pre-fix hosts broadcast the ticket envelope as a System message
// on the session log; the revoked-lurker fix moved the envelope to
// host→peer unicast. A malicious RW member (or stale host) that
// hand-rolls a `Request::Send` of a forged envelope is now rejected
// at the host's sequencing chokepoint — the impostor never enters
// the log, so it can't ride any joiner-visible surface. The joiner's
// `wait_for_ticket` therefore times out with NO workspace
// materialised.
//
// (Earlier this test asserted the send was accepted-but-suppressed;
// the ingress gate makes that a stronger property — rejected
// outright. The downstream log filters remain as defense-in-depth
// for a mixed-build host that sequenced one before the gate existed.)
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn broadcast_ticket_action_is_inert_for_joiner() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    // Alice hosts the artel session but does NOT stand up a
    // workspace. She attempts to broadcast a `TICKET_ACTION` payload
    // via `Request::Send` — mimicking a stale pre-fix host (or a
    // forged envelope from an RW member). The host's ingress gate
    // rejects the reserved action outright, so it never enters the
    // log.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let (session, artel_ticket) = match alice
        .request(Request::HostSession {
            display_name: "alice".into(),
            session: None,
        })
        .await
        .unwrap()
    {
        Response::HostSession {
            session, ticket, ..
        } => (session, ticket),
        other => panic!("HostSession: got {other:?}"),
    };

    let old_shape_payload = b"docaaa\
        cbbcaa3aacaaaaaaaaaaiiabaaaaaiabarbjzgaaaaaaaaaaaaaaaaaaaaaa"
        .to_vec();

    // Rejected at the host's sequencing chokepoint — the reserved
    // action is never member-authored. The client surfaces the
    // daemon's Response::Error as a protocol Err.
    let send_result = alice
        .request(Request::Send {
            session,
            payload: SendPayload {
                kind: MessageKind::System,
                action: TICKET_ACTION.to_string(),
                payload: old_shape_payload,
            },
        })
        .await;
    assert!(
        send_result.is_err(),
        "forged TICKET_ACTION send must be rejected at ingress, got {send_result:?}",
    );

    // Bob joins the artel session, then calls `Workspace::join_with`
    // with a bounded ticket wait. The broadcast TICKET_ACTION is
    // suppressed on every joiner-visible surface, so the join times
    // out — it must NOT decode the payload (the pre-fix behaviour
    // was a Malformed error from the envelope decode).
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: artel_ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = tempfile::tempdir().unwrap();

    let result = timeout(
        Duration::from_secs(20),
        Workspace::join_with(
            &bob,
            session,
            bob_dir.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            WorkspaceConfig::default()
                .with_endpoint_setup(testing_setup(&dns_pkarr))
                .with_join_ticket_timeout(Some(Duration::from_secs(8))),
        ),
    )
    .await
    .expect("Workspace::join_with must resolve via its own ticket timeout");

    let err = result.expect_err("join must fail — the broadcast is inert");
    match err {
        WorkspaceError::Iroh(msg) if msg.contains("timed out waiting for workspace.ticket") => {}
        other => panic!("expected ticket-wait timeout, got {other:?}"),
    }

    // Defence in depth: nothing should have been written to bob_dir
    // beyond the state dir the workspace would normally create —
    // the broadcast payload must not have driven any materialisation.
    let mut entries = tokio::fs::read_dir(bob_dir.path()).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let name = entry.file_name();
        let s = name.to_string_lossy();
        assert!(
            s == ".artel-fs",
            "unexpected entry in bob_dir after failed join: {s}",
        );
    }

    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// `PathRules` ride the `workspace.ticket` envelope from host to
// joiner intact.
//
// End-to-end proof that [`artel_fs::PathRules`] survive the wire.
// Asserts the joiner sees the host's rules deep-equal on
// `Workspace::rules()` — independent of any enforcement happening at
// the watcher / applier layer.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn rules_round_trip_via_envelope() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();

    let configured_rules = PathRules {
        default: Mode::ReadOnly,
        rules: vec![
            PathRule {
                glob: "shared/**".into(),
                mode: Mode::ReadWrite,
            },
            PathRule {
                glob: "*.lock".into(),
                mode: Mode::ReadOnly,
            },
        ],
    };

    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _alice_ws_events) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_rules(configured_rules.clone()),
    )
    .await
    .expect("Workspace::host_with");
    let session = alice_ws.session_id();
    let artel_ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();

    // Sanity: the host stores its own rules.
    assert_eq!(alice_ws.rules(), &configured_rules);

    // Bob joins. Joiner-side `WorkspaceConfig::rules` is intentionally
    // set to something *different* from the host's rules to confirm
    // the host's rules win on join.
    let bob_distractor_rules = PathRules {
        default: Mode::ReadWrite,
        rules: vec![PathRule {
            glob: "ignored/**".into(),
            mode: Mode::ReadOnly,
        }],
    };

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: artel_ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, _bob_ws_events) = timeout(
        Duration::from_secs(45),
        Workspace::join_with(
            &bob,
            session,
            bob_dir.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            WorkspaceConfig::default()
                .with_endpoint_setup(testing_setup(&dns_pkarr))
                .with_rules(bob_distractor_rules.clone()),
        ),
    )
    .await
    .expect("Workspace::join_with exceeded 45s")
    .expect("Workspace::join_with");

    // Bob's rules deep-equal the host's, *not* the distractor rules
    // Bob configured. Host wins.
    assert_eq!(bob_ws.rules(), &configured_rules);
    assert_ne!(bob_ws.rules(), &bob_distractor_rules);

    bob_ws.shutdown().await.expect("shutdown");
    alice_ws.shutdown().await.expect("shutdown");
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Tombstones must not bypass the workspace filter on either the
// applier or the bulk-export side.
//
// The historic ordering had `ReadOnly → tombstone → filter`, so a
// peer's tombstone for a path the local filter would skip
// (asymmetric ignore globs, version drift, attacker-crafted key
// targeting a hardcoded-skip path like `.git/HEAD`) reached
// `tokio::fs::remove_file` regardless. That deleted local state the
// workspace was never supposed to touch.
//
// Both call sites (`applier::handle_entry` and
// `workspace::bulk_export`) are covered here; they share the same
// intended ordering, and the fix moves the filter check ABOVE the
// tombstone branch in both. The hardcoded-skip path is the cheapest
// way to exercise the bug — `WorkspaceFilter` already refuses to let
// `.git/HEAD` through, no asymmetric-glob plumbing needed.
// =============================================================

/// Settling window after a tombstone propagates and the marker has
/// been observed. The marker idiom guarantees FIFO arrival, so a
/// short extra sleep is only insurance against the
/// remove-then-write reordering the macOS notify backend has
/// historically produced. 200ms is plenty.
const TOMBSTONE_SETTLE: Duration = Duration::from_millis(200);

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn applier_filter_check_gates_tombstone_for_hardcoded_skip() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();

    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _alice_events) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::host_with");
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, _bob_events) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join_with");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Pre-create `.git/HEAD` on Bob's side. This file lives outside
    // the workspace's filter (hardcoded skip) — Bob's watcher will
    // never publish it, and Alice's tombstone for the same key
    // therefore must not delete it.
    let bob_git_head = bob_ws.root.join(".git").join("HEAD");
    tokio::fs::create_dir_all(bob_git_head.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&bob_git_head, b"ref: refs/heads/main\n")
        .await
        .unwrap();

    // Inject a tombstone for `.git/HEAD` directly into Alice's doc.
    // First seed an entry so `del` produces a recognisable
    // zero-length tombstone (iroh-docs doesn't tombstone a key it's
    // never seen).
    let git_head_key = path_to_key(
        alice_ws.root.as_path(),
        &alice_ws.root.join(".git").join("HEAD"),
    )
    .expect("path_to_key for .git/HEAD");
    alice_ws
        .doc()
        .set_bytes(
            alice_ws.author(),
            git_head_key.clone(),
            Bytes::from_static(b"attacker-write"),
        )
        .await
        .expect("doc.set_bytes seeding");
    alice_ws
        .doc()
        .del(alice_ws.author(), git_head_key)
        .await
        .expect("doc.del tombstone");

    // Marker idiom: a non-skipped path lets us observe when the
    // applier has chewed through the tombstone above (FIFO).
    let marker_key = path_to_key(alice_ws.root.as_path(), &alice_ws.root.join("marker.txt"))
        .expect("path_to_key for marker");
    alice_ws
        .doc()
        .set_bytes(alice_ws.author(), marker_key, Bytes::from_static(b"go"))
        .await
        .expect("doc.set_bytes marker");

    wait_for_file(&bob_dir.path().join("marker.txt"), b"go").await;
    sleep(TOMBSTONE_SETTLE).await;

    // The bug: applier's filter check sits AFTER the tombstone
    // branch, so `.git/HEAD` was unlinked despite being a hardcoded
    // skip. The fix moves filter ABOVE the tombstone branch.
    assert!(
        bob_git_head.exists(),
        "applier deleted bob's .git/HEAD via tombstone bypass — \
         filter check must gate the remove_file branch",
    );
    let surviving = tokio::fs::read(&bob_git_head)
        .await
        .expect(".git/HEAD readable");
    assert_eq!(
        surviving, b"ref: refs/heads/main\n",
        ".git/HEAD contents must be untouched",
    );

    alice_ws.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bulk_export_filter_check_gates_tombstone_for_hardcoded_skip() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();

    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _alice_events) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::host_with");
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();

    // Seed-then-tombstone `.git/HEAD` in the host's doc so a
    // joiner who runs `bulk_export` against this doc sees the
    // zero-length entry. `include_empty()` on the bulk_export side
    // is what surfaces it.
    let git_head_key = path_to_key(
        alice_ws.root.as_path(),
        &alice_ws.root.join(".git").join("HEAD"),
    )
    .expect("path_to_key for .git/HEAD");
    alice_ws
        .doc()
        .set_bytes(
            alice_ws.author(),
            git_head_key.clone(),
            Bytes::from_static(b"attacker-write"),
        )
        .await
        .expect("doc.set_bytes seeding");
    alice_ws
        .doc()
        .del(alice_ws.author(), git_head_key)
        .await
        .expect("doc.del tombstone");

    // Bob's dir is non-empty: he already has `.git/HEAD`. The
    // attach-policy emptiness check exempts hardcoded-skip paths,
    // so `RequireEmpty` still passes. The point: bulk_export must
    // not delete this file when it sees the tombstone.
    let bob_dir = tempfile::tempdir().unwrap();
    let bob_git_head = bob_dir.path().join(".git").join("HEAD");
    tokio::fs::create_dir_all(bob_git_head.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&bob_git_head, b"ref: refs/heads/main\n")
        .await
        .unwrap();

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let (bob_ws, _bob_events) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join_with");

    // After bulk_export has run inside `join_with`, Bob's
    // `.git/HEAD` must still be on disk.
    assert!(
        bob_git_head.exists(),
        "bulk_export deleted bob's .git/HEAD via tombstone bypass — \
         filter check must gate the remove_file branch",
    );
    let surviving = tokio::fs::read(&bob_git_head)
        .await
        .expect(".git/HEAD readable");
    assert_eq!(
        surviving, b"ref: refs/heads/main\n",
        ".git/HEAD contents must be untouched",
    );

    bob_ws.shutdown().await.expect("shutdown");
    alice_ws.shutdown().await.expect("shutdown");
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// Sync exclusions (`WorkspaceConfig::exclude`) on the wire — issue
// #34. The gitignore layer is gone: a `.gitignore` listing a synced
// path must not stop it from syncing. The replacement is a local,
// consumer-owned exclude list defaulting to dotfiles, with every
// exclusion surfaced as `SkippedExcluded` (never silently).
// =============================================================

/// Host + joiner with per-side `WorkspaceConfig::exclude` values.
/// The caller pre-populates the host dir before calling, so the scan
/// path is exercised; the joiner's event stream is drained, the
/// host's is returned for assertions.
struct ExcludePair {
    alice_ws: Arc<Workspace>,
    bob_ws: Arc<Workspace>,
    alice_events: tokio::sync::mpsc::Receiver<WorkspaceEvent>,
    alice: Client,
    bob: Client,
    alice_handle: tokio::task::JoinHandle<()>,
    bob_handle: tokio::task::JoinHandle<()>,
}

impl ExcludePair {
    async fn spawn(
        daemon_a: &common::RunningDaemon,
        daemon_b: &common::RunningDaemon,
        dns_pkarr: &Arc<iroh::test_utils::DnsPkarrServer>,
        alice_dir: &Path,
        bob_dir: &Path,
        host_exclude: Option<Vec<String>>,
        join_exclude: Option<Vec<String>>,
    ) -> Self {
        let alice = Client::connect(&daemon_a.socket).await.unwrap();
        let (alice_ws, alice_events) = Workspace::host_with(
            &alice,
            "alice",
            alice_dir.to_path_buf(),
            AttachPolicy::AllowExisting,
            WorkspaceConfig::default()
                .with_endpoint_setup(testing_setup(dns_pkarr))
                .with_daemon_socket(daemon_a.socket.clone())
                .with_exclude(host_exclude),
        )
        .await
        .expect("Workspace::host_with");
        let session = alice_ws.session_id();
        let ticket = alice_ws
            .join_ticket()
            .expect("host has join_ticket")
            .clone();
        let alice_ws = Arc::new(alice_ws);
        let alice_handle = Arc::clone(&alice_ws).run().await;

        let bob = Client::connect(&daemon_b.socket).await.unwrap();
        let resp = bob
            .request(Request::JoinSession {
                display_name: "bob".into(),
                ticket,
            })
            .await
            .unwrap();
        assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

        let (bob_ws, bob_events) = Workspace::join_with(
            &bob,
            session,
            bob_dir.to_path_buf(),
            AttachPolicy::RequireEmpty,
            WorkspaceConfig::default()
                .with_endpoint_setup(testing_setup(dns_pkarr))
                .with_daemon_socket(daemon_b.socket.clone())
                .with_exclude(join_exclude),
        )
        .await
        .expect("Workspace::join_with");
        common::drain_ws_events(bob_events);
        let bob_ws = Arc::new(bob_ws);
        let bob_handle = Arc::clone(&bob_ws).run().await;

        Self {
            alice_ws,
            bob_ws,
            alice_events,
            alice,
            bob,
            alice_handle,
            bob_handle,
        }
    }

    async fn teardown(self) {
        self.alice_ws.shutdown().await.expect("shutdown");
        self.bob_ws.shutdown().await.expect("shutdown");
        let _ = timeout(Duration::from_secs(5), self.alice_handle).await;
        let _ = timeout(Duration::from_secs(5), self.bob_handle).await;
        drop(self.alice);
        drop(self.bob);
    }
}

/// Regression test for issue #34: a workspace-root `.gitignore`
/// listing a synced path must have NO effect on sync. Pre-fix, the
/// filter honoured it and `state/log.jsonl` never left the host —
/// silently.
#[tokio::test(flavor = "multi_thread")]
async fn gitignored_path_syncs_anyway() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice_dir = tempfile::tempdir().unwrap();
    // The trap from the bug report: the app's state dir is
    // gitignored (the normal thing for a user to do). Also exercise
    // scan (pre-existing file) and watcher (live write) paths.
    tokio::fs::write(alice_dir.path().join(".gitignore"), b"state/\n*.log\n")
        .await
        .unwrap();
    tokio::fs::create_dir_all(alice_dir.path().join("state"))
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("state/log.jsonl"), b"preseed")
        .await
        .unwrap();

    let bob_dir = tempfile::tempdir().unwrap();
    let pair = ExcludePair::spawn(
        &daemon_a,
        &daemon_b,
        &dns_pkarr,
        alice_dir.path(),
        bob_dir.path(),
        None,
        None,
    )
    .await;

    // Scan path: the gitignored pre-seed reaches Bob.
    wait_for_file(&bob_dir.path().join("state/log.jsonl"), b"preseed").await;

    // Watcher path: a live gitignored write propagates too.
    tokio::fs::write(alice_dir.path().join("app.log"), b"live")
        .await
        .unwrap();
    wait_for_file(&bob_dir.path().join("app.log"), b"live").await;

    pair.teardown().await;
    daemon_a.stop().await;
    daemon_b.stop().await;
}

/// The default exclude (dotfiles) blocks a hidden subtree on the
/// watcher path AND surfaces it as `SkippedExcluded { Outgoing }` —
/// the skip must be observable, not a debug-log whisper.
#[tokio::test(flavor = "multi_thread")]
async fn default_exclude_blocks_dotfiles_and_emits_event() {
    init_tracing();
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice_dir = tempfile::tempdir().unwrap();
    let bob_dir = tempfile::tempdir().unwrap();
    let mut pair = ExcludePair::spawn(
        &daemon_a,
        &daemon_b,
        &dns_pkarr,
        alice_dir.path(),
        bob_dir.path(),
        None,
        None,
    )
    .await;

    // Live write into a hidden subtree: watcher must skip + emit.
    //
    // Two steps, and the ordering is load-bearing. inotify attaches
    // one watch per directory, and notify backfills watches for a
    // freshly created subtree only after it processes the parent's
    // CREATE — a file written into `.state/log` before that backfill
    // produces NO event, ever. For *synced* subtrees the watcher's
    // rescan_dir closes that gap, but an excluded directory is
    // (correctly) never descended into, so the file-level
    // SkippedExcluded would be a coin flip under load (observed as a
    // CI-only flake on Linux, diagnosed 2026-07-19: the failing
    // trace shows both Create(Folder) events and no Create(File)).
    // Waiting for the directory's own SkippedExcluded first proves
    // notify processed the subtree's creation — its watch backfill
    // runs in that same processing pass, ≥300 ms (debounce) before
    // the event reaches us — so the file write below reliably gets
    // an inotify event of its own.
    tokio::fs::create_dir_all(alice_dir.path().join(".state/log"))
        .await
        .unwrap();
    wait_for_event(
        &mut pair.alice_events,
        PROPAGATE_BUDGET,
        "SkippedExcluded(Outgoing) for the hidden directory",
        |ev| {
            matches!(
                ev,
                WorkspaceEvent::SkippedExcluded {
                    direction: Direction::Outgoing,
                    path,
                } if path.ends_with(".state/log") || path.ends_with(".state")
            )
        },
    )
    .await;

    tokio::fs::write(alice_dir.path().join(".state/log/peer.jsonl"), b"nope")
        .await
        .unwrap();
    wait_for_event(
        &mut pair.alice_events,
        PROPAGATE_BUDGET,
        "SkippedExcluded(Outgoing) for the hidden write",
        |ev| {
            matches!(
                ev,
                WorkspaceEvent::SkippedExcluded {
                    direction: Direction::Outgoing,
                    path,
                } if path.ends_with(".state/log/peer.jsonl")
            )
        },
    )
    .await;

    // Marker idiom: a visible write that lands on Bob proves the
    // pipeline chewed past the hidden one — which must be absent.
    tokio::fs::write(alice_dir.path().join("marker.txt"), b"go")
        .await
        .unwrap();
    wait_for_file(&bob_dir.path().join("marker.txt"), b"go").await;
    assert!(
        !bob_dir.path().join(".state/log/peer.jsonl").exists(),
        "hidden subtree must not reach the joiner under the default exclude",
    );

    pair.teardown().await;
    daemon_a.stop().await;
    daemon_b.stop().await;
}

/// An explicit exclude list REPLACES the dotfile default: a hidden
/// state dir opted back in via a non-default list syncs end-to-end
/// (the harness-style consumer pattern), while a listed glob still
/// blocks.
#[tokio::test(flavor = "multi_thread")]
async fn explicit_exclude_replaces_default_and_hidden_dir_syncs() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice_dir = tempfile::tempdir().unwrap();
    // Pre-seed a hidden state dir (scan path) — the app pattern from
    // issue #34.
    tokio::fs::create_dir_all(alice_dir.path().join(".harness/log"))
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join(".harness/log/peer.jsonl"), b"preseed")
        .await
        .unwrap();

    let bob_dir = tempfile::tempdir().unwrap();
    // Both sides opt out of the dotfile default but still exclude
    // `*.secret` — proving replace-not-merge on the wire. The list is
    // local per node, so BOTH sides pass it (the joiner's applier
    // filters incoming entries by its own list).
    let excl = || Some(vec!["**/*.secret".to_string(), "*.secret".to_string()]);
    let pair = ExcludePair::spawn(
        &daemon_a,
        &daemon_b,
        &dns_pkarr,
        alice_dir.path(),
        bob_dir.path(),
        excl(),
        excl(),
    )
    .await;

    // Scan path: the hidden pre-seed reaches Bob.
    wait_for_file(&bob_dir.path().join(".harness/log/peer.jsonl"), b"preseed").await;

    // Watcher path: live hidden append propagates.
    tokio::fs::write(alice_dir.path().join(".harness/log/live.jsonl"), b"live")
        .await
        .unwrap();
    wait_for_file(&bob_dir.path().join(".harness/log/live.jsonl"), b"live").await;

    // The explicit glob still blocks: `api.secret` never lands.
    tokio::fs::write(alice_dir.path().join("api.secret"), b"hush")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("marker.txt"), b"go")
        .await
        .unwrap();
    wait_for_file(&bob_dir.path().join("marker.txt"), b"go").await;
    assert!(
        !bob_dir.path().join("api.secret").exists(),
        "explicitly-excluded glob must still block",
    );

    pair.teardown().await;
    daemon_a.stop().await;
    daemon_b.stop().await;
}

/// Deleting a locally-excluded path must not publish a tombstone —
/// the delete-side twin of the exclude gate. With asymmetric lists,
/// a permissive host publishes `.env`; the default-exclude joiner
/// never applies it but has its own local `.env`. When the joiner
/// deletes that local file, its watcher must NOT tombstone the key:
/// the host's live entry would be destroyed by a peer whose own
/// hygiene merely hides the path. (The hole was Linux-specific —
/// direct `Remove` events reach `on_removed`, which pre-fix had no
/// filter; macOS deletes ride `on_modified`'s already-gated `NotFound`
/// fallthrough. CI on Linux is where this test bites.)
#[tokio::test(flavor = "multi_thread")]
async fn deleting_locally_excluded_path_does_not_tombstone_peer_entry() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice_dir = tempfile::tempdir().unwrap();
    let bob_dir = tempfile::tempdir().unwrap();
    // Host syncs everything; joiner keeps the dotfile default.
    let pair = ExcludePair::spawn(
        &daemon_a,
        &daemon_b,
        &dns_pkarr,
        alice_dir.path(),
        bob_dir.path(),
        Some(vec![]),
        None,
    )
    .await;

    // Bob needs RW for his marker/delete writes to propagate at all.
    common::grant_rw_and_wait(
        &pair.alice,
        pair.alice_ws.session_id(),
        pair.bob.daemon_peer_id(),
        bob_dir.path(),
        alice_dir.path(),
    )
    .await;

    // Alice publishes .env (her empty exclude list allows it) and a
    // visible marker to gate on.
    tokio::fs::write(alice_dir.path().join(".env"), b"SECRET=1")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("marker1.txt"), b"m1")
        .await
        .unwrap();
    wait_for_file(&bob_dir.path().join("marker1.txt"), b"m1").await;

    // Bob has his own local .env — never synced (his applier refused
    // Alice's), purely local state.
    tokio::fs::write(bob_dir.path().join(".env"), b"BOB_LOCAL=1")
        .await
        .unwrap();
    sleep(Duration::from_millis(500)).await; // let the watcher chew (and skip) it
    tokio::fs::remove_file(bob_dir.path().join(".env"))
        .await
        .unwrap();

    // Second marker from Bob proves his watcher processed past the
    // delete; then Alice's .env must still exist — no tombstone leaked.
    tokio::fs::write(bob_dir.path().join("marker2.txt"), b"m2")
        .await
        .unwrap();
    wait_for_file(&alice_dir.path().join("marker2.txt"), b"m2").await;
    sleep(Duration::from_millis(500)).await; // grace for a (buggy) tombstone to apply
    let alice_env = tokio::fs::read(alice_dir.path().join(".env"))
        .await
        .expect("alice's .env must survive bob deleting his locally-excluded copy");
    assert_eq!(alice_env, b"SECRET=1");

    pair.teardown().await;
    daemon_a.stop().await;
    daemon_b.stop().await;
}

/// The joiner's own exclude list gates its applier (Incoming): a
/// host with a permissive list publishes a dotfile, but a joiner on
/// the default (dotfiles) refuses to apply it — and says so via
/// `SkippedExcluded { Incoming }` on the joiner's stream. Asymmetric
/// on purpose: the exclude list is local hygiene, not ticket-borne
/// workspace policy.
#[tokio::test(flavor = "multi_thread")]
async fn joiner_exclude_gates_incoming_applies() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice_dir = tempfile::tempdir().unwrap();
    let bob_dir = tempfile::tempdir().unwrap();
    // Host syncs everything; joiner keeps the dotfile default. We
    // need the JOINER's events here, so build this pair by hand
    // rather than via ExcludePair (which drains bob's).
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let (alice_ws, alice_events) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_exclude(Some(vec![])),
    )
    .await
    .expect("Workspace::host_with");
    common::drain_ws_events(alice_events);
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let (bob_ws, mut bob_events) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join_with");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Host publishes a dotfile (its empty exclude list allows it).
    tokio::fs::write(alice_dir.path().join(".env"), b"SECRET=1")
        .await
        .unwrap();

    // Joiner's applier must refuse it — observably.
    let ev = wait_for_event(
        &mut bob_events,
        PROPAGATE_BUDGET,
        "SkippedExcluded(Incoming) on the joiner",
        |ev| {
            matches!(
                ev,
                WorkspaceEvent::SkippedExcluded {
                    direction: Direction::Incoming,
                    ..
                }
            )
        },
    )
    .await;
    match ev {
        WorkspaceEvent::SkippedExcluded { path, .. } => {
            assert!(path.ends_with(".env"), "{path:?}");
        }
        other => panic!("expected SkippedExcluded, got {other:?}"),
    }
    assert!(
        !bob_dir.path().join(".env").exists(),
        "joiner-side exclude must keep the dotfile off disk",
    );

    alice_ws.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
