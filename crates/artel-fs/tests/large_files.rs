//! Large-file streaming sync (issue #33): the configurable size cap
//! and the streaming publish/apply pipeline.
//!
//! Tier A/B coverage:
//! - cap boundary end-to-end: at-limit syncs, over-limit skips with a
//!   `SkippedTooLarge` event (host side);
//! - `None` = unlimited: a multi-MiB file (bigger than a small
//!   configured cap would allow) round-trips host→joiner with
//!   content-hash equality;
//! - torn-write safety: the joiner's applied file appears atomically
//!   (rename), never as a half-exported prefix, and the watcher never
//!   publishes the `.artel-fs-tmp-*` temp file back into the doc;
//! - echo-guard suppression holds for a multi-MiB peer-driven write
//!   (no republish storm after apply).
//!
//! The Tier C (`_n0`) sibling in this file runs the multi-MiB
//! round-trip over the bin-shared localhost relay.
//!
//! Sizes are deliberately modest (a few MiB): they cross iroh-blobs'
//! chunk-group boundaries and the old 1 MiB cap — proving the
//! streaming plumbing — without making CI I/O-bound. The pipeline has
//! no size-proportional buffering left, so a GiB-scale soak adds cost,
//! not coverage.

#![allow(clippy::large_futures, clippy::cast_possible_truncation)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, Workspace, WorkspaceConfig, WorkspaceEvent};
use artel_protocol::{Request, Response};
use tokio::time::timeout;

use common::{Pair, spawn_pair, testing_setup};

/// Budget for cross-peer file propagation, matching `common::FILE_BUDGET`
/// but sized up for multi-MiB payloads through the localhost fixture.
const BIG_FILE_BUDGET: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Deterministic multi-MiB payload. Non-repeating enough that a
/// mis-ordered or truncated apply can't accidentally hash equal.
fn payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| {
            let x = i as u64;
            ((x.wrapping_mul(2_654_435_761) >> 7) % 251) as u8
        })
        .collect()
}

/// Poll `path` until its blake3 matches `expected_hash` (comparing
/// hashes rather than buffers keeps failure output readable for
/// multi-MiB payloads), or panic on timeout.
async fn wait_for_file_hash(path: &std::path::Path, expected_hash: blake3::Hash, len: usize) {
    let deadline = std::time::Instant::now() + BIG_FILE_BUDGET;
    loop {
        if let Ok(bytes) = tokio::fs::read(path).await
            && bytes.len() == len
            && blake3::hash(&bytes) == expected_hash
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "never saw expected content at {}",
            path.display(),
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Two-daemon host/joiner pair with per-side `WorkspaceConfig`s.
/// Returns everything a test needs to drive both sides and shut
/// down cleanly.
struct SyncedPair {
    daemon_a: common::RunningDaemon,
    daemon_b: common::RunningDaemon,
    alice: Client,
    bob: Client,
    alice_ws: Arc<Workspace>,
    bob_ws: Arc<Workspace>,
    alice_rx: Option<tokio::sync::mpsc::Receiver<WorkspaceEvent>>,
    bob_rx: Option<tokio::sync::mpsc::Receiver<WorkspaceEvent>>,
    alice_handle: tokio::task::JoinHandle<()>,
    bob_handle: tokio::task::JoinHandle<()>,
    _alice_dir: tempfile::TempDir,
    _bob_dir: tempfile::TempDir,
}

impl SyncedPair {
    async fn teardown(self) {
        self.alice_ws.shutdown().await.expect("alice shutdown");
        self.bob_ws.shutdown().await.expect("bob shutdown");
        let _ = timeout(Duration::from_secs(5), self.alice_handle).await;
        let _ = timeout(Duration::from_secs(5), self.bob_handle).await;
        drop(self.alice);
        drop(self.bob);
        self.daemon_a.stop().await;
        self.daemon_b.stop().await;
    }
}

/// Stand up host (alice) + joiner (bob) over the localhost
/// `DnsPkarr` fixture, with the given per-side size caps. `seed`
/// files are written into alice's dir before hosting.
async fn spawn_synced_pair(
    alice_cap: Option<u64>,
    bob_cap: Option<u64>,
    seed: &[(&str, &[u8])],
) -> SyncedPair {
    common::init_tracing();
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_dir = tempfile::tempdir().unwrap();
    for (name, bytes) in seed {
        tokio::fs::write(alice_dir.path().join(name), bytes)
            .await
            .unwrap();
    }

    let (alice_ws, alice_rx) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_a.socket.clone())
            .with_max_file_size(alice_cap),
    )
    .await
    .expect("Workspace::host");
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
    let (bob_ws, bob_rx) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_b.socket.clone())
            .with_max_file_size(bob_cap),
    )
    .await
    .expect("Workspace::join");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    SyncedPair {
        daemon_a,
        daemon_b,
        alice,
        bob,
        alice_ws,
        bob_ws,
        alice_rx: Some(alice_rx),
        bob_rx: Some(bob_rx),
        alice_handle,
        bob_handle,
        _alice_dir: alice_dir,
        _bob_dir: bob_dir,
    }
}

// =============================================================
// Cap boundary, end-to-end: exactly-at-cap syncs; one byte over
// skips with a SkippedTooLarge event carrying the actual size.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn at_cap_syncs_over_cap_skips_with_event() {
    // Small cap so fixtures stay cheap; the filter treats the value
    // opaquely (unit tests pin the strict-greater-than boundary).
    const CAP: u64 = 256 * 1024;
    let at = payload(CAP as usize);
    let over = payload((CAP as usize) + 1);

    let mut pair = spawn_synced_pair(Some(CAP), Some(CAP), &[]).await;
    common::drain_ws_events(pair.bob_rx.take().expect("bob_rx"));
    let mut alice_rx = pair.alice_rx.take().expect("alice_rx");

    // At-cap file: must round-trip.
    let at_hash = blake3::hash(&at);
    tokio::fs::write(pair.alice_ws.root.join("at-cap.bin"), &at)
        .await
        .unwrap();
    wait_for_file_hash(&pair.bob_ws.root.join("at-cap.bin"), at_hash, at.len()).await;

    // Over-cap file: alice's watcher must emit SkippedTooLarge with
    // the real size, and the file must never appear on bob.
    tokio::fs::write(pair.alice_ws.root.join("over-cap.bin"), &over)
        .await
        .unwrap();
    let ev = common::wait_for_event(
        &mut alice_rx,
        BIG_FILE_BUDGET,
        "SkippedTooLarge(over-cap.bin)",
        |ev| {
            matches!(
                ev,
                WorkspaceEvent::SkippedTooLarge { path, .. }
                    if path.file_name().is_some_and(|n| n == "over-cap.bin")
            )
        },
    )
    .await;
    match ev {
        WorkspaceEvent::SkippedTooLarge { size, .. } => assert_eq!(size, CAP + 1),
        other => panic!("expected SkippedTooLarge, got {other:?}"),
    }
    common::drain_ws_events(alice_rx);

    // Sentinel: a small write after the skipped one still syncs —
    // proving the pipeline is healthy — and the over-cap file is
    // still absent at that point (no fixed sleep).
    tokio::fs::write(pair.alice_ws.root.join("sentinel.txt"), b"after")
        .await
        .unwrap();
    common::wait_for_file(&pair.bob_ws.root.join("sentinel.txt"), b"after").await;
    assert!(
        !pair.bob_ws.root.join("over-cap.bin").exists(),
        "over-cap file must not reach the joiner",
    );

    pair.teardown().await;
}

// =============================================================
// None = unlimited: a file larger than the old 1 MiB hard cap (and
// larger than a peer's would-be small cap) round-trips when both
// sides run uncapped. Content-hash equality proves byte-exactness.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn uncapped_multi_mib_file_round_trips() {
    // 8 MiB + a ragged tail: over the old 1 MiB hard cap, over any
    // bao chunk-group boundary, cheap enough for CI.
    let big = payload(8 * 1024 * 1024 + 12_345);
    let big_hash = blake3::hash(&big);

    let mut pair = spawn_synced_pair(None, None, &[]).await;
    common::drain_ws_events(pair.alice_rx.take().expect("alice_rx"));
    let mut bob_rx = pair.bob_rx.take().expect("bob_rx");

    tokio::fs::write(pair.alice_ws.root.join("big.bin"), &big)
        .await
        .unwrap();

    // No SkippedTooLarge may fire on bob's side while the file
    // lands; assert byte-exact arrival, then scan the drained
    // events for a violation.
    let bob_path = pair.bob_ws.root.join("big.bin");
    let mut saw_skip = false;
    let arrival = async {
        loop {
            if let Ok(bytes) = tokio::fs::read(&bob_path).await
                && bytes.len() == big.len()
                && blake3::hash(&bytes) == big_hash
            {
                return;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    };
    tokio::pin!(arrival);
    let deadline = tokio::time::sleep(BIG_FILE_BUDGET);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            () = &mut arrival => break,
            ev = bob_rx.recv() => {
                if let Some(WorkspaceEvent::SkippedTooLarge { .. }) = ev {
                    saw_skip = true;
                }
            }
            () = &mut deadline => panic!("big.bin never reached bob byte-exact"),
        }
    }
    assert!(!saw_skip, "SkippedTooLarge fired for an uncapped workspace");

    common::drain_ws_events(bob_rx);
    pair.teardown().await;
}

// =============================================================
// Seed-scan path: a large pre-existing file on the host is
// published by scan_and_publish_existing (streaming import) and
// lands on the joiner via bulk_export (streaming export).
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn large_seed_file_bulk_exports_to_joiner() {
    let big = payload(3 * 1024 * 1024 + 7);
    let big_hash = blake3::hash(&big);

    let mut pair = spawn_synced_pair(None, None, &[("seed-big.bin", &big)]).await;
    common::drain_ws_events(pair.alice_rx.take().expect("alice_rx"));
    common::drain_ws_events(pair.bob_rx.take().expect("bob_rx"));

    wait_for_file_hash(&pair.bob_ws.root.join("seed-big.bin"), big_hash, big.len()).await;

    pair.teardown().await;
}

// =============================================================
// Torn-write + temp-file hygiene: while a multi-MiB peer write is
// being applied, the destination path must never hold a partial
// prefix (rename is atomic), no `.artel-fs-tmp-*` residue may
// survive, and — critically — the joiner's watcher must never
// publish the temp file back into the doc.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn apply_is_atomic_and_temp_files_never_publish() {
    use futures_util::StreamExt;
    use iroh_docs::store::Query;

    let big = payload(6 * 1024 * 1024);
    let big_hash = blake3::hash(&big);

    let mut pair = spawn_synced_pair(None, None, &[]).await;
    common::drain_ws_events(pair.alice_rx.take().expect("alice_rx"));
    common::drain_ws_events(pair.bob_rx.take().expect("bob_rx"));

    tokio::fs::write(pair.alice_ws.root.join("atomic.bin"), &big)
        .await
        .unwrap();

    // Poll bob's destination path while the transfer is in flight:
    // every observation must be either absent or complete-and-exact.
    // A partial prefix = torn write = the bug the rename prevents.
    let bob_path = pair.bob_ws.root.join("atomic.bin");
    let deadline = std::time::Instant::now() + BIG_FILE_BUDGET;
    loop {
        if let Ok(bytes) = tokio::fs::read(&bob_path).await {
            assert_eq!(
                bytes.len(),
                big.len(),
                "torn read: destination path held a partial file",
            );
            assert_eq!(blake3::hash(&bytes), big_hash, "content mismatch");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "atomic.bin never appeared on bob",
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Let bob's watcher flush any events the apply produced (temp
    // create + rename): > debounce 300 ms + scheduling margin. Bob is
    // a read-only joiner, so a probe-write sentinel can't reach alice
    // here — a bounded settle is the practical option.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // No temp residue in bob's dir.
    let mut dir = tokio::fs::read_dir(&pair.bob_ws.root).await.unwrap();
    while let Some(entry) = dir.next_entry().await.unwrap() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        assert!(
            !name.starts_with(".artel-fs-tmp-"),
            "temp residue left behind: {name}",
        );
    }

    // No doc entry may exist for any temp-file key: the watcher
    // never published the in-flight `.artel-fs-tmp-*` file.
    let doc = pair.bob_ws.doc();
    let stream = doc.get_many(Query::all()).await.expect("get_many");
    tokio::pin!(stream);
    while let Some(res) = stream.next().await {
        let Ok(entry) = res else { continue };
        let key = String::from_utf8_lossy(entry.key()).into_owned();
        assert!(
            !key.contains(".artel-fs-tmp-") && std::path::Path::new(&key).extension() != Some("tmp".as_ref()),
            "temp file leaked into the doc: {key}",
        );
    }

    pair.teardown().await;
}

// =============================================================
// Echo-guard suppression for a large peer-driven write: after bob
// applies alice's multi-MiB file, bob's watcher sees the on-disk
// change (post-rename event) and must NOT republish it — alice's
// doc must retain exactly one non-tombstone entry for the key,
// authored by alice.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn echo_guard_suppresses_large_peer_write() {
    use futures_util::StreamExt;
    use iroh_docs::store::Query;

    let big = payload(4 * 1024 * 1024 + 99);
    let big_hash = blake3::hash(&big);

    let mut pair = spawn_synced_pair(None, None, &[]).await;
    common::drain_ws_events(pair.alice_rx.take().expect("alice_rx"));
    common::drain_ws_events(pair.bob_rx.take().expect("bob_rx"));

    tokio::fs::write(pair.alice_ws.root.join("echo.bin"), &big)
        .await
        .unwrap();
    wait_for_file_hash(&pair.bob_ws.root.join("echo.bin"), big_hash, big.len()).await;

    // Give bob's watcher a full debounce window plus margin to
    // observe the applied file — if the echo guard failed, THIS is
    // when the republish would land in the doc under bob's author.
    tokio::time::sleep(Duration::from_millis(800)).await;

    let doc = pair.alice_ws.doc();
    let stream = doc
        .get_many(Query::key_exact(b"path/echo.bin".as_slice()))
        .await
        .expect("get_many");
    tokio::pin!(stream);
    let mut entries = 0usize;
    while let Some(res) = stream.next().await {
        let Ok(entry) = res else { continue };
        entries += 1;
        assert_eq!(
            entry.author().as_bytes(),
            pair.alice_ws
                .test_endpoint_id_bytes()
                .await
                .expect("alice node alive")
                .as_ref(),
            "echo republish detected: a non-alice author wrote path/echo.bin",
        );
    }
    assert_eq!(entries, 1, "expected exactly one live entry for echo.bin");

    pair.teardown().await;
}

// =============================================================
// Tier C: multi-MiB round-trip over the bin-shared localhost relay
// (real QUIC transport, production discovery wiring — the closest
// harness to production the suite runs; see
// project_noq_proto_handshake_poisoning for why localhost relay).
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn large_file_round_trip_localhost_relay_n0() {
    common::init_tracing();

    let big = payload(8 * 1024 * 1024 + 12_345);
    let big_hash = blake3::hash(&big);

    let alice_daemon =
        common::spawn_daemon_with_setup(common::fresh_state(), common::custom_relay_setup().await)
            .await;
    let bob_daemon =
        common::spawn_daemon_with_setup(common::fresh_state(), common::custom_relay_setup().await)
            .await;

    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, alice_rx) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default()
            .with_endpoint_setup(common::custom_relay_setup().await)
            .with_daemon_socket(alice_daemon.socket.clone())
            .with_max_file_size(None),
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
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, bob_rx) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(common::custom_relay_setup().await)
            .with_daemon_socket(bob_daemon.socket.clone())
            .with_join_ticket_timeout(Some(Duration::from_secs(45)))
            .with_max_file_size(None),
    )
    .await
    .expect("join_with over localhost relay");
    common::drain_ws_events(bob_rx);
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Live write on the host → streamed import → blob transfer over
    // the relay → streamed export on the joiner.
    tokio::fs::write(alice_ws.root.join("relay-big.bin"), &big)
        .await
        .unwrap();
    wait_for_file_hash(&bob_ws.root.join("relay-big.bin"), big_hash, big.len()).await;

    alice_ws.shutdown().await.expect("alice shutdown");
    bob_ws.shutdown().await.expect("bob shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    alice_daemon.stop().await;
    bob_daemon.stop().await;
}

// =============================================================
// Asymmetric caps: an uncapped host pushes a file bigger than the
// joiner's cap. The joiner must skip the incoming entry (emitting
// SkippedTooLarge with the entry's declared size) — the filter's
// stat-based size layer can't catch a file that doesn't exist on
// disk yet, so this pins the applier's entry-length check.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn incoming_entry_over_local_cap_is_skipped() {
    const BOB_CAP: u64 = 256 * 1024;
    let big = payload((BOB_CAP as usize) * 2);

    let mut pair = spawn_synced_pair(None, Some(BOB_CAP), &[]).await;
    common::drain_ws_events(pair.alice_rx.take().expect("alice_rx"));
    let mut bob_rx = pair.bob_rx.take().expect("bob_rx");

    tokio::fs::write(pair.alice_ws.root.join("too-big-for-bob.bin"), &big)
        .await
        .unwrap();

    let ev = common::wait_for_event(
        &mut bob_rx,
        BIG_FILE_BUDGET,
        "SkippedTooLarge(too-big-for-bob.bin) on the joiner",
        |ev| {
            matches!(
                ev,
                WorkspaceEvent::SkippedTooLarge { path, .. }
                    if path.file_name().is_some_and(|n| n == "too-big-for-bob.bin")
            )
        },
    )
    .await;
    match ev {
        WorkspaceEvent::SkippedTooLarge { size, .. } => {
            assert_eq!(size, u64::try_from(big.len()).unwrap());
        }
        other => panic!("expected SkippedTooLarge, got {other:?}"),
    }
    common::drain_ws_events(bob_rx);
    assert!(
        !pair.bob_ws.root.join("too-big-for-bob.bin").exists(),
        "over-cap incoming file must not be applied",
    );

    pair.teardown().await;
}
