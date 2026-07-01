//! Cross-peer sync end-to-end: bulk export, live edits in both
//! directions, deletes, empty files, ticket-timeout fallback, the
//! repeated full round-trip.
//!
//! Consolidated from six per-file bins (`delete_propagates`,
//! `empty_file_no_error`, `join_bulk_export`, `join_ticket_timeout`,
//! `live_edit`, `round_trip`) per
//! `docs/plans/2026-05-29-faster-cargo-test-plan.md` slice 2c. Each
//! original file's docstring is retained verbatim in section banners
//! below.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use artel_client::Client;
use artel_fs::{
    AttachPolicy, Workspace, WorkspaceConfig, WorkspaceError, WorkspaceEvent, key_to_path,
    path_to_key,
};
use artel_protocol::{PeerId, Request, Response, SessionId};
use futures_util::StreamExt;
use futures_util::future::FutureExt;
use iroh::test_utils::DnsPkarrServer;
use iroh_docs::store::Query;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

use common::{Pair, daemon_testing_setup, spawn_pair, testing_setup};

const WAIT_BUDGET: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

// =============================================================
// Deletion round-trip: Alice deletes a file → tombstone in the
// doc → Bob's applier removes it from disk.
//
// Exercises the watcher's `Removed` branch and the applier's
// `content_len() == 0` branch, neither of which is covered by
// `round_trip_3_in_a_row` below (which only tests writes).
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn alice_delete_propagates_to_bob() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    // Alice hosts; her workspace starts with one seed file so the
    // file is present on Bob's side after `Workspace::join`.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();

    let alice_dir = tempfile::tempdir().unwrap();
    tokio::fs::write(alice_dir.path().join("doomed.txt"), b"to be deleted")
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
    .expect("Workspace::host");
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // Bob joins. After bulk_export his dir should already contain
    // `doomed.txt` (sanity-checked before we delete).
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
    .expect("Workspace::join");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    let bob_path = bob_ws.root.join("doomed.txt");
    let bob_bytes = tokio::fs::read(&bob_path)
        .await
        .expect("bulk export should have populated doomed.txt");
    assert_eq!(bob_bytes, b"to be deleted");

    // No settling delay needed — `Workspace::run().await` already
    // resolved once the OS-level filesystem watch was attached.

    // Delete on Alice. The watcher emits a `Removed` event after
    // the 300ms debounce, which becomes a `Doc::del` (zero-length
    // entry). Bob's applier sees an `InsertRemote` with
    // `content_len() == 0` and calls `remove_file`.
    //
    // Cross-platform note: macOS FSEvents reports the unlink as
    // post-hoc `Modify(Metadata)` / `Modify(Data)` rather than a
    // clean `Remove`. The watcher's `on_modified` path handles
    // that case by tombstoning when the read fails with NotFound.
    tokio::fs::remove_file(alice_dir.path().join("doomed.txt"))
        .await
        .unwrap();

    common::wait_for_missing(&bob_path).await;

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
// Delete-then-recreate with identical bytes must propagate.
//
// Regression trap for the echo guard's `last_published` map
// surviving a delete (found via
// `returning_rw_member_offline_across_rotation_regains_write_real_n0`
// in the 2026-07-01 nightly): the applier records the blake3 of
// every peer-driven write so the watcher can skip the resulting
// filesystem echo, but nothing cleared that hash when the path was
// later tombstoned. A file re-created with the *same bytes* after
// a delete then hashed equal to the stale entry and the watcher
// swallowed the publish forever — the doc's latest entry stayed a
// tombstone while disk state diverged.
//
// Two acts, one per forget site:
// - Act 1 (applier-side `mark_remote_delete`): bob's hash for the
//   path came from his applier applying alice's write; alice
//   deletes; bob re-creates the identical bytes and alice must see
//   the file again.
// - Act 2 (watcher-side `forget` in `on_removed`): bob's hash came
//   from his own watcher publishing the act-1 write; bob deletes
//   locally and re-creates the identical bytes; alice must see the
//   file again. (Bob deletes — not alice — so the delete is from
//   steady state; see the in-test comment on Linux debouncer
//   Create+Remove annihilation.)
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn recreating_identical_bytes_after_delete_propagates() {
    const BYTES: &[u8] = b"rises from the ashes";
    common::init_tracing();

    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    // Alice hosts with the seed file already present so bob's
    // bulk_export applies it (populating his last_published map).
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_dir = tempfile::tempdir().unwrap();
    tokio::fs::write(alice_dir.path().join("phoenix.txt"), BYTES)
        .await
        .unwrap();

    let (alice_ws, _) = Workspace::host_with(
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
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let bob_peer = bob.daemon_peer_id();
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
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_b.socket.clone()),
    )
    .await
    .expect("Workspace::join");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    let alice_path = alice_ws.root.join("phoenix.txt");
    let bob_path = bob_ws.root.join("phoenix.txt");
    common::wait_for_file(&bob_path, BYTES).await;

    // Bob needs write capability for both acts.
    common::grant_rw_and_wait(&alice, session, bob_peer, bob_dir.path(), alice_dir.path()).await;

    // --- Act 1: applier-side forget (bob's hash came from applying
    // alice's seed write). Alice deletes; the tombstone removes
    // bob's copy; bob re-creates the identical bytes; alice must
    // see the file come back.
    eprintln!("--- act 1: peer delete, joiner re-creates identical bytes ---");
    tokio::fs::remove_file(&alice_path).await.unwrap();
    common::wait_for_missing(&bob_path).await;

    tokio::fs::write(&bob_path, BYTES).await.unwrap();
    common::wait_for_file(&alice_path, BYTES).await;

    // --- Act 2: watcher-side forget (bob's hash for this path came
    // from his own `record_local_publish` when his watcher published
    // the act-1 write). Bob deletes locally — his watcher's
    // `on_removed` fires and must forget the hash — then re-creates
    // the identical bytes; alice must see the file come back.
    //
    // The deleting node is bob, NOT alice, on purpose. Act 1 ended
    // with alice's applier re-creating alice's copy, and on Linux a
    // Remove arriving while that Create is still queued in the same
    // debounce window is annihilated by design — the debouncer's
    // model is "created then removed within one window = never
    // existed" (notify-debouncer-full 0.6, test case
    // `add_remove_event_after_create_event.hjson`: Remove after
    // queued Create ⇒ expected: {}); no event fires, no tombstone is
    // ever published, and the test deadlocks (this PR's first ubuntu
    // CI run captured exactly that — macOS was immune because
    // FSEvents' post-unlink Modify events reach the on_modified
    // NotFound→tombstone fallback regardless). Bob's side has no
    // such queued Create: act 1's wait_for_file(alice_path) proves
    // his watcher already flushed and published it — the file cannot
    // reach alice otherwise — so his delete is from steady state by
    // construction, no settling sleep needed.
    eprintln!("--- act 2: local delete on joiner, re-creates identical bytes ---");
    tokio::fs::remove_file(&bob_path).await.unwrap();
    common::wait_for_missing(&alice_path).await;

    tokio::fs::write(&bob_path, BYTES).await.unwrap();
    common::wait_for_file(&alice_path, BYTES).await;

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
// An empty file in the workspace must not propagate as a doc
// entry — iroh-docs reserves zero-length entries for tombstones,
// so a naive `set_bytes(_, _, &[])` is rejected with
// `"Attempted to insert an empty entry"`. The watcher silently
// skips empty files; once the user puts content into the file,
// the next debounced event picks it up.
//
// This test pins the contract at a single-host level (no joiner
// needed) by:
// 1. Spawning a workspace with a draining event-stream consumer
//    that records any [`WorkspaceEvent::Error`] it sees.
// 2. `touch`ing a fresh file inside the workspace; verifying that
//    no error is published and that the doc has no entry for it.
// 3. Writing content into the same file; verifying that the doc
//    *now* has an entry, proving the skip is transient and not a
//    permanent block on the path.
//
// TODO(empty-files): once we have a doc-layer encoding for genuine
// zero-length files (e.g. an inline-marker entry), this test should
// flip to assert the empty file *does* sync.
// =============================================================

const EMPTY_FILE_POLL: Duration = Duration::from_millis(50);

/// Minimal harness: one daemon, one client, one host workspace.
/// The joiner side is irrelevant for this property — empty-file
/// rejection is purely about the host's watcher → `set_bytes` path.
/// Returns the [`Arc<DnsPkarrServer>`] so the caller can keep it
/// alive for the workspace's lifetime — dropping it shuts down the
/// localhost pkarr+DNS pair the workspace endpoint needs.
async fn spawn_host_workspace_for_empty_test() -> (
    common::RunningDaemon,
    Client,
    Arc<Workspace>,
    tokio::task::JoinHandle<()>,
    mpsc::Receiver<WorkspaceEvent>,
    TempDir,
    Arc<DnsPkarrServer>,
) {
    let dns_pkarr = Arc::new(
        DnsPkarrServer::run_with_origin(artel_fs::TEST_DNS_ORIGIN.to_string())
            .await
            .expect("dns_pkarr"),
    );
    let daemon =
        common::spawn_daemon_with_setup(common::fresh_state(), daemon_testing_setup(&dns_pkarr))
            .await;
    let client = Client::connect(&daemon.socket).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cfg = WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr));
    let (ws, events) = Workspace::host_with(
        &client,
        "host",
        dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        cfg,
    )
    .await
    .expect("Workspace::host_with");
    let ws = Arc::new(ws);
    let handle = Arc::clone(&ws).run().await;
    (daemon, client, ws, handle, events, dir, dns_pkarr)
}

/// Drain `events` into a thread-shared `Vec<String>` of error
/// messages. Returns the join handle so the test can keep running
/// while the consumer task lives.
fn collect_errors(
    mut events: mpsc::Receiver<WorkspaceEvent>,
) -> Arc<tokio::sync::Mutex<Vec<String>>> {
    let errors = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
    let errors_for_task = Arc::clone(&errors);
    tokio::spawn(async move {
        while let Some(ev) = events.recv().await {
            if let WorkspaceEvent::Error(msg) = ev {
                errors_for_task.lock().await.push(msg);
            }
        }
    });
    errors
}

async fn doc_has_entry_at(ws: &Workspace, path: &Path) -> bool {
    // `ws.root` is canonicalised by `host_with`; tests build `path` from
    // raw `TempDir` paths, which on macOS live under `/var/...` that
    // canonicalises to `/private/var/...`. Canonicalise here so
    // `path_to_key`'s strip_prefix matches the root. Fall back to the
    // raw path when canonicalisation fails (e.g. an intentionally-absent
    // file): such a path can't be in the doc, so the lookup is false.
    let resolved = tokio::fs::canonicalize(path)
        .await
        .unwrap_or_else(|_| path.to_path_buf());
    let key = path_to_key(ws.root.as_path(), &resolved).expect("path_to_key");
    let stream = ws
        .doc()
        .get_many(Query::key_exact(key))
        .await
        .expect("get_many");
    tokio::pin!(stream);
    stream.next().await.is_some()
}

async fn wait_for_doc_entry(ws: &Workspace, path: &Path, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if doc_has_entry_at(ws, path).await {
            return true;
        }
        sleep(EMPTY_FILE_POLL).await;
    }
    false
}

// =============================================================
// Same-seed author binding (Slice 1): the workspace's doc author is
// seeded from its endpoint key, so AuthorId == endpoint_id and every
// authored entry carries that id. This is what lets the host resolve
// entry.author → daemon PeerId via the peer_map (no announcement).
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn author_id_equals_endpoint_id_and_stamps_entries() {
    // `_dir` (the TempDir) is held only to keep the workspace root alive
    // for the test; paths are built from `ws.root` (the canonical form).
    let (daemon, _client, ws, handle, _events, _dir, _dns_pkarr) =
        spawn_host_workspace_for_empty_test().await;

    // (1) The author the workspace stamps equals its endpoint id.
    let endpoint_id = ws
        .test_endpoint_id_bytes()
        .await
        .expect("node live: endpoint id available");
    assert_eq!(
        ws.author().as_bytes(),
        &endpoint_id,
        "same-seed binding: AuthorId must equal endpoint_id",
    );

    // (2) A real authored entry carries that same author — proving the
    // binding holds on the write path, not just at construction.
    // Build the path from `ws.root` (canonicalised by `host_with`), not
    // the raw `dir.path()`: on macOS the tempdir is under `/var/...`
    // which canonicalises to `/private/var/...`, so a `dir.path()`-based
    // path fails `path_to_key`'s strip_prefix against the canonical root.
    let file = ws.root.join("authored.txt");
    tokio::fs::write(&file, b"by me").await.unwrap();
    assert!(
        wait_for_doc_entry(&ws, &file, WAIT_BUDGET).await,
        "authored entry never landed in the doc",
    );
    let key = path_to_key(ws.root.as_path(), &file).expect("path_to_key");
    let stream = ws
        .doc()
        .get_many(Query::key_exact(key))
        .await
        .expect("get_many");
    tokio::pin!(stream);
    let entry = stream
        .next()
        .await
        .expect("entry present")
        .expect("entry ok");
    assert_eq!(
        entry.author().as_bytes(),
        &endpoint_id,
        "authored entry's author must be the same-seed endpoint id",
    );

    ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), handle).await;
    daemon.stop().await;
}

// =============================================================
// Auto-rotation on Evict (Slice 3e): a plain `Revoke` (no manual
// rotate call) makes the host's cap-listener auto-trigger the rotation
// task — rotate the namespace, distribute the new ticket to survivors,
// and reimport locally. After the evict, the evicted peer (still
// holding the OLD secret, watcher live) can no longer get writes to the
// host, and the host operates on the rotated namespace.
//
// This is the end-to-end write cut-off: the security goal of the whole
// feature, driven only by the Revoke verb.
// =============================================================

#[allow(clippy::too_many_lines)]
async fn evict_auto_rotation_body(dns_pkarr: Arc<DnsPkarrServer>, pair: Pair) {
    use artel_protocol::capability::Capability;

    let Pair {
        daemon_a, daemon_b, ..
    } = pair;

    // Alice hosts.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_a.socket.clone()),
    )
    .await
    .expect("Workspace::host");
    let session = alice_ws.session_id();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // Bob joins (Read), is promoted to RW.
    let read_ticket = match alice
        .request(Request::IssueTicket {
            session,
            granted_cap: Capability::Read,
            expiry_ms: 0,
        })
        .await
        .unwrap()
    {
        Response::IssuedTicket { ticket, .. } => ticket,
        other => panic!("expected IssuedTicket, got {other:?}"),
    };
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let bob_peer = bob.daemon_peer_id();
    assert!(matches!(
        bob.request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: read_ticket,
        })
        .await
        .unwrap(),
        Response::JoinSession { .. }
    ));
    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, _) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_b.socket.clone()),
    )
    .await
    .expect("Workspace::join");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    common::grant_rw_and_wait(&alice, session, bob_peer, bob_dir.path(), alice_dir.path()).await;

    // Confirm Bob (RW) propagates to Alice pre-evict.
    tokio::fs::write(bob_dir.path().join("pre.txt"), b"pre")
        .await
        .unwrap();
    common::wait_for_file(&alice_dir.path().join("pre.txt"), b"pre").await;
    let ns_before = alice_ws.test_current_namespace_bytes();

    // EVICT Bob — plain Revoke. The host cap-listener auto-triggers
    // rotation; no manual rotate call.
    common::revoke(&alice, session, bob_peer).await;

    // Wait until Alice has actually rotated (current namespace changed).
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if alice_ws.test_current_namespace_bytes() != ns_before {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "host never auto-rotated after Evict",
        );
        sleep(POLL_INTERVAL).await;
    }

    // Bob, still holding the OLD secret with a live watcher, writes
    // again. This must NOT reach Alice (her namespace rotated + the
    // PeerFilter blocks Bob).
    tokio::fs::write(bob_dir.path().join("post.txt"), b"post")
        .await
        .unwrap();

    // Alice writes a sentinel into her NEW namespace; she sees her own
    // write (proving her watcher is live on the rotated namespace).
    tokio::fs::write(alice_dir.path().join("sentinel.txt"), b"s")
        .await
        .unwrap();
    assert!(
        wait_for_doc_entry(
            &alice_ws,
            &alice_dir.path().join("sentinel.txt"),
            WAIT_BUDGET
        )
        .await,
        "host's post-rotation write never landed in the new namespace",
    );

    // Give any (illegitimate) propagation a generous window, then assert
    // Bob's post-evict write never reached Alice's disk.
    sleep(Duration::from_secs(2)).await;
    let leaked = tokio::fs::try_exists(alice_dir.path().join("post.txt"))
        .await
        .unwrap_or(false);
    assert!(
        !leaked,
        "evicted peer's post-rotation write reached the host — write cut-off broken",
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
async fn evict_auto_rotates_and_cuts_off_writes() {
    let pair = spawn_pair().await;
    let dns_pkarr = Arc::clone(&pair.dns_pkarr);
    // Box: the body's future sits at clippy::large_futures' 16 KiB
    // threshold on newer toolchains (CI's 1.96 flags it; 1.95 doesn't).
    Box::pin(evict_auto_rotation_body(dns_pkarr, pair)).await;
}

// Real-n0 variant of the auto-rotation write cut-off (Tier C). Binds
// against n0's public relay (`Production`) rather than the localhost
// DnsPkarrServer — see the matching note in `workspace_restart.rs` for
// why Tier C uses the public relay until the iroh 1.0 upgrade. Filtered
// out of the default profile by the `_n0` suffix; runs under
// `make test-n0` / the `n0` nextest profile.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::large_futures, clippy::too_many_lines)]
async fn evict_auto_rotates_and_cuts_off_writes_n0() {
    use artel_protocol::capability::Capability;

    common::init_tracing();

    // Hold the daemon-root TempDirs for the test's lifetime (dropping
    // them would delete the dir out from under the daemon).
    let alice_daemon_root = TempDir::new().unwrap();
    let bob_daemon_root = TempDir::new().unwrap();
    let alice_daemon = common::spawn_daemon_at(
        &common::DaemonPaths::at(alice_daemon_root.path()),
        artel_daemon::EndpointSetup::Production,
    )
    .await;
    let bob_daemon = common::spawn_daemon_at(
        &common::DaemonPaths::at(bob_daemon_root.path()),
        artel_daemon::EndpointSetup::Production,
    )
    .await;

    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, alice_ev) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(artel_fs::EndpointSetup::Production)
            .with_daemon_socket(alice_daemon.socket.clone()),
    )
    .await
    .expect("host");
    common::drain_ws_events(alice_ev);
    let session = alice_ws.session_id();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let read_ticket = match alice
        .request(Request::IssueTicket {
            session,
            granted_cap: Capability::Read,
            expiry_ms: 0,
        })
        .await
        .unwrap()
    {
        Response::IssuedTicket { ticket, .. } => ticket,
        other => panic!("expected IssuedTicket, got {other:?}"),
    };
    let bob = Client::connect(&bob_daemon.socket).await.unwrap();
    let bob_peer = bob.daemon_peer_id();
    assert!(matches!(
        bob.request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: read_ticket,
        })
        .await
        .unwrap(),
        Response::JoinSession { .. }
    ));
    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, bob_ev) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(artel_fs::EndpointSetup::Production)
            .with_daemon_socket(bob_daemon.socket.clone()),
    )
    .await
    .expect("join");
    common::drain_ws_events(bob_ev);
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    common::grant_rw_and_wait(&alice, session, bob_peer, bob_dir.path(), alice_dir.path()).await;

    tokio::fs::write(bob_dir.path().join("pre.txt"), b"pre")
        .await
        .unwrap();
    common::wait_for_file(&alice_dir.path().join("pre.txt"), b"pre").await;
    let ns_before = alice_ws.test_current_namespace_bytes();

    common::revoke(&alice, session, bob_peer).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if alice_ws.test_current_namespace_bytes() != ns_before {
            break;
        }
        assert!(Instant::now() < deadline, "host never auto-rotated (n0)");
        sleep(POLL_INTERVAL).await;
    }

    tokio::fs::write(bob_dir.path().join("post.txt"), b"post")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("sentinel.txt"), b"s")
        .await
        .unwrap();
    assert!(
        wait_for_doc_entry(
            &alice_ws,
            &alice_dir.path().join("sentinel.txt"),
            WAIT_BUDGET
        )
        .await,
        "host post-rotation write never landed (n0)",
    );
    sleep(Duration::from_secs(3)).await;
    assert!(
        !tokio::fs::try_exists(alice_dir.path().join("post.txt"))
            .await
            .unwrap_or(false),
        "evicted peer's write reached host after rotation (n0)",
    );

    alice_ws.shutdown().await.unwrap();
    bob_ws.shutdown().await.unwrap();
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    alice_daemon.stop().await;
    bob_daemon.stop().await;
}

// =============================================================
// Survivor follows rotation (Slice 3e, 3 peers): host + a kept RW
// survivor + an evicted RW peer. After evicting the bad peer, the
// surviving peer auto-reimports onto the rotated namespace and keeps
// round-tripping with the host, while the evicted peer is cut off.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines, clippy::items_after_statements)]
async fn survivor_follows_rotation_evicted_is_cut() {
    use artel_protocol::capability::Capability;

    // Three daemons sharing one DnsPkarrServer.
    let pair = spawn_pair().await;
    let dns_pkarr = Arc::clone(&pair.dns_pkarr);
    let daemon_c = common::spawn_daemon_with_setup(
        common::fresh_state(),
        common::daemon_testing_setup(&dns_pkarr),
    )
    .await;
    common::wait_for_endpoint(&dns_pkarr, &daemon_c.iroh_addr.as_ref().expect("addr").id).await;
    let Pair {
        daemon_a, daemon_b, ..
    } = pair;

    // Alice hosts.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_a.socket.clone()),
    )
    .await
    .expect("host");
    let session = alice_ws.session_id();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // Helper to join a daemon as an RW peer.
    async fn join_rw(
        host: &Client,
        session: SessionId,
        daemon: &common::RunningDaemon,
        dns_pkarr: &Arc<DnsPkarrServer>,
        name: &str,
    ) -> (
        Client,
        std::sync::Arc<Workspace>,
        tokio::task::JoinHandle<()>,
        TempDir,
    ) {
        let ticket = match host
            .request(Request::IssueTicket {
                session,
                granted_cap: Capability::Read,
                expiry_ms: 0,
            })
            .await
            .unwrap()
        {
            Response::IssuedTicket { ticket, .. } => ticket,
            other => panic!("expected IssuedTicket, got {other:?}"),
        };
        let client = Client::connect(&daemon.socket).await.unwrap();
        assert!(matches!(
            client
                .request(Request::JoinSession {
                    display_name: name.into(),
                    ticket,
                })
                .await
                .unwrap(),
            Response::JoinSession { .. }
        ));
        let dir = tempfile::tempdir().unwrap();
        let (ws, _) = Workspace::join_with(
            &client,
            session,
            dir.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            WorkspaceConfig::default()
                .with_endpoint_setup(testing_setup(dns_pkarr))
                .with_daemon_socket(daemon.socket.clone()),
        )
        .await
        .expect("join");
        let ws = Arc::new(ws);
        let handle = Arc::clone(&ws).run().await;
        (client, ws, handle, dir)
    }

    let (survivor_cli, survivor_ws, survivor_handle, survivor_dir) =
        join_rw(&alice, session, &daemon_b, &dns_pkarr, "survivor").await;
    let survivor_peer = survivor_cli.daemon_peer_id();
    let (evicted_cli, evicted_ws, evicted_handle, evicted_dir) =
        join_rw(&alice, session, &daemon_c, &dns_pkarr, "evicted").await;
    let evicted_peer = evicted_cli.daemon_peer_id();

    // Grant RW to each peer and wait for it. `grant_rw_and_wait`'s
    // probe filename embeds the peer id, so consecutive grants don't
    // race on a shared probe path.
    common::grant_rw_and_wait(
        &alice,
        session,
        survivor_peer,
        survivor_dir.path(),
        alice_dir.path(),
    )
    .await;
    common::grant_rw_and_wait(
        &alice,
        session,
        evicted_peer,
        evicted_dir.path(),
        alice_dir.path(),
    )
    .await;

    let ns_before = alice_ws.test_current_namespace_bytes();

    // Evict the bad peer. Host auto-rotates.
    common::revoke(&alice, session, evicted_peer).await;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if alice_ws.test_current_namespace_bytes() != ns_before {
            break;
        }
        assert!(Instant::now() < deadline, "host never auto-rotated");
        sleep(POLL_INTERVAL).await;
    }

    // The survivor must follow the rotation: a write from the survivor
    // after rotation reaches the host on the NEW namespace. Poll a
    // probe (the survivor needs a beat to receive + reimport).
    let probe = survivor_dir.path().join("survivor_after.txt");
    let host_sees = alice_dir.path().join("survivor_after.txt");
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let _ = tokio::fs::write(&probe, b"survivor-rw").await;
        sleep(POLL_INTERVAL).await;
        if tokio::fs::read(&host_sees)
            .await
            .is_ok_and(|b| b == b"survivor-rw")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "survivor's post-rotation write never reached host — survivor didn't follow rotation",
        );
    }

    // The evicted peer (old secret, live watcher) stays cut off.
    tokio::fs::write(evicted_dir.path().join("evicted_after.txt"), b"nope")
        .await
        .unwrap();
    sleep(Duration::from_secs(2)).await;
    assert!(
        !tokio::fs::try_exists(alice_dir.path().join("evicted_after.txt"))
            .await
            .unwrap_or(false),
        "evicted peer's write reached the host after rotation",
    );

    alice_ws.shutdown().await.unwrap();
    survivor_ws.shutdown().await.unwrap();
    evicted_ws.shutdown().await.unwrap();
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), survivor_handle).await;
    let _ = timeout(Duration::from_secs(5), evicted_handle).await;
    drop(alice);
    drop(survivor_cli);
    drop(evicted_cli);
    daemon_a.stop().await;
    daemon_b.stop().await;
    daemon_c.stop().await;
}

// =============================================================
// Post-rotation join + promotion (C1): after a rotation the host must
// refresh its durable distribution state so a peer that joins LATER
// lands on the rotated namespace (not the abandoned genesis), and a
// peer promoted to RW AFTER the rotation receives the rotated secret
// (not the stale genesis one). Without the fix the late joiner imports
// the frozen genesis namespace and a post-rotation promotion hands out
// a worthless secret.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn late_joiner_lands_on_rotated_namespace_and_can_be_promoted() {
    use artel_protocol::capability::Capability;

    // Three daemons sharing one DnsPkarrServer: alice hosts, bob is the
    // peer we evict to force a rotation, carol joins afterwards.
    let pair = spawn_pair().await;
    let dns_pkarr = Arc::clone(&pair.dns_pkarr);
    let daemon_c = common::spawn_daemon_with_setup(
        common::fresh_state(),
        common::daemon_testing_setup(&dns_pkarr),
    )
    .await;
    common::wait_for_endpoint(&dns_pkarr, &daemon_c.iroh_addr.as_ref().expect("addr").id).await;
    let Pair {
        daemon_a, daemon_b, ..
    } = pair;

    // Alice hosts.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, alice_ev) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_a.socket.clone()),
    )
    .await
    .expect("host");
    common::drain_ws_events(alice_ev);
    let session = alice_ws.session_id();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // Bob joins (Read ticket) and is granted RW so an Evict triggers a
    // real rotation.
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let bob_peer = bob.daemon_peer_id();
    let bob_dir = tempfile::tempdir().unwrap();
    let read_ticket = match alice
        .request(Request::IssueTicket {
            session,
            granted_cap: Capability::Read,
            expiry_ms: 0,
        })
        .await
        .unwrap()
    {
        Response::IssuedTicket { ticket, .. } => ticket,
        other => panic!("expected IssuedTicket, got {other:?}"),
    };
    assert!(matches!(
        bob.request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: read_ticket,
        })
        .await
        .unwrap(),
        Response::JoinSession { .. }
    ));
    let (bob_ws, bob_ev) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_b.socket.clone()),
    )
    .await
    .expect("bob join");
    common::drain_ws_events(bob_ev);
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;
    common::grant_rw_and_wait(&alice, session, bob_peer, bob_dir.path(), alice_dir.path()).await;

    // Evict bob → host auto-rotates.
    let ns_before = alice_ws.test_current_namespace_bytes();
    common::revoke(&alice, session, bob_peer).await;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if alice_ws.test_current_namespace_bytes() != ns_before {
            break;
        }
        assert!(Instant::now() < deadline, "host never auto-rotated");
        sleep(POLL_INTERVAL).await;
    }

    // Host writes content AFTER the rotation — this lives only in the
    // rotated namespace.
    tokio::fs::write(alice_dir.path().join("after_rotation.txt"), b"fresh")
        .await
        .unwrap();
    assert!(
        wait_for_doc_entry(
            &alice_ws,
            &alice_dir.path().join("after_rotation.txt"),
            WAIT_BUDGET
        )
        .await,
        "host post-rotation write never landed in the new namespace",
    );

    // Carol joins LATE (after the rotation). She must land on the
    // rotated namespace and bulk-export the post-rotation content — if
    // she imported the abandoned genesis, after_rotation.txt would never
    // appear.
    let carol = Client::connect(&daemon_c.socket).await.unwrap();
    let carol_peer = carol.daemon_peer_id();
    let carol_dir = tempfile::tempdir().unwrap();
    let carol_ticket = match alice
        .request(Request::IssueTicket {
            session,
            granted_cap: Capability::Read,
            expiry_ms: 0,
        })
        .await
        .unwrap()
    {
        Response::IssuedTicket { ticket, .. } => ticket,
        other => panic!("expected IssuedTicket, got {other:?}"),
    };
    assert!(matches!(
        carol
            .request(Request::JoinSession {
                display_name: "carol".into(),
                ticket: carol_ticket,
            })
            .await
            .unwrap(),
        Response::JoinSession { .. }
    ));
    let (carol_ws, carol_ev) = Workspace::join_with(
        &carol,
        session,
        carol_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_c.socket.clone()),
    )
    .await
    .expect("carol join");
    common::drain_ws_events(carol_ev);
    let carol_ws = Arc::new(carol_ws);
    let carol_handle = Arc::clone(&carol_ws).run().await;

    common::wait_for_file(&carol_dir.path().join("after_rotation.txt"), b"fresh").await;

    // Now promote carol to RW AFTER the rotation: she must receive the
    // ROTATED secret (not the stale genesis one), so her write reaches
    // the host on the live namespace.
    common::grant_rw(&alice, session, carol_peer).await;
    let probe = carol_dir.path().join("carol_rw.txt");
    let host_sees = alice_dir.path().join("carol_rw.txt");
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let _ = tokio::fs::write(&probe, b"carol-rw").await;
        sleep(POLL_INTERVAL).await;
        if tokio::fs::read(&host_sees)
            .await
            .is_ok_and(|b| b == b"carol-rw")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "post-rotation promotion: carol's RW write never reached host — stale secret",
        );
    }

    alice_ws.shutdown().await.unwrap();
    bob_ws.shutdown().await.unwrap();
    carol_ws.shutdown().await.unwrap();
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    let _ = timeout(Duration::from_secs(5), carol_handle).await;
    drop(alice);
    drop(bob);
    drop(carol);
    daemon_a.stop().await;
    daemon_b.stop().await;
    daemon_c.stop().await;
}

// =============================================================
// Namespace re-import (Slice 3d): after rotation, re-importing onto the
// new namespace swaps the live doc, respawns the watcher/applier
// against it, and keeps the workspace operational — a write made AFTER
// re-import lands in the NEW namespace, and the genesis-derived
// SessionId is unchanged.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn reimport_swaps_namespace_and_keeps_workspace_live() {
    // `_dir` held only for liveness; paths use `ws.root` (canonical) so
    // `path_to_key` strip_prefix works on macOS (/var -> /private/var).
    let (daemon, _client, ws, handle, _events, _dir, _dns_pkarr) =
        spawn_host_workspace_for_empty_test().await;

    let session_before = ws.session_id();

    // Seed a file and wait for it into the doc.
    tokio::fs::write(ws.root.join("kept.txt"), b"keep me")
        .await
        .unwrap();
    assert!(
        wait_for_doc_entry(&ws, &ws.root.join("kept.txt"), WAIT_BUDGET).await,
        "seed file never landed pre-rotation",
    );

    let ns_before = ws.test_current_namespace_bytes();

    // Rotate + re-import onto the new namespace (host self-rotation).
    let ns_after = ws
        .test_rotate_and_reimport(0)
        .await
        .expect("rotate_and_reimport");

    assert_ne!(
        ns_before, ns_after,
        "current namespace must change after rotation",
    );
    assert_eq!(
        ws.test_current_namespace_bytes(),
        ns_after,
        "live doc must now be the rotated namespace",
    );
    // SessionId is genesis-derived, so it must be unchanged by rotation.
    assert_eq!(
        ws.session_id(),
        session_before,
        "SessionId must be stable across rotation (genesis-derived)",
    );

    // The respawned watcher must be live on the new namespace: a write
    // made AFTER re-import lands in the new doc.
    tokio::fs::write(ws.root.join("post_rotate.txt"), b"after")
        .await
        .unwrap();
    assert!(
        wait_for_doc_entry(&ws, &ws.root.join("post_rotate.txt"), WAIT_BUDGET).await,
        "post-rotation write never landed — respawned watcher not live on new namespace",
    );
    // And the carried-forward entry is present in the new namespace.
    let keys = ws.test_namespace_keys(ns_after).await;
    assert!(
        keys.iter().any(|k| k.ends_with("kept.txt")),
        "rotated namespace must carry forward kept.txt; keys={keys:?}",
    );

    ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), handle).await;
    daemon.stop().await;
}

// =============================================================
// Reimport catch-up scan (rotation dead-window regression): a host
// write that lands on disk during the watcher teardown→reattach window
// of a rotation must still reach the rotated namespace.
//
// The bug: `reimport_namespace` cancels the old watcher, swaps the doc,
// then respawns a forward-only watcher. `notify` does no initial scan
// on attach, so a file that already exists when the new watch attaches
// produces no event — ever. If that file also wasn't published to the
// OLD namespace before the rotation snapshot, it is in neither the
// carried-forward survivor set nor any future watcher event, and is
// silently lost from the live namespace. This surfaced under load as
// `evict_auto_rotates_and_cuts_off_writes` /
// `late_joiner_lands_on_rotated_namespace_and_can_be_promoted` failing
// with "host post-rotation write never landed in the new namespace".
//
// This reproduces the window deterministically WITHOUT relying on load:
// write `unpublished.txt`, then rotate *within* the 300ms watcher
// debounce so the file is provably absent from the old doc's rotation
// snapshot (not a survivor) and pre-exists the respawned watch (no
// event). The catch-up scan in `reimport_namespace` is the only path
// that can carry it into the rotated namespace; without it this test
// fails.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn reimport_catch_up_scan_recovers_write_in_rotation_window() {
    // `_dir` held only for liveness; paths use `ws.root` (canonical) so
    // `path_to_key` strip_prefix works on macOS (/var -> /private/var).
    let (daemon, _client, ws, handle, _events, _dir, _dns_pkarr) =
        spawn_host_workspace_for_empty_test().await;

    // A survivor that IS published before rotation, so we also confirm
    // the catch-up scan doesn't disturb the carry-forward path.
    tokio::fs::write(ws.root.join("kept.txt"), b"keep me")
        .await
        .unwrap();
    assert!(
        wait_for_doc_entry(&ws, &ws.root.join("kept.txt"), WAIT_BUDGET).await,
        "seed file never landed pre-rotation",
    );

    // Write a file but do NOT wait for the watcher's 300ms debounce to
    // publish it, then rotate immediately. The rotation snapshot reads
    // the old doc synchronously, well within the debounce window, so
    // `unpublished.txt` is provably NOT a carried-forward survivor — and
    // because it already exists on disk when the respawned watcher
    // attaches, no filesystem event will ever publish it. The catch-up
    // scan is the only thing that can land it in the new namespace.
    tokio::fs::write(ws.root.join("unpublished.txt"), b"in the window")
        .await
        .unwrap();
    let ns_after = ws
        .test_rotate_and_reimport(0)
        .await
        .expect("rotate_and_reimport");

    // Both files must be present in the rotated namespace: kept.txt via
    // the survivor carry-forward, unpublished.txt via the catch-up scan.
    assert!(
        wait_for_doc_entry(&ws, &ws.root.join("unpublished.txt"), WAIT_BUDGET).await,
        "write in the rotation dead-window never reached the rotated \
         namespace — catch-up scan missing",
    );
    let keys = ws.test_namespace_keys(ns_after).await;
    assert!(
        keys.iter().any(|k| k.ends_with("kept.txt")),
        "rotated namespace must still carry forward kept.txt; keys={keys:?}",
    );
    assert!(
        keys.iter().any(|k| k.ends_with("unpublished.txt")),
        "rotated namespace must contain the dead-window write; keys={keys:?}",
    );

    ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), handle).await;
    daemon.stop().await;
}

// =============================================================
// Epoch persistence (C2 regression): namespace_epoch must survive a
// host restart. Without on-disk persistence the counter resets to 0,
// and a second eviction re-mints epoch 1 — which every survivor that
// already reached epoch 1 then *ignores* (stale/duplicate guard),
// silently stranding it on the pre-rotation namespace.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn namespace_epoch_survives_host_restart() {
    let dns_pkarr = Arc::new(
        DnsPkarrServer::run_with_origin(artel_fs::TEST_DNS_ORIGIN.to_string())
            .await
            .expect("dns_pkarr"),
    );
    // Content root + state dir live in tempdirs that outlive both
    // host lifetimes so the restart re-opens the same on-disk state.
    let root = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();

    let epoch_after_rotation = {
        let daemon = common::spawn_daemon_with_setup(
            common::fresh_state(),
            daemon_testing_setup(&dns_pkarr),
        )
        .await;
        let client = Client::connect(&daemon.socket).await.unwrap();
        let cfg = WorkspaceConfig::default()
            .with_state_dir(state.path().to_path_buf())
            .with_endpoint_setup(testing_setup(&dns_pkarr));
        let (ws, events) = Workspace::host_with(
            &client,
            "host",
            root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            cfg,
        )
        .await
        .expect("host phase 1");
        common::drain_ws_events(events);
        let ws = Arc::new(ws);
        let handle = Arc::clone(&ws).run().await;

        assert_eq!(ws.namespace_epoch(), 0, "fresh host starts at epoch 0");
        // Host self-rotation: bumps the epoch to 1 and persists it.
        ws.test_rotate_and_reimport(0)
            .await
            .expect("rotate_and_reimport");
        let epoch = ws.namespace_epoch();
        assert_eq!(epoch, 1, "rotation bumps the live epoch to 1");

        ws.shutdown().await.expect("shutdown phase 1");
        let _ = timeout(Duration::from_secs(5), handle).await;
        daemon.stop().await;
        epoch
    };
    assert_eq!(epoch_after_rotation, 1);

    // Phase 2: re-host the same state dir under a fresh daemon. The
    // epoch must be recovered from disk, not reset to 0.
    let daemon =
        common::spawn_daemon_with_setup(common::fresh_state(), daemon_testing_setup(&dns_pkarr))
            .await;
    let client = Client::connect(&daemon.socket).await.unwrap();
    let cfg = WorkspaceConfig::default()
        .with_state_dir(state.path().to_path_buf())
        .with_endpoint_setup(testing_setup(&dns_pkarr));
    let (ws, events) = Workspace::host_with(
        &client,
        "host",
        root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        cfg,
    )
    .await
    .expect("host phase 2");
    common::drain_ws_events(events);
    let ws = Arc::new(ws);
    let handle = Arc::clone(&ws).run().await;

    assert_eq!(
        ws.namespace_epoch(),
        1,
        "namespace_epoch must be recovered from disk after restart, not reset to 0",
    );

    ws.shutdown().await.expect("shutdown phase 2");
    let _ = timeout(Duration::from_secs(5), handle).await;
    daemon.stop().await;
}

// =============================================================
// Replayed-revoke idempotency (C3): when a host workspace restarts, its
// cap-listener re-subscribes from scratch and the daemon replays the
// session log — including historical Revoke messages. A replayed Revoke
// must NOT re-fire the rotation: the namespace + epoch must stay put.
// Without the fix, every past eviction re-rotates on each restart.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn replayed_revoke_does_not_re_rotate_on_workspace_restart() {
    use artel_protocol::capability::Capability;

    let dns_pkarr = Arc::new(
        DnsPkarrServer::run_with_origin(artel_fs::TEST_DNS_ORIGIN.to_string())
            .await
            .expect("dns_pkarr"),
    );
    // Alice's daemon stays up across the workspace restart, so its
    // session log (with the Revoke) persists. Alice's workspace state
    // dir is persistent so the re-host resumes the same session.
    let alice_daemon =
        common::spawn_daemon_with_setup(common::fresh_state(), daemon_testing_setup(&dns_pkarr))
            .await;
    let bob_daemon =
        common::spawn_daemon_with_setup(common::fresh_state(), daemon_testing_setup(&dns_pkarr))
            .await;

    let alice_dir = tempfile::tempdir().unwrap();
    let alice_state = tempfile::tempdir().unwrap();

    let session;
    let epoch_after_evict;
    let ns_after_evict;
    {
        let alice = Client::connect(&alice_daemon.socket).await.unwrap();
        let (alice_ws, alice_ev) = Workspace::host_with(
            &alice,
            "alice",
            alice_dir.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            WorkspaceConfig::default()
                .with_state_dir(alice_state.path().to_path_buf())
                .with_endpoint_setup(testing_setup(&dns_pkarr))
                .with_daemon_socket(alice_daemon.socket.clone()),
        )
        .await
        .expect("host phase 1");
        common::drain_ws_events(alice_ev);
        session = alice_ws.session_id();
        let alice_ws = Arc::new(alice_ws);
        let alice_handle = Arc::clone(&alice_ws).run().await;

        // Bob joins + RW, then evict → one rotation (epoch 1).
        let bob = Client::connect(&bob_daemon.socket).await.unwrap();
        let bob_peer = bob.daemon_peer_id();
        let read_ticket = match alice
            .request(Request::IssueTicket {
                session,
                granted_cap: Capability::Read,
                expiry_ms: 0,
            })
            .await
            .unwrap()
        {
            Response::IssuedTicket { ticket, .. } => ticket,
            other => panic!("expected IssuedTicket, got {other:?}"),
        };
        assert!(matches!(
            bob.request(Request::JoinSession {
                display_name: "bob".into(),
                ticket: read_ticket,
            })
            .await
            .unwrap(),
            Response::JoinSession { .. }
        ));
        let bob_dir = tempfile::tempdir().unwrap();
        let (bob_ws, bob_ev) = Workspace::join_with(
            &bob,
            session,
            bob_dir.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            WorkspaceConfig::default()
                .with_endpoint_setup(testing_setup(&dns_pkarr))
                .with_daemon_socket(bob_daemon.socket.clone()),
        )
        .await
        .expect("bob join");
        common::drain_ws_events(bob_ev);
        let bob_ws = Arc::new(bob_ws);
        let bob_handle = Arc::clone(&bob_ws).run().await;
        common::grant_rw_and_wait(&alice, session, bob_peer, bob_dir.path(), alice_dir.path())
            .await;

        let ns_before = alice_ws.test_current_namespace_bytes();
        common::revoke(&alice, session, bob_peer).await;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if alice_ws.test_current_namespace_bytes() != ns_before {
                break;
            }
            assert!(Instant::now() < deadline, "host never auto-rotated");
            sleep(POLL_INTERVAL).await;
        }
        epoch_after_evict = alice_ws.namespace_epoch();
        ns_after_evict = alice_ws.test_current_namespace_bytes();
        assert_eq!(epoch_after_evict, 1, "one eviction ⇒ epoch 1");

        alice_ws.shutdown().await.unwrap();
        bob_ws.shutdown().await.unwrap();
        let _ = timeout(Duration::from_secs(5), alice_handle).await;
        let _ = timeout(Duration::from_secs(5), bob_handle).await;
        drop(bob);
    }

    // Re-host alice against the SAME state dir + SAME daemon. The
    // daemon replays the historical Revoke to the new cap-listener.
    let alice = Client::connect(&alice_daemon.socket).await.unwrap();
    let (alice_ws, alice_ev) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default()
            .with_state_dir(alice_state.path().to_path_buf())
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(alice_daemon.socket.clone()),
    )
    .await
    .expect("host phase 2");
    common::drain_ws_events(alice_ev);
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // Give the replayed Revoke ample time to (wrongly) drive a rotation.
    sleep(Duration::from_secs(3)).await;

    assert_eq!(
        alice_ws.namespace_epoch(),
        epoch_after_evict,
        "replayed Revoke must not bump the epoch (no spurious re-rotation)",
    );
    assert_eq!(
        alice_ws.test_current_namespace_bytes(),
        ns_after_evict,
        "replayed Revoke must not change the current namespace",
    );

    alice_ws.shutdown().await.unwrap();
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    drop(alice);
    alice_daemon.stop().await;
    bob_daemon.stop().await;
}

// =============================================================
// Rotation-signal losslessness (C4): a burst of evictions enqueues many
// HostEvict signals faster than the rotation task drains them. Each must
// drive a rotation — a dropped HostEvict would leave that evicted peer
// cryptographically un-cut (the security failure the feature exists to
// prevent). With an unbounded signal channel every revoke rotates, so
// the epoch advances by exactly the number of distinct evictions.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn burst_of_evictions_all_rotate_no_signal_dropped() {
    // Far exceed the old bounded buffer (16) so a lossy try_send would
    // drop signals. Each Revoke targets a distinct synthetic PeerId, so
    // each gets a strictly higher log seq and clears the C3 watermark —
    // a genuine rotation per revoke.
    const BURST: u64 = 40;

    let dns_pkarr = Arc::new(
        DnsPkarrServer::run_with_origin(artel_fs::TEST_DNS_ORIGIN.to_string())
            .await
            .expect("dns_pkarr"),
    );
    let daemon =
        common::spawn_daemon_with_setup(common::fresh_state(), daemon_testing_setup(&dns_pkarr))
            .await;
    let client = Client::connect(&daemon.socket).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    // Wire the daemon socket so the cap-listener observes the host's own
    // Capability revokes and drives the rotation task.
    let (ws, events) = Workspace::host_with(
        &client,
        "host",
        dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon.socket.clone()),
    )
    .await
    .expect("host_with");
    common::drain_ws_events(events);
    let ws = Arc::new(ws);
    let handle = Arc::clone(&ws).run().await;

    let session = ws.session_id();
    for i in 0..BURST {
        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&(i + 1).to_le_bytes());
        common::revoke(&client, session, PeerId::from_bytes(bytes)).await;
    }

    // The rotation task drains serially; wait for the epoch to settle at
    // exactly BURST. Unbounded ⇒ no signal lost ⇒ every revoke rotated.
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let epoch = ws.namespace_epoch();
        if epoch == BURST {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "epoch settled at {epoch}, expected {BURST} — a rotation signal was dropped",
        );
        sleep(POLL_INTERVAL).await;
    }

    ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), handle).await;
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn touching_empty_file_does_not_error_and_does_not_publish() {
    let (daemon, client, ws, handle, events, dir, _dns_pkarr) =
        spawn_host_workspace_for_empty_test().await;
    let errors = collect_errors(events);

    // `touch` an empty file. canonicalise the workspace's root
    // because macOS rewrites `/var/...` → `/private/var/...` and
    // path_to_key needs the canonical form.
    let canonical_dir = ws.root.clone();
    let empty_path = canonical_dir.join("empty.txt");
    tokio::fs::File::create(&empty_path)
        .await
        .expect("create empty file");

    // Wait long enough for the watcher debounce (300ms) and a few
    // ticks beyond, then assert no error fired and no doc entry
    // exists for the path.
    sleep(Duration::from_millis(800)).await;
    let recorded = errors.lock().await.clone();
    assert!(
        recorded.is_empty(),
        "watcher must not surface an error for an empty file; got: {recorded:?}",
    );
    assert!(
        !doc_has_entry_at(&ws, &empty_path).await,
        "empty file must not produce a doc entry yet — \
         iroh-docs would reject a zero-length insert as a tombstone",
    );

    // Now write content to the same file. The next debounced event
    // should publish it normally — proving the skip was transient.
    tokio::fs::write(&empty_path, b"now i have content")
        .await
        .expect("write content");
    assert!(
        wait_for_doc_entry(&ws, &empty_path, Duration::from_secs(5)).await,
        "after content arrives the watcher must publish — \
         the skip should not permanently block the path",
    );
    let after = errors.lock().await.clone();
    assert!(
        after.is_empty(),
        "watcher must not error after content publish either; got: {after:?}",
    );

    // Tidy up.
    ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), handle).await;
    drop(client);
    daemon.stop().await;
    let _ = dir;
}

// =============================================================
// A joiner imports the host's `DocTicket` from the artel session
// and bulk-exports the doc to disk.
//
// Two daemons (cross-seeded address books for the artel session
// traffic). Alice on daemon A hosts and stands a workspace up with
// two pre-existing files; Bob on daemon B joins the artel session,
// then calls `Workspace::join` which: subscribes, reads the
// `workspace.ticket` system message, imports the ticket into its
// own iroh node, and writes the doc contents into Bob's empty
// tempdir.
//
// No watcher / applier yet — this test only proves the bulk path.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn joiner_bulk_imports_host_files() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    // Alice on daemon A hosts the artel session.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    // Alice's workspace dir has two seed files.
    let alice_dir = tempfile::tempdir().unwrap();
    tokio::fs::write(alice_dir.path().join("a.txt"), b"alpha")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("b.txt"), b"beta")
        .await
        .unwrap();

    // Stand Alice's workspace up. This publishes the existing files
    // into the doc and broadcasts the ticket on the session.
    let (alice_ws, _alice_ws_events) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::host");
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();

    // Bob on daemon B joins the artel session.
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    // Bob stands his workspace up. Empty dir to start with.
    let bob_dir = tempfile::tempdir().unwrap();

    let (bob_ws, _bob_ws_events) = timeout(
        Duration::from_secs(45),
        Workspace::join_with(
            &bob,
            session,
            bob_dir.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
        ),
    )
    .await
    .expect("Workspace::join exceeded 45s")
    .expect("Workspace::join");

    // Bob's dir should now contain both seed files with the same
    // contents.
    let a = tokio::fs::read(bob_dir.path().join("a.txt"))
        .await
        .expect("a.txt readable");
    let b = tokio::fs::read(bob_dir.path().join("b.txt"))
        .await
        .expect("b.txt readable");
    assert_eq!(a, b"alpha");
    assert_eq!(b, b"beta");

    bob_ws.shutdown().await.expect("shutdown");
    alice_ws.shutdown().await.expect("shutdown");
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// `WorkspaceConfig::join_ticket_timeout` controls how long a joiner
// waits for the host's `workspace.ticket` system message before
// giving up.
//
// Two scenarios exercised here, both against a session whose host
// never calls `Workspace::host` (so no ticket is ever published):
//
// - With `Some(short)`: `Workspace::join_with` errors within roughly
//   the configured budget.
// - With `None`: `Workspace::join_with` stays pending — the future
//   hasn't resolved several seconds in. Long-lived joiners that
//   arrive minutes or hours after the host first published are the
//   use case here.
// =============================================================

struct JoinerSetup {
    daemon_a: common::RunningDaemon,
    daemon_b: common::RunningDaemon,
    bob: Client,
    session: SessionId,
    bob_dir: TempDir,
    bob_state: TempDir,
    dns_pkarr: Arc<DnsPkarrServer>,
}

async fn host_session_without_workspace() -> JoinerSetup {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let (session, ticket) = match alice
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
    // Deliberately *do not* call `Workspace::host` on alice — we
    // want the joiner's `wait_for_ticket` to find an empty session.
    drop(alice);

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
    let bob_state = tempfile::tempdir().unwrap();

    JoinerSetup {
        daemon_a,
        daemon_b,
        bob,
        session,
        bob_dir,
        bob_state,
        dns_pkarr,
    }
}

fn ticket_timeout_config(
    state: &TempDir,
    timeout_value: Option<Duration>,
    dns_pkarr: &Arc<DnsPkarrServer>,
) -> WorkspaceConfig {
    WorkspaceConfig::default()
        .with_state_dir(state.path().to_path_buf())
        .with_join_ticket_timeout(timeout_value)
        .with_endpoint_setup(testing_setup(dns_pkarr))
}

#[tokio::test(flavor = "multi_thread")]
async fn join_with_short_timeout_errors_when_no_ticket_published() {
    let JoinerSetup {
        daemon_a,
        daemon_b,
        bob,
        session,
        bob_dir,
        bob_state,
        dns_pkarr,
    } = host_session_without_workspace().await;

    let cfg = ticket_timeout_config(&bob_state, Some(Duration::from_millis(500)), &dns_pkarr);
    let started = Instant::now();
    let err = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        cfg,
    )
    .await
    .expect_err("must time out — no host ever published a ticket");
    let elapsed = started.elapsed();

    match err {
        WorkspaceError::Iroh(msg) if msg.contains("timed out waiting for workspace.ticket") => {}
        other => panic!("expected ticket-timeout error, got {other:?}"),
    }
    // Generous upper bound: 500ms budget + setup/scheduling slack.
    // The point is "errored quickly", not "errored at exactly 500ms".
    assert!(
        elapsed < Duration::from_secs(5),
        "join with 500ms timeout took {elapsed:?} — should have errored fast",
    );

    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
    let _ = bob_dir;
    let _ = bob_state;
}

#[tokio::test(flavor = "multi_thread")]
async fn join_with_no_timeout_stays_pending_when_no_ticket_published() {
    let JoinerSetup {
        daemon_a,
        daemon_b,
        bob,
        session,
        bob_dir,
        bob_state,
        dns_pkarr,
    } = host_session_without_workspace().await;

    let cfg = ticket_timeout_config(&bob_state, None, &dns_pkarr);
    let bob_dir_path = bob_dir.path().to_path_buf();
    let mut join_fut = Box::pin(Workspace::join_with(
        &bob,
        session,
        bob_dir_path,
        AttachPolicy::RequireEmpty,
        cfg,
    ));

    // Real wall-clock wait — `tokio::time::pause` would also pause
    // the daemon's internal timers and risk false positives. 3s is
    // enough to be meaningful (the old hard-coded ceiling was 15s,
    // recently bumped to 60s; wall-time evidence at 3s shows the
    // wait is genuinely unbounded).
    sleep(Duration::from_secs(3)).await;
    assert!(
        (&mut join_fut).now_or_never().is_none(),
        "join_with(timeout=None) must stay pending while no ticket is published",
    );

    // Cancel the join by dropping it; tear down cleanly.
    drop(join_fut);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
    let _ = bob_dir;
    let _ = bob_state;
}

// =============================================================
// A Read-only joiner's writes must NOT propagate to the host.
//
// Bob joins without receiving a `grant_rw`, so he never receives the
// NamespaceSecret needed to produce valid signed entries. Even though
// his local watcher sees the file and attempts to set_bytes, the doc
// layer cannot produce a valid entry and the host never observes it.
//
// Sentinel approach: after Bob writes his file, Alice writes a second
// file. Once Bob sees Alice's second file (proving the sync pipeline
// is healthy), we assert Bob's file never reached Alice — no fixed
// sleep needed.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn read_only_joiner_write_does_not_propagate() {
    use artel_protocol::capability::Capability;

    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    // Alice hosts.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_a.socket.clone()),
    )
    .await
    .expect("Workspace::host");
    let session = alice_ws.session_id();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // Issue a Read-only ticket for Bob so his daemon-level cap is
    // Read (no auto-grant of RW, no upgrade delivery).
    let issue_resp = alice
        .request(Request::IssueTicket {
            session,
            granted_cap: Capability::Read,
            expiry_ms: 0,
        })
        .await
        .unwrap();
    let read_ticket = match issue_resp {
        Response::IssuedTicket { ticket: t, .. } => t,
        other => panic!("expected IssuedTicket, got {other:?}"),
    };

    // Bob joins with the Read-only ticket.
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: read_ticket,
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
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_b.socket.clone()),
    )
    .await
    .expect("Workspace::join");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Step 1: Alice writes a file → Bob should see it (Read syncs
    // inbound).
    let alice_first = alice_dir.path().join("from_alice.txt");
    tokio::fs::write(&alice_first, b"hello from alice")
        .await
        .unwrap();
    common::wait_for_file(&bob_dir.path().join("from_alice.txt"), b"hello from alice").await;

    // Step 2: Bob writes a file locally. Without the NamespaceSecret
    // this cannot produce a valid signed entry.
    let bob_file = bob_dir.path().join("from_bob.txt");
    tokio::fs::write(&bob_file, b"hello from bob")
        .await
        .unwrap();

    // Step 3: Alice writes a second sentinel file. Once Bob sees it,
    // the full pipeline has flushed; if Bob's file were going to
    // propagate, it would have arrived by then.
    sleep(Duration::from_secs(1)).await; // let Bob's watcher fire first
    let alice_sentinel = alice_dir.path().join("sentinel.txt");
    tokio::fs::write(&alice_sentinel, b"sentinel")
        .await
        .unwrap();
    common::wait_for_file(&bob_dir.path().join("sentinel.txt"), b"sentinel").await;

    // Step 4: Assert Alice does NOT have Bob's file.
    let leaked = tokio::fs::try_exists(alice_dir.path().join("from_bob.txt"))
        .await
        .unwrap_or(false);
    assert!(
        !leaked,
        "Read-only joiner's write propagated to host — capability enforcement broken",
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
// Cooperative demote (Slice 0): an RW joiner that the host demotes to
// Read receives a DOWNGRADE_ACTION notification and halts its own
// watcher, so its subsequent local writes stop propagating to the host.
//
// Exercises the full Slice-0 path: host detects RW→Read in its
// cap_listener → DeliverDowngrade → daemon emit_downgrade → joiner
// DOWNGRADE_ACTION handler → write_halted flag → watcher skip.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn demoted_joiner_writes_stop_propagating() {
    use artel_protocol::capability::Capability;

    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    // Alice hosts.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_a.socket.clone()),
    )
    .await
    .expect("Workspace::host");
    let session = alice_ws.session_id();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // Bob joins with a Read ticket, then Alice grants RW so his writes
    // propagate (the precondition we then revoke).
    let issue_resp = alice
        .request(Request::IssueTicket {
            session,
            granted_cap: Capability::Read,
            expiry_ms: 0,
        })
        .await
        .unwrap();
    let read_ticket = match issue_resp {
        Response::IssuedTicket { ticket: t, .. } => t,
        other => panic!("expected IssuedTicket, got {other:?}"),
    };

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let bob_peer = bob.daemon_peer_id();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: read_ticket,
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
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_b.socket.clone()),
    )
    .await
    .expect("Workspace::join");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Promote Bob to RW and wait until the upgrade has propagated (his
    // writes reach Alice).
    common::grant_rw_and_wait(&alice, session, bob_peer, bob_dir.path(), alice_dir.path()).await;

    // Sanity: a normal RW write reaches Alice.
    let pre = bob_dir.path().join("pre_demote.txt");
    tokio::fs::write(&pre, b"before demote").await.unwrap();
    common::wait_for_file(&alice_dir.path().join("pre_demote.txt"), b"before demote").await;

    // Demote Bob (RW → Read). The host's cap_listener fires the
    // DOWNGRADE_ACTION unicast; Bob's watcher halts.
    common::demote(&alice, session, bob_peer).await;

    // Poll until Bob's watcher is halted, proving the notification
    // arrived and was applied. We can't read Bob's flag across the
    // process boundary here (same process, but private), so we assert
    // behaviourally below; give the notification a moment to land.
    sleep(Duration::from_secs(2)).await;

    // Bob writes again post-demote. This must NOT propagate.
    let post = bob_dir.path().join("post_demote.txt");
    tokio::fs::write(&post, b"after demote").await.unwrap();

    // Alice writes a sentinel; once Bob sees it, the inbound pipeline
    // has flushed, so if Bob's post-demote write were going to arrive
    // it would have by now.
    let sentinel = alice_dir.path().join("sentinel.txt");
    tokio::fs::write(&sentinel, b"sentinel").await.unwrap();
    common::wait_for_file(&bob_dir.path().join("sentinel.txt"), b"sentinel").await;

    let leaked = tokio::fs::try_exists(alice_dir.path().join("post_demote.txt"))
        .await
        .unwrap_or(false);
    assert!(
        !leaked,
        "demoted joiner's post-demote write propagated to host — watcher-halt not honoured",
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
// Namespace rotation core (Slice 3c): after evicting an RW peer, the
// host rotates the namespace. The freshly minted namespace carries the
// surviving (still-RW) authors' entries but DROPS the revoked author's
// — the cryptographic write cut-off. Exercises the rotation core
// directly via test_rotate_namespace (the live doc swap is Slice 3d).
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn rotation_drops_revoked_author_entries() {
    use artel_protocol::capability::Capability;

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
            .with_daemon_socket(daemon_a.socket.clone()),
    )
    .await
    .expect("Workspace::host");
    let session = alice_ws.session_id();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // Bob joins (Read ticket) then is promoted to RW.
    let read_ticket = match alice
        .request(Request::IssueTicket {
            session,
            granted_cap: Capability::Read,
            expiry_ms: 0,
        })
        .await
        .unwrap()
    {
        Response::IssuedTicket { ticket, .. } => ticket,
        other => panic!("expected IssuedTicket, got {other:?}"),
    };
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let bob_peer = bob.daemon_peer_id();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: read_ticket,
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
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_b.socket.clone()),
    )
    .await
    .expect("Workspace::join");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    common::grant_rw_and_wait(&alice, session, bob_peer, bob_dir.path(), alice_dir.path()).await;

    // Alice writes host.txt; Bob writes joiner.txt. Wait until BOTH are
    // visible on Alice's doc (so the snapshot would include both).
    tokio::fs::write(alice_dir.path().join("host.txt"), b"by host")
        .await
        .unwrap();
    tokio::fs::write(bob_dir.path().join("joiner.txt"), b"by joiner")
        .await
        .unwrap();
    common::wait_for_file(&alice_dir.path().join("joiner.txt"), b"by joiner").await;

    // Demote Bob to Read (NOT Evict): this makes Bob's author non-RW in
    // the host's peer_map so the rotation snapshot drops his entries,
    // WITHOUT auto-triggering rotation (only Revoke does that — see
    // `evict_auto_rotates_and_cuts_off_writes`). That lets this test
    // drive `rotate_namespace` manually in isolation and assert on its
    // own drop count. The author-filter treats Read and revoked authors
    // identically (`endpoint_has_rw` is false for both).
    common::demote(&alice, session, bob_peer).await;
    // Give the host cap_listener a moment to apply the demote into its
    // peer_map before we snapshot.
    sleep(Duration::from_secs(2)).await;

    // Rotate. The new namespace must keep host.txt, drop joiner.txt.
    let (new_epoch, new_ns, survivors, dropped) = alice_ws
        .test_rotate_namespace(0)
        .await
        .expect("rotate_namespace");
    assert_eq!(new_epoch, 1, "epoch must bump 0→1");
    assert!(survivors >= 1, "host.txt must survive (got {survivors})");
    assert!(
        dropped >= 1,
        "revoked author's joiner.txt must be dropped (got {dropped})",
    );

    let keys = alice_ws.test_namespace_keys(new_ns).await;
    assert!(
        keys.iter().any(|k| k.ends_with("host.txt")),
        "rotated namespace must contain host.txt; keys={keys:?}",
    );
    assert!(
        !keys.iter().any(|k| k.ends_with("joiner.txt")),
        "rotated namespace must NOT contain revoked author's joiner.txt; keys={keys:?}",
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
// Rotation carries deletes forward (D3): a tombstone in the old
// namespace must reappear as a tombstone in the rotated namespace, not
// vanish. If it vanished, a survivor offline across the rotation would
// reimport a namespace with neither the file nor a delete for it, keep
// its stale local copy, and its respawned watcher could republish the
// deleted path — resurrecting it.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn rotation_carries_tombstones_forward() {
    // `_dir` held only for liveness; paths use `ws.root` (canonical) so
    // `path_to_key` strip_prefix works on macOS (/var -> /private/var).
    let (daemon, _client, ws, handle, _events, _dir, _dns_pkarr) =
        spawn_host_workspace_for_empty_test().await;

    // Create then delete a file: the doc ends with a tombstone for it,
    // and a live, surviving file alongside it.
    tokio::fs::write(ws.root.join("kept.txt"), b"keep")
        .await
        .unwrap();
    tokio::fs::write(ws.root.join("deleted.txt"), b"bye")
        .await
        .unwrap();
    assert!(
        wait_for_doc_entry(&ws, &ws.root.join("deleted.txt"), WAIT_BUDGET).await,
        "deleted.txt never landed before delete",
    );
    assert!(
        wait_for_doc_entry(&ws, &ws.root.join("kept.txt"), WAIT_BUDGET).await,
        "kept.txt never landed",
    );
    tokio::fs::remove_file(ws.root.join("deleted.txt"))
        .await
        .unwrap();
    // Wait for the tombstone to land in the current namespace.
    let deadline = Instant::now() + WAIT_BUDGET;
    loop {
        let toms = ws
            .test_namespace_tombstone_keys(ws.test_current_namespace_bytes())
            .await;
        if toms.iter().any(|k| k.ends_with("deleted.txt")) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "tombstone for deleted.txt never landed in the current namespace",
        );
        sleep(POLL_INTERVAL).await;
    }

    // Rotate (host self-rotation). The new namespace must carry the
    // tombstone forward AND keep the live file.
    let (_epoch, new_ns, _survivors, _dropped) =
        ws.test_rotate_namespace(0).await.expect("rotate_namespace");

    let live_keys = ws.test_namespace_keys(new_ns).await;
    assert!(
        live_keys.iter().any(|k| k.ends_with("kept.txt")),
        "rotated namespace must keep the live kept.txt; live={live_keys:?}",
    );
    assert!(
        !live_keys.iter().any(|k| k.ends_with("deleted.txt")),
        "deleted.txt must not be a LIVE entry in the rotated namespace; live={live_keys:?}",
    );
    let tomb_keys = ws.test_namespace_tombstone_keys(new_ns).await;
    assert!(
        tomb_keys.iter().any(|k| k.ends_with("deleted.txt")),
        "the delete must be carried forward as a tombstone (D3), not dropped; \
         tombstones={tomb_keys:?}",
    );

    ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), handle).await;
    daemon.stop().await;
}

// =============================================================
// A live edit on the host's filesystem propagates to the joiner via
// the watcher → doc → applier pipeline.
//
// Two daemons, Alice hosts the artel session and a workspace, Bob
// joins. Both call `Workspace::run` so their watchers + appliers are
// live. Alice writes `live.txt` *after* `Workspace::host` returned;
// Bob's filesystem should reflect it within a couple of seconds.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn live_edit_propagates_host_to_joiner() {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    // Alice hosts.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();

    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
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

    // Bob joins.
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
    .expect("Workspace::join");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    let alice_path = alice_dir.path().join("live.txt");
    let bob_path = bob_dir.path().join("live.txt");
    let payload = b"hello from a live edit";
    tokio::fs::write(&alice_path, payload).await.unwrap();

    // Poll Bob's tempdir for the file. The shared FILE_BUDGET (15s)
    // covers notify debounce (300ms) -> doc set_bytes -> sync ->
    // applier -> tokio::fs::write.
    common::wait_for_file(&bob_path, payload).await;

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
// A populated directory renamed INTO the workspace propagates its
// contents.
//
// inotify only reports the rename of the directory itself on the
// watched parent — the files inside never produce events of their
// own (they were written while the directory lived outside the
// watch). The watcher's directory-rescan branch must walk the
// moved-in subtree and publish each file.
//
// This is the deterministic twin of the new-subtree race that made
// `workspace_filter.rs`'s `first_match_wins_carries_through_wire` /
// `watcher_blocks_outgoing_read_only_write` flake under load: there,
// `create_dir` + immediate write occasionally lands the file before
// notify's watch backfill, so the file event is silently dropped and
// only the directory's Create event survives. Rename-in produces
// that exact "directory event only" shape on every run instead of
// one run in three.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn renamed_in_directory_contents_propagate() {
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
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
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
    let (bob_ws, _) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Stage a populated directory OUTSIDE the workspace, then rename
    // it in. Both tempdirs live under the same filesystem (/tmp), so
    // this is a true rename — the files inside generate no events.
    let staging = tempfile::tempdir().unwrap();
    let staged = staging.path().join("incoming");
    tokio::fs::create_dir_all(staged.join("nested"))
        .await
        .unwrap();
    tokio::fs::write(staged.join("data.txt"), b"moved in")
        .await
        .unwrap();
    tokio::fs::write(staged.join("nested/deep.txt"), b"deep moved in")
        .await
        .unwrap();
    tokio::fs::rename(&staged, alice_dir.path().join("incoming"))
        .await
        .unwrap();

    common::wait_for_file(&bob_dir.path().join("incoming/data.txt"), b"moved in").await;
    common::wait_for_file(
        &bob_dir.path().join("incoming/nested/deep.txt"),
        b"deep moved in",
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
// Full round-trip test.
//
// Two daemons + two `Workspace`s on the same artel session exercise
// the full watcher → doc → applier loop in both directions:
//
// - Alice writes `a.txt` → assert Bob sees it.
// - Bob writes `b.txt` → assert Alice sees it.
// - Alice writes `target/junk` → assert Bob does NOT see it
//   (hardcoded skip).
// - Echo guard sanity: count Doc entries for the key Bob just
//   applied — there should be exactly 1, not 2.
//
// Runs 3 times in a row to flush out gossip-on-gossip-on-fs flakiness.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn round_trip_3_in_a_row() {
    // Run the full scenario 3 consecutive times. The `_run` index
    // is purely informational; if any iteration fails, the test
    // panics with the iteration index in the message.
    for run in 0..3 {
        eprintln!("--- round_trip iteration {run} ---");
        round_trip_once(run).await;
    }
}

// Long, deliberately linear: this is a top-down e2e scenario, and
// extracting helpers per-step would obscure the order more than the
// length hurts.
#[allow(clippy::too_many_lines)]
async fn round_trip_once(run: usize) {
    let Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = spawn_pair().await;

    // Alice on daemon A hosts the artel session + workspace.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_a.socket.clone()),
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

    // Bob on daemon B joins, then mounts a workspace.
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let bob_peer_id = bob.daemon_peer_id();
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
        WorkspaceConfig::default()
            .with_endpoint_setup(testing_setup(&dns_pkarr))
            .with_daemon_socket(daemon_b.socket.clone()),
    )
    .await
    .expect("Workspace::join");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Grant Bob RW so the upgrade delivery gives him the
    // NamespaceSecret needed to produce valid signed entries.
    common::grant_rw_and_wait(
        &alice,
        session,
        bob_peer_id,
        bob_dir.path(),
        alice_dir.path(),
    )
    .await;

    // 1. Alice writes a.txt → Bob sees it.
    let alice_a = alice_dir.path().join("a.txt");
    tokio::fs::write(&alice_a, b"alpha")
        .await
        .expect("write a.txt on alice");
    wait_for_run_file(
        &bob_dir.path().join("a.txt"),
        b"alpha",
        "bob sees a.txt",
        run,
    )
    .await;

    // 2. Bob writes b.txt → Alice sees it.
    let bob_b = bob_dir.path().join("b.txt");
    tokio::fs::write(&bob_b, b"beta")
        .await
        .expect("write b.txt on bob");
    wait_for_run_file(
        &alice_dir.path().join("b.txt"),
        b"beta",
        "alice sees b.txt",
        run,
    )
    .await;

    // 3. Alice writes target/junk — hardcoded skip; Bob must NOT
    //    see it. Use a sentinel file (`sentinel.txt`) written
    //    *after* `target/junk` to drive timing end-to-end: once
    //    Bob has the sentinel, Alice's watcher pipeline has
    //    finished its debounce, published, and propagated. If
    //    `target/junk` were going to leak, it would have arrived by
    //    then too. This avoids picking a fixed settling delay.
    let alice_target = alice_dir.path().join("target");
    tokio::fs::create_dir_all(&alice_target).await.unwrap();
    tokio::fs::write(alice_target.join("junk"), b"build artifact")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("sentinel.txt"), b"after-junk")
        .await
        .unwrap();
    wait_for_run_file(
        &bob_dir.path().join("sentinel.txt"),
        b"after-junk",
        "bob sees sentinel after target/junk",
        run,
    )
    .await;
    let bob_target_path = bob_dir.path().join("target/junk");
    let leaked = tokio::fs::try_exists(&bob_target_path)
        .await
        .unwrap_or(false);
    assert!(
        !leaked,
        "[run {run}] target/junk leaked to bob: {}",
        bob_target_path.display(),
    );
    // Defense in depth: Alice's filter should have blocked the
    // publish in the first place, not just relied on Bob's applier
    // filter to catch it. Check Alice's doc directly.
    let junk_key = path_to_key(alice_ws.root.as_path(), &alice_ws.root.join("target/junk"))
        .expect("path_to_key for target/junk");
    let stream = alice_ws
        .doc()
        .get_many(Query::key_exact(junk_key))
        .await
        .expect("get_many on alice's doc");
    tokio::pin!(stream);
    let alice_published_junk = stream.next().await.is_some();
    assert!(
        !alice_published_junk,
        "[run {run}] alice's filter regression: target/junk made it into the doc",
    );

    // 4. Echo-guard sanity: count Doc entries for `a.txt` on Bob's
    //    side. The applier wrote `a.txt` to disk on bob, then the
    //    watcher fired — but the echo guard should suppress
    //    re-publishing. Net effect: exactly one entry per author
    //    (Alice's), zero from Bob.
    //
    //    Note: Bob's workspace root is canonicalised (e.g. macOS
    //    rewrites `/var/...` → `/private/var/...`), so we use
    //    `bob_ws.root` for the path-to-key call.
    let bob_a_canonical = bob_ws.root.join("a.txt");
    let key = path_to_key(bob_ws.root.as_path(), &bob_a_canonical).expect("key path");
    let stream = bob_ws
        .doc()
        .get_many(Query::key_exact(key.clone()))
        .await
        .expect("get_many");
    tokio::pin!(stream);
    let mut count = 0usize;
    while let Some(res) = stream.next().await {
        let _ = res.expect("entry ok");
        count += 1;
    }
    assert_eq!(
        count, 1,
        "[run {run}] expected exactly 1 doc entry for a.txt on bob; found {count}. \
         Echo guard regression?",
    );

    // Also sanity-check the key round-trips back to the right path
    // (catches a regression where path_to_key / key_to_path drift
    // out of sync).
    let recovered = key_to_path(bob_ws.root.as_path(), &key).expect("key_to_path");
    assert_eq!(recovered, bob_a_canonical);

    alice_ws.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), alice_handle).await;
    let _ = timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

/// Poll `path` until it contains `expected_payload` or the deadline
/// elapses. Panics with `who` and `run` index on failure.
async fn wait_for_run_file(path: &Path, expected_payload: &[u8], who: &str, run: usize) {
    let deadline = Instant::now() + WAIT_BUDGET;
    loop {
        if let Ok(bytes) = tokio::fs::read(path).await
            && bytes == expected_payload
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "[run {run}] {who}: never saw expected bytes at {}",
            path.display(),
        );
        sleep(POLL_INTERVAL).await;
    }
}
