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
use artel_protocol::{Request, Response, SessionId};
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
    let dns_pkarr = Arc::new(DnsPkarrServer::run().await.expect("dns_pkarr"));
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
    let key = path_to_key(ws.root.as_path(), path).expect("path_to_key");
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
        Response::HostSession { session, ticket } => (session, ticket),
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
        Response::IssuedTicket { ticket: t } => t,
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
