//! An empty file in the workspace must not propagate as a doc
//! entry — iroh-docs reserves zero-length entries for tombstones,
//! so a naive `set_bytes(_, _, &[])` is rejected with
//! `"Attempted to insert an empty entry"`. The watcher silently
//! skips empty files; once the user puts content into the file,
//! the next debounced event picks it up.
//!
//! This test pins the contract at a single-host level (no joiner
//! needed) by:
//! 1. Spawning a workspace with a draining event-stream consumer
//!    that records any [`WorkspaceEvent::Error`] it sees.
//! 2. `touch`ing a fresh file inside the workspace; verifying that
//!    no error is published and that the doc has no entry for it.
//! 3. Writing content into the same file; verifying that the doc
//!    *now* has an entry, proving the skip is transient and not a
//!    permanent block on the path.
//!
//! TODO(empty-files): once we have a doc-layer encoding for
//! genuine zero-length files (e.g. an inline-marker entry), this
//! test should flip to assert the empty file *does* sync.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use artel_client::Client;
use artel_fs::{AttachPolicy, Workspace, WorkspaceEvent, path_to_key};
use artel_protocol::{PeerId, PeerInfo, Request, Response};
use futures_util::StreamExt;
use iroh_docs::store::Query;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::time::sleep;

const POLL: Duration = Duration::from_millis(50);

/// Minimal harness: one daemon, one client, one host workspace.
/// The joiner side is irrelevant for this property — empty-file
/// rejection is purely about the host's watcher → `set_bytes` path.
async fn spawn_host_workspace() -> (
    common::RunningDaemon,
    Client,
    Arc<Workspace>,
    tokio::task::JoinHandle<()>,
    mpsc::Receiver<WorkspaceEvent>,
    TempDir,
) {
    let daemon = common::spawn_daemon_with_lookup(
        common::fresh_state(),
        iroh::address_lookup::memory::MemoryLookup::new(),
    )
    .await;
    let client = Client::connect(&daemon.socket).await.unwrap();
    let peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "host");
    let session = match client
        .request(Request::HostSession {
            peer,
            session: None,
        })
        .await
        .unwrap()
    {
        Response::HostSession { session, .. } => session,
        other => panic!("HostSession: got {other:?}"),
    };
    let dir = tempfile::tempdir().unwrap();
    let (ws, events) = Workspace::host(
        &client,
        session,
        dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
    )
    .await
    .expect("Workspace::host");
    let ws = Arc::new(ws);
    let handle = Arc::clone(&ws).run().await;
    (daemon, client, ws, handle, events, dir)
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

async fn doc_has_entry(ws: &Workspace, path: &std::path::Path) -> bool {
    let key = path_to_key(ws.root.as_path(), path).expect("path_to_key");
    let stream = ws
        .doc()
        .get_many(Query::key_exact(key))
        .await
        .expect("get_many");
    tokio::pin!(stream);
    stream.next().await.is_some()
}

async fn wait_for_entry(ws: &Workspace, path: &std::path::Path, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if doc_has_entry(ws, path).await {
            return true;
        }
        sleep(POLL).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread")]
async fn touching_empty_file_does_not_error_and_does_not_publish() {
    let (daemon, client, ws, handle, events, dir) = spawn_host_workspace().await;
    let errors = collect_errors(events);

    // `touch` an empty file. canonicalise the workspace's root
    // because macOS rewrites `/var/...` → `/private/var/...` and
    // path_to_key needs the canonical form.
    let canonical_dir = ws.root.clone();
    let empty_path: PathBuf = canonical_dir.join("empty.txt");
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
        !doc_has_entry(&ws, &empty_path).await,
        "empty file must not produce a doc entry yet — \
         iroh-docs would reject a zero-length insert as a tombstone",
    );

    // Now write content to the same file. The next debounced event
    // should publish it normally — proving the skip was transient.
    tokio::fs::write(&empty_path, b"now i have content")
        .await
        .expect("write content");
    assert!(
        wait_for_entry(&ws, &empty_path, Duration::from_secs(5)).await,
        "after content arrives the watcher must publish — \
         the skip should not permanently block the path",
    );
    let after = errors.lock().await.clone();
    assert!(
        after.is_empty(),
        "watcher must not error after content publish either; got: {after:?}",
    );

    // Tidy up.
    ws.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    drop(client);
    daemon.stop().await;
    let _ = dir;
}
