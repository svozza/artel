//! Filesystem-side change feed.
//!
//! Wraps `notify-debouncer-full` so a flurry of saves coalesces into
//! one debounced event per path, then publishes the resulting bytes
//! into the doc — guarded by [`crate::EchoGuard`] so peer-driven
//! writes (which the applier just laid down on disk) don't get
//! re-published in a loop.
//!
//! Ported near-verbatim from harness `session/workspace/watcher.rs`;
//! the only structural change is API drift in
//! `notify-debouncer-full` 0.6 (`Debouncer::watch` instead of
//! `Debouncer::watcher().watch`).

// Crate-private module; pair `unreachable_pub` with crate-pub funs.
// See `feedback_clippy_lint_conflict` in memory.
#![allow(clippy::redundant_pub_crate)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use notify::EventKind;
use notify_debouncer_full::DebounceEventResult;

use crate::filter::{FilterDecision, SkipReason, WorkspaceFilter};
use crate::workspace::{Workspace, WorkspaceEvent};
use crate::{EchoGuard, keys};

/// Local change observed by the debounced watcher. Two flavours
/// because deletes don't carry bytes — the applier (and the doc)
/// see a tombstone rather than a write.
#[derive(Debug)]
enum LocalChange {
    Modified(PathBuf),
    Removed(PathBuf),
}

/// Run the watcher loop until the workspace's shutdown token is
/// tripped or the underlying notify channel closes. Surfaces errors
/// as [`WorkspaceEvent::Error`] / [`WorkspaceEvent::SkippedTooLarge`]
/// rather than returning them; the watcher is a background task and
/// transient failures shouldn't take it down.
pub(crate) async fn run(workspace: Arc<Workspace>) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<LocalChange>();

    let mut debouncer = match notify_debouncer_full::new_debouncer(
        Duration::from_millis(300),
        None,
        move |res: DebounceEventResult| {
            let Ok(events) = res else { return };
            for ev in events {
                match &ev.event.kind {
                    EventKind::Modify(_) | EventKind::Create(_) => {
                        for path in &ev.event.paths {
                            let _ = tx.send(LocalChange::Modified(path.clone()));
                        }
                    }
                    EventKind::Remove(_) => {
                        for path in &ev.event.paths {
                            let _ = tx.send(LocalChange::Removed(path.clone()));
                        }
                    }
                    _ => {}
                }
            }
        },
    ) {
        Ok(d) => d,
        Err(err) => {
            let _ = workspace
                .events
                .send(WorkspaceEvent::Error(format!("watcher init failed: {err}")))
                .await;
            return;
        }
    };

    if let Err(err) = debouncer.watch(&workspace.root, notify::RecursiveMode::Recursive) {
        let _ = workspace
            .events
            .send(WorkspaceEvent::Error(format!("watch failed: {err}")))
            .await;
        return;
    }

    let filter = WorkspaceFilter::new(&workspace.root);
    // Same shared echo-guard handles the applier uses; we re-borrow
    // them via `.shared(...)` so we hold an `Arc` clone of the maps
    // rather than the workspace's own `EchoGuard` value.
    let guard = EchoGuard::shared(
        workspace.echo_guard.pending_handle(),
        workspace.echo_guard.last_published_handle(),
    );

    loop {
        tokio::select! {
            () = workspace.shutdown_token.cancelled() => return,
            change = rx.recv() => {
                match change {
                    Some(LocalChange::Modified(path)) => {
                        on_modified(&workspace, &filter, &guard, path).await;
                    }
                    Some(LocalChange::Removed(path)) => {
                        on_removed(&workspace, path).await;
                    }
                    None => return,
                }
            }
        }
    }
}

async fn on_modified(
    workspace: &Arc<Workspace>,
    filter: &WorkspaceFilter,
    guard: &EchoGuard,
    path: PathBuf,
) {
    match filter.check(&path) {
        FilterDecision::Skip(SkipReason::TooLarge { size }) => {
            let _ = workspace
                .events
                .send(WorkspaceEvent::SkippedTooLarge {
                    path: path.clone(),
                    size,
                })
                .await;
            return;
        }
        FilterDecision::Skip(_) => return,
        FilterDecision::Include => {}
    }

    let bytes = match tokio::fs::read(&path).await {
        Ok(b) => b,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            // macOS FSEvents reports post-unlink `Modify(Metadata)` /
            // `Modify(Data)` events instead of a clean `Remove` —
            // converting them to a tombstone here is what makes
            // deletion propagate cross-platform. Linux does send
            // `Remove`, and `on_removed` would handle it before we
            // got here.
            on_removed(workspace, path).await;
            return;
        }
        // Other read errors (permission, transient I/O) — drop
        // silently; a subsequent event will retry.
        Err(_) => return,
    };

    if guard.should_skip_local(&path, &bytes).await {
        return;
    }

    let key = match keys::path_to_key(&workspace.root, &path) {
        Ok(k) => k,
        Err(err) => {
            let _ = workspace
                .events
                .send(WorkspaceEvent::Error(format!(
                    "path_to_key {}: {err}",
                    path.display()
                )))
                .await;
            return;
        }
    };

    match workspace
        .doc
        .set_bytes(workspace.author, key, Bytes::from(bytes.clone()))
        .await
    {
        Ok(_) => {
            guard.record_local_publish(&path, &bytes).await;
        }
        Err(err) => {
            let _ = workspace
                .events
                .send(WorkspaceEvent::Error(format!(
                    "publish {} failed: {err}",
                    path.display(),
                )))
                .await;
        }
    }
}

async fn on_removed(workspace: &Arc<Workspace>, path: PathBuf) {
    let Ok(key) = keys::path_to_key(&workspace.root, &path) else {
        return;
    };
    let _ = workspace.doc.del(workspace.author, key).await;
}
