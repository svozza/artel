//! Filesystem-side change feed.
//!
//! Wraps `notify-debouncer-full` so a flurry of saves coalesces into
//! one debounced event per path, then publishes the resulting bytes
//! into the doc — guarded by [`crate::EchoGuard`] so peer-driven
//! writes (which the applier just laid down on disk) don't get
//! re-published in a loop.

#![allow(clippy::redundant_pub_crate)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use notify::EventKind;
use notify_debouncer_full::DebounceEventResult;

use tokio::sync::oneshot;

use crate::filter::{FilterDecision, SkipReason, WorkspaceFilter};
use crate::rules::Mode;
use crate::workspace::{Direction, Workspace, WorkspaceEvent};
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
///
/// `ready` is signalled exactly once, after the underlying notify
/// debouncer has successfully attached its OS-level watch (`FSEvents`
/// on macOS, inotify on Linux). Callers can `await` the matching
/// receiver to know that subsequent filesystem writes under
/// [`Workspace::root`] will reach this watcher — without this gate,
/// a write that lands between [`Workspace::run`] returning and the
/// debouncer attaching is silently missed.
///
/// On the early-return error paths (debouncer init failure, initial
/// watch failure), `ready` is dropped without being sent, so the
/// receiver resolves with [`oneshot::error::RecvError`]. Callers
/// should treat that as "watcher will never come up" and either bail
/// or proceed best-effort — the [`WorkspaceEvent::Error`] is also
/// emitted so the consumer's event stream sees what went wrong.
pub(crate) async fn run(workspace: Arc<Workspace>, ready: oneshot::Sender<()>) {
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
    // Watch is attached. Signal readiness so callers blocked in
    // `Workspace::run().await` can proceed. `send` only fails if the
    // receiver was dropped, which means the caller stopped waiting
    // — fine to ignore.
    let _ = ready.send(());

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

    // Rule check sits before the file read so a `ReadOnly` path
    // doesn't even hit the disk. `strip_prefix` shouldn't fail since
    // the watcher only reports paths under `workspace.root`, but
    // we fall through (rather than fail closed) if it does — a
    // pathological non-stripping path is more likely an unrelated
    // bug than a rule-evasion attempt, and failing closed would
    // mask it.
    let rel = path.strip_prefix(&workspace.root).unwrap_or(&path);
    if workspace.compiled_rules.mode_for(rel) == Mode::ReadOnly {
        let _ = workspace
            .events
            .send(WorkspaceEvent::SkippedReadOnly {
                path,
                direction: Direction::Outgoing,
            })
            .await;
        return;
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

    // Skip zero-length files: iroh-docs reserves zero-length entries
    // for tombstones and rejects an explicit empty `set_bytes` with
    // "Attempted to insert an empty entry". Once the file gets actual
    // content the next debounced event picks it up.
    //
    // TODO: support genuinely-empty files (e.g. `touch sentinel`) —
    // probably by storing an inline marker in the entry's metadata
    // or splitting "presence" from "content" at the doc layer.
    if bytes.is_empty() {
        return;
    }

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

    let bytes = Bytes::from(bytes);
    match workspace
        .doc
        .set_bytes(workspace.author, key, bytes.clone())
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
    // Belt-and-braces with `on_modified`: on macOS, FSEvents reports
    // post-unlink as `Modify(Metadata)` and `on_modified` already
    // gates on `ReadOnly` before its own fallthrough into here. On
    // Linux, `Remove` events arrive here directly and bypass that
    // gate, so the rule check has to live here too.
    let rel = path.strip_prefix(&workspace.root).unwrap_or(&path);
    if workspace.compiled_rules.mode_for(rel) == Mode::ReadOnly {
        let _ = workspace
            .events
            .send(WorkspaceEvent::SkippedReadOnly {
                path,
                direction: Direction::Outgoing,
            })
            .await;
        return;
    }
    let Ok(key) = keys::path_to_key(&workspace.root, &path) else {
        return;
    };
    let _ = workspace.doc.del(workspace.author, key).await;
}
