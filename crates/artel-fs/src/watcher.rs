//! Filesystem-side change feed.
//!
//! Wraps `notify-debouncer-full` so a flurry of saves coalesces into
//! one debounced event per path, then publishes the resulting bytes
//! into the doc — guarded by [`crate::EchoGuard`] so peer-driven
//! writes (which the applier just laid down on disk) don't get
//! re-published in a loop.

#![allow(clippy::redundant_pub_crate)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use notify::EventKind;
use notify_debouncer_full::DebounceEventResult;
use walkdir::WalkDir;

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use crate::filter::{FilterDecision, SkipReason, WorkspaceFilter};
use crate::rules::Mode;
use crate::workspace::{Direction, Workspace, WorkspaceEvent, emit_event};
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
            let events = match res {
                Ok(e) => e,
                Err(errs) => {
                    warn!(target: "artel_fs::watcher", errors = ?errs, "debouncer callback received error batch");
                    return;
                }
            };
            for ev in events {
                debug!(
                    target: "artel_fs::watcher",
                    kind = ?ev.event.kind,
                    paths = ?ev.event.paths,
                    "debouncer event"
                );
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
    debug!(
        target: "artel_fs::watcher",
        root = %workspace.root.display(),
        "debouncer attached recursive watch"
    );
    // Watch is attached. Signal readiness so callers blocked in
    // `Workspace::run().await` can proceed. `send` only fails if the
    // receiver was dropped, which means the caller stopped waiting
    // — fine to ignore.
    let _ = ready.send(());

    let filter = WorkspaceFilter::new(&workspace.root, workspace.exclude.clone());
    // A clone shares the workspace guard's state (Arc-backed), so we
    // observe everything the applier's clone marks.
    let guard = workspace.echo_guard.clone();

    // Doc-scoped token: cancelled at workspace shutdown AND on
    // namespace rotation (which respawns this task against the new
    // namespace). A child of the workspace shutdown token, so either
    // path stops the watcher.
    let doc_token = workspace.doc_token();

    loop {
        tokio::select! {
            () = doc_token.cancelled() => {
                debug!(target: "artel_fs::watcher", "doc token tripped, exiting watcher loop");
                return;
            }
            change = rx.recv() => {
                match change {
                    Some(LocalChange::Modified(path)) => {
                        on_modified(&workspace, &filter, &guard, path).await;
                    }
                    Some(LocalChange::Removed(path)) => {
                        on_removed(&workspace, &filter, &guard, path).await;
                    }
                    None => {
                        debug!(target: "artel_fs::watcher", "notify channel closed, exiting watcher loop");
                        return;
                    }
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
    debug!(target: "artel_fs::watcher", path = %path.display(), "on_modified entered");
    // Cooperative demotion: a downgraded node stops publishing its own
    // writes (voluntary write-stop). Checked first so a halted node
    // doesn't even read the file.
    if workspace.is_write_halted() {
        debug!(target: "artel_fs::watcher", path = %path.display(), "write-halted: skip publish");
        return;
    }
    match filter.check(&path) {
        FilterDecision::Skip(SkipReason::TooLarge { size }) => {
            debug!(target: "artel_fs::watcher", path = %path.display(), size, "filter: skip too-large");
            emit_event(
                &workspace.events,
                WorkspaceEvent::SkippedTooLarge {
                    path: path.clone(),
                    size,
                },
            );
            return;
        }
        FilterDecision::Skip(SkipReason::Excluded) => {
            debug!(target: "artel_fs::watcher", path = %path.display(), "filter: skip excluded");
            emit_event(
                &workspace.events,
                WorkspaceEvent::SkippedExcluded {
                    path: path.clone(),
                    direction: Direction::Outgoing,
                },
            );
            return;
        }
        FilterDecision::Skip(reason) => {
            debug!(target: "artel_fs::watcher", path = %path.display(), reason = ?reason, "filter: skip");
            return;
        }
        FilterDecision::Include => {}
    }

    // Directory events get a subtree rescan instead of a publish.
    // inotify attaches one watch per directory and notify backfills
    // watches for a freshly created subtree only after it processes
    // the parent's CREATE event — a file written into the new
    // directory before that backfill produces no event, ever, and
    // under load the gap stretches past the debounce window. The
    // event for the directory itself IS reliable (the already-watched
    // parent reports it), so use it as the cue to walk the subtree
    // and replay each file through this same pipeline. Idempotent:
    // per-file filter/rule checks re-run, and the echo guard's
    // last-published hash skips files whose bytes already made it
    // into the doc.
    if tokio::fs::metadata(&path).await.is_ok_and(|m| m.is_dir()) {
        rescan_dir(workspace, filter, guard, &path).await;
        return;
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
        debug!(target: "artel_fs::watcher", path = %path.display(), "rules: skip ReadOnly outgoing");
        emit_event(
            &workspace.events,
            WorkspaceEvent::SkippedReadOnly {
                path,
                direction: Direction::Outgoing,
            },
        );
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
            debug!(target: "artel_fs::watcher", path = %path.display(), "read NotFound -> tombstone");
            on_removed(workspace, filter, guard, path).await;
            return;
        }
        // Other read errors (permission, transient I/O) — drop
        // silently; a subsequent event will retry.
        Err(err) => {
            warn!(target: "artel_fs::watcher", path = %path.display(), %err, "read failed; dropping event");
            return;
        }
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
        debug!(target: "artel_fs::watcher", path = %path.display(), "skip zero-length file");
        return;
    }

    if guard.should_skip_local(&path, &bytes).await {
        debug!(target: "artel_fs::watcher", path = %path.display(), len = bytes.len(), "echo-guard: skip local (peer-driven write)");
        return;
    }

    let Some(key) = path_to_key_or_emit(&workspace.root, &path, &workspace.events) else {
        return;
    };

    let bytes = Bytes::from(bytes);
    let len = bytes.len();
    debug!(target: "artel_fs::watcher", path = %path.display(), len, "publishing via set_bytes");
    match workspace
        .doc()
        .set_bytes(workspace.author, key, bytes.clone())
        .await
    {
        Ok(_) => {
            debug!(target: "artel_fs::watcher", path = %path.display(), len, "set_bytes ok");
            guard.record_local_publish(&path, &bytes).await;
        }
        Err(err) => {
            warn!(target: "artel_fs::watcher", path = %path.display(), len, %err, "set_bytes failed");
            emit_event(
                &workspace.events,
                WorkspaceEvent::Error(format!("publish {} failed: {err}", path.display())),
            );
        }
    }
}

/// Replay every file under `dir` through [`on_modified`].
///
/// Closes the new-subtree inotify race (see the call site in
/// [`on_modified`]): a file that landed before notify's watch
/// backfill produces no event of its own, so the directory's event
/// is the only signal it exists. Walking is bounded by the workspace
/// filter at each file (hardcoded skips, gitignore, size cap), and
/// the echo guard's last-published hash turns the common "file's own
/// event already published it" case into a no-op.
async fn rescan_dir(
    workspace: &Arc<Workspace>,
    filter: &WorkspaceFilter,
    guard: &EchoGuard,
    dir: &Path,
) {
    debug!(target: "artel_fs::watcher", dir = %dir.display(), "rescan_dir entered");
    let mut files = 0usize;
    for entry in WalkDir::new(dir).follow_links(false).into_iter().flatten() {
        if !entry.file_type().is_file() {
            continue;
        }
        files += 1;
        // Box the recursion: on_modified -> rescan_dir -> on_modified
        // would otherwise be an infinitely-sized future. The cycle is
        // bounded in practice — a walked entry only re-enters the
        // directory branch if it turned into a directory between the
        // walk and the call.
        Box::pin(on_modified(workspace, filter, guard, entry.into_path())).await;
    }
    debug!(target: "artel_fs::watcher", dir = %dir.display(), files, "rescan_dir complete");
}

async fn on_removed(
    workspace: &Arc<Workspace>,
    filter: &WorkspaceFilter,
    guard: &EchoGuard,
    path: PathBuf,
) {
    debug!(target: "artel_fs::watcher", path = %path.display(), "on_removed entered");
    // Echo check: is this unlink the filesystem shadow of a tombstone
    // our applier just applied? Publishing it would re-tombstone the
    // key under OUR author with a newer timestamp, which syncs back
    // to the deleting peer and ping-pongs (see echo_guard module
    // docs, defence 3).
    if guard.should_skip_removal(&path).await {
        debug!(target: "artel_fs::watcher", path = %path.display(), "echo-guard: skip removal (peer-driven delete)");
        return;
    }
    // A genuine local delete. The file is gone, so its last-published
    // hash must go with it — a later re-creation with identical bytes
    // would otherwise be swallowed as an echo (see EchoGuard::forget).
    // Before the write-halt / ReadOnly / filter gates on purpose:
    // those suppress the *tombstone publish*, but the local state
    // fact "this path no longer exists" holds regardless.
    guard.forget(&path).await;
    // Cooperative demotion: a downgraded node stops propagating its
    // own deletes too (voluntary write-stop).
    if workspace.is_write_halted() {
        debug!(target: "artel_fs::watcher", path = %path.display(), "write-halted: skip delete");
        return;
    }
    // Filter gate, mirroring `on_modified`: a path this node's filter
    // refuses to publish must not have its *deletion* published
    // either. Without this, deleting a locally-excluded path would
    // tombstone a peer-published entry for the same key (asymmetric
    // exclude lists across peers) — an outbound write-path leak
    // through the delete side. On macOS, deletes often arrive via
    // `on_modified`'s NotFound fallthrough, which has already run
    // this check; on Linux, `Remove` events land here directly, so
    // the gate must live here too. The symlink/size layers stat the
    // (now-gone) path and pass — only the pure path-shape layers
    // (hardcoded, excluded) can and should gate a removal.
    match filter.check(&path) {
        FilterDecision::Skip(SkipReason::Excluded) => {
            debug!(target: "artel_fs::watcher", path = %path.display(), "filter: skip excluded tombstone");
            emit_event(
                &workspace.events,
                WorkspaceEvent::SkippedExcluded {
                    path,
                    direction: Direction::Outgoing,
                },
            );
            return;
        }
        FilterDecision::Skip(reason) => {
            debug!(target: "artel_fs::watcher", path = %path.display(), reason = ?reason, "filter: skip tombstone");
            return;
        }
        FilterDecision::Include => {}
    }
    // Belt-and-braces with `on_modified`: on macOS, FSEvents reports
    // post-unlink as `Modify(Metadata)` and `on_modified` already
    // gates on `ReadOnly` before its own fallthrough into here. On
    // Linux, `Remove` events arrive here directly and bypass that
    // gate, so the rule check has to live here too.
    let rel = path.strip_prefix(&workspace.root).unwrap_or(&path);
    if workspace.compiled_rules.mode_for(rel) == Mode::ReadOnly {
        debug!(target: "artel_fs::watcher", path = %path.display(), "rules: skip ReadOnly outgoing tombstone");
        emit_event(
            &workspace.events,
            WorkspaceEvent::SkippedReadOnly {
                path,
                direction: Direction::Outgoing,
            },
        );
        return;
    }
    let Some(key) = path_to_key_or_emit(&workspace.root, &path, &workspace.events) else {
        return;
    };
    match workspace.doc().del(workspace.author, key).await {
        Ok(removed) => {
            debug!(target: "artel_fs::watcher", path = %path.display(), removed, "doc.del ok");
        }
        Err(err) => {
            warn!(target: "artel_fs::watcher", path = %path.display(), %err, "doc.del failed");
            emit_event(
                &workspace.events,
                WorkspaceEvent::Error(format!("tombstone {} failed: {err}", path.display())),
            );
        }
    }
}

/// Translate a watcher-observed path to a doc key.
///
/// Mirrors the `on_modified` and `on_removed` failure shape: on
/// [`keys::path_to_key`] error, surface a [`WorkspaceEvent::Error`]
/// to the events stream and return `None` so the caller can bail out.
/// Callers that get `None` must NOT continue with a default / empty
/// key — the contract is "key-or-bail".
///
/// Pulled out so the modify- and remove-side flows can't drift in
/// what they emit on a `path_to_key` failure (the original bug:
/// `on_modified` emitted [`WorkspaceEvent::Error`] but `on_removed`
/// only `tracing::warn!`'d, hiding the failure from event-stream
/// consumers).
fn path_to_key_or_emit(
    root: &Path,
    path: &Path,
    events: &mpsc::Sender<WorkspaceEvent>,
) -> Option<Vec<u8>> {
    match keys::path_to_key(root, path) {
        Ok(k) => Some(k),
        Err(err) => {
            warn!(target: "artel_fs::watcher", path = %path.display(), %err, "path_to_key failed");
            emit_event(
                events,
                WorkspaceEvent::Error(format!("path_to_key {}: {err}", path.display())),
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for the `path_to_key`-failure code path shared by
    //! [`on_modified`] and [`on_removed`].
    //!
    //! Pre-fix `on_removed` only `tracing::warn!`'d on `path_to_key`
    //! failure while `on_modified` emitted [`WorkspaceEvent::Error`];
    //! peer never sees the change either way, but the event-stream
    //! consumer was blind to the remove-side failure. The
    //! `path_to_key_or_emit` helper consolidates the two so the
    //! asymmetry can't recur.
    //!
    //! Trigger: pass a path that isn't under `root`. `path_to_key`
    //! calls `strip_prefix(root)` first and returns
    //! [`crate::WorkspaceError::InvalidPath`] — same code path the
    //! handoff identifies (an invalid-UTF-8 path component would
    //! reach the same `Err(InvalidPath)` arm; the `strip_prefix`
    //! trigger is OS-portable so the test runs on macOS too).
    use super::*;
    use std::path::PathBuf;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, timeout};
    use tracing::error;

    fn root() -> PathBuf {
        PathBuf::from("/workspace")
    }

    /// `path_to_key_or_emit` returns `None` and emits a
    /// `WorkspaceEvent::Error` when the input path can't be
    /// translated. This is the property that `on_removed` was
    /// missing pre-fix.
    #[tokio::test]
    async fn path_to_key_or_emit_surfaces_error_event_on_failure() {
        let (tx, mut rx) = mpsc::channel::<WorkspaceEvent>(8);
        // A path that isn't under `root` makes `path_to_key`'s
        // `strip_prefix` fail.
        let outside = PathBuf::from("/elsewhere/foo.txt");
        let result = path_to_key_or_emit(&root(), &outside, &tx);
        assert!(
            result.is_none(),
            "path_to_key failure must return None — callers rely on \
             this to bail out without a bogus key",
        );
        let ev = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("event must arrive within 1s")
            .expect("channel must not close");
        match ev {
            WorkspaceEvent::Error(msg) => {
                assert!(
                    msg.contains("path_to_key"),
                    "error event should describe the failing operation: {msg}",
                );
                assert!(
                    msg.contains("foo.txt"),
                    "error event should mention the offending path: {msg}",
                );
            }
            other => {
                error!(?other, "unexpected event variant");
                panic!("expected WorkspaceEvent::Error, got {other:?}");
            }
        }
    }

    /// On the happy path the helper returns the key and emits no
    /// event — the events stream is reserved for problems.
    #[tokio::test]
    async fn path_to_key_or_emit_returns_key_on_success() {
        let (tx, mut rx) = mpsc::channel::<WorkspaceEvent>(8);
        let inside = root().join("a").join("b.txt");
        let key = path_to_key_or_emit(&root(), &inside, &tx)
            .expect("happy-path translation must succeed");
        assert_eq!(key, b"path/a/b.txt");
        // Nothing should land on the events channel; a short timeout
        // is enough — `mpsc::Sender::send` is synchronous from the
        // helper's POV, so anything published would already be in
        // the queue.
        let result = timeout(Duration::from_millis(50), rx.recv()).await;
        assert!(
            result.is_err(),
            "happy path must not emit an event; got {result:?}",
        );
    }
}
