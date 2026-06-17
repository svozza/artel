//! Apply remote doc events to disk.
//!
//! Subscribes to `Doc::subscribe()` and:
//! - on [`LiveEvent::InsertRemote`]: write the entry's content to
//!   disk under [`Workspace::root`], guarded by [`EchoGuard`] so
//!   the watcher won't republish what we just laid down,
//! - on [`LiveEvent::ContentReady`]: scan the doc for entries with
//!   the matching hash and apply them (covers the case where the
//!   `InsertRemote` arrived before bytes were locally available).
//!
//! Tombstones (zero-length entries) become `remove_file`.

#![allow(clippy::redundant_pub_crate)]

use std::sync::Arc;

use futures_util::StreamExt;
use iroh_docs::Entry;
use iroh_docs::engine::LiveEvent;
use iroh_docs::store::Query;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::echo_guard::PENDING_RELEASE_GRACE;
use crate::filter::{FilterDecision, SkipReason, WorkspaceFilter};
use crate::rules::Mode;
use crate::workspace::{Direction, Workspace, WorkspaceEvent, emit_event};
use crate::{EchoGuard, keys};

/// Subscribe to the doc's live event stream and apply incoming
/// remote writes / tombstones / content-ready notifications to disk.
///
/// `ready` is signalled once `doc.subscribe()` has returned a live
/// event stream — i.e. iroh-docs's subscriber list now includes us
/// and any subsequent `InsertRemote` / `ContentReady` will reach
/// this loop. Callers can `await` the matching receiver to know the
/// applier won't drop events fired against `Workspace`'s doc going
/// forward. Without this gate, an event that arrives between
/// [`Workspace::run`] returning and `subscribe()` completing is
/// silently lost — iroh-docs subscribers are push-to-vec, no replay.
///
/// On the early-return error path (`subscribe()` failed), `ready`
/// is dropped without being sent, so the receiver resolves with
/// [`oneshot::error::RecvError`]. The cause is also surfaced via
/// the [`WorkspaceEvent`] stream.
///
/// [`Workspace::run`]: crate::workspace::Workspace::run
pub(crate) async fn run(workspace: Arc<Workspace>, ready: oneshot::Sender<()>) {
    let mut events = match workspace.doc.subscribe().await {
        Ok(s) => s,
        Err(err) => {
            warn!(target: "artel_fs::applier", %err, "doc.subscribe failed");
            let _ = workspace
                .events
                .send(WorkspaceEvent::Error(format!("subscribe failed: {err}")))
                .await;
            return;
        }
    };
    debug!(
        target: "artel_fs::applier",
        root = %workspace.root.display(),
        "subscribed to doc live events"
    );
    // Subscription is live. Signal readiness so callers blocked in
    // `Workspace::run().await` can proceed. `send` only fails if
    // the receiver was dropped — fine to ignore.
    let _ = ready.send(());

    let guard = EchoGuard::shared(
        workspace.echo_guard.pending_handle(),
        workspace.echo_guard.last_published_handle(),
    );
    let filter = WorkspaceFilter::new(&workspace.root);

    // Doc-scoped token: cancelled at workspace shutdown AND on
    // namespace rotation. A child of the workspace shutdown token.
    let doc_token = workspace.doc_token();

    loop {
        tokio::select! {
            () = doc_token.cancelled() => {
                debug!(target: "artel_fs::applier", "doc token tripped, exiting applier loop");
                return;
            }
            ev = events.next() => {
                match ev {
                    Some(Ok(LiveEvent::InsertRemote { entry, .. })) => {
                        debug!(
                            target: "artel_fs::applier",
                            key = %String::from_utf8_lossy(entry.key()),
                            content_len = entry.content_len(),
                            "InsertRemote"
                        );
                        handle_entry(&workspace, &guard, &filter, entry).await;
                    }
                    Some(Ok(LiveEvent::ContentReady { hash })) => {
                        debug!(target: "artel_fs::applier", %hash, "ContentReady");
                        handle_content_ready(&workspace, &guard, &filter, hash).await;
                    }
                    Some(Ok(other)) => {
                        debug!(target: "artel_fs::applier", event = ?other, "ignored live event");
                    }
                    Some(Err(err)) => {
                        warn!(target: "artel_fs::applier", %err, "doc event error");
                        emit_event(
                            &workspace.events,
                            WorkspaceEvent::Error(format!("doc event error: {err}")),
                        );
                    }
                    None => {
                        debug!(target: "artel_fs::applier", "live event stream ended; exiting applier loop");
                        return;
                    }
                }
            }
        }
    }
}

async fn handle_entry(
    workspace: &Arc<Workspace>,
    guard: &EchoGuard,
    filter: &WorkspaceFilter,
    entry: Entry,
) {
    let path = match keys::key_to_path(&workspace.root, entry.key()) {
        Ok(p) => p,
        Err(err) => {
            warn!(
                target: "artel_fs::applier",
                key = %String::from_utf8_lossy(entry.key()),
                %err,
                "invalid key in remote entry"
            );
            emit_event(
                &workspace.events,
                WorkspaceEvent::Error(format!("invalid key: {err}")),
            );
            return;
        }
    };

    // Rule + filter checks sit ABOVE the tombstone branch on
    // purpose: a `ReadOnly` path's incoming tombstone must not
    // trigger `remove_file`, AND a hardcoded-skip / gitignored /
    // too-large path's incoming tombstone must not either. A
    // peer-published tombstone whose key resolves to a path the
    // local filter rejects — asymmetric ignore globs across peers,
    // version drift, an attacker-crafted key targeting `.git/HEAD`
    // — would otherwise reach `tokio::fs::remove_file` regardless,
    // deleting state the workspace was never supposed to touch.
    // `handle_content_ready` retries entries through this function,
    // so this single gate covers both cold and ready paths.
    let rel = path.strip_prefix(&workspace.root).unwrap_or(&path);
    if workspace.compiled_rules.mode_for(rel) == Mode::ReadOnly {
        debug!(target: "artel_fs::applier", path = %path.display(), "rules: skip ReadOnly incoming");
        emit_event(
            &workspace.events,
            WorkspaceEvent::SkippedReadOnly {
                path,
                direction: Direction::Incoming,
            },
        );
        return;
    }

    match filter.check(&path) {
        FilterDecision::Skip(SkipReason::TooLarge { size }) => {
            debug!(target: "artel_fs::applier", path = %path.display(), size, "filter: skip too-large incoming");
            emit_event(
                &workspace.events,
                WorkspaceEvent::SkippedTooLarge {
                    path: path.clone(),
                    size,
                },
            );
            return;
        }
        FilterDecision::Skip(reason) => {
            debug!(target: "artel_fs::applier", path = %path.display(), reason = ?reason, "filter: skip incoming");
            return;
        }
        FilterDecision::Include => {}
    }

    if entry.content_len() == 0 {
        debug!(target: "artel_fs::applier", path = %path.display(), "applying tombstone (remove_file)");
        let _ = tokio::fs::remove_file(&path).await;
        emit_event(&workspace.events, WorkspaceEvent::PeerDeleted { path });
        return;
    }

    // Bytes not yet available locally — applier::run will retry on
    // ContentReady.
    let bytes = match workspace
        .blobs
        .blobs()
        .get_bytes(entry.content_hash())
        .await
    {
        Ok(b) => b,
        Err(err) => {
            debug!(
                target: "artel_fs::applier",
                path = %path.display(),
                hash = %entry.content_hash(),
                %err,
                "blob bytes not yet available; awaiting ContentReady"
            );
            return;
        }
    };

    guard.mark_remote_write(&path, &bytes).await;

    if let Some(parent) = path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }

    if let Err(err) = tokio::fs::write(&path, &bytes).await {
        warn!(target: "artel_fs::applier", path = %path.display(), %err, "fs::write failed");
        emit_event(
            &workspace.events,
            WorkspaceEvent::Error(format!("write {} failed: {err}", path.display())),
        );
        return;
    }

    debug!(target: "artel_fs::applier", path = %path.display(), len = bytes.len(), "applied remote write to disk");
    guard.release_after(path.clone(), PENDING_RELEASE_GRACE);
    emit_event(&workspace.events, WorkspaceEvent::PeerWrote { path });
}

async fn handle_content_ready(
    workspace: &Arc<Workspace>,
    guard: &EchoGuard,
    filter: &WorkspaceFilter,
    hash: iroh_blobs::Hash,
) {
    // No direct rule check here — every entry funnels through
    // `handle_entry` below, which gates on `ReadOnly` before any
    // filter or write work. Keep that ordering invariant if this
    // function ever grows a fast-path: `Mode::ReadOnly` must be
    // honoured *before* the disk write.
    let stream = match workspace.doc.get_many(Query::all()).await {
        Ok(s) => s,
        Err(err) => {
            warn!(target: "artel_fs::applier", %hash, %err, "get_many failed in ContentReady handler");
            emit_event(
                &workspace.events,
                WorkspaceEvent::Error(format!("get_many failed: {err}")),
            );
            return;
        }
    };
    let mut stream = Box::pin(stream);

    let mut matched = 0usize;
    while let Some(res) = stream.next().await {
        let Ok(entry) = res else { continue };
        if entry.content_hash() == hash {
            matched += 1;
            handle_entry(workspace, guard, filter, entry).await;
        }
    }
    debug!(target: "artel_fs::applier", %hash, matched, "ContentReady scan complete");
}
