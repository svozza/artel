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

use crate::echo_guard::PENDING_RELEASE_GRACE;
use crate::filter::{FilterDecision, SkipReason, WorkspaceFilter};
use crate::workspace::{Workspace, WorkspaceEvent};
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
            let _ = workspace
                .events
                .send(WorkspaceEvent::Error(format!("subscribe failed: {err}")))
                .await;
            return;
        }
    };
    // Subscription is live. Signal readiness so callers blocked in
    // `Workspace::run().await` can proceed. `send` only fails if
    // the receiver was dropped — fine to ignore.
    let _ = ready.send(());

    let guard = EchoGuard::shared(
        workspace.echo_guard.pending_handle(),
        workspace.echo_guard.last_published_handle(),
    );
    let filter = WorkspaceFilter::new(&workspace.root);

    loop {
        tokio::select! {
            () = workspace.shutdown_token.cancelled() => return,
            ev = events.next() => {
                match ev {
                    Some(Ok(LiveEvent::InsertRemote { entry, .. })) => {
                        handle_entry(&workspace, &guard, &filter, entry).await;
                    }
                    Some(Ok(LiveEvent::ContentReady { hash })) => {
                        handle_content_ready(&workspace, &guard, &filter, hash).await;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(err)) => {
                        let _ = workspace
                            .events
                            .send(WorkspaceEvent::Error(format!("doc event error: {err}")))
                            .await;
                    }
                    None => return,
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
            let _ = workspace
                .events
                .send(WorkspaceEvent::Error(format!("invalid key: {err}")))
                .await;
            return;
        }
    };

    if entry.content_len() == 0 {
        let _ = tokio::fs::remove_file(&path).await;
        let _ = workspace
            .events
            .send(WorkspaceEvent::PeerDeleted { path })
            .await;
        return;
    }

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

    // Bytes not yet available locally — applier::run will retry on
    // ContentReady.
    let Ok(bytes) = workspace
        .blobs
        .blobs()
        .get_bytes(entry.content_hash())
        .await
    else {
        return;
    };

    guard.mark_remote_write(&path, &bytes).await;

    if let Some(parent) = path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }

    if let Err(err) = tokio::fs::write(&path, &bytes).await {
        let _ = workspace
            .events
            .send(WorkspaceEvent::Error(format!(
                "write {} failed: {err}",
                path.display(),
            )))
            .await;
        return;
    }

    guard.release_after(path.clone(), PENDING_RELEASE_GRACE);
    let _ = workspace
        .events
        .send(WorkspaceEvent::PeerWrote { path })
        .await;
}

async fn handle_content_ready(
    workspace: &Arc<Workspace>,
    guard: &EchoGuard,
    filter: &WorkspaceFilter,
    hash: iroh_blobs::Hash,
) {
    let stream = match workspace.doc.get_many(Query::all()).await {
        Ok(s) => s,
        Err(err) => {
            let _ = workspace
                .events
                .send(WorkspaceEvent::Error(format!("get_many failed: {err}")))
                .await;
            return;
        }
    };
    let mut stream = Box::pin(stream);

    while let Some(res) = stream.next().await {
        let Ok(entry) = res else { continue };
        if entry.content_hash() == hash {
            handle_entry(workspace, guard, filter, entry).await;
        }
    }
}
