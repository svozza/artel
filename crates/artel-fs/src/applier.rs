//! Apply remote doc events to disk.
//!
//! Subscribes to `Doc::subscribe()` and:
//! - on [`LiveEvent::InsertRemote`]: stream the entry's content to
//!   disk under [`Workspace::root`] (temp file + atomic rename —
//!   bounded memory at any size), guarded by [`EchoGuard`] so
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

use crate::echo_guard::{PENDING_RELEASE_GRACE, RemoteDeleteMark};
use crate::filter::{FilterDecision, SkipReason, WorkspaceFilter};
use crate::progress::TransferTrackers;
use crate::rules::Mode;
use crate::workspace::{
    ApplyOutcome, Direction, Workspace, WorkspaceEvent, apply_entry_streaming, emit_event,
};
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
    // Snapshot the current doc handle once for this task's lifetime. On
    // namespace rotation the task is cancelled (doc_token) and respawned
    // by re-import, picking up the new handle here.
    let doc = workspace.doc();
    let mut events = match doc.subscribe().await {
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

    // A clone shares the workspace guard's state (Arc-backed), so the
    // watcher's clone observes everything we mark here.
    let guard = workspace.echo_guard.clone();
    let filter = WorkspaceFilter::new(
        &workspace.root,
        workspace.exclude.clone(),
        workspace.max_file_size,
    );

    // Doc-scoped token: cancelled at workspace shutdown AND on
    // namespace rotation. A child of the workspace shutdown token.
    let doc_token = workspace.doc_token();

    // Progress trackers for in-flight blob downloads (issue #38),
    // keyed by content hash. Applier-owned: single-threaded access,
    // no locks. Dropping the registry (any exit from this loop)
    // cancels every tracker task; rotation additionally cancels them
    // via the doc token their tokens are children of.
    let mut trackers = TransferTrackers::new();

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
                        handle_entry(&workspace, &guard, &filter, &mut trackers, &doc_token, entry)
                            .await;
                    }
                    Some(Ok(LiveEvent::ContentReady { hash })) => {
                        debug!(target: "artel_fs::applier", %hash, "ContentReady");
                        handle_content_ready(
                            &workspace, &guard, &filter, &mut trackers, &doc_token, hash,
                        )
                        .await;
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

/// Returns `true` when the entry is left *pending download*: it
/// passed every gate but its blob isn't local yet, so a progress
/// tracker is registered and the entry awaits its `ContentReady`
/// retry. All other outcomes (applied, tombstoned, skipped, errored)
/// return `false`. `handle_content_ready` uses this to decide
/// whether the hash's tracker may be reaped after a rescan.
#[allow(clippy::too_many_lines)]
async fn handle_entry(
    workspace: &Arc<Workspace>,
    guard: &EchoGuard,
    filter: &WorkspaceFilter,
    trackers: &mut TransferTrackers,
    doc_token: &tokio_util::sync::CancellationToken,
    entry: Entry,
) -> bool {
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
            return false;
        }
    };

    // Rule + filter checks sit ABOVE the tombstone branch on
    // purpose: a `ReadOnly` path's incoming tombstone must not
    // trigger `remove_file`, AND a hardcoded-skip / excluded /
    // too-large path's incoming tombstone must not either. A
    // peer-published tombstone whose key resolves to a path the
    // local filter rejects — asymmetric exclude lists across peers,
    // version drift, an attacker-crafted key targeting `.git/HEAD`
    // — would otherwise reach `tokio::fs::remove_file` regardless,
    // deleting state the workspace was never supposed to touch.
    // `handle_content_ready` retries entries through this function,
    // so this single gate covers both cold and ready paths.
    //
    // Filter BEFORE rules, matching the watcher's outgoing order, so
    // a path that is both excluded and `ReadOnly` reports the same
    // skip reason in both directions.
    //
    // Every decisive branch below (skip, tombstone, apply, error)
    // starts by superseding the path in the tracker registry: this
    // entry is the key's newest state, so any tracker still narrating
    // an *older* entry's download for the path is stale — its events
    // would advertise a file that will never land (issue #38).
    match filter.check(&path) {
        FilterDecision::Skip(SkipReason::TooLarge { size }) => {
            debug!(target: "artel_fs::applier", path = %path.display(), size, "filter: skip too-large incoming");
            trackers.supersede(&path, None);
            emit_event(
                &workspace.events,
                WorkspaceEvent::SkippedTooLarge {
                    path: path.clone(),
                    size,
                },
            );
            return false;
        }
        FilterDecision::Skip(SkipReason::Excluded) => {
            debug!(target: "artel_fs::applier", path = %path.display(), "filter: skip excluded incoming");
            trackers.supersede(&path, None);
            emit_event(
                &workspace.events,
                WorkspaceEvent::SkippedExcluded {
                    path: path.clone(),
                    direction: Direction::Incoming,
                },
            );
            return false;
        }
        FilterDecision::Skip(reason) => {
            debug!(target: "artel_fs::applier", path = %path.display(), reason = ?reason, "filter: skip incoming");
            trackers.supersede(&path, None);
            return false;
        }
        FilterDecision::Include => {}
    }

    // Incoming size cap on the *entry's* declared length. The
    // filter's own size layer stats the local path, which doesn't
    // exist yet for a new incoming file — without this check a peer
    // running uncapped (or a larger cap) could push an arbitrarily
    // large file straight past a capped node's guard. Tombstones
    // (len 0) are never caught, preserving the tombstone flow below.
    if let Some(cap) = workspace.max_file_size
        && entry.content_len() > cap
    {
        let size = entry.content_len();
        debug!(target: "artel_fs::applier", path = %path.display(), size, "entry over cap; skip incoming");
        trackers.supersede(&path, None);
        emit_event(
            &workspace.events,
            WorkspaceEvent::SkippedTooLarge {
                path: path.clone(),
                size,
            },
        );
        return false;
    }

    let rel = path.strip_prefix(&workspace.root).unwrap_or(&path);
    if workspace.compiled_rules.mode_for(rel) == Mode::ReadOnly {
        debug!(target: "artel_fs::applier", path = %path.display(), "rules: skip ReadOnly incoming");
        trackers.supersede(&path, None);
        emit_event(
            &workspace.events,
            WorkspaceEvent::SkippedReadOnly {
                path,
                direction: Direction::Incoming,
            },
        );
        return false;
    }

    if entry.content_len() == 0 {
        trackers.supersede(&path, None);
        apply_tombstone(guard, &workspace.events, path).await;
        return false;
    }

    // Stream the blob to disk (temp file + rename — see
    // `apply_entry_streaming`). `NotReady` preserves the old
    // "bytes not yet local → wait for ContentReady" retry contract.
    match apply_entry_streaming(&workspace.doc(), &workspace.blobs, guard, &entry, &path).await {
        Ok(ApplyOutcome::Applied) => {
            debug!(
                target: "artel_fs::applier",
                path = %path.display(),
                len = entry.content_len(),
                "applied remote write to disk"
            );
            guard.release_after(path.clone(), PENDING_RELEASE_GRACE);
            // Kill every tracker narrating this path BEFORE the
            // terminal event: supersede's emit-under-lock barrier
            // covers trackers for *older* hashes, and finish awaits
            // this hash's own tracker's death — the events channel
            // is FIFO, so nothing can enqueue a Transferring behind
            // the PeerWrote.
            trackers.supersede(&path, Some(entry.content_hash()));
            trackers.finish(entry.content_hash()).await;
            emit_event(&workspace.events, WorkspaceEvent::PeerWrote { path });
            false
        }
        Ok(ApplyOutcome::NotReady) => {
            debug!(
                target: "artel_fs::applier",
                path = %path.display(),
                hash = %entry.content_hash(),
                "blob not yet available; awaiting ContentReady"
            );
            // Every gate above passed — we genuinely intend to apply
            // this entry once its bytes arrive. Surface the download
            // as throttled Transferring events (issue #38). `track`
            // supersedes the path's older trackers itself.
            trackers.track(
                &workspace.blobs,
                &workspace.events,
                doc_token,
                entry.content_hash(),
                entry.content_len(),
                path,
            );
            true
        }
        Err(err) => {
            warn!(target: "artel_fs::applier", path = %path.display(), %err, "apply failed");
            trackers.supersede(&path, None);
            emit_event(
                &workspace.events,
                WorkspaceEvent::Error(format!("write {} failed: {err}", path.display())),
            );
            false
        }
    }
}

/// Apply a peer tombstone to disk: mark the echo guard, remove the
/// file, emit [`WorkspaceEvent::PeerDeleted`].
///
/// Shared by the applier's `content_len() == 0` branch and the
/// `test-utils` hook that drives this operation directly from tests
/// (see `Workspace::test_apply_peer_tombstone`).
pub(crate) async fn apply_tombstone(
    guard: &EchoGuard,
    events: &tokio::sync::mpsc::Sender<WorkspaceEvent>,
    path: std::path::PathBuf,
) {
    debug!(target: "artel_fs::applier", path = %path.display(), "applying tombstone (remove_file)");
    // Mark BEFORE the remove so the watcher can't observe the
    // unlink first. Suppresses the removal echo and drops the
    // stale last-published hash (see EchoGuard::mark_remote_delete).
    match guard.mark_remote_delete(&path).await {
        RemoteDeleteMark::Duplicate => {
            // Our remove_file already ran for the original tombstone;
            // if the path exists NOW, it's a genuine local write that
            // raced in between the duplicates — deleting it would
            // swallow an unpublished creation with nothing to heal it
            // (the watcher's events for that write read NotFound
            // post-remove and are suppressed as peer-delete echoes).
            // Skip the remove; the marker stays armed, so echoes of
            // the original unlink are still eaten.
            debug!(
                target: "artel_fs::applier",
                path = %path.display(),
                "duplicate tombstone for already-deleted path; skipping remove_file"
            );
            return;
        }
        RemoteDeleteMark::Fresh => {}
    }
    let _ = tokio::fs::remove_file(&path).await;
    emit_event(events, WorkspaceEvent::PeerDeleted { path });
}

async fn handle_content_ready(
    workspace: &Arc<Workspace>,
    guard: &EchoGuard,
    filter: &WorkspaceFilter,
    trackers: &mut TransferTrackers,
    doc_token: &tokio_util::sync::CancellationToken,
    hash: iroh_blobs::Hash,
) {
    // No direct rule check here — every entry funnels through
    // `handle_entry` below, which gates on `ReadOnly` before any
    // filter or write work. Keep that ordering invariant if this
    // function ever grows a fast-path: `Mode::ReadOnly` must be
    // honoured *before* the disk write.
    //
    // `trackers.tracked_paths(hash)` already holds exactly the keys
    // NotReady-registered for this hash (`TransferTrackers::track`,
    // issue #38) — the tracker's path set IS the set of pending
    // consumers, kept in sync by `supersede`/`track` on every
    // `handle_entry` call. Re-deriving them via one `key_exact` lookup
    // per key is a bounded by-key-index range scan (O(matches)), vs.
    // `Query::all()`'s O(total live entries) scan. A hash with no
    // tracker (never registered, or dropped by the
    // `MAX_CONCURRENT_TRACKERS` cap) falls back to the full scan —
    // rare enough that the fallback's cost doesn't matter, and it
    // preserves the "every entry is retried on ContentReady, tracked
    // or not" correctness contract the cap fix relies on.
    let tracked = trackers.tracked_paths(hash);
    let mut matched = 0usize;
    let mut still_pending = false;
    if tracked.is_empty() {
        let stream = match workspace.doc().get_many(Query::all()).await {
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
        while let Some(res) = stream.next().await {
            let Ok(entry) = res else { continue };
            if entry.content_hash() == hash {
                matched += 1;
                still_pending |=
                    handle_entry(workspace, guard, filter, trackers, doc_token, entry).await;
            }
        }
    } else {
        for path in tracked {
            let Ok(key) = keys::path_to_key(&workspace.root, &path) else {
                continue;
            };
            let entry = match workspace
                .doc()
                .get_one(Query::single_latest_per_key().key_exact(key))
                .await
            {
                Ok(entry) => entry,
                Err(err) => {
                    warn!(target: "artel_fs::applier", %hash, path = %path.display(), %err, "get_one failed in ContentReady handler");
                    emit_event(
                        &workspace.events,
                        WorkspaceEvent::Error(format!("get_one failed: {err}")),
                    );
                    continue;
                }
            };
            let Some(entry) = entry else { continue };
            if entry.content_hash() == hash {
                matched += 1;
                still_pending |=
                    handle_entry(workspace, guard, filter, trackers, doc_token, entry).await;
            }
        }
    }
    // handle_entry's Applied arm reaps the tracker per entry; this
    // covers the residue when NO entry for the hash is left pending —
    // every one applied or was skipped/superseded, so the download is
    // no longer worth narrating. If an entry re-returned NotReady
    // (blob-status race: ContentReady fired but the status read still
    // said Partial), its freshly re-registered tracker must survive
    // for the eventual retry.
    if !still_pending {
        trackers.finish(hash).await;
    }
    debug!(target: "artel_fs::applier", %hash, matched, "ContentReady scan complete");
}
