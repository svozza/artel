//! Transfer-progress tracking for incoming large blobs (issue #38).
//!
//! When the applier sees an `InsertRemote` whose blob is not yet
//! fully local ([`ApplyOutcome::NotReady`]), the entry goes silent
//! until `ContentReady` fires — minutes of dead air for a multi-GB
//! blob over a relay. This module surfaces that window as throttled
//! [`WorkspaceEvent::Transferring`] events by driving
//! `blobs().observe(hash)` — a live stream of [`Bitfield`]s
//! describing which byte ranges are locally present. Purely local
//! observation: no transport or wire changes.
//!
//! One tracker task per in-flight *hash*, not per path —
//! `ContentReady` is hash-keyed and multiple doc keys can reference
//! one blob. The tracker holds the path set and emits one event per
//! path (paths are what consumers key on).
//!
//! # Ordering barrier
//!
//! The tracker task emits **while holding the path-set lock**, and
//! [`TransferTrackers::supersede`] removes a path under the same
//! lock. Once `supersede` returns, any event for the removed path is
//! already in the channel — so everything the applier enqueues
//! afterwards (a `PeerWrote` for a different hash, a `PeerDeleted`)
//! is FIFO-ordered after every stale emission. No `Transferring` can
//! trail its path's terminal event.
//!
//! [`ApplyOutcome::NotReady`]: crate::workspace::ApplyOutcome::NotReady
//! [`Bitfield`]: iroh_blobs::api::proto::Bitfield

#![allow(clippy::redundant_pub_crate)]

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use iroh_blobs::Hash;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::workspace::{WorkspaceEvent, emit_event};

/// Minimum progress between emissions, as a percentage of the blob's
/// total length. Deliberately not configurable: the stream is
/// advisory and nobody should tune this.
const MIN_DELTA_PERCENT: u64 = 1;

/// Minimum elapsed time between emissions when the percent gate
/// hasn't been crossed but `received` has still changed.
const MIN_INTERVAL: Duration = Duration::from_millis(500);

/// Ceiling on the number of distinct-hash tracker tasks
/// [`TransferTrackers`] will have live at once.
///
/// Every previously-unseen hash reaching [`TransferTrackers::track`]
/// spawns a task plus a `blobs().observe(hash)` stream — real,
/// unbounded-by-default resource use (task, channel, `Arc<Mutex<_>>`)
/// keyed purely on *entry count*, not bytes: a peer with write access
/// can drive this by publishing many distinct-hash entries just under
/// `max_file_size`, without ever having to serve real content for any
/// of them (`apply_entry_streaming`'s `NotReady` check is a purely
/// local blob-status read). Once the cap is hit, a newly-seen hash is
/// simply not narrated — the entry stays pending and is still applied
/// whenever its `ContentReady` fires; only the advisory `Transferring`
/// progress feed for that download is missing, same lossy contract as
/// any other event this module already documents as best-effort.
const MAX_CONCURRENT_TRACKERS: usize = 256;

/// Decides whether a progress observation is worth an event.
///
/// Emits when either (a) at least [`MIN_DELTA_PERCENT`] of `total`
/// progressed since the last emission, or (b) at least
/// [`MIN_INTERVAL`] elapsed since the last emission *and* `received`
/// changed. The first observation always emits — it gives consumers
/// an immediate "transfer started" signal with `received ≈ 0`.
struct ProgressThrottle {
    total: u64,
    /// `(received, at)` of the last emission; `None` until the first.
    last: Option<(u64, Instant)>,
}

impl ProgressThrottle {
    const fn new(total: u64) -> Self {
        Self { total, last: None }
    }

    /// `true` if an event should be emitted for `received` bytes
    /// observed at `now`. Updates the internal marker on `true`.
    fn should_emit(&mut self, received: u64, now: Instant) -> bool {
        let Some((last_received, last_at)) = self.last else {
            self.last = Some((received, now));
            return true;
        };
        if received == last_received {
            return false;
        }
        // For totals under 100 bytes the integer step degenerates to
        // 0; clamp to 1 so "any change" is the gate rather than
        // "every observation".
        let step = (self.total / 100 * MIN_DELTA_PERCENT).max(1);
        if received.saturating_sub(last_received) >= step
            || now.duration_since(last_at) >= MIN_INTERVAL
        {
            self.last = Some((received, now));
            return true;
        }
        false
    }
}

/// One live tracker: the task driving `observe()`, the path set it
/// fans events out to, and the token that kills it.
struct Tracker {
    /// Doc keys (as absolute paths) currently pointing at this hash,
    /// each mapped to *its* entry's declared content length —
    /// entries sharing a hash may declare different lengths, and a
    /// path's events must carry its own entry's `total`. Shared with
    /// the tracker task; see the module docs for the emit-under-lock
    /// ordering barrier this mutex provides.
    paths: Arc<Mutex<BTreeMap<PathBuf, u64>>>,
    /// Highest `received` the task has observed (the task only ever
    /// raises it). Lets [`TransferTrackers::track`] emit an
    /// immediate, current event for a path that joins after the
    /// download started — or after it silently completed and the
    /// task already exited.
    latest: Arc<AtomicU64>,
    /// Child of the applier's doc token — rotation or shutdown
    /// cancels the parent and every tracker dies with it.
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl Drop for Tracker {
    fn drop(&mut self) {
        // Belt over the doc-token braces: whatever path drops the
        // registry (applier exit on stream end, supersede-to-empty),
        // the observe task must not outlive it.
        self.cancel.cancel();
    }
}

/// Registry of in-flight download trackers, keyed by content hash.
///
/// Owned by the applier task (single-threaded access, no locks
/// beyond each tracker's shared path set). Dropped with the applier
/// on rotation — correct, since the tasks select on children of the
/// doc token the rotation cancels.
pub(crate) struct TransferTrackers {
    trackers: HashMap<Hash, Tracker>,
}

impl TransferTrackers {
    pub(crate) fn new() -> Self {
        Self {
            trackers: HashMap::new(),
        }
    }

    /// A new entry for `path` points at `keep` (or at nothing worth
    /// tracking — a tombstone, an applied blob, a skipped entry:
    /// `None`): every tracker for a *different* hash still holding
    /// this path is stale. Drop the path; drop the tracker when its
    /// path set empties.
    ///
    /// Ordering: the path is removed under the same lock the tracker
    /// task emits under, so when this returns no further event for
    /// `path` can be enqueued by those trackers (module docs).
    pub(crate) fn supersede(&mut self, path: &Path, keep: Option<Hash>) {
        self.trackers.retain(|hash, tracker| {
            if Some(*hash) == keep {
                return true;
            }
            let now_empty = {
                let mut paths = tracker.paths.lock().expect("tracker paths mutex");
                paths.remove(path);
                paths.is_empty()
            };
            if now_empty {
                debug!(
                    target: "artel_fs::progress",
                    %hash,
                    path = %path.display(),
                    "tracker superseded; cancelling"
                );
                tracker.cancel.cancel();
            }
            !now_empty
        });
    }

    /// Start (or extend) tracking `hash`: an entry for `path` with
    /// declared length `total` passed all apply gates but its blob
    /// isn't fully local yet.
    ///
    /// A path joining an *existing* tracker gets an immediate event
    /// carrying the download's current state — its "transfer
    /// started" signal must not wait for the next throttled update
    /// (which may never come if the blob completed and the task
    /// already exited; the entry's `ContentReady` retry still
    /// applies it).
    ///
    /// `doc_token` must be the token the calling applier captured at
    /// spawn — not re-read from the workspace, where rotation may
    /// already have installed the next namespace's token.
    ///
    /// A hash not already tracked is silently dropped once
    /// [`MAX_CONCURRENT_TRACKERS`] live trackers exist — the entry
    /// itself is unaffected (still applied on `ContentReady`), only
    /// its progress narration is skipped.
    pub(crate) fn track(
        &mut self,
        blobs: &iroh_blobs::BlobsProtocol,
        events: &tokio::sync::mpsc::Sender<WorkspaceEvent>,
        doc_token: &CancellationToken,
        hash: Hash,
        total: u64,
        path: PathBuf,
    ) {
        self.supersede(&path, Some(hash));
        if let Some(tracker) = self.trackers.get(&hash) {
            // Re-announce of a known (path, hash) — e.g. the
            // ContentReady rescan re-feeding an entry — refreshes the
            // total but must not duplicate the join event. Emitting
            // after the lock drops is safe here: supersede only ever
            // runs on this same applier task, so no removal can
            // interleave before the emit.
            let newly_joined = {
                let mut paths = tracker.paths.lock().expect("tracker paths mutex");
                paths.insert(path.clone(), total).is_none()
            };
            if newly_joined {
                let received = tracker.latest.load(Ordering::Relaxed).min(total);
                emit_event(
                    events,
                    WorkspaceEvent::Transferring {
                        path,
                        received,
                        total,
                    },
                );
            }
            return;
        }
        if self.trackers.len() >= MAX_CONCURRENT_TRACKERS {
            debug!(
                target: "artel_fs::progress",
                %hash,
                total,
                path = %path.display(),
                cap = MAX_CONCURRENT_TRACKERS,
                "tracker cap reached; not narrating this download's progress"
            );
            return;
        }
        debug!(
            target: "artel_fs::progress",
            %hash,
            total,
            path = %path.display(),
            "spawning transfer tracker"
        );
        let paths = Arc::new(Mutex::new(BTreeMap::from([(path, total)])));
        let latest = Arc::new(AtomicU64::new(0));
        let cancel = doc_token.child_token();
        let task = tokio::spawn(track_task(
            blobs.clone(),
            events.clone(),
            Arc::clone(&paths),
            Arc::clone(&latest),
            cancel.clone(),
            hash,
            total,
        ));
        self.trackers.insert(
            hash,
            Tracker {
                paths,
                latest,
                cancel,
                task,
            },
        );
    }

    /// Paths currently pointing at `hash`, if any tracker exists for
    /// it. Lets [`crate::applier::handle_content_ready`] resolve which
    /// doc keys a `ContentReady` hash affects without a full-document
    /// scan — only paths this registry already knows are waiting on
    /// `hash` need re-checking.
    pub(crate) fn tracked_paths(&self, hash: Hash) -> Vec<PathBuf> {
        self.trackers
            .get(&hash)
            .map(|tracker| {
                tracker
                    .paths
                    .lock()
                    .expect("tracker paths mutex")
                    .keys()
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The blob behind `hash` is ready (its `ContentReady` arrived,
    /// or an entry applied directly): stop the tracker and **await
    /// its death**. Combined with the emit-under-lock barrier this
    /// guarantees nothing for the hash's paths trails the terminal
    /// `PeerWrote` the caller emits next (FIFO channel).
    pub(crate) async fn finish(&mut self, hash: Hash) {
        if let Some(mut tracker) = self.trackers.remove(&hash) {
            tracker.cancel.cancel();
            // By &mut, not by value: `Tracker` has a `Drop` impl, so
            // the handle can't be moved out. `JoinHandle` is `Unpin`.
            let _ = (&mut tracker.task).await;
        }
    }
}

/// Drive `observe(hash)` and emit throttled [`WorkspaceEvent::Transferring`]
/// events for every path currently referencing the hash.
///
/// `throttle_total` is the spawning entry's declared length — used
/// only for the percent gate; each path's event carries the path's
/// own total from the shared map.
async fn track_task(
    blobs: iroh_blobs::BlobsProtocol,
    events: tokio::sync::mpsc::Sender<WorkspaceEvent>,
    paths: Arc<Mutex<BTreeMap<PathBuf, u64>>>,
    latest: Arc<AtomicU64>,
    cancel: CancellationToken,
    hash: Hash,
    throttle_total: u64,
) {
    // Setup is select-guarded too: if the store's observe request
    // stalls, `finish()`'s cancel must still terminate this task
    // rather than leaving the applier awaiting it forever.
    let stream = tokio::select! {
        () = cancel.cancelled() => return,
        s = blobs.store().blobs().observe(hash).stream() => match s {
            Ok(s) => s,
            Err(err) => {
                debug!(target: "artel_fs::progress", %hash, %err, "observe failed; no progress events");
                return;
            }
        },
    };
    tokio::pin!(stream);
    let mut throttle = ProgressThrottle::new(throttle_total);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            item = stream.next() => {
                let Some(bitfield) = item else { return };
                // Monotone by construction: a later bitfield never
                // reports less than the max we've already seen (the
                // store's observations are cumulative, but the
                // documented non-decreasing guarantee must not lean
                // on that implementation detail).
                let raw = bitfield.total_bytes();
                let received = latest.fetch_max(raw, Ordering::Relaxed).max(raw);
                if throttle.should_emit(received, Instant::now()) {
                    // Emit under the lock — the supersede barrier
                    // (module docs). `emit_event` is try_send: no
                    // await, no blocking, safe under a std mutex.
                    let paths = paths.lock().expect("tracker paths mutex");
                    for (path, total) in paths.iter() {
                        emit_event(
                            &events,
                            WorkspaceEvent::Transferring {
                                path: path.clone(),
                                // A path's entry may declare a length
                                // differing from the bitfield's size;
                                // clamp so received <= total holds
                                // per event.
                                received: received.min(*total),
                                total: *total,
                            },
                        );
                    }
                }
                if bitfield.is_complete() {
                    // No further updates will ever arrive; exit and
                    // free the observation. The registry entry is
                    // reaped by `finish` on ContentReady.
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- ProgressThrottle ----

    const T0: u64 = 8 * 1024 * 1024;

    #[test]
    fn first_observation_always_emits() {
        let mut t = ProgressThrottle::new(T0);
        assert!(t.should_emit(0, Instant::now()));
    }

    #[test]
    fn unchanged_received_never_emits_even_after_interval() {
        let mut t = ProgressThrottle::new(T0);
        let start = Instant::now();
        assert!(t.should_emit(100, start));
        assert!(!t.should_emit(100, start + MIN_INTERVAL * 4));
    }

    #[test]
    fn sub_percent_within_interval_does_not_emit() {
        let mut t = ProgressThrottle::new(T0);
        let start = Instant::now();
        assert!(t.should_emit(0, start));
        // 1% of 8 MiB is 83_886 bytes; one byte under stays quiet.
        assert!(!t.should_emit(T0 / 100 - 1, start + Duration::from_millis(1)));
    }

    #[test]
    fn percent_gate_emits_regardless_of_elapsed_time() {
        let mut t = ProgressThrottle::new(T0);
        let start = Instant::now();
        assert!(t.should_emit(0, start));
        assert!(t.should_emit(T0 / 100, start));
    }

    #[test]
    fn time_gate_emits_only_when_received_changed() {
        let mut t = ProgressThrottle::new(T0);
        let start = Instant::now();
        assert!(t.should_emit(0, start));
        let later = start + MIN_INTERVAL;
        assert!(t.should_emit(1, later));
        // Marker advanced: the same instant no longer satisfies the
        // time gate for the next byte.
        assert!(!t.should_emit(2, later));
    }

    #[test]
    fn tiny_total_clamps_step_to_one_byte() {
        let mut t = ProgressThrottle::new(50);
        let start = Instant::now();
        assert!(t.should_emit(0, start));
        // total/100 == 0; without the clamp this would emit on a
        // zero-byte "advance". With it, any 1-byte change emits.
        assert!(t.should_emit(1, start));
    }

    // ---- TransferTrackers ----

    use iroh_blobs::BlobsProtocol;
    use iroh_blobs::store::mem::MemStore;

    /// In-memory store that holds nothing — `observe` reports an
    /// empty bitfield, so a tracker emits its first observation
    /// (`received == 0`) and then idles until cancelled.
    fn test_blobs() -> BlobsProtocol {
        let store = MemStore::new();
        BlobsProtocol::new(&store, None)
    }

    async fn recv_transferring(
        rx: &mut tokio::sync::mpsc::Receiver<WorkspaceEvent>,
    ) -> (PathBuf, u64, u64) {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("event within budget")
            .expect("channel open")
        {
            WorkspaceEvent::Transferring {
                path,
                received,
                total,
            } => (path, received, total),
            other => panic!("expected Transferring, got {other:?}"),
        }
    }

    fn tracked_paths(registry: &TransferTrackers, hash: Hash) -> BTreeMap<PathBuf, u64> {
        let tracker = registry.trackers.get(&hash).expect("tracker present");
        let paths = tracker.paths.lock().expect("tracker paths mutex");
        paths.clone()
    }

    #[tokio::test]
    async fn two_paths_one_hash_each_get_first_observation() {
        let blobs = test_blobs();
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let doc_token = CancellationToken::new();
        let mut registry = TransferTrackers::new();
        let hash = Hash::new(b"absent blob");

        registry.track(&blobs, &tx, &doc_token, hash, 1_000, "/ws/a.bin".into());
        // b.bin joins the existing tracker: its join event fires
        // immediately (current-thread runtime — the spawned task
        // hasn't polled yet, so `latest` is still 0).
        registry.track(&blobs, &tx, &doc_token, hash, 1_000, "/ws/b.bin".into());
        assert_eq!(registry.trackers.len(), 1, "one tracker per hash");

        // Expected: b.bin's join event, then the task's first
        // observation fans out to both paths. Every event reads
        // received == 0 (nothing is local).
        let mut counts: BTreeMap<PathBuf, usize> = BTreeMap::new();
        for _ in 0..3 {
            let (path, received, total) = recv_transferring(&mut rx).await;
            assert_eq!(received, 0);
            assert_eq!(total, 1_000);
            *counts.entry(path).or_default() += 1;
        }
        assert!(
            counts.contains_key(Path::new("/ws/a.bin"))
                && counts.contains_key(Path::new("/ws/b.bin")),
            "both paths must be narrated: {counts:?}",
        );

        registry.finish(hash).await;
        assert!(registry.trackers.is_empty());
    }

    #[tokio::test]
    async fn late_join_gets_immediate_event_with_its_own_total() {
        let blobs = test_blobs();
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let doc_token = CancellationToken::new();
        let mut registry = TransferTrackers::new();
        let hash = Hash::new(b"absent blob");

        registry.track(&blobs, &tx, &doc_token, hash, 1_000, "/ws/a.bin".into());
        let (path, _, _) = recv_transferring(&mut rx).await;
        assert_eq!(path, PathBuf::from("/ws/a.bin"));

        // The task has polled (its first observation arrived above).
        // A second key with a *different declared length* joins: it
        // must get an immediate event carrying ITS total, not wait
        // for the next throttled update.
        registry.track(&blobs, &tx, &doc_token, hash, 500, "/ws/b.bin".into());
        let (path, received, total) = recv_transferring(&mut rx).await;
        assert_eq!(path, PathBuf::from("/ws/b.bin"));
        assert_eq!(total, 500, "join event must carry the path's own total");
        assert!(received <= total);

        registry.finish(hash).await;
    }

    #[tokio::test]
    async fn doc_token_cancel_kills_tracker_no_stale_events() {
        let blobs = test_blobs();
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let doc_token = CancellationToken::new();
        let mut registry = TransferTrackers::new();
        let hash = Hash::new(b"absent blob");

        registry.track(&blobs, &tx, &doc_token, hash, 1_000, "/ws/a.bin".into());
        let _ = recv_transferring(&mut rx).await;

        // Rotation / shutdown path: the doc token is the parent of
        // every tracker's token — cancelling it must end the task.
        doc_token.cancel();
        let mut tracker = registry.trackers.remove(&hash).expect("tracker present");
        tokio::time::timeout(Duration::from_secs(5), &mut tracker.task)
            .await
            .expect("tracker exits on doc-token cancel")
            .expect("tracker task panicked");

        // Nothing may trail the cancellation.
        drop(tx);
        assert!(rx.recv().await.is_none(), "stale event after cancel");
    }

    #[tokio::test]
    async fn supersede_drops_path_then_tracker_when_empty() {
        let blobs = test_blobs();
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let doc_token = CancellationToken::new();
        let mut registry = TransferTrackers::new();
        let hash = Hash::new(b"absent blob");

        registry.track(&blobs, &tx, &doc_token, hash, 1_000, "/ws/a.bin".into());
        registry.track(&blobs, &tx, &doc_token, hash, 1_000, "/ws/b.bin".into());
        for _ in 0..3 {
            let _ = recv_transferring(&mut rx).await;
        }

        // A newer entry for a.bin under a different hash evicts the
        // stale path but keeps the tracker (b.bin still pending).
        let newer = Hash::new(b"newer content");
        registry.supersede(Path::new("/ws/a.bin"), Some(newer));
        assert_eq!(
            tracked_paths(&registry, hash),
            BTreeMap::from([(PathBuf::from("/ws/b.bin"), 1_000)]),
        );

        // Evicting the last path cancels and drops the tracker.
        registry.supersede(Path::new("/ws/b.bin"), None);
        assert!(registry.trackers.is_empty(), "empty tracker reaped");

        drop(tx);
        assert!(rx.recv().await.is_none(), "no events after supersede");
    }

    #[tokio::test]
    async fn same_hash_reannounce_does_not_duplicate_join_event() {
        let blobs = test_blobs();
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let doc_token = CancellationToken::new();
        let mut registry = TransferTrackers::new();
        let hash = Hash::new(b"absent blob");

        registry.track(&blobs, &tx, &doc_token, hash, 1_000, "/ws/a.bin".into());
        // Re-announce of the same (path, hash) — e.g. the
        // ContentReady rescan re-feeding the entry — must not evict
        // the path from its own tracker, and must not emit a second
        // join event for a path already present.
        registry.track(&blobs, &tx, &doc_token, hash, 1_000, "/ws/a.bin".into());
        assert_eq!(
            tracked_paths(&registry, hash),
            BTreeMap::from([(PathBuf::from("/ws/a.bin"), 1_000)]),
        );

        // Exactly one event: the task's first observation. (The
        // re-announce emitted nothing.)
        let _ = recv_transferring(&mut rx).await;
        registry.finish(hash).await;
        drop(tx);
        assert!(
            rx.recv().await.is_none(),
            "re-announce must not duplicate events",
        );
    }

    /// A flood of distinct-hash `NotReady` entries — the resource-
    /// exhaustion vector this cap closes — must not grow the tracker
    /// registry past [`MAX_CONCURRENT_TRACKERS`]. Every hash claims a
    /// distinct, never-served blob: no real bytes need exist for the
    /// attack, only distinct doc-entry hashes.
    #[tokio::test]
    async fn tracker_count_is_capped() {
        let blobs = test_blobs();
        let (tx, rx) = tokio::sync::mpsc::channel(4096);
        let doc_token = CancellationToken::new();
        let mut registry = TransferTrackers::new();

        for i in 0..(MAX_CONCURRENT_TRACKERS + 50) {
            let hash = Hash::new(format!("distinct-blob-{i}"));
            registry.track(
                &blobs,
                &tx,
                &doc_token,
                hash,
                1_000,
                PathBuf::from(format!("/ws/file-{i}.bin")),
            );
        }

        assert_eq!(
            registry.trackers.len(),
            MAX_CONCURRENT_TRACKERS,
            "tracker registry must never grow past the cap regardless of \
             how many distinct hashes are announced",
        );

        drop(tx);
        drop(rx);
    }

    /// Once at the cap, a hash already being tracked must still be
    /// extendable (a second path joining it, or a re-announce) — the
    /// cap bounds *distinct hashes*, not updates to hashes already
    /// admitted.
    #[tokio::test]
    async fn existing_tracker_still_extendable_once_at_cap() {
        let blobs = test_blobs();
        let (tx, rx) = tokio::sync::mpsc::channel(4096);
        let doc_token = CancellationToken::new();
        let mut registry = TransferTrackers::new();

        let admitted_hash = Hash::new(b"admitted first");
        registry.track(
            &blobs,
            &tx,
            &doc_token,
            admitted_hash,
            1_000,
            "/ws/a.bin".into(),
        );

        // Fill the rest of the cap with other distinct hashes.
        for i in 0..(MAX_CONCURRENT_TRACKERS - 1) {
            let hash = Hash::new(format!("filler-{i}"));
            registry.track(
                &blobs,
                &tx,
                &doc_token,
                hash,
                1_000,
                PathBuf::from(format!("/ws/filler-{i}.bin")),
            );
        }
        assert_eq!(registry.trackers.len(), MAX_CONCURRENT_TRACKERS);

        // A brand-new hash is dropped (cap reached)...
        let over_cap_hash = Hash::new(b"over the cap");
        registry.track(
            &blobs,
            &tx,
            &doc_token,
            over_cap_hash,
            1_000,
            "/ws/over-cap.bin".into(),
        );
        assert_eq!(registry.trackers.len(), MAX_CONCURRENT_TRACKERS);
        assert!(
            !registry.trackers.contains_key(&over_cap_hash),
            "a hash announced past the cap must not get a tracker",
        );

        // ...but a second path joining the already-admitted hash still
        // works, and the already-admitted hash is unaffected.
        registry.track(
            &blobs,
            &tx,
            &doc_token,
            admitted_hash,
            1_000,
            "/ws/b.bin".into(),
        );
        assert_eq!(
            tracked_paths(&registry, admitted_hash),
            BTreeMap::from([
                (PathBuf::from("/ws/a.bin"), 1_000),
                (PathBuf::from("/ws/b.bin"), 1_000),
            ]),
            "an already-admitted hash must still accept new paths at the cap",
        );

        drop(tx);
        drop(rx);
    }
}
