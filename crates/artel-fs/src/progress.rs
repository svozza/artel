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
//! [`ApplyOutcome::NotReady`]: crate::workspace::ApplyOutcome::NotReady
//! [`Bitfield`]: iroh_blobs::api::proto::Bitfield

#![allow(clippy::redundant_pub_crate)]

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
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
    /// Doc keys (as absolute paths) currently pointing at this hash.
    /// Shared with the tracker task, which snapshots it per emission;
    /// the applier mutates it on supersede / additional keys.
    paths: Arc<Mutex<BTreeSet<PathBuf>>>,
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

    /// A new entry for `path` points at `keep` (or is a tombstone /
    /// skipped entry — `None`): every tracker for a *different* hash
    /// still holding this path is stale. Drop the path; drop the
    /// tracker when its path set empties.
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

    /// Start (or extend) tracking `hash`: an entry for `path` passed
    /// all apply gates but its blob isn't fully local yet.
    ///
    /// `doc_token` must be the token the calling applier captured at
    /// spawn — not re-read from the workspace, where rotation may
    /// already have installed the next namespace's token.
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
            tracker
                .paths
                .lock()
                .expect("tracker paths mutex")
                .insert(path);
            return;
        }
        debug!(
            target: "artel_fs::progress",
            %hash,
            total,
            path = %path.display(),
            "spawning transfer tracker"
        );
        let paths = Arc::new(Mutex::new(BTreeSet::from([path])));
        let cancel = doc_token.child_token();
        let task = tokio::spawn(track_task(
            blobs.clone(),
            events.clone(),
            Arc::clone(&paths),
            cancel.clone(),
            hash,
            total,
        ));
        self.trackers.insert(
            hash,
            Tracker {
                paths,
                cancel,
                task,
            },
        );
    }

    /// The blob behind `hash` is being applied (its `ContentReady`
    /// arrived, or an entry applied directly): stop the tracker and
    /// **await its death** so no `Transferring` can be emitted after
    /// the caller's `PeerWrote` (the events channel is FIFO — a
    /// tracker awaited here cannot enqueue behind it).
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
async fn track_task(
    blobs: iroh_blobs::BlobsProtocol,
    events: tokio::sync::mpsc::Sender<WorkspaceEvent>,
    paths: Arc<Mutex<BTreeSet<PathBuf>>>,
    cancel: CancellationToken,
    hash: Hash,
    total: u64,
) {
    let stream = match blobs.store().blobs().observe(hash).stream().await {
        Ok(s) => s,
        Err(err) => {
            debug!(target: "artel_fs::progress", %hash, %err, "observe failed; no progress events");
            return;
        }
    };
    tokio::pin!(stream);
    let mut throttle = ProgressThrottle::new(total);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            item = stream.next() => {
                let Some(bitfield) = item else { return };
                // An early bitfield may not know the blob's size yet
                // (0), and a bitfield's own size may disagree with the
                // entry's declared length; clamp so `received <= total`
                // always holds for consumers.
                let received = bitfield.total_bytes().min(total);
                if throttle.should_emit(received, Instant::now()) {
                    let snapshot: Vec<PathBuf> = {
                        let paths = paths.lock().expect("tracker paths mutex");
                        paths.iter().cloned().collect()
                    };
                    for path in snapshot {
                        emit_event(&events, WorkspaceEvent::Transferring { path, received, total });
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

    /// Spawn a registry + one tracked hash over an in-memory store
    /// that holds nothing — `observe` reports an empty bitfield, so
    /// the tracker emits its first observation (`received == 0`) and
    /// then idles until cancelled.
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

    #[tokio::test]
    async fn two_paths_one_hash_each_get_first_observation() {
        let blobs = test_blobs();
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let doc_token = CancellationToken::new();
        let mut registry = TransferTrackers::new();
        let hash = Hash::new(b"absent blob");

        // Both paths registered before the spawned task first polls
        // (current-thread runtime: spawn doesn't run until an await),
        // so the single first-observation emission covers both.
        registry.track(&blobs, &tx, &doc_token, hash, 1_000, "/ws/a.bin".into());
        registry.track(&blobs, &tx, &doc_token, hash, 1_000, "/ws/b.bin".into());
        assert_eq!(registry.trackers.len(), 1, "one tracker per hash");

        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..2 {
            let (path, received, total) = recv_transferring(&mut rx).await;
            assert_eq!(received, 0);
            assert_eq!(total, 1_000);
            seen.insert(path);
        }
        assert_eq!(
            seen,
            BTreeSet::from([PathBuf::from("/ws/a.bin"), PathBuf::from("/ws/b.bin")]),
        );

        registry.finish(hash).await;
        assert!(registry.trackers.is_empty());
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
        for _ in 0..2 {
            let _ = recv_transferring(&mut rx).await;
        }

        // A newer entry for a.bin under a different hash evicts the
        // stale path but keeps the tracker (b.bin still pending).
        let newer = Hash::new(b"newer content");
        registry.supersede(Path::new("/ws/a.bin"), Some(newer));
        let remaining = {
            let tracker = registry.trackers.get(&hash).expect("tracker kept");
            let paths = tracker.paths.lock().expect("tracker paths mutex");
            paths.clone()
        };
        assert_eq!(remaining, BTreeSet::from([PathBuf::from("/ws/b.bin")]));

        // Evicting the last path cancels and drops the tracker.
        registry.supersede(Path::new("/ws/b.bin"), None);
        assert!(registry.trackers.is_empty(), "empty tracker reaped");

        drop(tx);
        assert!(rx.recv().await.is_none(), "no events after supersede");
    }

    #[tokio::test]
    async fn same_hash_reuses_tracker_and_supersede_keeps_own_hash() {
        let blobs = test_blobs();
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let doc_token = CancellationToken::new();
        let mut registry = TransferTrackers::new();
        let hash = Hash::new(b"absent blob");

        registry.track(&blobs, &tx, &doc_token, hash, 1_000, "/ws/a.bin".into());
        // Re-announce of the same (path, hash) — e.g. the
        // ContentReady rescan re-feeding the entry — must not evict
        // the path from its own tracker.
        registry.track(&blobs, &tx, &doc_token, hash, 1_000, "/ws/a.bin".into());
        let paths = {
            let tracker = registry.trackers.get(&hash).expect("tracker present");
            let paths = tracker.paths.lock().expect("tracker paths mutex");
            paths.clone()
        };
        assert_eq!(paths, BTreeSet::from([PathBuf::from("/ws/a.bin")]));
    }
}
