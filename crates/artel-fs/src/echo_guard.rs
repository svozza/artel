//! Prevent the watcher↔applier loop from echoing peer writes back into
//! the doc.
//!
//! Without an echo guard:
//! - Peer P writes `foo.txt` → doc → applier on this node writes
//!   `foo.txt` to disk → watcher sees the disk change → publishes a
//!   *new* doc entry → other peers see a duplicate → 💥
//!
//! Three complementary defences:
//!
//! 1. **Pending set.** Right before the applier writes a peer-driven
//!    file, it inserts the path into a shared `HashSet`. The watcher
//!    consults the set and skips publishing while the path is in it.
//!    A short [`tokio`] timer removes the path once the inotify event
//!    has had time to fire ([`PENDING_RELEASE_GRACE`], defined here;
//!    the applier passes it to [`EchoGuard::release_after`]).
//!
//! 2. **Last-published hash.** Even after the pending entry has been
//!    cleared, watch events can race. We compare the hash of whatever
//!    content the watcher saw against the last content we wrote for
//!    that path; if they match it's an echo of *something we caused*
//!    and we skip. Hashes are blake3 — fast and collision-immune in
//!    practice. The guard takes hashes, not buffers: iroh's content
//!    hash IS the flat blake3 hash of the content (the bao tree root
//!    equals it — verified by `iroh_content_hash_is_flat_blake3` in
//!    the wire tests), so callers hand us `entry.content_hash()` /
//!    import-outcome hashes directly and no full file buffer ever
//!    needs to exist for the guard's sake.
//!
//! 3. **Last-removed set.** The delete-side analogue of (2). When the
//!    applier removes a file for a peer tombstone, the watcher sees
//!    the removal after the debounce window — too late for the
//!    pending set (grace 250 ms < debounce 300 ms), and there are no
//!    bytes to hash. Without a marker the watcher republishes the
//!    delete as a *new* tombstone with a newer timestamp, which syncs
//!    back out and ping-pongs between peers — and any legitimate
//!    write racing that storm is deleted by an echo tombstone that
//!    post-dates it. Entries persist until the path is re-created
//!    (locally or by a peer); they are not timer-released, because a
//!    removal event for a peer-deleted path is an echo no matter how
//!    late it fires.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use blake3::Hash;
use tokio::sync::Mutex;

/// Hash a file's contents by streaming — bounded memory at any size.
///
/// The publish path calls this *before* importing, so
/// [`EchoGuard::should_skip_local`] can suppress echoes without a
/// whole-file buffer ever existing. Runs on the blocking pool
/// (`spawn_blocking`): `update_reader` is synchronous I/O + hashing,
/// and a multi-GB file would otherwise stall the reactor. blake3's
/// own docs recommend `update_reader`/`update_mmap` over manual
/// chunked reads; the resulting hash equals iroh's content hash for
/// the same bytes (bao root == flat blake3).
///
/// # Errors
///
/// Propagates I/O errors from opening or reading the file — callers
/// treat them like the old `fs::read` failures (skip the event; a
/// later event retries).
pub async fn hash_file(path: &Path) -> io::Result<Hash> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)?;
        let mut hasher = blake3::Hasher::new();
        hasher.update_reader(io::BufReader::new(file))?;
        Ok(hasher.finalize())
    })
    .await
    .map_err(|join_err| io::Error::other(format!("hash_file task panicked: {join_err}")))?
}

/// How long the echo guard's pending entry survives after we apply
/// a peer write.
///
/// The watcher's debouncer is 300 ms; 250 ms is the largest grace
/// that still leaves the entry cleared by the time debounced events
/// arrive.
pub const PENDING_RELEASE_GRACE: Duration = Duration::from_millis(250);

/// Outcome of [`EchoGuard::mark_remote_delete`]: was the path newly
/// marked peer-deleted, or was it already marked?
///
/// `Duplicate` means this tombstone duplicates one already applied,
/// with no re-creation of the path observed since (both
/// [`EchoGuard::mark_remote_write`] and
/// [`EchoGuard::record_local_publish`] clear the marker) — so
/// anything on disk at that path now is an *unpublished local write*
/// that raced in, and the caller must NOT `remove_file` over it.
/// Duplicates are real: macOS `FSEvents` fans one unlink into
/// post-unlink `Modify` pairs that each take the watcher's
/// NotFound→tombstone fallback, publishing two tombstones (see the
/// straggler-tombstone case study in
/// `docs/diagnosing-flaky-tests.md`).
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteDeleteMark {
    /// Path newly marked — apply the tombstone's `remove_file`.
    Fresh,
    /// Already marked, no re-creation since — skip the disk removal.
    Duplicate,
}

/// Tracks pending peer writes, last-published bytes, and peer-driven
/// deletes per path.
///
/// Cheap to clone — all state is inside `Arc<Mutex<…>>`, so clones
/// share it. The watcher and applier each hold a clone of the
/// workspace's guard.
#[derive(Clone, Debug)]
pub struct EchoGuard {
    pending: Arc<Mutex<HashSet<PathBuf>>>,
    last_published: Arc<Mutex<HashMap<PathBuf, Hash>>>,
    peer_deleted: Arc<Mutex<HashSet<PathBuf>>>,
}

impl EchoGuard {
    /// Build a fresh guard with empty state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashSet::new())),
            last_published: Arc::new(Mutex::new(HashMap::new())),
            peer_deleted: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Record that a peer-driven write is about to land on disk for
    /// `path`. Inserts the path into the pending set and records the
    /// hash of the content we're about to write (the entry's iroh
    /// content hash — identical to the flat blake3 of the bytes). The
    /// path exists again, so any peer-delete marker is cleared.
    ///
    /// Call as close to the watcher-visible mutation (the rename) as
    /// possible: while pending, EVERY local event for the path is
    /// suppressed regardless of content, so a wide mark-to-write gap
    /// swallows genuine local edits (see `apply_entry_streaming`).
    pub async fn mark_remote_write(&self, path: &Path, hash: Hash) {
        let owned = path.to_path_buf();
        self.pending.lock().await.insert(owned.clone());
        self.peer_deleted.lock().await.remove(&owned);
        self.last_published.lock().await.insert(owned, hash);
    }

    /// Unwind [`Self::mark_remote_write`] when the write it announced
    /// never happened (the streaming apply's rename failed). Removes
    /// the pending entry and the recorded hash so future local events
    /// for the path are neither suppressed nor hash-deduped against
    /// content that never reached disk.
    ///
    /// Deliberately does NOT restore a previously-cleared peer-delete
    /// marker: the marker's contract is "cleared on re-creation", and
    /// erring toward publishing (worst case, one redundant tombstone)
    /// beats erring toward suppression (a swallowed genuine delete).
    pub async fn unmark_remote_write(&self, path: &Path) {
        self.pending.lock().await.remove(path);
        self.last_published.lock().await.remove(path);
    }

    /// Record that a peer-driven delete (tombstone) is about to be
    /// applied to disk for `path`.
    ///
    /// Marks the path so [`Self::should_skip_removal`] suppresses the
    /// watcher's resulting removal event, and drops the
    /// `last_published` hash — the file is gone, and a stale hash
    /// would swallow a later re-creation with identical bytes as an
    /// echo (see the module docs, defences 2 and 3).
    ///
    /// The returned [`RemoteDeleteMark`] tells the caller whether the
    /// path was newly marked ([`RemoteDeleteMark::Fresh`] — proceed
    /// with the removal) or already marked peer-deleted
    /// ([`RemoteDeleteMark::Duplicate`] — a duplicate tombstone; do
    /// not touch disk, see its docs for why).
    pub async fn mark_remote_delete(&self, path: &Path) -> RemoteDeleteMark {
        self.last_published.lock().await.remove(path);
        if self.peer_deleted.lock().await.insert(path.to_path_buf()) {
            RemoteDeleteMark::Fresh
        } else {
            RemoteDeleteMark::Duplicate
        }
    }

    /// Should the watcher skip a local *removal* event for `path`?
    /// `true` while the path is marked peer-deleted — i.e. the
    /// removal is the filesystem echo of a tombstone this node's
    /// applier laid down, not a user delete.
    ///
    /// Deliberately does NOT consume the marker: one unlink can fan
    /// out into several debounced watcher events (macOS `FSEvents`
    /// reports post-unlink `Modify(...)` pairs), and every one of
    /// them is an echo. The marker is cleared only when the path
    /// exists again ([`Self::mark_remote_write`] /
    /// [`Self::record_local_publish`]).
    pub async fn should_skip_removal(&self, path: &Path) -> bool {
        self.peer_deleted.lock().await.contains(path)
    }

    /// Schedule removal of `path` from the pending set after `grace`.
    /// Spawns a tokio task; safe to call repeatedly.
    pub fn release_after(&self, path: PathBuf, grace: Duration) {
        let pending = Arc::clone(&self.pending);
        tokio::spawn(async move {
            tokio::time::sleep(grace).await;
            pending.lock().await.remove(&path);
        });
    }

    /// Should the watcher skip a local change for `path` whose current
    /// content hashes to `hash`? `true` if the change is a known echo.
    ///
    /// Callers on the publish path obtain `hash` by streaming the file
    /// (see [`hash_file`]) — bounded memory at any size, and for small
    /// files the same cost as the old hash-the-buffer shape.
    pub async fn should_skip_local(&self, path: &Path, hash: Hash) -> bool {
        if self.pending.lock().await.contains(path) {
            return true;
        }
        self.last_published
            .lock()
            .await
            .get(path)
            .is_some_and(|last| *last == hash)
    }

    /// Note that we just published content hashing to `hash` for
    /// `path` (the import outcome's hash — identical to the flat
    /// blake3). Future watcher events with the same hash will be
    /// skipped via [`Self::should_skip_local`]. The path exists again,
    /// so any peer-delete marker is cleared.
    pub async fn record_local_publish(&self, path: &Path, hash: Hash) {
        self.peer_deleted.lock().await.remove(path);
        self.last_published
            .lock()
            .await
            .insert(path.to_path_buf(), hash);
    }

    /// Drop the `last_published` hash for `path`. Call when the file
    /// is deleted — tombstone applied from a peer, or local removal
    /// observed by the watcher.
    ///
    /// Without this, the hash outlives the file: a later re-creation
    /// with the *same bytes* hashes equal to the stale entry and
    /// [`Self::should_skip_local`] swallows the publish forever,
    /// leaving the doc's latest entry a tombstone while disk state
    /// diverges. (Found via the rw-redelivery n0 tests, whose
    /// per-peer probe files are deleted and re-created with identical
    /// bytes per grant.)
    ///
    /// The pending set is deliberately left alone: entries there
    /// expire on their own timer ([`release_after`]), and evicting
    /// one early from a removal event could unsuppress the echo of
    /// an applier write racing the same debounce window.
    ///
    /// [`release_after`]: Self::release_after
    pub async fn forget(&self, path: &Path) {
        self.last_published.lock().await.remove(path);
    }

    /// Drop **every** `last_published` hash. Call when the live doc is
    /// swapped out from under the guard (namespace rotation's
    /// `reimport_namespace`): the hashes record what we last published
    /// into the *old* namespace, and a path that was published there
    /// but is absent from the rotated namespace (e.g. a write landing
    /// in the survivor's rotation lag, after the host's snapshot) must
    /// be republishable — with a stale hash surviving the swap, every
    /// re-write of the same bytes is swallowed as an echo forever.
    ///
    /// Clearing publishes nothing by itself — a publish still needs a
    /// real filesystem event — so this cannot resurrect a
    /// host-tombstoned path the way a survivor-side catch-up scan
    /// could. `pending` and `peer_deleted` are left intact: an
    /// applier write in its grace window still echoes after the swap,
    /// and a peer-deleted marker is only cleared by re-creation
    /// regardless of which namespace laid the tombstone down.
    pub async fn forget_all_published(&self) {
        self.last_published.lock().await.clear();
    }
}

impl Default for EchoGuard {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use super::*;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    /// Shorthand: the hash a caller would derive for these bytes
    /// (streamed pre-hash on publish, `entry.content_hash()` on
    /// apply — both equal the flat blake3).
    fn h(bytes: &[u8]) -> Hash {
        blake3::hash(bytes)
    }

    #[tokio::test]
    async fn skips_local_during_pending_window() {
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");
        let hash = h(b"hello");

        guard.mark_remote_write(&path, hash).await;
        assert!(
            guard.should_skip_local(&path, hash).await,
            "should skip local while remote write is in flight",
        );
    }

    #[tokio::test]
    async fn does_not_skip_unrelated_path() {
        let guard = EchoGuard::new();
        guard.mark_remote_write(&p("/w/a.txt"), h(b"x")).await;
        assert!(!guard.should_skip_local(&p("/w/b.txt"), h(b"y")).await);
    }

    #[tokio::test]
    async fn release_clears_pending() {
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");
        guard.mark_remote_write(&path, h(b"x")).await;
        guard.release_after(path.clone(), Duration::from_millis(10));
        // Wall-clock margin for non-paused tests.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(
            !guard.should_skip_local(&path, h(b"different")).await,
            "after release, unrelated content should not be skipped",
        );
    }

    #[tokio::test]
    async fn skips_when_hash_matches_last_published() {
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");
        let hash = h(b"snapshot");
        guard.record_local_publish(&path, hash).await;
        assert!(
            guard.should_skip_local(&path, hash).await,
            "writing the same content we just published should be deduped",
        );
    }

    #[tokio::test]
    async fn does_not_skip_when_hash_differs_from_last_published() {
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");
        guard.record_local_publish(&path, h(b"v1")).await;
        assert!(
            !guard.should_skip_local(&path, h(b"v2")).await,
            "different content should not be deduped",
        );
    }

    #[tokio::test]
    async fn forget_clears_last_published() {
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");
        let hash = h(b"same bytes before and after delete");
        guard.record_local_publish(&path, hash).await;

        guard.forget(&path).await;
        assert!(
            !guard.should_skip_local(&path, hash).await,
            "after forget, re-creating identical bytes must publish, not be deduped",
        );
    }

    #[tokio::test]
    async fn forget_leaves_pending_intact() {
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");
        guard.mark_remote_write(&path, h(b"v1")).await;

        guard.forget(&path).await;
        assert!(
            guard.should_skip_local(&path, h(b"v2")).await,
            "forget must not evict a pending remote write still inside its grace window",
        );
    }

    #[tokio::test]
    async fn forget_unknown_path_is_noop() {
        let guard = EchoGuard::new();
        guard.record_local_publish(&p("/w/a.txt"), h(b"x")).await;

        guard.forget(&p("/w/never-seen.txt")).await;
        assert!(
            guard.should_skip_local(&p("/w/a.txt"), h(b"x")).await,
            "forgetting an unknown path must not disturb other entries",
        );
    }

    #[tokio::test]
    async fn unmark_clears_pending_and_hash() {
        // The streaming apply's rename-failure unwind: after an
        // unmark, local events for the path must be neither
        // pending-suppressed nor hash-deduped against content that
        // never reached disk.
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");
        let hash = h(b"never-written");
        guard.mark_remote_write(&path, hash).await;

        guard.unmark_remote_write(&path).await;
        assert!(
            !guard.should_skip_local(&path, hash).await,
            "after unmark, even the marked hash must publish",
        );
        assert!(
            !guard.should_skip_local(&path, h(b"other")).await,
            "after unmark, the pending suppression must be gone",
        );
    }

    #[tokio::test]
    async fn unmark_does_not_restore_peer_delete_marker() {
        // mark_remote_write cleared the peer-delete marker; the
        // unwind deliberately does not restore it — err toward
        // publishing (see method docs).
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");
        let _ = guard.mark_remote_delete(&path).await;
        guard.mark_remote_write(&path, h(b"v1")).await;

        guard.unmark_remote_write(&path).await;
        assert!(
            !guard.should_skip_removal(&path).await,
            "unmark must not re-arm the peer-delete marker",
        );
    }

    #[tokio::test]
    async fn clones_share_state() {
        let original = EchoGuard::new();
        original.mark_remote_write(&p("/w/x"), h(b"v")).await;
        let _ = original.mark_remote_delete(&p("/w/y")).await;

        let clone = original.clone();
        assert!(
            clone.should_skip_local(&p("/w/x"), h(b"v")).await,
            "a clone must observe the original's write state",
        );
        assert!(
            clone.should_skip_removal(&p("/w/y")).await,
            "a clone must observe the original's delete state",
        );
    }

    #[tokio::test]
    async fn hash_file_streams_and_matches_flat_blake3() {
        // `hash_file` (streamed, blocking-pool) must equal
        // `blake3::hash` over the same bytes — the guard compares the
        // two shapes against each other (publish-side pre-hash vs
        // import-outcome / entry content hash).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.bin");
        // Not a round multiple of the reader's internal chunk size,
        // to catch an off-by-one in the tail read.
        let bytes: Vec<u8> = (0..123_457u32).map(|i| (i % 251) as u8).collect();
        tokio::fs::write(&path, &bytes).await.unwrap();
        assert_eq!(hash_file(&path).await.unwrap(), blake3::hash(&bytes));
    }

    #[tokio::test]
    async fn hash_file_missing_file_is_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = hash_file(&dir.path().join("nope.bin")).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn peer_delete_suppresses_removal_until_recreated() {
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");

        assert!(
            !guard.should_skip_removal(&path).await,
            "an unmarked path's removal is a user delete and must publish",
        );

        let _ = guard.mark_remote_delete(&path).await;
        assert!(
            guard.should_skip_removal(&path).await,
            "removal right after a peer tombstone is an echo",
        );
        assert!(
            guard.should_skip_removal(&path).await,
            "the marker must survive repeated events (macOS fans one unlink into several)",
        );

        guard.mark_remote_write(&path, h(b"recreated")).await;
        assert!(
            !guard.should_skip_removal(&path).await,
            "a peer re-creation clears the marker; the next removal is meaningful again",
        );
    }

    #[tokio::test]
    async fn forget_all_clears_every_last_published_hash() {
        let guard = EchoGuard::new();
        guard
            .record_local_publish(&p("/w/a.txt"), h(b"alpha"))
            .await;
        guard.record_local_publish(&p("/w/b.txt"), h(b"beta")).await;

        guard.forget_all_published().await;
        assert!(
            !guard.should_skip_local(&p("/w/a.txt"), h(b"alpha")).await,
            "after a namespace swap, re-writing identical bytes must publish",
        );
        assert!(
            !guard.should_skip_local(&p("/w/b.txt"), h(b"beta")).await,
            "the clear must cover every path, not just one",
        );
    }

    #[tokio::test]
    async fn forget_all_leaves_pending_and_peer_deleted_intact() {
        let guard = EchoGuard::new();
        guard
            .mark_remote_write(&p("/w/pending.txt"), h(b"v1"))
            .await;
        let _ = guard.mark_remote_delete(&p("/w/deleted.txt")).await;

        guard.forget_all_published().await;
        assert!(
            guard
                .should_skip_local(&p("/w/pending.txt"), h(b"v2"))
                .await,
            "an applier write inside its grace window still echoes after the swap",
        );
        assert!(
            guard.should_skip_removal(&p("/w/deleted.txt")).await,
            "a peer-delete marker is cleared only by re-creation, not by a namespace swap",
        );
    }

    #[tokio::test]
    async fn mark_remote_delete_reports_duplicates() {
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");

        assert_eq!(
            guard.mark_remote_delete(&path).await,
            RemoteDeleteMark::Fresh,
            "first tombstone for a path is newly marked",
        );
        assert_eq!(
            guard.mark_remote_delete(&path).await,
            RemoteDeleteMark::Duplicate,
            "second tombstone with no intervening re-creation is a duplicate",
        );

        guard.mark_remote_write(&path, h(b"recreated")).await;
        assert_eq!(
            guard.mark_remote_delete(&path).await,
            RemoteDeleteMark::Fresh,
            "a peer re-creation clears the marker; the next tombstone is fresh again",
        );

        guard.record_local_publish(&path, h(b"local")).await;
        assert_eq!(
            guard.mark_remote_delete(&path).await,
            RemoteDeleteMark::Fresh,
            "a local publish clears the marker; the next tombstone is fresh again",
        );
    }

    /// The duplicate-report path must still suppress removal echoes:
    /// [`RemoteDeleteMark::Duplicate`] means the applier skips its
    /// `remove_file`, but the marker stays armed for the watcher
    /// (defence 3 is not consumed — no ping-pong reintroduced).
    #[tokio::test]
    async fn duplicate_delete_keeps_removal_suppressed() {
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");

        let _ = guard.mark_remote_delete(&path).await;
        let _ = guard.mark_remote_delete(&path).await;
        assert!(
            guard.should_skip_removal(&path).await,
            "marker must survive a duplicate tombstone report",
        );
    }

    #[tokio::test]
    async fn peer_delete_clears_last_published() {
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");
        let hash = h(b"same bytes before and after delete");
        guard.record_local_publish(&path, hash).await;

        let _ = guard.mark_remote_delete(&path).await;
        assert!(
            !guard.should_skip_local(&path, hash).await,
            "after a peer delete, re-creating identical bytes must publish, not be deduped",
        );
    }
}
