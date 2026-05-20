//! Prevent the watcher↔applier loop from echoing peer writes back into
//! the doc.
//!
//! Without an echo guard:
//! - Peer P writes `foo.txt` → doc → applier on this node writes
//!   `foo.txt` to disk → watcher sees the disk change → publishes a
//!   *new* doc entry → other peers see a duplicate → 💥
//!
//! Two complementary defences:
//!
//! 1. **Pending set.** Right before the applier writes a peer-driven
//!    file, it inserts the path into a shared `HashSet`. The watcher
//!    consults the set and skips publishing while the path is in it.
//!    A short [`tokio`] timer removes the path once the inotify event
//!    has had time to fire. (`PENDING_RELEASE_GRACE` lives at the
//!    applier level, not here — this module just stores the set.)
//!
//! 2. **Last-published hash.** Even after the pending entry has been
//!    cleared, watch events can race. We hash whatever bytes the
//!    watcher saw and compare against the last bytes we wrote for
//!    that path; if they match it's an echo of *something we caused*
//!    and we skip. Hashes are blake3 — fast and collision-immune in
//!    practice.
//!
//! Ported verbatim from harness `session/workspace/echo_guard.rs`,
//! shape and all. The only deviation is import paths.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use blake3::Hash;
use tokio::sync::Mutex;

/// Tracks pending peer writes and last-published bytes per path.
///
/// Cheap to clone — both maps are inside `Arc<Mutex<…>>`.
#[derive(Clone, Debug)]
pub struct EchoGuard {
    pending: Arc<Mutex<HashSet<PathBuf>>>,
    last_published: Arc<Mutex<HashMap<PathBuf, Hash>>>,
}

impl EchoGuard {
    /// Build a fresh guard with empty state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashSet::new())),
            last_published: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Build a guard that shares the supplied state. Used so the
    /// watcher and applier each carry a cheap handle to the same
    /// pending-set + hash map.
    #[must_use]
    pub const fn shared(
        pending: Arc<Mutex<HashSet<PathBuf>>>,
        last_published: Arc<Mutex<HashMap<PathBuf, Hash>>>,
    ) -> Self {
        Self {
            pending,
            last_published,
        }
    }

    /// Record that a peer-driven write is about to land on disk for
    /// `path`. Inserts the path into the pending set and records the
    /// hash of the bytes we're about to write.
    pub async fn mark_remote_write(&self, path: &Path, bytes: &[u8]) {
        let owned = path.to_path_buf();
        let hash = blake3::hash(bytes);
        self.pending.lock().await.insert(owned.clone());
        self.last_published.lock().await.insert(owned, hash);
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

    /// Should the watcher skip a local change for `path` with these
    /// `bytes`? `true` if the change is a known echo.
    pub async fn should_skip_local(&self, path: &Path, bytes: &[u8]) -> bool {
        if self.pending.lock().await.contains(path) {
            return true;
        }
        let hash = blake3::hash(bytes);
        self.last_published
            .lock()
            .await
            .get(path)
            .is_some_and(|last| *last == hash)
    }

    /// Note that we just published `bytes` for `path`. Future watcher
    /// events with the same hash will be skipped via
    /// [`Self::should_skip_local`].
    pub async fn record_local_publish(&self, path: &Path, bytes: &[u8]) {
        let hash = blake3::hash(bytes);
        self.last_published
            .lock()
            .await
            .insert(path.to_path_buf(), hash);
    }

    /// Clone of the pending set's `Arc<Mutex<…>>`. Hands out shared
    /// ownership to the watcher / applier so they can call
    /// [`Self::shared`] later.
    #[must_use]
    pub fn pending_handle(&self) -> Arc<Mutex<HashSet<PathBuf>>> {
        Arc::clone(&self.pending)
    }

    /// Same as [`Self::pending_handle`] for the last-published map.
    #[must_use]
    pub fn last_published_handle(&self) -> Arc<Mutex<HashMap<PathBuf, Hash>>> {
        Arc::clone(&self.last_published)
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

    #[tokio::test]
    async fn skips_local_during_pending_window() {
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");
        let bytes = b"hello";

        guard.mark_remote_write(&path, bytes).await;
        assert!(
            guard.should_skip_local(&path, bytes).await,
            "should skip local while remote write is in flight",
        );
    }

    #[tokio::test]
    async fn does_not_skip_unrelated_path() {
        let guard = EchoGuard::new();
        guard.mark_remote_write(&p("/w/a.txt"), b"x").await;
        assert!(!guard.should_skip_local(&p("/w/b.txt"), b"y").await);
    }

    #[tokio::test]
    async fn release_clears_pending() {
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");
        guard.mark_remote_write(&path, b"x").await;
        guard.release_after(path.clone(), Duration::from_millis(10));
        // Wall-clock margin for non-paused tests.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(
            !guard.should_skip_local(&path, b"different").await,
            "after release, unrelated bytes should not be skipped",
        );
    }

    #[tokio::test]
    async fn skips_when_bytes_match_last_published() {
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");
        let bytes = b"snapshot";
        guard.record_local_publish(&path, bytes).await;
        assert!(
            guard.should_skip_local(&path, bytes).await,
            "writing the same bytes we just published should be deduped",
        );
    }

    #[tokio::test]
    async fn does_not_skip_when_bytes_differ_from_last_published() {
        let guard = EchoGuard::new();
        let path = p("/w/a.txt");
        guard.record_local_publish(&path, b"v1").await;
        assert!(
            !guard.should_skip_local(&path, b"v2").await,
            "different bytes should not be deduped",
        );
    }

    #[tokio::test]
    async fn shared_handles_share_state() {
        let original = EchoGuard::new();
        original.mark_remote_write(&p("/w/x"), b"v").await;

        let shared = EchoGuard::shared(original.pending_handle(), original.last_published_handle());
        assert!(
            shared.should_skip_local(&p("/w/x"), b"v").await,
            "shared handle must observe the original's state",
        );
    }
}
