//! In-memory [`super::SessionStore`].
//!
//! Used for unit tests and as a baseline against which the disk
//! implementation is checked. Has no durability story: anything you
//! put in disappears when the process exits.

#![allow(
    clippy::redundant_pub_crate,
    // The MemoryStore implementation holds a std::sync::Mutex across
    // tiny critical sections; clippy wants every guard scoped manually
    // to release as early as possible. Worth doing for hot async code,
    // not for a test-only HashMap-backed mock.
    clippy::significant_drop_tightening
)]

use std::collections::HashMap;
use std::io;
use std::sync::Mutex;

use artel_protocol::{PeerId, PeerInfo, SessionId, SessionMessage};
use async_trait::async_trait;

#[cfg(test)]
use super::SessionKind;
use super::{SessionRecord, SessionStore, StoredAttachment};

/// One session's record plus its attachments.
///
/// Bundling them under one map entry means the cascade in `delete` is
/// a single `HashMap::remove` — no separate attachment map to keep in
/// sync, and no possibility of an attachment outliving its session
/// even under concurrent calls. Mirrors how `FsLogStore` gets the
/// cascade for free from `remove_dir_all`.
#[derive(Clone, Debug)]
struct SessionEntry {
    record: SessionRecord,
    attachments: HashMap<String, Vec<u8>>,
}

impl SessionEntry {
    fn new(record: SessionRecord) -> Self {
        Self {
            record,
            attachments: HashMap::new(),
        }
    }
}

/// In-memory store. Cheap to construct; tests use one per scenario.
#[derive(Debug, Default)]
pub(crate) struct MemoryStore {
    inner: Mutex<HashMap<SessionId, SessionEntry>>,
}

impl MemoryStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SessionStore for MemoryStore {
    async fn create(&self, record: &SessionRecord) -> io::Result<()> {
        self.inner
            .lock()
            .expect("poisoned")
            .insert(record.id, SessionEntry::new(record.clone()));
        Ok(())
    }

    async fn append(&self, session: SessionId, message: &SessionMessage) -> io::Result<()> {
        let mut guard = self.inner.lock().expect("poisoned");
        let entry = guard
            .get_mut(&session)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown session"))?;
        entry.record.head = message.seq;
        entry.record.log.push(message.clone());
        Ok(())
    }

    async fn add_member(&self, session: SessionId, peer: &PeerInfo) -> io::Result<()> {
        let mut guard = self.inner.lock().expect("poisoned");
        let entry = guard
            .get_mut(&session)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown session"))?;
        entry.record.members.insert(peer.id);
        Ok(())
    }

    async fn remove_member(&self, session: SessionId, peer: PeerId) -> io::Result<()> {
        let mut guard = self.inner.lock().expect("poisoned");
        let entry = guard
            .get_mut(&session)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown session"))?;
        entry.record.members.remove(&peer);
        Ok(())
    }

    async fn delete(&self, session: SessionId) -> io::Result<()> {
        // Single map entry → cascade is one remove. Attachments live
        // *inside* the SessionEntry, so they vanish by ownership.
        self.inner.lock().expect("poisoned").remove(&session);
        Ok(())
    }

    async fn load_all(&self) -> io::Result<Vec<SessionRecord>> {
        Ok(self
            .inner
            .lock()
            .expect("poisoned")
            .values()
            .map(|entry| entry.record.clone())
            .collect())
    }

    async fn put_attachment(
        &self,
        session: SessionId,
        kind: &str,
        payload: &[u8],
    ) -> io::Result<bool> {
        let mut guard = self.inner.lock().expect("poisoned");
        let Some(entry) = guard.get_mut(&session) else {
            return Ok(false);
        };
        entry.attachments.insert(kind.to_owned(), payload.to_vec());
        Ok(true)
    }

    async fn list_attachments(
        &self,
        kind_filter: Option<&str>,
    ) -> io::Result<Vec<StoredAttachment>> {
        let guard = self.inner.lock().expect("poisoned");
        let mut out = Vec::new();
        for (session, entry) in guard.iter() {
            for (kind, payload) in &entry.attachments {
                if kind_filter.is_some_and(|f| f != kind) {
                    continue;
                }
                out.push(StoredAttachment {
                    session: *session,
                    kind: kind.clone(),
                    payload: payload.clone(),
                });
            }
        }
        Ok(out)
    }

    async fn delete_attachment(&self, session: SessionId, kind: &str) -> io::Result<()> {
        let mut guard = self.inner.lock().expect("poisoned");
        if let Some(entry) = guard.get_mut(&session) {
            entry.attachments.remove(kind);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use artel_protocol::{MessageKind, PeerInfo, Seq, SessionId, SessionMessage};
    use pretty_assertions::assert_eq;

    use super::*;

    fn record() -> SessionRecord {
        SessionRecord {
            id: SessionId::from_bytes([1; 16]),
            host: PeerId::from_bytes([1; 32]),
            members: HashSet::from([PeerId::from_bytes([1; 32])]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Local,
        }
    }

    fn message(seq: u64) -> SessionMessage {
        SessionMessage::new(
            Seq::new(seq),
            0,
            PeerInfo::new(PeerId::from_bytes([1; 32]), "alice"),
            MessageKind::Chat,
            "x",
            vec![],
        )
    }

    #[tokio::test]
    async fn create_then_load_round_trips() {
        let store = MemoryStore::new();
        let r = record();
        store.create(&r).await.unwrap();
        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded, vec![r]);
    }

    #[tokio::test]
    async fn append_advances_head_and_appends_log() {
        let store = MemoryStore::new();
        store.create(&record()).await.unwrap();

        store.append(record().id, &message(1)).await.unwrap();
        store.append(record().id, &message(2)).await.unwrap();

        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].head, Seq::new(2));
        assert_eq!(loaded[0].log.len(), 2);
    }

    #[tokio::test]
    async fn append_to_unknown_session_errors() {
        let store = MemoryStore::new();
        let err = store
            .append(SessionId::from_bytes([9; 16]), &message(1))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn add_then_remove_member() {
        let store = MemoryStore::new();
        store.create(&record()).await.unwrap();
        let bob = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
        store.add_member(record().id, &bob).await.unwrap();
        assert!(store.load_all().await.unwrap()[0].members.contains(&bob.id));
        store.remove_member(record().id, bob.id).await.unwrap();
        assert!(!store.load_all().await.unwrap()[0].members.contains(&bob.id));
    }

    #[tokio::test]
    async fn delete_removes_session() {
        let store = MemoryStore::new();
        store.create(&record()).await.unwrap();
        store.delete(record().id).await.unwrap();
        assert!(store.load_all().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_unknown_is_noop() {
        let store = MemoryStore::new();
        store.delete(SessionId::from_bytes([9; 16])).await.unwrap();
    }

    #[tokio::test]
    async fn session_kind_round_trips_through_create_load() {
        let store = MemoryStore::new();
        let mut r = record();
        r.kind = SessionKind::Remote;
        store.create(&r).await.unwrap();
        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].kind, SessionKind::Remote);
    }

    // ---- attachments ----

    const KIND_V1: &str = "artel-fs/workspace/v1";

    #[tokio::test]
    async fn put_then_list_attachment_round_trips() {
        let store = MemoryStore::new();
        store.create(&record()).await.unwrap();
        let ok = store
            .put_attachment(record().id, KIND_V1, b"opaque")
            .await
            .unwrap();
        assert!(ok);
        let listed = store.list_attachments(None).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].kind, KIND_V1);
        assert_eq!(listed[0].payload, b"opaque");
    }

    #[tokio::test]
    async fn put_attachment_overwrites_existing_at_same_kind() {
        let store = MemoryStore::new();
        store.create(&record()).await.unwrap();
        store
            .put_attachment(record().id, KIND_V1, b"first")
            .await
            .unwrap();
        store
            .put_attachment(record().id, KIND_V1, b"second")
            .await
            .unwrap();
        let listed = store.list_attachments(None).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].payload, b"second");
    }

    #[tokio::test]
    async fn put_attachment_for_unknown_session_returns_false() {
        let store = MemoryStore::new();
        let ok = store
            .put_attachment(SessionId::from_bytes([9; 16]), KIND_V1, b"x")
            .await
            .unwrap();
        assert!(!ok);
        assert!(store.list_attachments(None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_attachments_filters_by_kind_exact_match() {
        let store = MemoryStore::new();
        store.create(&record()).await.unwrap();
        store
            .put_attachment(record().id, KIND_V1, b"a")
            .await
            .unwrap();
        store
            .put_attachment(record().id, "other/kind/v1", b"b")
            .await
            .unwrap();

        let v1_only = store.list_attachments(Some(KIND_V1)).await.unwrap();
        assert_eq!(v1_only.len(), 1);
        assert_eq!(v1_only[0].payload, b"a");
    }

    #[tokio::test]
    async fn delete_attachment_is_idempotent() {
        let store = MemoryStore::new();
        store.create(&record()).await.unwrap();
        store
            .put_attachment(record().id, KIND_V1, b"x")
            .await
            .unwrap();
        store.delete_attachment(record().id, KIND_V1).await.unwrap();
        store.delete_attachment(record().id, KIND_V1).await.unwrap();
        assert!(store.list_attachments(None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_attachment_on_unknown_session_is_ok() {
        let store = MemoryStore::new();
        store
            .delete_attachment(SessionId::from_bytes([9; 16]), KIND_V1)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delete_session_cascades_attachments() {
        let store = MemoryStore::new();
        store.create(&record()).await.unwrap();
        store
            .put_attachment(record().id, KIND_V1, b"a")
            .await
            .unwrap();
        store
            .put_attachment(record().id, "other/kind/v1", b"b")
            .await
            .unwrap();

        store.delete(record().id).await.unwrap();
        assert!(store.list_attachments(None).await.unwrap().is_empty());
    }
}
