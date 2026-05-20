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
use super::{SessionRecord, SessionStore};

/// In-memory store. Cheap to construct; tests use one per scenario.
#[derive(Debug, Default)]
pub(crate) struct MemoryStore {
    inner: Mutex<HashMap<SessionId, SessionRecord>>,
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
            .insert(record.id, record.clone());
        Ok(())
    }

    async fn append(&self, session: SessionId, message: &SessionMessage) -> io::Result<()> {
        let mut guard = self.inner.lock().expect("poisoned");
        let record = guard
            .get_mut(&session)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown session"))?;
        record.head = message.seq;
        record.log.push(message.clone());
        Ok(())
    }

    async fn add_member(&self, session: SessionId, peer: &PeerInfo) -> io::Result<()> {
        let mut guard = self.inner.lock().expect("poisoned");
        let record = guard
            .get_mut(&session)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown session"))?;
        record.members.insert(peer.id);
        Ok(())
    }

    async fn remove_member(&self, session: SessionId, peer: PeerId) -> io::Result<()> {
        let mut guard = self.inner.lock().expect("poisoned");
        let record = guard
            .get_mut(&session)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown session"))?;
        record.members.remove(&peer);
        Ok(())
    }

    async fn delete(&self, session: SessionId) -> io::Result<()> {
        self.inner.lock().expect("poisoned").remove(&session);
        Ok(())
    }

    async fn load_all(&self) -> io::Result<Vec<SessionRecord>> {
        Ok(self
            .inner
            .lock()
            .expect("poisoned")
            .values()
            .cloned()
            .collect())
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
}
