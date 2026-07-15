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

use artel_protocol::{PeerId, PeerInfo, SessionId, SessionMessage, TicketEntry};
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
    /// Epoch floors (session-ID-reuse replay finding), tracked in a map
    /// separate from `inner` so `delete` — which only removes `inner`'s
    /// entry — cannot reset a floor. Mirrors `FsLogStore`'s
    /// `EPOCH_FLOORS_DIR` sibling-directory design.
    epoch_floors: Mutex<HashMap<SessionId, u64>>,
}

impl MemoryStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Raise `session`'s epoch floor to `raise_to` if it isn't already
    /// at least that high. Monotonic, matching `FsLogStore`'s
    /// `read_and_raise_epoch_floor`.
    fn raise_epoch_floor(&self, session: SessionId, raise_to: u64) {
        let mut guard = self.epoch_floors.lock().expect("poisoned");
        let floor = guard.entry(session).or_insert(0);
        if raise_to > *floor {
            *floor = raise_to;
        }
    }
}

#[async_trait]
impl SessionStore for MemoryStore {
    async fn create(&self, record: &SessionRecord) -> io::Result<()> {
        self.raise_epoch_floor(record.id, record.host_epoch + 1);
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
        // Monotonic head, matching `FsLogStore::append`: a Remote mirror
        // can append a lower seq after a higher one (gossip frames apply
        // in arbitrary lock order), and the persisted head must not
        // regress. Keeping the two stores' semantics identical means the
        // in-memory test baseline catches a head-regression the disk
        // store would also suffer.
        entry.record.head = entry.record.head.max(message.seq);
        entry.record.log.push(message.clone());
        Ok(())
    }

    async fn bump_host_epoch(&self, session: SessionId, epoch: u64) -> io::Result<()> {
        self.raise_epoch_floor(session, epoch + 1);
        let mut guard = self.inner.lock().expect("poisoned");
        if let Some(entry) = guard.get_mut(&session) {
            entry.record.host_epoch = epoch;
        }
        Ok(())
    }

    async fn epoch_floor(&self, session: SessionId) -> io::Result<u64> {
        Ok(self
            .epoch_floors
            .lock()
            .expect("poisoned")
            .get(&session)
            .copied()
            .unwrap_or(0))
    }

    async fn put_tickets(&self, session: SessionId, tickets: &[TicketEntry]) -> io::Result<()> {
        let mut guard = self.inner.lock().expect("poisoned");
        let entry = guard
            .get_mut(&session)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown session"))?;
        entry.record.tickets = tickets.to_vec();
        Ok(())
    }

    async fn put_workspace_ticket(&self, session: SessionId, envelope: &[u8]) -> io::Result<()> {
        // Same cap the disk impl enforces (and the wire enforces on
        // delivery) so the test baseline can't accept an envelope the
        // production store would reject — see `FsLogStore`'s
        // `write_workspace_ticket`.
        if envelope.len() > artel_protocol::upgrade::WORKSPACE_TICKET_ENVELOPE_MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "workspace ticket envelope too large",
            ));
        }
        let mut guard = self.inner.lock().expect("poisoned");
        let entry = guard
            .get_mut(&session)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown session"))?;
        entry.record.workspace_ticket = Some(envelope.to_vec());
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
            host_epoch: 0,
            tickets: Vec::new(),
            workspace_ticket: None,
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
            artel_protocol::message::SIGNATURE_UNSIGNED,
            artel_protocol::message::SIGNATURE_UNSIGNED,
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

    // ---- ticket ledger (revocation slice) ----

    fn ticket_entry(id_byte: u8) -> artel_protocol::TicketEntry {
        artel_protocol::TicketEntry {
            ticket_id: artel_protocol::TicketId::from_bytes([id_byte; 16]),
            granted_cap: artel_protocol::Capability::Read,
            expiry_ms: 0,
            issued_at_ms: 1_700_000_000_000,
            status: artel_protocol::TicketStatus::Active,
            used_by: Vec::new(),
        }
    }

    #[tokio::test]
    async fn put_tickets_then_load_round_trips() {
        let store = MemoryStore::new();
        store.create(&record()).await.unwrap();
        let ledger = vec![ticket_entry(1), ticket_entry(2)];
        store.put_tickets(record().id, &ledger).await.unwrap();
        assert_eq!(store.load_all().await.unwrap()[0].tickets, ledger);
    }

    #[tokio::test]
    async fn put_tickets_for_unknown_session_errors_not_found() {
        let store = MemoryStore::new();
        let err = store
            .put_tickets(SessionId::from_bytes([9; 16]), &[ticket_entry(1)])
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn put_tickets_rewrite_replaces_previous_ledger() {
        let store = MemoryStore::new();
        store.create(&record()).await.unwrap();
        store
            .put_tickets(record().id, &[ticket_entry(1)])
            .await
            .unwrap();
        let after = vec![ticket_entry(2)];
        store.put_tickets(record().id, &after).await.unwrap();
        assert_eq!(store.load_all().await.unwrap()[0].tickets, after);
    }

    #[tokio::test]
    async fn delete_cascades_tickets_with_session() {
        let store = MemoryStore::new();
        store.create(&record()).await.unwrap();
        store
            .put_tickets(record().id, &[ticket_entry(1)])
            .await
            .unwrap();
        store.delete(record().id).await.unwrap();
        assert!(store.load_all().await.unwrap().is_empty());
    }

    // ---- workspace ticket envelope (revoked-lurker fix) ----

    #[tokio::test]
    async fn put_workspace_ticket_then_load_round_trips() {
        let store = MemoryStore::new();
        store.create(&record()).await.unwrap();
        store
            .put_workspace_ticket(record().id, &[0xab; 64])
            .await
            .unwrap();
        assert_eq!(
            store.load_all().await.unwrap()[0].workspace_ticket,
            Some(vec![0xab; 64]),
        );
    }

    #[tokio::test]
    async fn workspace_ticket_defaults_to_none() {
        let store = MemoryStore::new();
        store.create(&record()).await.unwrap();
        assert_eq!(store.load_all().await.unwrap()[0].workspace_ticket, None);
    }

    #[tokio::test]
    async fn put_workspace_ticket_rewrite_replaces_previous() {
        let store = MemoryStore::new();
        store.create(&record()).await.unwrap();
        store
            .put_workspace_ticket(record().id, &[1, 2, 3])
            .await
            .unwrap();
        store
            .put_workspace_ticket(record().id, &[9, 9])
            .await
            .unwrap();
        assert_eq!(
            store.load_all().await.unwrap()[0].workspace_ticket,
            Some(vec![9, 9]),
        );
    }

    #[tokio::test]
    async fn put_workspace_ticket_for_unknown_session_errors_not_found() {
        let store = MemoryStore::new();
        let err = store
            .put_workspace_ticket(SessionId::from_bytes([9; 16]), &[1])
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn delete_cascades_workspace_ticket_with_session() {
        let store = MemoryStore::new();
        store.create(&record()).await.unwrap();
        store.put_workspace_ticket(record().id, &[1]).await.unwrap();
        store.delete(record().id).await.unwrap();
        assert!(store.load_all().await.unwrap().is_empty());
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
