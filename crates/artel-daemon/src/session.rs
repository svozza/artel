//! In-memory session registry and RPC handlers.
//!
//! A [`Registry`] owns every active session by ID. Each [`Session`]
//! holds an ordered message log (host-sequenced), a member set, and a
//! `broadcast::Sender<Event>` for live subscribers. RPC handlers are
//! methods on `Registry`; they take peer info as an argument so the
//! transport layer can supply it (peer identity comes from the IPC
//! handshake rather than the message).
//!
//! [`JoinTicket`]s emitted here use the `artel:` text format defined
//! in [`artel_protocol::ticket`]. Phase 2c will extend the payload
//! with iroh `NodeAddr` and topic info; today the ticket carries the
//! session id and the host daemon's peer id, which is enough for a
//! local-only daemon to route a join request and rejects all
//! pre-2b ticket forms.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use artel_protocol::ticket::{self, SessionTicket};
use artel_protocol::{
    Event, JoinTicket, MessageKind, PeerId, PeerInfo, Seq, SessionId, SessionMessage,
    SessionSummary,
};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, broadcast};

use crate::store::{DynStore, SessionRecord};

/// Capacity of the per-session broadcast channel.
///
/// Slow subscribers that lag by more than this lose old events; the
/// transport surfaces that to the client as a message gap (which the
/// client can recover from with a `Subscribe { since }`). This is the
/// right shape — we do not want to back-pressure publishers because of
/// one slow subscriber.
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Errors the registry may return from RPC handlers.
#[derive(Debug, Error)]
pub enum SessionError {
    /// The referenced session does not exist.
    #[error("unknown session: {0}")]
    UnknownSession(SessionId),

    /// The peer is not a member of the session.
    #[error("not a member of session: {0}")]
    NotMember(SessionId),

    /// The peer is already a member of the session.
    #[error("already joined session: {0}")]
    AlreadyJoined(SessionId),

    /// Join ticket malformed or revoked.
    #[error("invalid join ticket")]
    InvalidTicket,

    /// Backing storage failed. The in-memory state was not changed.
    #[error("storage: {0}")]
    Storage(#[source] std::io::Error),
}

// io::Error doesn't impl PartialEq, so we hand-roll one for the
// Storage-free variants tests rely on.
impl PartialEq for SessionError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::UnknownSession(a), Self::UnknownSession(b))
            | (Self::NotMember(a), Self::NotMember(b))
            | (Self::AlreadyJoined(a), Self::AlreadyJoined(b)) => a == b,
            (Self::InvalidTicket, Self::InvalidTicket) => true,
            (Self::Storage(a), Self::Storage(b)) => a.kind() == b.kind(),
            _ => false,
        }
    }
}
impl Eq for SessionError {}

/// Outcome of a successful `subscribe`: a snapshot of the log to
/// replay, plus a live event receiver for everything that follows.
#[derive(Debug)]
pub struct Subscription {
    /// Log entries with `seq > since` at the moment of subscription.
    /// Empty if the caller already had everything.
    pub replay: Vec<SessionMessage>,
    /// Live event stream. The first event is whatever happens *after*
    /// the last entry in `replay`.
    pub events: broadcast::Receiver<Event>,
}

/// One active session.
#[derive(Debug)]
pub struct Session {
    id: SessionId,
    host: PeerId,
    members: HashSet<PeerId>,
    log: Vec<SessionMessage>,
    head: Seq,
    events_tx: broadcast::Sender<Event>,
}

impl Session {
    fn new(id: SessionId, host: &PeerInfo) -> Self {
        let mut members = HashSet::new();
        members.insert(host.id);
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            id,
            host: host.id,
            members,
            log: Vec::new(),
            head: Seq::ZERO,
            events_tx,
        }
    }

    /// Hydrate from a persisted [`SessionRecord`].
    fn from_record(record: SessionRecord) -> Self {
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            id: record.id,
            host: record.host,
            members: record.members,
            log: record.log,
            head: record.head,
            events_tx,
        }
    }

    /// Take a snapshot suitable for [`crate::store::SessionStore::create`].
    fn record(&self) -> SessionRecord {
        SessionRecord {
            id: self.id,
            host: self.host,
            members: self.members.clone(),
            head: self.head,
            log: self.log.clone(),
        }
    }

    /// Summary suitable for [`Registry::list`].
    fn summary(&self, daemon_peer_id: PeerId) -> SessionSummary {
        SessionSummary {
            id: self.id,
            is_host: self.host == daemon_peer_id,
            peer_count: u32::try_from(self.members.len()).unwrap_or(u32::MAX),
            last_seq: if self.head == Seq::ZERO {
                None
            } else {
                Some(self.head)
            },
        }
    }
}

/// In-memory session registry, backed by a [`crate::store::SessionStore`]
/// for durability.
#[derive(Debug)]
pub struct Registry {
    daemon_peer_id: PeerId,
    sessions: RwLock<HashMap<SessionId, Arc<Mutex<Session>>>>,
    store: DynStore,
}

impl Registry {
    /// Create a registry backed by `store`. The store is consulted only
    /// for mutations; in-memory state holds the live runtime view
    /// (broadcast channels, subscribers).
    ///
    /// Used by unit tests; production code goes through
    /// [`Registry::load`] which also rehydrates from the store.
    #[cfg(test)]
    pub(crate) fn new(daemon_peer_id: PeerId, store: DynStore) -> Self {
        Self {
            daemon_peer_id,
            sessions: RwLock::new(HashMap::new()),
            store,
        }
    }

    /// Build a registry whose initial state is the records the store
    /// returned from `load_all`. Called once at daemon startup.
    pub(crate) async fn load(daemon_peer_id: PeerId, store: DynStore) -> std::io::Result<Self> {
        let records = store.load_all().await?;
        let mut sessions = HashMap::with_capacity(records.len());
        for record in records {
            let id = record.id;
            sessions.insert(id, Arc::new(Mutex::new(Session::from_record(record))));
        }
        Ok(Self {
            daemon_peer_id,
            sessions: RwLock::new(sessions),
            store,
        })
    }

    /// The daemon's own peer id, returned in the `Hello` response.
    #[must_use]
    pub const fn daemon_peer_id(&self) -> PeerId {
        self.daemon_peer_id
    }

    /// Host a new session. Returns the new session's id and a join
    /// ticket the host distributes out-of-band.
    ///
    /// On store-write failure, the session is **not** added to the
    /// in-memory map and the error propagates. This keeps "registry
    /// thinks it has session X but disk doesn't" from happening.
    pub async fn host(&self, host_peer: PeerInfo) -> Result<(SessionId, JoinTicket), SessionError> {
        let session_id = SessionId::new_random();
        let ticket = JoinTicket::from(ticket::encode(&SessionTicket {
            session_id,
            host_peer_id: self.daemon_peer_id,
        }));
        let session = Session::new(session_id, &host_peer);
        let record = session.record();
        self.store
            .create(&record)
            .await
            .map_err(SessionError::Storage)?;
        self.sessions
            .write()
            .await
            .insert(session_id, Arc::new(Mutex::new(session)));
        Ok((session_id, ticket))
    }

    /// Join an existing session via its ticket. Returns the session id
    /// and the head seq at join time.
    pub async fn join(
        &self,
        ticket: &JoinTicket,
        peer: PeerInfo,
    ) -> Result<(SessionId, Option<Seq>), SessionError> {
        let SessionTicket { session_id, .. } = parse_ticket(ticket)?;
        let session = {
            let guard = self.sessions.read().await;
            guard
                .get(&session_id)
                .cloned()
                .ok_or(SessionError::UnknownSession(session_id))?
        };

        // Hold the session lock across the store write so a concurrent
        // join with the same peer doesn't race past the membership
        // check. This is the simplest correct shape; the store is fast
        // and uncontended in practice.
        let mut s = session.lock().await;
        if s.members.contains(&peer.id) {
            return Err(SessionError::AlreadyJoined(session_id));
        }
        self.store
            .add_member(session_id, &peer)
            .await
            .map_err(SessionError::Storage)?;
        s.members.insert(peer.id);

        // Notify other peers of the join. broadcast::send returns Err
        // when there are no receivers; that's fine, we treat it as a
        // "nobody listening" no-op.
        let _ = s.events_tx.send(Event::PeerJoined {
            session: session_id,
            peer,
        });

        let head = if s.head == Seq::ZERO {
            None
        } else {
            Some(s.head)
        };
        Ok((session_id, head))
    }

    /// Remove `peer` from `session`. If `peer` is the host, the entire
    /// session is closed and a [`Event::SessionClosed`] is emitted.
    pub async fn leave(&self, session: SessionId, peer: PeerId) -> Result<(), SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };

        let host;
        let session_closed;
        {
            let mut s = session_arc.lock().await;
            if !s.members.contains(&peer) {
                return Err(SessionError::NotMember(session));
            }
            host = s.host;
            session_closed = peer == host;

            // Persist before mutating in-memory state. If this fails,
            // the registry stays consistent with the store.
            if session_closed {
                self.store
                    .delete(session)
                    .await
                    .map_err(SessionError::Storage)?;
            } else {
                self.store
                    .remove_member(session, peer)
                    .await
                    .map_err(SessionError::Storage)?;
            }

            s.members.remove(&peer);
            if session_closed {
                let _ = s.events_tx.send(Event::SessionClosed { session });
            } else {
                let _ = s.events_tx.send(Event::PeerLeft { session, peer });
            }
        }

        if session_closed {
            self.sessions.write().await.remove(&session);
        }
        Ok(())
    }

    /// Snapshot of every active session as a [`SessionSummary`] list.
    pub async fn list(&self) -> Vec<SessionSummary> {
        // Take a cheap snapshot of the Arc handles, then release the
        // top-level lock before per-session locking. This keeps `host`/
        // `join`/`leave` callers from blocking on `list`.
        let arcs: Vec<Arc<Mutex<Session>>> = self.sessions.read().await.values().cloned().collect();
        let mut out = Vec::with_capacity(arcs.len());
        for arc in arcs {
            out.push(arc.lock().await.summary(self.daemon_peer_id));
        }
        out
    }

    /// Append a message to a session. Returns the assigned sequence
    /// number. Also broadcasts an [`Event::Message`] to subscribers.
    pub async fn send(
        &self,
        session: SessionId,
        peer: PeerInfo,
        kind: MessageKind,
        action: String,
        payload: Vec<u8>,
        timestamp_ms: u64,
    ) -> Result<Seq, SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };

        let mut s = session_arc.lock().await;
        if !s.members.contains(&peer.id) {
            return Err(SessionError::NotMember(session));
        }
        // Build the message under the session lock (so seq is stable),
        // then persist before bumping in-memory state and fanning out.
        // If the store fails, head and log are unchanged; the request
        // is rejected, the client gets a Storage error.
        //
        // We compute the prospective seq without committing it. If the
        // store write succeeds we commit; if not, we leave head alone.
        let prospective = s.head.next().expect("seq overflow");
        let message = SessionMessage::new(prospective, timestamp_ms, peer, kind, action, payload);
        if let Err(err) = self.store.append(session, &message).await {
            return Err(SessionError::Storage(err));
        }
        s.head = prospective;
        s.log.push(message.clone());

        // Snapshot the broadcast handle so we can drop the per-session
        // lock before fanning out — `broadcast::send` is cheap but
        // there's no reason to hold the session mutex across it.
        let events_tx = s.events_tx.clone();
        drop(s);
        let _ = events_tx.send(Event::Message { session, message });
        Ok(prospective)
    }

    /// Subscribe to live events for `session`, optionally backfilling
    /// every message with `seq > since` first.
    pub async fn subscribe(
        &self,
        session: SessionId,
        since: Option<Seq>,
    ) -> Result<Subscription, SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };

        let s = session_arc.lock().await;
        let cutoff = since.unwrap_or(Seq::ZERO);
        let replay: Vec<SessionMessage> =
            s.log.iter().filter(|m| m.seq > cutoff).cloned().collect();
        let events = s.events_tx.subscribe();
        drop(s);
        Ok(Subscription { replay, events })
    }
}

/// Parse an artel join ticket. Phase 2b: returns the session id; the
/// host peer id is decoded but not yet used (Phase 2c will route on
/// it). Any decode failure surfaces as [`SessionError::InvalidTicket`]
/// so the daemon doesn't leak parser internals over the wire.
fn parse_ticket(ticket: &JoinTicket) -> Result<SessionTicket, SessionError> {
    ticket::decode(ticket.as_str()).map_err(|err| {
        // Log the underlying TicketError at debug; the wire-facing
        // error stays generic so version-mismatch doesn't double as
        // an oracle.
        tracing::debug!(?err, "ticket decode failed");
        SessionError::InvalidTicket
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use artel_protocol::Event;
    use pretty_assertions::assert_eq;
    use tokio::time::timeout;

    use super::*;

    fn peer(byte: u8, name: &str) -> PeerInfo {
        PeerInfo::new(PeerId::from_bytes([byte; 32]), name)
    }

    fn registry() -> Registry {
        registry_with_peer(PeerId::from_bytes([0xff; 32]))
    }

    fn registry_with_peer(daemon_peer_id: PeerId) -> Registry {
        let store: DynStore = Arc::new(crate::store::MemoryStore::new());
        Registry::new(daemon_peer_id, store)
    }

    // ---- host ----

    #[tokio::test]
    async fn host_creates_session_and_returns_artel_ticket() {
        let daemon_peer = PeerId::from_bytes([0xff; 32]);
        let r = registry_with_peer(daemon_peer);
        let (id, ticket) = r.host(peer(1, "alice")).await.unwrap();
        assert!(ticket.as_str().starts_with("artel:"));
        // The ticket round-trips and embeds this daemon's peer id.
        let decoded = ticket::decode(ticket.as_str()).unwrap();
        assert_eq!(decoded.session_id, id);
        assert_eq!(decoded.host_peer_id, daemon_peer);
        let summaries = r.list().await;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, id);
        assert_eq!(summaries[0].peer_count, 1);
        assert_eq!(summaries[0].last_seq, None);
    }

    // ---- join ----

    #[tokio::test]
    async fn join_artel_ticket_succeeds_and_emits_peer_joined() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, ticket) = r.host(host).await.unwrap();

        // Subscribe before second peer joins so we observe the event.
        let mut sub = r.subscribe(id, None).await.unwrap();

        let joiner = peer(2, "bob");
        let (got_id, head) = r.join(&ticket, joiner.clone()).await.unwrap();
        assert_eq!(got_id, id);
        assert_eq!(head, None);

        let event = timeout(Duration::from_millis(100), sub.events.recv())
            .await
            .expect("event")
            .unwrap();
        match event {
            Event::PeerJoined { session, peer } => {
                assert_eq!(session, id);
                assert_eq!(peer, joiner);
            }
            other => panic!("expected PeerJoined, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn join_invalid_prefix_errors() {
        let r = registry();
        let err = r
            .join(&JoinTicket::from("iroh-fake:abc"), peer(2, "bob"))
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::InvalidTicket);
    }

    #[tokio::test]
    async fn join_legacy_artel_local_ticket_errors() {
        // Pre-2b strings are no longer accepted. We surface them as
        // InvalidTicket rather than UnknownSession so users get a
        // crisper signal when they paste old data.
        let r = registry();
        let bogus = SessionId::new_random();
        let err = r
            .join(
                &JoinTicket::from(format!("artel-local:{bogus}")),
                peer(2, "bob"),
            )
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::InvalidTicket);
    }

    #[tokio::test]
    async fn join_unknown_session_errors() {
        let r = registry();
        let bogus = SessionId::new_random();
        let ticket = JoinTicket::from(ticket::encode(&SessionTicket {
            session_id: bogus,
            host_peer_id: PeerId::from_bytes([0xff; 32]),
        }));
        let err = r.join(&ticket, peer(2, "bob")).await.unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    #[tokio::test]
    async fn join_twice_errors() {
        let r = registry();
        let (_id, ticket) = r.host(peer(1, "alice")).await.unwrap();
        r.join(&ticket, peer(2, "bob")).await.unwrap();
        let err = r.join(&ticket, peer(2, "bob")).await.unwrap_err();
        assert!(matches!(err, SessionError::AlreadyJoined(_)), "{err:?}");
    }

    // ---- send / sequencing ----

    #[tokio::test]
    async fn send_assigns_strictly_monotonic_seq() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host(host.clone()).await.unwrap();

        let s1 = r
            .send(id, host.clone(), MessageKind::Chat, "a".into(), vec![], 1)
            .await
            .unwrap();
        let s2 = r
            .send(id, host.clone(), MessageKind::Chat, "b".into(), vec![], 2)
            .await
            .unwrap();
        let s3 = r
            .send(id, host, MessageKind::Chat, "c".into(), vec![], 3)
            .await
            .unwrap();

        assert!(s1 < s2);
        assert!(s2 < s3);
        // First real seq is 1 (Seq::ZERO is reserved as "no messages").
        assert_eq!(s1, Seq::new(1));
    }

    #[tokio::test]
    async fn send_by_non_member_errors() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host(host).await.unwrap();
        let err = r
            .send(
                id,
                peer(9, "intruder"),
                MessageKind::Chat,
                "x".into(),
                vec![],
                0,
            )
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::NotMember(id));
    }

    #[tokio::test]
    async fn send_to_unknown_session_errors() {
        let r = registry();
        let bogus = SessionId::new_random();
        let err = r
            .send(
                bogus,
                peer(1, "alice"),
                MessageKind::Chat,
                "x".into(),
                vec![],
                0,
            )
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    #[tokio::test]
    async fn send_emits_message_event() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host(host.clone()).await.unwrap();

        let mut sub = r.subscribe(id, None).await.unwrap();
        let seq = r
            .send(
                id,
                host.clone(),
                MessageKind::Chat,
                "hello".into(),
                b"world".to_vec(),
                42,
            )
            .await
            .unwrap();

        let event = timeout(Duration::from_millis(100), sub.events.recv())
            .await
            .expect("event")
            .unwrap();
        match event {
            Event::Message { session, message } => {
                assert_eq!(session, id);
                assert_eq!(message.seq, seq);
                assert_eq!(message.action, "hello");
                assert_eq!(message.payload, b"world");
                assert_eq!(message.peer, host);
            }
            other => panic!("expected Message event, got {other:?}"),
        }
    }

    // ---- subscribe / replay ----

    #[tokio::test]
    async fn subscribe_replays_messages_after_since() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host(host.clone()).await.unwrap();

        let s1 = r
            .send(id, host.clone(), MessageKind::Chat, "1".into(), vec![], 0)
            .await
            .unwrap();
        let _s2 = r
            .send(id, host.clone(), MessageKind::Chat, "2".into(), vec![], 0)
            .await
            .unwrap();
        let _s3 = r
            .send(id, host, MessageKind::Chat, "3".into(), vec![], 0)
            .await
            .unwrap();

        // Subscribe with since = s1: replay should hold s2, s3 (in
        // order, no s1).
        let sub = r.subscribe(id, Some(s1)).await.unwrap();
        let actions: Vec<&str> = sub.replay.iter().map(|m| m.action.as_str()).collect();
        assert_eq!(actions, vec!["2", "3"]);
    }

    #[tokio::test]
    async fn subscribe_with_no_since_replays_full_log() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host(host.clone()).await.unwrap();
        for n in 0..5 {
            r.send(
                id,
                host.clone(),
                MessageKind::Chat,
                format!("m{n}"),
                vec![],
                0,
            )
            .await
            .unwrap();
        }
        let sub = r.subscribe(id, None).await.unwrap();
        assert_eq!(sub.replay.len(), 5);
    }

    #[tokio::test]
    async fn subscribe_unknown_session_errors() {
        let r = registry();
        let bogus = SessionId::new_random();
        let err = r.subscribe(bogus, None).await.unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    // ---- leave ----

    #[tokio::test]
    async fn member_leave_emits_peer_left_and_keeps_session() {
        let r = registry();
        let host = peer(1, "alice");
        let bob = peer(2, "bob");
        let (id, ticket) = r.host(host).await.unwrap();
        r.join(&ticket, bob.clone()).await.unwrap();

        let mut sub = r.subscribe(id, None).await.unwrap();
        r.leave(id, bob.id).await.unwrap();
        let event = timeout(Duration::from_millis(100), sub.events.recv())
            .await
            .expect("event")
            .unwrap();
        match event {
            Event::PeerLeft { session, peer } => {
                assert_eq!(session, id);
                assert_eq!(peer, bob.id);
            }
            other => panic!("expected PeerLeft, got {other:?}"),
        }
        // Session still exists.
        assert_eq!(r.list().await.len(), 1);
    }

    #[tokio::test]
    async fn host_leave_emits_session_closed_and_removes_session() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host(host.clone()).await.unwrap();
        let mut sub = r.subscribe(id, None).await.unwrap();
        r.leave(id, host.id).await.unwrap();

        let event = timeout(Duration::from_millis(100), sub.events.recv())
            .await
            .expect("event")
            .unwrap();
        assert_eq!(event, Event::SessionClosed { session: id });

        assert!(r.list().await.is_empty());
    }

    #[tokio::test]
    async fn leave_non_member_errors() {
        let r = registry();
        let (id, _) = r.host(peer(1, "alice")).await.unwrap();
        let err = r.leave(id, peer(9, "intruder").id).await.unwrap_err();
        assert_eq!(err, SessionError::NotMember(id));
    }

    #[tokio::test]
    async fn leave_unknown_session_errors() {
        let r = registry();
        let bogus = SessionId::new_random();
        let err = r.leave(bogus, peer(1, "alice").id).await.unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    // ---- list ----

    #[tokio::test]
    async fn list_summarises_each_session() {
        let r = registry();
        let host = peer(1, "alice");
        let bob = peer(2, "bob");
        let (id, ticket) = r.host(host.clone()).await.unwrap();
        r.join(&ticket, bob).await.unwrap();
        r.send(id, host, MessageKind::Chat, "x".into(), vec![], 0)
            .await
            .unwrap();

        let mut summaries = r.list().await;
        assert_eq!(summaries.len(), 1);
        let s = summaries.pop().unwrap();
        assert_eq!(s.id, id);
        assert_eq!(s.peer_count, 2);
        assert_eq!(s.last_seq, Some(Seq::new(1)));
        // Daemon peer id is 0xff, host is 0x01, so this daemon is not
        // the host of this session.
        assert!(!s.is_host);
    }

    #[tokio::test]
    async fn list_marks_is_host_when_daemon_is_session_host() {
        let daemon_peer = PeerId::from_bytes([7; 32]);
        let r = registry_with_peer(daemon_peer);
        let host = PeerInfo::new(daemon_peer, "self");
        r.host(host).await.unwrap();
        let summaries = r.list().await;
        assert!(summaries[0].is_host);
    }
}
