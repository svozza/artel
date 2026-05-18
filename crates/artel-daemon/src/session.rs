//! In-memory session registry and RPC handlers.
//!
//! A [`Registry`] owns every active session by ID. Each [`Session`]
//! holds an ordered message log (host-sequenced), a member set, and a
//! `broadcast::Sender<Event>` for live subscribers. RPC handlers are
//! methods on `Registry`; they take peer info as an argument so the
//! transport layer can supply it (peer identity comes from the IPC
//! handshake rather than the message).
//!
//! No iroh, no on-disk persistence yet. The placeholder
//! [`JoinTicket`] format is `artel-local:<session-uuid>` — only this
//! daemon understands it. The shape is deliberately distinct from
//! whatever real iroh-encoded tickets will look like, so misuse fails
//! loudly.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use artel_protocol::{
    Event, JoinTicket, MessageKind, PeerId, PeerInfo, Seq, SessionId, SessionMessage,
    SessionSummary,
};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, broadcast};

/// Capacity of the per-session broadcast channel.
///
/// Slow subscribers that lag by more than this lose old events; the
/// transport surfaces that to the client as a message gap (which the
/// client can recover from with a `Subscribe { since }`). This is the
/// right shape — we do not want to back-pressure publishers because of
/// one slow subscriber.
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Prefix on local-only join tickets.
///
/// Real iroh tickets will be opaque base32-or-similar blobs. Local
/// tickets are deliberately unmistakable so a misrouted ticket fails
/// fast.
const LOCAL_TICKET_PREFIX: &str = "artel-local:";

/// Errors the registry may return from RPC handlers.
#[derive(Debug, Error, PartialEq, Eq)]
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
}

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

    const fn next_seq(&mut self) -> Seq {
        // ZERO is reserved as "before any message". The first real
        // message gets Seq(1). overflow is panic-worthy: that's
        // 18 quintillion messages.
        let Some(next) = self.head.next() else {
            panic!("session sequence number overflowed u64");
        };
        self.head = next;
        next
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

/// In-memory session registry.
#[derive(Debug)]
pub struct Registry {
    daemon_peer_id: PeerId,
    sessions: RwLock<HashMap<SessionId, Arc<Mutex<Session>>>>,
}

impl Registry {
    /// Create an empty registry advertising `daemon_peer_id`.
    #[must_use]
    pub fn new(daemon_peer_id: PeerId) -> Self {
        Self {
            daemon_peer_id,
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// The daemon's own peer id, returned in the `Hello` response.
    #[must_use]
    pub const fn daemon_peer_id(&self) -> PeerId {
        self.daemon_peer_id
    }

    /// Host a new session. Returns the new session's id and a join
    /// ticket the host distributes out-of-band.
    pub async fn host(&self, host_peer: PeerInfo) -> (SessionId, JoinTicket) {
        let session_id = SessionId::new_random();
        let ticket = JoinTicket::from(format!("{LOCAL_TICKET_PREFIX}{session_id}"));
        let session = Session::new(session_id, &host_peer);
        self.sessions
            .write()
            .await
            .insert(session_id, Arc::new(Mutex::new(session)));
        (session_id, ticket)
    }

    /// Join an existing session via its ticket. Returns the session id
    /// and the head seq at join time.
    pub async fn join(
        &self,
        ticket: &JoinTicket,
        peer: PeerInfo,
    ) -> Result<(SessionId, Option<Seq>), SessionError> {
        let session_id = parse_local_ticket(ticket)?;
        let session = {
            let guard = self.sessions.read().await;
            guard
                .get(&session_id)
                .cloned()
                .ok_or(SessionError::UnknownSession(session_id))?
        };

        let mut s = session.lock().await;
        if !s.members.insert(peer.id) {
            return Err(SessionError::AlreadyJoined(session_id));
        }
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
            if !s.members.remove(&peer) {
                return Err(SessionError::NotMember(session));
            }
            host = s.host;
            session_closed = peer == host;
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
        let seq = s.next_seq();
        let message = SessionMessage::new(seq, timestamp_ms, peer, kind, action, payload);
        s.log.push(message.clone());
        // Snapshot the broadcast handle so we can drop the per-session
        // lock before fanning out — `broadcast::send` is cheap but
        // there's no reason to hold the session mutex across it.
        let events_tx = s.events_tx.clone();
        drop(s);
        let _ = events_tx.send(Event::Message { session, message });
        Ok(seq)
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

/// Parse a local-only join ticket (`artel-local:<uuid>`).
fn parse_local_ticket(ticket: &JoinTicket) -> Result<SessionId, SessionError> {
    let s = ticket.as_str();
    let rest = s
        .strip_prefix(LOCAL_TICKET_PREFIX)
        .ok_or(SessionError::InvalidTicket)?;
    rest.parse::<SessionId>()
        .map_err(|_| SessionError::InvalidTicket)
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
        Registry::new(PeerId::from_bytes([0xff; 32]))
    }

    // ---- host ----

    #[tokio::test]
    async fn host_creates_session_and_returns_local_ticket() {
        let r = registry();
        let (id, ticket) = r.host(peer(1, "alice")).await;
        assert!(ticket.as_str().starts_with(LOCAL_TICKET_PREFIX));
        assert!(ticket.as_str().ends_with(&id.to_string()));
        let summaries = r.list().await;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, id);
        assert_eq!(summaries[0].peer_count, 1);
        assert_eq!(summaries[0].last_seq, None);
    }

    // ---- join ----

    #[tokio::test]
    async fn join_local_ticket_succeeds_and_emits_peer_joined() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, ticket) = r.host(host).await;

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
    async fn join_invalid_uuid_errors() {
        let r = registry();
        let err = r
            .join(
                &JoinTicket::from(format!("{LOCAL_TICKET_PREFIX}not-a-uuid")),
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
        let ticket = JoinTicket::from(format!("{LOCAL_TICKET_PREFIX}{bogus}"));
        let err = r.join(&ticket, peer(2, "bob")).await.unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    #[tokio::test]
    async fn join_twice_errors() {
        let r = registry();
        let (_id, ticket) = r.host(peer(1, "alice")).await;
        r.join(&ticket, peer(2, "bob")).await.unwrap();
        let err = r.join(&ticket, peer(2, "bob")).await.unwrap_err();
        assert!(matches!(err, SessionError::AlreadyJoined(_)), "{err:?}");
    }

    // ---- send / sequencing ----

    #[tokio::test]
    async fn send_assigns_strictly_monotonic_seq() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host(host.clone()).await;

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
        let (id, _) = r.host(host).await;
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
        let (id, _) = r.host(host.clone()).await;

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
        let (id, _) = r.host(host.clone()).await;

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
        let (id, _) = r.host(host.clone()).await;
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
        let (id, ticket) = r.host(host).await;
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
        let (id, _) = r.host(host.clone()).await;
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
        let (id, _) = r.host(peer(1, "alice")).await;
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
        let (id, ticket) = r.host(host.clone()).await;
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
        let r = Registry::new(daemon_peer);
        let host = PeerInfo::new(daemon_peer, "self");
        r.host(host).await;
        let summaries = r.list().await;
        assert!(summaries[0].is_host);
    }
}
