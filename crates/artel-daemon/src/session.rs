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

use artel_protocol::ticket::{self, SessionTicket, WireEndpointAddr};
use artel_protocol::{
    Event, JoinTicket, MessageKind, PeerId, PeerInfo, Seq, SessionId, SessionMessage,
    SessionSummary,
};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, broadcast};

use crate::store::{DynStore, SessionKind, SessionRecord, StoredAttachment};

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

    /// Join ticket malformed or revoked.
    #[error("invalid join ticket")]
    InvalidTicket,

    /// Backing storage failed. The in-memory state was not changed.
    #[error("storage: {0}")]
    Storage(#[source] std::io::Error),

    /// Ticket carried a host addr the daemon couldn't parse.
    #[error("invalid host address in ticket: {0}")]
    InvalidAddr(String),

    /// Internal failure inside iroh gossip plumbing. Surfaces over
    /// the wire as `ProtocolError::Internal` — the joiner gets a
    /// generic error rather than iroh-specific detail.
    #[error("internal: {0}")]
    Internal(String),

    /// `Send` issued for a session whose host is a different
    /// daemon AND the daemon is built without the `iroh` feature
    /// (so there's no transport to forward through). With `iroh`
    /// on, joiner sends are routed through the gossip bridge.
    #[error("send is only supported on the host side in this build")]
    NotHost,

    /// `Registry::host(peer, Some(id))` was issued for an `id`
    /// that exists locally but with a different host or as a
    /// remote-mirror session. The caller is asking to resume a
    /// session they don't own. Maps to
    /// [`artel_protocol::ProtocolError::SessionConflict`] over
    /// the wire.
    #[error("session id {0} already exists with a different host or kind")]
    SessionConflict(SessionId),

    /// A joiner-side `Send` that we forwarded to the host over
    /// gossip came back with a wire-form rejection. The wrapped
    /// [`artel_protocol::ProtocolError`] is what the host
    /// authoritatively decided; we forward it verbatim to the
    /// IPC client so they see the host's actual reason rather than
    /// a flattened `Internal` shrug.
    #[error("host rejected send: {0}")]
    HostRejected(#[source] artel_protocol::ProtocolError),
}

// io::Error doesn't impl PartialEq, so we hand-roll one for the
// Storage-free variants tests rely on.
impl PartialEq for SessionError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::UnknownSession(a), Self::UnknownSession(b))
            | (Self::NotMember(a), Self::NotMember(b))
            | (Self::SessionConflict(a), Self::SessionConflict(b)) => a == b,
            (Self::Storage(a), Self::Storage(b)) => a.kind() == b.kind(),
            (Self::InvalidAddr(a), Self::InvalidAddr(b))
            | (Self::Internal(a), Self::Internal(b)) => a == b,
            (Self::HostRejected(a), Self::HostRejected(b)) => a == b,
            (Self::InvalidTicket, Self::InvalidTicket) | (Self::NotHost, Self::NotHost) => true,
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
    kind: SessionKind,
    members: HashSet<PeerId>,
    log: Vec<SessionMessage>,
    head: Seq,
    events_tx: broadcast::Sender<Event>,
}

impl Session {
    fn new(id: SessionId, host: &PeerInfo, kind: SessionKind) -> Self {
        let mut members = HashSet::new();
        members.insert(host.id);
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            id,
            host: host.id,
            kind,
            members,
            log: Vec::new(),
            head: Seq::ZERO,
            events_tx,
        }
    }

    /// Hydrate from a persisted [`SessionRecord`]. The record's
    /// `kind` is authoritative — a `Remote` mirror rehydrates as
    /// `Remote` so it doesn't try to assign seqs locally after a
    /// daemon restart.
    fn from_record(record: SessionRecord) -> Self {
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            id: record.id,
            host: record.host,
            kind: record.kind,
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
            kind: self.kind,
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
    /// The daemon's own [`WireEndpointAddr`], stamped into every
    /// outbound `host` ticket so joiners can dial back. Either a
    /// snapshot of the live iroh `Endpoint::addr()` or an
    /// `id_only` placeholder when the daemon is local-only.
    daemon_addr: WireEndpointAddr,
    sessions: RwLock<HashMap<SessionId, Arc<Mutex<Session>>>>,
    store: DynStore,
    /// Plumbing to the iroh gossip substrate. `Some` when the daemon
    /// is running with the `iroh` feature on and an `iroh_key_path`
    /// supplied; `None` for local-only embeds and unit tests.
    #[cfg(feature = "iroh")]
    bridge: Option<Arc<crate::gossip_bridge::GossipBridge>>,
    /// Daemon's iroh secret key, used to sign every locally-authored
    /// `SessionMessage` (Auth Slice B). `None` for unit tests that
    /// build a registry without an iroh runtime in scope; production
    /// code paths populate this from
    /// [`crate::server::IrohRuntime::signing_key`]. In B1 nothing
    /// reads this field yet (`Registry::send` still ships the
    /// `SIGNATURE_UNSIGNED` sentinel); B2 turns signing on.
    #[cfg(feature = "iroh")]
    #[allow(dead_code)] // wired in B1; consumed in B2.
    signing_key: Option<Arc<iroh::SecretKey>>,
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
            daemon_addr: WireEndpointAddr::id_only(daemon_peer_id),
            sessions: RwLock::new(HashMap::new()),
            store,
            #[cfg(feature = "iroh")]
            bridge: None,
            #[cfg(feature = "iroh")]
            signing_key: None,
        }
    }

    /// Build a registry whose initial state is the records the store
    /// returned from `load_all`. Called once at daemon startup.
    pub(crate) async fn load(
        daemon_peer_id: PeerId,
        daemon_addr: WireEndpointAddr,
        store: DynStore,
        #[cfg(feature = "iroh")] bridge: Option<Arc<crate::gossip_bridge::GossipBridge>>,
        #[cfg(feature = "iroh")] signing_key: Option<Arc<iroh::SecretKey>>,
    ) -> std::io::Result<Self> {
        let records = store.load_all().await?;
        let mut sessions = HashMap::with_capacity(records.len());
        for record in records {
            let id = record.id;
            sessions.insert(id, Arc::new(Mutex::new(Session::from_record(record))));
        }
        Ok(Self {
            daemon_peer_id,
            daemon_addr,
            sessions: RwLock::new(sessions),
            store,
            #[cfg(feature = "iroh")]
            bridge,
            #[cfg(feature = "iroh")]
            signing_key,
        })
    }

    /// The daemon's own peer id, returned in the `Hello` response.
    #[must_use]
    pub const fn daemon_peer_id(&self) -> PeerId {
        self.daemon_peer_id
    }

    /// Host or resume a session. Returns the session's id and a
    /// fresh join ticket stamped with this daemon's current
    /// [`WireEndpointAddr`].
    ///
    /// `requested_id` controls the session id and the resume path:
    ///
    /// - `None` (today's behaviour): mint a fresh random
    ///   [`SessionId`] and create a new session record.
    /// - `Some(id)` and **no existing local entry**: create a new
    ///   session record at `id`. Lets a caller (e.g. `artel-fs`)
    ///   pin the id to local state so a re-host always lands on
    ///   the same id.
    /// - `Some(id)` and **existing local entry whose host is
    ///   `host_peer.id` and whose kind is `Local`**: resume
    ///   verbatim. Members, log, head, and broadcast channel are
    ///   preserved. The returned ticket is re-stamped from the
    ///   *current* `daemon_addr`, which may differ from the addr
    ///   in the persisted record after a daemon restart.
    /// - `Some(id)` and **existing entry that doesn't match**
    ///   (different host, or `kind == Remote`): rejected with
    ///   [`SessionError::SessionConflict`]. The in-memory state
    ///   is not modified.
    ///
    /// On store-write failure for the create paths the session is
    /// **not** added to the in-memory map and the error propagates.
    /// This keeps "registry thinks it has session X but disk
    /// doesn't" from happening. The resume path doesn't write to
    /// the store at all — the record is already there.
    pub async fn host(
        &self,
        host_peer: PeerInfo,
        requested_id: Option<SessionId>,
    ) -> Result<(SessionId, JoinTicket), SessionError> {
        // Resume path: caller supplied an id and we already have a
        // matching local-host record. Reuse the in-memory session
        // verbatim and re-stamp the ticket with the current addr.
        if let Some(id) = requested_id {
            let existing = {
                let guard = self.sessions.read().await;
                guard.get(&id).cloned()
            };
            if let Some(arc) = existing {
                let s = arc.lock().await;
                if s.host != host_peer.id || s.kind != SessionKind::Local {
                    return Err(SessionError::SessionConflict(id));
                }
                drop(s);
                let ticket = JoinTicket::from(ticket::encode(&SessionTicket {
                    session_id: id,
                    host_peer_id: self.daemon_peer_id,
                    host_addr: self.daemon_addr.clone(),
                }));

                // Re-open the gossip topic. The bridge tracks per-
                // session state by id; if the daemon was restarted
                // since the original `host` call, the topic is gone
                // and we need to re-subscribe. If it's still around
                // (same-process resume), the existing entry is left
                // in place and we just ignore the re-host. Best-
                // effort: a bridge failure is non-fatal — the local
                // session still works; we just won't reach the
                // network until something else triggers a reattach.
                #[cfg(feature = "iroh")]
                if let Some(bridge) = &self.bridge
                    && let Err(err) = bridge.host_session(id).await
                {
                    tracing::warn!(?err, ?id, "gossip host_session failed on resume");
                }

                return Ok((id, ticket));
            }
        }

        // Create path. Either no `requested_id` (mint random) or
        // `Some(id)` whose entry doesn't exist locally yet.
        let session_id = requested_id.unwrap_or_else(SessionId::new_random);
        let ticket = JoinTicket::from(ticket::encode(&SessionTicket {
            session_id,
            host_peer_id: self.daemon_peer_id,
            host_addr: self.daemon_addr.clone(),
        }));
        let session = Session::new(session_id, &host_peer, SessionKind::Local);
        let record = session.record();
        self.store
            .create(&record)
            .await
            .map_err(SessionError::Storage)?;
        self.sessions
            .write()
            .await
            .insert(session_id, Arc::new(Mutex::new(session)));

        // If iroh is wired up, open a gossip topic for this session
        // so future Sends can fan out to remote joiners. Bridge
        // failure is non-fatal: the local session still works; we
        // just won't reach the network. Surface as a warn for ops.
        #[cfg(feature = "iroh")]
        if let Some(bridge) = &self.bridge
            && let Err(err) = bridge.host_session(session_id).await
        {
            tracing::warn!(?err, ?session_id, "gossip host_session failed");
        }

        Ok((session_id, ticket))
    }

    /// Join an existing session via its ticket. Returns the session id
    /// and the head seq at join time.
    ///
    /// Two cases:
    ///
    /// - **Local session.** The session is already in `self.sessions`
    ///   (we're the host or an earlier joiner-on-the-same-daemon).
    ///   Just adds the peer to membership and emits `PeerJoined`.
    /// - **Remote session** (`host_peer_id != self.daemon_peer_id`).
    ///   The session doesn't exist locally yet; we materialise a
    ///   mirror, ask the bridge to subscribe to the host's gossip
    ///   topic, and feed inbound messages into the mirror. Without
    ///   the iroh feature this is rejected as `InvalidTicket`.
    pub async fn join(
        &self,
        ticket: &JoinTicket,
        peer: PeerInfo,
    ) -> Result<(SessionId, Option<Seq>), SessionError> {
        let parsed = parse_ticket(ticket)?;
        let session_id = parsed.session_id;

        let session = {
            let guard = self.sessions.read().await;
            guard.get(&session_id).cloned()
        };

        let session = if let Some(existing) = session {
            existing
        } else {
            if parsed.host_peer_id == self.daemon_peer_id {
                // Same-daemon ticket but the session id isn't
                // registered locally — that's a stale or forged
                // ticket, not a "join a remote" request.
                return Err(SessionError::UnknownSession(session_id));
            }
            // Remote session: not yet known locally. Materialise
            // a mirror and wire up the gossip bridge so the host's
            // messages start flowing in.
            self.materialise_remote_session(
                session_id,
                &parsed.host_peer_id,
                &parsed.host_addr,
                &peer,
            )
            .await?
        };

        // Hold the session lock across the store write so a concurrent
        // join with the same peer doesn't race past the membership
        // check. This is the simplest correct shape; the store is fast
        // and uncontended in practice.
        let head;
        {
            let mut s = session.lock().await;
            head = if s.head == Seq::ZERO {
                None
            } else {
                Some(s.head)
            };
            if s.members.contains(&peer.id) {
                // Self-rejoin: caller's authenticated id is already
                // a member. Daemon-side membership is per-
                // authenticated-identity (persistent across consumer
                // remounts); a re-host or re-join from the same
                // daemon is a no-op. No second PeerJoined fires.
                // See `docs/plans/2026-06-01-auth-l1-fix3-plan.md`.
                return Ok((session_id, head));
            }
            self.store
                .add_member(session_id, &peer)
                .await
                .map_err(SessionError::Storage)?;
            s.members.insert(peer.id);

            // Notify other peers of the join. broadcast::send
            // returns Err when there are no receivers; that's fine,
            // we treat it as a "nobody listening" no-op.
            let _ = s.events_tx.send(Event::PeerJoined {
                session: session_id,
                peer,
            });
        }

        Ok((session_id, head))
    }

    /// Stand up a local mirror of a session whose authoritative log
    /// lives on another daemon. Inserts a new [`Session`] keyed by
    /// `session_id`, persists it, and asks the bridge to subscribe
    /// to the host's gossip topic. Inbound gossip messages land in
    /// the mirror's `log` and `events_tx`.
    async fn materialise_remote_session(
        &self,
        session_id: SessionId,
        host_peer_id: &PeerId,
        host_addr: &WireEndpointAddr,
        joiner: &PeerInfo,
    ) -> Result<Arc<Mutex<Session>>, SessionError> {
        // Without the `iroh` feature, there's no way to actually
        // reach the host; refuse cleanly rather than silently
        // creating an unreachable session.
        #[cfg(not(feature = "iroh"))]
        {
            let _ = (host_peer_id, host_addr, joiner);
            tracing::debug!(
                ?session_id,
                "remote ticket received but iroh feature is off",
            );
            return Err(SessionError::InvalidTicket);
        }

        #[cfg(feature = "iroh")]
        {
            let bridge = self
                .bridge
                .as_ref()
                .ok_or(SessionError::InvalidTicket)?
                .clone();

            // Persist the new session so a later daemon restart
            // doesn't lose the membership / log we're about to start
            // populating from the host. Host field is the *remote*
            // peer's id, which lets `summary` distinguish remote
            // sessions in `list` output.
            let mut session_obj = Session::new(
                session_id,
                &PeerInfo::new(*host_peer_id, "remote-host"),
                SessionKind::Remote,
            );
            // The constructor adds the host to `members`; for a
            // remote session that's right (the host is a member of
            // its own session) but we'll never see local Send from
            // them — Sends arrive via gossip and route through the
            // forwarder.
            session_obj.host = *host_peer_id;
            self.store
                .create(&session_obj.record())
                .await
                .map_err(SessionError::Storage)?;
            let arc = Arc::new(Mutex::new(session_obj));
            self.sessions
                .write()
                .await
                .insert(session_id, Arc::clone(&arc));

            // Hand the bridge a callback that writes into this very
            // mirror. We deliberately keep a strong Arc in the
            // closure so the session outlives the forwarder task
            // until forget_session aborts it. The store handle is
            // cloned so the callback can persist each message —
            // without that, a daemon restart loses the entire
            // remote-mirror log (`Subscribe { since: None }` replays
            // nothing on bob's restart, so a joiner that re-runs
            // `Workspace::join_with` hangs in `wait_for_ticket`
            // forever waiting for the host's `workspace.ticket`
            // System message that was never persisted).
            let mirror = Arc::clone(&arc);
            let store = self.store.clone();
            let session_for_log = session_id;
            let on_message = move |msg: SessionMessage| {
                let mirror = Arc::clone(&mirror);
                let store = store.clone();
                let session_for_log = session_for_log;
                // Spawn so the gossip forwarder doesn't block on
                // each message. Acceptable for now; if ordering
                // ever matters we can replace with a per-session
                // mpsc.
                tokio::spawn(async move {
                    // Persist BEFORE mutating in-memory state so a
                    // crash mid-callback doesn't leave the mirror's
                    // in-memory log ahead of disk. Idempotent on
                    // duplicate seq via the partition_point check
                    // below — but the store itself doesn't dedupe,
                    // so we check first.
                    let mut s = mirror.lock().await;
                    let pos = s.log.partition_point(|m| m.seq < msg.seq);
                    if pos < s.log.len() && s.log[pos].seq == msg.seq {
                        // Duplicate seq; drop quietly. Already on
                        // disk from the first delivery.
                        return;
                    }
                    // Persist while holding the lock so a concurrent
                    // remote-mirror cascade (`leave` of the joiner)
                    // can't race us. Failure: log, drop the message;
                    // the host will re-broadcast on Replay if the
                    // joiner asks again, and the in-memory state stays
                    // consistent with disk.
                    if let Err(err) = store.append(session_for_log, &msg).await {
                        tracing::warn!(
                            session = ?session_for_log,
                            seq = ?msg.seq,
                            error = %err,
                            "remote-mirror log persist failed; dropping message",
                        );
                        return;
                    }
                    s.log.insert(pos, msg.clone());
                    if msg.seq > s.head {
                        s.head = msg.seq;
                    }
                    let _ = s.events_tx.send(Event::Message {
                        session: session_for_log,
                        message: msg,
                    });
                });
            };

            // Wire `host_addr` is used as a synchronous addr hint to
            // sidestep pkarr propagation: the bridge feeds it into
            // its `MemoryLookup` before subscribing so the very
            // first dial finds the host's relay url + direct addrs
            // without waiting on n0 DNS / `DnsPkarrServer`. Falling
            // back on pkarr alone produced a ~500ms-to-15s race in
            // production where a fresh joiner would hit
            // `JOIN_READY_TIMEOUT` before the host's record reached
            // their resolver. The wire format is re-validated at
            // the bridge boundary; a bad addr surfaces as
            // [`SessionError::InvalidAddr`].
            bridge
                .join_session(
                    session_id,
                    joiner.clone(),
                    *host_peer_id,
                    host_addr,
                    on_message,
                )
                .await
                .map_err(|e| match e {
                    crate::gossip_bridge::BridgeError::InvalidAddr(msg) => {
                        SessionError::InvalidAddr(msg)
                    }
                    other => SessionError::Internal(other.to_string()),
                })?;

            Ok(arc)
        }
    }

    /// Remove `peer` from `session`. Three cases, distinguished by
    /// session [`SessionKind`] and whether the leaver is the host:
    ///
    /// 1. **Host of a `Local` session leaves** → the entire session is
    ///    closed: `store.delete(session)` (cascades any consumer
    ///    attachments via the store contract), in-memory entry
    ///    removed, [`Event::SessionClosed`] emitted, gossip topic
    ///    torn down with a final `SessionClosed` broadcast so remote
    ///    mirrors see the close.
    ///
    /// 2. **Joiner of a `Local` session leaves** → the session keeps
    ///    going (other members are still in it). Just `remove_member`
    ///    in the store and emit [`Event::PeerLeft`].
    ///
    /// 3. **Joiner of a `Remote` mirror leaves** → the local mirror
    ///    has no purpose without its only local consumer, so drop it
    ///    fully: `store.delete(session)` (cascading attachments),
    ///    in-memory entry removed, [`Event::SessionClosed`] emitted,
    ///    bridge per-session state forgotten. Symmetric with
    ///    [`Self::host_closed_session`] — same teardown shape, just
    ///    triggered by a local IPC leave instead of a gossip
    ///    `SessionClosed` from the host.
    pub async fn leave(&self, session: SessionId, peer: PeerId) -> Result<(), SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };

        // Decide the disposition under the per-session lock so a
        // concurrent `register_attachment` (which holds the same
        // lock) cannot land between our existence check and the
        // store cascade.
        let host;
        let kind;
        let drop_session;
        {
            let mut s = session_arc.lock().await;
            if !s.members.contains(&peer) {
                return Err(SessionError::NotMember(session));
            }
            host = s.host;
            kind = s.kind;
            // The session's local lifetime ends in two cases: the
            // host is leaving a Local session (case 1), or the
            // (sole local) joiner is leaving a Remote mirror
            // (case 3). Both run the same store-side cascade.
            drop_session = peer == host || kind == SessionKind::Remote;

            // Persist before mutating in-memory state. If this fails,
            // the registry stays consistent with the store.
            if drop_session {
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
            if drop_session {
                let _ = s.events_tx.send(Event::SessionClosed { session });
            } else {
                let _ = s.events_tx.send(Event::PeerLeft { session, peer });
            }
        }

        if drop_session {
            self.sessions.write().await.remove(&session);
            // Bridge teardown:
            // - Local-host leave (case 1): publish a final
            //   `SessionClosed` over gossip so remote mirrors see the
            //   close, then drop our topic state.
            // - Remote-mirror leave (case 3): we are NOT the host, so
            //   we do not publish anything — the host's mirror is
            //   none of our business. Just drop our local topic
            //   state so the forwarder task exits.
            #[cfg(feature = "iroh")]
            if let Some(bridge) = &self.bridge {
                if peer == host && kind == SessionKind::Local {
                    bridge.publish_session_closed(session).await;
                }
                bridge.forget_session(session).await;
            }
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

    /// Ensure `peer` is a member of `session`. No-op if they
    /// already are. Used by the gossip bridge on the host side to
    /// admit a remote joiner — typically driven by an inbound
    /// `JoinAnnouncement` frame, with `SendRequest` as an
    /// idempotent backstop in case the announcement was lost or
    /// arrives out of order. Persists the membership change and
    /// emits [`Event::PeerJoined`] when the peer is newly added.
    ///
    /// Returns `Err(UnknownSession)` if `session` doesn't exist on
    /// this daemon. Other failures surface the underlying
    /// [`SessionError`].
    #[cfg(feature = "iroh")]
    pub(crate) async fn ensure_member(
        &self,
        session: SessionId,
        peer: PeerInfo,
    ) -> Result<(), SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };
        let mut s = session_arc.lock().await;
        if s.members.contains(&peer.id) {
            return Ok(());
        }
        // Persist before mutating in-memory state — same shape as
        // `Registry::join`. If the store fails, the registry stays
        // consistent with disk.
        self.store
            .add_member(session, &peer)
            .await
            .map_err(SessionError::Storage)?;
        s.members.insert(peer.id);
        let events_tx = s.events_tx.clone();
        drop(s);
        let _ = events_tx.send(Event::PeerJoined { session, peer });
        Ok(())
    }

    /// Snapshot every message in `session`'s log with `seq > since`.
    /// Used by the host's gossip bridge to answer a joiner's
    /// `Replay` request — we re-broadcast each entry as a
    /// `Message` frame and the joiner's mirror dedups by seq.
    ///
    /// Returns `Err(UnknownSession)` if `session` doesn't exist on
    /// this daemon. Returns an empty Vec if the joiner is already
    /// caught up.
    #[cfg(feature = "iroh")]
    pub(crate) async fn log_since(
        &self,
        session: SessionId,
        since: Seq,
    ) -> Result<Vec<SessionMessage>, SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };
        let s = session_arc.lock().await;
        Ok(s.log.iter().filter(|m| m.seq > since).cloned().collect())
    }

    /// Drop a remote-mirror session because the host has signalled
    /// (via [`GossipBody::SessionClosed`]) that they're closing it.
    /// Deletes the persisted record, removes the in-memory session,
    /// emits [`Event::SessionClosed`] to local IPC subscribers, and
    /// tears down the bridge's per-session topic state. Idempotent:
    /// if the session is already gone (or was never `Remote`)
    /// returns `Ok(())` so a duplicate close broadcast doesn't
    /// surface as an error.
    ///
    /// Only meaningful for `Remote` sessions — the host's own
    /// close path is `Registry::leave(session, host_peer)`. We
    /// guard against the wrong kind defensively so a misrouted
    /// frame from a hostile peer can't poison a local session.
    ///
    /// [`GossipBody::SessionClosed`]: artel_protocol::gossip::GossipBody::SessionClosed
    #[cfg(feature = "iroh")]
    pub(crate) async fn host_closed_session(&self, session: SessionId) -> Result<(), SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard.get(&session).cloned()
        };
        let Some(session_arc) = session_arc else {
            // Already closed (or never present). Nothing to do.
            return Ok(());
        };
        // Hold the per-session lock across the store cascade so a
        // concurrent `register_attachment` (which also takes this
        // lock) cannot land an attachment after the cascade runs.
        // Same shape as `leave`'s critical section.
        let events_tx = {
            let s = session_arc.lock().await;
            if s.kind != SessionKind::Remote {
                tracing::warn!(?session, "ignoring SessionClosed for a non-remote session",);
                return Ok(());
            }
            self.store
                .delete(session)
                .await
                .map_err(SessionError::Storage)?;
            s.events_tx.clone()
        };

        self.sessions.write().await.remove(&session);
        let _ = events_tx.send(Event::SessionClosed { session });

        if let Some(bridge) = &self.bridge {
            bridge.forget_session(session).await;
        }
        Ok(())
    }

    /// Append a message to a session. Returns the freshly-built
    /// [`SessionMessage`] (with its host-assigned `seq`). Also
    /// broadcasts an [`Event::Message`] to local IPC subscribers and
    /// (when the bridge is wired up and this is a `Local` session)
    /// fans out over gossip.
    ///
    /// Remote-mirror sessions return [`SessionError::NotHost`] —
    /// the joiner-side path goes through
    /// [`crate::gossip_bridge::GossipBridge::send_remote`] which
    /// publishes a [`GossipBody::SendRequest`] and awaits a
    /// host-published [`GossipBody::SendAck`]. The outer registry
    /// caller (the IPC dispatch) is responsible for choosing the
    /// right path based on the session kind.
    ///
    /// [`GossipBody::SendRequest`]: artel_protocol::gossip::GossipBody::SendRequest
    /// [`GossipBody::SendAck`]: artel_protocol::gossip::GossipBody::SendAck
    pub async fn send(
        &self,
        session: SessionId,
        peer: PeerInfo,
        kind: MessageKind,
        action: String,
        payload: Vec<u8>,
        timestamp_ms: u64,
    ) -> Result<SessionMessage, SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };

        let kind_snapshot;
        {
            let s = session_arc.lock().await;
            if !s.members.contains(&peer.id) {
                return Err(SessionError::NotMember(session));
            }
            kind_snapshot = s.kind;
        }

        // Remote-mirror sessions can't append locally — the host is
        // the sequencer. Forward via gossip (`SendRequest`) and wait
        // for the host's `SendAck`. The host's local `Registry::send`
        // will produce the broadcast `Message` frame we'll see on
        // our own forwarder; the IPC reply uses the assigned
        // `SessionMessage` from the ack.
        #[cfg(feature = "iroh")]
        if kind_snapshot == SessionKind::Remote {
            let bridge = self.bridge.as_ref().ok_or_else(|| {
                SessionError::Internal("remote send requires gossip bridge".into())
            })?;
            let send_payload = artel_protocol::rpc::SendPayload {
                kind,
                action,
                payload,
            };
            return match bridge.send_remote(session, peer, send_payload).await {
                Ok(message) => Ok(message),
                Err(crate::gossip_bridge::BridgeError::HostRejected(err)) => {
                    Err(SessionError::HostRejected(err))
                }
                Err(crate::gossip_bridge::BridgeError::SendTimeout) => Err(SessionError::Internal(
                    "send_remote: timed out waiting for host ack".into(),
                )),
                Err(crate::gossip_bridge::BridgeError::UnknownSession(_)) => Err(
                    SessionError::Internal("send_remote: bridge missing session topic".into()),
                ),
                Err(crate::gossip_bridge::BridgeError::Iroh(msg)) => {
                    Err(SessionError::Internal(format!("gossip: {msg}")))
                }
                // `send_remote` doesn't take a wire-form addr, so
                // `InvalidAddr` shouldn't surface here today; map it
                // defensively the same way [`Self::join`] does so a
                // future refactor that funnels addr-validation
                // through `send_remote` doesn't silently flatten it.
                Err(crate::gossip_bridge::BridgeError::InvalidAddr(msg)) => {
                    Err(SessionError::InvalidAddr(msg))
                }
            };
        }
        #[cfg(not(feature = "iroh"))]
        if kind_snapshot == SessionKind::Remote {
            // Without the iroh feature we have no transport; remote
            // sessions can't even be materialised here, but keep the
            // arm for completeness.
            return Err(SessionError::NotHost);
        }
        let mut s = session_arc.lock().await;
        // Build the message under the session lock (so seq is stable),
        // then persist before bumping in-memory state and fanning out.
        // If the store fails, head and log are unchanged; the request
        // is rejected, the client gets a Storage error.
        //
        // We compute the prospective seq without committing it. If the
        // store write succeeds we commit; if not, we leave head alone.
        let prospective = s.head.next().expect("seq overflow");
        // TODO(slice-b2): replace SIGNATURE_UNSIGNED with sign_body using
        // the registry's signing key. Today we ship the sentinel; B2
        // turns verification on, at which point any unsigned path goes
        // red catastrophically — that's deliberate.
        let message = SessionMessage::new(
            prospective,
            timestamp_ms,
            peer,
            kind,
            action,
            payload,
            artel_protocol::message::SIGNATURE_UNSIGNED,
        );
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
        let _ = events_tx.send(Event::Message {
            session,
            message: message.clone(),
        });

        // Forward to remote joiners over gossip. Best-effort: if the
        // bridge isn't available (no iroh, or it errored), the local
        // fan-out has already happened so IPC clients are served.
        #[cfg(feature = "iroh")]
        if let Some(bridge) = &self.bridge {
            bridge.publish_message(session, message.clone()).await;
        }

        Ok(message)
    }

    /// Persist (or overwrite) a consumer-tagged attachment against
    /// `session`. The daemon never inspects `payload`; consumers
    /// (e.g. `artel-fs`) namespace `kind` and ship a postcard-encoded
    /// blob.
    ///
    /// Returns [`SessionError::UnknownSession`] when `session` is not
    /// known to the daemon. Idempotent within a `(session, kind)`
    /// pair: re-registering overwrites. Attachments cascade-delete
    /// with their session — see [`crate::store::SessionStore::delete`].
    ///
    /// Holds the per-session `Mutex<Session>` across the store write
    /// so a concurrent [`Self::leave`] (host) or
    /// [`Self::host_closed_session`] (remote mirror) cannot run its
    /// cascade between our existence check and the put — that would
    /// orphan the attachment. This is the synchronization point the
    /// store's [`crate::store::SessionStore::put_attachment`] doc
    /// references.
    pub(crate) async fn register_attachment(
        &self,
        session: SessionId,
        kind: String,
        payload: Vec<u8>,
    ) -> Result<(), SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };
        let _s = session_arc.lock().await;
        match self.store.put_attachment(session, &kind, &payload).await {
            Ok(true) => Ok(()),
            // The session existed when we took the lock but the store
            // disagrees — treat as UnknownSession; this is the only
            // way `Ok(false)` is reachable now.
            Ok(false) => Err(SessionError::UnknownSession(session)),
            Err(err) => Err(SessionError::Storage(err)),
        }
    }

    /// List every attachment matching `kind_filter`. `None` returns
    /// all kinds across all sessions; `Some(k)` returns only those
    /// tagged with `k`. Order unspecified.
    ///
    /// Does not take per-session locks — a concurrent register or
    /// cascade may shift the result by one entry but cannot produce
    /// a torn read of any individual attachment (each store op is
    /// itself atomic at the file/map-entry granularity).
    pub(crate) async fn list_attachments(
        &self,
        kind_filter: Option<&str>,
    ) -> Result<Vec<StoredAttachment>, SessionError> {
        self.store
            .list_attachments(kind_filter)
            .await
            .map_err(SessionError::Storage)
    }

    /// Remove the attachment at `(session, kind)` without removing
    /// the session itself. Idempotent: missing session OR missing
    /// attachment returns `Ok(())`.
    ///
    /// Holds the per-session lock when the session is known so a
    /// concurrent register cannot resurrect the attachment between
    /// our delete and the caller observing the empty list. If the
    /// session is already gone (cascade ran), the store's idempotent
    /// `delete_attachment` returns `Ok(())` directly.
    pub(crate) async fn forget_attachment(
        &self,
        session: SessionId,
        kind: String,
    ) -> Result<(), SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard.get(&session).cloned()
        };
        let _maybe_lock = match &session_arc {
            Some(arc) => Some(arc.lock().await),
            None => None,
        };
        self.store
            .delete_attachment(session, &kind)
            .await
            .map_err(SessionError::Storage)
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
        let (id, ticket) = r.host(peer(1, "alice"), None).await.unwrap();
        assert!(ticket.as_str().starts_with("artel:"));
        // The ticket round-trips and embeds this daemon's identity.
        let decoded = ticket::decode(ticket.as_str()).unwrap();
        assert_eq!(decoded.session_id, id);
        assert_eq!(decoded.host_peer_id, daemon_peer);
        // Without an iroh runtime the addr is id-only — the daemon
        // is local-only in this test path.
        assert_eq!(decoded.host_addr.peer_id, daemon_peer);
        assert!(decoded.host_addr.relay_url.is_empty());
        assert!(decoded.host_addr.direct_addrs.is_empty());
        let summaries = r.list().await;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, id);
        assert_eq!(summaries[0].peer_count, 1);
        assert_eq!(summaries[0].last_seq, None);
    }

    #[tokio::test]
    async fn host_with_some_id_creates_session_at_that_id() {
        // First-time host with a caller-supplied id and no
        // pre-existing record. The id propagates verbatim and is
        // persisted at that id.
        let r = registry();
        let alice = peer(1, "alice");
        let chosen = SessionId::from_bytes([0xab; 16]);
        let (id, _ticket) = r.host(alice.clone(), Some(chosen)).await.unwrap();
        assert_eq!(id, chosen);
        let summaries = r.list().await;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, chosen);
    }

    #[tokio::test]
    async fn host_with_some_id_resumes_existing_local_session() {
        // Pre-seed a Local-host record (mimicking what daemon
        // restart would rehydrate from disk), then resume via
        // host(peer, Some(id)). Members, log, and head should be
        // preserved verbatim; the ticket should re-stamp from the
        // current daemon_addr.
        let daemon_peer = PeerId::from_bytes([0xff; 32]);
        let alice = peer(1, "alice");
        let bob = peer(2, "bob");
        let session_id = SessionId::from_bytes([0xcd; 16]);

        let store = Arc::new(crate::store::MemoryStore::new());
        let log = vec![SessionMessage::new(
            Seq::new(1),
            42,
            alice.clone(),
            MessageKind::Chat,
            String::from("hello"),
            b"world".to_vec(),
            artel_protocol::message::SIGNATURE_UNSIGNED,
        )];
        let record = SessionRecord {
            id: session_id,
            host: alice.id,
            members: HashSet::from([alice.id, bob.id]),
            head: Seq::new(1),
            log: log.clone(),
            kind: SessionKind::Local,
        };
        store.create(&record).await.unwrap();
        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            store,
            #[cfg(feature = "iroh")]
            None,
            #[cfg(feature = "iroh")]
            None,
        )
        .await
        .unwrap();

        let (id, ticket) = r.host(alice.clone(), Some(session_id)).await.unwrap();
        assert_eq!(id, session_id);

        // Ticket re-stamped with this daemon's current addr.
        let decoded = ticket::decode(ticket.as_str()).unwrap();
        assert_eq!(decoded.session_id, session_id);
        assert_eq!(decoded.host_peer_id, daemon_peer);

        // Resumed session keeps members, log, head verbatim. Replay
        // the log via Subscribe to confirm.
        let sub = r.subscribe(session_id, None).await.unwrap();
        assert_eq!(sub.replay, log);

        let summaries = r.list().await;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].peer_count, 2);
        assert_eq!(summaries[0].last_seq, Some(Seq::new(1)));
    }

    #[tokio::test]
    async fn host_with_some_id_rejects_when_host_differs() {
        // Existing local-host record at `id` belongs to alice; bob
        // tries to resume it. Must reject with SessionConflict and
        // leave the in-memory state alone.
        let daemon_peer = PeerId::from_bytes([0xff; 32]);
        let alice = peer(1, "alice");
        let bob = peer(2, "bob");
        let session_id = SessionId::from_bytes([0xcd; 16]);

        let store = Arc::new(crate::store::MemoryStore::new());
        let record = SessionRecord {
            id: session_id,
            host: alice.id,
            members: HashSet::from([alice.id]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Local,
        };
        store.create(&record).await.unwrap();
        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            store,
            #[cfg(feature = "iroh")]
            None,
            #[cfg(feature = "iroh")]
            None,
        )
        .await
        .unwrap();

        let err = r.host(bob, Some(session_id)).await.unwrap_err();
        assert_eq!(err, SessionError::SessionConflict(session_id));

        // Existing session is still present and still alice's.
        let summaries = r.list().await;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].peer_count, 1);
    }

    #[tokio::test]
    async fn host_with_some_id_rejects_when_kind_is_remote() {
        // Existing record at `id` is a Remote mirror — somebody
        // tries to "host" it locally. Must reject regardless of
        // whether the supplied peer matches the recorded host.
        let daemon_peer = PeerId::from_bytes([0xff; 32]);
        let remote_host = peer(1, "alice");
        let session_id = SessionId::from_bytes([0xcd; 16]);

        let store = Arc::new(crate::store::MemoryStore::new());
        let record = SessionRecord {
            id: session_id,
            host: remote_host.id,
            members: HashSet::from([remote_host.id]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Remote,
        };
        store.create(&record).await.unwrap();
        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            store,
            #[cfg(feature = "iroh")]
            None,
            #[cfg(feature = "iroh")]
            None,
        )
        .await
        .unwrap();

        let err = r.host(remote_host, Some(session_id)).await.unwrap_err();
        assert_eq!(err, SessionError::SessionConflict(session_id));
    }

    // ---- join ----

    #[tokio::test]
    async fn join_artel_ticket_succeeds_and_emits_peer_joined() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, ticket) = r.host(host, None).await.unwrap();

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
        let host_peer_id = PeerId::from_bytes([0xff; 32]);
        let ticket = JoinTicket::from(ticket::encode(&SessionTicket {
            session_id: bogus,
            host_peer_id,
            host_addr: WireEndpointAddr::id_only(host_peer_id),
        }));
        let err = r.join(&ticket, peer(2, "bob")).await.unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    #[tokio::test]
    async fn join_twice_is_idempotent() {
        let r = registry();
        let (id, ticket) = r.host(peer(1, "alice"), None).await.unwrap();
        let (got_first, head_first) = r.join(&ticket, peer(2, "bob")).await.unwrap();
        // Second call must NOT error — it's a no-op for the same
        // authenticated id (auth L1 fix #3, idempotent self-rejoin).
        let (got_second, head_second) = r.join(&ticket, peer(2, "bob")).await.unwrap();
        assert_eq!(got_first, id);
        assert_eq!(got_second, id);
        assert_eq!(head_first, head_second);

        // Bob remains a single member of the session — the
        // idempotent path neither duplicates the entry nor races
        // through the store.
        let bob_id = peer(2, "bob").id;
        let session_arc = {
            let sessions = r.sessions.read().await;
            sessions.get(&id).expect("session exists").clone()
        };
        let (members, bob_count) = {
            let session = session_arc.lock().await;
            let count = session.members.iter().filter(|m| **m == bob_id).count();
            (session.members.clone(), count)
        };
        assert_eq!(bob_count, 1, "members: {members:?}");
    }

    #[tokio::test]
    async fn host_then_self_join_via_same_id_is_idempotent() {
        // Alice is the daemon's own peer (matches `registry()`'s
        // [0xff; 32]), so re-joining via her own ticket is the
        // self-rejoin case.
        let daemon_peer = PeerId::from_bytes([0xff; 32]);
        let r = registry();
        let alice = PeerInfo::new(daemon_peer, "alice");
        let (id, ticket) = r.host(alice.clone(), None).await.unwrap();
        // Same authenticated id rejoining via the host's own ticket.
        let (got, head) = r.join(&ticket, alice.clone()).await.unwrap();
        assert_eq!(got, id);
        assert_eq!(head, None, "no messages yet, head should be None");

        // Membership unchanged: alice is still a single member.
        let session_arc = {
            let sessions = r.sessions.read().await;
            sessions.get(&id).expect("session exists").clone()
        };
        let (members, alice_count) = {
            let session = session_arc.lock().await;
            let count = session
                .members
                .iter()
                .filter(|m| **m == daemon_peer)
                .count();
            (session.members.clone(), count)
        };
        assert_eq!(alice_count, 1, "members: {members:?}");
    }

    // ---- send / sequencing ----

    #[tokio::test]
    async fn send_assigns_strictly_monotonic_seq() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host(host.clone(), None).await.unwrap();

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

        assert!(s1.seq < s2.seq);
        assert!(s2.seq < s3.seq);
        // First real seq is 1 (Seq::ZERO is reserved as "no messages").
        assert_eq!(s1.seq, Seq::new(1));
    }

    #[tokio::test]
    async fn send_by_non_member_errors() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host(host, None).await.unwrap();
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
        let (id, _) = r.host(host.clone(), None).await.unwrap();

        let mut sub = r.subscribe(id, None).await.unwrap();
        let sent = r
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
                assert_eq!(message.seq, sent.seq);
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
        let (id, _) = r.host(host.clone(), None).await.unwrap();

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
        let sub = r.subscribe(id, Some(s1.seq)).await.unwrap();
        let actions: Vec<&str> = sub.replay.iter().map(|m| m.action.as_str()).collect();
        assert_eq!(actions, vec!["2", "3"]);
    }

    #[tokio::test]
    async fn subscribe_with_no_since_replays_full_log() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host(host.clone(), None).await.unwrap();
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
        let (id, ticket) = r.host(host, None).await.unwrap();
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
        let (id, _) = r.host(host.clone(), None).await.unwrap();
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
        let (id, _) = r.host(peer(1, "alice"), None).await.unwrap();
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
        let (id, ticket) = r.host(host.clone(), None).await.unwrap();
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
        r.host(host, None).await.unwrap();
        let summaries = r.list().await;
        assert!(summaries[0].is_host);
    }

    // ---- rehydrate with persisted SessionKind ----

    use crate::store::SessionStore;

    #[tokio::test]
    async fn load_rehydrates_remote_session_with_remote_kind() {
        // Pre-populate a store with a Remote-kind record (the shape
        // a daemon would have on disk after joining a remote
        // session and being restarted), load a registry on top of
        // it, and verify that local Send refuses to assign seqs —
        // i.e. the kind survived the round trip.
        let daemon_peer = PeerId::from_bytes([7; 32]);
        let remote_host = peer(1, "alice");
        let session_id = SessionId::from_bytes([0xaa; 16]);
        let me = peer(2, "bob");

        let store = Arc::new(crate::store::MemoryStore::new());
        let record = SessionRecord {
            id: session_id,
            host: remote_host.id,
            members: HashSet::from([remote_host.id, me.id]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Remote,
        };
        store.create(&record).await.unwrap();

        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            store,
            #[cfg(feature = "iroh")]
            None,
            #[cfg(feature = "iroh")]
            None,
        )
        .await
        .unwrap();

        let err = r
            .send(session_id, me, MessageKind::Chat, "x".into(), vec![], 0)
            .await
            .unwrap_err();
        // Without the iroh feature the registry surfaces NotHost
        // directly. With iroh on but no bridge attached, the
        // Remote-send branch reports the missing bridge as Internal —
        // either way confirms the kind was persisted as Remote
        // (a Local rehydrate would have just appended locally).
        #[cfg(feature = "iroh")]
        assert!(
            matches!(&err, SessionError::Internal(msg) if msg.contains("remote send")),
            "expected internal-no-bridge, got {err:?}",
        );
        #[cfg(not(feature = "iroh"))]
        assert_eq!(err, SessionError::NotHost);
    }

    // ---- host_closed_session (joiner-side mirror teardown) ----

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn host_closed_session_drops_remote_mirror_and_emits_event() {
        // Stand up a Remote-kind mirror by hand (no live bridge —
        // host_closed_session only consults bridge if Some, so a
        // None bridge is the right shape for this unit test).
        let daemon_peer = PeerId::from_bytes([7; 32]);
        let remote_host = peer(1, "alice");
        let session_id = SessionId::from_bytes([0xaa; 16]);
        let me = peer(2, "bob");

        let store = Arc::new(crate::store::MemoryStore::new());
        let record = SessionRecord {
            id: session_id,
            host: remote_host.id,
            members: HashSet::from([remote_host.id, me.id]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Remote,
        };
        store.create(&record).await.unwrap();

        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            store.clone(),
            None,
            None,
        )
        .await
        .unwrap();

        let mut sub = r.subscribe(session_id, None).await.unwrap();

        r.host_closed_session(session_id).await.unwrap();

        let event = timeout(Duration::from_millis(100), sub.events.recv())
            .await
            .expect("event")
            .unwrap();
        assert_eq!(
            event,
            Event::SessionClosed {
                session: session_id
            }
        );
        assert!(r.list().await.is_empty(), "mirror should be gone");
        assert!(
            store.load_all().await.unwrap().is_empty(),
            "persisted record should be deleted",
        );

        // Idempotency: a duplicate close broadcast (or one that
        // races with a manual leave) shouldn't surface as an error.
        r.host_closed_session(session_id).await.unwrap();
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn log_since_returns_only_messages_after_cursor() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host(host.clone(), None).await.unwrap();

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

        // since = ZERO returns the full log.
        let all = r.log_since(id, Seq::ZERO).await.unwrap();
        assert_eq!(all.len(), 3);

        // since = s1 skips the first.
        let after_s1 = r.log_since(id, s1.seq).await.unwrap();
        let actions: Vec<&str> = after_s1.iter().map(|m| m.action.as_str()).collect();
        assert_eq!(actions, vec!["2", "3"]);

        // since past head returns empty.
        let past = r.log_since(id, Seq::new(99)).await.unwrap();
        assert!(past.is_empty());
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn log_since_unknown_session_errors() {
        let r = registry();
        let bogus = SessionId::new_random();
        let err = r.log_since(bogus, Seq::ZERO).await.unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn host_closed_session_ignores_local_session() {
        // Defensive: a misrouted SessionClosed for a Local session
        // shouldn't take it down. The host's own close path is
        // `Registry::leave(session, host_peer)`, not this one.
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host(host, None).await.unwrap();

        r.host_closed_session(id).await.unwrap();

        // Session is still here.
        assert_eq!(r.list().await.len(), 1);
    }

    // ---- attachments ----

    const KIND_V1: &str = "artel-fs/workspace/v1";

    #[tokio::test]
    async fn register_attachment_persists_via_store() {
        let r = registry();
        let alice = peer(1, "alice");
        let (id, _) = r.host(alice, None).await.unwrap();
        r.register_attachment(id, KIND_V1.into(), b"payload".to_vec())
            .await
            .unwrap();
        let listed = r.list_attachments(None).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].session, id);
        assert_eq!(listed[0].kind, KIND_V1);
        assert_eq!(listed[0].payload, b"payload");
    }

    #[tokio::test]
    async fn register_attachment_for_unknown_session_returns_unknown_session_error() {
        let r = registry();
        let bogus = SessionId::new_random();
        let err = r
            .register_attachment(bogus, KIND_V1.into(), b"x".to_vec())
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    #[tokio::test]
    async fn list_attachments_returns_entries_across_multiple_sessions() {
        let r = registry();
        let (id1, _) = r.host(peer(1, "alice"), None).await.unwrap();
        let (id2, ticket2) = r.host(peer(2, "bob"), None).await.unwrap();
        let _ = ticket2;
        r.register_attachment(id1, KIND_V1.into(), b"one".to_vec())
            .await
            .unwrap();
        r.register_attachment(id2, KIND_V1.into(), b"two".to_vec())
            .await
            .unwrap();

        let mut listed = r.list_attachments(None).await.unwrap();
        listed.sort_by_key(|s| s.session);
        let mut want = vec![id1, id2];
        want.sort();
        assert_eq!(listed.iter().map(|s| s.session).collect::<Vec<_>>(), want);
    }

    #[tokio::test]
    async fn forget_attachment_removes_entry() {
        let r = registry();
        let (id, _) = r.host(peer(1, "alice"), None).await.unwrap();
        r.register_attachment(id, KIND_V1.into(), b"x".to_vec())
            .await
            .unwrap();
        r.forget_attachment(id, KIND_V1.into()).await.unwrap();
        assert!(r.list_attachments(None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn cascade_removes_attachments_when_host_leaves() {
        let r = registry();
        let alice = peer(1, "alice");
        let (id, _) = r.host(alice.clone(), None).await.unwrap();
        r.register_attachment(id, KIND_V1.into(), b"x".to_vec())
            .await
            .unwrap();

        r.leave(id, alice.id).await.unwrap();

        // Session is gone and so is the attachment — list_attachments
        // returns empty rather than a dangling entry.
        assert!(r.list_attachments(None).await.unwrap().is_empty());
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn cascade_removes_attachments_when_remote_session_closes() {
        let daemon_peer = PeerId::from_bytes([7; 32]);
        let remote_host = peer(1, "alice");
        let session_id = SessionId::from_bytes([0xaa; 16]);
        let me = peer(2, "bob");

        let store = Arc::new(crate::store::MemoryStore::new());
        let record = SessionRecord {
            id: session_id,
            host: remote_host.id,
            members: HashSet::from([remote_host.id, me.id]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Remote,
        };
        store.create(&record).await.unwrap();
        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            store.clone(),
            None,
            None,
        )
        .await
        .unwrap();

        r.register_attachment(session_id, KIND_V1.into(), b"x".to_vec())
            .await
            .unwrap();
        r.host_closed_session(session_id).await.unwrap();

        assert!(r.list_attachments(None).await.unwrap().is_empty());
    }

    /// A joiner leaving a `Remote` mirror fully drops the mirror —
    /// store record gone, in-memory entry gone, attachment cascaded,
    /// `Event::SessionClosed` emitted. Symmetric with
    /// `host_closed_session`'s teardown but triggered by a local IPC
    /// leave instead of a gossip `SessionClosed` from the host.
    #[tokio::test]
    async fn joiner_leave_remote_drops_mirror_and_cascades_attachment() {
        let daemon_peer = PeerId::from_bytes([7; 32]);
        let remote_host = peer(1, "alice");
        let session_id = SessionId::from_bytes([0xaa; 16]);
        let me = peer(2, "bob");

        let store = Arc::new(crate::store::MemoryStore::new());
        let record = SessionRecord {
            id: session_id,
            host: remote_host.id,
            members: HashSet::from([remote_host.id, me.id]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Remote,
        };
        store.create(&record).await.unwrap();
        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            store.clone(),
            None,
            None,
        )
        .await
        .unwrap();

        r.register_attachment(session_id, KIND_V1.into(), b"x".to_vec())
            .await
            .unwrap();
        let mut sub = r.subscribe(session_id, None).await.unwrap();

        r.leave(session_id, me.id).await.unwrap();

        let event = timeout(Duration::from_millis(100), sub.events.recv())
            .await
            .expect("event")
            .unwrap();
        assert_eq!(
            event,
            Event::SessionClosed {
                session: session_id,
            }
        );
        assert!(r.list().await.is_empty(), "mirror should be gone");
        assert!(
            store.load_all().await.unwrap().is_empty(),
            "persisted record should be deleted",
        );
        assert!(
            r.list_attachments(None).await.unwrap().is_empty(),
            "attachment should cascade-delete with the mirror",
        );
    }

    /// Joiner of a `Local` session leaving (i.e. another peer left
    /// our hosted session) keeps the session alive — the host and
    /// any other members are still in it. Just an unmember +
    /// `Event::PeerLeft`.
    #[tokio::test]
    async fn joiner_leave_local_session_keeps_session_alive() {
        let r = registry();
        let host = peer(1, "alice");
        let bob = peer(2, "bob");
        let charlie = peer(3, "charlie");
        let (id, ticket) = r.host(host.clone(), None).await.unwrap();
        r.join(&ticket, bob.clone()).await.unwrap();
        r.join(&ticket, charlie.clone()).await.unwrap();

        // Bob (a joiner of our Local session) leaves.
        r.leave(id, bob.id).await.unwrap();

        // Session still alive: alice + charlie remain.
        let summaries = r.list().await;
        assert_eq!(summaries.len(), 1, "session must persist");
        assert_eq!(summaries[0].peer_count, 2);
    }

    /// Race-regression: `register_attachment` vs. `leave` on the same
    /// session. Without the per-session lock in `register_attachment`
    /// + the matching lock in `leave`'s critical section, the put
    /// could land *after* the cascade ran, orphaning the attachment.
    ///
    /// Drives the race deterministically: spawn a register task and
    /// a leave task, await both, then assert the cascade contract:
    /// either register won (attachment present, session still there)
    /// or leave won (no session, no attachment) — never both.
    /// Loops to give the scheduler many chances to interleave; any
    /// orphan is a hard failure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn race_register_attachment_vs_leave_does_not_orphan() {
        for _ in 0..200 {
            let r = Arc::new(registry());
            let alice = peer(1, "alice");
            let (id, _) = r.host(alice.clone(), None).await.unwrap();

            let r1 = Arc::clone(&r);
            let r2 = Arc::clone(&r);
            let alice_id = alice.id;
            let register = tokio::spawn(async move {
                r1.register_attachment(id, KIND_V1.into(), b"x".to_vec())
                    .await
            });
            let leave = tokio::spawn(async move { r2.leave(id, alice_id).await });

            // Both tasks must complete (one will race-win, the other
            // may see UnknownSession or the cascade already-ran path).
            let _ = register.await.unwrap();
            let _ = leave.await.unwrap();

            // Cascade contract: if the session is gone, no attachment
            // for it may survive in the store.
            let listed = r.list_attachments(None).await.unwrap();
            for entry in &listed {
                assert_eq!(
                    entry.session, id,
                    "stray attachment for unknown session: {entry:?}"
                );
            }
            // The session may or may not still exist depending on
            // which task observably won. If it doesn't, list_attachments
            // must be empty.
            let session_present = r.list().await.iter().any(|s| s.id == id);
            if !session_present {
                assert!(
                    listed.is_empty(),
                    "session gone but attachment leaked: {listed:?}",
                );
            }
        }
    }
}
