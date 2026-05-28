//! Bridge between [`crate::session::Registry`] and the iroh gossip
//! substrate.
//!
//! The bridge owns nothing the registry does — it doesn't store
//! sessions, doesn't sequence messages. Its only job is to plumb
//! bytes between two places:
//!
//! - **Host side.** When [`crate::session::Registry::send`] commits
//!   a [`SessionMessage`], the bridge wraps it in a
//!   [`GossipBody::Message`] and broadcasts it on the session's
//!   gossip topic. Inbound [`GossipBody::SendRequest`] frames from
//!   joiners get routed back into [`crate::session::Registry::send`]
//!   on the host's behalf, and the result published as a
//!   [`GossipBody::SendAck`].
//! - **Joiner side.** When the registry detects a remote ticket
//!   (`host_peer_id` ≠ self), the bridge subscribes to the topic,
//!   spawns a forwarder task that decodes inbound frames and feeds
//!   them into the local `Session`'s `events_tx` and `log` so the
//!   joiner's IPC subscribers see the host's messages. Outbound
//!   IPC `Send` calls go through [`GossipBridge::send_remote`],
//!   which publishes a [`GossipBody::SendRequest`] and awaits the
//!   matching [`GossipBody::SendAck`] on a `pending_sends` map.
//!
//! All inter-daemon traffic — both directions — rides the same
//! gossip topic so the protocol stays symmetric. ADR-001's "future
//! evolution" toward a sequencer-less P2P model only has to change
//! the protocol, not the transport.

#![allow(clippy::redundant_pub_crate)]
//!
//! ## Topic id
//!
//! Derived deterministically from the session id — the first 16
//! bytes of the topic's 32-byte id are the session UUID, the last
//! 16 bytes are zeros. That keeps the ticket compact (no extra
//! topic field) and means a session that gets re-hosted from the
//! same id lands on the same topic by construction.

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use artel_protocol::gossip::{self, GossipBody};
use artel_protocol::rpc::SendPayload;
use artel_protocol::{PeerId, PeerInfo, ProtocolError, Seq, SessionId, SessionMessage};
use bytes::Bytes;
use futures_util::StreamExt;
use iroh_gossip::api::Event as IrohGossipEvent;
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use tokio::sync::{Mutex, oneshot};
use tokio::time::timeout;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::session::{Registry, SessionError};

/// Bounded wait for the gossip mesh to form on the joiner side. If
/// no `NeighborUp` arrives in this window the join fails — a longer
/// fallback would just hide a real connectivity problem.
const JOIN_READY_TIMEOUT: Duration = Duration::from_secs(15);

/// Bounded wait for the host's `SendAck` reply to a joiner-initiated
/// send. One gossip RTT in the common case; the ceiling is a
/// fail-loud upper bound for connectivity drops mid-request.
const SEND_REMOTE_TIMEOUT: Duration = Duration::from_secs(10);

/// Inter-daemon plumbing. One instance per daemon, shared across
/// every session it joins or hosts.
#[derive(Debug)]
pub(crate) struct GossipBridge {
    gossip: Gossip,
    /// Live topic handles, keyed by session id. We keep the sender
    /// alive for the lifetime of the session so broadcasts work; the
    /// receiver is owned by a forwarder task whose `JoinHandle` lives
    /// here too.
    sessions: Mutex<HashMap<SessionId, SessionState>>,
    /// In-flight joiner-side sends keyed by their `req_id`. The
    /// forwarder resolves the matching oneshot when a `SendAck`
    /// arrives. The map is shared across sessions because `req_id`
    /// is globally unique (Uuid v4).
    pending_sends: Mutex<HashMap<Uuid, oneshot::Sender<Result<SessionMessage, ProtocolError>>>>,
    /// Back-reference to the Registry, set once via [`Self::attach_registry`]
    /// after the registry is wrapped in [`Arc`]. Held as a `Weak` to
    /// avoid an Arc cycle: the registry already owns an `Arc<Self>`.
    /// The host-side forwarder upgrades it on every inbound
    /// `SendRequest`; if the registry has been dropped, the request
    /// is silently ignored (the daemon is shutting down).
    registry: Mutex<Weak<Registry>>,
}

#[derive(Debug)]
struct SessionState {
    sender: iroh_gossip::api::GossipSender,
    forwarder: tokio::task::JoinHandle<()>,
}

/// Map a [`SessionId`] to the gossip [`TopicId`] used for its traffic.
fn topic_for(session: SessionId) -> TopicId {
    let mut bytes = [0u8; 32];
    bytes[..16].copy_from_slice(session.as_bytes());
    TopicId::from_bytes(bytes)
}

/// Per-session role on this daemon. Drives which inbound gossip
/// frames the forwarder dispatches to which handler.
enum SessionRole {
    /// We host this session locally. Inbound `SendRequest` frames
    /// route into `Registry::send` via the bridge's back-reference;
    /// `Message` and `SendAck` frames are ignored (the host's IPC
    /// subscribers are served by `Registry`'s local broadcast).
    Host,
    /// We mirror a session whose authoritative log lives on
    /// another daemon. Inbound `Message` frames push into
    /// `on_message`; `SendAck` frames are looked up against
    /// `pending_sends`; `SendRequest` frames are ignored (only
    /// the host services them).
    Joiner { on_message: MessageHandler },
}

impl GossipBridge {
    /// Construct a bridge wrapping `gossip`. The daemon's
    /// [`iroh::Endpoint`] does its own discovery via the configured
    /// [`crate::EndpointSetup`] — this used to take a
    /// `MemoryLookup` the bridge would seed on join, but
    /// `EndpointSetup::Testing`'s [`iroh::test_utils::DnsPkarrServer`]
    /// publishes the host's addr automatically and
    /// `EndpointSetup::Production` resolves it from n0's DNS, so
    /// the bridge no longer needs an addr-book back-channel.
    pub(crate) fn new(gossip: Gossip) -> Self {
        Self {
            gossip,
            sessions: Mutex::new(HashMap::new()),
            pending_sends: Mutex::new(HashMap::new()),
            registry: Mutex::new(Weak::new()),
        }
    }

    /// Inject the back-reference to the [`Registry`] this bridge
    /// serves. Called once at daemon startup, after the registry is
    /// wrapped in an [`Arc`]. Held as a [`Weak`] so the bridge
    /// doesn't keep the registry alive past shutdown.
    pub(crate) async fn attach_registry(&self, registry: Weak<Registry>) {
        *self.registry.lock().await = registry;
    }

    /// Subscribe to a session's gossip topic as **host**. No
    /// bootstrap peers — we wait for joiners to find us. Returns
    /// once the topic handle is registered; broadcasts via
    /// [`Self::publish_message`] start working immediately, although
    /// they reach no-one until at least one joiner connects.
    pub(crate) async fn host_session(
        self: &Arc<Self>,
        session: SessionId,
    ) -> Result<(), BridgeError> {
        self.subscribe_inner(session, vec![], SessionRole::Host)
            .await
    }

    /// Subscribe to a session's gossip topic as **joiner**.
    /// Subscribes with `host_peer` as the bootstrap and lets iroh's
    /// configured discovery (n0 DNS in production, the test
    /// `DnsPkarrServer` under [`crate::EndpointSetup::Testing`])
    /// resolve the host's `EndpointAddr` on demand. Spawns a
    /// forwarder task that decodes inbound gossip frames and pushes
    /// the resulting [`SessionMessage`]s into `on_message`.
    ///
    /// Once the gossip mesh is up, broadcasts a
    /// [`GossipBody::JoinAnnouncement`] carrying `joiner` so the
    /// host can admit the peer to membership and emit
    /// [`Event::PeerJoined`] proactively — without this, the
    /// joiner stays invisible until their first `SendRequest`.
    ///
    /// [`Event::PeerJoined`]: artel_protocol::Event::PeerJoined
    pub(crate) async fn join_session(
        self: &Arc<Self>,
        session: SessionId,
        joiner: PeerInfo,
        host_peer: PeerId,
        on_message: impl Fn(SessionMessage) + Send + Sync + 'static,
    ) -> Result<(), BridgeError> {
        let host_endpoint_id = iroh::EndpointId::from_bytes(host_peer.as_bytes())
            .map_err(|e| BridgeError::Iroh(format!("bad host peer id: {e}")))?;
        self.subscribe_inner(
            session,
            vec![host_endpoint_id],
            SessionRole::Joiner {
                on_message: Arc::new(on_message),
            },
        )
        .await?;
        // Mesh is up (subscribe_inner waits for `joined()` on the
        // joiner path). Announce ourselves so the host's bridge can
        // call `Registry::ensure_member` and emit `PeerJoined`.
        // Best-effort: a failure here just degrades to lazy
        // admission on first `SendRequest`, the same shape we had
        // pre-2c-2d.
        self.publish_join_announcement(session, joiner).await;
        // Ask the host to backfill any committed messages we
        // missed before the mesh was up. Since this is a fresh
        // mirror, we have nothing — `since: ZERO` requests the
        // full log. Best-effort: a failure here just means the
        // joiner's mirror starts empty, same shape as pre-replay.
        self.publish_replay(session, Seq::ZERO).await;
        Ok(())
    }

    /// Tear down all per-session topic state for `session`. Called
    /// from [`Registry::leave`] (host or joiner) so the forwarder
    /// task exits, the gossip topic is left, and `bridge.sessions`
    /// doesn't grow unbounded as sessions come and go.
    ///
    /// Safe to call for a session the bridge never knew about
    /// (e.g. a daemon built without iroh) — no-op in that case.
    /// Per iroh-gossip docs, the topic is left once both the
    /// `GossipSender` and `GossipReceiver` halves are dropped; we
    /// own the sender here and abort the forwarder task to drop
    /// the receiver.
    pub(crate) async fn forget_session(&self, session: SessionId) {
        let removed = self.sessions.lock().await.remove(&session);
        let Some(state) = removed else {
            debug!(?session, "forget_session: bridge had no entry");
            return;
        };
        // Abort first so the forwarder stops processing inbound
        // frames immediately — the receiver inside it is dropped
        // when the task unwinds. Then drop the sender so
        // iroh-gossip leaves the topic.
        state.forwarder.abort();
        drop(state.sender);
        // pending_sends entries that were keyed to this session
        // (joiner-side outbound `Send` waiting for ack) will
        // resolve to SendTimeout on their own deadline. Not worth
        // hunting them down here — the map is keyed by req_id, not
        // session_id, so we'd be doing a linear scan for an event
        // that's already self-cleaning.
    }

    /// Joiner-side `Send`: publish a [`GossipBody::SendRequest`] on
    /// the session's topic and await the matching `SendAck`. The
    /// host's response carries the assigned [`SessionMessage`] (on
    /// success) or a [`ProtocolError`] (on rejection). Returns a
    /// [`BridgeError::SendTimeout`] if no ack arrives within
    /// [`SEND_REMOTE_TIMEOUT`].
    pub(crate) async fn send_remote(
        &self,
        session: SessionId,
        peer: PeerInfo,
        payload: SendPayload,
    ) -> Result<SessionMessage, BridgeError> {
        let sender = self
            .sessions
            .lock()
            .await
            .get(&session)
            .map(|s| s.sender.clone())
            .ok_or(BridgeError::UnknownSession(session))?;

        let req_id = Uuid::new_v4();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.pending_sends.lock().await.insert(req_id, reply_tx);

        let body = GossipBody::SendRequest {
            req_id,
            peer,
            payload,
        };
        let bytes = Bytes::from(gossip::encode(&body));
        if let Err(err) = sender.broadcast(bytes).await {
            // Drop the pending entry so it doesn't leak; nobody is
            // ever going to resolve it.
            self.pending_sends.lock().await.remove(&req_id);
            return Err(BridgeError::Iroh(format!("send_remote broadcast: {err}")));
        }

        match timeout(SEND_REMOTE_TIMEOUT, reply_rx).await {
            Ok(Ok(Ok(message))) => Ok(message),
            Ok(Ok(Err(err))) => Err(BridgeError::HostRejected(err)),
            Ok(Err(_canceled)) => {
                // Sender dropped without resolving — bridge tearing
                // down. Surface as a generic failure.
                Err(BridgeError::Iroh(
                    "send_remote: pending entry dropped without ack".into(),
                ))
            }
            Err(_elapsed) => {
                self.pending_sends.lock().await.remove(&req_id);
                Err(BridgeError::SendTimeout)
            }
        }
    }

    /// Common subscribe path.
    ///
    /// Joiners (those with a non-empty bootstrap) additionally wait
    /// for [`GossipReceiver::joined`] before returning so the gossip
    /// mesh is actually wired up by the time `JoinSession` reports
    /// success. Without this, a host that calls `publish_message`
    /// immediately after the joiner's IPC handshake completes can
    /// broadcast into a topic that has no neighbors yet, and the
    /// joiner silently misses the message.
    ///
    /// [`GossipReceiver::joined`]: iroh_gossip::api::GossipReceiver::joined
    async fn subscribe_inner(
        self: &Arc<Self>,
        session: SessionId,
        bootstrap: Vec<iroh::EndpointId>,
        role: SessionRole,
    ) -> Result<(), BridgeError> {
        // Idempotency: a same-process resume re-calls `host_session`
        // for an id we're already subscribed to. The existing
        // `SessionState` (sender + forwarder) is fine; tearing it
        // down and re-subscribing would briefly drop the topic.
        if self.sessions.lock().await.contains_key(&session) {
            return Ok(());
        }
        let topic_id = topic_for(session);
        let wait_for_neighbor = !bootstrap.is_empty();
        let topic = self
            .gossip
            .subscribe(topic_id, bootstrap)
            .await
            .map_err(|e| BridgeError::Iroh(format!("subscribe: {e}")))?;
        let (sender, mut receiver) = topic.split();
        if wait_for_neighbor {
            timeout(JOIN_READY_TIMEOUT, receiver.joined())
                .await
                .map_err(|_| BridgeError::Iroh("timed out waiting for gossip neighbor".into()))?
                .map_err(|e| BridgeError::Iroh(format!("joined: {e}")))?;
        }
        let bridge = Arc::clone(self);
        let session_for_log = session;
        let forwarder = tokio::spawn(async move {
            while let Some(item) = receiver.next().await {
                match item {
                    Ok(IrohGossipEvent::Received(msg)) => match gossip::decode(&msg.content) {
                        Ok(body) => {
                            handle_inbound_frame(&bridge, session_for_log, &role, body).await;
                        }
                        Err(err) => {
                            warn!(?err, "gossip frame decode failed; dropping");
                        }
                    },
                    Ok(_) => {} // NeighborUp/Down/Lagged: nothing to forward.
                    Err(err) => {
                        warn!(error = %err, "gossip receiver error; forwarder exiting");
                        break;
                    }
                }
            }
            debug!(?session_for_log, "gossip forwarder exited");
        });
        self.sessions
            .lock()
            .await
            .insert(session, SessionState { sender, forwarder });
        Ok(())
    }

    /// Broadcast `msg` on the gossip topic for `session`. Called by
    /// the host side of [`Registry::send`] after a successful
    /// store-write. No-op (with a debug log) if the bridge wasn't
    /// hosting this session — the local fan-out has already
    /// happened, this is just bonus reach.
    pub(crate) async fn publish_message(&self, session: SessionId, msg: SessionMessage) {
        let sender = self
            .sessions
            .lock()
            .await
            .get(&session)
            .map(|s| s.sender.clone());
        let Some(sender) = sender else {
            debug!(
                ?session,
                "publish_message called for session without gossip topic; dropping",
            );
            return;
        };
        let bytes = Bytes::from(gossip::encode(&GossipBody::Message(msg)));
        if let Err(err) = sender.broadcast(bytes).await {
            warn!(error = %err, "gossip broadcast failed");
        }
    }

    /// Broadcast a [`GossipBody::Replay`] asking the host to
    /// re-emit every committed message with `seq > since`. Called
    /// from [`Self::join_session`] right after the announcement.
    /// Best-effort: a failed broadcast just means the joiner's
    /// mirror starts empty rather than backfilled.
    async fn publish_replay(&self, session: SessionId, since: Seq) {
        let sender = self
            .sessions
            .lock()
            .await
            .get(&session)
            .map(|s| s.sender.clone());
        let Some(sender) = sender else {
            debug!(
                ?session,
                "publish_replay: session has no gossip topic; dropping",
            );
            return;
        };
        let bytes = Bytes::from(gossip::encode(&GossipBody::Replay { since }));
        if let Err(err) = sender.broadcast(bytes).await {
            warn!(error = %err, "replay broadcast failed");
        }
    }

    /// Broadcast a [`GossipBody::SessionClosed`] on the topic for
    /// `session`. Called from the host side of [`Registry::leave`]
    /// just before [`Self::forget_session`] tears the topic down,
    /// so joiners' forwarders see the close before the gossip
    /// neighbor goes silent. Best-effort — if the broadcast fails,
    /// joiners fall back to discovering the close via their next
    /// `SendRequest` timing out.
    pub(crate) async fn publish_session_closed(&self, session: SessionId) {
        let sender = self
            .sessions
            .lock()
            .await
            .get(&session)
            .map(|s| s.sender.clone());
        let Some(sender) = sender else {
            debug!(
                ?session,
                "publish_session_closed: session has no gossip topic; dropping",
            );
            return;
        };
        let bytes = Bytes::from(gossip::encode(&GossipBody::SessionClosed));
        if let Err(err) = sender.broadcast(bytes).await {
            warn!(error = %err, "session_closed broadcast failed");
        }
    }

    /// Broadcast a [`GossipBody::JoinAnnouncement`] on the topic
    /// for `session`. Called from [`Self::join_session`] once the
    /// mesh is up. Best-effort — a failed broadcast falls back to
    /// the lazy-admission path (the host learns about us on our
    /// first `SendRequest`).
    async fn publish_join_announcement(&self, session: SessionId, peer: PeerInfo) {
        let sender = self
            .sessions
            .lock()
            .await
            .get(&session)
            .map(|s| s.sender.clone());
        let Some(sender) = sender else {
            debug!(
                ?session,
                "publish_join_announcement: session has no gossip topic; dropping",
            );
            return;
        };
        let body = GossipBody::JoinAnnouncement {
            peer,
            timestamp_ms: now_ms(),
        };
        let bytes = Bytes::from(gossip::encode(&body));
        if let Err(err) = sender.broadcast(bytes).await {
            warn!(error = %err, "join_announcement broadcast failed");
        }
    }

    /// Broadcast a [`GossipBody::SendAck`] on the topic for
    /// `session`. Called from the host's inbound forwarder after
    /// driving `Registry::send` for a joiner-issued request.
    async fn publish_send_ack(
        &self,
        session: SessionId,
        req_id: Uuid,
        result: Result<SessionMessage, ProtocolError>,
    ) {
        let sender = self
            .sessions
            .lock()
            .await
            .get(&session)
            .map(|s| s.sender.clone());
        let Some(sender) = sender else {
            debug!(
                ?session,
                "publish_send_ack: session has no gossip topic; dropping ack",
            );
            return;
        };
        let bytes = Bytes::from(gossip::encode(&GossipBody::SendAck { req_id, result }));
        if let Err(err) = sender.broadcast(bytes).await {
            warn!(error = %err, "send_ack broadcast failed");
        }
    }
}

/// Dispatch an inbound gossip frame body for `session` based on the
/// local role. Lives outside the impl so the spawned forwarder task
/// can call it without re-borrowing `self`.
async fn handle_inbound_frame(
    bridge: &Arc<GossipBridge>,
    session: SessionId,
    role: &SessionRole,
    body: GossipBody,
) {
    match (role, body) {
        // Host receives a joiner's send request — drive Registry::send,
        // broadcast the SendAck.
        (
            SessionRole::Host,
            GossipBody::SendRequest {
                req_id,
                peer,
                payload,
            },
        ) => {
            let result = run_host_send(bridge, session, peer, payload).await;
            bridge.publish_send_ack(session, req_id, result).await;
        }
        // Host receives a joiner's mesh-up announcement — admit them
        // to the session's membership eagerly so `PeerJoined` lands
        // on local IPC subscribers without waiting for a SendRequest.
        // `ensure_member` is idempotent, so a duplicate announcement
        // (or one that races with the lazy-admission path inside
        // run_host_send) is harmless.
        (
            SessionRole::Host,
            GossipBody::JoinAnnouncement {
                peer,
                timestamp_ms: _,
            },
        ) => {
            let registry_weak = bridge.registry.lock().await.clone();
            if let Some(registry) = registry_weak.upgrade()
                && let Err(err) = registry.ensure_member(session, peer).await
            {
                warn!(?err, "join_announcement: ensure_member failed");
            }
        }
        // Joiner receives the host's "I am closing this session"
        // broadcast — drop the local mirror, emit
        // `Event::SessionClosed`, and tear down our own bridge
        // entry. Idempotent on the registry side, so a stray
        // duplicate frame is harmless.
        (SessionRole::Joiner { .. }, GossipBody::SessionClosed) => {
            let registry_weak = bridge.registry.lock().await.clone();
            if let Some(registry) = registry_weak.upgrade()
                && let Err(err) = registry.host_closed_session(session).await
            {
                warn!(?err, "session_closed: host_closed_session failed");
            }
        }
        // Host receives a joiner's `Replay` request — fetch the
        // log entries with seq > since and re-broadcast each as
        // a `Message` frame. Other joiners on the topic see the
        // replay traffic too and dedup-skip it; that's wasteful
        // but correct (see `GossipBody::Replay` doc-comment).
        (SessionRole::Host, GossipBody::Replay { since }) => {
            run_host_replay(bridge, session, since).await;
        }
        // Host ignores its own Message+SendAck broadcasts (Registry
        // already fanned out locally) and its own SessionClosed
        // (we publish before forget_session, so the broadcast
        // round-trips back here on the same forwarder); joiners
        // ignore each other's SendRequests + JoinAnnouncements
        // + Replays (only the host services them at this layer).
        (
            SessionRole::Host,
            GossipBody::Message(_) | GossipBody::SendAck { .. } | GossipBody::SessionClosed,
        )
        | (
            SessionRole::Joiner { .. },
            GossipBody::SendRequest { .. }
            | GossipBody::JoinAnnouncement { .. }
            | GossipBody::Replay { .. },
        ) => {}

        // Joiner receives the host's broadcast — push into the local
        // mirror's events_tx + log.
        (SessionRole::Joiner { on_message }, GossipBody::Message(m)) => on_message(m),
        // Joiner receives the host's reply to one of our outbound
        // sends — resolve the matching pending oneshot.
        (SessionRole::Joiner { .. }, GossipBody::SendAck { req_id, result }) => {
            let pending = bridge.pending_sends.lock().await.remove(&req_id);
            if let Some(tx) = pending {
                let _ = tx.send(result);
            } else {
                debug!(?req_id, "SendAck for unknown req_id; dropping");
            }
        }
    }
}

/// Run the host-side `Registry::send` corresponding to an inbound
/// `SendRequest`. Translates [`SessionError`] into the wire form
/// the joiner expects to see in `SendAck.result`.
async fn run_host_send(
    bridge: &Arc<GossipBridge>,
    session: SessionId,
    peer: PeerInfo,
    payload: SendPayload,
) -> Result<SessionMessage, ProtocolError> {
    let registry_weak = bridge.registry.lock().await.clone();
    let Some(registry) = registry_weak.upgrade() else {
        // Daemon shutting down; nothing useful to say.
        return Err(ProtocolError::Internal("daemon shutting down".into()));
    };
    // Idempotent backstop: in the common case the joiner's
    // `JoinAnnouncement` already drove ensure_member, but if the
    // announcement broadcast was lost (or this SendRequest beat it
    // through the mesh) we want to admit the peer here too. No-op
    // when membership is already in place.
    if let Err(err) = registry.ensure_member(session, peer.clone()).await {
        return Err(session_error_to_wire(&err));
    }
    let SendPayload {
        kind,
        action,
        payload,
    } = payload;
    match registry
        .send(session, peer, kind, action, payload, now_ms())
        .await
    {
        Ok(message) => Ok(message),
        Err(err) => Err(session_error_to_wire(&err)),
    }
}

/// Run the host-side reply to an inbound `Replay`. Snapshots the
/// session's log via `Registry::log_since` and re-broadcasts each
/// entry as a `Message` frame on the topic. Best-effort: any
/// failure (registry gone, session unknown, broadcast hiccup) is
/// logged at warn and the joiner falls back to whatever it
/// already has.
async fn run_host_replay(bridge: &Arc<GossipBridge>, session: SessionId, since: Seq) {
    let registry_weak = bridge.registry.lock().await.clone();
    let Some(registry) = registry_weak.upgrade() else {
        return;
    };
    let messages = match registry.log_since(session, since).await {
        Ok(msgs) => msgs,
        Err(err) => {
            warn!(?err, "replay: log_since failed");
            return;
        }
    };
    for msg in messages {
        // Reuse the host's existing publish_message path so the
        // wire shape is identical to live broadcasts. The joiner's
        // mirror dedups by seq, so this is safe even if the
        // joiner already has some of these entries.
        bridge.publish_message(session, msg).await;
    }
}

/// Wall-clock millis since the Unix epoch. The host stamps its own
/// clock on messages (joiners trust the host as sequencer here).
fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Mirror of `server::session_error_to_protocol` — same translation
/// rules, but living next to the bridge to avoid a circular
/// `pub(crate)` dance with the server module. Keep in sync if the
/// other one grows variants.
fn session_error_to_wire(err: &SessionError) -> ProtocolError {
    match err {
        SessionError::UnknownSession(s) => ProtocolError::UnknownSession(*s),
        SessionError::NotMember(_) => ProtocolError::Internal("not a member".into()),
        SessionError::AlreadyJoined(s) => ProtocolError::AlreadyJoined(*s),
        SessionError::InvalidTicket => ProtocolError::InvalidTicket,
        SessionError::Storage(io_err) => ProtocolError::Internal(format!("storage: {io_err}")),
        SessionError::InvalidAddr(msg) => ProtocolError::Internal(format!("invalid addr: {msg}")),
        SessionError::Internal(msg) => ProtocolError::Internal(msg.clone()),
        SessionError::NotHost => ProtocolError::NotHost,
        SessionError::SessionConflict(s) => ProtocolError::SessionConflict(*s),
        // Should never occur on the host side — only joiners
        // receive HostRejected from `send_remote`. Surface
        // defensively so a future bug doesn't silently swallow it.
        SessionError::HostRejected(err) => err.clone(),
    }
}

type MessageHandler = Arc<dyn Fn(SessionMessage) + Send + Sync + 'static>;

/// Errors the bridge surfaces. Distinct from
/// [`crate::session::SessionError`] so the registry can decide how
/// to fold them in.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BridgeError {
    /// Underlying iroh / gossip plumbing failure. The string is for
    /// diagnostics only; the registry maps this to
    /// [`ProtocolError::Internal`] so iroh detail doesn't leak to
    /// clients.
    #[error("iroh: {0}")]
    Iroh(String),

    /// `send_remote` was called for a session this bridge has no
    /// gossip topic for. Indicates a logic bug in the registry —
    /// remote sends should only be issued after `join_session`
    /// succeeded.
    #[error("no gossip topic registered for session {0}")]
    UnknownSession(SessionId),

    /// `send_remote` got no `SendAck` from the host within
    /// [`SEND_REMOTE_TIMEOUT`]. Surfaces to the joiner's IPC
    /// client as a transport timeout.
    #[error("timed out waiting for host send_ack")]
    SendTimeout,

    /// The host accepted the request and explicitly rejected it
    /// (e.g., `SessionClosed`, `Storage` error). The joiner forwards
    /// this exact error back through its IPC reply.
    #[error("host rejected send: {0}")]
    HostRejected(#[source] ProtocolError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use artel_protocol::SessionId;

    #[test]
    fn topic_id_is_derived_from_session_uuid() {
        let session = SessionId::from_bytes([0xab; 16]);
        let topic = topic_for(session);
        let bytes = topic.as_bytes();
        assert_eq!(&bytes[..16], session.as_bytes());
        assert!(
            bytes[16..].iter().all(|b| *b == 0),
            "tail must be zeros, got {:?}",
            &bytes[16..],
        );
    }

    #[test]
    fn distinct_sessions_get_distinct_topics() {
        let a = topic_for(SessionId::from_bytes([1; 16]));
        let b = topic_for(SessionId::from_bytes([2; 16]));
        assert_ne!(a.as_bytes(), b.as_bytes());
    }
}
