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
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use artel_protocol::gossip::{self, GossipBody};
use artel_protocol::rpc::{SendPayload, SignedSendPayload};
use artel_protocol::ticket::WireEndpointAddr;
use artel_protocol::{
    Capability, PeerId, PeerInfo, ProtocolError, Seq, SessionId, SessionMessage, SigBytes, TicketId,
};
use bytes::Bytes;
use futures_util::StreamExt;
use iroh::EndpointAddr;
use iroh::address_lookup::memory::MemoryLookup;
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

/// Verify that the application-level `peer.id` carried inside a
/// gossip-frame body matches the gossip-authenticated
/// `delivered_from` for that frame. The body field is shipped by the
/// sender; `delivered_from` is signed by the iroh transport and
/// trustworthy. A mismatch is the L1 spoofed-authorship /
/// ghost-membership attack class; we drop the frame at the bridge
/// with a warn log.
///
/// This is a byte equality check, not a key validation: it relies on
/// the invariant `iroh::EndpointId == 32 bytes that ARE an Ed25519
/// public key` (the same shape `PeerId` carries). If iroh ever
/// changes that primitive, the assumption needs revisiting.
fn peer_id_matches_delivered_from(body_peer_id: PeerId, delivered_from: iroh::EndpointId) -> bool {
    body_peer_id.as_bytes() == delivered_from.as_bytes()
}

/// Returns `true` (and logs a warn) when `peer_id` does NOT match
/// `delivered_from`, signalling the host arm should drop the frame.
/// Common shape for the two body-`peer`-carrying host arms
/// (`SendRequest`, `JoinAnnouncement`).
fn drop_if_spoofed(session: SessionId, peer_id: PeerId, delivered_from: iroh::EndpointId) -> bool {
    if peer_id_matches_delivered_from(peer_id, delivered_from) {
        return false;
    }
    warn!(
        ?session,
        body_peer = %peer_id,
        authenticated = %PeerId::from_bytes(*delivered_from.as_bytes()),
        "dropping gossip frame: body peer.id does not match delivered_from",
    );
    true
}

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
    /// Local address-lookup service the bridge populates with the
    /// host's wire-form addr from each inbound join ticket. Cloned
    /// from the [`MemoryLookup`] installed in the daemon's iroh
    /// `Endpoint` at startup, so `add_endpoint_info` calls here are
    /// visible to iroh's resolver chain immediately. Sidesteps the
    /// pkarr/DNS propagation race that otherwise pushes joiner-side
    /// gossip subscribe to `JOIN_READY_TIMEOUT` whenever a joiner
    /// dials a host whose pkarr publish hasn't propagated yet.
    addr_hint: MemoryLookup,
    /// Shared tracker of `EndpointId`s that have been seeded into
    /// [`Self::addr_hint`] (or are otherwise worth surveying at
    /// shutdown). The daemon's [`IrohRuntime`] owns the canonical
    /// reference; the bridge gets a clone so its inserts are
    /// visible to the shutdown-snapshot path. iroh 0.98.2 has no
    /// public iterator over `MemoryLookup` entries, so this
    /// shadow is the daemon's only enumerable source. Drives
    /// finding #5c's host-restart peer-addr cache.
    tracked_peer_ids: Arc<std::sync::Mutex<std::collections::BTreeSet<iroh::EndpointId>>>,
    /// The daemon's own iroh `EndpointId`, cached as a [`PeerId`]
    /// for cheap reads at outbound-stamp sites.
    authenticated_peer_id: PeerId,
    /// Daemon's iroh secret key, cloned from
    /// [`crate::server::IrohRuntime::signing_key`] at construction.
    /// Used by [`Self::send_remote`] to sign joiner-authored bodies
    /// before publishing the [`GossipBody::SendRequest`]. Held as
    /// `Arc<iroh::SecretKey>` so all spawned forwarders see the same
    /// 32 bytes without a refcount-per-publish.
    signing_key: Arc<iroh::SecretKey>,
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
    /// `on_message` (which verifies the host seq-sig after dedup);
    /// `SendAck` frames are verified against `host_pubkey` then looked
    /// up against `pending_sends`; `SendRequest` frames are ignored
    /// (only the host services them). `SessionClosed`/`EpochBeacon`
    /// frames are verified against `host_pubkey` and gated/advanced
    /// against `host_epoch_watermark` (Auth Slice B.5.3).
    Joiner {
        on_message: MessageHandler,
        /// The host's public key, from the ticket's `host_peer_id`
        /// (= `session.host`). Origin authentication is by signature
        /// against this key, not by who relayed the frame —
        /// topology-independent.
        host_pubkey: PeerId,
        /// Highest host epoch verified via a signed `EpochBeacon`.
        /// **Only** the `EpochBeacon` arm advances it; the
        /// `SessionClosed` arm reads it to gate replayed closes. A
        /// genuine `Message` replayed on an unseen seq must never move
        /// it (`replayed_message_cannot_poison_watermark`).
        host_epoch_watermark: Arc<AtomicU64>,
    },
}

impl GossipBridge {
    /// Construct a bridge wrapping `gossip`. The `addr_hint`
    /// [`MemoryLookup`] must be the same instance installed in the
    /// iroh `Endpoint`'s address-lookup chain at startup; the bridge
    /// populates it with each inbound ticket's wire-form addr in
    /// [`Self::join_session`] so the very first dial has the host's
    /// relay url + direct addrs in hand without waiting for pkarr
    /// propagation. `EndpointSetup::Testing`'s
    /// [`iroh::test_utils::DnsPkarrServer`] and
    /// `EndpointSetup::Production`'s n0 DNS still serve as the
    /// fallback / canonical address-lookup chain — `addr_hint` is a
    /// best-effort short-circuit, not a replacement.
    pub(crate) fn new(
        gossip: Gossip,
        addr_hint: MemoryLookup,
        tracked_peer_ids: Arc<std::sync::Mutex<std::collections::BTreeSet<iroh::EndpointId>>>,
        endpoint_id: iroh::EndpointId,
        signing_key: Arc<iroh::SecretKey>,
    ) -> Self {
        let authenticated_peer_id = PeerId::from_bytes(*endpoint_id.as_bytes());
        Self {
            gossip,
            sessions: Mutex::new(HashMap::new()),
            pending_sends: Mutex::new(HashMap::new()),
            registry: Mutex::new(Weak::new()),
            addr_hint,
            tracked_peer_ids,
            authenticated_peer_id,
            signing_key,
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
    /// `host_addr` is the wire-form addr the ticket carried; if it
    /// has any usable transport info (relay url or direct addrs) we
    /// install it into [`Self::addr_hint`] before subscribing, so
    /// the very first dial doesn't have to wait on pkarr/DNS
    /// propagation. The pkarr+DNS chain still services later
    /// resolutions and other endpoints; this is a synchronous
    /// shortcut for the join-time race.
    ///
    /// Once the gossip mesh is up, broadcasts a
    /// [`GossipBody::JoinAnnouncement`] carrying `joiner` so the
    /// host can admit the peer to membership and emit
    /// [`Event::PeerJoined`] proactively — without this, the
    /// joiner stays invisible until their first `SendRequest`.
    ///
    /// [`Event::PeerJoined`]: artel_protocol::Event::PeerJoined
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn join_session(
        self: &Arc<Self>,
        session: SessionId,
        joiner: PeerInfo,
        host_peer: PeerId,
        host_addr: &WireEndpointAddr,
        host_epoch_watermark: Arc<AtomicU64>,
        on_message: impl Fn(SessionMessage) + Send + Sync + 'static,
        ticket_id: TicketId,
        granted_cap: Capability,
        expiry_ms: u64,
        cap_sig: SigBytes,
    ) -> Result<(), BridgeError> {
        let host_endpoint_id = iroh::EndpointId::from_bytes(host_peer.as_bytes())
            .map_err(|e| BridgeError::Iroh(format!("bad host peer id: {e}")))?;
        // Seed the addr-hint memory lookup BEFORE subscribing so
        // iroh's first attempt to resolve `host_endpoint_id` finds
        // the relay url / direct addrs the ticket carried. The
        // wire-form addr's `peer_id` was already self-consistency-
        // checked by `ticket::decode`; we re-validate here against
        // the parameter we're keying on so a future caller that
        // builds the WireEndpointAddr by hand can't smuggle a
        // mismatched id past us.
        if host_addr.peer_id != host_peer {
            return Err(BridgeError::InvalidAddr(format!(
                "host_addr.peer_id {:?} does not match host_peer {:?}",
                host_addr.peer_id, host_peer,
            )));
        }
        if !host_addr.relay_url.is_empty() || !host_addr.direct_addrs.is_empty() {
            let endpoint_addr = wire_addr_to_iroh(host_addr).map_err(BridgeError::InvalidAddr)?;
            self.tracked_peer_ids
                .lock()
                .expect("poisoned")
                .insert(endpoint_addr.id);
            self.addr_hint.add_endpoint_info(endpoint_addr);
        } else {
            // Even id-only seeds are worth tracking: a future
            // shutdown snapshot may find that iroh has since
            // learned addrs for this peer through normal discovery,
            // and we want to persist those so the next daemon
            // incarnation has a head start.
            self.tracked_peer_ids
                .lock()
                .expect("poisoned")
                .insert(host_endpoint_id);
        }
        self.subscribe_inner(
            session,
            vec![host_endpoint_id],
            SessionRole::Joiner {
                on_message: Arc::new(on_message),
                host_pubkey: host_peer,
                host_epoch_watermark,
            },
        )
        .await?;
        // Mesh is up (subscribe_inner waits for `joined()` on the
        // joiner path). Announce ourselves so the host's bridge can
        // call `Registry::ensure_member` and emit `PeerJoined`.
        // Best-effort, but load-bearing: the announcement is the
        // SOLE admission path — `run_host_send` deliberately does
        // not admit on `SendRequest` (that would bypass ticket
        // verification). If this publish is lost, the host rejects
        // our sends with NotMember until a re-announcement gets
        // through.
        self.publish_join_announcement(session, joiner, ticket_id, granted_cap, expiry_ms, cap_sig)
            .await;
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

        // Override the IPC-caller-supplied peer.id with the daemon's
        // authenticated id so the host's L1 check
        // (peer_id_matches_delivered_from) succeeds. The IPC caller's
        // claimed id is not trustworthy; only the daemon's own iroh
        // EndpointId is. Display name is preserved.
        let peer = PeerInfo {
            id: self.authenticated_peer_id,
            ..peer
        };
        // Sign the body with this daemon's iroh secret key over the
        // canonical bytes (`session`, `MESSAGE_FORMAT`,
        // `timestamp_ms`, `peer`, `kind`, `action`, `payload`). The
        // host preserves `timestamp_ms` and `signature` verbatim
        // through the round-trip so the bytes the host re-broadcasts
        // verify against this peer.id.
        let SendPayload {
            kind,
            action,
            payload,
        } = payload;
        let timestamp_ms = now_ms();
        let signature = artel_protocol::signing::sign_body(
            self.signing_key.as_signing_key(),
            session,
            artel_protocol::message::MESSAGE_FORMAT,
            timestamp_ms,
            &peer,
            kind,
            &action,
            &payload,
        );
        let signed_payload = SignedSendPayload {
            timestamp_ms,
            kind,
            action,
            payload,
            signature,
        };
        let body = GossipBody::SendRequest {
            req_id,
            peer,
            payload: signed_payload,
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
                    Ok(IrohGossipEvent::Received(msg)) => {
                        // The application-level `PeerId` carried inside
                        // frame bodies is NOT a valid iroh public key
                        // in general (joiners stamp arbitrary 32-byte
                        // ids onto outbound `PeerInfo`); only
                        // `msg.delivered_from` (the iroh `EndpointId`
                        // of the mesh neighbor) is signed by the iroh
                        // transport. `handle_inbound_frame` records
                        // it into `tracked_peer_ids` only after the
                        // host-arm spoof check passes, so a peer that
                        // only ever sends spoofed bodies isn't
                        // captured for the shutdown-snapshot path.
                        match gossip::decode(&msg.content) {
                            Ok(body) => {
                                handle_inbound_frame(
                                    &bridge,
                                    session_for_log,
                                    &role,
                                    body,
                                    msg.delivered_from,
                                )
                                .await;
                            }
                            Err(err) => {
                                warn!(?err, "gossip frame decode failed; dropping");
                            }
                        }
                    }
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
    pub(crate) async fn publish_session_closed(&self, session: SessionId, host_epoch: u64) {
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
        // Host-sign over `"artel/ctrl-v1" || session || host_epoch`
        // (Auth Slice B.5, D3) — the same canonical bytes as the
        // EpochBeacon, so the joiner's `verify_ctrl` serves both.
        let host_sig = artel_protocol::signing::sign_ctrl(
            self.signing_key.as_signing_key(),
            session,
            host_epoch,
        );
        let bytes = Bytes::from(gossip::encode(&GossipBody::SessionClosed {
            host_epoch,
            host_sig,
        }));
        if let Err(err) = sender.broadcast(bytes).await {
            warn!(error = %err, "session_closed broadcast failed");
        }
    }

    /// Broadcast a signed [`GossipBody::EpochBeacon`] on the topic for
    /// `session` (Auth Slice B.5, D3). Called from `Registry::host`'s
    /// resume branch after re-subscribing, so already-joined joiners
    /// learn the bumped incarnation epoch immediately — independent of
    /// session activity. Best-effort (warn on failure), modeled on
    /// [`Self::publish_join_announcement`]. This is the only frame that
    /// advances a joiner's `host_epoch` watermark, and it carries a
    /// host-*signed* epoch so an attacker can't forge a high value.
    pub(crate) async fn publish_epoch_beacon(&self, session: SessionId, host_epoch: u64) {
        let sender = self
            .sessions
            .lock()
            .await
            .get(&session)
            .map(|s| s.sender.clone());
        let Some(sender) = sender else {
            debug!(
                ?session,
                "publish_epoch_beacon: session has no gossip topic; dropping",
            );
            return;
        };
        let host_sig = artel_protocol::signing::sign_ctrl(
            self.signing_key.as_signing_key(),
            session,
            host_epoch,
        );
        let bytes = Bytes::from(gossip::encode(&GossipBody::EpochBeacon {
            host_epoch,
            host_sig,
        }));
        if let Err(err) = sender.broadcast(bytes).await {
            warn!(error = %err, "epoch_beacon broadcast failed");
        }
    }

    /// Broadcast a [`GossipBody::JoinAnnouncement`] on the topic
    /// for `session`. Called from [`Self::join_session`] once the
    /// mesh is up. Best-effort — a failed broadcast falls back to
    /// the lazy-admission path (the host learns about us on our
    /// first `SendRequest`).
    async fn publish_join_announcement(
        &self,
        session: SessionId,
        peer: PeerInfo,
        ticket_id: TicketId,
        granted_cap: Capability,
        expiry_ms: u64,
        cap_sig: SigBytes,
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
                "publish_join_announcement: session has no gossip topic; dropping",
            );
            return;
        };
        // Override the IPC-caller-supplied peer.id with the daemon's
        // authenticated id so the host's L1 check
        // (peer_id_matches_delivered_from) succeeds. Same rationale as
        // `send_remote`: only the daemon's iroh EndpointId is
        // trustworthy. Display name preserved.
        let peer = PeerInfo {
            id: self.authenticated_peer_id,
            ..peer
        };
        let body = GossipBody::JoinAnnouncement {
            peer,
            timestamp_ms: now_ms(),
            ticket_id,
            granted_cap,
            expiry_ms,
            cap_sig,
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
        // Host-sign over `"artel/ack-v1" || session || req_id || result`
        // (Auth Slice B.5, D2) so a racing peer can't forge an ack or
        // flip Ok↔Err. `result` is bound into the signed scope.
        let host_sig = artel_protocol::signing::sign_ack(
            self.signing_key.as_signing_key(),
            session,
            req_id,
            &result,
        );
        let bytes = Bytes::from(gossip::encode(&GossipBody::SendAck {
            req_id,
            result,
            host_sig,
        }));
        if let Err(err) = sender.broadcast(bytes).await {
            warn!(error = %err, "send_ack broadcast failed");
        }
    }
}

/// Dispatch an inbound gossip frame body for `session` based on the
/// local role. Lives outside the impl so the spawned forwarder task
/// can call it without re-borrowing `self`.
///
/// `delivered_from` is the iroh `EndpointId` of the peer that
/// delivered the frame on the gossip mesh — signed by the iroh
/// transport, trustworthy. Host-role arms whose body carries a
/// `peer.id` (`SendRequest`, `JoinAnnouncement`) verify
/// `peer.id == delivered_from` and drop the frame on mismatch.
///
/// `delivered_from` is recorded into [`GossipBridge::tracked_peer_ids`]
/// only on arms that accept the frame as legitimate (after the spoof
/// check passes on host arms). A peer that ever-only sends spoofed
/// frames is never captured for the shutdown-snapshot path, so its
/// addr never lands in the persisted peer-addr cache — the daemon
/// won't seed it back into `addr_hint` at the next startup.
///
/// Joiner-role arms (`Message`, `SendAck::Ok`) carry
/// [`SessionMessage`] bodies whose `peer.id` is the *original
/// sender's* id, not the host that re-published the frame. Verifying
/// `body.peer.id == delivered_from` would be wrong; the correct
/// invariant is "the host vouches for the message", which needs L3
/// per-message signatures to enforce. Joiner-side enforcement of
/// `Message` / `SendAck::Ok` is therefore deferred to Slice B of the
/// auth story (see
/// `docs/brainstorms/2026-05-30-auth-story-brainstorm.md`). Until B
/// lands, joiners trust their host (the existing model).
// One `match (role, body)` dispatcher: every arm is a distinct
// role×frame case and the verify-then-act logic reads best inline
// rather than scattered across per-arm helpers. The Slice B.5 joiner
// verification arms pushed it past the line lint.
#[allow(clippy::too_many_lines)]
async fn handle_inbound_frame(
    bridge: &Arc<GossipBridge>,
    session: SessionId,
    role: &SessionRole,
    body: GossipBody,
    delivered_from: iroh::EndpointId,
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
            if drop_if_spoofed(session, peer.id, delivered_from) {
                return;
            }
            track_authenticated_peer(bridge, delivered_from);
            let result = run_host_send(bridge, session, peer, payload).await;
            bridge.publish_send_ack(session, req_id, result).await;
        }
        // Host receives a joiner's mesh-up announcement — admit them
        // to the session's membership eagerly so `PeerJoined` lands
        // on local IPC subscribers without waiting for a SendRequest.
        // `ensure_member` is idempotent, so a duplicate announcement
        // (or one that races with the lazy-admission path inside
        // run_host_send) is harmless.
        //
        // On success, immediately replay the backlog for the freshly
        // admitted (or re-announcing) peer. With `Replay` now
        // membership-gated, the joiner's own post-announce `Replay`
        // can race its admission and be dropped — this
        // admission-triggered replay closes that race structurally:
        // the new member always needs the backlog, so the host serves
        // it the moment admission lands, no joiner retries, no
        // timers. Re-announces re-serve the backlog too; the joiner
        // mirror dedups by seq.
        (
            SessionRole::Host,
            GossipBody::JoinAnnouncement {
                peer,
                timestamp_ms: _,
                ticket_id,
                granted_cap,
                expiry_ms,
                cap_sig,
            },
        ) => {
            if drop_if_spoofed(session, peer.id, delivered_from) {
                return;
            }
            track_authenticated_peer(bridge, delivered_from);
            let cap_claim = crate::session::CapClaim {
                ticket_id,
                granted_cap,
                expiry_ms,
                cap_sig,
            };
            let registry_weak = bridge.registry.lock().await.clone();
            if let Some(registry) = registry_weak.upgrade() {
                match registry.ensure_member(session, peer, Some(cap_claim)).await {
                    Ok(()) => run_host_replay(bridge, session, Seq::ZERO).await,
                    Err(err) => warn!(?err, "join_announcement: ensure_member failed"),
                }
            }
        }
        // Joiner receives the host's "I am closing this session"
        // broadcast — drop the local mirror, emit
        // `Event::SessionClosed`, and tear down our own bridge
        // entry. Idempotent on the registry side, so a stray
        // duplicate frame is harmless.
        (
            SessionRole::Joiner {
                host_pubkey,
                host_epoch_watermark,
                ..
            },
            GossipBody::SessionClosed {
                host_epoch,
                host_sig,
            },
        ) => {
            // Accept the close iff (a) the host signature verifies over
            // `(session, host_epoch)` AND (b) host_epoch >= the
            // beacon-advanced watermark. A forged close fails (a)
            // (no host key); a close captured from an earlier
            // incarnation and replayed after a resume fails (b) (its
            // epoch is below the watermark the resume beacon advanced).
            if let Err(err) =
                artel_protocol::signing::verify_ctrl(host_pubkey, session, host_epoch, &host_sig)
            {
                warn!(
                    ?session,
                    host_epoch,
                    ?err,
                    "dropping SessionClosed: host_sig verify failed"
                );
                return;
            }
            let watermark = host_epoch_watermark.load(Ordering::Acquire);
            if host_epoch < watermark {
                warn!(
                    ?session,
                    host_epoch,
                    watermark,
                    "dropping SessionClosed: epoch below beacon watermark (replay across resume)",
                );
                return;
            }
            track_authenticated_peer(bridge, delivered_from);
            let registry_weak = bridge.registry.lock().await.clone();
            if let Some(registry) = registry_weak.upgrade()
                && let Err(err) = registry.host_closed_session(session).await
            {
                warn!(?err, "session_closed: host_closed_session failed");
            }
        }
        // Host receives a `Replay` request — fetch the log entries
        // with seq > since and re-broadcast each as a `Message`
        // frame. Membership-gated (revoked-lurker fix): the topic
        // subscription is unauthenticated, so an un-admitted bearer
        // of a revoked/expired ticket can publish a `Replay` — it
        // must get nothing. `delivered_from` is the trustworthy id
        // (same L1 topology assumption as `drop_if_spoofed`: in the
        // star topology the host hears joiners directly). A fresh
        // joiner whose admission is still in flight is covered by
        // the admission-triggered replay in the JoinAnnouncement arm
        // above; this arm serves the already-member resubscribe.
        // Other members on the topic see the replay traffic too and
        // dedup-skip it; that's wasteful but correct (see
        // `GossipBody::Replay` doc-comment).
        (SessionRole::Host, GossipBody::Replay { since }) => {
            let requester = PeerId::from_bytes(*delivered_from.as_bytes());
            let registry_weak = bridge.registry.lock().await.clone();
            let Some(registry) = registry_weak.upgrade() else {
                return;
            };
            if registry.is_member(session, requester).await != Some(true) {
                warn!(
                    ?session,
                    peer = %requester,
                    "dropping Replay from non-member",
                );
                return;
            }
            track_authenticated_peer(bridge, delivered_from);
            run_host_replay(bridge, session, since).await;
        }
        // Host ignores its own Message+SendAck broadcasts (Registry
        // already fanned out locally), its own SessionClosed
        // (we publish before forget_session, so the broadcast
        // round-trips back here on the same forwarder), and its own
        // EpochBeacon round-trip; joiners ignore each other's
        // SendRequests + JoinAnnouncements + Replays (only the host
        // services them at this layer).
        (
            SessionRole::Host,
            GossipBody::Message(_)
            | GossipBody::SendAck { .. }
            | GossipBody::SessionClosed { .. }
            | GossipBody::EpochBeacon { .. },
        )
        | (
            SessionRole::Joiner { .. },
            GossipBody::SendRequest { .. }
            | GossipBody::JoinAnnouncement { .. }
            | GossipBody::Replay { .. },
        ) => {}

        // Joiner receives the host's broadcast — push into the local
        // mirror's events_tx + log. The host seq-sig is verified
        // *inside* `on_message`, after dedup (dedup → author sig →
        // host seq-sig), so routine duplicate deliveries don't re-pay
        // crypto and the watermark is never touched here.
        (SessionRole::Joiner { on_message, .. }, GossipBody::Message(m)) => {
            track_authenticated_peer(bridge, delivered_from);
            on_message(m);
        }
        // Joiner receives the host's reply to one of our outbound
        // sends. Verify the host ack-sig over `(session, req_id,
        // result)` BEFORE resolving the oneshot. A forged ack fails
        // here and we do NOT resolve — the joiner's `send_remote`
        // times out, far better than surfacing a spoofed result. (A
        // replayed genuine ack self-limits: `req_id` is a fresh v4
        // uuid, so `pending_sends.remove` returns None for a
        // non-pending id.)
        (
            SessionRole::Joiner { host_pubkey, .. },
            GossipBody::SendAck {
                req_id,
                result,
                host_sig,
            },
        ) => {
            if let Err(err) = artel_protocol::signing::verify_ack(
                host_pubkey,
                session,
                req_id,
                &result,
                &host_sig,
            ) {
                warn!(
                    ?session,
                    ?req_id,
                    ?err,
                    "dropping SendAck: host_sig verify failed"
                );
                return;
            }
            track_authenticated_peer(bridge, delivered_from);
            let pending = bridge.pending_sends.lock().await.remove(&req_id);
            if let Some(tx) = pending {
                let _ = tx.send(result);
            } else {
                debug!(?req_id, "SendAck for unknown req_id; dropping");
            }
        }
        // Joiner receives a host epoch beacon. This is the ONLY site
        // that advances the watermark, and only on a host-signed
        // value: verify the ctrl-sig over `(session, host_epoch)`,
        // then advance the watermark to max(watermark, host_epoch) and
        // persist it to the mirror record. A wrong-key beacon is
        // dropped; a replayed old beacon can't lower the monotonic
        // watermark.
        (
            SessionRole::Joiner {
                host_pubkey,
                host_epoch_watermark,
                ..
            },
            GossipBody::EpochBeacon {
                host_epoch,
                host_sig,
            },
        ) => {
            if let Err(err) =
                artel_protocol::signing::verify_ctrl(host_pubkey, session, host_epoch, &host_sig)
            {
                warn!(
                    ?session,
                    host_epoch,
                    ?err,
                    "dropping EpochBeacon: host_sig verify failed"
                );
                return;
            }
            track_authenticated_peer(bridge, delivered_from);
            // fetch_max returns the previous value; advance only logs /
            // persists when the watermark actually moved forward.
            let prev = host_epoch_watermark.fetch_max(host_epoch, Ordering::AcqRel);
            if host_epoch > prev {
                let registry_weak = bridge.registry.lock().await.clone();
                if let Some(registry) = registry_weak.upgrade()
                    && let Err(err) = registry
                        .advance_host_epoch_watermark(session, host_epoch)
                        .await
                {
                    warn!(
                        ?err,
                        ?session,
                        host_epoch,
                        "epoch_beacon: persist watermark failed"
                    );
                }
            }
        }
    }
}

/// Record `delivered_from` into the bridge's `tracked_peer_ids` set.
/// Called from each `handle_inbound_frame` arm that accepts the frame
/// as legitimate; never called on a spoof-dropped frame, so a peer
/// that only ever sends spoofed bodies isn't captured for the
/// shutdown-snapshot path.
fn track_authenticated_peer(bridge: &GossipBridge, delivered_from: iroh::EndpointId) {
    bridge
        .tracked_peer_ids
        .lock()
        .expect("poisoned")
        .insert(delivered_from);
}

/// Run the host-side `Registry::send` corresponding to an inbound
/// `SendRequest`. Translates [`SessionError`] into the wire form
/// the joiner expects to see in `SendAck.result`.
///
/// `payload` is a [`SignedSendPayload`] — the joiner's daemon stamped
/// `timestamp_ms` and signed before publishing. The host preserves
/// `timestamp_ms` and `signature` verbatim into the resulting
/// [`SessionMessage`] so receivers see the same bytes the joiner
/// signed. Signature verification happens authoritatively inside
/// `Registry::send`'s `Remote` authoring arm (verify-before-append);
/// this function only translates the resulting [`SessionError`] into
/// the wire form the joiner expects in `SendAck.result` and logs the
/// rejection with bridge-local context.
async fn run_host_send(
    bridge: &Arc<GossipBridge>,
    session: SessionId,
    peer: PeerInfo,
    payload: SignedSendPayload,
) -> Result<SessionMessage, ProtocolError> {
    let registry_weak = bridge.registry.lock().await.clone();
    let Some(registry) = registry_weak.upgrade() else {
        // Daemon shutting down; nothing useful to say.
        return Err(ProtocolError::Internal("daemon shutting down".into()));
    };
    // The JoinAnnouncement is the sole admission path: it carries
    // the signed CapClaim that determines the peer's tier. If the
    // announcement was lost, the peer must re-send it — we do NOT
    // admit on the SendRequest path because that would bypass ticket
    // verification and grant unconditional RW (privilege escalation).
    // The send() below will reject with NotMember if the peer hasn't
    // been admitted yet.
    let SignedSendPayload {
        timestamp_ms,
        kind,
        action,
        payload,
        signature,
    } = payload;
    // Signature verification is the registry's job: `Registry::send`'s
    // `Remote` authoring arm verifies against the body's `peer.id`
    // before assigning a seq or appending, returning
    // `SessionError::SignatureRejected` on failure. We do NOT
    // pre-verify here — a second `verify_message` would re-run the
    // ed25519 scalar-mult and re-allocate the canonical bytes on the
    // host's hot path for no added safety. Instead we attach the
    // bridge-local context (`peer.id`, which `drop_if_spoofed` has
    // already pinned to `delivered_from`) to the warn on the way out.
    let peer_id = peer.id;
    match registry
        .send(
            session,
            peer,
            kind,
            action,
            payload,
            crate::session::Authoring::Remote {
                timestamp_ms,
                signature,
            },
        )
        .await
    {
        Ok(message) => Ok(message),
        Err(err) => {
            if let SessionError::SignatureRejected { reason, .. } = &err {
                warn!(
                    ?session,
                    peer = %peer_id,
                    %reason,
                    "host bridge: dropping SendRequest with bad signature",
                );
            }
            Err(session_error_to_wire(&err))
        }
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

/// Translate session-layer errors into wire errors for the `SendAck`
/// path. The mapping itself lives on `SessionError` (one shared
/// `From` impl with the IPC server) so the two wire surfaces cannot
/// drift.
fn session_error_to_wire(err: &SessionError) -> ProtocolError {
    err.into()
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

    /// `join_session` was called with a [`WireEndpointAddr`] whose
    /// fields couldn't be parsed into the iroh form (bad relay URL,
    /// peer-id mismatch, etc.). The registry maps this to
    /// [`crate::session::SessionError::InvalidAddr`]; over the wire
    /// it surfaces as a generic `Internal` error so we don't leak
    /// parser detail.
    #[error("invalid host addr in ticket: {0}")]
    InvalidAddr(String),
}

/// Convert a wire-form [`WireEndpointAddr`] into an iroh
/// [`EndpointAddr`]. Empty `relay_url` means "no home relay" and
/// is allowed; a non-empty value that fails to parse as a URL is
/// rejected as [`BridgeError::InvalidAddr`]. The
/// `WireEndpointAddr`'s `peer_id` is the source of truth for the
/// returned `EndpointAddr`'s id.
fn wire_addr_to_iroh(addr: &WireEndpointAddr) -> Result<EndpointAddr, String> {
    let endpoint_id = iroh::EndpointId::from_bytes(addr.peer_id.as_bytes())
        .map_err(|e| format!("peer id: {e}"))?;
    let mut iroh_addr = EndpointAddr::new(endpoint_id);
    if !addr.relay_url.is_empty() {
        let url =
            iroh::RelayUrl::from_str(&addr.relay_url).map_err(|e| format!("relay_url: {e}"))?;
        iroh_addr = iroh_addr.with_relay_url(url);
    }
    for direct in &addr.direct_addrs {
        iroh_addr = iroh_addr.with_ip_addr(*direct);
    }
    Ok(iroh_addr)
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

    /// Generate a deterministic `EndpointId` whose underlying bytes
    /// are a valid Ed25519 public key (constant-byte ids like
    /// `[0xab; 32]` don't satisfy the curve check). Mirrors the
    /// helper in `peer_addr_cache::tests`.
    fn valid_endpoint_id(seed: u8) -> iroh::EndpointId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn peer_id_matches_delivered_from_byte_equality() {
        // Pins the helper as pure byte equality, not curve / key
        // validation. End-to-end pinning of the live invariant
        // (bridge field → outbound frame → host check) lives in
        // `tests/auth_l1_spoofing.rs::joiner_outbound_stamps_authenticated_peer_id`.
        let endpoint_id = valid_endpoint_id(0xab);
        let matching = PeerId::from_bytes(*endpoint_id.as_bytes());
        assert!(peer_id_matches_delivered_from(matching, endpoint_id));

        let mut flipped = *endpoint_id.as_bytes();
        flipped[0] ^= 0x01;
        let mismatched = PeerId::from_bytes(flipped);
        assert!(!peer_id_matches_delivered_from(mismatched, endpoint_id));
    }
}
