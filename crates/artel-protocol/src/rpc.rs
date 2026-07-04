//! Request / response / event types for the daemon ↔ client RPC.
//!
//! See ADR-001 § "RPC surface". The IPC framing (length prefix, transport)
//! is intentionally not specified here — it lives in `crate::transport`
//! (feature `tokio`). This module only defines the *payload* of each frame.

use serde::{Deserialize, Serialize};

use crate::error::ProtocolError;
use crate::ids::{PeerId, Seq, SessionId};
use crate::message::{PeerInfo, SessionMessage, SigBytes};
use crate::version::ProtocolVersion;

/// Correlates a [`Request`] with its [`Response`].
///
/// Allocated by the client. Unique per connection. Reuse across connections
/// is fine; uniqueness within a connection is what matters.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct RequestId(u64);

impl RequestId {
    /// The first valid request id.
    pub const ZERO: Self = Self(0);

    /// Construct from a raw `u64`.
    #[must_use]
    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    /// The raw `u64` value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Allocate the next request id, returning `None` on overflow.
    ///
    /// At one request per nanosecond a `u64` lasts 584 years, so overflow
    /// in practice means a connection-id-recycling bug.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(n) => Some(Self(n)),
            None => None,
        }
    }
}

/// An opaque join ticket distributed out-of-band.
///
/// The `artel:` text codec and payload are defined in [`crate::ticket`]
/// ([`crate::ticket::SessionTicket`]: ticket id, session id, host peer
/// id + wire address, granted capability tier, expiry, capability
/// signature — the gossip topic is not carried; it is derived from the
/// session id). This type keeps the string opaque for RPC plumbing; the
/// daemon decodes and validates it. Tickets are bearer credentials —
/// anyone with one can join.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JoinTicket(pub String);

impl JoinTicket {
    /// Borrow the underlying string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for JoinTicket {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for JoinTicket {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// Brief, list-safe summary of a session.
///
/// Returned by [`Request::ListSessions`]. Does *not* include the full log;
/// use [`Request::Subscribe`] for that.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSummary {
    /// Session identifier.
    pub id: SessionId,
    /// Whether this daemon is the host of the session.
    pub is_host: bool,
    /// Number of admitted members, including ourself. Durable
    /// membership, not live connectivity.
    pub peer_count: u32,
    /// Highest sequence number seen so far.
    pub last_seq: Option<Seq>,
}

/// A request the client sends to the daemon.
///
/// Every variant has a corresponding successful [`Response`] variant; the
/// daemon may also respond with [`Response::Error`] on any request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Request {
    /// Negotiate protocol version. Must be the first request on a fresh
    /// connection. The daemon either accepts or returns
    /// [`ProtocolError::VersionMismatch`].
    Hello {
        /// Version the client speaks.
        client_version: ProtocolVersion,
    },

    /// Create or resume a hosted session. Returns the session's
    /// [`SessionId`] and a shareable [`JoinTicket`].
    ///
    /// `session` controls how the id is chosen:
    /// - `None` (the default): mint a fresh random id. This is
    ///   today's behaviour and the right choice for first-time
    ///   hosts that have nothing to resume.
    /// - `Some(id)`: resume the session at that id when a matching
    ///   local-host record exists; otherwise create a new session
    ///   with the supplied id. Used by callers (e.g. `artel-fs`)
    ///   that derive a stable id from local state so a re-host of
    ///   the same workspace lands on the same session id across
    ///   daemon restarts.
    ///
    /// A `Some(id)` request whose existing record has a different
    /// host or is a remote-mirror session is rejected with
    /// [`ProtocolError::SessionConflict`].
    HostSession {
        /// Display name to advertise as this peer's label. Display
        /// names are advisory and never authenticated. The
        /// authenticated [`PeerId`] is the daemon's own iroh
        /// `EndpointId`; the IPC caller cannot influence it. See
        /// `docs/brainstorms/2026-06-01-auth-l1-fix3-shape.md` and
        /// `docs/plans/2026-06-01-auth-l1-fix3-plan.md`.
        display_name: String,
        /// Caller-supplied session id, or `None` to mint a fresh one.
        session: Option<SessionId>,
    },

    /// Join an existing session via its ticket.
    JoinSession {
        /// Display name to advertise as this peer's label. Display
        /// names are advisory and never authenticated. The
        /// authenticated [`PeerId`] is the daemon's own iroh
        /// `EndpointId`; the IPC caller cannot influence it. See
        /// `docs/brainstorms/2026-06-01-auth-l1-fix3-shape.md`.
        display_name: String,
        /// Ticket obtained out-of-band from the host.
        ticket: JoinTicket,
    },

    /// List sessions the daemon is currently hosting or joined.
    ListSessions,

    /// Subscribe to event delivery for a session. Optionally replays the
    /// log from `since` onwards.
    Subscribe {
        /// Session to subscribe to.
        session: SessionId,
        /// If set, replay messages with `seq > since` before live delivery
        /// resumes. If `None`, the daemon replays the full log (treated
        /// as `Seq::ZERO`); artel-fs's join path relies on this to
        /// observe the workspace ticket.
        since: Option<Seq>,
    },

    /// Send a message into a session. The daemon assigns the [`Seq`].
    Send {
        /// Session to send into.
        session: SessionId,
        /// Application payload. The daemon does not inspect it.
        payload: SendPayload,
    },

    /// Leave a session. For hosts this also closes the session for all
    /// peers; for joiners it disconnects only this peer.
    LeaveSession {
        /// Session to leave.
        session: SessionId,
    },

    /// Register an opaque attachment against a session.
    ///
    /// `kind` is a consumer-chosen tag (e.g. `"artel-fs/workspace/v1"`)
    /// the daemon uses only for indexing — it never parses `payload`.
    /// Within a `(session, kind)` pair, registering overwrites any
    /// existing entry; this is the idempotent re-register flow
    /// consumers use on restart.
    ///
    /// Returns [`ProtocolError::UnknownSession`] if the session is not
    /// known to the daemon. Attachments cascade-delete with their
    /// session.
    RegisterAttachment {
        /// Session this attachment is bound to.
        session: SessionId,
        /// Consumer-namespaced tag, e.g. `"artel-fs/workspace/v1"`.
        /// Treated as opaque by the daemon.
        kind: String,
        /// Consumer-defined bytes. Daemon never inspects.
        #[serde(with = "send_payload_bytes")]
        payload: Vec<u8>,
    },

    /// List attachments the daemon knows about.
    ///
    /// `kind` is an exact-match filter. `None` returns every
    /// attachment for every known session. `Some(k)` returns only
    /// attachments tagged with `k`. Order is not specified; callers
    /// that care should sort client-side.
    ListAttachments {
        /// Optional exact-match `kind` filter. `None` = all kinds.
        kind: Option<String>,
    },

    /// Remove an attachment without removing its session.
    ///
    /// Used by consumers that want their entry gone but the underlying
    /// session still alive. Idempotent: forgetting an attachment that
    /// does not exist is `Ok(())`. Forgetting an attachment whose
    /// session doesn't exist is also `Ok(())` (the cascade already
    /// cleared it).
    ForgetAttachment {
        /// Session the attachment is bound to.
        session: SessionId,
        /// Tag of the attachment to remove.
        kind: String,
    },

    /// Issue an additional ticket for an existing hosted session with
    /// a specific capability level and optional expiry.
    ///
    /// Only the host of a `SessionKind::Local` session may issue
    /// tickets. Returns [`ProtocolError::NotHost`] if the session is
    /// remote.
    IssueTicket {
        /// Session to issue the ticket for.
        session: SessionId,
        /// Capability the ticket grants to the joiner.
        granted_cap: crate::capability::Capability,
        /// Ticket expiry in milliseconds since epoch (0 = no expiry).
        expiry_ms: u64,
    },

    /// Revoke a previously issued ticket so it no longer admits
    /// bearers. Only the host of a `SessionKind::Local` session may
    /// revoke; returns [`ProtocolError::NotHost`] otherwise.
    ///
    /// Idempotent on an already-revoked ticket. A `ticket_id` that was
    /// never issued for this session is rejected with
    /// [`ProtocolError::UnknownTicket`] — reporting success for an
    /// unknown id would falsely reassure the caller a leaked ticket is
    /// dead. Revocation gates *future admissions only*: a peer already
    /// admitted via this ticket keeps its membership and capabilities
    /// (use a capability revoke for that — see
    /// [`Response::Tickets`]' `used_by`).
    RevokeTicket {
        /// Session the ticket was issued for.
        session: SessionId,
        /// Id of the ticket to revoke, as returned by
        /// [`Response::IssuedTicket`] / [`Response::HostSession`] or
        /// listed by [`Request::ListTickets`].
        ticket_id: crate::ids::TicketId,
    },

    /// List every ticket issued for a hosted session — id, tier,
    /// expiry, status, and which peers were admitted with it. Only
    /// the host of a `SessionKind::Local` session may list; returns
    /// [`ProtocolError::NotHost`] otherwise. The encoded bearer
    /// strings are never returned.
    ListTickets {
        /// Session whose ledger to list.
        session: SessionId,
    },

    /// Deliver the `NamespaceSecret` directly to a target peer via a
    /// dedicated QUIC stream. Only the host of a `SessionKind::Local`
    /// session may call this.
    ///
    /// The daemon opens a direct stream to the target peer using
    /// [`crate::upgrade::UPGRADE_ALPN`], sends the secret, and waits
    /// for an ACK. Returns [`Response::UpgradeDelivered`] on success.
    DeliverUpgrade {
        /// Session the upgrade applies to.
        session: SessionId,
        /// Peer to deliver the secret to.
        target_peer: PeerId,
        /// The 32-byte namespace secret.
        #[serde(with = "serde_bytes")]
        namespace_secret: [u8; 32],
    },

    /// Publish the workspace's read-capability ticket envelope to the
    /// daemon, which persists it on the session record and owns its
    /// distribution: unicast over the direct-stream delivery channel
    /// to every current member on publish, and to each peer at
    /// admission. Replaces the host workspace's former broadcast
    /// `Send` of the `workspace.ticket` System message — nothing
    /// capability-bearing rides the gossip topic any more
    /// (revoked-lurker fix, `PROTOCOL_VERSION` 9).
    ///
    /// Only the host of a `SessionKind::Local` session may call this.
    /// Returns [`Response::WorkspaceTicketPublished`] on success.
    PublishWorkspaceTicket {
        /// Session the envelope belongs to.
        session: SessionId,
        /// postcard-encoded `WorkspaceTicketEnvelope`, opaque to the
        /// daemon.
        #[serde(with = "send_payload_bytes")]
        envelope_bytes: Vec<u8>,
    },

    /// Deliver a cooperative-downgrade (RW → Read) notification to a
    /// peer over the direct stream. Host-only; only the `Local` host of
    /// the session may call this. Mirrors [`Self::DeliverUpgrade`] but
    /// carries no key material — it only tells the peer to stop writing
    /// (the demoted node halts its own watcher). The daemon opens a
    /// stream on [`crate::upgrade::UPGRADE_ALPN`], sends a
    /// [`crate::upgrade::DeliveryFrame::Downgrade`], and waits for an
    /// ACK. Returns [`Response::DowngradeDelivered`] on success.
    DeliverDowngrade {
        /// Session the downgrade applies to.
        session: SessionId,
        /// Peer to notify of its demotion.
        target_peer: PeerId,
    },

    /// Deliver a rotated namespace's Write `DocTicket` + epoch to a
    /// surviving RW peer over the direct stream (Evict /
    /// write-revocation, Slice 3e). Host-only. The daemon opens a stream
    /// on [`crate::upgrade::UPGRADE_ALPN`], sends a
    /// [`crate::upgrade::DeliveryFrame::Rotate`], and waits for an ACK.
    /// Returns [`Response::RotateDelivered`] on success.
    DeliverRotate {
        /// Session the rotation applies to.
        session: SessionId,
        /// Survivor to deliver the rotated namespace to.
        target_peer: PeerId,
        /// New namespace epoch.
        namespace_epoch: u64,
        /// Rotated namespace's `DocTicket::to_string()`.
        doc_ticket: String,
    },

    /// Host-authority removal of another peer from a hosted session's
    /// durable membership.
    ///
    /// Distinct from [`Self::LeaveSession`], which removes the *caller*
    /// (membership keyed to this connection). This removes a *different*
    /// peer named by `target_peer` — the host evicting a member. The
    /// workspace layer issues it when it observes a capability `Revoke`,
    /// so the host stops serving the evicted peer gossip (notably the
    /// membership-gated log `Replay`). The daemon is told only to drop a
    /// member; it does not parse capabilities (ADR-003).
    ///
    /// Only the host of a `SessionKind::Local` session may call this;
    /// [`ProtocolError::NotHost`] otherwise. Idempotent: removing a
    /// non-member is success. Removing the host itself is a no-op
    /// (the cap-log root stays a member). Returns
    /// [`Response::MemberRemoved`]. Append-only variant — keep last.
    RemoveSessionMember {
        /// Session to remove the member from.
        session: SessionId,
        /// Peer to evict from the session's membership.
        target_peer: PeerId,
    },
}

/// Fields of a [`Request::Send`] that the client supplies.
///
/// Distinct from [`SessionMessage`] because the host-assigned `seq`,
/// `version`, and `peer` are filled in by the daemon, not the client.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendPayload {
    /// Top-level category.
    pub kind: crate::message::MessageKind,
    /// Application-defined verb.
    pub action: String,
    /// Opaque application bytes.
    #[serde(with = "send_payload_bytes")]
    pub payload: Vec<u8>,
}

/// Joiner-authored, signed body shipped on the gossip wire as the
/// payload of [`crate::gossip::GossipBody::SendRequest`].
///
/// The IPC `Request::Send` body is [`SendPayload`] (no key on the IPC
/// caller); the joiner's daemon stamps `timestamp_ms` and signs to
/// produce a `SignedSendPayload`, which then rides on
/// `GossipBody::SendRequest`. The host preserves `timestamp_ms` and
/// `signature` verbatim into the broadcast [`SessionMessage`];
/// receivers verify against the joiner's `peer.id` (which is the
/// `peer` field on the carrying frame).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedSendPayload {
    /// Authoring time, stamped by the joiner's daemon at the moment
    /// the body is signed. Preserved verbatim by the host through the
    /// round-trip; included in the signed scope so a malicious host
    /// cannot rewrite it.
    pub timestamp_ms: u64,
    /// Top-level category.
    pub kind: crate::message::MessageKind,
    /// Application-defined verb.
    pub action: String,
    /// Opaque application bytes.
    #[serde(with = "send_payload_bytes")]
    pub payload: Vec<u8>,
    /// 64-byte ed25519 signature over
    /// `crate::signing::canonical_bytes(session_id, MESSAGE_FORMAT,
    /// timestamp_ms, peer, kind, action, payload)`. The session id is
    /// not on this struct because the carrier (the gossip topic)
    /// already names the session; the host re-binds it when
    /// rebuilding the [`SessionMessage`].
    #[serde(with = "crate::message::signature_serde")]
    pub signature: SigBytes,
}

mod send_payload_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            s.collect_seq(bytes.iter().copied())
        } else {
            s.serialize_bytes(bytes)
        }
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        if d.is_human_readable() {
            Vec::<u8>::deserialize(d)
        } else {
            serde_bytes::ByteBuf::deserialize(d).map(serde_bytes::ByteBuf::into_vec)
        }
    }
}

/// The daemon's reply to a [`Request`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Response {
    /// Reply to [`Request::Hello`]. Contains the daemon's own version so
    /// the client can log it; mismatches arrive via [`Response::Error`]
    /// rather than this variant.
    Hello {
        /// Version the daemon speaks.
        daemon_version: ProtocolVersion,
        /// The daemon's own peer id, advertised so clients can show it
        /// without having to host or join a session first.
        daemon_peer_id: PeerId,
    },

    /// Reply to [`Request::HostSession`].
    HostSession {
        /// Identifier for the new session.
        session: SessionId,
        /// Ticket the host distributes out-of-band to invitees.
        ticket: JoinTicket,
        /// Id of the minted ticket, for later
        /// [`Request::RevokeTicket`] without decoding `ticket`.
        ticket_id: crate::ids::TicketId,
    },

    /// Reply to [`Request::JoinSession`].
    JoinSession {
        /// Identifier for the joined session.
        session: SessionId,
        /// Highest seq the daemon has seen for the session at join time.
        /// Useful for clients deciding what to backfill.
        head: Option<Seq>,
    },

    /// Reply to [`Request::ListSessions`].
    ListSessions {
        /// One entry per active session.
        sessions: Vec<SessionSummary>,
    },

    /// Reply to [`Request::Subscribe`]. The client should expect [`Event`]
    /// frames after this.
    Subscribed {
        /// Session that was subscribed to.
        session: SessionId,
    },

    /// Reply to [`Request::Send`]. Acknowledges the message and reports
    /// the assigned sequence number.
    Sent {
        /// Session the message was added to.
        session: SessionId,
        /// Sequence number the daemon assigned.
        seq: Seq,
    },

    /// Reply to [`Request::LeaveSession`].
    Left {
        /// Session that was left.
        session: SessionId,
    },

    /// Reply to [`Request::RegisterAttachment`] (success).
    AttachmentRegistered,

    /// Reply to [`Request::ListAttachments`].
    Attachments {
        /// Matching attachments. Order unspecified.
        entries: Vec<Attachment>,
    },

    /// Reply to [`Request::ForgetAttachment`] (success).
    AttachmentForgotten,

    /// Reply to [`Request::IssueTicket`].
    IssuedTicket {
        /// The newly minted ticket.
        ticket: JoinTicket,
        /// Id of the minted ticket, for later
        /// [`Request::RevokeTicket`] without decoding `ticket`.
        ticket_id: crate::ids::TicketId,
    },

    /// Reply to [`Request::DeliverUpgrade`]. Confirms the target peer
    /// received and acknowledged the namespace secret.
    UpgradeDelivered,

    /// Any request may produce this in place of its expected variant.
    ///
    /// Must stay at postcard variant index 12: the version handshake
    /// delivers [`ProtocolError::VersionMismatch`] through this
    /// variant to clients of *other* protocol versions, so its wire
    /// position is the one part of `Response` that cannot move. New
    /// variants are appended below, never inserted above — see the
    /// `handshake_postcard_indices_are_pinned` test.
    Error {
        /// Wire-representable error.
        error: ProtocolError,
    },

    /// Reply to [`Request::RevokeTicket`] (success, including the
    /// idempotent already-revoked case).
    TicketRevoked,

    /// Reply to [`Request::ListTickets`].
    Tickets {
        /// Every ticket issued for the session, mint order.
        entries: Vec<crate::ticket::TicketEntry>,
    },

    /// Reply to [`Request::PublishWorkspaceTicket`] (success). The
    /// envelope is durably persisted on the session record;
    /// per-member unicast delivery is best-effort (offline members
    /// are covered by admission-redelivery on their re-announce).
    WorkspaceTicketPublished,

    /// Reply to [`Request::DeliverDowngrade`]. Confirms the target peer
    /// received and acknowledged the downgrade notification.
    DowngradeDelivered,

    /// Reply to [`Request::DeliverRotate`]. Confirms the survivor
    /// received and acknowledged the rotated namespace ticket.
    RotateDelivered,

    /// Reply to [`Request::RemoveSessionMember`] (success, including the
    /// idempotent non-member and self-targeted-host no-op cases).
    /// Append-only variant — keep last.
    MemberRemoved,
}

/// One entry in [`Response::Attachments`].
///
/// Pure data, no daemon-side semantics attached. The daemon never parses
/// `payload`; consumers tag entries with `kind` and ship a postcard- (or
/// other-) encoded blob inside `payload`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    /// Session this attachment is bound to.
    pub session: SessionId,
    /// Consumer-namespaced tag.
    pub kind: String,
    /// Consumer-defined opaque bytes. Postcard-encoded as a byte slice
    /// (matches [`SendPayload::payload`]) rather than a `Vec<u8>` seq.
    #[serde(with = "send_payload_bytes")]
    pub payload: Vec<u8>,
}

/// Asynchronous event the daemon pushes to a subscribed client.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Event {
    /// A new (or replayed) message in a subscribed session.
    Message {
        /// Session the message belongs to.
        session: SessionId,
        /// The message itself.
        message: SessionMessage,
    },

    /// A peer joined a subscribed session.
    PeerJoined {
        /// Session the peer joined.
        session: SessionId,
        /// The peer.
        peer: PeerInfo,
    },

    /// A peer left a subscribed session.
    PeerLeft {
        /// Session the peer left.
        session: SessionId,
        /// Peer id of the departed peer.
        peer: PeerId,
    },

    /// The session was closed by the host.
    SessionClosed {
        /// Session that closed.
        session: SessionId,
    },

    /// One or more events were dropped for this subscriber before they
    /// could be delivered (the daemon's per-subscriber broadcast buffer
    /// overflowed — see `EVENT_CHANNEL_CAPACITY`). The stream stays
    /// open; the daemon does **not** close the connection. The
    /// subscriber recovers by re-`Subscribe`ing from its last-seen seq
    /// (`Subscribe { since }`), which replays every logged message past
    /// the gap. Live-only events (`PeerJoined`/`PeerLeft`/
    /// `SessionClosed`) that fell in the gap are not replayed; a
    /// consumer that needs them must reconcile membership separately.
    ///
    /// Appended last (postcard index 4) on `PROTOCOL_VERSION` 10 so the
    /// earlier variants keep their wire positions — see the
    /// `event_gap_is_appended_at_index_four` test (M3 Part B).
    Gap {
        /// Session whose stream dropped events.
        session: SessionId,
    },
}

/// One frame on the IPC wire.
///
/// Requests and responses are correlated by [`RequestId`]; events have no
/// id since they are server-pushed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireMessage {
    /// Client → daemon.
    Request {
        /// Identifier the client picked.
        id: RequestId,
        /// Request payload.
        request: Request,
    },
    /// Daemon → client, in reply to a request with this id.
    Response {
        /// Echoes the [`WireMessage::Request`] id this is replying to.
        id: RequestId,
        /// Response payload.
        response: Response,
    },
    /// Daemon → client, unsolicited. Server-push of a subscription event.
    Event {
        /// Event payload.
        event: Event,
    },
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    use super::*;
    use crate::error::ProtocolError;
    use crate::message::{MessageKind, PeerInfo};
    use crate::version::{PROTOCOL_VERSION, VersionMismatch};

    fn sample_peer() -> PeerInfo {
        PeerInfo::new(PeerId::from_bytes([1; 32]), "alice")
    }

    fn sample_session_message() -> SessionMessage {
        SessionMessage::new(
            Seq::new(3),
            42,
            sample_peer(),
            MessageKind::Chat,
            "chat.message",
            b"hi".to_vec(),
            crate::message::SIGNATURE_UNSIGNED,
            crate::message::SIGNATURE_UNSIGNED,
        )
    }

    // ---- RequestId ----

    #[test]
    fn request_id_zero_then_one() {
        assert_eq!(RequestId::ZERO.next(), Some(RequestId::new(1)));
    }

    #[test]
    fn request_id_overflow_returns_none() {
        assert_eq!(RequestId::new(u64::MAX).next(), None);
    }

    // ---- JoinTicket ----

    #[test]
    fn join_ticket_from_str_and_string() {
        let from_str: JoinTicket = "abc".into();
        let from_string: JoinTicket = String::from("abc").into();
        assert_eq!(from_str, from_string);
        assert_eq!(from_str.as_str(), "abc");
    }

    // ---- Request round-trips ----

    #[test]
    fn hello_request_round_trip() {
        let req = Request::Hello {
            client_version: PROTOCOL_VERSION,
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn host_session_request_round_trip() {
        let req = Request::HostSession {
            display_name: "alice".into(),
            session: None,
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn host_session_request_round_trip_with_session_id() {
        let req = Request::HostSession {
            display_name: "alice".into(),
            session: Some(SessionId::from_bytes([7; 16])),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);

        // JSON round-trip too — externally-tagged enum, snake_case fields.
        let json = serde_json::to_string(&req).unwrap();
        let back: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn join_session_request_round_trip() {
        let req = Request::JoinSession {
            display_name: "bob".into(),
            ticket: JoinTicket::from("ticket-blob"),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn list_sessions_request_is_unit_shaped() {
        let req = Request::ListSessions;
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);
        // JSON of a unit variant is a bare string.
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, "\"list_sessions\"");
    }

    #[test]
    fn subscribe_request_round_trip_with_and_without_since() {
        let session = SessionId::from_bytes([7; 16]);
        for since in [None, Some(Seq::new(99))] {
            let req = Request::Subscribe { session, since };
            let bytes = postcard::to_allocvec(&req).unwrap();
            let back: Request = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(req, back);
        }
    }

    #[test]
    fn register_attachment_request_round_trip() {
        let req = Request::RegisterAttachment {
            session: SessionId::from_bytes([8; 16]),
            kind: "artel-fs/workspace/v1".into(),
            payload: b"opaque-bytes".to_vec(),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);

        let json = serde_json::to_string(&req).unwrap();
        let back: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn list_attachments_request_round_trip_with_kind() {
        let req = Request::ListAttachments {
            kind: Some("artel-fs/workspace/v1".into()),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn list_attachments_request_round_trip_without_kind() {
        let req = Request::ListAttachments { kind: None };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);

        // Pin the JSON shape so a future drift to a non-`None` default
        // is caught.
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, "{\"list_attachments\":{\"kind\":null}}");
    }

    #[test]
    fn forget_attachment_request_round_trip() {
        let req = Request::ForgetAttachment {
            session: SessionId::from_bytes([9; 16]),
            kind: "artel-fs/workspace/v1".into(),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn deliver_upgrade_request_round_trip() {
        let req = Request::DeliverUpgrade {
            session: SessionId::from_bytes([0xab; 16]),
            target_peer: PeerId::from_bytes([0xcd; 32]),
            namespace_secret: [0x42; 32],
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);

        let json = serde_json::to_string(&req).unwrap();
        let back: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn upgrade_delivered_response_round_trip() {
        let resp = Response::UpgradeDelivered;
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let back: Response = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, back);
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, "\"upgrade_delivered\"");
    }

    #[test]
    fn publish_workspace_ticket_request_round_trip() {
        let req = Request::PublishWorkspaceTicket {
            session: SessionId::from_bytes([0xab; 16]),
            envelope_bytes: vec![0x42; 512],
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);

        let json = serde_json::to_string(&req).unwrap();
        let back: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn workspace_ticket_published_response_round_trip() {
        let resp = Response::WorkspaceTicketPublished;
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let back: Response = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, back);
        // Unit-shaped: JSON renders as a bare snake_case string.
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, "\"workspace_ticket_published\"");
    }

    #[test]
    fn issue_ticket_request_round_trip() {
        for cap in [
            crate::capability::Capability::Read,
            crate::capability::Capability::ReadWrite,
        ] {
            let req = Request::IssueTicket {
                session: SessionId::from_bytes([0xab; 16]),
                granted_cap: cap,
                expiry_ms: 1_700_000_000_000,
            };
            let bytes = postcard::to_allocvec(&req).unwrap();
            let back: Request = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(req, back);

            let json = serde_json::to_string(&req).unwrap();
            let back: Request = serde_json::from_str(&json).unwrap();
            assert_eq!(req, back);
        }
    }

    #[test]
    fn send_request_round_trip() {
        let req = Request::Send {
            session: SessionId::from_bytes([2; 16]),
            payload: SendPayload {
                kind: MessageKind::Tool,
                action: "tool.exec".into(),
                payload: b"args".to_vec(),
            },
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);
    }

    // ---- SignedSendPayload ----

    #[test]
    fn signed_send_payload_postcard_round_trip() {
        let p = SignedSendPayload {
            timestamp_ms: 1_700_000_000_000,
            kind: MessageKind::Tool,
            action: "tool.exec".into(),
            payload: b"args".to_vec(),
            signature: [0xcd; 64],
        };
        let bytes = postcard::to_allocvec(&p).unwrap();
        let back: SignedSendPayload = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn signed_send_payload_json_round_trip_carries_hex_signature() {
        let p = SignedSendPayload {
            timestamp_ms: 42,
            kind: MessageKind::Chat,
            action: "chat.message".into(),
            payload: b"hi".to_vec(),
            signature: [0xab; 64],
        };
        let json = serde_json::to_string(&p).unwrap();
        // 128-char lowercase hex on the wire — same shape as PeerId.
        assert!(
            json.contains(&format!("\"signature\":\"{}\"", "ab".repeat(64))),
            "json missing hex signature: {json}"
        );
        let back: SignedSendPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(p, back);
    }

    // ---- Response round-trips ----

    #[test]
    fn hello_response_round_trip() {
        let resp = Response::Hello {
            daemon_version: PROTOCOL_VERSION,
            daemon_peer_id: PeerId::from_bytes([9; 32]),
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let back: Response = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, back);
    }

    #[test]
    fn host_session_response_round_trip() {
        let resp = Response::HostSession {
            session: SessionId::from_bytes([3; 16]),
            ticket: JoinTicket::from("xyz"),
            ticket_id: crate::ids::TicketId::from_bytes([7; 16]),
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let back: Response = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, back);
    }

    #[test]
    fn list_sessions_response_round_trip_empty_and_populated() {
        for sessions in [
            Vec::<SessionSummary>::new(),
            vec![SessionSummary {
                id: SessionId::from_bytes([4; 16]),
                is_host: true,
                peer_count: 3,
                last_seq: Some(Seq::new(42)),
            }],
        ] {
            let resp = Response::ListSessions {
                sessions: sessions.clone(),
            };
            let bytes = postcard::to_allocvec(&resp).unwrap();
            let back: Response = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(resp, back);
        }
    }

    #[test]
    fn attachment_registered_response_round_trip() {
        let resp = Response::AttachmentRegistered;
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let back: Response = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, back);
        // Unit-shaped: JSON renders as a bare snake_case string.
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, "\"attachment_registered\"");
    }

    #[test]
    fn attachment_forgotten_response_round_trip() {
        let resp = Response::AttachmentForgotten;
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let back: Response = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, back);
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, "\"attachment_forgotten\"");
    }

    #[test]
    fn attachments_response_round_trip_empty() {
        let resp = Response::Attachments {
            entries: Vec::new(),
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let back: Response = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, back);
    }

    #[test]
    fn attachments_response_round_trip_multi_kind() {
        let resp = Response::Attachments {
            entries: vec![
                Attachment {
                    session: SessionId::from_bytes([1; 16]),
                    kind: "artel-fs/workspace/v1".into(),
                    payload: b"first".to_vec(),
                },
                Attachment {
                    session: SessionId::from_bytes([2; 16]),
                    kind: "other.consumer/thing/v2".into(),
                    // boundary: empty payload
                    payload: Vec::new(),
                },
                Attachment {
                    session: SessionId::from_bytes([3; 16]),
                    kind: "artel-fs/workspace/v1".into(),
                    // size sanity, not pinning a max
                    payload: vec![0xCD; 64 * 1024],
                },
            ],
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let back: Response = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, back);
    }

    #[test]
    fn attachment_payload_round_trip_postcard_uses_bytes_encoding() {
        // If `payload` were serialized as a generic `Vec<u8>` sequence,
        // postcard would emit one byte per element plus tag overhead;
        // with `serde_bytes` it's a length-prefixed byte slice. Pin the
        // tighter encoding so a future drop of the `serde(with = ...)`
        // attribute is caught.
        let attachment = Attachment {
            session: SessionId::from_bytes([0; 16]),
            kind: "k".into(),
            payload: vec![0xAB; 4],
        };
        let bytes = postcard::to_allocvec(&attachment).unwrap();
        // 16-byte session + 1-byte kind length + 1-byte 'k' +
        // 1-byte payload length + 4 payload bytes = 23.
        // Allow a small slack (e.g. variable-length encodings) but
        // catch the >= 4×payload regression a Vec<u8> would cause.
        assert!(
            bytes.len() <= 32,
            "encoded attachment longer than expected ({} bytes): {:?}",
            bytes.len(),
            bytes,
        );
        let back: Attachment = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(attachment, back);
    }

    #[test]
    fn handshake_postcard_indices_are_pinned() {
        // The version handshake is the one IPC exchange that happens
        // BEFORE versions are known to agree: any client build must be
        // able to send Hello to any daemon build and decode the
        // daemon's Hello-or-Error reply (version.rs documents that
        // clients surface VersionMismatch to the user). Postcard
        // encodes enum variants by declaration index, so these
        // variants must keep their wire positions across protocol
        // versions — everything else is gated behind the handshake
        // and may move freely on a PROTOCOL_VERSION bump.
        let hello_req = Request::Hello {
            client_version: PROTOCOL_VERSION,
        };
        assert_eq!(
            postcard::to_allocvec(&hello_req).unwrap()[0],
            0,
            "Request::Hello must stay at variant index 0",
        );

        let hello_resp = Response::Hello {
            daemon_version: PROTOCOL_VERSION,
            daemon_peer_id: PeerId::from_bytes([1; 32]),
        };
        assert_eq!(
            postcard::to_allocvec(&hello_resp).unwrap()[0],
            0,
            "Response::Hello must stay at variant index 0",
        );

        // Index 12 is Error's position since PROTOCOL_VERSION 7 — the
        // last release whose clients are told "upgrade or restart the
        // daemon" via a decodable VersionMismatch reply. New Response
        // variants are appended after Error, never inserted before it.
        let err_resp = Response::Error {
            error: ProtocolError::VersionMismatch(VersionMismatch {
                client: ProtocolVersion::new(7),
                daemon: PROTOCOL_VERSION,
            }),
        };
        assert_eq!(
            postcard::to_allocvec(&err_resp).unwrap()[0],
            12,
            "Response::Error must stay at variant index 12 so \
             pre-v8 clients can decode the VersionMismatch reply",
        );
    }

    #[test]
    fn issued_ticket_response_round_trip() {
        let resp = Response::IssuedTicket {
            ticket: JoinTicket::from("artel:some-ticket-data"),
            ticket_id: crate::ids::TicketId::from_bytes([8; 16]),
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let back: Response = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, back);

        let json = serde_json::to_string(&resp).unwrap();
        let back: Response = serde_json::from_str(&json).unwrap();
        assert_eq!(resp, back);
    }

    #[test]
    fn revoke_ticket_request_round_trip() {
        let req = Request::RevokeTicket {
            session: SessionId::from_bytes([5; 16]),
            ticket_id: crate::ids::TicketId::from_bytes([6; 16]),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);

        let json = serde_json::to_string(&req).unwrap();
        let back: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn list_tickets_request_round_trip() {
        let req = Request::ListTickets {
            session: SessionId::from_bytes([5; 16]),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn ticket_revoked_response_round_trip() {
        let resp = Response::TicketRevoked;
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let back: Response = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, back);
        // Unit-shaped: JSON renders as a bare snake_case string.
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, "\"ticket_revoked\"");
    }

    #[test]
    fn tickets_response_round_trip_empty_and_populated() {
        use crate::ticket::{TicketEntry, TicketStatus};
        for entries in [
            Vec::<TicketEntry>::new(),
            vec![
                TicketEntry {
                    ticket_id: crate::ids::TicketId::from_bytes([1; 16]),
                    granted_cap: crate::capability::Capability::ReadWrite,
                    expiry_ms: 0,
                    issued_at_ms: 1_700_000_000_000,
                    status: TicketStatus::Active,
                    used_by: vec![],
                },
                TicketEntry {
                    ticket_id: crate::ids::TicketId::from_bytes([2; 16]),
                    granted_cap: crate::capability::Capability::Read,
                    expiry_ms: 1_800_000_000_000,
                    issued_at_ms: 1_700_000_000_001,
                    status: TicketStatus::Revoked,
                    used_by: vec![PeerId::from_bytes([3; 32]), PeerId::from_bytes([4; 32])],
                },
            ],
        ] {
            let resp = Response::Tickets {
                entries: entries.clone(),
            };
            let bytes = postcard::to_allocvec(&resp).unwrap();
            let back: Response = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(resp, back);

            let json = serde_json::to_string(&resp).unwrap();
            let back: Response = serde_json::from_str(&json).unwrap();
            assert_eq!(resp, back);
        }
    }

    #[test]
    fn error_response_carries_protocol_error() {
        let resp = Response::Error {
            error: ProtocolError::VersionMismatch(VersionMismatch {
                client: PROTOCOL_VERSION,
                daemon: ProtocolVersion::new(99),
            }),
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let back: Response = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, back);
    }

    // ---- Event round-trips ----

    #[test]
    fn event_message_round_trip() {
        let ev = Event::Message {
            session: SessionId::from_bytes([5; 16]),
            message: sample_session_message(),
        };
        let bytes = postcard::to_allocvec(&ev).unwrap();
        let back: Event = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn event_peer_joined_round_trip() {
        let ev = Event::PeerJoined {
            session: SessionId::from_bytes([6; 16]),
            peer: sample_peer(),
        };
        let bytes = postcard::to_allocvec(&ev).unwrap();
        let back: Event = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn event_gap_round_trip() {
        let ev = Event::Gap {
            session: SessionId::from_bytes([7; 16]),
        };
        let bytes = postcard::to_allocvec(&ev).unwrap();
        let back: Event = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn event_gap_is_appended_at_index_four() {
        // M3 Part B: `Gap` is appended last so the existing variants
        // keep their postcard indices (Message=0, PeerJoined=1,
        // PeerLeft=2, SessionClosed=3, Gap=4). Pre-existing subscribers
        // decode the older variants unchanged.
        let gap = Event::Gap {
            session: SessionId::from_bytes([1; 16]),
        };
        assert_eq!(postcard::to_allocvec(&gap).unwrap()[0], 4);
    }

    // ---- WireMessage envelope ----

    #[test]
    fn wire_message_request_round_trip() {
        let frame = WireMessage::Request {
            id: RequestId::new(1),
            request: Request::Hello {
                client_version: PROTOCOL_VERSION,
            },
        };
        let bytes = postcard::to_allocvec(&frame).unwrap();
        let back: WireMessage = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(frame, back);
    }

    #[test]
    fn wire_message_response_correlates_id() {
        let id = RequestId::new(123);
        let frame = WireMessage::Response {
            id,
            response: Response::Sent {
                session: SessionId::from_bytes([0; 16]),
                seq: Seq::new(7),
            },
        };
        let bytes = postcard::to_allocvec(&frame).unwrap();
        let back: WireMessage = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(frame, back);
        if let WireMessage::Response { id: back_id, .. } = back {
            assert_eq!(back_id, id);
        } else {
            panic!("expected Response");
        }
    }

    #[test]
    fn wire_message_event_has_no_id() {
        let frame = WireMessage::Event {
            event: Event::SessionClosed {
                session: SessionId::from_bytes([0; 16]),
            },
        };
        let bytes = postcard::to_allocvec(&frame).unwrap();
        let back: WireMessage = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(frame, back);
    }

    #[test]
    fn wire_message_unknown_variant_rejected() {
        let bad = "{\"made_up\":{}}";
        let result: Result<WireMessage, _> = serde_json::from_str(bad);
        assert!(result.is_err());
    }

    // ---- Property-based ----

    fn arb_send_payload() -> impl Strategy<Value = SendPayload> {
        (
            prop_oneof![
                Just(MessageKind::Chat),
                Just(MessageKind::Tool),
                Just(MessageKind::System),
            ],
            "[\\PC]{0,32}",
            proptest::collection::vec(any::<u8>(), 0..256),
        )
            .prop_map(|(kind, action, payload)| SendPayload {
                kind,
                action,
                payload,
            })
    }

    fn arb_display_name() -> impl Strategy<Value = String> {
        "[\\PC]{0,32}".prop_map(String::from)
    }

    fn arb_request() -> impl Strategy<Value = Request> {
        prop_oneof![
            any::<u32>().prop_map(|v| Request::Hello {
                client_version: ProtocolVersion::new(v),
            }),
            (arb_display_name(), proptest::option::of(any::<[u8; 16]>())).prop_map(|(name, s)| {
                Request::HostSession {
                    display_name: name,
                    session: s.map(SessionId::from_bytes),
                }
            }),
            (arb_display_name(), "[\\PC]{0,128}").prop_map(|(name, ticket)| {
                Request::JoinSession {
                    display_name: name,
                    ticket: JoinTicket::from(ticket),
                }
            }),
            Just(Request::ListSessions),
            (any::<[u8; 16]>(), proptest::option::of(any::<u64>())).prop_map(|(s, since)| {
                Request::Subscribe {
                    session: SessionId::from_bytes(s),
                    since: since.map(Seq::new),
                }
            }),
            (any::<[u8; 16]>(), arb_send_payload()).prop_map(|(s, payload)| Request::Send {
                session: SessionId::from_bytes(s),
                payload,
            }),
            any::<[u8; 16]>().prop_map(|s| Request::LeaveSession {
                session: SessionId::from_bytes(s),
            }),
            (
                any::<[u8; 16]>(),
                "[\\PC]{0,32}",
                proptest::collection::vec(any::<u8>(), 0..256),
            )
                .prop_map(|(s, kind, payload)| Request::RegisterAttachment {
                    session: SessionId::from_bytes(s),
                    kind,
                    payload,
                }),
            proptest::option::of("[\\PC]{0,32}").prop_map(|kind| Request::ListAttachments { kind }),
            (any::<[u8; 16]>(), "[\\PC]{0,32}").prop_map(|(s, kind)| {
                Request::ForgetAttachment {
                    session: SessionId::from_bytes(s),
                    kind,
                }
            }),
            (any::<[u8; 16]>(), any::<bool>(), any::<u64>()).prop_map(|(s, is_rw, expiry_ms)| {
                Request::IssueTicket {
                    session: SessionId::from_bytes(s),
                    granted_cap: if is_rw {
                        crate::capability::Capability::ReadWrite
                    } else {
                        crate::capability::Capability::Read
                    },
                    expiry_ms,
                }
            },),
            (any::<[u8; 16]>(), any::<[u8; 32]>(), any::<[u8; 32]>()).prop_map(
                |(s, peer, secret)| Request::DeliverUpgrade {
                    session: SessionId::from_bytes(s),
                    target_peer: PeerId::from_bytes(peer),
                    namespace_secret: secret,
                },
            ),
            (
                any::<[u8; 16]>(),
                proptest::collection::vec(any::<u8>(), 0..512),
            )
                .prop_map(|(s, envelope_bytes)| {
                    Request::PublishWorkspaceTicket {
                        session: SessionId::from_bytes(s),
                        envelope_bytes,
                    }
                }),
        ]
    }

    proptest! {
        #[test]
        fn request_round_trip(req in arb_request()) {
            let bytes = postcard::to_allocvec(&req).unwrap();
            let back: Request = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(req, back);
        }

        #[test]
        fn request_json_round_trip(req in arb_request()) {
            let json = serde_json::to_string(&req).unwrap();
            let back: Request = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(req, back);
        }

        #[test]
        fn request_id_postcard_round_trip(v in any::<u64>()) {
            let id = RequestId::new(v);
            let bytes = postcard::to_allocvec(&id).unwrap();
            let back: RequestId = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(id, back);
        }

        #[test]
        fn wire_message_round_trip(req in arb_request(), id in any::<u64>()) {
            let frame = WireMessage::Request {
                id: RequestId::new(id),
                request: req,
            };
            let bytes = postcard::to_allocvec(&frame).unwrap();
            let back: WireMessage = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(frame, back);
        }

        #[test]
        fn request_id_next_is_monotonic(v in 0u64..u64::MAX) {
            let id = RequestId::new(v);
            let next = id.next().unwrap();
            prop_assert!(next > id);
            prop_assert_eq!(next.get(), v + 1);
        }
    }
}
