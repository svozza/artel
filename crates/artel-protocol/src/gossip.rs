//! Wire frames for inter-daemon gossip traffic.
//!
//! When two daemons share a session via iroh-gossip, every payload
//! they exchange on the topic is a postcard-encoded [`GossipBody`].
//! Lives in `artel-protocol` so both sides agree on the bytes
//! without pulling iroh into the protocol crate.
//!
//! ## Frame model
//!
//! All inter-daemon traffic — both directions — rides the same
//! gossip topic. The protocol stays symmetric so we can drop the
//! host-as-sequencer model later without ripping out a transport.
//!
//! - **Host → all.** Each freshly-sequenced [`SessionMessage`] is
//!   wrapped in [`GossipBody::Message`] and broadcast. Joiners
//!   decode and forward to their local IPC subscribers; the
//!   originating joiner uses it as the data path for its own
//!   outbound send (see below).
//! - **Joiner → host.** A joiner-side `Send` IPC call publishes a
//!   [`GossipBody::SendRequest`] carrying a `req_id`, the
//!   originating peer info, and the application payload. The host's
//!   bridge accepts the request, drives `Registry::send` locally
//!   (which in turn produces a [`GossipBody::Message`] broadcast),
//!   and replies on the same topic with [`GossipBody::SendAck`] —
//!   carrying the assigned [`SessionMessage`] on success or the
//!   host's [`ProtocolError`] on rejection. The joiner correlates
//!   the ack to its in-flight oneshot via `req_id`.
//! - **Joiner → host (membership).** Once the gossip mesh is up, a
//!   joiner broadcasts a [`GossipBody::JoinAnnouncement`] so the
//!   host can admit them to membership and emit `PeerJoined`
//!   eagerly, rather than waiting for their first `SendRequest`.
//!
//! ## No wire versioning yet
//!
//! Pre-1.0, both daemons rebuild together; we have zero on-the-wire
//! compatibility surface to defend. Adding a wire-version envelope
//! later is ~30 lines (struct + decode check + error variant) and
//! by then we'll have a real story for capability negotiation
//! anyway. Until then, an unrecognised frame surfaces as
//! [`GossipFrameError::Malformed`] from postcard and is dropped at
//! the bridge with a warn log.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ProtocolError;
use crate::ids::Seq;
use crate::message::{PeerInfo, SessionMessage};
use crate::rpc::SendPayload;

/// One frame on a session's gossip topic. Externally tagged so
/// postcard can serialise it (see workspace memo on postcard +
/// `tag`/`content`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GossipBody {
    /// Authoritative session message published by the host.
    /// Carries the full [`SessionMessage`] including its assigned
    /// `seq` so receivers can deduplicate and order.
    Message(SessionMessage),

    /// Joiner-published request asking the host to append a
    /// message on its behalf. Correlated with a matching
    /// [`GossipBody::SendAck`] via `req_id`.
    SendRequest {
        /// Joiner-assigned correlation id. Round-tripped verbatim
        /// in the matching [`GossipBody::SendAck`].
        req_id: Uuid,
        /// The originating peer (the joiner client's identity).
        /// We trust this in v1 — signing is Phase 4 territory. The
        /// host writes the resulting [`SessionMessage`] with this
        /// peer in the `peer` field.
        peer: PeerInfo,
        /// What the joiner asked to append.
        payload: SendPayload,
    },

    /// Host-published reply to a [`GossipBody::SendRequest`].
    /// Carries the freshly-sequenced [`SessionMessage`] on success
    /// or the host's [`ProtocolError`] on rejection.
    SendAck {
        /// Correlation id copied from the originating request.
        req_id: Uuid,
        /// The host's verdict. `Ok` carries the same
        /// [`SessionMessage`] that's also broadcast as a parallel
        /// [`GossipBody::Message`]; `Err` is the host's wire
        /// rejection so the joiner sees the real reason rather
        /// than a generic timeout.
        result: Result<SessionMessage, ProtocolError>,
    },

    /// Joiner-published announcement that they have subscribed to
    /// this session's topic. The host admits the peer to the
    /// session's membership on receipt (idempotent, so a duplicate
    /// announcement is harmless). Lets the host emit `PeerJoined`
    /// proactively rather than lazily on the joiner's first
    /// `SendRequest`.
    JoinAnnouncement {
        /// The joining peer's identity.
        peer: PeerInfo,
        /// Joiner-local wall-clock millis at the moment they
        /// finished subscribing. Carried so the host's
        /// `PeerJoined` event has a meaningful timestamp; not
        /// otherwise interpreted.
        timestamp_ms: u64,
    },

    /// Host-published broadcast that the session has been closed.
    /// Sent on the way out of `Registry::leave` (host-closes path)
    /// so joiners learn the truth proactively instead of
    /// discovering it via a `SendRequest` that never gets acked.
    /// On receipt, joiners drop their local mirror and emit
    /// `Event::SessionClosed` to their IPC subscribers.
    SessionClosed,

    /// Joiner-published request asking the host to replay every
    /// committed message with `seq > since`. The host's response
    /// is plain [`GossipBody::Message`] frames re-broadcast on the
    /// same topic; the joiner's mirror dedups by seq, so a Message
    /// that arrives twice (once live, once via replay) is harmless.
    ///
    /// Carries no correlation id: replay is fire-and-forget, and
    /// every `Message` already carries its own seq for ordering.
    /// Other joiners on the topic see the replay traffic too and
    /// dedup-skip it — wasteful but not incorrect; can be tightened
    /// later (e.g. unicast over a dedicated stream) if it matters.
    Replay {
        /// Highest seq the joiner has already seen. The host
        /// re-broadcasts every committed message with `seq > since`.
        /// Use [`Seq::ZERO`] to ask for the full log.
        since: Seq,
    },
}

/// Errors [`decode`] may return.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GossipFrameError {
    /// Bytes did not deserialize as a [`GossipBody`].
    #[error("malformed gossip frame: {0}")]
    Malformed(String),
}

/// Encode `body` to the bytes broadcast on the gossip topic.
#[must_use]
pub fn encode(body: &GossipBody) -> Vec<u8> {
    postcard::to_stdvec(body).expect("postcard encode of fixed-shape types")
}

/// Decode `bytes` into a [`GossipBody`].
pub fn decode(bytes: &[u8]) -> Result<GossipBody, GossipFrameError> {
    postcard::from_bytes(bytes).map_err(|e| GossipFrameError::Malformed(e.to_string()))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::ids::{PeerId, Seq};
    use crate::message::{MessageKind, PeerInfo};

    fn sample_msg() -> SessionMessage {
        SessionMessage::new(
            Seq::new(7),
            12_345,
            PeerInfo::new(PeerId::from_bytes([2; 32]), "alice"),
            MessageKind::Chat,
            "chat.message",
            b"hi".to_vec(),
        )
    }

    fn sample_payload() -> SendPayload {
        SendPayload {
            kind: MessageKind::Chat,
            action: "chat.message".into(),
            payload: b"hi from joiner".to_vec(),
        }
    }

    #[test]
    fn message_frame_round_trips() {
        let body = GossipBody::Message(sample_msg());
        let bytes = encode(&body);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn send_request_frame_round_trips() {
        let body = GossipBody::SendRequest {
            req_id: Uuid::from_u128(0x4242_4242_4242_4242_4242_4242_4242_4242),
            peer: PeerInfo::new(PeerId::from_bytes([3; 32]), "bob"),
            payload: sample_payload(),
        };
        let bytes = encode(&body);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn send_ack_ok_frame_round_trips() {
        let body = GossipBody::SendAck {
            req_id: Uuid::from_u128(0x1),
            result: Ok(sample_msg()),
        };
        let bytes = encode(&body);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn send_ack_err_frame_round_trips() {
        let body = GossipBody::SendAck {
            req_id: Uuid::from_u128(0x2),
            result: Err(ProtocolError::Internal("session closed".into())),
        };
        let bytes = encode(&body);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn session_closed_frame_round_trips() {
        let body = GossipBody::SessionClosed;
        let bytes = encode(&body);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn replay_frame_round_trips() {
        let body = GossipBody::Replay {
            since: Seq::new(42),
        };
        let bytes = encode(&body);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn join_announcement_frame_round_trips() {
        let body = GossipBody::JoinAnnouncement {
            peer: PeerInfo::new(PeerId::from_bytes([4; 32]), "carol"),
            timestamp_ms: 1_700_000_000_000,
        };
        let bytes = encode(&body);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn truncated_bytes_error_as_malformed() {
        let bytes = encode(&GossipBody::Message(sample_msg()));
        let truncated = &bytes[..bytes.len() / 2];
        match decode(truncated) {
            Err(GossipFrameError::Malformed(_)) => {}
            other => panic!("expected Malformed, got {other:?}"),
        }
    }
}
