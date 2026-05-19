//! Wire envelope for inter-daemon gossip traffic.
//!
//! When two daemons share a session via iroh-gossip, every payload
//! they exchange on the topic is a postcard-encoded
//! [`GossipFrame`]. Lives in `artel-protocol` so both sides agree
//! on the bytes without pulling iroh into the protocol crate.
//!
//! ## Frame model
//!
//! Phase 2c-2c rounds out send fanout in both directions, all over
//! the same gossip topic:
//!
//! - **Host → all.** Each freshly-sequenced [`SessionMessage`] is
//!   wrapped in [`GossipBody::Message`] and broadcast. Joiners
//!   decode and forward to their local IPC subscribers; the
//!   originating joiner uses it as the ack for its own outbound
//!   send (see below).
//! - **Joiner → host.** A joiner-side `Send` IPC call publishes a
//!   [`GossipBody::SendRequest`] carrying a `req_id`, the
//!   originating peer info, and the application payload. The host's
//!   bridge accepts the request, drives `Registry::send` locally
//!   (which in turn produces a [`GossipBody::Message`] broadcast),
//!   and replies on the same topic with [`GossipBody::SendAck`] —
//!   carrying the assigned [`SessionMessage`] on success or the
//!   host's [`ProtocolError`] on rejection. The joiner correlates
//!   the ack to its in-flight oneshot via `req_id`.
//!
//! Routing everything through gossip (rather than a dedicated
//! direct-QUIC sidechannel) is deliberate: the protocol stays
//! symmetric so we can drop the host-as-sequencer model later
//! without ripping out a transport.
//!
//! ## Versioning
//!
//! Carried as the leading byte ([`GOSSIP_FRAME_VERSION`]). Bumped
//! when a structural change makes older daemons unable to parse.
//! v2 added [`GossipBody::SendRequest`] and [`GossipBody::SendAck`].

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ProtocolError;
use crate::message::{PeerInfo, SessionMessage};
use crate::rpc::SendPayload;

/// Current envelope version. Receivers that see a different version
/// drop the frame with [`GossipFrameError::UnsupportedVersion`]. v2
/// added the joiner→host send path.
pub const GOSSIP_FRAME_VERSION: u8 = 2;

/// One frame on a session's gossip topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GossipFrame {
    /// Envelope version. Only [`GOSSIP_FRAME_VERSION`] is currently
    /// understood; receivers reject anything else with
    /// [`GossipFrameError::UnsupportedVersion`].
    pub version: u8,
    /// The actual payload.
    pub body: GossipBody,
}

/// Frame payloads. Externally tagged (postcard-friendly).
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
}

/// Errors [`decode`] may return.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GossipFrameError {
    /// Bytes did not deserialize as a [`GossipFrame`].
    #[error("malformed gossip frame: {0}")]
    Malformed(String),

    /// Frame version does not match what this build understands.
    #[error("unsupported gossip frame version {0} (this build speaks v{GOSSIP_FRAME_VERSION})")]
    UnsupportedVersion(u8),
}

/// Encode `frame` to the bytes broadcast on the gossip topic.
#[must_use]
pub fn encode(frame: &GossipFrame) -> Vec<u8> {
    postcard::to_stdvec(frame).expect("postcard encode of fixed-shape types")
}

/// Decode `bytes` into a [`GossipFrame`]. Rejects unknown versions
/// up front so we never partially-deserialize garbage.
pub fn decode(bytes: &[u8]) -> Result<GossipFrame, GossipFrameError> {
    let frame: GossipFrame =
        postcard::from_bytes(bytes).map_err(|e| GossipFrameError::Malformed(e.to_string()))?;
    if frame.version != GOSSIP_FRAME_VERSION {
        return Err(GossipFrameError::UnsupportedVersion(frame.version));
    }
    Ok(frame)
}

/// Convenience: build a [`GossipFrame`] wrapping a session message
/// at the current envelope version.
#[must_use]
pub const fn message_frame(msg: SessionMessage) -> GossipFrame {
    GossipFrame {
        version: GOSSIP_FRAME_VERSION,
        body: GossipBody::Message(msg),
    }
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
        let frame = message_frame(sample_msg());
        let bytes = encode(&frame);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn send_request_frame_round_trips() {
        let frame = GossipFrame {
            version: GOSSIP_FRAME_VERSION,
            body: GossipBody::SendRequest {
                req_id: Uuid::from_u128(0x4242_4242_4242_4242_4242_4242_4242_4242),
                peer: PeerInfo::new(PeerId::from_bytes([3; 32]), "bob"),
                payload: sample_payload(),
            },
        };
        let bytes = encode(&frame);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn send_ack_ok_frame_round_trips() {
        let frame = GossipFrame {
            version: GOSSIP_FRAME_VERSION,
            body: GossipBody::SendAck {
                req_id: Uuid::from_u128(0x1),
                result: Ok(sample_msg()),
            },
        };
        let bytes = encode(&frame);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn send_ack_err_frame_round_trips() {
        let frame = GossipFrame {
            version: GOSSIP_FRAME_VERSION,
            body: GossipBody::SendAck {
                req_id: Uuid::from_u128(0x2),
                result: Err(ProtocolError::Internal("session closed".into())),
            },
        };
        let bytes = encode(&frame);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn unknown_version_byte_errors_clearly() {
        // Hand-craft a frame with version 99, encode, and verify
        // decode rejects without touching the body.
        let bogus = GossipFrame {
            version: 99,
            body: GossipBody::Message(sample_msg()),
        };
        let bytes = postcard::to_stdvec(&bogus).unwrap();
        assert_eq!(
            decode(&bytes),
            Err(GossipFrameError::UnsupportedVersion(99))
        );
    }

    #[test]
    fn v1_frame_is_rejected_by_v2_decoder() {
        // A peer running an older build still emits version=1 on
        // the wire. We must surface that as UnsupportedVersion
        // rather than misparsing — the body shape changed
        // (variants added) so postcard's discriminator is no
        // longer trustworthy across versions.
        let stale = GossipFrame {
            version: 1,
            body: GossipBody::Message(sample_msg()),
        };
        let bytes = postcard::to_stdvec(&stale).unwrap();
        assert_eq!(decode(&bytes), Err(GossipFrameError::UnsupportedVersion(1)));
    }

    #[test]
    fn truncated_bytes_error_as_malformed() {
        let frame = message_frame(sample_msg());
        let bytes = encode(&frame);
        let truncated = &bytes[..bytes.len() / 2];
        match decode(truncated) {
            Err(GossipFrameError::Malformed(_)) => {}
            other => panic!("expected Malformed, got {other:?}"),
        }
    }
}
