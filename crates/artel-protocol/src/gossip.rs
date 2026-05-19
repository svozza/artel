//! Wire envelope for inter-daemon gossip traffic.
//!
//! When two daemons share a session via iroh-gossip, every payload
//! they exchange on the topic is a postcard-encoded
//! [`GossipFrame`]. Lives in `artel-protocol` so both sides agree
//! on the bytes without pulling iroh into the protocol crate.
//!
//! ## Frame model
//!
//! Phase 2c-2b ships a one-direction fanout only:
//!
//! - **Host → joiners.** The host wraps each freshly-sequenced
//!   [`SessionMessage`] in a [`GossipFrame::Message`] and broadcasts
//!   it. Joiners decode and forward to their local IPC subscribers.
//!
//! Future variants ([`GossipFrame::SendRequest`], for joiner-issued
//! sends) are not encoded yet but the enum is non-exhaustive on
//! deserialise: a v1 daemon receiving a future variant treats it as
//! an unknown frame and skips it via the `Unknown` fallback.
//!
//! ## Versioning
//!
//! Carried as the leading byte ([`GOSSIP_FRAME_VERSION`]). Bumped
//! when a structural change makes older daemons unable to parse.

use serde::{Deserialize, Serialize};

use crate::message::SessionMessage;

/// Current envelope version. Receivers that see a higher version
/// drop the frame.
pub const GOSSIP_FRAME_VERSION: u8 = 1;

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

    #[test]
    fn message_frame_round_trips() {
        let frame = message_frame(sample_msg());
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
