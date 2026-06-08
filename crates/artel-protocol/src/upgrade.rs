//! Direct-stream upgrade protocol wire types.
//!
//! The host daemon delivers the `NamespaceSecret` to a promoted joiner
//! over a dedicated QUIC stream (ALPN [`UPGRADE_ALPN`]) rather than
//! broadcasting it over gossip. This module defines the on-the-wire frame
//! and constants shared between sender and receiver.

use serde::{Deserialize, Serialize};

use crate::SessionId;

/// ALPN for the direct-stream upgrade protocol.
pub const UPGRADE_ALPN: &[u8] = b"artel/upgrade/1";

/// Single-byte ACK sent from target → host after successful import.
pub const UPGRADE_ACK: u8 = 0x01;

/// Frame sent from host → target over the direct stream.
///
/// Serialized with postcard, length-prefixed (4-byte LE) on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeFrame {
    pub session_id: SessionId,
    #[serde(with = "serde_bytes")]
    pub namespace_secret: [u8; 32],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrade_frame_postcard_round_trip() {
        let frame = UpgradeFrame {
            session_id: SessionId::from_bytes([0xab; 16]),
            namespace_secret: [0x42; 32],
        };
        let bytes = postcard::to_allocvec(&frame).unwrap();
        let back: UpgradeFrame = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(frame, back);
    }

    #[test]
    fn upgrade_frame_postcard_deterministic_encoding() {
        let frame = UpgradeFrame {
            session_id: SessionId::from_bytes([1; 16]),
            namespace_secret: [2; 32],
        };
        let a = postcard::to_allocvec(&frame).unwrap();
        let b = postcard::to_allocvec(&frame).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn upgrade_frame_corrupt_bytes_rejected() {
        let result: Result<UpgradeFrame, _> = postcard::from_bytes(&[0xff; 4]);
        assert!(result.is_err());
    }

    #[test]
    fn upgrade_alpn_is_valid_utf8() {
        let s = std::str::from_utf8(UPGRADE_ALPN).unwrap();
        assert_eq!(s, "artel/upgrade/1");
    }

    #[test]
    fn upgrade_ack_value() {
        assert_eq!(UPGRADE_ACK, 0x01);
    }
}
