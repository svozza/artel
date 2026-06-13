//! Direct-stream delivery protocol wire types.
//!
//! The host daemon delivers session-key material to a peer over a
//! dedicated QUIC stream (ALPN [`UPGRADE_ALPN`]) rather than
//! broadcasting it over gossip — the one sanctioned exception to the
//! gossip-only inter-daemon rule. Two payload kinds ride the channel:
//! the `NamespaceSecret` for a promoted RW joiner
//! ([`DeliveryFrame::Secret`]) and the read-capability workspace
//! ticket envelope delivered at admission
//! ([`DeliveryFrame::WorkspaceTicket`]). This module defines the
//! on-the-wire frame and constants shared between sender and
//! receiver.

use serde::{Deserialize, Serialize};

use crate::{PeerId, SessionId};

/// ALPN for the direct-stream delivery protocol.
///
/// `/1` → `/2` with the [`DeliveryFrame`] enum cutover (2026-06-12
/// revoked-lurker slice): the bare `UpgradeFrame` wire shape was
/// replaced outright. Mixed-version daemons fail ALPN negotiation —
/// intended (alpha, no interop shims).
pub const UPGRADE_ALPN: &[u8] = b"artel/upgrade/2";

/// Single-byte ACK sent from target → host after successful import.
pub const UPGRADE_ACK: u8 = 0x01;

/// Cap on one encoded [`DeliveryFrame`] (postcard bytes, excluding
/// the 4-byte length prefix).
///
/// Shared by the sender and the receiving protocol handler so the two
/// can't drift: a frame one side will emit, the other will accept.
/// Raised from the secret-only 1 KiB when `WorkspaceTicket` joined the
/// channel — envelopes carry user-authored `PathRules` globs.
pub const MAX_DELIVERY_FRAME: usize = 64 * 1024;

/// Cap on the raw `WorkspaceTicketEnvelope` bytes accepted anywhere
/// they flow.
///
/// Enforced at producer encode (`artel-fs`), publish ingress, store
/// persistence, and unicast delivery. Strictly smaller than
/// [`MAX_DELIVERY_FRAME`] — the 64-byte headroom covers the
/// [`DeliveryFrame::WorkspaceTicket`] framing (variant tag + 16-byte
/// session id + length varint), so an envelope that passes this cap
/// is deliverable by construction. One constant for all sites: a
/// persisted envelope the wire would reject, or a published envelope
/// the store would refuse to load back, are both bugs this shared
/// bound makes unrepresentable.
pub const WORKSPACE_TICKET_ENVELOPE_MAX: usize = MAX_DELIVERY_FRAME - 64;

/// Frame sent from host → target over the direct stream.
///
/// Serialized with postcard, length-prefixed (4-byte LE) on the wire.
/// Externally tagged (serde default) — postcard cannot encode
/// adjacently- or internally-tagged enums.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliveryFrame {
    /// `NamespaceSecret` delivery to a promoted RW joiner. The inner
    /// [`UpgradeFrame`] is the pre-`/2` wire struct, unchanged.
    Secret(UpgradeFrame),
    /// Read-capability `WorkspaceTicketEnvelope` delivered host→peer
    /// at admission (and on host-side publish to current members).
    /// The envelope bytes are opaque at this layer — the daemon
    /// persists and forwards them; only `artel-fs` decodes.
    ///
    /// Receivers cap the whole frame at 64 KiB (envelopes carry
    /// user-authored `PathRules` globs; the old 1 KiB secret-only
    /// cap is too tight). A workspace whose rules exceed 64 KiB is
    /// misconfigured.
    WorkspaceTicket {
        /// Session the envelope belongs to.
        session_id: SessionId,
        /// postcard-encoded `WorkspaceTicketEnvelope`, opaque here.
        #[serde(with = "serde_bytes")]
        envelope_bytes: Vec<u8>,
    },
}

/// `NamespaceSecret` payload of [`DeliveryFrame::Secret`].
///
/// Was the whole `/1` wire frame; kept as the inner struct so its
/// field order, types, and round-trip tests carry over unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeFrame {
    pub session_id: SessionId,
    #[serde(with = "serde_bytes")]
    pub namespace_secret: [u8; 32],
}

/// Payload of the synthetic `workspace.upgrade` system message the
/// joiner daemon injects into its session event stream once it has
/// received the `NamespaceSecret` over the direct stream.
///
/// The daemon serializes this with postcard; `artel-fs`'s
/// `cap_listener` deserializes it to learn which peer was promoted and
/// the secret to import. Shared here so the two crates can't drift on
/// field order or type without a compile break.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradePayload {
    pub target_peer: PeerId,
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
    fn delivery_frame_secret_round_trip() {
        let frame = DeliveryFrame::Secret(UpgradeFrame {
            session_id: SessionId::from_bytes([0xab; 16]),
            namespace_secret: [0x42; 32],
        });
        let bytes = postcard::to_allocvec(&frame).unwrap();
        let back: DeliveryFrame = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(frame, back);
    }

    #[test]
    fn delivery_frame_workspace_ticket_round_trip() {
        let frame = DeliveryFrame::WorkspaceTicket {
            session_id: SessionId::from_bytes([0xcd; 16]),
            envelope_bytes: vec![0x99; 300],
        };
        let bytes = postcard::to_allocvec(&frame).unwrap();
        let back: DeliveryFrame = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(frame, back);
    }

    #[test]
    fn delivery_frame_externally_tagged_shape_is_pinned() {
        // postcard encodes the enum variant as a leading varint
        // discriminant. Pin both indices so neither variant can move
        // without a deliberate wire break.
        let secret = DeliveryFrame::Secret(UpgradeFrame {
            session_id: SessionId::from_bytes([1; 16]),
            namespace_secret: [2; 32],
        });
        assert_eq!(
            postcard::to_allocvec(&secret).unwrap()[0],
            0,
            "Secret must stay at variant index 0",
        );

        let ticket = DeliveryFrame::WorkspaceTicket {
            session_id: SessionId::from_bytes([1; 16]),
            envelope_bytes: vec![0xaa; 4],
        };
        assert_eq!(
            postcard::to_allocvec(&ticket).unwrap()[0],
            1,
            "WorkspaceTicket must stay at variant index 1",
        );
    }

    #[test]
    fn delivery_frame_secret_payload_matches_bare_upgrade_frame_encoding() {
        // The Secret variant is tag byte + the old bare UpgradeFrame
        // bytes. Pin that so the inner struct can't drift from its
        // pre-/2 encoding.
        let inner = UpgradeFrame {
            session_id: SessionId::from_bytes([7; 16]),
            namespace_secret: [9; 32],
        };
        let bare = postcard::to_allocvec(&inner).unwrap();
        let framed = postcard::to_allocvec(&DeliveryFrame::Secret(inner)).unwrap();
        assert_eq!(&framed[1..], bare.as_slice());
    }

    #[test]
    fn delivery_frame_unknown_variant_byte_rejected() {
        // Variant index 2 doesn't exist; postcard must reject.
        let result: Result<DeliveryFrame, _> = postcard::from_bytes(&[0x02, 0x00, 0x00]);
        assert!(result.is_err());
    }

    #[test]
    fn delivery_frame_workspace_ticket_uses_bytes_encoding() {
        // serde_bytes ⇒ length-prefixed byte slice, not a per-element
        // seq. 16-byte session + 1 tag + ~2 length bytes + payload.
        let frame = DeliveryFrame::WorkspaceTicket {
            session_id: SessionId::from_bytes([0; 16]),
            envelope_bytes: vec![0xab; 8],
        };
        let bytes = postcard::to_allocvec(&frame).unwrap();
        assert!(
            bytes.len() <= 32,
            "encoded frame longer than expected ({} bytes)",
            bytes.len(),
        );
    }

    #[test]
    fn envelope_cap_leaves_room_for_delivery_framing() {
        // An envelope at the producer/store cap must still fit inside
        // a DeliveryFrame under the receiver's MAX_DELIVERY_FRAME —
        // otherwise an envelope that persists and loads fine could
        // never be delivered. Encode the worst case (max-size
        // envelope) and assert the framed bytes clear the wire cap.
        let frame = DeliveryFrame::WorkspaceTicket {
            session_id: SessionId::from_bytes([0xcd; 16]),
            envelope_bytes: vec![0u8; WORKSPACE_TICKET_ENVELOPE_MAX],
        };
        let encoded = postcard::to_allocvec(&frame).unwrap();
        assert!(
            encoded.len() <= MAX_DELIVERY_FRAME,
            "max envelope ({WORKSPACE_TICKET_ENVELOPE_MAX}) frames to {} bytes, over the {MAX_DELIVERY_FRAME} wire cap",
            encoded.len(),
        );
    }

    #[test]
    fn upgrade_alpn_is_valid_utf8_v2() {
        let s = std::str::from_utf8(UPGRADE_ALPN).unwrap();
        assert_eq!(s, "artel/upgrade/2");
    }

    #[test]
    fn upgrade_ack_value() {
        assert_eq!(UPGRADE_ACK, 0x01);
    }

    #[test]
    fn upgrade_payload_postcard_round_trip() {
        let payload = UpgradePayload {
            target_peer: PeerId::from_bytes([0x11; 32]),
            namespace_secret: [0x42; 32],
        };
        let bytes = postcard::to_allocvec(&payload).unwrap();
        let back: UpgradePayload = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(payload, back);
    }
}
