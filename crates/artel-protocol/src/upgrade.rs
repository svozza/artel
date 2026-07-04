//! Direct-stream delivery protocol wire types.
//!
//! The host daemon delivers session-key material to a peer over a
//! dedicated QUIC stream (ALPN [`UPGRADE_ALPN`]) rather than
//! broadcasting it over gossip — the one sanctioned exception to the
//! gossip-only inter-daemon rule. Four payload kinds ride the channel:
//! the `NamespaceSecret` for a promoted RW joiner
//! ([`DeliveryFrame::Secret`]), the read-capability workspace ticket
//! envelope delivered at admission
//! ([`DeliveryFrame::WorkspaceTicket`]), the cooperative RW → Read
//! demotion notice ([`DeliveryFrame::Downgrade`]), and the rotated
//! namespace ticket for a surviving RW peer
//! ([`DeliveryFrame::Rotate`]). This module defines the on-the-wire
//! frame and constants shared between sender and receiver.

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
/// Enforced by the receiving protocol handler before it allocates the
/// payload. Senders stay under it by construction rather than by
/// checking this constant: `Secret` and `Downgrade` are fixed-size,
/// `Rotate` carries an encoded `DocTicket` (small in practice), and
/// `WorkspaceTicket` is bounded at every producer site by
/// [`WORKSPACE_TICKET_ENVELOPE_MAX`], whose headroom guarantees the
/// framed bytes clear this cap. Raised from the secret-only 1 KiB when
/// `WorkspaceTicket` joined the channel — envelopes carry
/// user-authored `PathRules` globs.
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
    /// Cooperative downgrade (RW → Read) notification, host→peer at the
    /// moment the host demotes the peer. Carries no key material — the
    /// demoted peer keeps reading and (until it self-halts) keeps its
    /// retained `NamespaceSecret`; this frame only *tells* it to stop
    /// writing. The daemon couriers it opaquely (no namespace
    /// knowledge — ADR-003) and injects a synthetic
    /// [`crate::DOWNGRADE_ACTION`] System message. Append-only variant
    /// index 2.
    Downgrade {
        /// Session the demotion applies to.
        session_id: SessionId,
    },
    /// Namespace-rotation delivery (Evict / write-revocation, Slice 3e):
    /// host→survivor, carrying the rotated namespace's Write `DocTicket`
    /// string (capability + the host's addresses) and the bumped
    /// `namespace_epoch`. The daemon couriers it opaquely (no namespace
    /// knowledge — ADR-003) and injects a synthetic
    /// [`crate::ROTATE_ACTION`] System message; the survivor's FS layer
    /// reimports onto the new namespace. Append-only variant index 3.
    Rotate {
        /// Session the rotation applies to.
        session_id: SessionId,
        /// The new monotonic namespace epoch (survivor reimports only on
        /// a strictly-higher epoch than it last saw).
        namespace_epoch: u64,
        /// `iroh_docs::DocTicket::to_string()` for the rotated namespace.
        doc_ticket: String,
    },
}

/// Payload of the synthetic `workspace.rotate` system message the joiner
/// daemon injects on receiving a [`DeliveryFrame::Rotate`].
///
/// Carries the rotated namespace's Write `DocTicket` (capability +
/// addresses) and the epoch; the survivor's `cap_listener` reimports if
/// the epoch is newer than what it holds. Shared here so the daemon's
/// injector and `artel-fs`'s consumer can't drift.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RotatePayload {
    /// The peer being moved to the rotated namespace — the receiving
    /// daemon's own `PeerId`, stamped by the daemon at injection.
    pub target_peer: PeerId,
    /// New namespace epoch.
    pub namespace_epoch: u64,
    /// `iroh_docs::DocTicket::to_string()` for the rotated namespace.
    pub doc_ticket: String,
}

/// Payload of the synthetic `workspace.downgrade` system message the
/// joiner daemon injects once it has received a [`DeliveryFrame::Downgrade`]
/// over the direct stream.
///
/// Mirrors [`UpgradePayload`] but carries no secret: a demotion conveys
/// no key, only *which* peer (the receiving daemon itself) must halt its
/// watcher. Shared here so the daemon's injector and `artel-fs`'s
/// `cap_listener` consumer can't drift on field order or type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DowngradePayload {
    /// The peer being demoted — the receiving daemon's own `PeerId`,
    /// stamped by the daemon at injection (mirrors
    /// [`UpgradePayload::target_peer`]).
    pub target_peer: PeerId,
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

        let downgrade = DeliveryFrame::Downgrade {
            session_id: SessionId::from_bytes([1; 16]),
        };
        assert_eq!(
            postcard::to_allocvec(&downgrade).unwrap()[0],
            2,
            "Downgrade must stay at variant index 2",
        );

        let rotate = DeliveryFrame::Rotate {
            session_id: SessionId::from_bytes([1; 16]),
            namespace_epoch: 1,
            doc_ticket: "docticket".into(),
        };
        assert_eq!(
            postcard::to_allocvec(&rotate).unwrap()[0],
            3,
            "Rotate must stay at variant index 3",
        );
    }

    #[test]
    fn delivery_frame_downgrade_round_trip() {
        let frame = DeliveryFrame::Downgrade {
            session_id: SessionId::from_bytes([0x7c; 16]),
        };
        let bytes = postcard::to_allocvec(&frame).unwrap();
        let back: DeliveryFrame = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(frame, back);
    }

    #[test]
    fn downgrade_payload_postcard_round_trip() {
        let payload = DowngradePayload {
            target_peer: PeerId::from_bytes([0x22; 32]),
        };
        let bytes = postcard::to_allocvec(&payload).unwrap();
        let back: DowngradePayload = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(payload, back);
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
        // Variant index 4 doesn't exist; postcard must reject.
        let result: Result<DeliveryFrame, _> = postcard::from_bytes(&[0x04, 0x00, 0x00]);
        assert!(result.is_err());
    }

    #[test]
    fn delivery_frame_rotate_round_trip() {
        let frame = DeliveryFrame::Rotate {
            session_id: SessionId::from_bytes([0x5a; 16]),
            namespace_epoch: 42,
            doc_ticket: "docaaaabbbbcccc".into(),
        };
        let bytes = postcard::to_allocvec(&frame).unwrap();
        let back: DeliveryFrame = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(frame, back);
    }

    #[test]
    fn rotate_payload_postcard_round_trip() {
        let payload = RotatePayload {
            target_peer: PeerId::from_bytes([0x33; 32]),
            namespace_epoch: 7,
            doc_ticket: "docticket-string".into(),
        };
        let bytes = postcard::to_allocvec(&payload).unwrap();
        let back: RotatePayload = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(payload, back);
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
