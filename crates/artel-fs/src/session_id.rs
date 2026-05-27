//! Stable [`SessionId`] derivation from an iroh-docs [`NamespaceId`].
//!
//! `Workspace::host_with` calls [`session_id_for`] with the workspace's
//! `NamespaceId` so a re-host of the same dir under a fresh daemon
//! lands on the same session id (and therefore the same gossip topic),
//! letting joiners resume across host-daemon restarts. Pure function,
//! no I/O, no caching, never fails.
//!
//! See `docs/plans/2026-05-26-stable-session-id-plan.md` § 1c.

use artel_protocol::SessionId;
use iroh_docs::NamespaceId;

/// Domain tag for the v1 derivation.
///
/// blake3's `keyed_hash` requires a 32-byte key, so the human-readable
/// tag `"artel-fs/session-id/v1"` (22 bytes) is null-padded to 32. The
/// exact byte sequence is part of the v1 contract — bumping it (or
/// changing the padding) is a breaking change for every existing
/// on-disk workspace, the same upgrade contract as `NamespaceId`
/// stability. Pinned by [`tests::domain_tag_byte_sequence_is_pinned`].
const DOMAIN_TAG: &[u8; 32] = b"artel-fs/session-id/v1\0\0\0\0\0\0\0\0\0\0";

/// Derive a stable, version-tagged [`SessionId`] from a workspace's
/// [`NamespaceId`].
///
/// Properties:
/// - **Stable**: the same `NamespaceId` always maps to the same
///   `SessionId`. Re-hosting the same workspace dir under a fresh
///   daemon recovers the original id.
/// - **Domain-separated**: the v1 derivation uses [`DOMAIN_TAG`] as a
///   blake3 key so the output can never collide with another use of
///   the namespace bytes.
/// - **UUID v8**: the variant + version bits per RFC 9562 §5.8 are
///   stamped so the resulting id is a valid UUID v8 in addition to
///   being a 16-byte session id.
#[must_use]
pub fn session_id_for(ns: NamespaceId) -> SessionId {
    let hash = blake3::keyed_hash(DOMAIN_TAG, ns.as_bytes());
    let mut bytes: [u8; 16] = hash.as_bytes()[..16]
        .try_into()
        .expect("16-byte slice from 32-byte blake3 hash");
    // UUID v8 per RFC 9562 §5.8: high nibble of byte 6 = version (8),
    // top two bits of byte 8 = variant (10).
    bytes[6] = (bytes[6] & 0x0F) | 0x80;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    SessionId::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_tag_byte_sequence_is_pinned() {
        // A naked equality check against the literal so any accidental
        // edit to DOMAIN_TAG (different padding, wrong version string,
        // typo) trips this test rather than silently rolling every
        // workspace's session id over.
        let expected: &[u8; 32] = b"artel-fs/session-id/v1\0\0\0\0\0\0\0\0\0\0";
        assert_eq!(DOMAIN_TAG, expected);
        assert_eq!(DOMAIN_TAG.len(), 32);
        // The first 22 bytes are the human-readable tag.
        assert_eq!(&DOMAIN_TAG[..22], b"artel-fs/session-id/v1");
        // The remaining 10 bytes are NULs.
        assert!(DOMAIN_TAG[22..].iter().all(|b| *b == 0));
    }

    #[test]
    fn session_id_is_stable_for_a_given_namespace_id() {
        let ns = NamespaceId::from(&[7u8; 32]);
        let a = session_id_for(ns);
        let b = session_id_for(ns);
        assert_eq!(a, b);
    }

    #[test]
    fn session_id_differs_for_distinct_namespace_ids() {
        let a = session_id_for(NamespaceId::from(&[1u8; 32]));
        let b = session_id_for(NamespaceId::from(&[2u8; 32]));
        assert_ne!(a, b);
    }

    #[test]
    fn session_id_has_uuid_v8_variant_bits() {
        // Spot-check several seeds: a single seed could pass by chance
        // if the derivation accidentally bypassed the bit-stamping step
        // and the underlying hash happened to land on the right bits.
        for seed in [0u8, 1, 7, 42, 0xAB, 0xFF] {
            let id = session_id_for(NamespaceId::from(&[seed; 32]));
            let bytes = id.as_bytes();
            assert_eq!(bytes[6] & 0xF0, 0x80, "byte[6] high nibble for seed {seed}");
            assert_eq!(
                bytes[8] & 0xC0,
                0x80,
                "byte[8] top two bits for seed {seed}"
            );
        }
    }
}
