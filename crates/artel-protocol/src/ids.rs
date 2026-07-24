//! Identifier types: [`SessionId`], [`PeerId`], [`Seq`], [`TicketId`].
//!
//! All four are newtypes so that the wire format is explicit and so that
//! mixing them in API signatures is a compile error.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Lowercase hex alphabet, used for `PeerId` rendering.
const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";

/// Globally-unique identifier for a session.
///
/// Backed by a v4 UUID. The on-the-wire form is the UUID's bytes; the
/// human form is the canonical hyphenated lowercase representation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(Uuid);

impl SessionId {
    /// Generate a new random session id.
    #[must_use]
    pub fn new_random() -> Self {
        Self(Uuid::new_v4())
    }

    /// Construct from raw bytes. The bytes are interpreted as a UUID; no
    /// validation beyond UUID layout is performed.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    /// Return the underlying UUID bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// The wrapped [`Uuid`].
    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for SessionId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

impl From<Uuid> for SessionId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

/// Globally-unique identifier for an issued join ticket.
///
/// Backed by a v4 UUID, identical in shape to [`SessionId`]. Carried in
/// the ticket wire form since `TICKET_VERSION` 3 (Auth Slice C) so the
/// revocation layer could later name a specific ticket without a wire
/// bump — which is what happened: the ticket-revocation slice enforces
/// it at admission against the host's issued-ticket ledger
/// (issued-only, fail closed). See `crate::ticket::TicketEntry` and
/// `docs/plans/2026-06-11-ticket-revocation-plan.md`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TicketId(Uuid);

impl TicketId {
    /// Generate a new random ticket id.
    #[must_use]
    pub fn new_random() -> Self {
        Self(Uuid::new_v4())
    }

    /// Construct from raw bytes. The bytes are interpreted as a UUID; no
    /// validation beyond UUID layout is performed.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    /// Return the underlying UUID bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// The wrapped [`Uuid`].
    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl fmt::Display for TicketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for TicketId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

impl From<Uuid> for TicketId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

/// Opaque 32-byte identifier for a peer.
///
/// 32 bytes that ARE an `iroh::EndpointId` (an Ed25519 public key). The
/// protocol crate stays free of any iroh dependency, but daemon-side
/// enforcement requires this invariant — the host drops gossip frames
/// whose body `peer.id` doesn't match the gossip-authenticated sender.
/// `PeerId::from_bytes` accepts any 32 bytes for use in unit-test
/// fixtures that don't cross a real gossip mesh; once the bytes hit the
/// network they're checked against `delivered_from`.
///
/// Equality and ordering are byte-wise. Display is lowercase hex.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PeerId(#[serde(with = "serde_bytes_array")] [u8; 32]);

impl PeerId {
    /// Construct from raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrow the underlying bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Format as lowercase hex (64 chars).
    #[must_use]
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in self.0 {
            s.push(HEX_LOWER[(b >> 4) as usize] as char);
            s.push(HEX_LOWER[(b & 0x0f) as usize] as char);
        }
        s
    }
}

impl fmt::Debug for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PeerId({})", self.to_hex())
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl From<[u8; 32]> for PeerId {
    fn from(b: [u8; 32]) -> Self {
        Self(b)
    }
}

impl From<PeerId> for [u8; 32] {
    fn from(p: PeerId) -> Self {
        p.0
    }
}

/// Monotonic sequence number assigned by the host.
///
/// Wraps a `u64`. Increment via [`Seq::next`], which returns `None` on
/// overflow. At one message per nanosecond a `u64` lasts 584 years, so
/// overflow in practice means a bug; the API still surfaces it.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Seq(u64);

impl Seq {
    /// The first valid sequence number.
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

    /// Return the next sequence number, or `None` on overflow.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(n) => Some(Self(n)),
            None => None,
        }
    }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl From<u64> for Seq {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<Seq> for u64 {
    fn from(s: Seq) -> Self {
        s.0
    }
}

/// Wire encoding for `[u8; N]`. Without this, serde encodes it as a
/// length-prefixed sequence of `u8`s in JSON, which is huge and ugly. With
/// this, JSON gets a base64-or-array form via `serde_bytes` semantics, and
/// postcard gets a fixed-length byte run.
mod serde_bytes_array {
    use serde::de::{self, Deserializer, SeqAccess, Visitor};
    use serde::{Serialize, Serializer};

    pub(super) fn serialize<S, const N: usize>(bytes: &[u8; N], s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if s.is_human_readable() {
            let mut out = String::with_capacity(N * 2);
            for b in bytes {
                out.push(super::HEX_LOWER[(b >> 4) as usize] as char);
                out.push(super::HEX_LOWER[(b & 0x0f) as usize] as char);
            }
            out.serialize(s)
        } else {
            // Compact byte array for binary formats.
            serde_bytes::Bytes::new(bytes.as_slice()).serialize(s)
        }
    }

    pub(super) fn deserialize<'de, D, const N: usize>(d: D) -> Result<[u8; N], D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ArrayVisitor<const N: usize>;

        impl<'de, const N: usize> Visitor<'de> for ArrayVisitor<N> {
            type Value = [u8; N];

            fn expecting(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                write!(f, "{N} hex chars or {N} bytes", N = N * 2)
            }

            fn visit_str<E: de::Error>(self, s: &str) -> Result<Self::Value, E> {
                if s.len() != N * 2 {
                    return Err(E::invalid_length(s.len(), &self));
                }
                let mut out = [0u8; N];
                for (i, chunk) in s.as_bytes().chunks_exact(2).enumerate() {
                    let hi = decode_nibble(chunk[0]).ok_or_else(|| E::custom("invalid hex"))?;
                    let lo = decode_nibble(chunk[1]).ok_or_else(|| E::custom("invalid hex"))?;
                    out[i] = (hi << 4) | lo;
                }
                Ok(out)
            }

            fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                v.try_into().map_err(|_| E::invalid_length(v.len(), &self))
            }

            fn visit_borrowed_bytes<E: de::Error>(self, v: &'de [u8]) -> Result<Self::Value, E> {
                self.visit_bytes(v)
            }

            fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
                self.visit_bytes(&v)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut out = [0u8; N];
                for (i, slot) in out.iter_mut().enumerate() {
                    *slot = seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(i, &self))?;
                }
                if seq.next_element::<u8>()?.is_some() {
                    return Err(de::Error::invalid_length(N + 1, &self));
                }
                Ok(out)
            }
        }

        if d.is_human_readable() {
            d.deserialize_str(ArrayVisitor::<N>)
        } else {
            d.deserialize_bytes(ArrayVisitor::<N>)
        }
    }

    const fn decode_nibble(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    use super::*;

    // ---- SessionId ----

    #[test]
    fn session_id_random_unique() {
        let a = SessionId::new_random();
        let b = SessionId::new_random();
        assert_ne!(a, b, "v4 collision is astronomically unlikely");
    }

    #[test]
    fn session_id_display_parses_back() {
        let s = SessionId::new_random();
        let parsed: SessionId = s.to_string().parse().unwrap();
        assert_eq!(s, parsed);
    }

    #[test]
    fn session_id_invalid_string_errors() {
        let result: Result<SessionId, _> = "not-a-uuid".parse();
        assert!(result.is_err());
    }

    #[test]
    fn session_id_json_is_transparent_uuid() {
        let s = SessionId::from_bytes([1; 16]);
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, format!("\"{}\"", s));
    }

    // ---- TicketId ----

    #[test]
    fn ticket_id_random_unique() {
        let a = TicketId::new_random();
        let b = TicketId::new_random();
        assert_ne!(a, b, "v4 collision is astronomically unlikely");
    }

    #[test]
    fn ticket_id_display_parses_back() {
        let t = TicketId::new_random();
        let parsed: TicketId = t.to_string().parse().unwrap();
        assert_eq!(t, parsed);
    }

    #[test]
    fn ticket_id_json_is_transparent_uuid() {
        let t = TicketId::from_bytes([7; 16]);
        let json = serde_json::to_string(&t).unwrap();
        assert_eq!(json, format!("\"{}\"", t));
    }

    #[test]
    fn ticket_id_from_bytes_round_trips() {
        let t = TicketId::from_bytes([0xab; 16]);
        assert_eq!(t.as_bytes(), &[0xab; 16]);
    }

    // ---- PeerId ----

    #[test]
    fn peer_id_hex_round_trips_via_json() {
        let p = PeerId::from_bytes([0xab; 32]);
        let json = serde_json::to_string(&p).unwrap();
        let expected = format!("\"{}\"", "ab".repeat(32));
        assert_eq!(json, expected);
        let back: PeerId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn peer_id_postcard_round_trip() {
        let p = PeerId::from_bytes([
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c,
            0x1d, 0x1e, 0x1f, 0x20,
        ]);
        let bytes = postcard::to_allocvec(&p).unwrap();
        let back: PeerId = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn peer_id_debug_includes_hex() {
        let p = PeerId::from_bytes([0xff; 32]);
        let dbg = format!("{p:?}");
        assert!(dbg.contains(&"ff".repeat(32)), "{dbg}");
    }

    #[test]
    fn peer_id_invalid_hex_rejected() {
        // Wrong length.
        let bad = "\"abcd\"";
        let err: Result<PeerId, _> = serde_json::from_str(bad);
        assert!(err.is_err());

        // Right length, non-hex char.
        let bad2 = format!("\"{}\"", "zz".repeat(32));
        let err2: Result<PeerId, _> = serde_json::from_str(&bad2);
        assert!(err2.is_err());
    }

    // ---- Seq ----

    #[test]
    fn seq_zero_then_one() {
        let s = Seq::ZERO;
        assert_eq!(s.next(), Some(Seq::new(1)));
    }

    #[test]
    fn seq_overflow_returns_none() {
        let s = Seq::new(u64::MAX);
        assert_eq!(s.next(), None);
    }

    #[test]
    fn seq_default_is_zero() {
        assert_eq!(Seq::default(), Seq::ZERO);
    }

    #[test]
    fn seq_display_matches_inner_u64() {
        assert_eq!(Seq::new(42).to_string(), "42");
    }

    proptest! {
        #[test]
        fn session_id_postcard_round_trip(bytes in any::<[u8; 16]>()) {
            let s = SessionId::from_bytes(bytes);
            let v = postcard::to_allocvec(&s).unwrap();
            let back: SessionId = postcard::from_bytes(&v).unwrap();
            prop_assert_eq!(s, back);
        }

        #[test]
        fn session_id_json_round_trip(bytes in any::<[u8; 16]>()) {
            let s = SessionId::from_bytes(bytes);
            let v = serde_json::to_string(&s).unwrap();
            let back: SessionId = serde_json::from_str(&v).unwrap();
            prop_assert_eq!(s, back);
        }

        #[test]
        fn ticket_id_postcard_round_trip(bytes in any::<[u8; 16]>()) {
            let t = TicketId::from_bytes(bytes);
            let v = postcard::to_allocvec(&t).unwrap();
            let back: TicketId = postcard::from_bytes(&v).unwrap();
            prop_assert_eq!(t, back);
        }

        #[test]
        fn ticket_id_json_round_trip(bytes in any::<[u8; 16]>()) {
            let t = TicketId::from_bytes(bytes);
            let v = serde_json::to_string(&t).unwrap();
            let back: TicketId = serde_json::from_str(&v).unwrap();
            prop_assert_eq!(t, back);
        }

        #[test]
        fn peer_id_postcard_round_trip_random(bytes in any::<[u8; 32]>()) {
            let p = PeerId::from_bytes(bytes);
            let v = postcard::to_allocvec(&p).unwrap();
            let back: PeerId = postcard::from_bytes(&v).unwrap();
            prop_assert_eq!(p, back);
        }

        #[test]
        fn peer_id_json_round_trip(bytes in any::<[u8; 32]>()) {
            let p = PeerId::from_bytes(bytes);
            let v = serde_json::to_string(&p).unwrap();
            let back: PeerId = serde_json::from_str(&v).unwrap();
            prop_assert_eq!(p, back);
        }

        #[test]
        fn peer_id_hex_is_64_lowercase_chars(bytes in any::<[u8; 32]>()) {
            let p = PeerId::from_bytes(bytes);
            let hex = p.to_hex();
            prop_assert_eq!(hex.len(), 64);
            prop_assert!(hex.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        }

        #[test]
        fn peer_id_ord_consistent_with_byte_order(a in any::<[u8; 32]>(), b in any::<[u8; 32]>()) {
            let pa = PeerId::from_bytes(a);
            let pb = PeerId::from_bytes(b);
            prop_assert_eq!(pa.cmp(&pb), a.cmp(&b));
        }

        #[test]
        fn seq_next_is_monotonic(v in 0u64..u64::MAX) {
            let s = Seq::new(v);
            let n = s.next().unwrap();
            prop_assert!(n > s);
            prop_assert_eq!(n.get(), v + 1);
        }

        #[test]
        fn seq_postcard_round_trip(v in any::<u64>()) {
            let s = Seq::new(v);
            let bytes = postcard::to_allocvec(&s).unwrap();
            let back: Seq = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(s, back);
        }

        #[test]
        fn seq_ord_consistent_with_inner(a in any::<u64>(), b in any::<u64>()) {
            let sa = Seq::new(a);
            let sb = Seq::new(b);
            prop_assert_eq!(sa.cmp(&sb), a.cmp(&b));
        }
    }
}
