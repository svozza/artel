//! Per-session message types.
//!
//! See ADR-001 § "Versioned message envelope, opaque payload". The daemon
//! understands [`SessionMessage`] enough to assign sequence numbers, persist
//! the log, and answer "messages since seq N", but it never inspects
//! [`SessionMessage::payload`] — that is for the application.

use serde::{Deserialize, Serialize};

use crate::ids::{PeerId, Seq};

/// Message-format version stamped on every [`SessionMessage`].
///
/// Distinct from the IPC handshake version ([`crate::ProtocolVersion`]):
/// IPC version describes the daemon↔client wire; message version describes
/// the on-the-wire shape of a single session message and its payload
/// envelope. The two evolve independently.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct MessageFormat(u8);

impl MessageFormat {
    /// Construct from a raw byte.
    #[must_use]
    pub const fn new(v: u8) -> Self {
        Self(v)
    }

    /// The raw byte value.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// Current message format version.
///
/// Bumped to `2` on 2026-06-02 when [`SessionMessage::signature`] became
/// part of the wire envelope (Auth Slice B). Bumped to `3` on 2026-06-03
/// when [`SessionMessage::host_sig`] — the host's sequencing signature —
/// became part of the wire envelope (Auth Slice B.5). Pre-3 daemons can't
/// decode v3 frames; the version-3 verifier is the floor.
pub const MESSAGE_FORMAT: MessageFormat = MessageFormat::new(3);

/// 64-byte ed25519 signature carried inline on every [`SessionMessage`].
///
/// Wire-form is fixed-length so postcard encodes it as a flat byte run.
/// The `signing` feature enables `crate::signing` helpers that produce and
/// verify these bytes; consumers without the feature still see the field
/// (and can ferry the bytes around) but cannot interpret them.
pub type SigBytes = [u8; 64];

/// All-zero signature used as the in-memory sentinel for "not yet signed".
///
/// Test fixtures and any pre-Slice-B2 code paths populate this and
/// `crate::signing::verify_message` rejects it with
/// `VerifyError::SentinelUnsigned` — that turns "we forgot to call
/// `sign_body`" into a loud test failure once verification is on.
pub const SIGNATURE_UNSIGNED: SigBytes = [0u8; 64];

/// Identity of the peer that authored a message.
///
/// Includes a human-readable display name distinct from the cryptographic
/// [`PeerId`]. Display names are advisory and never authenticated. Trust
/// comes from the peer id, which is itself authenticated by the gossip
/// transport.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Cryptographic identity. MUST equal the iroh `EndpointId` that
    /// delivered the carrying gossip frame; the host drops mismatched
    /// frames. See ADR-001 § Auth and capability model and
    /// `docs/brainstorms/2026-05-30-auth-story-brainstorm.md`.
    pub id: PeerId,
    /// Display name. Advisory only — not authenticated.
    pub display_name: String,
}

impl PeerInfo {
    /// Construct a new `PeerInfo`.
    #[must_use]
    pub fn new(id: PeerId, display_name: impl Into<String>) -> Self {
        Self {
            id,
            display_name: display_name.into(),
        }
    }
}

/// Top-level category for a session message.
///
/// The daemon uses this for tooling (filtering, log views) but never to
/// dispatch on payloads. Apps may carry any payload under any kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    /// Human chat content.
    Chat,
    /// Tool / agent action result.
    Tool,
    /// Session-control or metadata events from the application layer.
    System,
}

/// One ordered message in a session log.
///
/// Field order and types match ADR-001 § "Versioned message envelope".
/// `signature` is appended after `payload` and is **not** part of the
/// signed scope — it carries the signature itself; the canonical bytes
/// the signature covers are built from the preceding fields (minus
/// `seq`) plus the carrying `SessionId`. See `crate::signing` and
/// `docs/brainstorms/2026-05-30-auth-story-brainstorm.md` § L3.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMessage {
    /// Message format version. See [`MessageFormat`] / [`MESSAGE_FORMAT`].
    pub version: MessageFormat,
    /// Host-assigned sequence number. Strictly monotonic within a session.
    ///
    /// **Excluded from the signed scope** so the joiner can sign before
    /// the host stamps the seq. See `crate::signing::canonical_bytes`.
    pub seq: Seq,
    /// Authoring time, in milliseconds since the Unix epoch.
    pub timestamp_ms: u64,
    /// Peer that authored the message.
    pub peer: PeerInfo,
    /// Top-level category.
    pub kind: MessageKind,
    /// Application-defined action / verb (e.g. `"chat.message"`,
    /// `"tool.exec"`). Opaque to the daemon.
    pub action: String,
    /// Application payload bytes. Opaque to the daemon. The application
    /// chooses its own serialization.
    #[serde(with = "payload_serde")]
    pub payload: Vec<u8>,
    /// 64-byte ed25519 signature over `crate::signing::canonical_bytes`
    /// of (`session_id`, `version`, `timestamp_ms`, `peer`, `kind`,
    /// `action`, `payload`). [`SIGNATURE_UNSIGNED`] is the in-memory
    /// sentinel for "not yet signed"; receivers reject the sentinel
    /// once verification is on (Slice B2). Wire-form is a flat
    /// 64-byte run via `serde_bytes`.
    #[serde(with = "signature_serde")]
    pub signature: SigBytes,
    /// 64-byte ed25519 signature produced by the **host** over
    /// `crate::signing::seq_canonical_bytes` of (`session_id`, `seq`,
    /// `author_sig`) — i.e. `"artel/seq-v1" || session_id || seq ||
    /// signature`. Distinct from the author [`signature`](Self::signature):
    /// the author signs the body (seq excluded, so a joiner can sign
    /// before the host stamps the seq); the host then binds *this seq* to
    /// *that author signature* when it sequences the message, so a captured
    /// frame replayed under a different seq fails the host's check (Auth
    /// Slice B.5, finding #1). Persisted (D1), so each log entry is
    /// self-authenticating per its sequencer. [`SIGNATURE_UNSIGNED`] is the
    /// "not yet host-signed" sentinel; joiners reject it once verification
    /// is on (Slice B.5.3). Wire-form is a flat 64-byte run via
    /// `serde_bytes`.
    #[serde(with = "signature_serde")]
    pub host_sig: SigBytes,
}

impl SessionMessage {
    /// Construct a `SessionMessage` with the current [`MESSAGE_FORMAT`].
    ///
    /// Callers that don't yet have a signing key (test fixtures and
    /// the pre-Slice-B2 `Registry::send` code path) pass
    /// [`SIGNATURE_UNSIGNED`]; verification rejects the sentinel as
    /// soon as Slice B2 turns it on, which is the lit fuse that
    /// catches "we forgot to wire signing in" loudly rather than
    /// silently.
    #[must_use]
    #[allow(clippy::too_many_arguments)] // mirrors the wire field set
    pub fn new(
        seq: Seq,
        timestamp_ms: u64,
        peer: PeerInfo,
        kind: MessageKind,
        action: impl Into<String>,
        payload: Vec<u8>,
        signature: SigBytes,
        host_sig: SigBytes,
    ) -> Self {
        Self {
            version: MESSAGE_FORMAT,
            seq,
            timestamp_ms,
            peer,
            kind,
            action: action.into(),
            payload,
            signature,
            host_sig,
        }
    }
}

/// Encoding for the opaque payload bytes.
///
/// In binary formats (postcard) the payload is a flat byte run via
/// `serde_bytes`. In human-readable formats (JSON) it is rendered as a
/// numeric byte array, which is verbose but lossless and avoids pulling in a
/// base64 dep.
mod payload_serde {
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

/// Encoding for the 64-byte signature.
///
/// Postcard sees a flat byte run (length-prefixed by `serde_bytes`);
/// JSON sees a 128-character lowercase-hex string. Hex is the same shape
/// as `PeerId` so a manual log/fixture inspection reads consistently.
///
/// Crate-visible so [`crate::rpc::SignedSendPayload`] can reuse the
/// same encoding for its own signature field.
#[allow(clippy::redundant_pub_crate)]
pub(crate) mod signature_serde {
    use serde::de::{self, Deserializer, SeqAccess, Visitor};
    use serde::{Serialize, Serializer};

    use super::SigBytes;

    const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";

    pub(crate) fn serialize<S: Serializer>(bytes: &SigBytes, s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            let mut out = String::with_capacity(128);
            for b in bytes {
                out.push(HEX_LOWER[(b >> 4) as usize] as char);
                out.push(HEX_LOWER[(b & 0x0f) as usize] as char);
            }
            out.serialize(s)
        } else {
            serde_bytes::Bytes::new(bytes.as_slice()).serialize(s)
        }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<SigBytes, D::Error> {
        struct SigVisitor;

        impl<'de> Visitor<'de> for SigVisitor {
            type Value = SigBytes;

            fn expecting(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                f.write_str("128 hex chars or 64 bytes")
            }

            fn visit_str<E: de::Error>(self, s: &str) -> Result<Self::Value, E> {
                if s.len() != 128 {
                    return Err(E::invalid_length(s.len(), &self));
                }
                let mut out = [0u8; 64];
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
                let mut out = [0u8; 64];
                for (i, slot) in out.iter_mut().enumerate() {
                    *slot = seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(i, &self))?;
                }
                if seq.next_element::<u8>()?.is_some() {
                    return Err(de::Error::invalid_length(65, &self));
                }
                Ok(out)
            }
        }

        if d.is_human_readable() {
            d.deserialize_str(SigVisitor)
        } else {
            d.deserialize_bytes(SigVisitor)
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

    fn sample_peer() -> PeerInfo {
        PeerInfo::new(PeerId::from_bytes([0x42; 32]), "alice")
    }

    fn sample_message() -> SessionMessage {
        SessionMessage::new(
            Seq::new(7),
            1_700_000_000_000,
            sample_peer(),
            MessageKind::Chat,
            "chat.message",
            b"hello".to_vec(),
            SIGNATURE_UNSIGNED,
            SIGNATURE_UNSIGNED,
        )
    }

    // ---- MessageFormat ----

    #[test]
    fn message_format_constant_is_three() {
        assert_eq!(MESSAGE_FORMAT, MessageFormat::new(3));
        assert_eq!(MESSAGE_FORMAT.get(), 3);
    }

    #[test]
    fn message_format_default_is_zero() {
        assert_eq!(MessageFormat::default(), MessageFormat::new(0));
    }

    // ---- MessageKind ----

    #[test]
    fn message_kind_serializes_snake_case() {
        let json = serde_json::to_string(&MessageKind::Chat).unwrap();
        assert_eq!(json, "\"chat\"");
        let json = serde_json::to_string(&MessageKind::Tool).unwrap();
        assert_eq!(json, "\"tool\"");
        let json = serde_json::to_string(&MessageKind::System).unwrap();
        assert_eq!(json, "\"system\"");
    }

    #[test]
    fn message_kind_unknown_variant_rejected() {
        let result: Result<MessageKind, _> = serde_json::from_str("\"bogus\"");
        assert!(result.is_err());
    }

    // ---- SessionMessage ----

    #[test]
    fn session_message_new_sets_current_format() {
        let m = sample_message();
        assert_eq!(m.version, MESSAGE_FORMAT);
        assert_eq!(m.seq, Seq::new(7));
        assert_eq!(m.action, "chat.message");
    }

    #[test]
    fn session_message_postcard_round_trip() {
        let m = sample_message();
        let bytes = postcard::to_allocvec(&m).unwrap();
        let back: SessionMessage = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn session_message_json_round_trip() {
        let m = sample_message();
        let json = serde_json::to_string(&m).unwrap();
        let back: SessionMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn session_message_empty_payload_round_trips() {
        let m = SessionMessage::new(
            Seq::ZERO,
            0,
            sample_peer(),
            MessageKind::System,
            "system.heartbeat",
            Vec::new(),
            SIGNATURE_UNSIGNED,
            SIGNATURE_UNSIGNED,
        );
        let bytes = postcard::to_allocvec(&m).unwrap();
        let back: SessionMessage = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(m, back);
        assert!(back.payload.is_empty());
    }

    #[test]
    fn session_message_large_payload_round_trips() {
        let m = SessionMessage::new(
            Seq::new(u64::MAX),
            u64::MAX,
            sample_peer(),
            MessageKind::Tool,
            "tool.exec",
            vec![0xab; 64 * 1024],
            [0xcd; 64],
            [0xce; 64],
        );
        let bytes = postcard::to_allocvec(&m).unwrap();
        let back: SessionMessage = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(m, back);
        assert_eq!(back.payload.len(), 64 * 1024);
    }

    #[test]
    fn session_message_postcard_is_compact() {
        // A small message should not blow up: payload is a flat byte run in
        // postcard, not a length-prefixed sequence of u8s.
        let m = SessionMessage::new(
            Seq::new(1),
            0,
            PeerInfo::new(PeerId::from_bytes([0; 32]), ""),
            MessageKind::Chat,
            "x",
            vec![0xff; 16],
            SIGNATURE_UNSIGNED,
            SIGNATURE_UNSIGNED,
        );
        let bytes = postcard::to_allocvec(&m).unwrap();
        // u8 + varint(1) + varint(0) + 32 bytes peer + 0-len name + variant
        // tag + 1-char action + 16 bytes payload + 64-byte signature run +
        // 64-byte host_sig run, plus framing varints. Generous upper bound;
        // will fail loudly if encoding regresses.
        assert!(
            bytes.len() < 240,
            "postcard size grew unexpectedly: {} bytes",
            bytes.len()
        );
    }

    #[test]
    fn session_message_signature_field_round_trips_postcard() {
        // Pin the signature field's wire shape: a 64-byte run.
        let mut sig = [0u8; 64];
        for (i, b) in sig.iter_mut().enumerate() {
            *b = u8::try_from(i).unwrap();
        }
        let m = SessionMessage::new(
            Seq::new(1),
            42,
            sample_peer(),
            MessageKind::Chat,
            "chat.message",
            b"hi".to_vec(),
            sig,
            SIGNATURE_UNSIGNED,
        );
        let bytes = postcard::to_allocvec(&m).unwrap();
        let back: SessionMessage = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back.signature, sig);
    }

    #[test]
    fn session_message_signature_field_round_trips_json_as_hex() {
        // JSON renders the signature as a 128-char lowercase-hex string,
        // matching `PeerId`'s shape.
        let sig = [0xabu8; 64];
        let m = SessionMessage::new(
            Seq::new(1),
            42,
            sample_peer(),
            MessageKind::Chat,
            "chat.message",
            b"hi".to_vec(),
            sig,
            SIGNATURE_UNSIGNED,
        );
        let json = serde_json::to_string(&m).unwrap();
        let expected = format!("\"signature\":\"{}\"", "ab".repeat(64));
        assert!(
            json.contains(&expected),
            "json missing hex signature: {json}"
        );
        let back: SessionMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(back.signature, sig);
    }

    #[test]
    fn session_message_host_sig_round_trips_postcard() {
        // Pin the host_sig field's wire shape: a 64-byte run, distinct
        // from the author `signature`.
        let mut host_sig = [0u8; 64];
        for (i, b) in host_sig.iter_mut().enumerate() {
            *b = u8::try_from((i * 3) % 256).unwrap();
        }
        let m = SessionMessage::new(
            Seq::new(1),
            42,
            sample_peer(),
            MessageKind::Chat,
            "chat.message",
            b"hi".to_vec(),
            [0x11; 64],
            host_sig,
        );
        let bytes = postcard::to_allocvec(&m).unwrap();
        let back: SessionMessage = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back.host_sig, host_sig);
        // The two signatures are independent fields.
        assert_eq!(back.signature, [0x11; 64]);
        assert_ne!(back.host_sig, back.signature);
    }

    #[test]
    fn session_message_host_sig_round_trips_json_as_hex() {
        // JSON renders host_sig as its own 128-char lowercase-hex string.
        let host_sig = [0xcdu8; 64];
        let m = SessionMessage::new(
            Seq::new(1),
            42,
            sample_peer(),
            MessageKind::Chat,
            "chat.message",
            b"hi".to_vec(),
            [0xab; 64],
            host_sig,
        );
        let json = serde_json::to_string(&m).unwrap();
        let expected = format!("\"host_sig\":\"{}\"", "cd".repeat(64));
        assert!(json.contains(&expected), "json missing hex host_sig: {json}");
        let back: SessionMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(back.host_sig, host_sig);
    }

    #[test]
    fn signature_unsigned_is_all_zero() {
        // The sentinel must stay 64 zero bytes — a fixture regression
        // detector. `crate::signing::verify_message` rejects this exact
        // value as `VerifyError::SentinelUnsigned` once verification is
        // on.
        assert_eq!(SIGNATURE_UNSIGNED, [0u8; 64]);
    }

    // ---- PeerInfo ----

    #[test]
    fn peer_info_round_trip_postcard() {
        let p = sample_peer();
        let bytes = postcard::to_allocvec(&p).unwrap();
        let back: PeerInfo = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(p, back);
    }

    proptest! {
        #[test]
        fn message_format_postcard_round_trip(v in any::<u8>()) {
            let f = MessageFormat::new(v);
            let bytes = postcard::to_allocvec(&f).unwrap();
            let back: MessageFormat = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(f, back);
        }

        #[test]
        fn message_kind_postcard_round_trip(k in prop_oneof![
            Just(MessageKind::Chat),
            Just(MessageKind::Tool),
            Just(MessageKind::System),
        ]) {
            let bytes = postcard::to_allocvec(&k).unwrap();
            let back: MessageKind = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(k, back);
        }

        #[test]
        fn peer_info_postcard_round_trip(
            id in any::<[u8; 32]>(),
            name in "[\\PC]{0,64}"
        ) {
            let original = PeerInfo::new(PeerId::from_bytes(id), name);
            let bytes = postcard::to_allocvec(&original).unwrap();
            let back: PeerInfo = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(original, back);
        }

        #[test]
        fn session_message_postcard_round_trip_arb(
            version in any::<u8>(),
            seq in any::<u64>(),
            timestamp_ms in any::<u64>(),
            peer_id in any::<[u8; 32]>(),
            display_name in "[\\PC]{0,32}",
            kind_idx in 0u8..3,
            action in "[\\PC]{0,64}",
            payload in proptest::collection::vec(any::<u8>(), 0..512),
            signature in any::<[u8; 64]>(),
            host_sig in any::<[u8; 64]>(),
        ) {
            let kind = match kind_idx {
                0 => MessageKind::Chat,
                1 => MessageKind::Tool,
                _ => MessageKind::System,
            };
            let m = SessionMessage {
                version: MessageFormat::new(version),
                seq: Seq::new(seq),
                timestamp_ms,
                peer: PeerInfo::new(PeerId::from_bytes(peer_id), display_name),
                kind,
                action,
                payload,
                signature,
                host_sig,
            };
            let bytes = postcard::to_allocvec(&m).unwrap();
            let back: SessionMessage = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(m, back);
        }

        #[test]
        fn session_message_json_round_trip_arb(
            seq in any::<u64>(),
            timestamp_ms in any::<u64>(),
            peer_id in any::<[u8; 32]>(),
            display_name in "[\\PC]{0,32}",
            kind_idx in 0u8..3,
            action in "[\\PC]{0,64}",
            payload in proptest::collection::vec(any::<u8>(), 0..256),
            signature in any::<[u8; 64]>(),
            host_sig in any::<[u8; 64]>(),
        ) {
            let kind = match kind_idx {
                0 => MessageKind::Chat,
                1 => MessageKind::Tool,
                _ => MessageKind::System,
            };
            let m = SessionMessage::new(
                Seq::new(seq),
                timestamp_ms,
                PeerInfo::new(PeerId::from_bytes(peer_id), display_name),
                kind,
                action,
                payload,
                signature,
                host_sig,
            );
            let json = serde_json::to_string(&m).unwrap();
            let back: SessionMessage = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(m, back);
        }
    }
}
