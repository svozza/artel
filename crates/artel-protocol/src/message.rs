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
pub const MESSAGE_FORMAT: MessageFormat = MessageFormat::new(1);

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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMessage {
    /// Message format version. See [`MessageFormat`] / [`MESSAGE_FORMAT`].
    pub version: MessageFormat,
    /// Host-assigned sequence number. Strictly monotonic within a session.
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
}

impl SessionMessage {
    /// Construct a `SessionMessage` with the current [`MESSAGE_FORMAT`].
    #[must_use]
    pub fn new(
        seq: Seq,
        timestamp_ms: u64,
        peer: PeerInfo,
        kind: MessageKind,
        action: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            version: MESSAGE_FORMAT,
            seq,
            timestamp_ms,
            peer,
            kind,
            action: action.into(),
            payload,
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
        )
    }

    // ---- MessageFormat ----

    #[test]
    fn message_format_constant_is_one() {
        assert_eq!(MESSAGE_FORMAT, MessageFormat::new(1));
        assert_eq!(MESSAGE_FORMAT.get(), 1);
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
        );
        let bytes = postcard::to_allocvec(&m).unwrap();
        // u8 + varint(1) + varint(0) + 32 bytes peer + 0-len name + variant
        // tag + 1-char action + 16 bytes payload, plus framing varints.
        // Generous upper bound; will fail loudly if encoding regresses.
        assert!(
            bytes.len() < 80,
            "postcard size grew unexpectedly: {} bytes",
            bytes.len()
        );
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
            );
            let json = serde_json::to_string(&m).unwrap();
            let back: SessionMessage = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(m, back);
        }
    }
}
