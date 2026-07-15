//! Errors the daemon may return to a client over the wire.
//!
//! These are *protocol* errors: they are serialized, sent across the IPC
//! boundary, and reconstructed on the other side. Transport errors (broken
//! socket, framing, malformed bytes) live in this crate's `transport`
//! module (`TransportError`, feature `tokio`), since they cannot be sent
//! over the very transport that failed.

use serde::{Deserialize, Serialize};

use crate::ids::SessionId;
use crate::version::VersionMismatch;

/// A protocol-level error returned by the daemon to a client.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolError {
    /// The client and daemon disagree on protocol version.
    #[error(transparent)]
    VersionMismatch(#[from] VersionMismatch),

    /// The referenced session does not exist on this daemon.
    #[error("unknown session: {0}")]
    UnknownSession(SessionId),

    /// The client tried to send to or unsubscribe from a session it had not
    /// subscribed to.
    #[error("not subscribed to session: {0}")]
    NotSubscribed(SessionId),

    /// The provided join ticket could not be parsed or has been revoked.
    #[error("invalid join ticket")]
    InvalidTicket,

    /// The daemon refuses the request because it has not finished starting
    /// up. Clients should retry after a short delay.
    ///
    /// Reserved: no daemon path constructs it today (startup completes
    /// before the socket accepts connections). Kept for its pinned
    /// postcard index and for a future async-startup daemon.
    #[error("daemon is not ready yet")]
    NotReady,

    /// The operation requires hosting the session but the daemon holds
    /// only a remote mirror: issue/revoke/list tickets, member removal,
    /// workspace-ticket publish — and `Send` on a mirror when the
    /// daemon is built without its iroh transport (with iroh on,
    /// joiner sends are forwarded to the host instead).
    #[error("operation requires hosting the session (this daemon holds a remote mirror)")]
    NotHost,

    /// `HostSession { session: Some(id) }` was issued for an `id`
    /// that exists locally but with a different host or as a
    /// remote-mirror session. The caller is asking to resume a
    /// session they don't own.
    #[error("session id {0} already exists with a different host or kind")]
    SessionConflict(SessionId),

    /// A per-message ed25519 signature failed to verify (or was the
    /// unsigned sentinel). The wrapped string is the diagnostic
    /// reason — it names the failure mode (sentinel / bad key / bad
    /// sig) but never leaks bytes that would help an attacker tune.
    /// See `crate::signing::VerifyError` and Auth Slice B2.
    #[error("signature rejected: {0}")]
    Signature(String),

    /// A capability check failed: either the payload was a malformed
    /// [`crate::capability::CapabilityAction`], or the author lacked the
    /// `ReadWrite` capability required to author the message at its seq
    /// (Auth Slice C / L2). The wrapped string is the diagnostic reason;
    /// it names the failure but never leaks payload or signature bytes.
    /// See `crate::capability` and the host-side `Registry::send`
    /// rejection path.
    #[error("capability denied: {0}")]
    Capability(String),

    /// Catch-all for daemon-side failures the client cannot otherwise
    /// distinguish. The string is for diagnostics only.
    #[error("internal daemon error: {0}")]
    Internal(String),

    /// `RevokeTicket` named a ticket id that was never issued for the
    /// session. Distinct from [`Self::InvalidTicket`], which is the
    /// joiner-facing (deliberately opaque) rejection; this variant is
    /// host-operator-facing — reporting success for an unknown id
    /// would falsely reassure the caller a leaked ticket is dead.
    ///
    /// Declared after [`Self::Internal`]: this enum crosses the
    /// inter-daemon gossip wire (postcard, index = declaration order),
    /// so new variants are appended, never inserted — see the
    /// `postcard_variant_indices_are_pinned` test.
    #[error("ticket {0} was never issued for this session")]
    UnknownTicket(crate::ids::TicketId),

    /// A `Send` was rejected because the message, once framed for the
    /// inter-daemon gossip wire, would exceed
    /// [`crate::gossip::MAX_GOSSIP_MESSAGE_SIZE`]. Rejected at send
    /// time — before the message is sequenced or persisted — because a
    /// message the gossip transport cannot carry would otherwise be
    /// acked to the sender and then silently never reach remote
    /// mirrors (adversarial-review finding #2). `size` is the encoded
    /// frame length that was refused; `max` the ceiling it had to fit.
    ///
    /// Appended after [`Self::UnknownTicket`] — postcard wire index is
    /// declaration order; see `postcard_variant_indices_are_pinned`.
    #[error("message too large for gossip transport: {size} bytes (max {max})")]
    PayloadTooLarge {
        /// Encoded frame length of the rejected message.
        size: u64,
        /// The ceiling the frame had to fit under.
        max: u64,
    },
}

impl ProtocolError {
    /// Stable string slug useful for metrics or logs. Matches the `kind`
    /// tag used in the JSON wire form.
    #[must_use]
    pub const fn slug(&self) -> &'static str {
        match self {
            Self::VersionMismatch(_) => "version_mismatch",
            Self::UnknownSession(_) => "unknown_session",
            Self::NotSubscribed(_) => "not_subscribed",
            Self::InvalidTicket => "invalid_ticket",
            Self::NotReady => "not_ready",
            Self::NotHost => "not_host",
            Self::SessionConflict(_) => "session_conflict",
            Self::Signature(_) => "signature",
            Self::Capability(_) => "capability",
            Self::UnknownTicket(_) => "unknown_ticket",
            Self::Internal(_) => "internal",
            Self::PayloadTooLarge { .. } => "payload_too_large",
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    use super::*;
    use crate::version::ProtocolVersion;

    fn sample_session() -> SessionId {
        SessionId::from_bytes([1; 16])
    }

    #[test]
    fn slug_is_stable_per_variant() {
        assert_eq!(
            ProtocolError::VersionMismatch(VersionMismatch {
                client: ProtocolVersion::new(1),
                daemon: ProtocolVersion::new(2),
            })
            .slug(),
            "version_mismatch"
        );
        assert_eq!(
            ProtocolError::UnknownSession(sample_session()).slug(),
            "unknown_session"
        );
        assert_eq!(
            ProtocolError::NotSubscribed(sample_session()).slug(),
            "not_subscribed"
        );
        assert_eq!(ProtocolError::InvalidTicket.slug(), "invalid_ticket");
        assert_eq!(ProtocolError::NotReady.slug(), "not_ready");
        assert_eq!(ProtocolError::NotHost.slug(), "not_host");
        assert_eq!(
            ProtocolError::SessionConflict(sample_session()).slug(),
            "session_conflict"
        );
        assert_eq!(
            ProtocolError::Signature("bad sig".into()).slug(),
            "signature"
        );
        assert_eq!(
            ProtocolError::Capability("read only".into()).slug(),
            "capability"
        );
        assert_eq!(
            ProtocolError::UnknownTicket(crate::ids::TicketId::from_bytes([2; 16])).slug(),
            "unknown_ticket"
        );
        assert_eq!(ProtocolError::Internal("x".into()).slug(), "internal");
        assert_eq!(
            ProtocolError::PayloadTooLarge { size: 2, max: 1 }.slug(),
            "payload_too_large"
        );
    }

    #[test]
    fn display_messages_are_human_readable() {
        let s = sample_session();
        let cases = [
            ProtocolError::UnknownSession(s),
            ProtocolError::NotSubscribed(s),
            ProtocolError::InvalidTicket,
            ProtocolError::NotReady,
            ProtocolError::NotHost,
            ProtocolError::SessionConflict(s),
            ProtocolError::Signature("zero sentinel".into()),
            ProtocolError::Capability("had Read, needs ReadWrite".into()),
            ProtocolError::UnknownTicket(crate::ids::TicketId::from_bytes([2; 16])),
            ProtocolError::Internal("disk full".into()),
            ProtocolError::PayloadTooLarge {
                size: 2_000_000,
                max: 1_048_064,
            },
        ];
        for c in cases {
            let msg = c.to_string();
            assert!(!msg.is_empty(), "empty message for {c:?}");
        }
    }

    #[test]
    fn version_mismatch_via_from_is_constructible() {
        let mismatch = VersionMismatch {
            client: ProtocolVersion::new(2),
            daemon: ProtocolVersion::new(1),
        };
        let err: ProtocolError = mismatch.into();
        assert!(matches!(err, ProtocolError::VersionMismatch(_)));
    }

    #[test]
    fn json_uses_external_variant_tag() {
        // Unit variants serialize as a bare snake_case string.
        let err = ProtocolError::InvalidTicket;
        assert_eq!(serde_json::to_string(&err).unwrap(), "\"invalid_ticket\"");
        assert_eq!(
            serde_json::to_string(&ProtocolError::NotReady).unwrap(),
            "\"not_ready\""
        );

        // Tuple variants serialize as `{ variant: payload }`.
        let err = ProtocolError::Internal("disk full".into());
        assert_eq!(
            serde_json::to_string(&err).unwrap(),
            "{\"internal\":\"disk full\"}"
        );
    }

    #[test]
    fn unknown_kind_is_rejected() {
        let result: Result<ProtocolError, _> = serde_json::from_str("\"made_up\"");
        assert!(result.is_err());
        let result: Result<ProtocolError, _> = serde_json::from_str("{\"made_up\":null}");
        assert!(result.is_err());
    }

    fn arb_error() -> impl Strategy<Value = ProtocolError> {
        prop_oneof![
            (any::<u32>(), any::<u32>()).prop_map(|(c, d)| ProtocolError::VersionMismatch(
                VersionMismatch {
                    client: ProtocolVersion::new(c),
                    daemon: ProtocolVersion::new(d),
                }
            )),
            any::<[u8; 16]>().prop_map(|b| ProtocolError::UnknownSession(SessionId::from_bytes(b))),
            any::<[u8; 16]>().prop_map(|b| ProtocolError::NotSubscribed(SessionId::from_bytes(b))),
            Just(ProtocolError::InvalidTicket),
            Just(ProtocolError::NotReady),
            Just(ProtocolError::NotHost),
            any::<[u8; 16]>()
                .prop_map(|b| ProtocolError::SessionConflict(SessionId::from_bytes(b))),
            "[\\PC]{0,64}".prop_map(ProtocolError::Signature),
            "[\\PC]{0,64}".prop_map(ProtocolError::Capability),
            any::<[u8; 16]>()
                .prop_map(|b| ProtocolError::UnknownTicket(crate::ids::TicketId::from_bytes(b))),
            "[\\PC]{0,64}".prop_map(ProtocolError::Internal),
            (any::<u64>(), any::<u64>())
                .prop_map(|(size, max)| ProtocolError::PayloadTooLarge { size, max }),
        ]
    }

    #[test]
    fn postcard_variant_indices_are_pinned() {
        // ProtocolError rides the inter-daemon gossip wire inside
        // `GossipBody::SendAck { result: Err(..) }`, which is NOT
        // covered by the IPC PROTOCOL_VERSION handshake — daemons of
        // different builds decode each other's frames as long as
        // GOSSIP_WIRE_VERSION matches. Postcard encodes enum variants
        // by declaration index, so variants must only ever be
        // APPENDED. This pins every index at its wire value; if it
        // fails, you inserted a variant mid-enum — move it to the end
        // (and if removal is ever needed, that's a GOSSIP_WIRE_VERSION
        // bump, not a re-pin).
        let s = sample_session();
        let tid = crate::ids::TicketId::from_bytes([2; 16]);
        let cases: [(ProtocolError, u8); 12] = [
            (
                ProtocolError::VersionMismatch(VersionMismatch {
                    client: ProtocolVersion::new(1),
                    daemon: ProtocolVersion::new(2),
                }),
                0,
            ),
            (ProtocolError::UnknownSession(s), 1),
            (ProtocolError::NotSubscribed(s), 2),
            (ProtocolError::InvalidTicket, 3),
            (ProtocolError::NotReady, 4),
            (ProtocolError::NotHost, 5),
            (ProtocolError::SessionConflict(s), 6),
            (ProtocolError::Signature("x".into()), 7),
            (ProtocolError::Capability("x".into()), 8),
            // Pre-revocation (PROTOCOL_VERSION 7) wire index — pinned
            // so mixed-build meshes keep decoding each other's
            // Internal acks.
            (ProtocolError::Internal("x".into()), 9),
            // Revocation slice: appended, never inserted.
            (ProtocolError::UnknownTicket(tid), 10),
            // Gossip frame-size slice: appended, never inserted.
            (ProtocolError::PayloadTooLarge { size: 2, max: 1 }, 11),
        ];
        for (err, index) in cases {
            let bytes = postcard::to_allocvec(&err).unwrap();
            assert_eq!(
                bytes[0], index,
                "postcard index for {err:?} moved — variants must only be appended",
            );
        }
    }

    proptest! {
        #[test]
        fn postcard_round_trip(e in arb_error()) {
            let bytes = postcard::to_allocvec(&e).unwrap();
            let back: ProtocolError = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(e, back);
        }

        #[test]
        fn json_round_trip(e in arb_error()) {
            let json = serde_json::to_string(&e).unwrap();
            let back: ProtocolError = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(e, back);
        }
    }
}
