//! Errors the daemon may return to a client over the wire.
//!
//! These are *protocol* errors: they are serialized, sent across the IPC
//! boundary, and reconstructed on the other side. Transport errors (broken
//! socket, framing, malformed bytes) live in `artel-client` / `artel-daemon`
//! respectively, since they cannot be sent over the very transport that
//! failed.

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

    /// The client is already a member of the session.
    #[error("already joined session: {0}")]
    AlreadyJoined(SessionId),

    /// The daemon refuses the request because it has not finished starting
    /// up. Clients should retry after a short delay.
    #[error("daemon is not ready yet")]
    NotReady,

    /// Catch-all for daemon-side failures the client cannot otherwise
    /// distinguish. The string is for diagnostics only.
    #[error("internal daemon error: {0}")]
    Internal(String),
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
            Self::AlreadyJoined(_) => "already_joined",
            Self::NotReady => "not_ready",
            Self::Internal(_) => "internal",
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
        assert_eq!(
            ProtocolError::AlreadyJoined(sample_session()).slug(),
            "already_joined"
        );
        assert_eq!(ProtocolError::NotReady.slug(), "not_ready");
        assert_eq!(ProtocolError::Internal("x".into()).slug(), "internal");
    }

    #[test]
    fn display_messages_are_human_readable() {
        let s = sample_session();
        let cases = [
            ProtocolError::UnknownSession(s),
            ProtocolError::NotSubscribed(s),
            ProtocolError::InvalidTicket,
            ProtocolError::AlreadyJoined(s),
            ProtocolError::NotReady,
            ProtocolError::Internal("disk full".into()),
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
            any::<[u8; 16]>().prop_map(|b| ProtocolError::AlreadyJoined(SessionId::from_bytes(b))),
            Just(ProtocolError::NotReady),
            "[\\PC]{0,64}".prop_map(ProtocolError::Internal),
        ]
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
