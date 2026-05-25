//! Versioned envelope around the workspace's [`iroh_docs::DocTicket`].
//!
//! The `workspace.ticket` system message previously carried the
//! bare `DocTicket::to_string().into_bytes()`. The envelope wraps
//! that in a postcard-encoded shape so [`crate::PathRules`] can ride
//! alongside the doc ticket.
//!
//! The envelope's version byte is **not** related to
//! `artel-protocol`'s `TICKET_VERSION` (the artel-session ticket).
//! That stays at v2; this is a fresh `WorkspaceTicketEnvelope` v1
//! around the `workspace.ticket` payload.
//!
//! Wire compatibility: pre-1.0, no consumers in the wild. We
//! **hard-reject** old `DocTicket`-string-only payloads — any bytes
//! that don't postcard-decode as the envelope produce
//! [`TicketEnvelopeError::Malformed`]. A silent fallback to a
//! permissive default would re-introduce the wrong-dir hazard
//! [`crate::AttachPolicy::RequireEmpty`] closes.
//!
//! Encoded size: postcard, ~`len(glob) + 1 byte mode + ~2 bytes
//! length prefix` per rule. Practically unbounded ceiling — gossip
//! frames carry the message, not a base32 URL.

use serde::{Deserialize, Serialize};

use crate::rules::PathRules;

/// Current envelope version.
const ENVELOPE_VERSION: u8 = 1;

/// Versioned envelope shipped as the `workspace.ticket` payload.
///
/// `doc_ticket` is the [`iroh_docs::DocTicket::to_string()`] form so
/// the joiner can `DocTicket::from_str` it after decoding the
/// envelope. `rules` are the host's [`PathRules`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceTicketEnvelope {
    /// Envelope version byte. `1` today; future revisions increment
    /// and are rejected by older joiners with
    /// [`TicketEnvelopeError::UnsupportedVersion`].
    pub version: u8,
    /// `iroh_docs::DocTicket::to_string()`. The joiner re-parses via
    /// `DocTicket::from_str`.
    pub doc_ticket: String,
    /// Host-bound rules. Validated at encode-time and decode-time.
    pub rules: PathRules,
}

impl WorkspaceTicketEnvelope {
    /// Build a v1 envelope around `doc_ticket` and `rules`.
    #[must_use]
    pub const fn new(doc_ticket: String, rules: PathRules) -> Self {
        Self {
            version: ENVELOPE_VERSION,
            doc_ticket,
            rules,
        }
    }
}

/// Encode `env` to the on-wire byte sequence.
///
/// Validates `env.rules` first — a malformed rule set is rejected
/// here rather than producing a payload that the joiner would refuse
/// to decode.
pub fn encode(env: &WorkspaceTicketEnvelope) -> Result<Vec<u8>, TicketEnvelopeError> {
    env.rules
        .validate()
        .map_err(TicketEnvelopeError::PathRules)?;
    postcard::to_allocvec(env).map_err(|e| TicketEnvelopeError::Malformed(e.to_string()))
}

/// Decode `bytes` into a [`WorkspaceTicketEnvelope`].
///
/// Returns:
/// - [`TicketEnvelopeError::Malformed`] if the bytes don't postcard-
///   decode as the envelope shape (covers old raw `DocTicket`
///   strings — they fail this branch).
/// - [`TicketEnvelopeError::UnsupportedVersion`] if the version byte
///   is not [`ENVELOPE_VERSION`].
/// - [`TicketEnvelopeError::PathRules`] if the embedded rules don't
///   pass [`PathRules::validate`].
pub fn decode(bytes: &[u8]) -> Result<WorkspaceTicketEnvelope, TicketEnvelopeError> {
    let env: WorkspaceTicketEnvelope =
        postcard::from_bytes(bytes).map_err(|e| TicketEnvelopeError::Malformed(e.to_string()))?;
    if env.version != ENVELOPE_VERSION {
        return Err(TicketEnvelopeError::UnsupportedVersion(env.version));
    }
    env.rules
        .validate()
        .map_err(TicketEnvelopeError::PathRules)?;
    Ok(env)
}

/// Why an envelope encode/decode failed.
#[derive(Debug, thiserror::Error)]
pub enum TicketEnvelopeError {
    /// Bytes didn't postcard-decode as a workspace ticket envelope.
    /// Most likely cause: old-shape payload (raw `DocTicket` string)
    /// from a host that hasn't been upgraded.
    #[error("workspace ticket envelope: malformed bytes ({0})")]
    Malformed(String),
    /// Version byte unrecognised. Older joiners against newer hosts
    /// will see this once a v2 envelope ships.
    #[error("workspace ticket envelope: unsupported version {0}")]
    UnsupportedVersion(u8),
    /// Embedded [`PathRules`] failed validation.
    #[error("workspace ticket envelope: invalid rules: {0}")]
    PathRules(#[from] crate::rules::PathRulesError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::{Mode, PathRule, PathRules};

    fn rules_empty() -> PathRules {
        PathRules::read_write()
    }

    fn rules_dozen() -> PathRules {
        PathRules {
            default: Mode::ReadOnly,
            rules: (0..12)
                .map(|i| PathRule {
                    glob: format!("dir{i}/**/*.rs"),
                    mode: if i % 2 == 0 {
                        Mode::ReadWrite
                    } else {
                        Mode::ReadOnly
                    },
                })
                .collect(),
        }
    }

    #[test]
    fn envelope_round_trips_with_empty_rules() {
        let env = WorkspaceTicketEnvelope::new("docticket-string".into(), rules_empty());
        let bytes = encode(&env).unwrap();
        let back = decode(&bytes).unwrap();
        assert_eq!(env, back);
    }

    #[test]
    fn envelope_round_trips_with_dozen_rules() {
        let env = WorkspaceTicketEnvelope::new("docticket-string".into(), rules_dozen());
        let bytes = encode(&env).unwrap();
        let back = decode(&bytes).unwrap();
        assert_eq!(env, back);
    }

    #[test]
    fn envelope_decode_rejects_malformed_bytes() {
        let err = decode(b"not a postcard envelope").expect_err("should fail");
        assert!(
            matches!(err, TicketEnvelopeError::Malformed(_)),
            "got {err:?}",
        );
    }

    #[test]
    fn envelope_decode_rejects_wrong_version_byte() {
        // Encode a real envelope, then tamper with the version byte.
        // postcard encodes a u8 as a single byte at offset 0 (varint
        // for u8 in range 0..=127).
        let env = WorkspaceTicketEnvelope::new("docticket".into(), rules_empty());
        let mut bytes = encode(&env).unwrap();
        assert_eq!(bytes[0], ENVELOPE_VERSION);
        bytes[0] = 99;
        let err = decode(&bytes).expect_err("should fail");
        assert!(
            matches!(err, TicketEnvelopeError::UnsupportedVersion(99)),
            "got {err:?}",
        );
    }

    #[test]
    fn envelope_decode_rejects_raw_doc_ticket_string() {
        // Pre-envelope hosts shipped the bare DocTicket base32
        // string. Make sure we hard-reject.
        let raw_doc_ticket = b"docaaa\
            cbbcaa3aacaaaaaaaaaaiiabaaaaaiabarbjzgaaaaaaaaaaaaaaaaaaaaaa";
        let err = decode(raw_doc_ticket).expect_err("should fail");
        assert!(
            matches!(err, TicketEnvelopeError::Malformed(_)),
            "got {err:?}",
        );
    }
}
