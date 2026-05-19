//! Wire format for join tickets.
//!
//! A ticket is the out-of-band string the host gives a joiner so the
//! joiner's daemon can find the host's session. Ours are
//! `artel:<base32-nopad-lowercase>` where the base32 body is a
//! `postcard`-encoded [`SessionTicket`].
//!
//! The bytes carry a wire version, the [`SessionId`], and the host
//! daemon's [`PeerId`]. `NodeAddr` / topic / doc-ticket fields are
//! deliberately absent at this stage — they land in Phase 2c (P2P
//! transport) and Phase 3 (workspace docs) when there's something
//! concrete to put in them. The wire version makes that extension
//! safe.
//!
//! ## Why postcard + base32
//!
//! - `postcard` keeps the payload tight (1 + 16 + 32 = 49 bytes for v1).
//! - `base32-nopad` is case-insensitive, copy-paste safe, and avoids
//!   the `+ /` characters base64 sprays into URLs.
//! - The `artel:` prefix makes "is this an artel ticket?" obvious by
//!   inspection and lets old `artel-local:<uuid>` strings be rejected
//!   cleanly.

use data_encoding::BASE32_NOPAD;
use serde::{Deserialize, Serialize};

use crate::ids::{PeerId, SessionId};

/// Current ticket envelope version. Incremented when the structure
/// of the [`Wire`] payload changes in a way old daemons can't
/// understand.
pub const TICKET_VERSION: u8 = 1;

/// Prefix shared by every well-formed ticket. Distinguishes artel
/// tickets from raw base32 input and makes obsolete `artel-local:`
/// strings easy to reject.
pub const TICKET_PREFIX: &str = "artel:";

/// Decoded form of an artel join ticket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTicket {
    /// The host session's id. Joiners use this to identify which
    /// session they're joining.
    pub session_id: SessionId,
    /// The host daemon's [`PeerId`]. In Phase 2c, joiners use this to
    /// dial the host over iroh; today it serves as a stable identifier
    /// for routing and audit.
    pub host_peer_id: PeerId,
}

/// Errors [`decode`] may return.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TicketError {
    /// String didn't begin with [`TICKET_PREFIX`]. Most often means
    /// the user pasted an old `artel-local:<uuid>` ticket or a raw
    /// session id.
    #[error("ticket missing `{TICKET_PREFIX}` prefix")]
    MissingPrefix,

    /// The base32 body didn't decode.
    #[error("invalid base32 in ticket: {0}")]
    InvalidBase32(String),

    /// The decoded bytes didn't deserialize as a [`Wire`] payload.
    #[error("malformed ticket payload: {0}")]
    Malformed(String),

    /// Wire version doesn't match what this build understands.
    /// Future-compat: a daemon two versions ahead can decline a
    /// ticket without crashing.
    #[error("unsupported ticket version {0} (this build speaks v{TICKET_VERSION})")]
    UnsupportedVersion(u8),
}

/// On-the-wire body. Versioned so we can extend without breaking old
/// joiners.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Wire {
    version: u8,
    session_id: SessionId,
    host_peer_id: PeerId,
}

/// Encode `ticket` to its `artel:<base32>` text form.
#[must_use]
pub fn encode(ticket: &SessionTicket) -> String {
    let wire = Wire {
        version: TICKET_VERSION,
        session_id: ticket.session_id,
        host_peer_id: ticket.host_peer_id,
    };
    let bytes = postcard::to_stdvec(&wire).expect("postcard encode of fixed-size types");
    let body = BASE32_NOPAD.encode(&bytes).to_ascii_lowercase();
    format!("{TICKET_PREFIX}{body}")
}

/// Decode a ticket from its text form. Whitespace inside the input
/// is ignored — paste-friendly.
pub fn decode(raw: &str) -> Result<SessionTicket, TicketError> {
    let trimmed: String = raw.chars().filter(|c| !c.is_whitespace()).collect();
    let body = trimmed
        .strip_prefix(TICKET_PREFIX)
        .ok_or(TicketError::MissingPrefix)?;
    let bytes = BASE32_NOPAD
        .decode(body.to_ascii_uppercase().as_bytes())
        .map_err(|e| TicketError::InvalidBase32(e.to_string()))?;
    let wire: Wire =
        postcard::from_bytes(&bytes).map_err(|e| TicketError::Malformed(e.to_string()))?;
    if wire.version != TICKET_VERSION {
        return Err(TicketError::UnsupportedVersion(wire.version));
    }
    Ok(SessionTicket {
        session_id: wire.session_id,
        host_peer_id: wire.host_peer_id,
    })
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    use super::*;

    fn sample() -> SessionTicket {
        SessionTicket {
            session_id: SessionId::from_bytes([0xab; 16]),
            host_peer_id: PeerId::from_bytes([0x42; 32]),
        }
    }

    #[test]
    fn round_trip_recovers_original() {
        let original = sample();
        let encoded = encode(&original);
        assert!(encoded.starts_with(TICKET_PREFIX));
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn encoded_form_is_lowercase_base32_after_prefix() {
        let encoded = encode(&sample());
        let body = encoded.strip_prefix(TICKET_PREFIX).unwrap();
        assert!(
            body.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
            "body must be lowercase base32: {body}",
        );
    }

    #[test]
    fn whitespace_in_decode_input_is_tolerated() {
        let encoded = encode(&sample());
        let mangled = encoded.replace("artel:", "artel:\n  ");
        let decoded = decode(&mangled).unwrap();
        assert_eq!(decoded, sample());
    }

    #[test]
    fn missing_prefix_errors() {
        let body = "abcdef";
        assert_eq!(decode(body), Err(TicketError::MissingPrefix));
    }

    #[test]
    fn legacy_artel_local_string_is_rejected_with_missing_prefix() {
        // The Phase 2a ticket form lives in the daemon's history; it
        // is now an unrecognised string. We classify as
        // MissingPrefix because that's what the user actually has —
        // it's clearer than "malformed body".
        let legacy = "artel-local:01234567-89ab-cdef-0123-456789abcdef";
        assert_eq!(decode(legacy), Err(TicketError::MissingPrefix));
    }

    #[test]
    fn empty_string_errors() {
        assert_eq!(decode(""), Err(TicketError::MissingPrefix));
    }

    #[test]
    fn prefix_only_errors_as_malformed() {
        // base32 of zero bytes → empty string. postcard can't decode
        // an empty buffer into a Wire, so we surface Malformed.
        let raw = TICKET_PREFIX;
        match decode(raw) {
            Err(TicketError::Malformed(_)) => {}
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn invalid_base32_errors() {
        // Base32 alphabet is A-Z and 2-7. '0', '1', '8', '9' are not.
        let bad = format!("{TICKET_PREFIX}1");
        match decode(&bad) {
            Err(TicketError::InvalidBase32(_)) => {}
            other => panic!("expected InvalidBase32, got {other:?}"),
        }
    }

    #[test]
    fn unknown_version_byte_errors_clearly() {
        // Hand-craft a Wire with version != TICKET_VERSION, encode it
        // ourselves, and verify the decoder rejects it without
        // touching the inner fields.
        let bogus = Wire {
            version: 0xff,
            session_id: SessionId::from_bytes([0; 16]),
            host_peer_id: PeerId::from_bytes([0; 32]),
        };
        let bytes = postcard::to_stdvec(&bogus).unwrap();
        let body = BASE32_NOPAD.encode(&bytes).to_ascii_lowercase();
        let raw = format!("{TICKET_PREFIX}{body}");
        assert_eq!(decode(&raw), Err(TicketError::UnsupportedVersion(0xff)));
    }

    #[test]
    fn ticket_text_is_short_enough_to_paste() {
        // 1 (version) + 16 (UUID) + 32 (peer id) = 49 bytes.
        // base32 of 49 bytes = ceil(49 * 8 / 5) = 79 chars.
        // Plus the "artel:" prefix → 85 chars total. Comfortable
        // copy-paste range.
        let encoded = encode(&sample());
        assert!(
            encoded.len() <= 96,
            "ticket text too long ({} chars): {encoded}",
            encoded.len(),
        );
    }

    proptest! {
        #[test]
        fn round_trip_arb(
            sid in any::<[u8; 16]>(),
            peer in any::<[u8; 32]>(),
        ) {
            let original = SessionTicket {
                session_id: SessionId::from_bytes(sid),
                host_peer_id: PeerId::from_bytes(peer),
            };
            let encoded = encode(&original);
            let decoded = decode(&encoded).expect("round trip");
            prop_assert_eq!(decoded, original);
        }
    }
}
