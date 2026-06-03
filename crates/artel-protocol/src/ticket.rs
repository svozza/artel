//! Wire format for join tickets.
//!
//! A ticket is the out-of-band string the host gives a joiner so the
//! joiner's daemon can find the host's session. Ours are
//! `artel:<base32-nopad-lowercase>` where the base32 body is a
//! `postcard`-encoded [`SessionTicket`].
//!
//! The bytes carry a wire version, the [`SessionId`], the host
//! daemon's [`PeerId`], and a [`WireEndpointAddr`] describing how to
//! reach the host on the iroh network. The doc-ticket extension
//! lands in Phase 3 (workspace docs).
//!
//! The gossip topic id is **not** in the ticket — joiners derive it
//! deterministically from the session id, so the ticket only carries
//! data the joiner couldn't otherwise compute.
//!
//! ## Why postcard + base32
//!
//! - `postcard` keeps the payload tight (1 + 16 + 32 = 49 bytes for v1).
//! - `base32-nopad` is case-insensitive, copy-paste safe, and avoids
//!   the `+ /` characters base64 sprays into URLs.
//! - The `artel:` prefix makes "is this an artel ticket?" obvious by
//!   inspection and lets old `artel-local:<uuid>` strings be rejected
//!   cleanly.

use std::collections::BTreeSet;
use std::net::SocketAddr;

use data_encoding::BASE32_NOPAD;
use serde::{Deserialize, Serialize};

use crate::ids::{PeerId, SessionId, TicketId};

/// Current ticket envelope version. Incremented when the structure
/// of the [`Wire`] payload changes in a way old daemons can't
/// understand.
///
/// Bumped to `3` on 2026-06-03 when [`SessionTicket::ticket_id`] joined
/// the wire form (Auth Slice C / L2). The cap-log root of trust is the
/// originator's first grant; the joiner verifies it against the
/// originator pubkey, which in today's star topology *is*
/// [`SessionTicket::host_peer_id`] — so no new pubkey field is needed,
/// only the `ticket_id` (which names the ticket for a future revocation
/// layer).
pub const TICKET_VERSION: u8 = 3;

/// Prefix shared by every well-formed ticket. Distinguishes artel
/// tickets from raw base32 input and makes obsolete `artel-local:`
/// strings easy to reject.
pub const TICKET_PREFIX: &str = "artel:";

/// Decoded form of an artel join ticket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTicket {
    /// Unique id naming this issued ticket. Carried for a future
    /// revocation layer (Auth Slice C); *carried* but not *enforced* in
    /// v1. See [`TicketId`].
    pub ticket_id: TicketId,
    /// The host session's id. Joiners use this to identify which
    /// session they're joining.
    pub session_id: SessionId,
    /// The host daemon's [`PeerId`]. Identical to
    /// `host_addr.peer_id`; kept as a top-level field so callers can
    /// route by id without parsing the addr.
    pub host_peer_id: PeerId,
    /// How to reach the host daemon on the iroh network: zero or one
    /// home-relay URL, plus zero or more direct socket addresses.
    /// May be empty if the host has no published transport addresses
    /// yet, in which case dialers fall back to whatever address-
    /// lookup mechanism is configured.
    pub host_addr: WireEndpointAddr,
}

/// Wire-friendly mirror of `iroh::EndpointAddr`. Lives in
/// `artel-protocol` so the wire format stays iroh-free; the daemon
/// converts to/from the iroh type at the boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireEndpointAddr {
    /// Same bytes as the parent ticket's `host_peer_id`. Carried here
    /// too so the addr is self-contained — the daemon can hand the
    /// whole struct to iroh without re-stitching.
    pub peer_id: PeerId,
    /// Home relay URL as a string (we don't pull `url::Url` into the
    /// protocol crate). Empty when the host has no relay configured.
    pub relay_url: String,
    /// Direct UDP socket addresses where the host is reachable.
    /// Sorted into a `BTreeSet` for deterministic encoding.
    pub direct_addrs: BTreeSet<SocketAddr>,
}

impl WireEndpointAddr {
    /// Construct an addr with no relay and no direct addrs. Useful
    /// for tests that only care about the peer id.
    #[must_use]
    pub const fn id_only(peer_id: PeerId) -> Self {
        Self {
            peer_id,
            relay_url: String::new(),
            direct_addrs: BTreeSet::new(),
        }
    }
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
    ticket_id: TicketId,
    session_id: SessionId,
    host_peer_id: PeerId,
    host_addr: WireEndpointAddr,
}

/// Encode `ticket` to its `artel:<base32>` text form.
#[must_use]
pub fn encode(ticket: &SessionTicket) -> String {
    let wire = Wire {
        version: TICKET_VERSION,
        ticket_id: ticket.ticket_id,
        session_id: ticket.session_id,
        host_peer_id: ticket.host_peer_id,
        host_addr: ticket.host_addr.clone(),
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
    if wire.host_addr.peer_id != wire.host_peer_id {
        // Self-consistency check: the addr's peer id has to match
        // the ticket-level host id. A mismatch means tampering or
        // cross-version drift; either way, refuse it.
        return Err(TicketError::Malformed(
            "host_addr.peer_id does not match host_peer_id".into(),
        ));
    }
    Ok(SessionTicket {
        ticket_id: wire.ticket_id,
        session_id: wire.session_id,
        host_peer_id: wire.host_peer_id,
        host_addr: wire.host_addr,
    })
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    use super::*;

    fn sample() -> SessionTicket {
        let peer = PeerId::from_bytes([0x42; 32]);
        SessionTicket {
            ticket_id: TicketId::from_bytes([0x01; 16]),
            session_id: SessionId::from_bytes([0xab; 16]),
            host_peer_id: peer,
            host_addr: WireEndpointAddr::id_only(peer),
        }
    }

    fn sample_with_addrs() -> SessionTicket {
        let peer = PeerId::from_bytes([0x77; 32]);
        let mut addrs = BTreeSet::new();
        addrs.insert("127.0.0.1:7777".parse().unwrap());
        addrs.insert("[::1]:7778".parse().unwrap());
        SessionTicket {
            ticket_id: TicketId::from_bytes([0x02; 16]),
            session_id: SessionId::from_bytes([0xcd; 16]),
            host_peer_id: peer,
            host_addr: WireEndpointAddr {
                peer_id: peer,
                relay_url: "https://relay.example.com".into(),
                direct_addrs: addrs,
            },
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
        let peer = PeerId::from_bytes([0; 32]);
        let bogus = Wire {
            version: 0xff,
            ticket_id: TicketId::from_bytes([0; 16]),
            session_id: SessionId::from_bytes([0; 16]),
            host_peer_id: peer,
            host_addr: WireEndpointAddr::id_only(peer),
        };
        let bytes = postcard::to_stdvec(&bogus).unwrap();
        let body = BASE32_NOPAD.encode(&bytes).to_ascii_lowercase();
        let raw = format!("{TICKET_PREFIX}{body}");
        assert_eq!(decode(&raw), Err(TicketError::UnsupportedVersion(0xff)));
    }

    #[test]
    fn host_peer_id_addr_mismatch_errors_as_malformed() {
        // Hand-craft a current-version Wire whose host_addr.peer_id
        // differs from the top-level host_peer_id and verify decode
        // rejects it.
        let bad = Wire {
            version: TICKET_VERSION,
            ticket_id: TicketId::from_bytes([3; 16]),
            session_id: SessionId::from_bytes([1; 16]),
            host_peer_id: PeerId::from_bytes([0xaa; 32]),
            host_addr: WireEndpointAddr::id_only(PeerId::from_bytes([0xbb; 32])),
        };
        let bytes = postcard::to_stdvec(&bad).unwrap();
        let body = BASE32_NOPAD.encode(&bytes).to_ascii_lowercase();
        let raw = format!("{TICKET_PREFIX}{body}");
        match decode(&raw) {
            Err(TicketError::Malformed(msg)) => assert!(
                msg.contains("host_addr.peer_id"),
                "msg should mention the field: {msg}",
            ),
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn ticket_text_for_id_only_is_paste_friendly() {
        // id-only (no relay, no direct addrs) = 1 (ver) + 16 (UUID) +
        // 32 (peer id) + 32 (addr peer id) + 0 (empty relay) +
        // 0 (empty addrs) ≈ ~82 bytes postcard. base32-encoded plus
        // the "artel:" prefix lands well under 200 chars — fits on
        // one terminal line.
        let encoded = encode(&sample());
        assert!(
            encoded.len() <= 200,
            "id-only ticket too long ({} chars): {encoded}",
            encoded.len(),
        );
    }

    #[test]
    fn ticket_with_relay_and_two_addrs_round_trips() {
        let original = sample_with_addrs();
        let encoded = encode(&original);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn ticket_version_is_three() {
        assert_eq!(TICKET_VERSION, 3);
    }

    #[test]
    fn ticket_id_survives_round_trip() {
        // The new field must make the round trip intact, not just be
        // defaulted away on decode.
        let original = sample();
        let decoded = decode(&encode(&original)).unwrap();
        assert_eq!(decoded.ticket_id, original.ticket_id);
        assert_eq!(decoded.ticket_id, TicketId::from_bytes([0x01; 16]));
    }

    #[test]
    fn previous_version_byte_is_now_unsupported() {
        // The version gate must reject the previous ticket version (2)
        // now that the wire shape gained `ticket_id`. Hand-craft a Wire
        // that is otherwise well-formed (current shape, so it
        // deserializes) but stamps version 2 — decode parses it, then
        // the version check fires. Mirrors
        // `unknown_version_byte_errors_clearly` for the immediate
        // predecessor rather than an arbitrary byte.
        let peer = PeerId::from_bytes([0x55; 32]);
        let prev = Wire {
            version: 2,
            ticket_id: TicketId::from_bytes([0x44; 16]),
            session_id: SessionId::from_bytes([0x66; 16]),
            host_peer_id: peer,
            host_addr: WireEndpointAddr::id_only(peer),
        };
        let bytes = postcard::to_stdvec(&prev).unwrap();
        let body = BASE32_NOPAD.encode(&bytes).to_ascii_lowercase();
        let raw = format!("{TICKET_PREFIX}{body}");
        assert_eq!(decode(&raw), Err(TicketError::UnsupportedVersion(2)));
    }

    proptest! {
        #[test]
        fn round_trip_arb(
            tid in any::<[u8; 16]>(),
            sid in any::<[u8; 16]>(),
            peer in any::<[u8; 32]>(),
        ) {
            let host_peer_id = PeerId::from_bytes(peer);
            let original = SessionTicket {
                ticket_id: TicketId::from_bytes(tid),
                session_id: SessionId::from_bytes(sid),
                host_peer_id,
                host_addr: WireEndpointAddr::id_only(host_peer_id),
            };
            let encoded = encode(&original);
            let decoded = decode(&encoded).expect("round trip");
            prop_assert_eq!(decoded, original);
        }
    }
}
