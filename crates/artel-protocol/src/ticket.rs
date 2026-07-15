//! Wire format for join tickets.
//!
//! A ticket is the out-of-band string the host gives a joiner so the
//! joiner's daemon can find the host's session. Ours are
//! `artel:<base32-nopad-lowercase>` where the base32 body is a
//! `postcard`-encoded [`SessionTicket`].
//!
//! The bytes carry a wire version, the [`TicketId`], the
//! [`SessionId`], the host daemon's [`PeerId`], a
//! [`WireEndpointAddr`] describing how to reach the host on the iroh
//! network, and the tiered-capability fields (`granted_cap`,
//! `expiry_ms`, `cap_sig`). Workspace doc tickets do not ride here —
//! they are delivered separately as a `DeliveryFrame::WorkspaceTicket`
//! over the direct stream.
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

use crate::capability::Capability;
use crate::ids::{PeerId, SessionId, TicketId};
use crate::message::SigBytes;

/// Current ticket envelope version. Incremented when the structure
/// of the `Wire` payload changes in a way old daemons can't
/// understand.
///
/// Bumped to `4` on 2026-06-05 when tiered tickets landed:
/// [`SessionTicket::granted_cap`], [`SessionTicket::expiry_ms`], and
/// [`SessionTicket::cap_sig`] joined the wire form. The host signs
/// the cap claim under `"artel/ticket-cap-v1"` and verifies its own
/// signature at admission — stateless bearer-token capability.
pub const TICKET_VERSION: u8 = 4;

/// Prefix shared by every well-formed ticket. Distinguishes artel
/// tickets from raw base32 input and makes obsolete `artel-local:`
/// strings easy to reject.
pub const TICKET_PREFIX: &str = "artel:";

/// Lifecycle state of an issued ticket in the host's ledger.
///
/// Admission requires `Active`; `Revoked` (and ledger *absence* —
/// issued-only, fail closed) reject. There is no un-revoke.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TicketStatus {
    /// Ticket admits bearers (subject to expiry and cap-sig checks).
    Active,
    /// Ticket was revoked by the host; bearers are rejected at
    /// admission. Entry is retained for listing until session close.
    Revoked,
}

/// One entry in the host's issued-ticket ledger.
///
/// The ledger is the host-side record of every ticket minted for a
/// session — the authoritative answer to both "may this ticket admit?"
/// (status must be `Active`) and "what can I revoke?". It holds
/// *metadata only*: the encoded bearer string is never stored or
/// returned. Carried on the IPC wire by `Response::Tickets`; persisted
/// by the daemon next to the session log. Host-local — never crosses
/// the gossip wire (issuance metadata would leak to all members for
/// zero enforcement benefit; the host is the sole admission gate).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketEntry {
    /// Id of the issued ticket (also embedded in the bearer string).
    pub ticket_id: TicketId,
    /// Capability tier the ticket grants. Cross-checked against the
    /// bearer's claim at admission.
    pub granted_cap: Capability,
    /// Expiry in ms since Unix epoch (`0` = none). Cross-checked
    /// against the bearer's claim at admission.
    pub expiry_ms: u64,
    /// When the host minted this ticket, ms since Unix epoch.
    pub issued_at_ms: u64,
    /// Current lifecycle state.
    pub status: TicketStatus,
    /// Peers admitted via this ticket, in admission order. Advisory
    /// metadata (tickets are multi-use bearer tokens): tells the
    /// operator a revoke of an already-used ticket may also warrant a
    /// peer-level `CapabilityAction::Revoke`.
    pub used_by: Vec<PeerId>,
}

/// Decoded form of an artel join ticket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTicket {
    /// Unique id naming this issued ticket. Enforced at admission
    /// against the host's issued-ticket ledger (issued-only, fail
    /// closed): the id must be present and [`TicketStatus::Active`].
    /// See [`TicketId`] and [`TicketEntry`].
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
    /// Capability tier this ticket grants the bearer on admission.
    pub granted_cap: Capability,
    /// Expiry as milliseconds since Unix epoch. `0` means no expiry.
    /// The host checks this at admission time; decode does NOT reject
    /// expired tickets (the host clock is authoritative).
    pub expiry_ms: u64,
    /// Host signature over `(ticket_id, session_id, granted_cap,
    /// expiry_ms)` under the `"artel/ticket-cap-v1"` domain. Verified
    /// by the host at admission to prove this ticket was genuinely issued.
    pub cap_sig: SigBytes,
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

    /// The decoded bytes didn't deserialize as a `Wire` payload.
    #[error("malformed ticket payload: {0}")]
    Malformed(String),

    /// Wire version doesn't match what this build understands.
    /// Future-compat: a daemon two versions ahead can decline a
    /// ticket without crashing.
    #[error("unsupported ticket version {0} (this build speaks v{TICKET_VERSION})")]
    UnsupportedVersion(u8),
}

/// On-the-wire body, **without** the version byte.
///
/// The version lives *outside* this struct as a structural leading
/// byte on the encoded frame (`[version: u8][postcard(WireBody)]`),
/// mirroring the gossip-frame convention (see
/// [`crate::gossip::encode`]). Keeping it out of the postcard body lets
/// [`decode`] dispatch on the version *before* deserializing any
/// version-specific field, so a future/unknown version is rejected as
/// [`TicketError::UnsupportedVersion`] up front rather than surfacing as
/// a [`TicketError::Malformed`] once its reshaped body fails to parse —
/// and no untrusted field is deserialized under a version this build
/// doesn't understand.
///
/// This is byte-compatible with the previous layout, where `version`
/// was the first field of the combined struct: postcard encodes a `u8`
/// as a bare leading byte with no struct framing, so
/// `[TICKET_VERSION] ++ postcard(WireBody)` reproduces the old
/// `postcard(Wire)` output exactly. No [`TICKET_VERSION`] bump needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireBody {
    ticket_id: TicketId,
    session_id: SessionId,
    host_peer_id: PeerId,
    host_addr: WireEndpointAddr,
    granted_cap: Capability,
    expiry_ms: u64,
    #[serde(with = "crate::message::signature_serde")]
    cap_sig: SigBytes,
}

/// Encode `ticket` to its `artel:<base32>` text form.
///
/// The encoded frame is `[TICKET_VERSION][postcard(body)]`; the version
/// rides outside the postcard body so [`decode`] can dispatch on it
/// before deserializing any version-specific field.
///
/// # Panics
///
/// Panics if postcard fails to encode the wire body. Its fields are
/// all fixed-size types that always serialize, so this is unreachable
/// in practice.
#[must_use]
pub fn encode(ticket: &SessionTicket) -> String {
    let body = WireBody {
        ticket_id: ticket.ticket_id,
        session_id: ticket.session_id,
        host_peer_id: ticket.host_peer_id,
        host_addr: ticket.host_addr.clone(),
        granted_cap: ticket.granted_cap,
        expiry_ms: ticket.expiry_ms,
        cap_sig: ticket.cap_sig,
    };
    let mut bytes = vec![TICKET_VERSION];
    bytes.extend_from_slice(
        &postcard::to_stdvec(&body).expect("postcard encode of fixed-size types"),
    );
    let encoded = BASE32_NOPAD.encode(&bytes).to_ascii_lowercase();
    format!("{TICKET_PREFIX}{encoded}")
}

/// Decode a ticket from its text form. Whitespace inside the input
/// is ignored — paste-friendly.
///
/// The version byte is read and checked *before* the rest of the frame
/// is deserialized, so an unknown version is reported as
/// [`TicketError::UnsupportedVersion`] without parsing any
/// version-specific field.
///
/// # Errors
///
/// Returns [`TicketError::MissingPrefix`] if the trimmed input does not
/// start with [`TICKET_PREFIX`], [`TicketError::InvalidBase32`] if the
/// body is not valid base32, [`TicketError::UnsupportedVersion`] if the
/// leading version byte is not [`TICKET_VERSION`], and
/// [`TicketError::Malformed`] if the frame is empty, the remaining bytes
/// do not postcard-decode into the wire body, or its `host_addr.peer_id`
/// disagrees with its `host_peer_id`.
pub fn decode(raw: &str) -> Result<SessionTicket, TicketError> {
    let trimmed: String = raw.chars().filter(|c| !c.is_whitespace()).collect();
    let body = trimmed
        .strip_prefix(TICKET_PREFIX)
        .ok_or(TicketError::MissingPrefix)?;
    let bytes = BASE32_NOPAD
        .decode(body.to_ascii_uppercase().as_bytes())
        .map_err(|e| TicketError::InvalidBase32(e.to_string()))?;
    // Dispatch on the structural version byte before deserializing any
    // version-specific field (finding #2).
    let (&version, rest) = bytes
        .split_first()
        .ok_or_else(|| TicketError::Malformed("empty ticket frame".into()))?;
    if version != TICKET_VERSION {
        return Err(TicketError::UnsupportedVersion(version));
    }
    let wire: WireBody =
        postcard::from_bytes(rest).map_err(|e| TicketError::Malformed(e.to_string()))?;
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
        granted_cap: wire.granted_cap,
        expiry_ms: wire.expiry_ms,
        cap_sig: wire.cap_sig,
    })
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    use super::*;
    use crate::message::SIGNATURE_UNSIGNED;

    /// Build a raw `artel:<base32>` frame from an explicit version byte
    /// and a [`WireBody`], mirroring [`encode`]'s
    /// `[version][postcard(body)]` layout. Lets version-dispatch tests
    /// stamp an arbitrary leading version without going through
    /// [`encode`] (which always writes [`TICKET_VERSION`]).
    fn raw_frame(version: u8, body: &WireBody) -> String {
        let mut bytes = vec![version];
        bytes.extend_from_slice(&postcard::to_stdvec(body).unwrap());
        let encoded = BASE32_NOPAD.encode(&bytes).to_ascii_lowercase();
        format!("{TICKET_PREFIX}{encoded}")
    }

    fn sample() -> SessionTicket {
        let peer = PeerId::from_bytes([0x42; 32]);
        SessionTicket {
            ticket_id: TicketId::from_bytes([0x01; 16]),
            session_id: SessionId::from_bytes([0xab; 16]),
            host_peer_id: peer,
            host_addr: WireEndpointAddr::id_only(peer),
            granted_cap: Capability::ReadWrite,
            expiry_ms: 0,
            cap_sig: [0xaa; 64],
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
            granted_cap: Capability::Read,
            expiry_ms: 1_700_000_000_000,
            cap_sig: [0xbb; 64],
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
        // base32 of zero bytes → empty string → a zero-length frame:
        // there is no leading version byte to split off, so we surface
        // Malformed rather than panic.
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
        let peer = PeerId::from_bytes([0; 32]);
        let bogus = WireBody {
            ticket_id: TicketId::from_bytes([0; 16]),
            session_id: SessionId::from_bytes([0; 16]),
            host_peer_id: peer,
            host_addr: WireEndpointAddr::id_only(peer),
            granted_cap: Capability::Read,
            expiry_ms: 0,
            cap_sig: SIGNATURE_UNSIGNED,
        };
        let raw = raw_frame(0xff, &bogus);
        assert_eq!(decode(&raw), Err(TicketError::UnsupportedVersion(0xff)));
    }

    #[test]
    fn unknown_version_rejected_before_body_is_parsed() {
        // Finding #2: an unknown version whose body is *garbage* must
        // still report UnsupportedVersion, proving the version byte is
        // checked before any attempt to deserialize the body. Pre-fix
        // (version inside the postcard body) this surfaced as Malformed,
        // because the whole struct was deserialized first.
        let mut bytes = vec![0xfe_u8]; // unknown version
        bytes.extend_from_slice(&[0xff; 3]); // truncated / nonsense body
        let encoded = BASE32_NOPAD.encode(&bytes).to_ascii_lowercase();
        let raw = format!("{TICKET_PREFIX}{encoded}");
        assert_eq!(decode(&raw), Err(TicketError::UnsupportedVersion(0xfe)));
    }

    #[test]
    fn host_peer_id_addr_mismatch_errors_as_malformed() {
        let bad = WireBody {
            ticket_id: TicketId::from_bytes([3; 16]),
            session_id: SessionId::from_bytes([1; 16]),
            host_peer_id: PeerId::from_bytes([0xaa; 32]),
            host_addr: WireEndpointAddr::id_only(PeerId::from_bytes([0xbb; 32])),
            granted_cap: Capability::Read,
            expiry_ms: 0,
            cap_sig: SIGNATURE_UNSIGNED,
        };
        let raw = raw_frame(TICKET_VERSION, &bad);
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
        // id-only (no relay, no direct addrs) = 1 (ver) + 16 (ticket_id) +
        // 16 (session_id) + 32 (peer id) + 32 (addr peer id) + 0 (empty
        // relay) + 0 (empty addrs) + 1 (cap) + 8 (expiry) + 64 (cap_sig)
        // ≈ ~170 bytes postcard. base32 encodes at 8:5, so ~272 chars +
        // "artel:" prefix. Fits on two terminal lines; still paste-friendly.
        let encoded = encode(&sample());
        assert!(
            encoded.len() <= 300,
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
    fn ticket_version_is_four() {
        // Deliberately unchanged by the revocation slice: TicketId has
        // been on the wire since v3, so enforcement needs no bump. Also
        // unchanged by the version-dispatch refactor (finding #2): the
        // version moved *out* of the postcard body to a leading byte,
        // but the byte layout — and thus the version — is identical.
        assert_eq!(TICKET_VERSION, 4);
    }

    #[test]
    fn encoded_frame_is_version_byte_then_postcard_body() {
        // Pin the wire layout: the decoded frame must be exactly
        // [TICKET_VERSION] followed by postcard(WireBody). This is what
        // keeps the version-outside-the-body refactor byte-compatible
        // with the old version-first-field-of-Wire encoding, and guards
        // against anyone folding the version back into the body.
        let ticket = sample();
        let encoded = encode(&ticket);
        let frame = BASE32_NOPAD
            .decode(
                encoded
                    .strip_prefix(TICKET_PREFIX)
                    .unwrap()
                    .to_ascii_uppercase()
                    .as_bytes(),
            )
            .unwrap();
        assert_eq!(frame[0], TICKET_VERSION, "leading byte is the version");

        let body = WireBody {
            ticket_id: ticket.ticket_id,
            session_id: ticket.session_id,
            host_peer_id: ticket.host_peer_id,
            host_addr: ticket.host_addr.clone(),
            granted_cap: ticket.granted_cap,
            expiry_ms: ticket.expiry_ms,
            cap_sig: ticket.cap_sig,
        };
        assert_eq!(
            &frame[1..],
            postcard::to_stdvec(&body).unwrap().as_slice(),
            "remaining bytes are the postcard body, version excluded",
        );
    }

    #[test]
    fn ticket_entry_round_trips_postcard_and_json() {
        let entry = TicketEntry {
            ticket_id: TicketId::from_bytes([0x11; 16]),
            granted_cap: Capability::Read,
            expiry_ms: 1_800_000_000_000,
            issued_at_ms: 1_700_000_000_000,
            status: TicketStatus::Revoked,
            used_by: vec![PeerId::from_bytes([0x22; 32])],
        };
        let bytes = postcard::to_stdvec(&entry).unwrap();
        let back: TicketEntry = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, entry);

        let json = serde_json::to_string(&entry).unwrap();
        let back: TicketEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back, entry);
    }

    #[test]
    fn ticket_status_json_is_snake_case() {
        assert_eq!(
            serde_json::to_string(&TicketStatus::Active).unwrap(),
            "\"active\""
        );
        assert_eq!(
            serde_json::to_string(&TicketStatus::Revoked).unwrap(),
            "\"revoked\""
        );
    }

    #[test]
    fn ticket_id_survives_round_trip() {
        let original = sample();
        let decoded = decode(&encode(&original)).unwrap();
        assert_eq!(decoded.ticket_id, original.ticket_id);
        assert_eq!(decoded.ticket_id, TicketId::from_bytes([0x01; 16]));
    }

    #[test]
    fn read_cap_ticket_round_trips() {
        let original = sample_with_addrs();
        assert_eq!(original.granted_cap, Capability::Read);
        let decoded = decode(&encode(&original)).unwrap();
        assert_eq!(decoded.granted_cap, Capability::Read);
        assert_eq!(decoded.expiry_ms, 1_700_000_000_000);
        assert_eq!(decoded.cap_sig, [0xbb; 64]);
    }

    #[test]
    fn readwrite_cap_ticket_round_trips() {
        let original = sample();
        assert_eq!(original.granted_cap, Capability::ReadWrite);
        let decoded = decode(&encode(&original)).unwrap();
        assert_eq!(decoded.granted_cap, Capability::ReadWrite);
        assert_eq!(decoded.expiry_ms, 0);
        assert_eq!(decoded.cap_sig, [0xaa; 64]);
    }

    #[test]
    fn previous_version_byte_is_now_unsupported() {
        let peer = PeerId::from_bytes([0x55; 32]);
        let prev = WireBody {
            ticket_id: TicketId::from_bytes([0x44; 16]),
            session_id: SessionId::from_bytes([0x66; 16]),
            host_peer_id: peer,
            host_addr: WireEndpointAddr::id_only(peer),
            granted_cap: Capability::ReadWrite,
            expiry_ms: 0,
            cap_sig: SIGNATURE_UNSIGNED,
        };
        let raw = raw_frame(3, &prev);
        assert_eq!(decode(&raw), Err(TicketError::UnsupportedVersion(3)));
    }

    proptest! {
        #[test]
        fn round_trip_arb(
            tid in any::<[u8; 16]>(),
            sid in any::<[u8; 16]>(),
            peer in any::<[u8; 32]>(),
            cap_is_rw in any::<bool>(),
            expiry in any::<u64>(),
            sig in any::<[u8; 64]>(),
        ) {
            let host_peer_id = PeerId::from_bytes(peer);
            let granted_cap = if cap_is_rw { Capability::ReadWrite } else { Capability::Read };
            let original = SessionTicket {
                ticket_id: TicketId::from_bytes(tid),
                session_id: SessionId::from_bytes(sid),
                host_peer_id,
                host_addr: WireEndpointAddr::id_only(host_peer_id),
                granted_cap,
                expiry_ms: expiry,
                cap_sig: sig,
            };
            let encoded = encode(&original);
            let decoded = decode(&encoded).expect("round trip");
            prop_assert_eq!(decoded, original);
        }
    }
}
