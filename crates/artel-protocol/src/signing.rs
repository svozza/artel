//! Per-message ed25519 signing for [`SessionMessage`].
//!
//! See `docs/brainstorms/2026-05-30-auth-story-brainstorm.md` § L3 and
//! `docs/plans/2026-06-02-auth-slice-b-l3-signing-plan.md` § B1. The
//! signature scope is intentionally narrower than the full struct —
//! [`Seq`](crate::ids::Seq) is host-assigned and excluded so a joiner
//! can sign its body before the host stamps the seq.
//!
//! ## Canonical bytes
//!
//! The signed bytes are NOT the postcard encoding of the whole
//! message. They are a domain-separated, fixed-format byte string
//! built from a stable subset:
//!
//! ```text
//! "artel/sig-v1"  ||  session_id (16 bytes)
//!                ||  message_format (1 byte)
//!                ||  timestamp_ms_be (8 bytes)
//!                ||  peer.id (32 bytes)
//!                ||  kind_tag (1 byte: chat=0, tool=1, system=2,
//!                                       capability=3 reserved)
//!                ||  action_len_be (4 bytes) || action_utf8
//!                ||  payload_len_be (4 bytes) || payload_bytes
//! ```
//!
//! Domain prefix (`"artel/sig-v1"`) prevents cross-protocol reuse: a
//! signed [`SessionMessage`] body cannot be replayed as, say, a signed
//! iroh routing frame even if both happen to be ed25519-signed.
//! `session_id` is included so future `Grant`-style events can't be
//! cross-session-replayed (see brainstorm § Threat Model).
//! `message_format` rides inside the signed bytes so a downgrade
//! attack (force `version` back to `1`, the unsigned-era shape) flips
//! the signature; `version=2` is the floor.
//!
//! Capability kind tag is reserved at byte `3` even though the enum
//! doesn't define [`MessageKind::Capability`] yet — Slice C lands the
//! variant, and pre-allocating the byte means existing v2 signatures
//! stay valid post-C.
//!
//! ## Why hand-rolled, not postcard
//!
//! Postcard's varint for `seq` would bleed seq into the signed bytes,
//! and we explicitly *don't* sign seq. Hand-rolling the canonical
//! layout avoids both that and any future postcard-encoding tweak
//! breaking existing signatures.

use ed25519_dalek::{Signature, Signer, VerifyingKey};
pub use ed25519_dalek::{SigningKey, VerifyingKey as EdVerifyingKey};
use uuid::Uuid;

use crate::capability::Capability;
use crate::error::ProtocolError;
use crate::ids::{PeerId, Seq, SessionId, TicketId};
use crate::message::{
    MESSAGE_FORMAT, MessageFormat, MessageKind, PeerInfo, SIGNATURE_UNSIGNED, SessionMessage,
    SigBytes,
};

/// Domain-prefix string baked into every canonical-bytes blob.
///
/// Versioned so a future reshape (different field order, new field
/// set, different domain) is unambiguous: bumping to `artel/sig-v2`
/// invalidates every old signature by construction.
pub const DOMAIN_TAG: &[u8] = b"artel/sig-v1";

const fn kind_tag(kind: MessageKind) -> u8 {
    match kind {
        MessageKind::Chat => 0,
        MessageKind::Tool => 1,
        MessageKind::System => 2,
        // Byte 3 was pre-reserved for this in Slice B so existing v3
        // signatures stay valid once Slice C lands the variant.
        MessageKind::Capability => 3,
    }
}

/// Build the canonical signed bytes for a message body.
///
/// Used by both signers and verifiers — they MUST agree byte-for-byte.
/// Length-prefix every variable field to foreclose
/// extension/truncation collisions.
#[must_use]
pub fn canonical_bytes(
    session_id: SessionId,
    version: MessageFormat,
    timestamp_ms: u64,
    peer: &PeerInfo,
    kind: MessageKind,
    action: &str,
    payload: &[u8],
) -> Vec<u8> {
    let action = action.as_bytes();
    let mut out = Vec::with_capacity(
        DOMAIN_TAG.len() + 16 + 1 + 8 + 32 + 1 + 4 + action.len() + 4 + payload.len(),
    );
    out.extend_from_slice(DOMAIN_TAG);
    out.extend_from_slice(session_id.as_bytes());
    out.push(version.get());
    out.extend_from_slice(&timestamp_ms.to_be_bytes());
    out.extend_from_slice(peer.id.as_bytes());
    out.push(kind_tag(kind));
    out.extend_from_slice(
        &u32::try_from(action.len())
            .expect("action <= 4 GiB")
            .to_be_bytes(),
    );
    out.extend_from_slice(action);
    out.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("payload <= 4 GiB")
            .to_be_bytes(),
    );
    out.extend_from_slice(payload);
    out
}

/// Sign a message body using a daemon's signing key.
///
/// Returns the 64-byte ed25519 signature; the caller stamps it onto
/// the [`SessionMessage`] (or a [`crate::rpc::SignedSendPayload`])
/// before broadcasting.
#[must_use]
#[allow(clippy::too_many_arguments)] // mirrors the canonical-bytes scope
pub fn sign_body(
    key: &SigningKey,
    session_id: SessionId,
    version: MessageFormat,
    timestamp_ms: u64,
    peer: &PeerInfo,
    kind: MessageKind,
    action: &str,
    payload: &[u8],
) -> SigBytes {
    let bytes = canonical_bytes(
        session_id,
        version,
        timestamp_ms,
        peer,
        kind,
        action,
        payload,
    );
    key.sign(&bytes).to_bytes()
}

/// Verify a [`SessionMessage`]'s signature against `peer.id` as a
/// public key.
///
/// The caller passes `session_id` separately because it isn't on the
/// message — the carrier (gossip topic / on-disk directory) names the
/// session.
///
/// # Errors
///
/// - [`VerifyError::SentinelUnsigned`] if `signature` is the all-zero
///   sentinel ([`SIGNATURE_UNSIGNED`]). A freshly-constructed v2
///   message that never went through [`sign_body`] hits this.
/// - [`VerifyError::VersionTooOld`] if `message.version` is below
///   [`MESSAGE_FORMAT`] — the signed-era floor. This is the active
///   downgrade defense: the doc-comment's "the signature flips" only
///   holds against *tampering* a v2 frame, but an author who signs a
///   self-consistent sub-floor frame would otherwise verify fine.
///   Rejecting `version < MESSAGE_FORMAT` here makes the floor real
///   rather than aspirational.
/// - [`VerifyError::BadKey`] if `peer.id` is not a valid ed25519
///   public key (i.e. doesn't decode to a curve point).
/// - [`VerifyError::BadSig`] if the signature does not verify under
///   `peer.id`. Uses ed25519 *strict* verification, which additionally
///   rejects the malleable / small-order signature forms that the
///   permissive `verify` would accept — so a captured signature can't
///   be reshaped into a second distinct-but-valid 64-byte blob.
pub fn verify_message(
    session_id: SessionId,
    message: &SessionMessage,
    signature: &SigBytes,
) -> Result<(), VerifyError> {
    if signature == &SIGNATURE_UNSIGNED {
        return Err(VerifyError::SentinelUnsigned);
    }
    if message.version < MESSAGE_FORMAT {
        return Err(VerifyError::VersionTooOld {
            version: message.version.get(),
            floor: MESSAGE_FORMAT.get(),
        });
    }
    let verifying =
        VerifyingKey::from_bytes(message.peer.id.as_bytes()).map_err(|_| VerifyError::BadKey)?;
    let sig = Signature::from_bytes(signature);
    let bytes = canonical_bytes(
        session_id,
        message.version,
        message.timestamp_ms,
        &message.peer,
        message.kind,
        &message.action,
        &message.payload,
    );
    // `verify_strict` (not the permissive `Verifier::verify`) so
    // ed25519 signature malleability is rejected: only the canonical
    // (R, S) form passes. See `VerifyError::BadSig`.
    verifying
        .verify_strict(&bytes, &sig)
        .map_err(|_| VerifyError::BadSig)
}

/// Why a [`verify_message`] call rejected a candidate.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VerifyError {
    /// The signature was the all-zero sentinel — the body was never
    /// signed. Once Slice B2 turns verification on, every receive
    /// path must reject this catastrophically: it's the lit fuse for
    /// "we forgot to wire signing in".
    #[error("signature is the zero sentinel; message was never signed")]
    SentinelUnsigned,
    /// `message.version` is below the signed-era floor
    /// ([`MESSAGE_FORMAT`]). A downgrade to a pre-signing wire shape
    /// is rejected before any crypto runs — the floor is enforced
    /// here, not merely folded into the canonical bytes.
    #[error("message version {version} is below the signed floor {floor}")]
    VersionTooOld {
        /// The version stamped on the rejected message.
        version: u8,
        /// The minimum accepted version ([`MESSAGE_FORMAT`]).
        floor: u8,
    },
    /// `peer.id` does not decode as an ed25519 public key. Drop and
    /// do not append.
    #[error("peer id is not a valid ed25519 public key")]
    BadKey,
    /// Signature does not verify against `peer.id` over the canonical
    /// bytes.
    #[error("signature does not verify against peer id")]
    BadSig,
}

/// Short, allocation-free diagnostic for a [`VerifyError`].
///
/// Suitable for the `reason` half of a wire-facing
/// [`crate::ProtocolError::Signature`]. Names the failure mode but
/// never leaks signature bytes. Shared by both daemon-side rejection
/// sites (host bridge and the registry's `Remote` authoring arm) so
/// the joiner-facing wording is defined once.
#[must_use]
pub const fn verify_reason(err: &VerifyError) -> &'static str {
    match err {
        VerifyError::SentinelUnsigned => "unsigned sentinel",
        VerifyError::VersionTooOld { .. } => "message version below signed floor",
        VerifyError::BadKey => "peer.id is not a valid ed25519 key",
        VerifyError::BadSig => "signature does not verify",
    }
}

// ===========================================================================
// Host-origin signatures (Auth Slice B.5)
//
// Three additional domain-separated canonical-byte layouts the **host**
// signs over, distinct from the author `canonical_bytes` above. Each binds
// a host-originated or host-sequenced frame to the host key so a joiner can
// authenticate origin against the host pubkey it persists as `session.host`
// (= the ticket's `host_peer_id`) — topology-independent, not relayer-based.
//
// Same hand-rolled, big-endian, length-prefixed, domain-separated discipline
// as `canonical_bytes`: fixed-width fields are emitted as-is; variable-width
// fields are length-prefixed. ed25519 hashes the message internally so no
// separate digest dependency is needed.
// ===========================================================================

/// Domain prefix for the host's per-message **sequencing** signature.
///
/// Binds *this seq* to *this author signature* under the host key:
/// `"artel/seq-v1" || session_id || seq || author_sig`. A genuine frame
/// replayed under a different seq fails this check (the captured `host_sig`
/// is bound to the original seq) — closes finding #1.
pub const SEQ_DOMAIN_TAG: &[u8] = b"artel/seq-v1";

/// Domain prefix for the host's [`crate::gossip::GossipBody::SendAck`]
/// signature.
///
/// Binds the ack `result` so a signed `Ok` cannot be flipped to `Err` (or
/// vice-versa): `"artel/ack-v1" || session_id || req_id ||
/// result_discriminant || postcard(result)`.
pub const ACK_DOMAIN_TAG: &[u8] = b"artel/ack-v1";

/// Domain prefix for the host's control-frame signature.
///
/// Shared by [`crate::gossip::GossipBody::SessionClosed`] **and**
/// [`crate::gossip::GossipBody::EpochBeacon`]:
/// `"artel/ctrl-v1" || session_id || host_epoch`. One verifier
/// ([`verify_ctrl`]) serves both frames, so a `host_sig` produced for a
/// beacon validates a close at the same epoch.
pub const CTRL_DOMAIN_TAG: &[u8] = b"artel/ctrl-v1";

/// Canonical bytes for the host's per-message sequencing signature.
///
/// `SEQ_DOMAIN_TAG || session_id(16) || seq.to_be_bytes()(8) ||
/// author_sig(64)`. All fields fixed-width.
#[must_use]
pub fn seq_canonical_bytes(session_id: SessionId, seq: Seq, author_sig: &SigBytes) -> Vec<u8> {
    let mut out = Vec::with_capacity(SEQ_DOMAIN_TAG.len() + 16 + 8 + 64);
    out.extend_from_slice(SEQ_DOMAIN_TAG);
    out.extend_from_slice(session_id.as_bytes());
    out.extend_from_slice(&seq.get().to_be_bytes());
    out.extend_from_slice(author_sig);
    out
}

/// Sign the sequencing canonical bytes with the host's key.
#[must_use]
pub fn sign_seq(
    key: &SigningKey,
    session_id: SessionId,
    seq: Seq,
    author_sig: &SigBytes,
) -> SigBytes {
    key.sign(&seq_canonical_bytes(session_id, seq, author_sig))
        .to_bytes()
}

/// Verify a host sequencing signature against the host's public key.
///
/// # Errors
///
/// - [`VerifyError::SentinelUnsigned`] if `host_sig` is the all-zero
///   sentinel ([`SIGNATURE_UNSIGNED`]).
/// - [`VerifyError::BadKey`] if `host_pubkey` is not a valid ed25519 key.
/// - [`VerifyError::BadSig`] if the signature does not verify (wrong host,
///   tampered seq, or tampered author sig). Uses strict verification.
pub fn verify_seq(
    host_pubkey: &PeerId,
    session_id: SessionId,
    seq: Seq,
    author_sig: &SigBytes,
    host_sig: &SigBytes,
) -> Result<(), VerifyError> {
    if host_sig == &SIGNATURE_UNSIGNED {
        return Err(VerifyError::SentinelUnsigned);
    }
    let verifying =
        VerifyingKey::from_bytes(host_pubkey.as_bytes()).map_err(|_| VerifyError::BadKey)?;
    let sig = Signature::from_bytes(host_sig);
    verifying
        .verify_strict(&seq_canonical_bytes(session_id, seq, author_sig), &sig)
        .map_err(|_| VerifyError::BadSig)
}

/// Canonical bytes for a host [`crate::gossip::GossipBody::SendAck`]
/// signature.
///
/// `ACK_DOMAIN_TAG || session_id(16) || req_id(16) || disc(1) ||
/// postcard(result)`. The 1-byte discriminant (`0`=Ok, `1`=Err) plus the
/// postcard-encoded `result` bind the verdict so Ok↔Err cannot be flipped.
#[must_use]
pub fn ack_canonical_bytes(
    session_id: SessionId,
    req_id: Uuid,
    result: &Result<SessionMessage, ProtocolError>,
) -> Vec<u8> {
    let (disc, body): (u8, Vec<u8>) = match result {
        Ok(msg) => (
            0,
            postcard::to_allocvec(msg).expect("postcard encode SessionMessage"),
        ),
        Err(err) => (
            1,
            postcard::to_allocvec(err).expect("postcard encode ProtocolError"),
        ),
    };
    let mut out = Vec::with_capacity(ACK_DOMAIN_TAG.len() + 16 + 16 + 1 + body.len());
    out.extend_from_slice(ACK_DOMAIN_TAG);
    out.extend_from_slice(session_id.as_bytes());
    out.extend_from_slice(req_id.as_bytes());
    out.push(disc);
    out.extend_from_slice(&body);
    out
}

/// Sign a host `SendAck` verdict.
#[must_use]
pub fn sign_ack(
    key: &SigningKey,
    session_id: SessionId,
    req_id: Uuid,
    result: &Result<SessionMessage, ProtocolError>,
) -> SigBytes {
    key.sign(&ack_canonical_bytes(session_id, req_id, result))
        .to_bytes()
}

/// Verify a host `SendAck` signature against the host's public key.
///
/// # Errors
///
/// Mirrors [`verify_seq`]: sentinel, bad key, or bad sig. A flipped
/// `result` (Ok↔Err) or tampered message yields [`VerifyError::BadSig`].
pub fn verify_ack(
    host_pubkey: &PeerId,
    session_id: SessionId,
    req_id: Uuid,
    result: &Result<SessionMessage, ProtocolError>,
    host_sig: &SigBytes,
) -> Result<(), VerifyError> {
    if host_sig == &SIGNATURE_UNSIGNED {
        return Err(VerifyError::SentinelUnsigned);
    }
    let verifying =
        VerifyingKey::from_bytes(host_pubkey.as_bytes()).map_err(|_| VerifyError::BadKey)?;
    let sig = Signature::from_bytes(host_sig);
    verifying
        .verify_strict(&ack_canonical_bytes(session_id, req_id, result), &sig)
        .map_err(|_| VerifyError::BadSig)
}

/// Canonical bytes for a host control-frame signature
/// ([`crate::gossip::GossipBody::SessionClosed`] and
/// [`crate::gossip::GossipBody::EpochBeacon`]).
///
/// `CTRL_DOMAIN_TAG || session_id(16) || host_epoch.to_be_bytes()(8)`. The
/// `host_epoch` is the freshness element defeating replay across a same-id
/// host resume (the iroh endpoint secret is stable, so a host signature
/// alone can't distinguish incarnations N and N+1).
#[must_use]
pub fn ctrl_canonical_bytes(session_id: SessionId, host_epoch: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(CTRL_DOMAIN_TAG.len() + 16 + 8);
    out.extend_from_slice(CTRL_DOMAIN_TAG);
    out.extend_from_slice(session_id.as_bytes());
    out.extend_from_slice(&host_epoch.to_be_bytes());
    out
}

/// Sign a host control-frame (close / epoch beacon) at `host_epoch`.
#[must_use]
pub fn sign_ctrl(key: &SigningKey, session_id: SessionId, host_epoch: u64) -> SigBytes {
    key.sign(&ctrl_canonical_bytes(session_id, host_epoch))
        .to_bytes()
}

/// Verify a host control-frame signature against the host's public key.
///
/// Shared by `SessionClosed` and `EpochBeacon` — a `host_sig` produced for
/// either verifies the other at the same epoch.
///
/// # Errors
///
/// Mirrors [`verify_seq`]: sentinel, bad key, or bad sig. A tampered
/// `host_epoch` yields [`VerifyError::BadSig`].
pub fn verify_ctrl(
    host_pubkey: &PeerId,
    session_id: SessionId,
    host_epoch: u64,
    host_sig: &SigBytes,
) -> Result<(), VerifyError> {
    if host_sig == &SIGNATURE_UNSIGNED {
        return Err(VerifyError::SentinelUnsigned);
    }
    let verifying =
        VerifyingKey::from_bytes(host_pubkey.as_bytes()).map_err(|_| VerifyError::BadKey)?;
    let sig = Signature::from_bytes(host_sig);
    verifying
        .verify_strict(&ctrl_canonical_bytes(session_id, host_epoch), &sig)
        .map_err(|_| VerifyError::BadSig)
}

// ===========================================================================
// Ticket-cap signatures (Tiered Tickets)
//
// The host signs (ticket_id, session_id, granted_cap, expiry_ms) under a
// dedicated domain so the ticket is a self-contained, stateless bearer token.
// The host verifies its own signature at admission — no persistent registry of
// issued tickets needed.
// ===========================================================================

/// Domain prefix for ticket capability claims.
pub const TICKET_CAP_DOMAIN_TAG: &[u8] = b"artel/ticket-cap-v1";

/// Pinned single-byte encoding for [`Capability`] inside the signed scope.
///
/// NOT postcard-derived — we pin these manually so a serde representation
/// change can never silently invalidate existing ticket signatures.
const fn cap_byte(cap: Capability) -> u8 {
    match cap {
        Capability::Read => 0,
        Capability::ReadWrite => 1,
    }
}

/// Build the canonical signed bytes for a ticket capability claim.
///
/// Layout: `TICKET_CAP_DOMAIN_TAG || ticket_id(16) || session_id(16) ||
/// cap_byte(1) || expiry_ms_be(8)`. All fields fixed-width.
#[must_use]
pub fn ticket_cap_canonical_bytes(
    ticket_id: TicketId,
    session_id: SessionId,
    granted_cap: Capability,
    expiry_ms: u64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(TICKET_CAP_DOMAIN_TAG.len() + 16 + 16 + 1 + 8);
    out.extend_from_slice(TICKET_CAP_DOMAIN_TAG);
    out.extend_from_slice(ticket_id.as_bytes());
    out.extend_from_slice(session_id.as_bytes());
    out.push(cap_byte(granted_cap));
    out.extend_from_slice(&expiry_ms.to_be_bytes());
    out
}

/// Sign a ticket capability claim with the host's signing key.
#[must_use]
pub fn sign_ticket_cap(
    key: &SigningKey,
    ticket_id: TicketId,
    session_id: SessionId,
    granted_cap: Capability,
    expiry_ms: u64,
) -> SigBytes {
    key.sign(&ticket_cap_canonical_bytes(
        ticket_id, session_id, granted_cap, expiry_ms,
    ))
    .to_bytes()
}

/// Verify a ticket capability claim against the host's public key.
///
/// # Errors
///
/// - [`VerifyError::SentinelUnsigned`] if `cap_sig` is the all-zero sentinel.
/// - [`VerifyError::BadKey`] if `host_pubkey` is not a valid ed25519 key.
/// - [`VerifyError::BadSig`] if the signature does not verify (wrong host,
///   tampered fields, or wrong cap). Uses strict verification.
pub fn verify_ticket_cap(
    host_pubkey: &PeerId,
    ticket_id: TicketId,
    session_id: SessionId,
    granted_cap: Capability,
    expiry_ms: u64,
    cap_sig: &SigBytes,
) -> Result<(), VerifyError> {
    if cap_sig == &SIGNATURE_UNSIGNED {
        return Err(VerifyError::SentinelUnsigned);
    }
    let verifying =
        VerifyingKey::from_bytes(host_pubkey.as_bytes()).map_err(|_| VerifyError::BadKey)?;
    let sig = Signature::from_bytes(cap_sig);
    verifying
        .verify_strict(
            &ticket_cap_canonical_bytes(ticket_id, session_id, granted_cap, expiry_ms),
            &sig,
        )
        .map_err(|_| VerifyError::BadSig)
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::ids::PeerId;
    use crate::message::{MESSAGE_FORMAT, PeerInfo, SIGNATURE_UNSIGNED, SessionMessage};

    /// Deterministic 32-byte seed for fixture keys; gives us the same
    /// signing key across runs without a `Date.now()` / RNG dance.
    const FIXTURE_SEED_A: [u8; 32] = [0x11; 32];
    const FIXTURE_SEED_B: [u8; 32] = [0x22; 32];

    fn key_a() -> SigningKey {
        SigningKey::from_bytes(&FIXTURE_SEED_A)
    }
    fn key_b() -> SigningKey {
        SigningKey::from_bytes(&FIXTURE_SEED_B)
    }

    fn peer_for(key: &SigningKey) -> PeerInfo {
        let pk = key.verifying_key();
        PeerInfo::new(PeerId::from_bytes(pk.to_bytes()), "alice")
    }

    fn sample_session_id() -> SessionId {
        SessionId::from_bytes([0xab; 16])
    }

    /// Builds a `SessionMessage` whose **non-seq** fields exactly match
    /// what the per-test `sign_body` call passed in. seq is arbitrary
    /// (host-assigned, excluded from the signed scope).
    fn body_matching(
        peer: PeerInfo,
        timestamp_ms: u64,
        kind: MessageKind,
        action: &str,
        payload: &[u8],
        signature: SigBytes,
    ) -> SessionMessage {
        SessionMessage::new(
            crate::ids::Seq::new(7),
            timestamp_ms,
            peer,
            kind,
            action.to_string(),
            payload.to_vec(),
            signature,
            SIGNATURE_UNSIGNED,
        )
    }

    #[test]
    fn domain_tag_is_artel_sig_v1() {
        assert_eq!(DOMAIN_TAG, b"artel/sig-v1");
    }

    #[test]
    fn canonical_bytes_includes_session_id_at_a_known_offset() {
        // Build two blobs that differ ONLY in session_id; the diff
        // must live at byte DOMAIN_TAG.len() (= 12). Pins the field
        // order without snapshotting the whole blob.
        let peer = peer_for(&key_a());
        let a = canonical_bytes(
            SessionId::from_bytes([0x01; 16]),
            MESSAGE_FORMAT,
            42,
            &peer,
            MessageKind::Chat,
            "x",
            b"y",
        );
        let b = canonical_bytes(
            SessionId::from_bytes([0x02; 16]),
            MESSAGE_FORMAT,
            42,
            &peer,
            MessageKind::Chat,
            "x",
            b"y",
        );
        let first_diff = a
            .iter()
            .zip(b.iter())
            .position(|(x, y)| x != y)
            .expect("blobs must differ");
        assert_eq!(first_diff, DOMAIN_TAG.len());
    }

    #[test]
    fn canonical_bytes_excludes_seq() {
        // `seq` lives on `SessionMessage` but NOT on the canonical
        // bytes — the joiner signs before the host stamps seq. This
        // test pins that property: canonical_bytes() doesn't take
        // seq, so reaching for it is impossible by construction;
        // we re-assert it via `sign_body` returning the same bytes
        // for two messages that differ only in seq.
        let key = key_a();
        let peer = peer_for(&key);
        let s = sample_session_id();
        let (ts, kind, action, payload) = (42u64, MessageKind::Chat, "x", &b"y"[..]);
        let sig1 = sign_body(&key, s, MESSAGE_FORMAT, ts, &peer, kind, action, payload);
        let sig2 = sign_body(&key, s, MESSAGE_FORMAT, ts, &peer, kind, action, payload);
        assert_eq!(sig1, sig2, "deterministic over inputs");
        // Mutating only seq on a `SessionMessage` does not flip the
        // verifier — the canonical bytes ignore seq.
        let mut m = body_matching(peer, ts, kind, action, payload, sig1);
        verify_message(s, &m, &sig1).unwrap();
        m.seq = crate::ids::Seq::new(99);
        verify_message(s, &m, &sig1).unwrap();
    }

    #[test]
    fn sign_then_verify_round_trip() {
        let key = key_a();
        let peer = peer_for(&key);
        let s = sample_session_id();
        let (ts, kind, action, payload) = (42u64, MessageKind::Chat, "chat.message", &b"hi"[..]);
        let sig = sign_body(&key, s, MESSAGE_FORMAT, ts, &peer, kind, action, payload);
        let m = body_matching(peer, ts, kind, action, payload, sig);
        verify_message(s, &m, &sig).unwrap();
    }

    #[test]
    fn kind_tag_values_are_pinned() {
        // The byte values are part of the signed bytes; changing one
        // silently invalidates every existing signature of that kind.
        // Pin all four — `Capability` fills the byte Slice B reserved.
        assert_eq!(kind_tag(MessageKind::Chat), 0);
        assert_eq!(kind_tag(MessageKind::Tool), 1);
        assert_eq!(kind_tag(MessageKind::System), 2);
        assert_eq!(kind_tag(MessageKind::Capability), 3);
    }

    #[test]
    fn capability_body_sign_then_verify_round_trip() {
        // A `Capability`-kind body signs and verifies through the exact
        // same `sign_body`/`verify_message` path as every other kind —
        // proving the pre-reserved byte-3 arm "just works" with no
        // signing.rs change beyond filling the arm.
        let key = key_a();
        let peer = peer_for(&key);
        let s = sample_session_id();
        let (ts, kind, action, payload) = (
            42u64,
            MessageKind::Capability,
            "capability.grant",
            &b"some-postcard-action"[..],
        );
        let sig = sign_body(&key, s, MESSAGE_FORMAT, ts, &peer, kind, action, payload);
        let m = body_matching(peer, ts, kind, action, payload, sig);
        verify_message(s, &m, &sig).unwrap();
    }

    #[test]
    fn preexisting_kind_signature_still_verifies_post_capability() {
        // Regression guard for the "no MESSAGE_FORMAT bump" decision
        // (brainstorm Q6): adding `MessageKind::Capability` must not
        // perturb the canonical bytes of the three pre-existing kinds,
        // so a signature produced for a Chat/Tool/System body still
        // verifies after the variant lands. If `kind_tag` ever reorders
        // the existing arms, this fails loudly.
        let key = key_a();
        let peer = peer_for(&key);
        let s = sample_session_id();
        for kind in [MessageKind::Chat, MessageKind::Tool, MessageKind::System] {
            let (ts, action, payload) = (7u64, "x", &b"y"[..]);
            let sig = sign_body(&key, s, MESSAGE_FORMAT, ts, &peer, kind, action, payload);
            let m = body_matching(peer.clone(), ts, kind, action, payload, sig);
            verify_message(s, &m, &sig)
                .unwrap_or_else(|e| panic!("{kind:?} signature should still verify: {e:?}"));
        }
    }

    #[test]
    fn verify_rejects_sentinel_unsigned() {
        let peer = peer_for(&key_a());
        let m = body_matching(peer, 42, MessageKind::Chat, "x", b"hi", SIGNATURE_UNSIGNED);
        let err = verify_message(sample_session_id(), &m, &SIGNATURE_UNSIGNED).unwrap_err();
        assert_eq!(err, VerifyError::SentinelUnsigned);
    }

    #[test]
    fn verify_rejects_wrong_signer() {
        let signer = key_a();
        let other = key_b();
        // Body says "this peer is `signer`'s public key" but we sign
        // with `other` — verifier loads `signer`'s pubkey from peer.id
        // and rejects the foreign sig.
        let peer = peer_for(&signer);
        let s = sample_session_id();
        let (ts, kind, action, payload) = (42u64, MessageKind::Chat, "x", &b"y"[..]);
        let sig = sign_body(&other, s, MESSAGE_FORMAT, ts, &peer, kind, action, payload);
        let m = body_matching(peer, ts, kind, action, payload, sig);
        assert_eq!(
            verify_message(s, &m, &sig).unwrap_err(),
            VerifyError::BadSig
        );
    }

    #[test]
    fn verify_rejects_tampered_payload() {
        let key = key_a();
        let peer = peer_for(&key);
        let s = sample_session_id();
        let (ts, kind, action, payload) = (42u64, MessageKind::Chat, "x", &b"hi"[..]);
        let sig = sign_body(&key, s, MESSAGE_FORMAT, ts, &peer, kind, action, payload);
        let mut m = body_matching(peer, ts, kind, action, payload, sig);
        m.payload = b"hj".to_vec(); // flip one byte
        assert_eq!(
            verify_message(s, &m, &sig).unwrap_err(),
            VerifyError::BadSig
        );
    }

    #[test]
    fn verify_rejects_tampered_session_id() {
        let key = key_a();
        let peer = peer_for(&key);
        let signed_for = SessionId::from_bytes([0x01; 16]);
        let (ts, kind, action, payload) = (42u64, MessageKind::Chat, "x", &b"hi"[..]);
        let sig = sign_body(
            &key,
            signed_for,
            MESSAGE_FORMAT,
            ts,
            &peer,
            kind,
            action,
            payload,
        );
        let m = body_matching(peer, ts, kind, action, payload, sig);
        let other = SessionId::from_bytes([0x02; 16]);
        assert_eq!(
            verify_message(other, &m, &sig).unwrap_err(),
            VerifyError::BadSig,
        );
    }

    #[test]
    fn verify_rejects_tampered_kind() {
        let key = key_a();
        let peer = peer_for(&key);
        let s = sample_session_id();
        // Sign as Chat; fake the body as Tool.
        let (ts, kind, action, payload) = (42u64, MessageKind::Chat, "x", &b"hi"[..]);
        let sig = sign_body(&key, s, MESSAGE_FORMAT, ts, &peer, kind, action, payload);
        let mut m = body_matching(peer, ts, kind, action, payload, sig);
        m.kind = MessageKind::Tool;
        assert_eq!(
            verify_message(s, &m, &sig).unwrap_err(),
            VerifyError::BadSig,
        );
    }

    #[test]
    fn verify_rejects_tampered_timestamp() {
        let key = key_a();
        let peer = peer_for(&key);
        let s = sample_session_id();
        let (ts, kind, action, payload) = (42u64, MessageKind::Chat, "x", &b"hi"[..]);
        let sig = sign_body(&key, s, MESSAGE_FORMAT, ts, &peer, kind, action, payload);
        let mut m = body_matching(peer, ts, kind, action, payload, sig);
        m.timestamp_ms = 43;
        assert_eq!(
            verify_message(s, &m, &sig).unwrap_err(),
            VerifyError::BadSig,
        );
    }

    #[test]
    fn verify_rejects_garbage_peer_id() {
        // Pin the surface: a `peer.id` that isn't the signer's
        // verifying key cannot pass. Whether dalek surfaces this as
        // `BadKey` (non-curve-point compressed y) or `BadSig`
        // (decodes but doesn't verify) is dalek-version dependent
        // and not what we're pinning here — the invariant is
        // "invalid peer.id ⇒ not Ok(())". Both `verify_rejects_*`
        // companion tests directly exercise BadSig; this one
        // exercises the no-trust-in-arbitrary-bytes property.
        let key = key_a();
        let peer_real = peer_for(&key);
        let s = sample_session_id();
        let (ts, kind, action, payload) = (42u64, MessageKind::Chat, "x", &b"hi"[..]);
        let sig = sign_body(
            &key,
            s,
            MESSAGE_FORMAT,
            ts,
            &peer_real,
            kind,
            action,
            payload,
        );
        for cand in [[0xffu8; 32], [0x42u8; 32], [0u8; 32]] {
            let mut m = body_matching(peer_real.clone(), ts, kind, action, payload, sig);
            m.peer = PeerInfo::new(PeerId::from_bytes(cand), "alice");
            match verify_message(s, &m, &sig) {
                Err(VerifyError::BadKey | VerifyError::BadSig) => {}
                other => panic!("garbage peer id {cand:?} passed verification: {other:?}"),
            }
        }
    }

    #[test]
    fn verify_rejects_version_below_floor() {
        // A message claiming a sub-floor version is rejected before
        // any crypto runs — even if its signature is self-consistent
        // over that lower version. Pins the downgrade floor as a real
        // check, not just a byte folded into the canonical bytes.
        let key = key_a();
        let peer = peer_for(&key);
        let s = sample_session_id();
        let (ts, kind, action, payload) = (42u64, MessageKind::Chat, "x", &b"hi"[..]);
        // Sign over version 1 (one below the floor) so the signature
        // is internally valid for the bytes — the floor check must
        // still reject it.
        let v1 = MessageFormat::new(1);
        let sig = sign_body(&key, s, v1, ts, &peer, kind, action, payload);
        let mut m = body_matching(peer, ts, kind, action, payload, sig);
        m.version = v1;
        assert_eq!(
            verify_message(s, &m, &sig).unwrap_err(),
            VerifyError::VersionTooOld {
                version: 1,
                floor: MESSAGE_FORMAT.get(),
            },
        );
    }

    #[test]
    fn verify_rejects_malleated_signature() {
        // ed25519 signatures are malleable under the permissive
        // `verify`: given a valid (R, S), the variant (R, S + L)
        // (L = group order) also satisfies the non-strict equation.
        // `verify_strict` rejects the non-canonical S. Construct the
        // malleated form and assert it no longer verifies.
        //
        // L for ed25519: 2^252 + 27742317777372353535851937790883648493,
        // little-endian.
        const L: [u8; 32] = [
            0xed, 0xd3, 0xf5, 0x5c, 0x1a, 0x63, 0x12, 0x58, 0xd6, 0x9c, 0xf7, 0xa2, 0xde, 0xf9,
            0xde, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x10,
        ];
        let key = key_a();
        let peer = peer_for(&key);
        let s = sample_session_id();
        let (ts, kind, action, payload) = (42u64, MessageKind::Chat, "x", &b"hi"[..]);
        let sig = sign_body(&key, s, MESSAGE_FORMAT, ts, &peer, kind, action, payload);
        let m = body_matching(peer, ts, kind, action, payload, sig);
        // Sanity: the canonical signature verifies.
        verify_message(s, &m, &sig).unwrap();

        // Add the group order L to the scalar S (bytes 32..64,
        // little-endian) to produce a malleated-but-equivalent sig
        // under non-strict verification.
        let mut malleated = sig;
        let mut carry = 0u16;
        for i in 0..32 {
            let sum = u16::from(sig[32 + i]) + u16::from(L[i]) + carry;
            malleated[32 + i] = (sum & 0xff) as u8;
            carry = sum >> 8;
        }
        // Only meaningful if S + L didn't overflow the 32nd byte (it
        // doesn't for a canonical S, which has the high bits clear).
        assert_eq!(carry, 0, "S + L overflowed; test fixture invalid");
        assert_ne!(malleated, sig, "malleated sig must differ");
        assert_eq!(
            verify_message(s, &m, &malleated).unwrap_err(),
            VerifyError::BadSig,
            "verify_strict must reject the malleated signature",
        );
    }

    #[test]
    fn verify_reason_names_each_failure_mode() {
        assert_eq!(
            verify_reason(&VerifyError::SentinelUnsigned),
            "unsigned sentinel"
        );
        assert_eq!(
            verify_reason(&VerifyError::VersionTooOld {
                version: 1,
                floor: 2
            }),
            "message version below signed floor",
        );
        assert_eq!(
            verify_reason(&VerifyError::BadKey),
            "peer.id is not a valid ed25519 key",
        );
        assert_eq!(
            verify_reason(&VerifyError::BadSig),
            "signature does not verify"
        );
    }

    #[test]
    fn verify_rejects_seq_change() {
        // Pin the property: mutating `seq` post-sign does not flip
        // verification. This is what allows the host to stamp seq
        // after the joiner has already signed.
        let key = key_a();
        let peer = peer_for(&key);
        let s = sample_session_id();
        let (ts, kind, action, payload) = (42u64, MessageKind::Chat, "x", &b"hi"[..]);
        let sig = sign_body(&key, s, MESSAGE_FORMAT, ts, &peer, kind, action, payload);
        let mut m = body_matching(peer, ts, kind, action, payload, sig);
        verify_message(s, &m, &sig).unwrap();
        m.seq = crate::ids::Seq::new(u64::MAX);
        verify_message(s, &m, &sig).unwrap();
    }

    // ====================================================================
    // Host-origin signatures (Auth Slice B.5)
    // ====================================================================

    use uuid::Uuid;

    use crate::error::ProtocolError;
    use crate::ids::Seq;

    fn host_pubkey(key: &SigningKey) -> PeerId {
        PeerId::from_bytes(key.verifying_key().to_bytes())
    }

    // ---- domain tags ----

    #[test]
    fn host_domain_tags_are_versioned() {
        assert_eq!(SEQ_DOMAIN_TAG, b"artel/seq-v1");
        assert_eq!(ACK_DOMAIN_TAG, b"artel/ack-v1");
        assert_eq!(CTRL_DOMAIN_TAG, b"artel/ctrl-v1");
    }

    // ---- seq canonical bytes / round-trip ----

    #[test]
    fn seq_canonical_bytes_field_offsets() {
        // session_id sits right after the domain tag; seq right after
        // session_id (offset tag+16); author_sig after that (tag+16+8).
        let author = [0x55u8; 64];
        let base = seq_canonical_bytes(SessionId::from_bytes([0x01; 16]), Seq::new(7), &author);

        let diff_sid = seq_canonical_bytes(SessionId::from_bytes([0x02; 16]), Seq::new(7), &author);
        let first = base
            .iter()
            .zip(diff_sid.iter())
            .position(|(x, y)| x != y)
            .expect("session_id diff");
        assert_eq!(first, SEQ_DOMAIN_TAG.len());

        let diff_seq = seq_canonical_bytes(SessionId::from_bytes([0x01; 16]), Seq::new(8), &author);
        let first = base
            .iter()
            .zip(diff_seq.iter())
            .position(|(x, y)| x != y)
            .expect("seq diff");
        // be-encoded u64 differs in its last byte: tag + 16 + 7.
        assert_eq!(first, SEQ_DOMAIN_TAG.len() + 16 + 7);

        let mut author2 = author;
        author2[0] ^= 0xff;
        let diff_author =
            seq_canonical_bytes(SessionId::from_bytes([0x01; 16]), Seq::new(7), &author2);
        let first = base
            .iter()
            .zip(diff_author.iter())
            .position(|(x, y)| x != y)
            .expect("author_sig diff");
        assert_eq!(first, SEQ_DOMAIN_TAG.len() + 16 + 8);
    }

    #[test]
    fn sign_then_verify_seq_round_trip() {
        let key = key_a();
        let s = sample_session_id();
        let author = [0x33u8; 64];
        let host_sig = sign_seq(&key, s, Seq::new(42), &author);
        verify_seq(&host_pubkey(&key), s, Seq::new(42), &author, &host_sig).unwrap();
    }

    #[test]
    fn verify_seq_rejects_wrong_host_key() {
        let signer = key_a();
        let s = sample_session_id();
        let author = [0x33u8; 64];
        let host_sig = sign_seq(&signer, s, Seq::new(42), &author);
        // Verify under a different host pubkey → BadSig.
        assert_eq!(
            verify_seq(&host_pubkey(&key_b()), s, Seq::new(42), &author, &host_sig).unwrap_err(),
            VerifyError::BadSig,
        );
    }

    #[test]
    fn verify_seq_rejects_seq_change() {
        // Finding #1: a captured (seq, author_sig, host_sig) tuple
        // replayed under a different seq fails the host seq-sig.
        let key = key_a();
        let s = sample_session_id();
        let author = [0x33u8; 64];
        let host_sig = sign_seq(&key, s, Seq::new(42), &author);
        verify_seq(&host_pubkey(&key), s, Seq::new(42), &author, &host_sig).unwrap();
        assert_eq!(
            verify_seq(&host_pubkey(&key), s, Seq::new(43), &author, &host_sig).unwrap_err(),
            VerifyError::BadSig,
        );
    }

    #[test]
    fn verify_seq_rejects_author_sig_change() {
        let key = key_a();
        let s = sample_session_id();
        let author = [0x33u8; 64];
        let host_sig = sign_seq(&key, s, Seq::new(42), &author);
        let mut author2 = author;
        author2[10] ^= 0x01;
        assert_eq!(
            verify_seq(&host_pubkey(&key), s, Seq::new(42), &author2, &host_sig).unwrap_err(),
            VerifyError::BadSig,
        );
    }

    #[test]
    fn verify_seq_rejects_sentinel() {
        let key = key_a();
        let s = sample_session_id();
        assert_eq!(
            verify_seq(
                &host_pubkey(&key),
                s,
                Seq::new(1),
                &[0x33; 64],
                &SIGNATURE_UNSIGNED
            )
            .unwrap_err(),
            VerifyError::SentinelUnsigned,
        );
    }

    // ---- ack canonical bytes / round-trip ----

    fn sample_ack_message() -> SessionMessage {
        SessionMessage::new(
            Seq::new(3),
            42,
            peer_for(&key_a()),
            MessageKind::Chat,
            "chat.message",
            b"hi".to_vec(),
            [0x11; 64],
            [0x22; 64],
        )
    }

    #[test]
    fn ack_canonical_bytes_field_offsets() {
        let req = Uuid::from_u128(0x1234);
        let result = Ok(sample_ack_message());
        let base = ack_canonical_bytes(SessionId::from_bytes([0x01; 16]), req, &result);

        let diff_sid = ack_canonical_bytes(SessionId::from_bytes([0x02; 16]), req, &result);
        let first = base
            .iter()
            .zip(diff_sid.iter())
            .position(|(x, y)| x != y)
            .expect("session_id diff");
        assert_eq!(first, ACK_DOMAIN_TAG.len());

        let diff_req = ack_canonical_bytes(
            SessionId::from_bytes([0x01; 16]),
            Uuid::from_u128(0x1235),
            &result,
        );
        let first = base
            .iter()
            .zip(diff_req.iter())
            .position(|(x, y)| x != y)
            .expect("req_id diff");
        // req_id is a 16-byte big-endian-ish UUID after the 16-byte sid.
        assert!(first >= ACK_DOMAIN_TAG.len() + 16);
        assert!(first < ACK_DOMAIN_TAG.len() + 16 + 16);
    }

    #[test]
    fn sign_then_verify_ack_round_trip_ok_and_err() {
        let key = key_a();
        let s = sample_session_id();
        let req = Uuid::from_u128(0xabcd);
        for result in [
            Ok(sample_ack_message()),
            Err(ProtocolError::Internal("closed".into())),
        ] {
            let host_sig = sign_ack(&key, s, req, &result);
            verify_ack(&host_pubkey(&key), s, req, &result, &host_sig).unwrap();
        }
    }

    #[test]
    fn verify_ack_rejects_result_flip() {
        // Sign over Ok(msg); verifying against an Err-shaped result must
        // fail, and vice-versa. Pins the result binding (cross-check the
        // "ack signs a message carrying its own host_sig" circularity).
        let key = key_a();
        let s = sample_session_id();
        let req = Uuid::from_u128(0xabcd);
        let ok = Ok(sample_ack_message());
        let err = Err(ProtocolError::Internal("closed".into()));

        let sig_over_ok = sign_ack(&key, s, req, &ok);
        assert_eq!(
            verify_ack(&host_pubkey(&key), s, req, &err, &sig_over_ok).unwrap_err(),
            VerifyError::BadSig,
        );

        let sig_over_err = sign_ack(&key, s, req, &err);
        assert_eq!(
            verify_ack(&host_pubkey(&key), s, req, &ok, &sig_over_err).unwrap_err(),
            VerifyError::BadSig,
        );
    }

    #[test]
    fn verify_ack_rejects_wrong_host_key() {
        let key = key_a();
        let s = sample_session_id();
        let req = Uuid::from_u128(0x1);
        let result = Ok(sample_ack_message());
        let host_sig = sign_ack(&key, s, req, &result);
        assert_eq!(
            verify_ack(&host_pubkey(&key_b()), s, req, &result, &host_sig).unwrap_err(),
            VerifyError::BadSig,
        );
    }

    // ---- ctrl canonical bytes / round-trip ----

    #[test]
    fn ctrl_canonical_bytes_field_offsets() {
        let base = ctrl_canonical_bytes(SessionId::from_bytes([0x01; 16]), 7);
        let diff_sid = ctrl_canonical_bytes(SessionId::from_bytes([0x02; 16]), 7);
        let first = base
            .iter()
            .zip(diff_sid.iter())
            .position(|(x, y)| x != y)
            .expect("session_id diff");
        assert_eq!(first, CTRL_DOMAIN_TAG.len());

        let diff_epoch = ctrl_canonical_bytes(SessionId::from_bytes([0x01; 16]), 8);
        let first = base
            .iter()
            .zip(diff_epoch.iter())
            .position(|(x, y)| x != y)
            .expect("epoch diff");
        // be-encoded u64 epoch differs in its last byte: tag + 16 + 7.
        assert_eq!(first, CTRL_DOMAIN_TAG.len() + 16 + 7);
    }

    #[test]
    fn sign_then_verify_ctrl_round_trip() {
        let key = key_a();
        let s = sample_session_id();
        let host_sig = sign_ctrl(&key, s, 5);
        verify_ctrl(&host_pubkey(&key), s, 5, &host_sig).unwrap();
    }

    #[test]
    fn verify_ctrl_rejects_epoch_change() {
        let key = key_a();
        let s = sample_session_id();
        let host_sig = sign_ctrl(&key, s, 5);
        verify_ctrl(&host_pubkey(&key), s, 5, &host_sig).unwrap();
        assert_eq!(
            verify_ctrl(&host_pubkey(&key), s, 6, &host_sig).unwrap_err(),
            VerifyError::BadSig,
        );
    }

    #[test]
    fn verify_ctrl_rejects_wrong_host_key() {
        let key = key_a();
        let s = sample_session_id();
        let host_sig = sign_ctrl(&key, s, 5);
        assert_eq!(
            verify_ctrl(&host_pubkey(&key_b()), s, 5, &host_sig).unwrap_err(),
            VerifyError::BadSig,
        );
    }

    #[test]
    fn verify_ctrl_shared_by_beacon_and_close() {
        // The defining property of the shared CTRL canonical bytes: a
        // host_sig produced for an EpochBeacon verifies a SessionClosed
        // at the same epoch and vice-versa — they sign identical bytes.
        let key = key_a();
        let s = sample_session_id();
        // "beacon" and "close" are just two call sites; the sig is over
        // (session_id, host_epoch) regardless of frame.
        let beacon_sig = sign_ctrl(&key, s, 9);
        // Verify it as if it rode on a SessionClosed at epoch 9.
        verify_ctrl(&host_pubkey(&key), s, 9, &beacon_sig).unwrap();
        let close_sig = sign_ctrl(&key, s, 9);
        verify_ctrl(&host_pubkey(&key), s, 9, &close_sig).unwrap();
        assert_eq!(beacon_sig, close_sig, "deterministic over identical bytes");
    }

    #[test]
    fn verify_ctrl_rejects_sentinel() {
        let key = key_a();
        let s = sample_session_id();
        assert_eq!(
            verify_ctrl(&host_pubkey(&key), s, 1, &SIGNATURE_UNSIGNED).unwrap_err(),
            VerifyError::SentinelUnsigned,
        );
    }

    // ====================================================================
    // Ticket-cap signatures (Tiered Tickets)
    // ====================================================================

    use crate::capability::Capability;
    use crate::ids::TicketId;

    #[test]
    fn ticket_cap_domain_tag_is_versioned() {
        assert_eq!(TICKET_CAP_DOMAIN_TAG, b"artel/ticket-cap-v1");
    }

    #[test]
    fn cap_byte_values_are_pinned() {
        assert_eq!(cap_byte(Capability::Read), 0);
        assert_eq!(cap_byte(Capability::ReadWrite), 1);
    }

    #[test]
    fn ticket_cap_canonical_bytes_field_offsets() {
        let tid = TicketId::from_bytes([0x01; 16]);
        let sid = SessionId::from_bytes([0x01; 16]);
        let base = ticket_cap_canonical_bytes(tid, sid, Capability::Read, 1000);

        // ticket_id starts after the domain tag
        let diff_tid = ticket_cap_canonical_bytes(
            TicketId::from_bytes([0x02; 16]),
            sid,
            Capability::Read,
            1000,
        );
        let first = base
            .iter()
            .zip(diff_tid.iter())
            .position(|(x, y)| x != y)
            .expect("ticket_id diff");
        assert_eq!(first, TICKET_CAP_DOMAIN_TAG.len());

        // session_id starts after ticket_id
        let diff_sid = ticket_cap_canonical_bytes(
            tid,
            SessionId::from_bytes([0x02; 16]),
            Capability::Read,
            1000,
        );
        let first = base
            .iter()
            .zip(diff_sid.iter())
            .position(|(x, y)| x != y)
            .expect("session_id diff");
        assert_eq!(first, TICKET_CAP_DOMAIN_TAG.len() + 16);

        // cap_byte is after session_id
        let diff_cap = ticket_cap_canonical_bytes(tid, sid, Capability::ReadWrite, 1000);
        let first = base
            .iter()
            .zip(diff_cap.iter())
            .position(|(x, y)| x != y)
            .expect("cap diff");
        assert_eq!(first, TICKET_CAP_DOMAIN_TAG.len() + 16 + 16);

        // expiry_ms is after cap_byte (last byte differs for 1000 vs 1001)
        let diff_expiry = ticket_cap_canonical_bytes(tid, sid, Capability::Read, 1001);
        let first = base
            .iter()
            .zip(diff_expiry.iter())
            .position(|(x, y)| x != y)
            .expect("expiry diff");
        assert_eq!(first, TICKET_CAP_DOMAIN_TAG.len() + 16 + 16 + 1 + 7);
    }

    #[test]
    fn ticket_cap_canonical_bytes_total_length() {
        let bytes = ticket_cap_canonical_bytes(
            TicketId::from_bytes([0; 16]),
            SessionId::from_bytes([0; 16]),
            Capability::Read,
            0,
        );
        assert_eq!(bytes.len(), TICKET_CAP_DOMAIN_TAG.len() + 16 + 16 + 1 + 8);
    }

    #[test]
    fn sign_then_verify_ticket_cap_round_trip() {
        let key = key_a();
        let tid = TicketId::from_bytes([0x01; 16]);
        let sid = sample_session_id();
        for cap in [Capability::Read, Capability::ReadWrite] {
            let sig = sign_ticket_cap(&key, tid, sid, cap, 5000);
            verify_ticket_cap(&host_pubkey(&key), tid, sid, cap, 5000, &sig).unwrap();
        }
    }

    #[test]
    fn verify_ticket_cap_rejects_wrong_host_key() {
        let signer = key_a();
        let tid = TicketId::from_bytes([0x01; 16]);
        let sid = sample_session_id();
        let sig = sign_ticket_cap(&signer, tid, sid, Capability::Read, 0);
        assert_eq!(
            verify_ticket_cap(&host_pubkey(&key_b()), tid, sid, Capability::Read, 0, &sig)
                .unwrap_err(),
            VerifyError::BadSig,
        );
    }

    #[test]
    fn verify_ticket_cap_rejects_tampered_ticket_id() {
        let key = key_a();
        let tid = TicketId::from_bytes([0x01; 16]);
        let sid = sample_session_id();
        let sig = sign_ticket_cap(&key, tid, sid, Capability::Read, 0);
        let other_tid = TicketId::from_bytes([0x02; 16]);
        assert_eq!(
            verify_ticket_cap(&host_pubkey(&key), other_tid, sid, Capability::Read, 0, &sig)
                .unwrap_err(),
            VerifyError::BadSig,
        );
    }

    #[test]
    fn verify_ticket_cap_rejects_tampered_session_id() {
        let key = key_a();
        let tid = TicketId::from_bytes([0x01; 16]);
        let sid = sample_session_id();
        let sig = sign_ticket_cap(&key, tid, sid, Capability::ReadWrite, 0);
        let other_sid = SessionId::from_bytes([0x02; 16]);
        assert_eq!(
            verify_ticket_cap(
                &host_pubkey(&key),
                tid,
                other_sid,
                Capability::ReadWrite,
                0,
                &sig
            )
            .unwrap_err(),
            VerifyError::BadSig,
        );
    }

    #[test]
    fn verify_ticket_cap_rejects_tampered_cap() {
        let key = key_a();
        let tid = TicketId::from_bytes([0x01; 16]);
        let sid = sample_session_id();
        let sig = sign_ticket_cap(&key, tid, sid, Capability::Read, 0);
        assert_eq!(
            verify_ticket_cap(
                &host_pubkey(&key),
                tid,
                sid,
                Capability::ReadWrite,
                0,
                &sig
            )
            .unwrap_err(),
            VerifyError::BadSig,
        );
    }

    #[test]
    fn verify_ticket_cap_rejects_tampered_expiry() {
        let key = key_a();
        let tid = TicketId::from_bytes([0x01; 16]);
        let sid = sample_session_id();
        let sig = sign_ticket_cap(&key, tid, sid, Capability::ReadWrite, 1000);
        assert_eq!(
            verify_ticket_cap(
                &host_pubkey(&key),
                tid,
                sid,
                Capability::ReadWrite,
                1001,
                &sig
            )
            .unwrap_err(),
            VerifyError::BadSig,
        );
    }

    #[test]
    fn verify_ticket_cap_rejects_sentinel() {
        let key = key_a();
        let tid = TicketId::from_bytes([0x01; 16]);
        let sid = sample_session_id();
        assert_eq!(
            verify_ticket_cap(
                &host_pubkey(&key),
                tid,
                sid,
                Capability::Read,
                0,
                &SIGNATURE_UNSIGNED
            )
            .unwrap_err(),
            VerifyError::SentinelUnsigned,
        );
    }
}
