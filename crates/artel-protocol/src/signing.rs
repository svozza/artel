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

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

use crate::ids::SessionId;
use crate::message::{
    MessageFormat, MessageKind, PeerInfo, SIGNATURE_UNSIGNED, SessionMessage, SigBytes,
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
        // 3 reserved for `MessageKind::Capability` (Slice C).
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
/// - [`VerifyError::BadKey`] if `peer.id` is not a valid ed25519
///   public key (i.e. doesn't decode to a curve point).
/// - [`VerifyError::BadSig`] if the signature does not verify under
///   `peer.id`.
pub fn verify_message(
    session_id: SessionId,
    message: &SessionMessage,
    signature: &SigBytes,
) -> Result<(), VerifyError> {
    if signature == &SIGNATURE_UNSIGNED {
        return Err(VerifyError::SentinelUnsigned);
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
    verifying
        .verify(&bytes, &sig)
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
    /// `peer.id` does not decode as an ed25519 public key. Drop and
    /// do not append.
    #[error("peer id is not a valid ed25519 public key")]
    BadKey,
    /// Signature does not verify against `peer.id` over the canonical
    /// bytes.
    #[error("signature does not verify against peer id")]
    BadSig,
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
}
