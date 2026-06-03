//! Wire frames for inter-daemon gossip traffic.
//!
//! When two daemons share a session via iroh-gossip, every payload
//! they exchange on the topic is a postcard-encoded [`GossipBody`].
//! Lives in `artel-protocol` so both sides agree on the bytes
//! without pulling iroh into the protocol crate.
//!
//! ## Frame model
//!
//! All inter-daemon traffic — both directions — rides the same
//! gossip topic. The protocol stays symmetric so we can drop the
//! host-as-sequencer model later without ripping out a transport.
//!
//! - **Host → all.** Each freshly-sequenced [`SessionMessage`] is
//!   wrapped in [`GossipBody::Message`] and broadcast. Joiners
//!   decode and forward to their local IPC subscribers; the
//!   originating joiner uses it as the data path for its own
//!   outbound send (see below).
//! - **Joiner → host.** A joiner-side `Send` IPC call publishes a
//!   [`GossipBody::SendRequest`] carrying a `req_id`, the
//!   originating peer info, and the application payload. The host's
//!   bridge accepts the request, drives `Registry::send` locally
//!   (which in turn produces a [`GossipBody::Message`] broadcast),
//!   and replies on the same topic with [`GossipBody::SendAck`] —
//!   carrying the assigned [`SessionMessage`] on success or the
//!   host's [`ProtocolError`] on rejection. The joiner correlates
//!   the ack to its in-flight oneshot via `req_id`.
//! - **Joiner → host (membership).** Once the gossip mesh is up, a
//!   joiner broadcasts a [`GossipBody::JoinAnnouncement`] so the
//!   host can admit them to membership and emit `PeerJoined`
//!   eagerly, rather than waiting for their first `SendRequest`.
//!
//! ## No wire versioning yet
//!
//! Pre-1.0, both daemons rebuild together; we have zero on-the-wire
//! compatibility surface to defend. Adding a wire-version envelope
//! later is ~30 lines (struct + decode check + error variant) and
//! by then we'll have a real story for capability negotiation
//! anyway. Until then, an unrecognised frame surfaces as
//! [`GossipFrameError::Malformed`] from postcard and is dropped at
//! the bridge with a warn log.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ProtocolError;
use crate::ids::Seq;
use crate::message::{PeerInfo, SessionMessage, SigBytes};
use crate::rpc::SignedSendPayload;

/// Leading byte stamped on every encoded gossip frame
/// (`[version: u8][postcard(GossipBody)]`).
///
/// A hard inter-daemon cutover (Auth Slice B.5, D4): the reshaped frames
/// are wire-incompatible with the pre-B.5 mesh, so a mixed-version mesh
/// fails cleanly at the version byte ([`GossipFrameError::UnsupportedVersion`])
/// instead of mis-decoding postcard bytes into the wrong variant.
pub const GOSSIP_WIRE_VERSION: u8 = 1;

/// One frame on a session's gossip topic. Externally tagged so
/// postcard can serialise it (see workspace memo on postcard +
/// `tag`/`content`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GossipBody {
    /// Authoritative session message published by the host.
    /// Carries the full [`SessionMessage`] including its assigned
    /// `seq` so receivers can deduplicate and order.
    Message(SessionMessage),

    /// Joiner-published request asking the host to append a
    /// message on its behalf. Correlated with a matching
    /// [`GossipBody::SendAck`] via `req_id`.
    ///
    /// The `payload` is a [`SignedSendPayload`] — the joiner's
    /// daemon stamped `timestamp_ms` and signed the canonical
    /// bytes before publishing (Slice B2; in B1 the signature
    /// is `SIGNATURE_UNSIGNED` and verification is off). The
    /// host preserves both verbatim into the broadcast
    /// [`SessionMessage`] so receivers can verify against the
    /// joiner's `peer.id` without a second sign-and-verify pass.
    SendRequest {
        /// Joiner-assigned correlation id. Round-tripped verbatim
        /// in the matching [`GossipBody::SendAck`].
        req_id: Uuid,
        /// The originating peer (the joiner client's identity).
        /// The daemon enforces that `peer.id` matches the
        /// gossip-authenticated `delivered_from` of the carrying
        /// frame; mismatched frames are dropped at the bridge.
        peer: PeerInfo,
        /// What the joiner asked to append, plus the joiner's
        /// timestamp + signature.
        payload: SignedSendPayload,
    },

    /// Host-published reply to a [`GossipBody::SendRequest`].
    /// Carries the freshly-sequenced [`SessionMessage`] on success
    /// or the host's [`ProtocolError`] on rejection.
    SendAck {
        /// Correlation id copied from the originating request.
        req_id: Uuid,
        /// The host's verdict. `Ok` carries the same
        /// [`SessionMessage`] that's also broadcast as a parallel
        /// [`GossipBody::Message`]; `Err` is the host's wire
        /// rejection so the joiner sees the real reason rather
        /// than a generic timeout.
        result: Result<SessionMessage, ProtocolError>,
        /// Host signature over `crate::signing::ack_canonical_bytes`
        /// of (`session_id`, `req_id`, `result`) — `"artel/ack-v1"`.
        /// Binds the verdict so a racing peer can't forge an ack or
        /// flip a signed `Ok` into `Err` (Auth Slice B.5, D2, finding
        /// #3). The joiner verifies against the host pubkey from the
        /// ticket before resolving its in-flight oneshot. No epoch:
        /// `req_id` v4 freshness self-limits a replayed genuine ack.
        #[serde(with = "crate::message::signature_serde")]
        host_sig: SigBytes,
    },

    /// Joiner-published announcement that they have subscribed to
    /// this session's topic. The host admits the peer to the
    /// session's membership on receipt (idempotent, so a duplicate
    /// announcement is harmless). Lets the host emit `PeerJoined`
    /// proactively rather than lazily on the joiner's first
    /// `SendRequest`.
    JoinAnnouncement {
        /// The joining peer's identity. The daemon enforces that
        /// `peer.id` matches the gossip-authenticated
        /// `delivered_from` of the carrying frame; mismatched
        /// frames are dropped at the bridge.
        peer: PeerInfo,
        /// Joiner-local wall-clock millis at the moment they
        /// finished subscribing. Carried so the host's
        /// `PeerJoined` event has a meaningful timestamp; not
        /// otherwise interpreted.
        timestamp_ms: u64,
    },

    /// Host-published broadcast that the session has been closed.
    /// Sent on the way out of `Registry::leave` (host-closes path)
    /// so joiners learn the truth proactively instead of
    /// discovering it via a `SendRequest` that never gets acked.
    /// On receipt, joiners verify the host signature and the epoch
    /// against their beacon-advanced watermark; only then do they
    /// drop their local mirror and emit `Event::SessionClosed` to
    /// their IPC subscribers (Auth Slice B.5, D3, finding #2).
    SessionClosed {
        /// Host incarnation counter the close was signed at. A close
        /// captured from incarnation N is rejected against a same-id
        /// resume at N+1 once the joiner's watermark has advanced via
        /// an [`GossipBody::EpochBeacon`]. The iroh endpoint secret is
        /// stable across restart, so this epoch — not the host key — is
        /// the freshness element.
        host_epoch: u64,
        /// Host signature over `crate::signing::ctrl_canonical_bytes`
        /// of (`session_id`, `host_epoch`) — `"artel/ctrl-v1"`. Shares
        /// canonical bytes with [`GossipBody::EpochBeacon`], so one
        /// verifier (`verify_ctrl`) serves both.
        #[serde(with = "crate::message::signature_serde")]
        host_sig: SigBytes,
    },

    /// Host-published broadcast of the current host incarnation epoch.
    ///
    /// Sent best-effort on every host resume (`bridge.host_session`),
    /// so already-joined joiners learn the new epoch immediately —
    /// independent of session activity. **The only frame that advances
    /// the joiner's `host_epoch` watermark**, and it advances only on a
    /// host-*signed* value, so an attacker cannot forge a high epoch and
    /// a replayed old beacon cannot lower a monotonic watermark (Auth
    /// Slice B.5, D3). A genuine `Message` replayed on an unseen seq
    /// must **not** move the watermark — that is the
    /// `replayed_message_cannot_poison_watermark` invariant.
    EpochBeacon {
        /// The host's current incarnation counter.
        host_epoch: u64,
        /// Host signature over `crate::signing::ctrl_canonical_bytes`
        /// of (`session_id`, `host_epoch`) — `"artel/ctrl-v1"`, the
        /// **same** canonical bytes as [`GossipBody::SessionClosed`].
        #[serde(with = "crate::message::signature_serde")]
        host_sig: SigBytes,
    },

    /// Joiner-published request asking the host to replay every
    /// committed message with `seq > since`. The host's response
    /// is plain [`GossipBody::Message`] frames re-broadcast on the
    /// same topic; the joiner's mirror dedups by seq, so a Message
    /// that arrives twice (once live, once via replay) is harmless.
    ///
    /// Carries no correlation id: replay is fire-and-forget, and
    /// every `Message` already carries its own seq for ordering.
    /// Other joiners on the topic see the replay traffic too and
    /// dedup-skip it — wasteful but not incorrect; can be tightened
    /// later (e.g. unicast over a dedicated stream) if it matters.
    Replay {
        /// Highest seq the joiner has already seen. The host
        /// re-broadcasts every committed message with `seq > since`.
        /// Use [`Seq::ZERO`] to ask for the full log.
        since: Seq,
    },
}

/// Errors [`decode`] may return.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GossipFrameError {
    /// Bytes did not deserialize as a [`GossipBody`].
    #[error("malformed gossip frame: {0}")]
    Malformed(String),
    /// The frame's leading version byte is not one this build speaks.
    /// A mixed-version mesh fails here cleanly instead of mis-decoding.
    #[error("unsupported gossip wire version: found {found}, expected {expected}")]
    UnsupportedVersion {
        /// The leading byte found on the frame.
        found: u8,
        /// The version this build emits / accepts ([`GOSSIP_WIRE_VERSION`]).
        expected: u8,
    },
    /// The frame was empty — no leading version byte at all.
    #[error("empty gossip frame: missing version byte")]
    Empty,
}

/// Encode `body` to the bytes broadcast on the gossip topic.
///
/// Wire form is `[version: u8][postcard(body)]` — the leading
/// [`GOSSIP_WIRE_VERSION`] byte lets [`decode`] reject a frame from an
/// incompatible mesh before postcard touches it.
#[must_use]
pub fn encode(body: &GossipBody) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(GOSSIP_WIRE_VERSION);
    out.extend_from_slice(&postcard::to_stdvec(body).expect("postcard encode of fixed-shape types"));
    out
}

/// Decode `bytes` into a [`GossipBody`].
///
/// Rejects an empty frame ([`GossipFrameError::Empty`]) or an unknown
/// leading version byte ([`GossipFrameError::UnsupportedVersion`]) before
/// attempting a postcard decode of the remainder.
pub fn decode(bytes: &[u8]) -> Result<GossipBody, GossipFrameError> {
    let (&version, rest) = bytes.split_first().ok_or(GossipFrameError::Empty)?;
    if version != GOSSIP_WIRE_VERSION {
        return Err(GossipFrameError::UnsupportedVersion {
            found: version,
            expected: GOSSIP_WIRE_VERSION,
        });
    }
    postcard::from_bytes(rest).map_err(|e| GossipFrameError::Malformed(e.to_string()))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::ids::{PeerId, Seq};
    use crate::message::{MessageKind, PeerInfo};

    fn sample_msg() -> SessionMessage {
        SessionMessage::new(
            Seq::new(7),
            12_345,
            PeerInfo::new(PeerId::from_bytes([2; 32]), "alice"),
            MessageKind::Chat,
            "chat.message",
            b"hi".to_vec(),
            crate::message::SIGNATURE_UNSIGNED,
            crate::message::SIGNATURE_UNSIGNED,
        )
    }

    fn sample_payload() -> SignedSendPayload {
        SignedSendPayload {
            timestamp_ms: 1_700_000_000_000,
            kind: MessageKind::Chat,
            action: "chat.message".into(),
            payload: b"hi from joiner".to_vec(),
            signature: crate::message::SIGNATURE_UNSIGNED,
        }
    }

    #[test]
    fn message_frame_round_trips() {
        let body = GossipBody::Message(sample_msg());
        let bytes = encode(&body);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn send_request_frame_round_trips() {
        let body = GossipBody::SendRequest {
            req_id: Uuid::from_u128(0x4242_4242_4242_4242_4242_4242_4242_4242),
            peer: PeerInfo::new(PeerId::from_bytes([3; 32]), "bob"),
            payload: sample_payload(),
        };
        let bytes = encode(&body);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn send_ack_ok_frame_round_trips() {
        let body = GossipBody::SendAck {
            req_id: Uuid::from_u128(0x1),
            result: Ok(sample_msg()),
            host_sig: [0x44; 64],
        };
        let bytes = encode(&body);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn send_ack_err_frame_round_trips() {
        let body = GossipBody::SendAck {
            req_id: Uuid::from_u128(0x2),
            result: Err(ProtocolError::Internal("session closed".into())),
            host_sig: [0x55; 64],
        };
        let bytes = encode(&body);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn session_closed_frame_round_trips() {
        let body = GossipBody::SessionClosed {
            host_epoch: 7,
            host_sig: [0x66; 64],
        };
        let bytes = encode(&body);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn epoch_beacon_frame_round_trips() {
        let body = GossipBody::EpochBeacon {
            host_epoch: 42,
            host_sig: [0x77; 64],
        };
        let bytes = encode(&body);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn replay_frame_round_trips() {
        let body = GossipBody::Replay {
            since: Seq::new(42),
        };
        let bytes = encode(&body);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn join_announcement_frame_round_trips() {
        let body = GossipBody::JoinAnnouncement {
            peer: PeerInfo::new(PeerId::from_bytes([4; 32]), "carol"),
            timestamp_ms: 1_700_000_000_000,
        };
        let bytes = encode(&body);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn truncated_bytes_error_as_malformed() {
        let bytes = encode(&GossipBody::Message(sample_msg()));
        let truncated = &bytes[..bytes.len() / 2];
        match decode(truncated) {
            Err(GossipFrameError::Malformed(_)) => {}
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn gossip_frame_has_version_byte() {
        let bytes = encode(&GossipBody::SessionClosed {
            host_epoch: 0,
            host_sig: crate::message::SIGNATURE_UNSIGNED,
        });
        assert_eq!(bytes[0], GOSSIP_WIRE_VERSION);
        // The remainder is the bare postcard body (no version byte).
        let body = GossipBody::SessionClosed {
            host_epoch: 0,
            host_sig: crate::message::SIGNATURE_UNSIGNED,
        };
        assert_eq!(&bytes[1..], postcard::to_stdvec(&body).unwrap().as_slice());
    }

    #[test]
    fn decode_rejects_unknown_version() {
        let mut bytes = encode(&GossipBody::Message(sample_msg()));
        bytes[0] = GOSSIP_WIRE_VERSION.wrapping_add(1);
        match decode(&bytes) {
            Err(GossipFrameError::UnsupportedVersion { found, expected }) => {
                assert_eq!(found, GOSSIP_WIRE_VERSION.wrapping_add(1));
                assert_eq!(expected, GOSSIP_WIRE_VERSION);
            }
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_empty() {
        match decode(&[]) {
            Err(GossipFrameError::Empty) => {}
            other => panic!("expected Empty, got {other:?}"),
        }
    }
}
