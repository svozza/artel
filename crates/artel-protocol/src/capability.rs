//! Capability vocabulary for the event-sourced authorization model.
//!
//! See `docs/brainstorms/2026-06-03-auth-slice-c-l2-capabilities-seed.md`.
//!
//! A capability change is not host-side mutable state: it is a signed
//! [`crate::SessionMessage`] of [`crate::MessageKind::Capability`] whose
//! payload is a postcard-encoded [`CapabilityAction`]. The cap set is a
//! *projection* derived by replaying these messages in seq order;
//! grant/revoke are *commands*, replay is deterministic. In v1 that
//! projection is host-only: `Capability` messages never leave the host
//! (they ride neither live fanout nor replay), and enforcement happens
//! at the host's `send` chokepoint.
//!
//! Two tiers for v1: [`Capability::Read`] (subscribe + consume) and
//! [`Capability::ReadWrite`] (author messages). Grant/revoke *authority*
//! is host-only in v1: any `ReadWrite` holder can author a `Capability`
//! message, but the projection ignores it unless the host authored it
//! (tightened from the original "any RW holder" rule to stop a malicious
//! joiner revoking peers). A peer absent from the cap set is treated as
//! `Read`-only for
//! write checks — "absent ⇒ Read" is the floor, so a
//! [`CapabilityAction::Revoke`] is just removal from the set.

use serde::{Deserialize, Serialize};

use crate::error::ProtocolError;
use crate::ids::PeerId;

/// `action` string stamped on a [`CapabilityAction::Grant`]'s carrying
/// [`crate::SessionMessage`].
///
/// **Advisory only.** The daemon never dispatches on
/// [`crate::SessionMessage::action`]; the authoritative verb is the
/// [`CapabilityAction`] postcard-encoded into the payload. The string
/// exists for human-readable log views, mirroring how every other kind
/// treats `action`. A message whose `action` disagrees with its payload
/// projects by *payload* — see the plan's "action string vs payload
/// authority" risk note.
pub const ACTION_GRANT: &str = "capability.grant";

/// `action` string stamped on a [`CapabilityAction::Revoke`]'s carrying
/// [`crate::SessionMessage`]. Advisory only — see [`ACTION_GRANT`].
pub const ACTION_REVOKE: &str = "capability.revoke";

/// Capability tier held by a peer in a session.
///
/// Externally-tagged (the serde default; see
/// `feedback_postcard_externally_tagged_enums` — postcard rejects
/// adjacently / internally tagged enums). A plain C-like enum, so its wire
/// form is a single variant tag.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// May subscribe to the session and consume its log, but may not
    /// author non-capability messages.
    Read,
    /// Full access: author messages of any kind, including
    /// [`CapabilityAction`] carriers. Grant/revoke *authority* is
    /// host-only in v1 (a non-host `Capability` message is inert at
    /// projection); there is no separate `Admin` tier.
    ReadWrite,
}

impl Capability {
    /// Whether this tier permits authoring messages. (Grant/revoke
    /// authority is a separate, host-only rule — see the module doc.)
    ///
    /// The single source of the "can write" rule, so the enforcement
    /// sites in the daemon don't each re-encode it.
    #[must_use]
    pub const fn permits_write(self) -> bool {
        matches!(self, Self::ReadWrite)
    }
}

/// The grant / revoke verb carried in a [`crate::MessageKind::Capability`]
/// message's payload.
///
/// Externally-tagged (serde default). Postcard encodes the variant tag
/// followed by the variant's fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityAction {
    /// Grant `peer` the capability `cap`. If `peer` already holds a
    /// capability, this replaces it (an upgrade or downgrade).
    Grant {
        /// The peer being granted a capability.
        peer: PeerId,
        /// The capability granted.
        cap: Capability,
    },
    /// Revoke `peer`'s capability entirely — remove it from the cap set.
    /// A removed peer falls back to the "absent ⇒ Read" floor.
    Revoke {
        /// The peer whose capability is revoked.
        peer: PeerId,
    },
}

impl CapabilityAction {
    /// Postcard-encode this action for a
    /// [`crate::SessionMessage::payload`].
    ///
    /// # Panics
    ///
    /// Never in practice: the action is composed of fixed-size types
    /// (`PeerId`, a C-like enum) that postcard always encodes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("postcard encode of fixed-size CapabilityAction")
    }

    /// Decode a [`CapabilityAction`] from a
    /// [`crate::SessionMessage::payload`].
    ///
    /// # Errors
    ///
    /// [`ProtocolError::Capability`] if `bytes` is not a well-formed
    /// postcard encoding of a `CapabilityAction` (truncated, trailing
    /// garbage, or an unknown variant tag).
    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        postcard::from_bytes(bytes)
            .map_err(|e| ProtocolError::Capability(format!("malformed capability payload: {e}")))
    }

    /// The advisory `action` string for this verb. See [`ACTION_GRANT`].
    #[must_use]
    pub const fn action_str(&self) -> &'static str {
        match self {
            Self::Grant { .. } => ACTION_GRANT,
            Self::Revoke { .. } => ACTION_REVOKE,
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    use super::*;

    fn peer(b: u8) -> PeerId {
        PeerId::from_bytes([b; 32])
    }

    // ---- Capability ----

    #[test]
    fn capability_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&Capability::Read).unwrap(),
            "\"read\""
        );
        assert_eq!(
            serde_json::to_string(&Capability::ReadWrite).unwrap(),
            "\"read_write\""
        );
    }

    #[test]
    fn capability_unknown_variant_rejected() {
        let result: Result<Capability, _> = serde_json::from_str("\"admin\"");
        assert!(result.is_err());
    }

    #[test]
    fn permits_write_truth_table() {
        assert!(!Capability::Read.permits_write());
        assert!(Capability::ReadWrite.permits_write());
    }

    #[test]
    fn capability_postcard_round_trip() {
        for cap in [Capability::Read, Capability::ReadWrite] {
            let bytes = postcard::to_allocvec(&cap).unwrap();
            let back: Capability = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(cap, back);
        }
    }

    // ---- CapabilityAction ----

    #[test]
    fn grant_round_trips_postcard() {
        let action = CapabilityAction::Grant {
            peer: peer(0x11),
            cap: Capability::ReadWrite,
        };
        let bytes = action.encode();
        let back = CapabilityAction::decode(&bytes).unwrap();
        assert_eq!(action, back);
    }

    #[test]
    fn revoke_round_trips_postcard() {
        let action = CapabilityAction::Revoke { peer: peer(0x22) };
        let bytes = action.encode();
        let back = CapabilityAction::decode(&bytes).unwrap();
        assert_eq!(action, back);
    }

    #[test]
    fn grant_and_revoke_encode_distinctly() {
        // The externally-tagged variant tag distinguishes the two verbs
        // on the wire even when they name the same peer.
        let grant = CapabilityAction::Grant {
            peer: peer(0x33),
            cap: Capability::Read,
        };
        let revoke = CapabilityAction::Revoke { peer: peer(0x33) };
        assert_ne!(grant.encode(), revoke.encode());
    }

    #[test]
    fn decode_rejects_garbage() {
        // A truncated / nonsense buffer surfaces ProtocolError::Capability,
        // never a panic.
        match CapabilityAction::decode(&[0xff, 0xff, 0xff]) {
            Err(ProtocolError::Capability(_)) => {}
            other => panic!("expected Capability error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_empty() {
        match CapabilityAction::decode(&[]) {
            Err(ProtocolError::Capability(_)) => {}
            other => panic!("expected Capability error, got {other:?}"),
        }
    }

    #[test]
    fn action_str_matches_variant() {
        assert_eq!(
            CapabilityAction::Grant {
                peer: peer(1),
                cap: Capability::ReadWrite,
            }
            .action_str(),
            ACTION_GRANT,
        );
        assert_eq!(
            CapabilityAction::Revoke { peer: peer(1) }.action_str(),
            ACTION_REVOKE,
        );
        assert_eq!(ACTION_GRANT, "capability.grant");
        assert_eq!(ACTION_REVOKE, "capability.revoke");
    }

    #[test]
    fn action_uses_external_variant_tag_in_json() {
        // Externally-tagged: a struct variant renders as
        // `{ "grant": { ... } }`, never a flattened `{ "type": "grant" }`.
        let json = serde_json::to_string(&CapabilityAction::Revoke { peer: peer(0) }).unwrap();
        assert!(json.starts_with("{\"revoke\":"), "json: {json}");
    }

    fn arb_capability() -> impl Strategy<Value = Capability> {
        prop_oneof![Just(Capability::Read), Just(Capability::ReadWrite)]
    }

    fn arb_action() -> impl Strategy<Value = CapabilityAction> {
        prop_oneof![
            (any::<[u8; 32]>(), arb_capability()).prop_map(|(p, cap)| CapabilityAction::Grant {
                peer: PeerId::from_bytes(p),
                cap,
            }),
            any::<[u8; 32]>().prop_map(|p| CapabilityAction::Revoke {
                peer: PeerId::from_bytes(p),
            }),
        ]
    }

    proptest! {
        #[test]
        fn capability_postcard_round_trip_arb(cap in arb_capability()) {
            let bytes = postcard::to_allocvec(&cap).unwrap();
            let back: Capability = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(cap, back);
        }

        #[test]
        fn action_encode_decode_round_trip(action in arb_action()) {
            let bytes = action.encode();
            let back = CapabilityAction::decode(&bytes).unwrap();
            prop_assert_eq!(action, back);
        }

        #[test]
        fn action_json_round_trip(action in arb_action()) {
            let json = serde_json::to_string(&action).unwrap();
            let back: CapabilityAction = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(action, back);
        }
    }
}
