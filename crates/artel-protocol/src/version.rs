//! Protocol version negotiation.
//!
//! The client sends its [`ProtocolVersion`] on connect; the daemon either
//! accepts or replies with [`VersionMismatch`]. There is no fallback to a
//! partial protocol — clients are expected to surface the error and prompt
//! the user to upgrade or restart the daemon.

use std::fmt;

use serde::{Deserialize, Serialize};

/// The protocol version this build of `artel-protocol` speaks.
///
/// Bump on any wire-incompatible change. Additive changes that are
/// backwards-compatible at the serde level (e.g. new optional fields) do not
/// require a bump.
pub const PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::new(12);

/// A monotonically-increasing protocol version.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct ProtocolVersion(u32);

impl ProtocolVersion {
    /// Construct a version from a raw `u32`.
    #[must_use]
    pub const fn new(v: u32) -> Self {
        Self(v)
    }

    /// The raw `u32` value.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

impl From<u32> for ProtocolVersion {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl From<ProtocolVersion> for u32 {
    fn from(v: ProtocolVersion) -> Self {
        v.0
    }
}

/// The daemon refused the client's reported [`ProtocolVersion`].
///
/// The client should surface this to the user as "restart required". v1 does
/// not attempt N-1 compatibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("unsupported protocol version: client={client}, daemon={daemon}, restart required")]
pub struct VersionMismatch {
    /// Version the client reported on connect.
    pub client: ProtocolVersion,
    /// Version the daemon speaks.
    pub daemon: ProtocolVersion,
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn current_protocol_version_is_twelve() {
        assert_eq!(PROTOCOL_VERSION, ProtocolVersion::new(12));
        assert_eq!(PROTOCOL_VERSION.get(), 12);
    }

    #[test]
    fn display_is_v_prefixed() {
        assert_eq!(ProtocolVersion::new(0).to_string(), "v0");
        assert_eq!(ProtocolVersion::new(42).to_string(), "v42");
        assert_eq!(
            ProtocolVersion::new(u32::MAX).to_string(),
            format!("v{}", u32::MAX)
        );
    }

    #[test]
    fn ord_matches_underlying_u32() {
        assert!(ProtocolVersion::new(0) < ProtocolVersion::new(1));
        assert!(ProtocolVersion::new(1) < ProtocolVersion::new(u32::MAX));
        assert_eq!(ProtocolVersion::new(7), ProtocolVersion::new(7));
    }

    #[test]
    fn default_is_zero() {
        assert_eq!(ProtocolVersion::default(), ProtocolVersion::new(0));
    }

    #[test]
    fn version_mismatch_display_includes_both_versions() {
        let err = VersionMismatch {
            client: ProtocolVersion::new(2),
            daemon: ProtocolVersion::new(1),
        };
        let msg = err.to_string();
        assert!(msg.contains("client=v2"), "msg: {msg}");
        assert!(msg.contains("daemon=v1"), "msg: {msg}");
    }

    #[test]
    fn json_is_transparent() {
        let v = ProtocolVersion::new(7);
        let s = serde_json::to_string(&v).unwrap();
        assert_eq!(s, "7");
        let back: ProtocolVersion = serde_json::from_str(&s).unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn postcard_round_trip_short() {
        let v = ProtocolVersion::new(7);
        let bytes = postcard::to_allocvec(&v).unwrap();
        let back: ProtocolVersion = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, v);
    }

    proptest! {
        #[test]
        fn json_round_trip(v in any::<u32>()) {
            let original = ProtocolVersion::new(v);
            let s = serde_json::to_string(&original).unwrap();
            let back: ProtocolVersion = serde_json::from_str(&s).unwrap();
            prop_assert_eq!(original, back);
        }

        #[test]
        fn postcard_round_trip(v in any::<u32>()) {
            let original = ProtocolVersion::new(v);
            let bytes = postcard::to_allocvec(&original).unwrap();
            let back: ProtocolVersion = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(original, back);
        }

        #[test]
        fn ord_consistent_with_inner(a in any::<u32>(), b in any::<u32>()) {
            let pa = ProtocolVersion::new(a);
            let pb = ProtocolVersion::new(b);
            prop_assert_eq!(pa.cmp(&pb), a.cmp(&b));
        }

        #[test]
        fn from_into_u32_round_trip(v in any::<u32>()) {
            let pv: ProtocolVersion = v.into();
            let back: u32 = pv.into();
            prop_assert_eq!(back, v);
        }

        #[test]
        fn version_mismatch_round_trip(c in any::<u32>(), d in any::<u32>()) {
            let original = VersionMismatch {
                client: ProtocolVersion::new(c),
                daemon: ProtocolVersion::new(d),
            };
            let bytes = postcard::to_allocvec(&original).unwrap();
            let back: VersionMismatch = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(original, back);
        }
    }
}
