//! Maps workspace `EndpointId`s to daemon `PeerId`s and maintains a
//! local cap-set projection for the docs gate.

// Crate-private module: pair `unreachable_pub` with the
// crate-visibility lint so they stop fighting (see memory).
#![allow(clippy::redundant_pub_crate)]

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use artel_protocol::PeerId;
use artel_protocol::capability::{Capability, CapabilityAction};
use iroh::EndpointId;

/// Verdict for a doc-entry author at namespace-rotation time (D2). Only
/// [`Self::Rw`] carries forward; the rest are dropped, but the caller
/// distinguishes the *expected* drop ([`Self::Revoked`]) from the
/// *alarming* one ([`Self::Unresolvable`], a survivor whose mapping
/// hadn't landed yet) for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthorDisposition {
    /// Author resolves to a currently-`ReadWrite` peer — keep its entry.
    Rw,
    /// Author resolves to a peer explicitly revoked by the host — drop
    /// (the intended effect of an Evict).
    Revoked,
    /// Author resolves to a known peer that is neither RW nor revoked
    /// (a Read peer, or one dropped to the Read floor) — drop, unalarming.
    NotRw,
    /// Author's `EndpointId` has no `PeerId` mapping yet — drop
    /// fail-closed, but this is a liveness race that silently loses a
    /// (possibly trusted) peer's data, so the caller logs it loudly.
    Unresolvable,
}

#[derive(Debug)]
pub(crate) struct PeerMap {
    /// Workspace `EndpointId` → daemon `PeerId`.
    id_map: RwLock<HashMap<EndpointId, PeerId>>,
    /// Daemon `PeerId` → current capability (projected from session log).
    caps: RwLock<HashMap<PeerId, Capability>>,
    /// Peers that were explicitly revoked (removed from caps). A peer
    /// in this set is blocked unconditionally, even if their endpoint
    /// mapping is still registered. Distinguishes "never granted yet"
    /// and "Read-only" from "was a member, then kicked".
    revoked: RwLock<HashSet<PeerId>>,
    /// The host's daemon `PeerId` (cap-log root, always RW).
    host: PeerId,
}

impl PeerMap {
    pub(crate) fn new(host: PeerId) -> Self {
        let mut caps = HashMap::new();
        caps.insert(host, Capability::ReadWrite);
        Self {
            id_map: RwLock::new(HashMap::new()),
            caps: RwLock::new(caps),
            revoked: RwLock::new(HashSet::new()),
            host,
        }
    }

    /// Register a workspace `EndpointId` → daemon `PeerId` link.
    /// Returns `true` if the peer is currently revoked (the mapping
    /// arrived after the revocation — the peer may already have an
    /// active connection that predates the mapping).
    pub(crate) fn register(&self, workspace_id: EndpointId, daemon_peer: PeerId) -> bool {
        let is_revoked = self.revoked.read().unwrap().contains(&daemon_peer);
        if is_revoked {
            tracing::warn!(
                target: "artel_fs::peer_map",
                %workspace_id, %daemon_peer,
                "register: mapping arrived for already-revoked peer",
            );
        } else {
            tracing::debug!(
                target: "artel_fs::peer_map",
                %workspace_id, %daemon_peer,
                "register",
            );
        }
        self.id_map
            .write()
            .unwrap()
            .insert(workspace_id, daemon_peer);
        is_revoked
    }

    /// Apply a Capability message to the local projection.
    /// Only host-authored messages mutate (matches daemon-side rule).
    pub(crate) fn apply_capability(&self, author: PeerId, payload: &[u8]) {
        if author != self.host {
            return;
        }
        let Ok(action) = CapabilityAction::decode(payload) else {
            return;
        };
        tracing::debug!(
            target: "artel_fs::peer_map",
            ?action,
            "apply_capability",
        );
        let mut caps = self.caps.write().unwrap();
        let revoke_peer = match action {
            CapabilityAction::Grant { peer, cap } => {
                caps.insert(peer, cap);
                Some((peer, false))
            }
            CapabilityAction::Revoke { peer } => {
                caps.remove(&peer);
                Some((peer, true))
            }
        };
        drop(caps);
        if let Some((peer, is_revoke)) = revoke_peer {
            let mut revoked = self.revoked.write().unwrap();
            if is_revoke {
                revoked.insert(peer);
            } else {
                revoked.remove(&peer);
            }
        }
    }

    /// The host's daemon `PeerId`.
    pub(crate) const fn host_peer_id(&self) -> PeerId {
        self.host
    }

    /// Whether `peer` currently holds `ReadWrite` in the local
    /// projection. Used to gate upgrade delivery so revoked peers
    /// don't receive re-deliveries on host resume.
    pub(crate) fn has_rw(&self, peer: PeerId) -> bool {
        self.caps
            .read()
            .unwrap()
            .get(&peer)
            .is_some_and(|c| *c == Capability::ReadWrite)
    }

    /// All peers currently holding `ReadWrite`, **excluding the host
    /// itself**. The survivor set for namespace-rotation distribution:
    /// the host ships the rotated namespace's ticket to exactly these
    /// peers (never the revoked one, which `apply_capability` has
    /// already removed from `caps`).
    pub(crate) fn rw_peers_except_host(&self) -> Vec<PeerId> {
        let caps = self.caps.read().unwrap();
        caps.iter()
            .filter(|(peer, cap)| **peer != self.host && **cap == Capability::ReadWrite)
            .map(|(peer, _)| *peer)
            .collect()
    }

    /// Classify a doc-entry author's `EndpointId` for the namespace-
    /// rotation filter (D2). Both [`AuthorDisposition::Revoked`] and
    /// [`AuthorDisposition::Unresolvable`] are dropped from the rotated
    /// snapshot (fail-closed), but the caller logs them differently:
    /// a `Revoked` drop is the *intended* effect of an Evict, while an
    /// `Unresolvable` drop is a liveness race (the survivor's
    /// `NODE_ID_ACTION` mapping hadn't landed when rotation fired) that
    /// silently loses a *trusted* peer's data — worth a loud warning.
    pub(crate) fn classify_author(&self, workspace_id: EndpointId) -> AuthorDisposition {
        let id_map = self.id_map.read().unwrap();
        let Some(&daemon_peer) = id_map.get(&workspace_id) else {
            return AuthorDisposition::Unresolvable;
        };
        drop(id_map);
        if self.has_rw(daemon_peer) {
            AuthorDisposition::Rw
        } else if self.revoked.read().unwrap().contains(&daemon_peer) {
            AuthorDisposition::Revoked
        } else {
            // Resolvable but neither RW nor explicitly revoked — a Read
            // peer, or one granted then dropped to the Read floor. Its
            // writes shouldn't carry forward (not RW), but it isn't the
            // evicted adversary either; treat as a non-alarming drop.
            AuthorDisposition::NotRw
        }
    }

    /// If the workspace `EndpointId` belongs to a peer that was
    /// explicitly revoked, return that peer's daemon `PeerId` so the
    /// caller can name it in the block signal it surfaces. Returns
    /// `None` (allow) for:
    /// - Unknown `EndpointId`s (haven't seen the mapping yet)
    /// - Peers not yet granted (race: registered before grant arrives)
    /// - Read-only peers (legitimate tiered-ticket holders)
    ///
    /// Only returns `Some` for peers that once held a capability and
    /// were then explicitly revoked by the host.
    pub(crate) fn revoked_daemon_peer(&self, workspace_id: EndpointId) -> Option<PeerId> {
        let id_map = self.id_map.read().unwrap();
        let &daemon_peer = id_map.get(&workspace_id)?;
        drop(id_map);
        self.revoked
            .read()
            .unwrap()
            .contains(&daemon_peer)
            .then_some(daemon_peer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_host() -> PeerId {
        PeerId::from_bytes([1; 32])
    }

    fn test_peer() -> PeerId {
        PeerId::from_bytes([2; 32])
    }

    fn test_workspace_id() -> EndpointId {
        EndpointId::from_bytes(&[3; 32]).unwrap()
    }

    fn grant_payload(peer: PeerId, cap: Capability) -> Vec<u8> {
        CapabilityAction::Grant { peer, cap }.encode()
    }

    fn revoke_payload(peer: PeerId) -> Vec<u8> {
        CapabilityAction::Revoke { peer }.encode()
    }

    #[test]
    fn unknown_endpoint_id_is_allowed() {
        let map = PeerMap::new(test_host());
        assert!(map.revoked_daemon_peer(test_workspace_id()).is_none());
    }

    #[test]
    fn registered_rw_peer_is_allowed() {
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();

        map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        map.register(wid, peer);

        assert!(map.revoked_daemon_peer(wid).is_none());
    }

    #[test]
    fn registered_revoked_peer_is_rejected() {
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();

        map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        map.register(wid, peer);
        map.apply_capability(host, &revoke_payload(peer));

        assert_eq!(map.revoked_daemon_peer(wid), Some(peer));
    }

    #[test]
    fn grant_then_revoke_transitions() {
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();

        map.register(wid, peer);
        // Before grant: peer absent from caps but never revoked → allowed
        assert!(map.revoked_daemon_peer(wid).is_none());

        map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        assert!(map.revoked_daemon_peer(wid).is_none());

        map.apply_capability(host, &revoke_payload(peer));
        assert_eq!(map.revoked_daemon_peer(wid), Some(peer));
    }

    #[test]
    fn non_host_authored_grant_is_ignored() {
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();
        let impostor = PeerId::from_bytes([99; 32]);

        // Grant from host, then revoke, then impostor tries to re-grant.
        map.register(wid, peer);
        map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        map.apply_capability(host, &revoke_payload(peer));
        assert_eq!(map.revoked_daemon_peer(wid), Some(peer));

        // Impostor grant is ignored — peer stays revoked.
        map.apply_capability(impostor, &grant_payload(peer, Capability::ReadWrite));
        assert_eq!(map.revoked_daemon_peer(wid), Some(peer));
    }

    #[test]
    fn non_host_authored_revoke_is_ignored() {
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();
        let impostor = PeerId::from_bytes([99; 32]);

        map.register(wid, peer);
        map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        map.apply_capability(impostor, &revoke_payload(peer));

        // Still allowed — the non-host revoke was ignored
        assert!(map.revoked_daemon_peer(wid).is_none());
    }

    #[test]
    fn read_only_cap_is_allowed() {
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();

        map.register(wid, peer);
        map.apply_capability(host, &grant_payload(peer, Capability::Read));

        assert!(map.revoked_daemon_peer(wid).is_none());
    }

    #[test]
    fn re_grant_after_revoke_clears_revoked_status() {
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();

        map.register(wid, peer);
        map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        map.apply_capability(host, &revoke_payload(peer));
        assert_eq!(map.revoked_daemon_peer(wid), Some(peer));

        map.apply_capability(host, &grant_payload(peer, Capability::Read));
        assert!(map.revoked_daemon_peer(wid).is_none());
    }

    #[test]
    fn host_always_has_rw() {
        let host = test_host();
        let map = PeerMap::new(host);
        let host_wid = EndpointId::from_bytes(&[10; 32]).unwrap();
        map.register(host_wid, host);

        assert!(map.revoked_daemon_peer(host_wid).is_none());
    }

    #[test]
    fn register_returns_true_for_already_revoked_peer() {
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();

        map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        map.apply_capability(host, &revoke_payload(peer));

        // Mapping arrives after revocation — register signals it.
        assert!(map.register(wid, peer));
    }

    #[test]
    fn register_returns_false_for_non_revoked_peer() {
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();

        map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        assert!(!map.register(wid, peer));
    }

    // ---- classify_author (D2) ----

    #[test]
    fn classify_author_unresolvable_when_no_mapping() {
        let map = PeerMap::new(test_host());
        // Endpoint never registered → no PeerId mapping.
        assert_eq!(
            map.classify_author(test_workspace_id()),
            AuthorDisposition::Unresolvable,
        );
    }

    #[test]
    fn classify_author_rw_for_granted_peer() {
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();
        map.register(wid, peer);
        map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        assert_eq!(map.classify_author(wid), AuthorDisposition::Rw);
    }

    #[test]
    fn classify_author_revoked_after_evict() {
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();
        map.register(wid, peer);
        map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        map.apply_capability(host, &revoke_payload(peer));
        assert_eq!(map.classify_author(wid), AuthorDisposition::Revoked);
    }

    #[test]
    fn classify_author_not_rw_for_read_peer() {
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();
        map.register(wid, peer);
        map.apply_capability(host, &grant_payload(peer, Capability::Read));
        // Resolvable, holds a cap, but not RW and not revoked.
        assert_eq!(map.classify_author(wid), AuthorDisposition::NotRw);
    }

    #[test]
    fn classify_author_distinguishes_revoked_from_unresolvable() {
        // The D2 distinction that matters: a revoked peer (intended drop)
        // must classify differently from an unmapped author (alarming
        // drop — possible survivor data loss), even though the rotation
        // filter drops both.
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();
        map.register(wid, peer);
        map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        map.apply_capability(host, &revoke_payload(peer));

        // A distinct, valid endpoint id that was never registered.
        // (Fill byte 10 is a valid ed25519 point; 7 is not.)
        let unmapped = EndpointId::from_bytes(&[10; 32]).unwrap();
        assert_eq!(map.classify_author(wid), AuthorDisposition::Revoked);
        assert_eq!(
            map.classify_author(unmapped),
            AuthorDisposition::Unresolvable,
        );
        assert_ne!(
            map.classify_author(wid),
            map.classify_author(unmapped),
            "revoked and unresolvable must be distinguishable for D2 diagnostics",
        );
    }
}
