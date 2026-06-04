//! Maps workspace `EndpointId`s to daemon `PeerId`s and maintains a
//! local cap-set projection for the docs gate.

// Crate-private module: pair `unreachable_pub` with the
// crate-visibility lint so they stop fighting (see memory).
#![allow(clippy::redundant_pub_crate)]

use std::collections::HashMap;
use std::sync::RwLock;

use artel_protocol::PeerId;
use artel_protocol::capability::{Capability, CapabilityAction};
use iroh::EndpointId;

#[derive(Debug)]
pub(crate) struct PeerMap {
    /// Workspace `EndpointId` → daemon `PeerId`.
    id_map: RwLock<HashMap<EndpointId, PeerId>>,
    /// Daemon `PeerId` → current capability (projected from session log).
    caps: RwLock<HashMap<PeerId, Capability>>,
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
            host,
        }
    }

    /// Register a workspace `EndpointId` → daemon `PeerId` link.
    pub(crate) fn register(&self, workspace_id: EndpointId, daemon_peer: PeerId) {
        tracing::debug!(
            target: "artel_fs::peer_map",
            %workspace_id, %daemon_peer,
            "register",
        );
        self.id_map
            .write()
            .unwrap()
            .insert(workspace_id, daemon_peer);
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
        match action {
            CapabilityAction::Grant { peer, cap } => {
                caps.insert(peer, cap);
            }
            CapabilityAction::Revoke { peer } => {
                caps.remove(&peer);
            }
        }
    }

    /// Check whether an incoming workspace `EndpointId` belongs to a
    /// revoked peer. Returns `false` (allow) for unknown `EndpointId`s
    /// — if we haven't seen the mapping yet, we can't revoke them.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn is_revoked_workspace_id(&self, workspace_id: EndpointId) -> bool {
        let id_map = self.id_map.read().unwrap();
        let Some(&daemon_peer) = id_map.get(&workspace_id) else {
            return false;
        };
        drop(id_map);
        let caps = self.caps.read().unwrap();
        !caps.get(&daemon_peer).is_some_and(|c| c.permits_write())
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
        assert!(!map.is_revoked_workspace_id(test_workspace_id()));
    }

    #[test]
    fn registered_rw_peer_is_allowed() {
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();

        map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        map.register(wid, peer);

        assert!(!map.is_revoked_workspace_id(wid));
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

        assert!(map.is_revoked_workspace_id(wid));
    }

    #[test]
    fn grant_then_revoke_transitions() {
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();

        map.register(wid, peer);
        // Before grant: peer absent from caps → revoked
        assert!(map.is_revoked_workspace_id(wid));

        map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        assert!(!map.is_revoked_workspace_id(wid));

        map.apply_capability(host, &revoke_payload(peer));
        assert!(map.is_revoked_workspace_id(wid));
    }

    #[test]
    fn non_host_authored_grant_is_ignored() {
        let map = PeerMap::new(test_host());
        let peer = test_peer();
        let wid = test_workspace_id();
        let impostor = PeerId::from_bytes([99; 32]);

        map.register(wid, peer);
        map.apply_capability(impostor, &grant_payload(peer, Capability::ReadWrite));

        // Still revoked — the non-host grant was ignored
        assert!(map.is_revoked_workspace_id(wid));
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
        assert!(!map.is_revoked_workspace_id(wid));
    }

    #[test]
    fn read_only_cap_is_considered_revoked() {
        let map = PeerMap::new(test_host());
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();

        map.register(wid, peer);
        map.apply_capability(host, &grant_payload(peer, Capability::Read));

        assert!(map.is_revoked_workspace_id(wid));
    }

    #[test]
    fn host_always_has_rw() {
        let host = test_host();
        let map = PeerMap::new(host);
        let host_wid = EndpointId::from_bytes(&[10; 32]).unwrap();
        map.register(host_wid, host);

        assert!(!map.is_revoked_workspace_id(host_wid));
    }
}
