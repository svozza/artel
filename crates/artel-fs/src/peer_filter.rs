//! `EndpointHooks` implementation that blocks both outbound dials to
//! and inbound connections from revoked workspace endpoints at the
//! transport layer.

// Crate-private module: pair `unreachable_pub` with the
// crate-visibility lint so they stop fighting (see memory).
#![allow(clippy::redundant_pub_crate)]

use std::sync::Arc;

use iroh::EndpointAddr;
use iroh::endpoint::{
    AfterHandshakeOutcome, BeforeConnectOutcome, ConnectionInfo, EndpointHooks, Side, VarInt,
};

use crate::peer_map::PeerMap;

#[derive(Debug)]
pub(crate) struct PeerFilter {
    peer_map: Arc<PeerMap>,
}

impl PeerFilter {
    pub(crate) const fn new(peer_map: Arc<PeerMap>) -> Self {
        Self { peer_map }
    }
}

impl EndpointHooks for PeerFilter {
    async fn before_connect<'a>(
        &'a self,
        remote_addr: &'a EndpointAddr,
        _alpn: &'a [u8],
    ) -> BeforeConnectOutcome {
        if self.peer_map.is_revoked_workspace_id(remote_addr.id) {
            tracing::warn!(
                target: "artel_fs::peer_filter",
                remote_id = %remote_addr.id,
                "blocked outbound dial to revoked peer",
            );
            BeforeConnectOutcome::Reject
        } else {
            BeforeConnectOutcome::Accept
        }
    }

    async fn after_handshake<'a>(&'a self, conn: &'a ConnectionInfo) -> AfterHandshakeOutcome {
        if conn.side() == Side::Server && self.peer_map.is_revoked_workspace_id(conn.remote_id()) {
            tracing::warn!(
                target: "artel_fs::peer_filter",
                remote_id = %conn.remote_id(),
                "rejected inbound connection from revoked peer",
            );
            AfterHandshakeOutcome::Reject {
                error_code: VarInt::from_u32(1),
                reason: b"revoked".to_vec(),
            }
        } else {
            AfterHandshakeOutcome::Accept
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use artel_protocol::PeerId;
    use artel_protocol::capability::{Capability, CapabilityAction};
    use iroh::EndpointId;

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

    fn make_filter() -> PeerFilter {
        let peer_map = Arc::new(PeerMap::new(test_host()));
        PeerFilter::new(peer_map)
    }

    #[tokio::test]
    async fn unknown_endpoint_is_accepted_outbound() {
        let filter = make_filter();
        let addr = EndpointAddr::new(test_workspace_id());
        let outcome = filter.before_connect(&addr, b"test/alpn").await;
        assert!(matches!(outcome, BeforeConnectOutcome::Accept));
    }

    #[tokio::test]
    async fn active_rw_peer_is_accepted_outbound() {
        let peer_map = Arc::new(PeerMap::new(test_host()));
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();

        peer_map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        peer_map.register(wid, peer);

        let filter = PeerFilter::new(peer_map);
        let addr = EndpointAddr::new(wid);
        let outcome = filter.before_connect(&addr, iroh_docs::ALPN).await;
        assert!(matches!(outcome, BeforeConnectOutcome::Accept));
    }

    #[tokio::test]
    async fn revoked_peer_is_rejected_outbound() {
        let peer_map = Arc::new(PeerMap::new(test_host()));
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();

        peer_map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        peer_map.register(wid, peer);
        peer_map.apply_capability(host, &revoke_payload(peer));

        let filter = PeerFilter::new(peer_map);
        let addr = EndpointAddr::new(wid);
        let outcome = filter.before_connect(&addr, iroh_docs::ALPN).await;
        assert!(matches!(outcome, BeforeConnectOutcome::Reject));
    }

    #[tokio::test]
    async fn rejects_outbound_regardless_of_alpn() {
        let peer_map = Arc::new(PeerMap::new(test_host()));
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();

        peer_map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        peer_map.register(wid, peer);
        peer_map.apply_capability(host, &revoke_payload(peer));

        let filter = PeerFilter::new(peer_map);
        let addr = EndpointAddr::new(wid);

        let outcome = filter.before_connect(&addr, iroh_docs::ALPN).await;
        assert!(matches!(outcome, BeforeConnectOutcome::Reject));

        let outcome = filter.before_connect(&addr, iroh_blobs::ALPN).await;
        assert!(matches!(outcome, BeforeConnectOutcome::Reject));

        let outcome = filter.before_connect(&addr, b"something/else").await;
        assert!(matches!(outcome, BeforeConnectOutcome::Reject));
    }
}
