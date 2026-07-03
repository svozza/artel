//! `EndpointHooks` implementation that blocks both outbound dials to
//! and inbound connections from revoked workspace endpoints at the
//! transport layer.

// Crate-private module: pair `unreachable_pub` with the
// crate-visibility lint so they stop fighting (see memory).
#![allow(clippy::redundant_pub_crate)]

use std::sync::Arc;

use iroh::EndpointAddr;
use iroh::endpoint::{
    AfterHandshakeOutcome, BeforeConnectOutcome, Connection, EndpointHooks, Side, VarInt,
};
use tokio::sync::mpsc;

use crate::peer_map::PeerMap;
use crate::workspace::{Direction, WorkspaceEvent, emit_event};

#[derive(Debug)]
pub(crate) struct PeerFilter {
    peer_map: Arc<PeerMap>,
    /// Surfaces a [`WorkspaceEvent::RevokedPeerBlocked`] per block.
    /// Non-blocking (`emit_event`): these hooks sit on the connection
    /// path and must never park on a slow event consumer.
    events: mpsc::Sender<WorkspaceEvent>,
}

impl PeerFilter {
    pub(crate) const fn new(peer_map: Arc<PeerMap>, events: mpsc::Sender<WorkspaceEvent>) -> Self {
        Self { peer_map, events }
    }
}

impl EndpointHooks for PeerFilter {
    async fn before_connect<'a>(
        &'a self,
        remote_addr: &'a EndpointAddr,
        _alpn: &'a [u8],
    ) -> BeforeConnectOutcome {
        let Some(peer) = self.peer_map.revoked_daemon_peer(remote_addr.id) else {
            return BeforeConnectOutcome::Accept;
        };
        tracing::warn!(
            target: "artel_fs::peer_filter",
            remote_id = %remote_addr.id,
            "blocked outbound dial to revoked peer",
        );
        emit_event(
            &self.events,
            WorkspaceEvent::RevokedPeerBlocked {
                peer,
                direction: Direction::Outgoing,
            },
        );
        BeforeConnectOutcome::Reject
    }

    async fn after_handshake<'a>(&'a self, conn: &'a Connection) -> AfterHandshakeOutcome {
        if conn.side() == Side::Server
            && let Some(peer) = self.peer_map.revoked_daemon_peer(conn.remote_id())
        {
            tracing::warn!(
                target: "artel_fs::peer_filter",
                remote_id = %conn.remote_id(),
                "rejected inbound connection from revoked peer",
            );
            emit_event(
                &self.events,
                WorkspaceEvent::RevokedPeerBlocked {
                    peer,
                    direction: Direction::Incoming,
                },
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

    fn make_filter(peer_map: Arc<PeerMap>) -> (PeerFilter, mpsc::Receiver<WorkspaceEvent>) {
        let (tx, rx) = mpsc::channel(8);
        (PeerFilter::new(peer_map, tx), rx)
    }

    /// Assert the next pending event is a `RevokedPeerBlocked` naming
    /// exactly `peer` / `direction`.
    fn expect_blocked_event(
        rx: &mut mpsc::Receiver<WorkspaceEvent>,
        peer: PeerId,
        direction: Direction,
    ) {
        match rx.try_recv() {
            Ok(WorkspaceEvent::RevokedPeerBlocked {
                peer: got_peer,
                direction: got_direction,
            }) => {
                assert_eq!(got_peer, peer);
                assert_eq!(got_direction, direction);
            }
            other => panic!("expected RevokedPeerBlocked, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unknown_endpoint_is_accepted_outbound() {
        let (filter, mut rx) = make_filter(Arc::new(PeerMap::new(test_host())));
        let addr = EndpointAddr::new(test_workspace_id());
        let outcome = filter.before_connect(&addr, b"test/alpn").await;
        assert!(matches!(outcome, BeforeConnectOutcome::Accept));
        assert!(rx.try_recv().is_err(), "accept must not emit an event");
    }

    #[tokio::test]
    async fn active_rw_peer_is_accepted_outbound() {
        let peer_map = Arc::new(PeerMap::new(test_host()));
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();

        peer_map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        peer_map.register(wid, peer);

        let (filter, mut rx) = make_filter(peer_map);
        let addr = EndpointAddr::new(wid);
        let outcome = filter.before_connect(&addr, iroh_docs::ALPN).await;
        assert!(matches!(outcome, BeforeConnectOutcome::Accept));
        assert!(rx.try_recv().is_err(), "accept must not emit an event");
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

        let (filter, mut rx) = make_filter(peer_map);
        let addr = EndpointAddr::new(wid);
        let outcome = filter.before_connect(&addr, iroh_docs::ALPN).await;
        assert!(matches!(outcome, BeforeConnectOutcome::Reject));
        expect_blocked_event(&mut rx, peer, Direction::Outgoing);
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

        let (filter, mut rx) = make_filter(peer_map);
        let addr = EndpointAddr::new(wid);

        let outcome = filter.before_connect(&addr, iroh_docs::ALPN).await;
        assert!(matches!(outcome, BeforeConnectOutcome::Reject));

        let outcome = filter.before_connect(&addr, iroh_blobs::ALPN).await;
        assert!(matches!(outcome, BeforeConnectOutcome::Reject));

        let outcome = filter.before_connect(&addr, b"something/else").await;
        assert!(matches!(outcome, BeforeConnectOutcome::Reject));

        // One event per blocked dial, all naming the same peer.
        for _ in 0..3 {
            expect_blocked_event(&mut rx, peer, Direction::Outgoing);
        }
        assert!(rx.try_recv().is_err(), "no extra events expected");
    }

    #[tokio::test]
    async fn full_event_channel_still_rejects() {
        // The emit is advisory and non-blocking: a consumer that never
        // drains the channel must not affect the block itself.
        let peer_map = Arc::new(PeerMap::new(test_host()));
        let host = test_host();
        let peer = test_peer();
        let wid = test_workspace_id();

        peer_map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));
        peer_map.register(wid, peer);
        peer_map.apply_capability(host, &revoke_payload(peer));

        let (tx, _rx) = mpsc::channel(1);
        let filter = PeerFilter::new(peer_map, tx);
        let addr = EndpointAddr::new(wid);

        // Second and later dials overflow the 1-slot channel; the
        // outcome must stay Reject and the call must not block.
        for _ in 0..4 {
            let outcome = filter.before_connect(&addr, iroh_docs::ALPN).await;
            assert!(matches!(outcome, BeforeConnectOutcome::Reject));
        }
    }
}
