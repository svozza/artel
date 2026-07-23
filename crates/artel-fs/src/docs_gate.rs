//! `ProtocolHandler` wrapper that rejects incoming iroh-docs sync
//! connections from peers whose daemon-level capability has been
//! revoked.
//!
//! NOTE: `PeerFilter::after_handshake` now rejects ALL inbound
//! connections from revoked peers at the transport layer, making this
//! gate technically redundant. Kept as defense-in-depth for now;
//! candidate for removal once `PeerFilter` has proven stable.

// Crate-private module: pair `unreachable_pub` with the
// crate-visibility lint so they stop fighting (see memory).
#![allow(clippy::redundant_pub_crate)]

use std::sync::Arc;

use iroh::endpoint::{Connection, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh_docs::protocol::Docs;
use n0_error::e;
use tokio::sync::mpsc;

use crate::peer_map::PeerMap;
use crate::workspace::{Direction, WorkspaceEvent, emit_event};

#[derive(Debug, Clone)]
pub(crate) struct DocsGate {
    inner: Docs,
    peer_map: Arc<PeerMap>,
    /// Surfaces a [`WorkspaceEvent::RevokedPeerBlocked`] per reject.
    /// Non-blocking (`emit_event`): the accept path must never park
    /// on a slow event consumer.
    events: mpsc::Sender<WorkspaceEvent>,
}

impl DocsGate {
    pub(crate) const fn new(
        inner: Docs,
        peer_map: Arc<PeerMap>,
        events: mpsc::Sender<WorkspaceEvent>,
    ) -> Self {
        Self {
            inner,
            peer_map,
            events,
        }
    }
}

impl ProtocolHandler for DocsGate {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote_id = connection.remote_id();
        if let Some(peer) = self.peer_map.revoked_daemon_peer(remote_id) {
            tracing::warn!(
                target: "artel_fs::docs_gate",
                %remote_id,
                "rejected connection from revoked peer",
            );
            emit_event(
                &self.events,
                WorkspaceEvent::RevokedPeerBlocked {
                    peer,
                    direction: Direction::Incoming,
                },
            );
            connection.close(VarInt::from_u32(1), b"revoked");
            return Err(e!(AcceptError::NotAllowed));
        }
        self.inner.accept(connection).await
    }

    async fn shutdown(&self) {
        self.inner.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use artel_protocol::PeerId;
    use artel_protocol::capability::{Capability, CapabilityAction};
    use iroh::Endpoint;
    use iroh::endpoint::presets;
    use iroh::protocol::ProtocolHandler;
    use iroh_blobs::store::mem::MemStore;
    use tokio::sync::mpsc;

    use super::DocsGate;
    use crate::peer_map::PeerMap;
    use crate::workspace::{Direction, WorkspaceEvent};

    fn test_host() -> PeerId {
        PeerId::from_bytes([1; 32])
    }

    fn test_peer() -> PeerId {
        PeerId::from_bytes([2; 32])
    }

    fn grant_payload(peer: PeerId, cap: Capability) -> Vec<u8> {
        CapabilityAction::Grant { peer, cap }.encode()
    }

    fn revoke_payload(peer: PeerId) -> Vec<u8> {
        CapabilityAction::Revoke { peer }.encode()
    }

    async fn test_docs(endpoint: &Endpoint) -> super::Docs {
        let store = MemStore::new();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
        super::Docs::memory()
            .spawn(endpoint.clone(), (*store).clone(), gossip)
            .await
            .expect("spawn docs")
    }

    /// Bind on IPv4 loopback only, matching iroh's own `direct_pair`
    /// test fixture: multi-homed CI hosts otherwise advertise
    /// addresses the test's localhost-only dial can't reach.
    async fn bind_loopback(alpns: Vec<Vec<u8>>) -> Endpoint {
        Endpoint::builder(presets::Minimal)
            .alpns(alpns)
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))
            .expect("valid loopback bind addr")
            .bind()
            .await
            .expect("bind endpoint")
    }

    /// Dial `server`'s address from a fresh client endpoint and hand back
    /// the server-side `Connection` the dial produced, so tests can drive
    /// `DocsGate::accept` directly against a real handshake.
    ///
    /// `server.accept()` only resolves once the first handshake packet
    /// arrives — completing the handshake needs a further `.accept()` +
    /// await that must run *concurrently* with the client's `connect()`
    /// (the client's future won't resolve until the server side responds).
    /// Both sides are therefore driven inside one `tokio::join!`.
    async fn dial_in(server: &Endpoint, alpn: &[u8]) -> iroh::endpoint::Connection {
        let client = bind_loopback(vec![]).await;
        let addr = server.addr();
        let server_side = async {
            let incoming = server.accept().await.expect("server accept");
            incoming
                .accept()
                .expect("accept incoming")
                .await
                .expect("server handshake")
        };
        let (client_conn, server_conn) = tokio::join!(client.connect(addr, alpn), server_side);
        client_conn.expect("client connect");
        server_conn
    }

    #[tokio::test]
    async fn revoked_peer_connection_is_rejected() {
        let server = bind_loopback(vec![iroh_docs::ALPN.to_vec()]).await;
        let docs = test_docs(&server).await;

        let peer_map = Arc::new(PeerMap::new(test_host()));
        let host = test_host();
        let peer = test_peer();
        peer_map.apply_capability(host, &grant_payload(peer, Capability::ReadWrite));

        let connection = dial_in(&server, iroh_docs::ALPN).await;
        peer_map.register(connection.remote_id(), peer);
        peer_map.apply_capability(host, &revoke_payload(peer));

        let (tx, mut rx) = mpsc::channel(8);
        let gate = DocsGate::new(docs, Arc::clone(&peer_map), tx);

        let result = gate.accept(connection).await;
        assert!(
            matches!(result, Err(iroh::protocol::AcceptError::NotAllowed { .. })),
            "expected NotAllowed, got {result:?}"
        );

        match rx.try_recv() {
            Ok(WorkspaceEvent::RevokedPeerBlocked {
                peer: got_peer,
                direction,
            }) => {
                assert_eq!(got_peer, peer);
                assert_eq!(direction, Direction::Incoming);
            }
            other => panic!("expected RevokedPeerBlocked, got {other:?}"),
        }

        server.close().await;
    }

    #[tokio::test]
    async fn shutdown_delegates_to_inner_docs() {
        let server = bind_loopback(vec![iroh_docs::ALPN.to_vec()]).await;
        let docs = test_docs(&server).await;

        let peer_map = Arc::new(PeerMap::new(test_host()));
        let (tx, _rx) = mpsc::channel(8);
        let gate = DocsGate::new(docs, peer_map, tx);

        // Delegates to the wrapped `Docs::shutdown`; proving it doesn't
        // panic/hang is the whole contract — there's no wrapper-level
        // state to assert on.
        gate.shutdown().await;

        server.close().await;
    }
}
