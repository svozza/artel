//! `ProtocolHandler` wrapper that rejects incoming iroh-docs sync
//! connections from peers whose daemon-level capability has been
//! revoked.

// Crate-private module: pair `unreachable_pub` with the
// crate-visibility lint so they stop fighting (see memory).
#![allow(clippy::redundant_pub_crate)]

use std::sync::Arc;

use iroh::endpoint::{Connection, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh_docs::protocol::Docs;
use n0_error::e;

use crate::peer_map::PeerMap;

#[derive(Debug, Clone)]
pub(crate) struct DocsGate {
    inner: Docs,
    peer_map: Arc<PeerMap>,
}

impl DocsGate {
    pub(crate) const fn new(inner: Docs, peer_map: Arc<PeerMap>) -> Self {
        Self { inner, peer_map }
    }
}

impl ProtocolHandler for DocsGate {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote_id = connection.remote_id();
        if self.peer_map.is_revoked_workspace_id(remote_id) {
            tracing::warn!(
                target: "artel_fs::docs_gate",
                %remote_id,
                "rejected connection from revoked peer",
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
