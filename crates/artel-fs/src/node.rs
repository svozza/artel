//! The per-`Workspace` iroh runtime.
//!
//! ADR-001 § "Doc handles across IPC" picked the **ticket-handout**
//! shape: each app process that wants doc sync spins up its **own**
//! iroh node, distinct from the daemon's. This module is that node.
//!
//! A [`WorkspaceNode`] owns:
//! - an [`Endpoint`] (its network identity, ed25519 + QUIC),
//! - a [`Gossip`] handle attached to the same endpoint,
//! - an in-memory [`Docs`] + [`MemStore`]/[`BlobsProtocol`] pair,
//! - a [`Router`] accepting the gossip / docs / blobs ALPNs.
//!
//! Constructors are private; the only way to build one is through
//! [`crate::Workspace::host`] / `join`. Dropping the node aborts the
//! router and shuts the endpoint down with it.
//!
//! Storage is **memory only** for the MVP (per
//! `docs/handoff-phase-3-mvp.md` § "Architectural shape"). Disk-backed
//! Docs/Blobs is deliberately deferred to a follow-up slice.

// Crate-private module: pair `unreachable_pub` with the
// crate-visibility lint so they stop fighting (see memory).
#![allow(clippy::redundant_pub_crate)]

use iroh::protocol::Router;
use iroh::{Endpoint, SecretKey};
use iroh_blobs::BlobsProtocol;
use iroh_blobs::store::mem::MemStore;
use iroh_docs::protocol::Docs;
use iroh_gossip::net::Gossip;

use crate::WorkspaceError;

/// Per-`Workspace` iroh runtime. Held for the workspace's lifetime;
/// drop teardown is best-effort via [`Self::shutdown`].
#[derive(Debug)]
// `endpoint` is used by the joiner path (3a-4) and watcher (3a-5).
#[allow(dead_code)]
pub(crate) struct WorkspaceNode {
    /// Endpoint owning the workspace's ed25519 identity. Distinct
    /// from the artel daemon's endpoint.
    pub endpoint: Endpoint,
    /// Doc + author handles. `Clone` is cheap.
    pub docs: Docs,
    /// Blob protocol handler over the same store; `BlobsProtocol`
    /// derefs to the `Store` so callers can `.blobs().get_bytes(...)`.
    pub blobs: BlobsProtocol,
    /// Holding the router keeps the accept loop alive. Calling
    /// [`Router::shutdown`] on it during teardown closes the
    /// endpoint for us.
    router: Router,
}

impl WorkspaceNode {
    /// Stand up a fresh in-memory iroh node. Generates a one-shot
    /// `SecretKey` — the workspace doesn't need a stable identity for
    /// the MVP since it carries no persistent state across restarts.
    pub(crate) async fn spawn() -> Result<Self, WorkspaceError> {
        let secret = SecretKey::generate();
        let endpoint = Endpoint::builder(iroh::endpoint::presets::N0)
            .secret_key(secret)
            .bind()
            .await
            .map_err(|e| WorkspaceError::Iroh(format!("bind endpoint: {e}")))?;

        let gossip = Gossip::builder().spawn(endpoint.clone());

        let blob_store = MemStore::new();
        let blobs = BlobsProtocol::new(&blob_store, None);
        let docs = Docs::memory()
            .spawn(endpoint.clone(), (*blob_store).clone(), gossip.clone())
            .await
            .map_err(|e| WorkspaceError::Iroh(format!("spawn docs: {e}")))?;

        let router = Router::builder(endpoint.clone())
            .accept(iroh_gossip::ALPN, gossip)
            .accept(iroh_blobs::ALPN, blobs.clone())
            .accept(iroh_docs::ALPN, docs.clone())
            .spawn();

        // Block until the endpoint is ready to accept; without this a
        // joiner that follows immediately can race us.
        endpoint.online().await;

        Ok(Self {
            endpoint,
            docs,
            blobs,
            router,
        })
    }

    /// Tear the node down gracefully. Best-effort; errors are logged.
    pub(crate) async fn shutdown(self) {
        if let Err(err) = self.router.shutdown().await {
            tracing::warn!(error = %err, "workspace iroh router shutdown failed");
        }
    }
}
