//! The per-`Workspace` iroh runtime.
//!
//! ADR-001 § "Doc handles across IPC" picked the **ticket-handout**
//! shape: each app process that wants doc sync spins up its **own**
//! iroh node, distinct from the daemon's. This module is that node.
//!
//! A [`WorkspaceNode`] owns:
//! - an [`Endpoint`] (its network identity, ed25519 + QUIC),
//! - a [`Gossip`] handle attached to the same endpoint,
//! - a disk-backed [`Docs`] + [`FsStore`]/[`BlobsProtocol`] pair,
//! - a [`Router`] accepting the gossip / docs / blobs ALPNs.
//!
//! Constructors are private; the only way to build one is through
//! [`crate::Workspace::host`] / `join`. Dropping the node aborts the
//! router and shuts the endpoint down with it.

// Crate-private module: pair `unreachable_pub` with the
// crate-visibility lint so they stop fighting (see memory).
#![allow(clippy::redundant_pub_crate)]

use std::path::Path;

use iroh::Endpoint;
use iroh::protocol::Router;
use iroh_blobs::BlobsProtocol;
use iroh_blobs::store::fs::FsStore;
use iroh_docs::protocol::Docs;
use iroh_gossip::net::Gossip;

use crate::WorkspaceError;
use crate::endpoint_setup::EndpointSetup;
use crate::keystore::load_or_create_secret;

/// Per-`Workspace` iroh runtime. Held for the workspace's lifetime;
/// drop teardown is best-effort via [`Self::shutdown`].
#[derive(Debug)]
pub(crate) struct WorkspaceNode {
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
    /// Stand up a disk-backed iroh node rooted at `state_dir`.
    ///
    /// On disk:
    /// - `state_dir/iroh.key` — workspace's ed25519 secret (mode
    ///   `0600`, generated on first call).
    /// - `state_dir/docs/` — `iroh-docs` persistent store (redb +
    ///   `default-author`).
    /// - `state_dir/blobs/` — `iroh-blobs` `FsStore`.
    ///
    /// Reuses any state already present, creates whatever's missing.
    ///
    /// `setup` controls the discovery layer (production n0 vs.
    /// localhost test fixtures). [`EndpointSetup::Production`] also
    /// awaits [`Endpoint::online`] for home-relay readiness;
    /// [`EndpointSetup::Testing`] skips it (Minimal has no relay
    /// and would hang).
    pub(crate) async fn spawn(
        state_dir: &Path,
        setup: &EndpointSetup,
    ) -> Result<Self, WorkspaceError> {
        let secret = load_or_create_secret(&state_dir.join("iroh.key"))
            .map_err(|e| WorkspaceError::Iroh(format!("workspace key: {e}")))?;

        // Start from `presets::Empty` (no defaults set) and let the
        // `EndpointSetup::apply` chain layer the discovery preset of
        // its choice. Both Production (N0) and Testing (Minimal +
        // DnsPkarrServer) set the crypto provider via the Minimal
        // preset they each include.
        let endpoint = setup
            .apply(Endpoint::builder(iroh::endpoint::presets::Empty).secret_key(secret))
            .bind()
            .await
            .map_err(|e| WorkspaceError::Iroh(format!("bind endpoint: {e}")))?;

        let gossip = Gossip::builder().spawn(endpoint.clone());

        let blobs_dir = state_dir.join("blobs");
        let docs_dir = state_dir.join("docs");
        // `FsStore::load` and `Docs::persistent` both create their
        // directories if missing, but parent creation is on us.
        if let Err(err) = std::fs::create_dir_all(&blobs_dir) {
            return Err(WorkspaceError::Iroh(format!(
                "create blobs dir {}: {err}",
                blobs_dir.display(),
            )));
        }
        if let Err(err) = std::fs::create_dir_all(&docs_dir) {
            return Err(WorkspaceError::Iroh(format!(
                "create docs dir {}: {err}",
                docs_dir.display(),
            )));
        }

        let blob_store = FsStore::load(&blobs_dir)
            .await
            .map_err(|e| WorkspaceError::Iroh(format!("load blob store: {e}")))?;
        let blobs = BlobsProtocol::new(&blob_store, None);
        let docs = Docs::persistent(docs_dir)
            .spawn(endpoint.clone(), (*blob_store).clone(), gossip.clone())
            .await
            .map_err(|e| WorkspaceError::Iroh(format!("spawn docs: {e}")))?;

        let router = Router::builder(endpoint.clone())
            .accept(iroh_gossip::ALPN, gossip)
            .accept(iroh_blobs::ALPN, blobs.clone())
            .accept(iroh_docs::ALPN, docs.clone())
            .spawn();

        // Production: block until the home-relay handshake
        // completes so a joiner that follows immediately doesn't
        // race us. Testing: skip — `presets::Minimal` has no relay
        // and `online()` would never resolve. Direct UDP is already
        // bound by the time `bind().await` returned, and the
        // localhost pkarr+DNS pair publishes our addr synchronously.
        if setup.awaits_relay() {
            endpoint.online().await;
        }

        Ok(Self {
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
