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

    /// Tear the node down gracefully.
    ///
    /// Returns `Err` if `Router::shutdown` reported a teardown failure
    /// — the relay session may not have closed cleanly. The caller is
    /// expected to surface that to its own caller so the
    /// [`crate::Workspace`] Drop bomb can stay armed (a router that
    /// failed to shut down is exactly the misuse the bomb documents).
    pub(crate) async fn shutdown(self) -> Result<(), WorkspaceError> {
        // Test-only fault injection: when the parent test sets
        // `force_shutdown_failure(true)`, we synthesise an error
        // BEFORE touching the real router. Used by
        // `tests/workspace_shutdown_contract.rs` to prove
        // `Workspace::shutdown` propagates router failures and keeps
        // the Drop bomb armed when teardown didn't actually succeed.
        // No production effect — gated entirely on the
        // `test-utils` cargo feature.
        #[cfg(feature = "test-utils")]
        if test_hooks::take_force_shutdown_failure() {
            // Best-effort: still try to tear the real router down so
            // we don't leak the endpoint into the next test. Ignore
            // its result — the synthesised error is what the test
            // wants to observe.
            let _ = self.router.shutdown().await;
            return Err(WorkspaceError::Iroh(
                "test-utils fault injection: router shutdown forced to fail".into(),
            ));
        }
        self.router
            .shutdown()
            .await
            .map_err(|err| WorkspaceError::Iroh(format!("router shutdown: {err}")))
    }
}

/// Test-only fault-injection knobs for [`WorkspaceNode`].
///
/// Sole consumer is `tests/workspace_shutdown_contract.rs`, which
/// needs to coerce `Router::shutdown` into returning `Err` to prove
/// that [`crate::Workspace::shutdown`] propagates the failure and
/// leaves the Drop bomb armed. There is no other way to fail an iroh
/// router shutdown in-process; mocking the entire iroh stack would
/// be a much bigger surface to maintain.
///
/// Single-shot: each `force_shutdown_failure(true)` arms exactly one
/// failure; the next call to [`WorkspaceNode::shutdown`] consumes
/// the flag and returns the synthesised error.
#[cfg(feature = "test-utils")]
#[allow(unreachable_pub)]
pub(crate) mod test_hooks {
    use std::sync::atomic::{AtomicBool, Ordering};

    static FORCE_SHUTDOWN_FAILURE: AtomicBool = AtomicBool::new(false);

    /// Arm the next [`super::WorkspaceNode::shutdown`] to return an
    /// error without consulting the real router. Single-shot — the
    /// flag is consumed on read. Re-exported from the crate root as
    /// [`crate::test_hooks::force_shutdown_failure`].
    pub fn force_shutdown_failure(armed: bool) {
        FORCE_SHUTDOWN_FAILURE.store(armed, Ordering::SeqCst);
    }

    /// Read-and-clear the fault-injection flag. Called from inside
    /// [`super::WorkspaceNode::shutdown`].
    pub(super) fn take_force_shutdown_failure() -> bool {
        FORCE_SHUTDOWN_FAILURE.swap(false, Ordering::SeqCst)
    }
}
