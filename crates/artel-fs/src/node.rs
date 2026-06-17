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
use std::sync::Arc;
use std::time::Duration;

use iroh::protocol::Router;
use iroh::{Endpoint, EndpointId};
use iroh_blobs::BlobsProtocol;
use iroh_blobs::store::fs::FsStore;
use iroh_docs::protocol::Docs;
use iroh_gossip::net::Gossip;

use crate::WorkspaceError;
use crate::docs_gate::DocsGate;
use crate::endpoint_setup::EndpointSetup;
use crate::keystore::load_or_create_secret;
use crate::peer_filter::PeerFilter;
use crate::peer_map::PeerMap;

/// How long the substrate waits for the home-relay handshake
/// (`endpoint.online()`) before surfacing
/// [`WorkspaceError::RelayUnreachable`]. Tight enough to fail fast
/// when the relay is unreachable (offline laptop, captive portal,
/// n0 outage), loose enough to cover normal startup. Mirrored in
/// `artel_daemon::server::HOME_RELAY_BUDGET` — keep the two in
/// sync until the `EndpointSetup` duplication (handoff finding
/// #11) is resolved.
const HOME_RELAY_BUDGET: Duration = Duration::from_secs(30);

/// Per-`Workspace` iroh runtime. Held for the workspace's lifetime;
/// drop teardown is best-effort via [`Self::shutdown`].
#[derive(Debug)]
pub(crate) struct WorkspaceNode {
    /// Doc + author handles. `Clone` is cheap.
    pub docs: Docs,
    /// Blob protocol handler over the same store; `BlobsProtocol`
    /// derefs to the `Store` so callers can `.blobs().get_bytes(...)`.
    pub blobs: BlobsProtocol,
    /// The workspace node's peer map (shared with `DocsGate`).
    /// Retained for test access; production code accesses via the
    /// separate `Arc` held by the workspace constructor.
    #[allow(dead_code)]
    pub peer_map: Arc<PeerMap>,
    /// This node's `EndpointId`, captured before the router takes
    /// ownership.
    pub endpoint_id: EndpointId,
    /// The iroh-docs author this node stamps on its writes, **seeded
    /// from the same bytes as the endpoint key** so `AuthorId` ==
    /// `endpoint_id`. This binds every doc entry's author to the peer
    /// whose capability the session tracks (the `peer_map` resolves
    /// `entry.author` → daemon `PeerId` for free), without an
    /// announcement. Replaces iroh-docs' random `author_default()`.
    /// See ADR-002 / CONTEXT.md "Author binding". The key reuse is safe:
    /// a TLS `CertificateVerify` payload can never collide with an
    /// iroh-docs `entry.to_vec()` (distinct fixed prefixes).
    pub author: iroh_docs::AuthorId,
    /// Holding the router keeps the accept loop alive. Calling
    /// [`Router::shutdown`] on it during teardown closes the
    /// endpoint for us.
    router: Router,
    /// Test-only fault-injection flag. When set, the next call to
    /// [`Self::shutdown`] returns `Err` instead of awaiting the
    /// real router shutdown. Per-instance (rather than process-
    /// wide) so two parallel tests in the same integration binary
    /// don't trip each other's fault injection. Wrapped in an
    /// `Arc<AtomicBool>` so `Workspace::test_arm_shutdown_failure`
    /// can hand a clone back to the test harness without holding
    /// the workspace's node mutex.
    #[cfg(feature = "test-utils")]
    pub(crate) shutdown_failure_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
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
        peer_map: Arc<PeerMap>,
    ) -> Result<Self, WorkspaceError> {
        let secret = load_or_create_secret(&state_dir.join("iroh.key"))
            .map_err(|e| WorkspaceError::Iroh(format!("workspace key: {e}")))?;
        // Capture the raw key bytes before `secret` moves into the
        // endpoint builder — they seed the doc author below so
        // `AuthorId` == `endpoint_id` (same-seed author binding).
        let secret_bytes = secret.to_bytes();

        // Start from `presets::Empty` (no defaults set) and let the
        // `EndpointSetup::apply` chain layer the discovery preset of
        // its choice. Both Production (N0) and Testing (Minimal +
        // DnsPkarrServer) set the crypto provider via the Minimal
        // preset they each include.
        let endpoint = setup
            .apply(
                Endpoint::builder(iroh::endpoint::presets::Empty)
                    .secret_key(secret)
                    .hooks(PeerFilter::new(Arc::clone(&peer_map))),
            )
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

        let endpoint_id = endpoint.id();

        // Same-seed author binding: import an author keyed by the
        // endpoint's own secret bytes and make it the default, so every
        // write this node stamps carries `AuthorId == endpoint_id`. The
        // `peer_map` already maps `endpoint_id → daemon PeerId`, so this
        // binds doc-entry authorship to the capability-tracked peer with
        // no announcement. Replaces the random `author_default()`.
        let author = iroh_docs::Author::from_bytes(&secret_bytes);
        let author_id = author.id();
        docs.author_import(author)
            .await
            .map_err(|e| WorkspaceError::Doc(format!("author_import: {e}")))?;
        docs.author_set_default(author_id)
            .await
            .map_err(|e| WorkspaceError::Doc(format!("author_set_default: {e}")))?;
        debug_assert_eq!(
            author_id.as_bytes(),
            endpoint_id.as_bytes(),
            "same-seed author must equal endpoint id",
        );

        let router = Router::builder(endpoint.clone())
            .accept(iroh_gossip::ALPN, gossip)
            .accept(iroh_blobs::ALPN, blobs.clone())
            .accept(
                iroh_docs::ALPN,
                DocsGate::new(docs.clone(), Arc::clone(&peer_map)),
            )
            .spawn();

        // Production: block until the home-relay handshake
        // completes so a joiner that follows immediately doesn't
        // race us. Testing: skip — `presets::Minimal` has no relay
        // and `online()` would never resolve. Direct UDP is already
        // bound by the time `bind().await` returned, and the
        // localhost pkarr+DNS pair publishes our addr synchronously.
        //
        // The timeout exists to fail fast when the relay is
        // unreachable (offline laptop, captive portal, n0 outage,
        // or the `TestingUnreachableRelay` fixture). Without it
        // `Workspace::host_with` / `join_with` would hang forever
        // here. Surfacing a typed error lets the caller distinguish
        // "no relay" from a generic Iroh failure.
        if setup.awaits_relay()
            && tokio::time::timeout(HOME_RELAY_BUDGET, endpoint.online())
                .await
                .is_err()
        {
            return Err(WorkspaceError::RelayUnreachable(HOME_RELAY_BUDGET));
        }

        Ok(Self {
            docs,
            blobs,
            peer_map,
            endpoint_id,
            author: author_id,
            router,
            #[cfg(feature = "test-utils")]
            shutdown_failure_flag: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
        // Test-only fault injection: when this node's flag is armed
        // (via `Workspace::test_arm_shutdown_failure`), synthesise
        // an error BEFORE touching the real router. Per-instance,
        // so two tests in the same integration binary running in
        // parallel never trip each other.
        // `tests/workspace_shutdown_contract.rs` uses this to prove
        // `Workspace::shutdown` propagates router failures and keeps
        // the Drop bomb armed when teardown didn't actually succeed.
        // No production effect — gated entirely on the
        // `test-utils` cargo feature.
        #[cfg(feature = "test-utils")]
        if self
            .shutdown_failure_flag
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            // Best-effort: still try to tear the real router down so
            // we don't leak the endpoint into the next test. Ignore
            // its result — the synthesised error is what the test
            // wants to observe.
            let _ = self.router.shutdown().await;
            return Err(WorkspaceError::Iroh(
                "test-utils fault injection: router shutdown forced to fail".into(),
            ));
        }
        tracing::debug!(target: "artel_fs::node", "shutdown: router.shutdown()");
        self.router
            .shutdown()
            .await
            .map_err(|err| WorkspaceError::Iroh(format!("router shutdown: {err}")))?;
        tracing::debug!(target: "artel_fs::node", "shutdown: router done, dropping node resources");
        Ok(())
    }
}
