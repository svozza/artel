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

use iroh::protocol::Router;
use iroh::{Endpoint, EndpointId};
use iroh_blobs::BlobsProtocol;
use iroh_blobs::store::fs::FsStore;
use iroh_docs::protocol::Docs;
use iroh_gossip::net::Gossip;

use crate::WorkspaceError;
use crate::docs_gate::DocsGate;
use crate::peer_filter::PeerFilter;
use crate::peer_map::PeerMap;
use artel_iroh_setup::EndpointSetup;

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
    /// See CONTEXT.md "Author binding". The key reuse is safe:
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
        events: tokio::sync::mpsc::Sender<crate::WorkspaceEvent>,
    ) -> Result<Self, WorkspaceError> {
        let secret = artel_iroh_setup::load_or_create(&state_dir.join("iroh.key"))
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
                    .hooks(PeerFilter::new(Arc::clone(&peer_map), events.clone())),
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
        // endpoint's own secret bytes, so every write this node stamps
        // carries `AuthorId == endpoint_id`. The `peer_map` already maps
        // `endpoint_id → daemon PeerId`, so this binds doc-entry
        // authorship to the capability-tracked peer with no announcement.
        // Replaces the random `author_default()`.
        //
        // We deliberately do NOT call `author_set_default`. Every write
        // path (`set_bytes` / `del` / `set_hash`) passes `self.author`
        // explicitly, so the store's *default* author is never read —
        // making it dead weight. Worse, setting it is a crash hazard:
        // iroh-docs' `DefaultAuthor::set` writes the `docs/default-author`
        // pointer file immediately, but `author_import` only writes the
        // author row into redb, which redb commits on a batched (~500ms)
        // delay. A SIGKILL in that window leaves a durable pointer file
        // referencing an author row that never committed; the next
        // `Docs::persistent().spawn()` then hard-fails in
        // `DefaultAuthor::load` with "The default author is missing from
        // the docs store" (it is a dangling reference). iroh-docs' own
        // fresh-author path guards this with a `flush_store()` between
        // import and persist (see iroh_docs::engine `DefaultAuthorStorage::load`),
        // but `author_set_default` exposes no such flush — so we avoid the
        // pointer file entirely rather than race the commit.
        let author = iroh_docs::Author::from_bytes(&secret_bytes);
        let author_id = author.id();
        docs.author_import(author)
            .await
            .map_err(|e| WorkspaceError::Doc(format!("author_import: {e}")))?;
        // Hard-assert the same-seed binding (not just debug_assert): it is
        // security-load-bearing. Namespace rotation's author-filter keeps
        // an entry only if `peer_map.classify_author(entry.author)` is RW,
        // which resolves *because* `AuthorId == endpoint_id`. If a future
        // iroh-docs change made `Author::from_bytes` derive its id
        // differently, the binding would silently break in release and the
        // fail-closed filter would drop *every* survivor's entries on the
        // next rotation. Fail loudly at spawn instead, before any data
        // depends on it. Runs once per node — the cost is irrelevant.
        if author_id.as_bytes() != endpoint_id.as_bytes() {
            return Err(WorkspaceError::Iroh(format!(
                "same-seed author binding broken: author id {author_id} != endpoint id \
                 {endpoint_id}; namespace-rotation author-filter would fail closed",
            )));
        }

        let router = Router::builder(endpoint.clone())
            .accept(iroh_gossip::ALPN, gossip)
            .accept(iroh_blobs::ALPN, blobs.clone())
            .accept(
                iroh_docs::ALPN,
                DocsGate::new(docs.clone(), Arc::clone(&peer_map), events),
            )
            .spawn();

        // Production: block until the home-relay handshake
        // completes so a joiner that follows immediately doesn't
        // race us. Testing: skip — `presets::Minimal` has no relay
        // and `online()` would never resolve. Direct UDP is already
        // bound by the time `bind().await` returned, and the
        // localhost pkarr+DNS pair publishes our addr synchronously.
        // Bounded so an unreachable relay (offline laptop, captive
        // portal, n0 outage, or the `TestingUnreachableRelay`
        // fixture) fails fast as a typed error the caller can
        // distinguish from a generic Iroh failure, rather than
        // hanging `Workspace::host_with` / `join_with` forever.
        artel_iroh_setup::await_relay_ready(setup, &endpoint)
            .await
            .map_err(WorkspaceError::RelayUnreachable)?;

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

#[cfg(test)]
mod tests {
    use iroh_docs::sync::{Entry, Record, RecordIdentifier};
    use iroh_docs::{AuthorId, NamespaceId};

    /// Cross-protocol key-reuse safety (Slice 1 / D4).
    ///
    /// We reuse one ed25519 secret across two signing domains: the iroh
    /// transport (TLS 1.3 raw-public-key handshake) and iroh-docs entry
    /// authorship. Reuse is only sound if the two domains sign provably
    /// disjoint byte-strings, so a signature minted in one can never be
    /// replayed as a valid signature in the other.
    ///
    /// - **TLS 1.3 `CertificateVerify`** (RFC 8446 §4.4.3): the signed
    ///   content begins with exactly **64 octets of `0x20`** (a fixed
    ///   padding prefix), followed by a context string, a `0x00`
    ///   separator, and the transcript hash.
    /// - **iroh-docs author signature**: the author signs
    ///   `Entry::to_vec()`, which begins with the `RecordIdentifier` —
    ///   whose first 32 bytes are the **`NamespaceId`** (the namespace
    ///   ed25519 *public* key), followed by the 32-byte `AuthorId` and
    ///   the key.
    ///
    /// This test pins the iroh-docs half — that the signed payload starts
    /// with the 32-byte namespace pubkey — and asserts that prefix can
    /// never collide with the TLS 64×`0x20` prefix. The first 32 bytes of
    /// a TLS payload are all `0x20`; a 32-byte ed25519 public key equal to
    /// `[0x20; 32]` is not a structural possibility an attacker controls,
    /// and even if it were, the *next* 32 bytes of the TLS payload are
    /// still `0x20` while the docs payload carries the author id — so the
    /// 64-byte windows are disjoint by construction. If a future iroh-docs
    /// release reorders `Entry::to_vec()` so the namespace no longer leads,
    /// this test fails and the key-reuse safety argument must be revisited.
    #[test]
    fn author_signed_payload_prefix_is_disjoint_from_tls_certificate_verify() {
        // A distinctive namespace id so we can find it as the prefix.
        let namespace = NamespaceId::from(&[0x37u8; 32]);
        let author = AuthorId::from(&[0x99u8; 32]);
        let id = RecordIdentifier::new(namespace, author, b"path/some-file.txt");
        let record = Record::new(iroh_blobs::Hash::EMPTY, 0, 0);
        let entry = Entry::new(id, record);

        let signed = entry.to_vec();

        // The author-signed payload begins with the 32-byte namespace
        // pubkey (the prefix the cross-protocol-safety claim rests on).
        assert!(
            signed.len() >= 64,
            "signed entry payload shorter than the 64-byte TLS prefix window",
        );
        assert_eq!(
            &signed[..32],
            &[0x37u8; 32],
            "iroh-docs author-signed payload must begin with the NamespaceId; \
             if this changed, cross-protocol key-reuse safety must be re-proven",
        );
        // The next 32 bytes are the author id, NOT more padding — the
        // 64-byte window can't be all `0x20` the way a TLS CertificateVerify
        // payload's leading 64 octets are.
        assert_eq!(
            &signed[32..64],
            &[0x99u8; 32],
            "bytes 32..64 must be the AuthorId, keeping the 64-byte window \
             distinct from TLS's 64×0x20 prefix",
        );

        // The TLS 1.3 CertificateVerify signed content (RFC 8446 §4.4.3)
        // begins with 64 octets of 0x20. Our author payload's 64-byte
        // window is (namespace || author) — for it to collide, BOTH the
        // namespace pubkey and the author id would have to be all-0x20,
        // and even then the domains differ in the trailing context. Pin
        // the disjointness explicitly: a realistic namespace/author pair
        // is not 64 bytes of 0x20.
        let tls_prefix = [0x20u8; 64];
        assert_ne!(
            &signed[..64],
            &tls_prefix,
            "author payload's 64-byte window must not equal TLS's 64×0x20 prefix",
        );
    }
}
