//! Iroh-internal smoke tests: the production-discovery doc-ticket
//! round-trip (Tier C), the deterministic `DnsPkarrServer`-backed
//! sibling (Tier B), and the `TestingUnreachableRelay` typed-error
//! contract (Tier B).
//!
//! Consolidated from three per-file bins (`iroh_docs_smoke`,
//! `iroh_docs_smoke_pkarr`, `relay_unreachable`) per
//! `docs/plans/2026-05-29-faster-cargo-test-plan.md` slice 2e.
//!
//! Naming: each section's `Node` has different fixture wiring under
//! the hood, so the consolidated bin uses section-prefixed names
//! (`N0Node` / `PkarrNode`) rather than try to share an abstraction.
//! The phase wrappers and tracing init are shared across sections.

#![cfg(feature = "test-utils")]
#![allow(clippy::large_futures)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{
    AttachPolicy, EndpointSetup, TEST_DNS_ORIGIN, Workspace, WorkspaceConfig, WorkspaceError,
};
use bytes::Bytes;
use futures_util::StreamExt;
use iroh::address_lookup::{AddrFilter, DnsAddressLookup, PkarrPublisher};
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::test_utils::DnsPkarrServer;
use iroh::{Endpoint, SecretKey};
use iroh_blobs::BlobsProtocol;
use iroh_blobs::store::mem::MemStore;
use iroh_docs::engine::LiveEvent;
use iroh_docs::protocol::Docs;
use iroh_docs::store::Query;
use iroh_gossip::net::Gossip;
use tokio::time::timeout;

// =============================================================
// iroh-docs / iroh-blobs version-compat smoke test (Tier C, real n0).
//
// Proves the version matrix in `[workspace.dependencies]` actually
// compiles + works together, and that a `DocTicket` carries enough
// `EndpointAddr` info on its own for the joiner to dial the host
// without any out-of-band address-book seeding.
//
// Sync-vs-blob ordering: doc metadata syncs before blob bytes are
// downloaded. iroh-docs surfaces this via `LiveEvent::ContentReady`.
// Calling `get_bytes` before that races against the in-progress
// download and trips iroh-blobs' leaf-hash verifier with
// `LeafHashMismatch`. The test subscribes before waiting for the
// entry, then gates the read on `ContentReady`.
//
// Sync-retry on dial failure: iroh-docs's `Docs::import` triggers a
// single dial attempt. If the host's pkarr publish hasn't propagated
// to `dns.iroh.link` yet, the joiner polls `get_many(Query::all())`
// and relies on iroh-docs eventually noticing the host once pkarr
// propagates. Best read as a happy-path canary.
// =============================================================

/// Per-phase deadline for the n0 smoke test. Generous so a slow-
/// but-working n0 doesn't trip the bound; tight enough that a stuck
/// phase fails fast with a clear "phase X hung" message.
const N0_PHASE_BUDGET: Duration = Duration::from_secs(20);

/// First-contact phase budget. Discovery + reconcile + blob fetch
/// over a real network. Tight enough that we observe flakes rather
/// than mask them; generous enough that a healthy run isn't
/// gambling on a fast pkarr round-trip.
const N0_DISCOVERY_BUDGET: Duration = Duration::from_secs(30);

async fn n0_phase<F, T>(name: &'static str, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    common::phase_budgeted(name, N0_PHASE_BUDGET, fut).await
}

fn init_n0_tracing() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| {
            concat!(
                "info,",
                "iroh=debug,",
                "iroh::discovery=trace,",
                "iroh_docs=debug,",
                "iroh_gossip=debug,",
                "iroh_blobs=debug",
            )
            .to_string()
        });
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .with_test_writer()
            .with_target(true)
            .try_init();
    });
}

/// One full iroh node bound against `presets::N0` (production
/// pkarr+DNS + production relay map).
struct N0Node {
    _endpoint: Endpoint,
    docs: Docs,
    blobs: BlobsProtocol,
    router: Router,
}

impl N0Node {
    async fn spawn() -> Self {
        let secret = SecretKey::generate();
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret)
            .bind()
            .await
            .expect("bind endpoint");

        let gossip = Gossip::builder().spawn(endpoint.clone());

        let blob_store = MemStore::new();
        let blobs = BlobsProtocol::new(&blob_store, None);
        let docs = Docs::memory()
            .spawn(endpoint.clone(), (*blob_store).clone(), gossip.clone())
            .await
            .expect("spawn docs");

        let router = Router::builder(endpoint.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .accept(iroh_blobs::ALPN, blobs.clone())
            .accept(iroh_docs::ALPN, docs.clone())
            .spawn();

        endpoint.online().await;

        Self {
            _endpoint: endpoint,
            docs,
            blobs,
            router,
        }
    }

    async fn shutdown(self) {
        let _ = self.router.shutdown().await;
    }
}

/// Import a doc from a ticket and read its contents back, with the
/// joiner *not* given the host's `EndpointAddr` out of band. If
/// discovery fails this test hangs at `get_bytes`; the per-phase
/// timeouts convert that into a clear failure with a phase label.
#[tokio::test(flavor = "multi_thread")]
async fn doc_ticket_round_trips_without_manual_address_seeding_n0() {
    init_n0_tracing();

    let host = n0_phase(
        "host node spawn (bind + online + n0 publish)",
        N0Node::spawn(),
    )
    .await;
    let joiner = n0_phase(
        "joiner node spawn (bind + online + n0 publish)",
        N0Node::spawn(),
    )
    .await;

    let host_author = n0_phase("host author_create", host.docs.author_create())
        .await
        .expect("author_create");
    let host_doc = n0_phase("host doc create", host.docs.create())
        .await
        .expect("create doc");
    let key = b"path/hello.txt".to_vec();
    let value = Bytes::from_static(b"hello from host");
    n0_phase(
        "host set_bytes",
        host_doc.set_bytes(host_author, key.clone(), value.clone()),
    )
    .await
    .expect("set_bytes");

    let ticket = n0_phase(
        "host doc share",
        host_doc.share(
            iroh_docs::api::protocol::ShareMode::Write,
            iroh_docs::api::protocol::AddrInfoOptions::default(),
        ),
    )
    .await
    .expect("share doc");

    let joiner_doc = n0_phase(
        "joiner doc import (no manual addr seed)",
        joiner.docs.import(ticket),
    )
    .await
    .expect("import ticket");

    let mut events = joiner_doc.subscribe().await.expect("subscribe");

    let entry_hash = common::phase_budgeted(
        "joiner: wait for entry + ContentReady",
        N0_DISCOVERY_BUDGET,
        async {
            let mut entry_seen: Option<iroh_blobs::Hash> = None;
            loop {
                if entry_seen.is_none() {
                    let stream = joiner_doc.get_many(Query::all()).await.expect("get_many");
                    tokio::pin!(stream);
                    while let Some(entry) = stream.next().await {
                        let entry = entry.expect("entry ok");
                        if entry.key() == key.as_slice() {
                            entry_seen = Some(entry.content_hash());
                            break;
                        }
                    }
                }

                if let Some(want) = entry_seen {
                    while let Some(ev) = events.next().await {
                        let ev = ev.expect("live event");
                        if let LiveEvent::ContentReady { hash } = ev
                            && hash == want
                        {
                            return want;
                        }
                    }
                    panic!("doc event stream ended before ContentReady");
                }

                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        },
    )
    .await;

    let bytes = n0_phase(
        "joiner: get_bytes for downloaded entry",
        joiner.blobs.blobs().get_bytes(entry_hash),
    )
    .await
    .expect("get_bytes");

    assert_eq!(bytes.as_ref(), value.as_ref());

    drop(joiner_doc);
    drop(host_doc);
    n0_phase("host shutdown", host.shutdown()).await;
    n0_phase("joiner shutdown", joiner.shutdown()).await;
}

// =============================================================
// Deterministic sibling of the n0 smoke test.
//
// Same shape: two in-process iroh nodes, one writes a doc entry,
// shares the ticket, the other imports and reads the entry back. The
// difference is the discovery layer: `presets::Empty` + `Minimal` +
// hand-rolled chain pointed at an in-process `DnsPkarrServer`, so
// both endpoints write to and read from the same localhost server.
// No retry loop needed.
//
// Why both: this pair is the test-pyramid signal that distinguishes
// substrate regressions from n0 infrastructure flakes. If both fail,
// the bug is in our code or in iroh. If only the n0 sibling fails,
// it's an n0 infra flake (rate limit, DNS propagation window, etc.)
// and the substrate is fine.
// =============================================================

const PKARR_PHASE_BUDGET: Duration = Duration::from_secs(15);

async fn pkarr_phase<F, T>(name: &'static str, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    common::phase_budgeted(name, PKARR_PHASE_BUDGET, fut).await
}

/// One full iroh node bound against `presets::Empty + Minimal` plus
/// a hand-rolled chain pointed at the supplied `DnsPkarrServer`.
struct PkarrNode {
    endpoint: Endpoint,
    docs: Docs,
    blobs: BlobsProtocol,
    router: Router,
}

impl PkarrNode {
    async fn spawn(dns_pkarr: &Arc<DnsPkarrServer>) -> Self {
        let secret = SecretKey::generate();
        // `presets::Empty` + `Minimal` (crypto provider) + a hand-
        // rolled chain that mirrors `DnsPkarrServer::preset()` except
        // for the `AddrFilter`. The upstream preset defaults to
        // `relay_only`, which means the publisher publishes nothing
        // without a relay map configured — localhost direct UDP
        // doesn't qualify. We override to `ip_only` so the joiner can
        // dial the host's direct socket. iroh-docs's own `tests/util.rs`
        // paves over the same constraint by spinning a test relay; we
        // don't want a relay in the test.
        // `pkarr_url` / `endpoint_origin` became private in iroh 1.0
        // (only `pkarr_url()` is public). Own the origin via
        // `TEST_DNS_ORIGIN`, matching the `run_with_origin` the fixture
        // is constructed with below.
        let pkarr_publisher = PkarrPublisher::builder(dns_pkarr.pkarr_url().clone())
            .addr_filter(AddrFilter::ip_only());
        let endpoint = Endpoint::builder(presets::Empty)
            .secret_key(secret)
            .preset(presets::Minimal)
            .address_lookup(DnsAddressLookup::builder(TEST_DNS_ORIGIN.to_string()))
            .address_lookup(pkarr_publisher)
            .dns_resolver(dns_pkarr.dns_resolver())
            .bind()
            .await
            .expect("bind endpoint");

        let gossip = Gossip::builder().spawn(endpoint.clone());

        let blob_store = MemStore::new();
        let blobs = BlobsProtocol::new(&blob_store, None);
        let docs = Docs::memory()
            .spawn(endpoint.clone(), (*blob_store).clone(), gossip.clone())
            .await
            .expect("spawn docs");

        let router = Router::builder(endpoint.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .accept(iroh_blobs::ALPN, blobs.clone())
            .accept(iroh_docs::ALPN, docs.clone())
            .spawn();

        // No `endpoint.online()` — Minimal has no relay, the call
        // would never resolve. Direct UDP is bound by the time
        // `bind().await` returned, and the localhost pkarr server has
        // the publish synchronously. We gate on `on_endpoint` below
        // to be sure the record is queryable before the joiner dials.

        Self {
            endpoint,
            docs,
            blobs,
            router,
        }
    }

    async fn shutdown(self) {
        let _ = self.router.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn doc_ticket_round_trips_via_localhost_pkarr_dns() {
    init_n0_tracing();

    let dns_pkarr = Arc::new(
        DnsPkarrServer::run_with_origin(TEST_DNS_ORIGIN.to_string())
            .await
            .expect("DnsPkarrServer::run_with_origin"),
    );

    let host = pkarr_phase("host node spawn", PkarrNode::spawn(&dns_pkarr)).await;
    let joiner = pkarr_phase("joiner node spawn", PkarrNode::spawn(&dns_pkarr)).await;

    // Determinism gate: wait until both endpoints' pkarr records are
    // queryable on the localhost server before doing anything that
    // depends on cross-peer dialing. Without this, the joiner's first
    // import races the host's publish loop.
    let host_id = host.endpoint.id();
    let joiner_id = joiner.endpoint.id();
    pkarr_phase(
        "host pkarr-published",
        dns_pkarr.on_endpoint(&host_id, Duration::from_secs(5)),
    )
    .await
    .expect("host published");
    pkarr_phase(
        "joiner pkarr-published",
        dns_pkarr.on_endpoint(&joiner_id, Duration::from_secs(5)),
    )
    .await
    .expect("joiner published");

    let host_author = pkarr_phase("host author_create", host.docs.author_create())
        .await
        .expect("author_create");
    let host_doc = pkarr_phase("host doc create", host.docs.create())
        .await
        .expect("create doc");
    let key = b"path/hello.txt".to_vec();
    let value = Bytes::from_static(b"hello from host");
    pkarr_phase(
        "host set_bytes",
        host_doc.set_bytes(host_author, key.clone(), value.clone()),
    )
    .await
    .expect("set_bytes");

    let ticket = pkarr_phase(
        "host doc share",
        host_doc.share(
            iroh_docs::api::protocol::ShareMode::Write,
            iroh_docs::api::protocol::AddrInfoOptions::default(),
        ),
    )
    .await
    .expect("share doc");

    let joiner_doc = pkarr_phase("joiner doc import", joiner.docs.import(ticket))
        .await
        .expect("import ticket");

    let mut events = joiner_doc.subscribe().await.expect("subscribe");

    let entry_hash = pkarr_phase("joiner: wait for entry + ContentReady", async {
        let mut entry_seen: Option<iroh_blobs::Hash> = None;
        loop {
            if entry_seen.is_none() {
                let stream = joiner_doc.get_many(Query::all()).await.expect("get_many");
                tokio::pin!(stream);
                while let Some(entry) = stream.next().await {
                    let entry = entry.expect("entry ok");
                    if entry.key() == key.as_slice() {
                        entry_seen = Some(entry.content_hash());
                        break;
                    }
                }
            }
            if let Some(want) = entry_seen {
                while let Some(ev) = events.next().await {
                    let ev = ev.expect("live event");
                    if let LiveEvent::ContentReady { hash } = ev
                        && hash == want
                    {
                        return want;
                    }
                }
                panic!("doc event stream ended before ContentReady");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    let bytes = pkarr_phase(
        "joiner: get_bytes for downloaded entry",
        joiner.blobs.blobs().get_bytes(entry_hash),
    )
    .await
    .expect("get_bytes");

    assert_eq!(bytes.as_ref(), value.as_ref());

    drop(joiner_doc);
    drop(host_doc);
    pkarr_phase("host shutdown", host.shutdown()).await;
    pkarr_phase("joiner shutdown", joiner.shutdown()).await;
}

// =============================================================
// `Workspace::host_with` against `EndpointSetup::TestingUnreachableRelay`
// must return `WorkspaceError::RelayUnreachable` within a budget,
// NOT hang forever in `iroh::Endpoint::online`. Pre-fix the call in
// `WorkspaceNode::spawn` (node.rs) was a bare
// `endpoint.online().await` with no wrapper — this test hangs in
// that state until the harness-side timeout panics with the phase
// name (per `docs/diagnosing-flaky-tests.md`).
//
// The fixture uses RFC 5737 TEST-NET-1 (`192.0.2.1`), guaranteed
// unrouteable on the public internet. No external network access is
// required.
// =============================================================

/// Harness headroom for the work `Workspace::host_with` does *before*
/// the relay wait starts — chiefly `endpoint.bind()`. Measured
/// 2026-07-22 (daemon sibling test): bind takes ~4.7s typically but
/// 10–15s under full-suite parallelism (UDP/scheduler contention);
/// 30s covers 2x the worst observed 15s.
const BIND_ALLOWANCE: Duration = Duration::from_secs(30);

/// How long the harness gives `Workspace::host_with` to surface the
/// typed error. Against the unreachable-relay fixture the substrate's
/// internal `endpoint.online()` wait must run its *full*
/// [`artel_iroh_setup::HOME_RELAY_BUDGET`] before the typed error can
/// exist, so the harness budget is derived from that constant plus
/// [`BIND_ALLOWANCE`] — the two can't drift into racing each other.
/// (Previously this was an independent 40s magic number that lost the
/// race whenever bind exceeded ~10s.)
const RELAY_HARNESS_BUDGET: Duration =
    artel_iroh_setup::HOME_RELAY_BUDGET.saturating_add(BIND_ALLOWANCE);

#[tokio::test(flavor = "multi_thread")]
async fn host_with_unreachable_relay_returns_typed_error() {
    // Daemon stays local-only (no `iroh_key_path`) — only the
    // workspace endpoint exercises the relay path.
    let harness = common::LocalDaemon::spawn().await;
    let client = Client::connect(&harness.socket).await.unwrap();

    let ws_dir = tempfile::tempdir().unwrap();

    let config =
        WorkspaceConfig::default().with_endpoint_setup(EndpointSetup::TestingUnreachableRelay);

    // `Workspace::host_with` must return Err within the harness
    // budget. Pre-fix this hangs in `endpoint.online()` and the
    // harness panics with the phase name.
    let result = timeout(
        RELAY_HARNESS_BUDGET,
        Workspace::host_with(
            &client,
            "alice",
            ws_dir.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            config,
        ),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "phase hung past {RELAY_HARNESS_BUDGET:?}: \
             Workspace::host_with did not return Err within budget — \
             the timeout wrapper around endpoint.online() is missing"
        )
    });

    match result {
        Err(WorkspaceError::RelayUnreachable(budget)) => {
            assert_eq!(
                budget,
                artel_iroh_setup::HOME_RELAY_BUDGET,
                "the typed error must carry the substrate's internal \
                 `endpoint.online()` budget"
            );
        }
        Ok(_) => panic!(
            "expected WorkspaceError::RelayUnreachable, but host_with succeeded — \
             that's impossible against a TEST-NET-1 relay"
        ),
        Err(other) => panic!("expected WorkspaceError::RelayUnreachable, got {other:?}"),
    }

    drop(client);
    harness.stop().await;
}

// =============================================================
// bao-root == flat-blake3 equivalence (Tier A, no network).
//
// The hash-based `EchoGuard` (issue #33) compares hashes from two
// different derivations against each other:
// - publish side: a streamed flat `blake3::Hasher` over the file
//   (`echo_guard::hash_file`), and later the `import_file` outcome's
//   iroh hash for the same bytes;
// - apply side: `entry.content_hash()` — the bao tree root iroh
//   computed when the blob was added.
//
// The guard is sound only if iroh's content hash IS the flat blake3
// hash of the content (the bao outboard changes how the tree is
// *stored*, not the root). That's documented BLAKE3/bao behaviour,
// but it is load-bearing for echo suppression, so we pin it with a
// test instead of trusting it: if an iroh-blobs upgrade ever changed
// the hash derivation (domain separation, chunk-group tweak to the
// root, ...), echo suppression would silently break everywhere —
// this test turns that into a loud versioned failure.
// =============================================================

#[tokio::test]
#[allow(clippy::cast_possible_truncation)]
async fn iroh_content_hash_is_flat_blake3() {
    let store = MemStore::new();
    // Sizes straddling bao chunk (1 KiB) and chunk-group (16 KiB)
    // boundaries, where a tree-vs-flat divergence would show up:
    // sub-chunk, exact chunk, chunk+1, exact group, group+1, and a
    // multi-group size with a ragged tail.
    for size in [1usize, 1024, 1025, 16 * 1024, 16 * 1024 + 1, 123_457] {
        let bytes: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let tag = store.blobs().add_bytes(bytes.clone()).await.unwrap();
        let flat = blake3::hash(&bytes);
        assert_eq!(
            blake3::Hash::from(tag.hash),
            flat,
            "iroh content hash != flat blake3 at size {size} — \
             the hash-based EchoGuard's equivalence assumption is broken",
        );
    }
}
