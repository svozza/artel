//! iroh-docs / iroh-blobs version-compat smoke test.
//!
//! Proves the version matrix in `[workspace.dependencies]` actually
//! compiles + works together, and that a `DocTicket` carries enough
//! `EndpointAddr` info on its own for the joiner to dial the host
//! without any out-of-band address-book seeding.
//!
//! Two in-process iroh nodes:
//! - host: spawns Endpoint + Gossip + Docs/Blobs, creates a Doc,
//!   writes one entry, shares the ticket.
//! - joiner: spawns Endpoint + Gossip + Docs/Blobs (NO address-lookup
//!   override, NO manual `add_endpoint_info`), imports the ticket,
//!   reads the entry back. If discovery from the ticket alone fails,
//!   the per-phase timeouts convert that into a clear failure
//!   pinpointing which step hung.
//!
//! Sync-vs-blob ordering: doc metadata syncs before blob bytes are
//! downloaded. iroh-docs surfaces this via the
//! [`LiveEvent::ContentReady`] event — fired once a blob's bytes
//! are actually present in the local blob store. Calling
//! `get_bytes` before that races against the in-progress download
//! and trips iroh-blobs' leaf-hash verifier with
//! `DecodeError::LeafHashMismatch`. The test subscribes before
//! waiting for the entry, then gates the read on `ContentReady`.
//!
//! Sync-retry on dial failure: iroh-docs's `Docs::import` triggers
//! a single dial attempt. If the host's pkarr publish hasn't
//! propagated to `dns.iroh.link` yet (484 ms apart in one observed
//! flake), the DNS TXT lookup returns empty and iroh-docs emits
//! `sync failed err="Failed to establish connection"` and **does
//! not retry**. Production consumers would normally have a
//! reconcile-on-reconnect loop or a periodic re-`start_sync`. This
//! test does NOT — the joiner's entry-poll loop simply sleeps 50 ms
//! between `Doc::get_one` calls and relies on iroh-docs eventually
//! noticing the host once pkarr propagates. That means a slow
//! propagation can still trip `DISCOVERY_BUDGET`; the test is best
//! read as a happy-path canary, not a hardened retry property.

// `phase()` wrapping `Node::spawn()` produces a 21 KiB future
// because of how iroh-docs' `Docs::spawn` cascades; the test runs
// fine, the lint is just noisy on test code.
#![allow(clippy::large_futures)]

use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, SecretKey};
use iroh_blobs::BlobsProtocol;
use iroh_blobs::store::mem::MemStore;
use iroh_docs::protocol::Docs;
use iroh_docs::store::Query;
use iroh_gossip::net::Gossip;
use tokio::time::timeout;

/// Per-phase deadline. Generous so a slow-but-working n0 doesn't
/// trip the bound; tight enough that a stuck phase fails fast with
/// a clear "phase X hung" message instead of waiting the full
/// outer budget on a single combined `timeout`.
const PHASE_BUDGET: Duration = Duration::from_secs(20);

/// First-contact phase budget. Discovery + reconcile + blob fetch
/// is the only phase that goes over a real network. Tight enough
/// that we observe flakes rather than mask them; generous enough
/// that a healthy run isn't gambling on a fast pkarr round-trip.
const DISCOVERY_BUDGET: Duration = Duration::from_secs(30);

async fn phase<F, T>(name: &'static str, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    phase_budgeted(name, PHASE_BUDGET, fut).await
}

async fn phase_budgeted<F, T>(name: &'static str, budget: Duration, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    eprintln!(">>> phase begin: {name} (budget {budget:?})");
    let res = timeout(budget, fut)
        .await
        .unwrap_or_else(|_| panic!("phase hung past {budget:?}: {name}"));
    eprintln!("<<< phase end:   {name}");
    res
}

/// Subscribe to tracing once per test process so iroh's discovery /
/// relay / gossip / docs / blobs surfaces are visible when the test
/// fails. Honours `RUST_LOG`; defaults are deliberately wide so a
/// captured failing log surfaces every layer that could plausibly
/// be the cause of a hang in the entry-arrival phase. Narrow via
/// `RUST_LOG=...` if you want to isolate a specific subsystem.
fn init_tracing() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| {
            // Coarse default. Pull individual crate filters up to
            // `debug` when investigating a flake; `trace` for
            // discovery is the next escalation.
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

/// One full iroh node: Endpoint + Gossip + Docs/Blobs + Router.
///
/// Mirrors the shape an `artel-fs::Workspace` will spin up later. Holding
/// the `Router` keeps everything alive; dropping the struct shuts it
/// down via the `Drop` chain.
struct Node {
    _endpoint: Endpoint,
    docs: Docs,
    blobs: BlobsProtocol,
    router: Router,
}

impl Node {
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

/// Import a doc from a ticket and read its contents back.
///
/// The joiner is *not* given the host's `EndpointAddr` out of
/// band. If discovery fails this test hangs at `get_bytes`; the
/// per-phase timeouts (see [`phase`]) convert that into a clear
/// failure with a phase label so the cause is visible without
/// having to bisect the test body.
#[tokio::test(flavor = "multi_thread")]
async fn doc_ticket_round_trips_without_manual_address_seeding() {
    init_tracing();

    let host = phase(
        "host node spawn (bind + online + n0 publish)",
        Node::spawn(),
    )
    .await;
    let joiner = phase(
        "joiner node spawn (bind + online + n0 publish)",
        Node::spawn(),
    )
    .await;

    // Host writes one entry, shares the ticket.
    let host_author = phase("host author_create", host.docs.author_create())
        .await
        .expect("author_create");
    let host_doc = phase("host doc create", host.docs.create())
        .await
        .expect("create doc");
    let key = b"path/hello.txt".to_vec();
    let value = Bytes::from_static(b"hello from host");
    phase(
        "host set_bytes",
        host_doc.set_bytes(host_author, key.clone(), value.clone()),
    )
    .await
    .expect("set_bytes");

    let ticket = phase(
        "host doc share",
        host_doc.share(
            iroh_docs::api::protocol::ShareMode::Write,
            iroh_docs::api::protocol::AddrInfoOptions::default(),
        ),
    )
    .await
    .expect("share doc");

    // Joiner imports the ticket. NO `MemoryLookup`, NO
    // `add_endpoint_info` — the only addressing info is whatever the
    // ticket carries. `import` does not block on sync; it just adds
    // the namespace to the joiner's docs store and kicks off a sync
    // attempt. Discovery / dial + initial reconcile happens
    // asynchronously after this returns.
    let joiner_doc = phase(
        "joiner doc import (no manual addr seed)",
        joiner.docs.import(ticket),
    )
    .await
    .expect("import ticket");

    // Subscribe to the doc's live event stream BEFORE we go looking
    // for the entry. We need the [`LiveEvent::ContentReady`] signal to
    // know when the blob bytes are actually available locally —
    // calling `get_bytes` before that races against an in-progress
    // download and trips the iroh-blobs leaf-hash verifier with
    // `LeafHashMismatch`. Subscribing late could miss the event;
    // doing it before the entry-wait phase is the safe order.
    let mut events = joiner_doc.subscribe().await.expect("subscribe");

    // Wait for the entry to appear (doc metadata sync) AND for its
    // bytes to be downloaded (`ContentReady`). Doing both together
    // means a single phase covers the discovery + reconcile + blob
    // fetch round trip, and the `get_bytes` that follows is purely
    // a local read.
    let entry_hash = phase_budgeted(
        "joiner: wait for entry + ContentReady",
        DISCOVERY_BUDGET,
        async {
            let mut entry_seen: Option<iroh_blobs::Hash> = None;
            loop {
                // Cheap entry-list scan first so we know the metadata is
                // synced; iroh-docs delivers metadata before the
                // ContentReady fires.
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

                // Then wait for ContentReady matching that hash. Other
                // live events (NeighborUp, SyncFinished, …) are dropped.
                if let Some(want) = entry_seen {
                    while let Some(ev) = events.next().await {
                        let ev = ev.expect("live event");
                        if let iroh_docs::engine::LiveEvent::ContentReady { hash } = ev
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

    // Now `get_bytes` is a local read against the joiner's blob
    // store. Any error here is a real iroh-blobs bug, not a sync race.
    let bytes = phase(
        "joiner: get_bytes for downloaded entry",
        joiner.blobs.blobs().get_bytes(entry_hash),
    )
    .await
    .expect("get_bytes");

    assert_eq!(bytes.as_ref(), value.as_ref());

    drop(joiner_doc);
    drop(host_doc);
    phase("host shutdown", host.shutdown()).await;
    phase("joiner shutdown", joiner.shutdown()).await;
}
