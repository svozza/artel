//! Deterministic sibling of [`iroh_docs_smoke`].
//!
//! Same shape: two in-process iroh nodes, one writes a doc entry,
//! shares the ticket, the other imports and reads the entry back.
//! The difference is the discovery layer:
//!
//! - `iroh_docs_smoke.rs` runs against n0's production pkarr+DNS
//!   infrastructure (`presets::N0`) and includes a hand-rolled
//!   retry loop because iroh-docs gives up after one dial when
//!   propagation hasn't completed.
//! - This file runs against an in-process
//!   [`iroh::test_utils::DnsPkarrServer`] (`presets::Minimal +
//!   dns_pkarr.preset()`) — same code path as production minus
//!   the physical infrastructure. The propagation race goes away
//!   because both endpoints write to and read from the same
//!   localhost server; no retry loop needed.
//!
//! Why both: this pair is the test-pyramid signal that
//! distinguishes substrate regressions from n0 infrastructure
//! flakes. If both fail, the bug is in our code or in iroh. If
//! only the n0 sibling fails, it's an n0 infra flake (rate limit,
//! DNS propagation window, etc.) and the substrate is fine. The
//! pkarr sibling is the reliable canary; the n0 sibling is the
//! production-path acceptance test.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use iroh::address_lookup::{AddrFilter, DnsAddressLookup, PkarrPublisher};
// `AddrFilter` is used only in `Node::spawn`'s preset chain; keep
// the explicit import so the chain reads top-to-bottom.
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

/// Per-phase deadline. Tighter than the n0 sibling — the
/// localhost DNS+pkarr pair has no propagation window, so a hang
/// is a real bug rather than something to wait out.
const PHASE_BUDGET: Duration = Duration::from_secs(15);

async fn phase<F, T>(name: &'static str, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    eprintln!(">>> phase begin: {name} (budget {PHASE_BUDGET:?})");
    let res = timeout(PHASE_BUDGET, fut)
        .await
        .unwrap_or_else(|_| panic!("phase hung past {PHASE_BUDGET:?}: {name}"));
    eprintln!("<<< phase end:   {name}");
    res
}

/// Subscribe to tracing once per test process so iroh's discovery
/// surface is visible when the test fails. Honours `RUST_LOG`.
fn init_tracing() {
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

/// One full iroh node: Endpoint + Gossip + Docs/Blobs + Router.
/// Mirrors the [`iroh_docs_smoke`] sibling's `Node` exactly except
/// for the discovery preset.
struct Node {
    endpoint: Endpoint,
    docs: Docs,
    blobs: BlobsProtocol,
    router: Router,
}

impl Node {
    async fn spawn(dns_pkarr: &Arc<DnsPkarrServer>) -> Self {
        let secret = SecretKey::generate();
        // `presets::Empty` + `Minimal` (crypto provider) + a hand-
        // rolled chain that mirrors `DnsPkarrServer::preset()`
        // except for the `AddrFilter`. The upstream preset
        // defaults to `relay_only`, which means the publisher
        // publishes nothing without a relay map configured —
        // localhost direct UDP doesn't qualify. We override to
        // `ip_only` so the joiner can dial the host's direct
        // socket. iroh-docs's own `tests/util.rs` paves over the
        // same constraint by spinning a test relay; we don't want
        // a relay in the test.
        let pkarr_publisher =
            PkarrPublisher::builder(dns_pkarr.pkarr_url.clone()).addr_filter(AddrFilter::ip_only());
        let endpoint = Endpoint::builder(presets::Empty)
            .secret_key(secret)
            .preset(presets::Minimal)
            .address_lookup(DnsAddressLookup::builder(dns_pkarr.endpoint_origin.clone()))
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
        // `bind().await` returned, and the localhost pkarr server
        // has the publish synchronously. We gate on `on_endpoint`
        // below to be sure the record is queryable before the
        // joiner dials.

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
    init_tracing();

    let dns_pkarr = Arc::new(DnsPkarrServer::run().await.expect("DnsPkarrServer::run"));

    let host = phase("host node spawn", Node::spawn(&dns_pkarr)).await;
    let joiner = phase("joiner node spawn", Node::spawn(&dns_pkarr)).await;

    // Determinism gate: wait until both endpoints' pkarr records
    // are queryable on the localhost server before doing anything
    // that depends on cross-peer dialing. Without this, the
    // joiner's first import races the host's publish loop.
    let host_id = host.endpoint.id();
    let joiner_id = joiner.endpoint.id();
    phase(
        "host pkarr-published",
        dns_pkarr.on_endpoint(&host_id, Duration::from_secs(5)),
    )
    .await
    .expect("host published");
    phase(
        "joiner pkarr-published",
        dns_pkarr.on_endpoint(&joiner_id, Duration::from_secs(5)),
    )
    .await
    .expect("joiner published");

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

    // Joiner imports the ticket. Discovery resolves through the
    // shared localhost DnsPkarrServer; no retry loop needed
    // because the propagation race that drives the n0 sibling's
    // retry can't happen here.
    let joiner_doc = phase("joiner doc import", joiner.docs.import(ticket))
        .await
        .expect("import ticket");

    // Subscribe before waiting on the entry — `LiveEvent`s are
    // push-to-vec so a late subscriber misses pre-subscribe
    // events. Same shape as the n0 sibling.
    let mut events = joiner_doc.subscribe().await.expect("subscribe");

    // Wait for the entry's metadata to sync AND its bytes to
    // arrive locally. `ContentReady` is the load-bearing signal —
    // calling `get_bytes` before that races the in-progress
    // download and trips iroh-blobs' leaf-hash verifier.
    let entry_hash = phase("joiner: wait for entry + ContentReady", async {
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

    // Local read against the joiner's blob store; any error here
    // is a real iroh-blobs bug, not a sync race.
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
