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
//!   `get_bytes` hangs and the timeout converts that into a clear
//!   failure.

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
/// 30-second timeout converts that into a clear failure.
#[tokio::test(flavor = "multi_thread")]
async fn doc_ticket_round_trips_without_manual_address_seeding() {
    let host = Node::spawn().await;
    let joiner = Node::spawn().await;

    // Host writes one entry, shares the ticket.
    let host_author = host.docs.author_create().await.expect("author_create");
    let host_doc = host.docs.create().await.expect("create doc");
    let key = b"path/hello.txt".to_vec();
    let value = Bytes::from_static(b"hello from host");
    host_doc
        .set_bytes(host_author, key.clone(), value.clone())
        .await
        .expect("set_bytes");

    let ticket = host_doc
        .share(
            iroh_docs::api::protocol::ShareMode::Write,
            iroh_docs::api::protocol::AddrInfoOptions::default(),
        )
        .await
        .expect("share doc");

    // Joiner imports the ticket. NO `MemoryLookup`, NO
    // `add_endpoint_info` — the only addressing info is whatever the
    // ticket carries.
    let joiner_doc = joiner.docs.import(ticket).await.expect("import ticket");

    // Wait for sync. iroh-docs syncs on import; the entry should land
    // shortly after. Poll the entry list until it shows up or we time
    // out.
    let bytes = timeout(Duration::from_secs(30), async {
        loop {
            let stream = joiner_doc.get_many(Query::all()).await.expect("get_many");
            tokio::pin!(stream);
            while let Some(entry) = stream.next().await {
                let entry = entry.expect("entry ok");
                if entry.key() == key.as_slice() {
                    let bytes = joiner
                        .blobs
                        .blobs()
                        .get_bytes(entry.content_hash())
                        .await
                        .expect("get_bytes");
                    return bytes;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("joiner never received entry — DocTicket discovery insufficient?");

    assert_eq!(bytes.as_ref(), value.as_ref());

    drop(joiner_doc);
    drop(host_doc);
    host.shutdown().await;
    joiner.shutdown().await;
}
