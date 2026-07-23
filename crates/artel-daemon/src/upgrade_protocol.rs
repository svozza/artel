//! Receiving side of the direct-stream delivery protocol.
//!
//! The host daemon sends a [`DeliveryFrame`] over a dedicated QUIC
//! stream (ALPN [`UPGRADE_ALPN`]). This module implements the
//! [`ProtocolHandler`] that accepts such connections, validates the
//! frame, dispatches by payload kind — `Secret` → the session's
//! upgrade event, `WorkspaceTicket` → persist + synthetic
//! `TICKET_ACTION` System message, `Downgrade` → synthetic
//! `DOWNGRADE_ACTION` message, `Rotate` → synthetic `ROTATE_ACTION`
//! message — and returns a 1-byte ACK.

// Crate-private module: pair `unreachable_pub` with the
// crate-visibility lint so they stop fighting (see memory).
#![allow(clippy::redundant_pub_crate)]

use std::sync::Arc;
use std::time::Duration;

use artel_protocol::PeerId;
use artel_protocol::upgrade::{DeliveryFrame, MAX_DELIVERY_FRAME, UPGRADE_ACK, UPGRADE_ALPN};
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::warn;

use crate::session::Registry;

/// Maximum number of inbound upgrade handshakes accepted concurrently.
///
/// Each in-flight `accept` reads a caller-controlled, up-to-
/// [`MAX_DELIVERY_FRAME`] (64 KiB) body and allocates a buffer for it
/// BEFORE authorization runs, so without a cap a reachable peer could
/// pin unbounded heap + task slots by opening many connections (M5).
/// The genuine traffic is rare (admission / RW-promotion deliveries),
/// so a small cap is ample; excess connections are dropped and the
/// host re-delivers on the next re-announce / republish.
const MAX_CONCURRENT_ACCEPTS: usize = 32;

/// Whole-handshake deadline for one inbound upgrade delivery (read the
/// length + body, decode, dispatch, ACK). Bounds a peer that opens a
/// connection and then stalls mid-frame, which would otherwise hold an
/// accept slot (and its buffer) indefinitely.
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(10);

/// Protocol handler registered on the daemon's [`iroh::protocol::Router`]
/// under [`UPGRADE_ALPN`]. Accepts inbound direct-stream upgrade
/// deliveries from a session's host.
#[derive(Debug, Clone)]
pub(crate) struct UpgradeProtocol {
    registry: Arc<Registry>,
    /// Bounds concurrent in-flight handshakes (M5). Shared across the
    /// handler's clones (the router clones it per accepted connection),
    /// so the cap is process-wide for this ALPN.
    accept_permits: Arc<Semaphore>,
}

impl UpgradeProtocol {
    pub(crate) fn new(registry: Arc<Registry>) -> Self {
        Self {
            registry,
            accept_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_ACCEPTS)),
        }
    }

    /// The ALPN this handler is registered under.
    pub(crate) const fn alpn() -> &'static [u8] {
        UPGRADE_ALPN
    }

    /// Try to claim one of the [`MAX_CONCURRENT_ACCEPTS`] admission
    /// slots. `None` when the cap is reached — the caller drops the
    /// connection without committing a read buffer. The permit frees the
    /// slot on drop (when `accept` returns).
    fn try_begin_accept(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.accept_permits).try_acquire_owned().ok()
    }
}

impl ProtocolHandler for UpgradeProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        // Bound concurrent in-flight handshakes (M5): claim a slot
        // before committing any read buffer. If the cap is reached, drop
        // the connection — the host re-delivers on its next re-announce
        // / republish, so a refused delivery self-heals. The permit is
        // held for the whole handshake and frees on return.
        let Some(_permit) = self.try_begin_accept() else {
            warn!("upgrade_protocol: concurrent-accept cap reached; dropping connection");
            return Err(AcceptError::from_err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "upgrade accept cap reached",
            )));
        };

        // Bound the whole handshake so a peer that opens a connection and
        // then stalls mid-frame can't hold the slot + buffer forever.
        tokio::time::timeout(ACCEPT_TIMEOUT, self.handle_accept(connection))
            .await
            .map_err(|_| {
                warn!("upgrade_protocol: handshake timed out");
                AcceptError::from_err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "upgrade handshake timed out",
                ))
            })?
    }
}

impl UpgradeProtocol {
    /// The body of [`ProtocolHandler::accept`], split out so the caller
    /// can wrap it in a concurrency permit + timeout (M5).
    async fn handle_accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote_id = connection.remote_id();
        let remote_peer = PeerId::from_bytes(*remote_id.as_bytes());

        let (mut send, mut recv) = connection.accept_bi().await.map_err(|e| {
            warn!(error = %e, "upgrade_protocol: accept_bi failed");
            AcceptError::from_err(std::io::Error::other(e.to_string()))
        })?;

        // Read length-prefixed frame (4-byte LE length + postcard payload).
        let mut len_buf = [0u8; 4];
        recv.read_exact(&mut len_buf).await.map_err(|e| {
            warn!(error = %e, "upgrade_protocol: failed to read frame length");
            AcceptError::from_err(std::io::Error::other(e.to_string()))
        })?;
        let len = u32::from_le_bytes(len_buf) as usize;

        if len > MAX_DELIVERY_FRAME {
            warn!(len, "upgrade_protocol: frame length exceeds cap");
            return Err(AcceptError::from_err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "delivery frame too large",
            )));
        }

        let mut buf = vec![0u8; len];
        recv.read_exact(&mut buf).await.map_err(|e| {
            warn!(error = %e, "upgrade_protocol: failed to read frame body");
            AcceptError::from_err(std::io::Error::other(e.to_string()))
        })?;

        let frame: DeliveryFrame = postcard::from_bytes(&buf).map_err(|e| {
            warn!(error = %e, "upgrade_protocol: failed to decode DeliveryFrame");
            AcceptError::from_err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            ))
        })?;

        // Dispatch into the registry, which validates that the
        // session exists, is Remote, and that remote_peer is the
        // session's host — for both payload kinds.
        let result = match frame {
            DeliveryFrame::Secret(upgrade) => {
                self.registry
                    .emit_upgrade(upgrade.session_id, remote_peer, upgrade.namespace_secret)
                    .await
            }
            DeliveryFrame::WorkspaceTicket {
                session_id,
                envelope_bytes,
            } => {
                self.registry
                    .emit_workspace_ticket(session_id, remote_peer, envelope_bytes)
                    .await
            }
            DeliveryFrame::Downgrade { session_id } => {
                self.registry.emit_downgrade(session_id, remote_peer).await
            }
            DeliveryFrame::Rotate {
                session_id,
                namespace_epoch,
                doc_ticket,
            } => {
                self.registry
                    .emit_rotate(session_id, remote_peer, namespace_epoch, doc_ticket)
                    .await
            }
        };
        result.map_err(|e| {
            warn!(error = %e, "upgrade_protocol: delivery rejected");
            AcceptError::from_err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                e.to_string(),
            ))
        })?;

        // ACK: single byte back to the host.
        send.write_all(&[UPGRADE_ACK]).await.map_err(|e| {
            warn!(error = %e, "upgrade_protocol: failed to send ACK");
            AcceptError::from_err(std::io::Error::other(e.to_string()))
        })?;
        send.finish()
            .map_err(|e| AcceptError::from_err(std::io::Error::other(e.to_string())))?;
        // Wait until the remote has received all data. Without this,
        // returning from `accept` drops the Connection and the ACK byte
        // may not reach the sender.
        send.stopped().await.ok();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proto() -> UpgradeProtocol {
        // A registry with no store ops exercised — we only test the
        // admission cap, not delivery.
        let store: crate::store::DynStore = Arc::new(crate::store::MemoryStore::new());
        let registry = Arc::new(Registry::new(PeerId::from_bytes([9; 32]), store));
        UpgradeProtocol::new(registry)
    }

    #[test]
    fn concurrent_accept_cap_bounds_in_flight_handshakes() {
        // M5: an unauthenticated peer must not be able to pin unbounded
        // concurrent accept tasks (each allocating up to MAX_DELIVERY_FRAME
        // before authorization runs). `try_begin_accept` hands out at most
        // MAX_CONCURRENT_ACCEPTS permits; beyond that it returns None so
        // accept() drops the connection instead of committing resources.
        let p = proto();
        let held: Vec<_> = (0..MAX_CONCURRENT_ACCEPTS)
            .map(|_| {
                p.try_begin_accept()
                    .expect("permits up to the cap must be granted")
            })
            .collect();
        assert_eq!(held.len(), MAX_CONCURRENT_ACCEPTS);
        assert!(
            p.try_begin_accept().is_none(),
            "an accept beyond the cap must be refused",
        );

        // Releasing a permit frees a slot for the next accept.
        drop(held);
        assert!(
            p.try_begin_accept().is_some(),
            "a freed permit must be reusable",
        );
    }

    // ---- end-to-end accept path over a real hermetic iroh connection ----
    //
    // The cap test above only exercises `try_begin_accept`; the actual
    // `ProtocolHandler::accept` / `handle_accept` body (frame read,
    // length-cap rejection, postcard decode, dispatch, ACK) needs a
    // live `iroh::endpoint::Connection` and was previously untested at
    // this layer — only reachable indirectly through a full
    // `Daemon::start`. These tests dial `UpgradeProtocol` directly over
    // two hermetic (`EndpointSetup::Testing`) endpoints sharing a
    // localhost `DnsPkarrServer`, mirroring the pattern in
    // `server.rs`'s own iroh test module and
    // `artel-fs/tests/iroh_internals.rs`'s `PkarrNode`.
    #[cfg(all(test, feature = "iroh", feature = "test-utils"))]
    mod accept_path {
        use std::time::Duration;

        use artel_protocol::ticket::WireEndpointAddr;
        use artel_protocol::upgrade::{DeliveryFrame, UPGRADE_ACK, UPGRADE_ALPN};
        use artel_protocol::{Seq, SessionId};
        use iroh::Endpoint;
        use iroh::protocol::Router;
        use iroh::test_utils::DnsPkarrServer;

        use super::*;
        use crate::store::{SessionKind, SessionRecord};

        /// One hermetic endpoint bound against `EndpointSetup::Testing`,
        /// publishing/resolving via the supplied localhost
        /// `DnsPkarrServer`. Mirrors `PkarrNode::spawn` in
        /// `artel-fs/tests/iroh_internals.rs`, minus the docs/blobs/gossip
        /// protocols this test doesn't need.
        async fn bind_endpoint(dns_pkarr: &Arc<DnsPkarrServer>) -> Endpoint {
            let setup = artel_iroh_setup::EndpointSetup::Testing {
                dns_pkarr: Arc::clone(dns_pkarr),
            };
            setup
                .apply(Endpoint::builder(iroh::endpoint::presets::Empty))
                .bind()
                .await
                .expect("bind hermetic endpoint")
        }

        /// Frame a `DeliveryFrame` exactly as `deliver_frame_inner`
        /// (`server.rs`) does: postcard body, 4-byte LE length prefix.
        fn frame_bytes(frame: &DeliveryFrame) -> Vec<u8> {
            let body = postcard::to_allocvec(frame).expect("encode DeliveryFrame");
            let len = u32::try_from(body.len())
                .expect("test frame fits in u32")
                .to_le_bytes();
            let mut out = Vec::with_capacity(4 + body.len());
            out.extend_from_slice(&len);
            out.extend_from_slice(&body);
            out
        }

        /// Registry pre-seeded with a `Remote` session whose host is
        /// `host_peer` — the minimal state `handle_accept`'s dispatch
        /// (via `lock_remote_session_from_host`) requires to accept a
        /// delivery instead of rejecting it as an unknown/non-host
        /// sender. Mirrors `session.rs`'s `remote_mirror_registry`.
        async fn remote_registry_with_host(
            daemon_peer: PeerId,
            host_peer: PeerId,
            session_id: SessionId,
        ) -> Registry {
            let store: crate::store::DynStore = Arc::new(crate::store::MemoryStore::new());
            let record = SessionRecord {
                id: session_id,
                host: host_peer,
                members: std::collections::HashSet::from([host_peer, daemon_peer]),
                head: Seq::ZERO,
                log: Vec::new(),
                kind: SessionKind::Remote,
                host_epoch: 0,
                tickets: Vec::new(),
                workspace_ticket: None,
            };
            store.create(&record).await.expect("seed session record");
            Registry::load(
                daemon_peer,
                WireEndpointAddr::id_only(daemon_peer),
                store,
                None,
                None,
                None,
            )
            .await
            .expect("load registry")
        }

        /// A `Downgrade` delivery from the recognized host is accepted:
        /// the receiver ACKs and the registry emits the synthetic
        /// `DOWNGRADE_ACTION` event — pinning the full accept path (frame
        /// read, decode, dispatch, ACK) rather than just the admission
        /// cap.
        #[tokio::test(flavor = "multi_thread")]
        async fn downgrade_delivery_from_host_is_accepted_and_acked() {
            let dns_pkarr = Arc::new(
                DnsPkarrServer::run_with_origin(artel_iroh_setup::TEST_DNS_ORIGIN.to_string())
                    .await
                    .expect("DnsPkarrServer::run_with_origin"),
            );

            let host_endpoint = bind_endpoint(&dns_pkarr).await;
            let target_endpoint = bind_endpoint(&dns_pkarr).await;

            let host_peer = PeerId::from_bytes(*host_endpoint.id().as_bytes());
            let target_peer = PeerId::from_bytes(*target_endpoint.id().as_bytes());
            let session_id = SessionId::new_random();

            let registry =
                Arc::new(remote_registry_with_host(target_peer, host_peer, session_id).await);
            let mut sub = registry
                .subscribe(session_id, None)
                .await
                .expect("subscribe before delivery");

            let router = Router::builder(target_endpoint.clone())
                .accept(UpgradeProtocol::alpn(), UpgradeProtocol::new(registry))
                .spawn();

            // Determinism gate: wait for both pkarr records to be
            // queryable before dialing, or the connect races the publish.
            dns_pkarr
                .on_endpoint(&host_endpoint.id(), Duration::from_secs(5))
                .await
                .expect("host published");
            dns_pkarr
                .on_endpoint(&target_endpoint.id(), Duration::from_secs(5))
                .await
                .expect("target published");

            let connection = host_endpoint
                .connect(target_endpoint.id(), UPGRADE_ALPN)
                .await
                .expect("connect to target");
            let (mut send, mut recv) = connection.open_bi().await.expect("open_bi");

            let frame = DeliveryFrame::Downgrade { session_id };
            send.write_all(&frame_bytes(&frame))
                .await
                .expect("write frame");
            send.finish().expect("finish send stream");

            let mut ack = [0u8; 1];
            recv.read_exact(&mut ack).await.expect("read ACK");
            assert_eq!(ack[0], UPGRADE_ACK, "receiver must ACK a valid delivery");

            let ev = tokio::time::timeout(Duration::from_secs(1), sub.events.recv())
                .await
                .expect("event within timeout")
                .expect("channel open");
            match ev {
                artel_protocol::Event::Message { session, message } => {
                    assert_eq!(session, session_id);
                    assert_eq!(message.action, artel_protocol::DOWNGRADE_ACTION);
                }
                other => panic!("expected Message, got {other:?}"),
            }

            router.shutdown().await.ok();
            host_endpoint.close().await;
        }

        /// A delivery whose sender is not the session's recorded host is
        /// rejected: `handle_accept` maps the registry's error into an
        /// `AcceptError`, closing the stream without an ACK — the
        /// spoofing-guard half of the same dispatch path exercised above.
        #[tokio::test(flavor = "multi_thread")]
        async fn downgrade_delivery_from_non_host_is_rejected() {
            let dns_pkarr = Arc::new(
                DnsPkarrServer::run_with_origin(artel_iroh_setup::TEST_DNS_ORIGIN.to_string())
                    .await
                    .expect("DnsPkarrServer::run_with_origin"),
            );

            let real_host_endpoint = bind_endpoint(&dns_pkarr).await;
            let imposter_endpoint = bind_endpoint(&dns_pkarr).await;
            let target_endpoint = bind_endpoint(&dns_pkarr).await;

            // Session records the *real* host, not the imposter dialing in.
            let real_host_peer = PeerId::from_bytes(*real_host_endpoint.id().as_bytes());
            let target_peer = PeerId::from_bytes(*target_endpoint.id().as_bytes());
            let session_id = SessionId::new_random();

            let registry =
                Arc::new(remote_registry_with_host(target_peer, real_host_peer, session_id).await);

            let router = Router::builder(target_endpoint.clone())
                .accept(UpgradeProtocol::alpn(), UpgradeProtocol::new(registry))
                .spawn();

            dns_pkarr
                .on_endpoint(&imposter_endpoint.id(), Duration::from_secs(5))
                .await
                .expect("imposter published");
            dns_pkarr
                .on_endpoint(&target_endpoint.id(), Duration::from_secs(5))
                .await
                .expect("target published");

            let connection = imposter_endpoint
                .connect(target_endpoint.id(), UPGRADE_ALPN)
                .await
                .expect("connect to target");
            let (mut send, mut recv) = connection.open_bi().await.expect("open_bi");

            let frame = DeliveryFrame::Downgrade { session_id };
            send.write_all(&frame_bytes(&frame))
                .await
                .expect("write frame");
            send.finish().expect("finish send stream");

            // No ACK: the handler rejects before writing one, and the
            // stream ends without the single ACK byte ever arriving.
            let mut ack = [0u8; 1];
            let result = recv.read_exact(&mut ack).await;
            assert!(
                result.is_err(),
                "non-host sender must not receive an ACK, got {result:?}",
            );

            router.shutdown().await.ok();
            imposter_endpoint.close().await;
        }
    }
}
