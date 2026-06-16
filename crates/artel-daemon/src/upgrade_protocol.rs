//! Receiving side of the direct-stream delivery protocol.
//!
//! The host daemon sends a [`DeliveryFrame`] over a dedicated QUIC
//! stream (ALPN [`UPGRADE_ALPN`]). This module implements the
//! [`ProtocolHandler`] that accepts such connections, validates the
//! frame, dispatches by payload kind — `Secret` → the session's
//! upgrade event, `WorkspaceTicket` → persist + synthetic
//! `TICKET_ACTION` System message — and returns a 1-byte ACK.

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
}
