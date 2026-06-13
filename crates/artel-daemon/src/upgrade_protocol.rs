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

use artel_protocol::PeerId;
use artel_protocol::upgrade::{DeliveryFrame, MAX_DELIVERY_FRAME, UPGRADE_ACK, UPGRADE_ALPN};
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use tracing::warn;

use crate::session::Registry;

/// Protocol handler registered on the daemon's [`iroh::protocol::Router`]
/// under [`UPGRADE_ALPN`]. Accepts inbound direct-stream upgrade
/// deliveries from a session's host.
#[derive(Debug, Clone)]
pub(crate) struct UpgradeProtocol {
    registry: Arc<Registry>,
}

impl UpgradeProtocol {
    pub(crate) const fn new(registry: Arc<Registry>) -> Self {
        Self { registry }
    }

    /// The ALPN this handler is registered under.
    pub(crate) const fn alpn() -> &'static [u8] {
        UPGRADE_ALPN
    }
}

impl ProtocolHandler for UpgradeProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
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
