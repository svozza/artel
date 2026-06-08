//! Receiving side of the direct-stream upgrade protocol.
//!
//! The host daemon sends an [`UpgradeFrame`] over a dedicated QUIC
//! stream (ALPN [`UPGRADE_ALPN`]). This module implements the
//! [`ProtocolHandler`] that accepts such connections, validates the
//! frame, emits a synthetic [`Event::Message`] into the session's
//! broadcast channel (so the existing `cap_listener` picks it up),
//! and returns a 1-byte ACK.

// Crate-private module: pair `unreachable_pub` with the
// crate-visibility lint so they stop fighting (see memory).
#![allow(clippy::redundant_pub_crate)]

use std::sync::Arc;

use artel_protocol::upgrade::{UPGRADE_ACK, UPGRADE_ALPN, UpgradeFrame};
use artel_protocol::PeerId;
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use tracing::warn;

use crate::session::Registry;

/// Protocol handler registered on the daemon's [`iroh::protocol::Router`]
/// under [`UPGRADE_ALPN`]. Accepts inbound direct-stream upgrade
/// deliveries from a session's host.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Wired into the Router in Phase 4.
pub(crate) struct UpgradeProtocol {
    registry: Arc<Registry>,
}

#[allow(dead_code)] // Wired into the Router in Phase 4.
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

        // Sanity cap: an UpgradeFrame is ~48 bytes; reject anything
        // unreasonably large to avoid allocating on attacker input.
        if len > 1024 {
            warn!(len, "upgrade_protocol: frame length exceeds cap");
            return Err(AcceptError::from_err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "upgrade frame too large",
            )));
        }

        let mut buf = vec![0u8; len];
        recv.read_exact(&mut buf).await.map_err(|e| {
            warn!(error = %e, "upgrade_protocol: failed to read frame body");
            AcceptError::from_err(std::io::Error::other(e.to_string()))
        })?;

        let frame: UpgradeFrame = postcard::from_bytes(&buf).map_err(|e| {
            warn!(error = %e, "upgrade_protocol: failed to decode UpgradeFrame");
            AcceptError::from_err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            ))
        })?;

        // Emit the upgrade into the session's event channel. The
        // registry validates that the session exists, is Remote, and
        // that remote_peer is the host.
        self.registry
            .emit_upgrade(frame.session_id, remote_peer, frame.namespace_secret)
            .await
            .map_err(|e| {
                warn!(error = %e, session = %frame.session_id, "upgrade_protocol: emit_upgrade rejected");
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
        let _ = send.finish();

        Ok(())
    }
}
