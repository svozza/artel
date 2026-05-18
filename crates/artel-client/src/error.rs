//! Errors surfaced by the [`crate::Client`].
//!
//! Three layers fold into one:
//!
//! - Transport-level failures (broken pipe, framing, malformed bytes)
//!   from [`artel_protocol::transport::TransportError`].
//! - Wire-level errors from the daemon
//!   ([`artel_protocol::ProtocolError`]).
//! - Client-state errors that have no wire equivalent — connection
//!   closed mid-request, the reader task panicked, the daemon answered
//!   the wrong response variant, etc.

use artel_protocol::transport::TransportError;
use artel_protocol::{ProtocolError, Response};

use crate::spawn::SpawnError;

/// Anything the [`crate::Client`] may surface to its caller.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// IPC framing or socket error.
    #[error(transparent)]
    Transport(#[from] TransportError),

    /// The daemon returned a [`Response::Error`].
    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    /// The connection was closed before the request completed.
    ///
    /// Either the daemon dropped us, the reader task exited, or the
    /// underlying socket EOF'd between request and response.
    #[error("connection closed before response")]
    ConnectionClosed,

    /// The daemon answered with a [`Response`] variant the client did
    /// not expect for that request kind. Indicates a daemon bug or a
    /// protocol-version skew the handshake didn't catch.
    #[error("unexpected response variant: {0:?}")]
    UnexpectedResponse(Response),

    /// Sent on `Client::shutdown` or when the `Client` is dropped while
    /// callers still hold pending request futures.
    #[error("client is shutting down")]
    Shutdown,

    /// Auto-spawning a daemon via [`crate::Client::connect_or_spawn`]
    /// failed.
    #[error(transparent)]
    Spawn(#[from] SpawnError),
}
