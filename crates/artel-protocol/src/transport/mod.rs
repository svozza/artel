//! Async IPC transport for the artel wire protocol.
//!
//! Available only with the `tokio` feature. Provides:
//!
//! - [`WireMessageCodec`]: a `tokio_util` codec that encodes and decodes
//!   [`crate::WireMessage`] frames as length-prefixed postcard.
//! - [`Framed`]: a typed [`Stream`] / [`Sink`] adapter built on the codec.
//! - [`server::Listener`] and [`client::connect`]: Unix-socket
//!   server/client glue so the daemon and clients can talk over an
//!   actual local socket.
//! - [`path`] helpers for resolving the per-user state directory and
//!   socket path.
//!
//! The framing wraps `tokio_util::codec::LengthDelimitedCodec`, which
//! uses a 4-byte big-endian length prefix. The maximum frame size is
//! [`MAX_FRAME_SIZE`] bytes; oversized frames are rejected as
//! [`TransportError::FrameTooLarge`] before the payload is even read.
//!
//! # Platform support
//!
//! The socket layer ([`server`], [`client`], [`path`]) is Unix-only —
//! Linux and macOS. Windows named-pipe support is deliberately
//! deferred. The codec and `Framed` wrapper are platform-agnostic and
//! work over any `AsyncRead + AsyncWrite`.
//!
//! [`Stream`]: futures_util::Stream
//! [`Sink`]: futures_util::Sink

#[cfg(not(unix))]
compile_error!("the artel-protocol `tokio` feature only supports Unix targets (Linux, macOS)");

mod codec;
mod framed;

#[cfg(unix)]
pub mod client;
#[cfg(unix)]
pub mod path;
#[cfg(unix)]
pub mod server;

pub use codec::{MAX_FRAME_SIZE, TransportError, WireMessageCodec};
pub use framed::{Framed, new};
