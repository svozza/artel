//! Async IPC transport for the artel wire protocol.
//!
//! Available only with the `tokio` feature. Provides:
//!
//! - [`WireMessageCodec`]: a `tokio_util` codec that encodes and decodes
//!   [`crate::WireMessage`] frames as length-prefixed postcard.
//! - [`Framed`]: a typed [`Stream`] / [`Sink`] adapter built on the codec.
//!
//! The framing wraps `tokio_util::codec::LengthDelimitedCodec`, which
//! uses a 4-byte big-endian length prefix. The maximum frame size is
//! [`MAX_FRAME_SIZE`] bytes; oversized frames are rejected as
//! [`TransportError::FrameTooLarge`] before the payload is even read.
//!
//! [`Stream`]: futures_util::Stream
//! [`Sink`]: futures_util::Sink

mod codec;
mod framed;

pub use codec::{MAX_FRAME_SIZE, TransportError, WireMessageCodec};
pub use framed::{Framed, new};
