//! Length-prefixed postcard codec for [`WireMessage`].

use std::io;

use bytes::{Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use crate::rpc::WireMessage;

/// Maximum length, in bytes, of a single encoded `WireMessage`.
///
/// Frames larger than this are rejected without reading the payload, so a
/// peer cannot exhaust memory by claiming a huge length. 16 MiB is well
/// above any reasonable single message — even a `SessionMessage` with a
/// large opaque payload should sit comfortably below it. If a real use
/// case wants larger payloads, raise this thoughtfully and bump
/// [`crate::PROTOCOL_VERSION`].
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Errors the transport may emit during framing.
///
/// These are *transport* errors and live alongside the codec rather than in
/// [`crate::ProtocolError`], because they describe a broken pipe rather
/// than a wire-form refusal from the peer.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// I/O error from the underlying socket.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// The peer announced a frame larger than [`MAX_FRAME_SIZE`].
    #[error("frame too large: {announced} bytes (max {})", MAX_FRAME_SIZE)]
    FrameTooLarge {
        /// Length the peer announced in the length prefix.
        announced: usize,
    },

    /// The frame's bytes did not deserialize as a [`WireMessage`].
    #[error("malformed frame: {0}")]
    Malformed(#[from] postcard::Error),
}

/// Codec that turns a stream of bytes into a stream of [`WireMessage`].
///
/// Each frame is `[u32 BE length][postcard-encoded WireMessage]`.
#[derive(Debug)]
pub struct WireMessageCodec {
    inner: LengthDelimitedCodec,
}

impl WireMessageCodec {
    /// Construct a new codec with [`MAX_FRAME_SIZE`] as the cap.
    #[must_use]
    pub fn new() -> Self {
        let inner = LengthDelimitedCodec::builder()
            .length_field_type::<u32>()
            .max_frame_length(MAX_FRAME_SIZE)
            .new_codec();
        Self { inner }
    }
}

impl Default for WireMessageCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for WireMessageCodec {
    type Item = WireMessage;
    type Error = TransportError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.inner.decode(src) {
            Ok(Some(frame)) => {
                let msg = postcard::from_bytes(&frame)?;
                Ok(Some(msg))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(translate_length_error(err)),
        }
    }
}

impl Encoder<WireMessage> for WireMessageCodec {
    type Error = TransportError;

    fn encode(&mut self, item: WireMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = postcard::to_allocvec(&item)?;
        if bytes.len() > MAX_FRAME_SIZE {
            return Err(TransportError::FrameTooLarge {
                announced: bytes.len(),
            });
        }
        self.inner
            .encode(Bytes::from(bytes), dst)
            .map_err(translate_length_error)
    }
}

/// `LengthDelimitedCodec` reports the max-length violation as an
/// `io::Error` with `InvalidData`. We translate it into the structured
/// variant so callers can match on it.
fn translate_length_error(err: io::Error) -> TransportError {
    if err.kind() == io::ErrorKind::InvalidData {
        // The message is e.g. "frame size too big". We don't have the
        // announced length without re-implementing the codec, so we pass
        // 0 as a placeholder rather than fabricating a number.
        TransportError::FrameTooLarge { announced: 0 }
    } else {
        TransportError::Io(err)
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use pretty_assertions::assert_eq;
    use tokio_util::codec::{Decoder, Encoder};

    use super::*;
    use crate::ids::{PeerId, SessionId};
    use crate::message::MessageKind;
    use crate::rpc::{Request, RequestId, Response, SendPayload, WireMessage};
    use crate::version::PROTOCOL_VERSION;

    fn sample_request_frame() -> WireMessage {
        WireMessage::Request {
            id: RequestId::new(1),
            request: Request::Hello {
                client_version: PROTOCOL_VERSION,
            },
        }
    }

    fn sample_send_frame() -> WireMessage {
        WireMessage::Request {
            id: RequestId::new(99),
            request: Request::Send {
                session: SessionId::from_bytes([7; 16]),
                payload: SendPayload {
                    kind: MessageKind::Tool,
                    action: "tool.exec".into(),
                    payload: vec![0xab; 32],
                },
            },
        }
    }

    fn sample_response_frame() -> WireMessage {
        WireMessage::Response {
            id: RequestId::new(2),
            response: Response::Hello {
                daemon_version: PROTOCOL_VERSION,
                daemon_peer_id: PeerId::from_bytes([3; 32]),
            },
        }
    }

    #[test]
    fn round_trip_request() {
        let mut codec = WireMessageCodec::new();
        let frame = sample_request_frame();
        let mut buf = BytesMut::new();
        codec.encode(frame.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().expect("frame");
        assert_eq!(decoded, frame);
        // Buffer fully consumed.
        assert!(buf.is_empty());
    }

    #[test]
    fn round_trip_response() {
        let mut codec = WireMessageCodec::new();
        let frame = sample_response_frame();
        let mut buf = BytesMut::new();
        codec.encode(frame.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().expect("frame");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn round_trip_pipelined_three_frames() {
        let mut codec = WireMessageCodec::new();
        let frames = [
            sample_request_frame(),
            sample_send_frame(),
            sample_response_frame(),
        ];
        let mut buf = BytesMut::new();
        for f in &frames {
            codec.encode(f.clone(), &mut buf).unwrap();
        }
        for f in &frames {
            let decoded = codec.decode(&mut buf).unwrap().expect("frame");
            assert_eq!(decoded, *f);
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn decode_returns_none_on_partial_length_prefix() {
        let mut codec = WireMessageCodec::new();
        let mut buf = BytesMut::new();
        // Only two of the four length-prefix bytes.
        buf.extend_from_slice(&[0, 0]);
        let res = codec.decode(&mut buf).unwrap();
        assert!(res.is_none());
        // Buffer is preserved for the next read.
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn decode_returns_none_on_partial_payload() {
        let mut codec = WireMessageCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(sample_request_frame(), &mut buf).unwrap();
        // Truncate the last byte: codec should ask for more data.
        buf.truncate(buf.len() - 1);
        let res = codec.decode(&mut buf).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn split_reads_eventually_assemble() {
        let mut codec = WireMessageCodec::new();
        let frame = sample_send_frame();

        // Encode into a separate buffer, then drip-feed one byte at a time.
        let mut full = BytesMut::new();
        codec.encode(frame.clone(), &mut full).unwrap();

        let mut sink = BytesMut::new();
        let mut decoded = None;
        for byte in full.iter().copied() {
            sink.extend_from_slice(&[byte]);
            if let Some(msg) = codec.decode(&mut sink).unwrap() {
                decoded = Some(msg);
            }
        }
        assert_eq!(decoded, Some(frame));
        assert!(sink.is_empty());
    }

    #[test]
    fn malformed_payload_is_surfaced() {
        let mut codec = WireMessageCodec::new();
        let mut buf = BytesMut::new();
        // 4-byte length prefix announcing 3 bytes, then 3 garbage bytes.
        // The length prefix is big-endian u32 per LengthDelimitedCodec.
        buf.extend_from_slice(&3u32.to_be_bytes());
        buf.extend_from_slice(&[0xff, 0xff, 0xff]);
        let err = codec.decode(&mut buf).unwrap_err();
        assert!(matches!(err, TransportError::Malformed(_)), "{err:?}");
    }

    #[test]
    fn oversized_frame_announcement_is_rejected() {
        let mut codec = WireMessageCodec::new();
        let mut buf = BytesMut::new();
        // Announce a frame just over the cap. The codec should refuse
        // before allocating the payload.
        let too_big = u32::try_from(MAX_FRAME_SIZE + 1).unwrap();
        buf.extend_from_slice(&too_big.to_be_bytes());
        let err = codec.decode(&mut buf).unwrap_err();
        assert!(
            matches!(err, TransportError::FrameTooLarge { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn encoding_huge_frame_is_rejected() {
        // We can't realistically build a postcard payload that exceeds the
        // cap from a real WireMessage in a unit test, so verify the encode
        // path by feeding directly into the inner codec via a pseudo
        // pre-check: encode an oversized payload manually and confirm the
        // size guard fires.
        //
        // Simulate by using a Send variant whose opaque payload is huge.
        let mut codec = WireMessageCodec::new();
        let frame = WireMessage::Request {
            id: RequestId::new(1),
            request: Request::Send {
                session: SessionId::from_bytes([0; 16]),
                payload: SendPayload {
                    kind: MessageKind::System,
                    action: String::new(),
                    payload: vec![0u8; MAX_FRAME_SIZE + 1],
                },
            },
        };
        let mut buf = BytesMut::new();
        let err = codec.encode(frame, &mut buf).unwrap_err();
        assert!(
            matches!(err, TransportError::FrameTooLarge { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn empty_buffer_decodes_to_none() {
        let mut codec = WireMessageCodec::new();
        let mut buf = BytesMut::new();
        let res = codec.decode(&mut buf).unwrap();
        assert!(res.is_none());
    }
}
