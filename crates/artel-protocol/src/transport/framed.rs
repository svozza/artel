//! Typed `Stream` / `Sink` wrapper around [`WireMessageCodec`].

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed as TokioFramed;

use super::codec::WireMessageCodec;

/// A bidirectional, framed stream of [`crate::WireMessage`]s.
///
/// Built on top of `tokio_util::codec::Framed`. Implements
/// [`Stream<Item = Result<WireMessage, TransportError>>`][stream] for reads
/// and [`Sink<WireMessage, Error = TransportError>`][sink] for writes.
///
/// [stream]: futures_util::Stream
/// [sink]: futures_util::Sink
pub type Framed<IO> = TokioFramed<IO, WireMessageCodec>;

/// Wrap any `AsyncRead + AsyncWrite` in the artel framing.
///
/// Use this with `tokio::io::duplex` for in-process tests, or with a real
/// Unix socket / named-pipe connection in production.
#[must_use]
pub fn new<IO: AsyncRead + AsyncWrite>(io: IO) -> Framed<IO> {
    Framed::new(io, WireMessageCodec::new())
}

#[cfg(test)]
mod tests {
    use futures_util::{SinkExt, StreamExt};
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;
    use tokio::io::duplex;
    use tokio::runtime::Builder;

    use super::*;
    use crate::ids::{PeerId, Seq, SessionId};
    use crate::message::{MessageKind, PeerInfo, SessionMessage};
    use crate::rpc::{Event, Request, RequestId, Response, SendPayload, WireMessage};
    use crate::version::PROTOCOL_VERSION;

    fn rt() -> tokio::runtime::Runtime {
        Builder::new_current_thread().enable_all().build().unwrap()
    }

    fn sample_frames() -> Vec<WireMessage> {
        vec![
            WireMessage::Request {
                id: RequestId::new(1),
                request: Request::Hello {
                    client_version: PROTOCOL_VERSION,
                },
            },
            WireMessage::Response {
                id: RequestId::new(1),
                response: Response::Hello {
                    daemon_version: PROTOCOL_VERSION,
                    daemon_peer_id: PeerId::from_bytes([0xab; 32]),
                },
            },
            WireMessage::Event {
                event: Event::Message {
                    session: SessionId::from_bytes([1; 16]),
                    message: SessionMessage::new(
                        Seq::new(7),
                        42,
                        PeerInfo::new(PeerId::from_bytes([2; 32]), "alice"),
                        MessageKind::Chat,
                        "chat.message",
                        b"hello".to_vec(),
                    ),
                },
            },
        ]
    }

    #[test]
    fn duplex_round_trip_pipelined() {
        rt().block_on(async {
            let (a, b) = duplex(64 * 1024);
            let mut tx = new(a);
            let mut rx = new(b);

            let frames = sample_frames();
            for f in &frames {
                tx.send(f.clone()).await.unwrap();
            }
            tx.close().await.unwrap();

            for expected in &frames {
                let got = rx.next().await.expect("frame").unwrap();
                assert_eq!(got, *expected);
            }
            // Stream ends when the writer closes.
            assert!(rx.next().await.is_none());
        });
    }

    #[test]
    fn duplex_concurrent_send_recv() {
        rt().block_on(async {
            let (a, b) = duplex(64 * 1024);
            let mut tx = new(a);
            let mut rx = new(b);

            let frame = WireMessage::Request {
                id: RequestId::new(42),
                request: Request::ListSessions,
            };

            let send_frame = frame.clone();
            let send = tokio::spawn(async move {
                tx.send(send_frame).await.unwrap();
                tx.close().await.unwrap();
            });

            let got = rx.next().await.expect("frame").unwrap();
            assert_eq!(got, frame);
            send.await.unwrap();
        });
    }

    #[test]
    fn framed_stream_ends_on_eof() {
        rt().block_on(async {
            let (a, b) = duplex(1024);
            // Drop the writer immediately to signal EOF.
            drop(a);
            let mut rx = new(b);
            assert!(rx.next().await.is_none());
        });
    }

    #[test]
    fn truncated_stream_yields_io_error() {
        rt().block_on(async {
            let (mut a, b) = duplex(1024);
            // Write a partial length prefix and then drop.
            tokio::io::AsyncWriteExt::write_all(&mut a, &[0u8, 0u8])
                .await
                .unwrap();
            drop(a);
            let mut rx = new(b);
            // The codec asks for more bytes; on EOF it surfaces an io error.
            let res = rx.next().await.expect("an error item");
            assert!(res.is_err(), "expected error, got {res:?}");
        });
    }

    fn arb_send_payload() -> impl Strategy<Value = SendPayload> {
        (
            prop_oneof![
                Just(MessageKind::Chat),
                Just(MessageKind::Tool),
                Just(MessageKind::System),
            ],
            "[\\PC]{0,32}",
            proptest::collection::vec(any::<u8>(), 0..512),
        )
            .prop_map(|(kind, action, payload)| SendPayload {
                kind,
                action,
                payload,
            })
    }

    fn arb_request() -> impl Strategy<Value = Request> {
        prop_oneof![
            any::<u32>().prop_map(|v| Request::Hello {
                client_version: crate::ProtocolVersion::new(v),
            }),
            Just(Request::ListSessions),
            (any::<[u8; 16]>(), arb_send_payload()).prop_map(|(s, payload)| Request::Send {
                session: SessionId::from_bytes(s),
                payload,
            }),
        ]
    }

    fn arb_wire_message() -> impl Strategy<Value = WireMessage> {
        (any::<u64>(), arb_request()).prop_map(|(id, req)| WireMessage::Request {
            id: RequestId::new(id),
            request: req,
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 64,
            ..ProptestConfig::default()
        })]

        #[test]
        fn duplex_round_trip_arb(frames in proptest::collection::vec(arb_wire_message(), 1..8)) {
            let frames_for_check = frames.clone();
            rt().block_on(async move {
                let (a, b) = duplex(1024 * 1024);
                let mut tx = new(a);
                let mut rx = new(b);
                for f in &frames {
                    tx.send(f.clone()).await.unwrap();
                }
                tx.close().await.unwrap();
                for expected in &frames {
                    let got = rx.next().await.expect("frame").unwrap();
                    prop_assert_eq!(got, expected.clone());
                }
                prop_assert!(rx.next().await.is_none());
                Ok(())
            })?;
            // Touch the captured copy so the borrow checker is happy and
            // the closure has a real value to compare against.
            prop_assert!(!frames_for_check.is_empty());
        }
    }
}
