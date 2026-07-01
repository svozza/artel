//! Client-side socket dialer.
//!
//! [`connect`] surfaces OS errors unchanged — `NotFound` (socket absent)
//! and `ConnectionRefused` (no listener) are the two the caller cares
//! about. Stale-socket recovery lives elsewhere: the daemon removes a
//! stale socket file before binding, and the client's spawn layer
//! retries the connect while the daemon starts.

use std::io;
use std::path::Path;

use tokio::net::UnixStream;

use super::framed::{Framed, new as new_framed};

/// Connect to a daemon listening at `path` and return a framed
/// transport over the resulting stream.
///
/// # Errors
///
/// Returns the OS error from connecting the Unix socket unchanged —
/// notably [`io::ErrorKind::NotFound`] if no socket file exists at
/// `path` and [`io::ErrorKind::ConnectionRefused`] if the file exists
/// but no daemon is listening.
pub async fn connect(path: impl AsRef<Path>) -> io::Result<Framed<UnixStream>> {
    let stream = UnixStream::connect(path).await?;
    Ok(new_framed(stream))
}

#[cfg(test)]
mod tests {
    use futures_util::{SinkExt, StreamExt};
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;
    use tokio::runtime::Builder;

    use super::*;
    use crate::ids::PeerId;
    use crate::rpc::{Request, RequestId, Response, WireMessage};
    use crate::transport::server::Listener;
    use crate::version::PROTOCOL_VERSION;

    fn rt() -> tokio::runtime::Runtime {
        Builder::new_current_thread().enable_all().build().unwrap()
    }

    #[test]
    fn connect_against_missing_socket_yields_not_found() {
        rt().block_on(async {
            let dir = tempdir().unwrap();
            let sock = dir.path().join("absent.sock");
            let err = connect(&sock).await.unwrap_err();
            assert_eq!(
                err.kind(),
                io::ErrorKind::NotFound,
                "expected NotFound, got {err:?}"
            );
        });
    }

    #[test]
    fn end_to_end_hello_round_trip() {
        rt().block_on(async {
            let dir = tempdir().unwrap();
            let sock = dir.path().join("daemon.sock");
            let listener = Listener::bind(&sock).await.unwrap();

            // Spawn a tiny "daemon" that accepts one connection and
            // replies to a Hello request.
            let server = tokio::spawn(async move {
                let mut framed = listener.accept().await.unwrap();
                let frame = framed.next().await.expect("frame").unwrap();
                let WireMessage::Request { id, request } = frame else {
                    panic!("expected request, got {frame:?}");
                };
                assert!(matches!(request, Request::Hello { .. }));
                framed
                    .send(WireMessage::Response {
                        id,
                        response: Response::Hello {
                            daemon_version: PROTOCOL_VERSION,
                            daemon_peer_id: PeerId::from_bytes([0xab; 32]),
                        },
                    })
                    .await
                    .unwrap();
                framed.close().await.unwrap();
                // Keep the listener alive until the client has finished.
                listener
            });

            let mut client = connect(&sock).await.unwrap();
            client
                .send(WireMessage::Request {
                    id: RequestId::new(1),
                    request: Request::Hello {
                        client_version: PROTOCOL_VERSION,
                    },
                })
                .await
                .unwrap();

            let resp = client.next().await.expect("response").unwrap();
            match resp {
                WireMessage::Response { id, response } => {
                    assert_eq!(id, RequestId::new(1));
                    assert!(matches!(response, Response::Hello { .. }));
                }
                other => panic!("expected response, got {other:?}"),
            }

            let _listener = server.await.unwrap();
        });
    }

    #[test]
    fn end_to_end_pipelined_messages() {
        rt().block_on(async {
            let dir = tempdir().unwrap();
            let sock = dir.path().join("daemon.sock");
            let listener = Listener::bind(&sock).await.unwrap();

            let messages = vec![
                WireMessage::Request {
                    id: RequestId::new(1),
                    request: Request::ListSessions,
                },
                WireMessage::Request {
                    id: RequestId::new(2),
                    request: Request::HostSession {
                        display_name: "alice".into(),
                        session: None,
                    },
                },
                WireMessage::Request {
                    id: RequestId::new(3),
                    request: Request::ListSessions,
                },
            ];
            let messages_for_server = messages.clone();

            let server = tokio::spawn(async move {
                let mut accepted = listener.accept().await.unwrap();
                for expected in messages_for_server {
                    let got = accepted.next().await.expect("frame").unwrap();
                    assert_eq!(got, expected);
                }
                listener
            });

            let mut client = connect(&sock).await.unwrap();
            for m in messages {
                client.send(m).await.unwrap();
            }
            client.close().await.unwrap();

            let _listener = server.await.unwrap();
        });
    }

    #[test]
    fn server_drop_closes_client_stream() {
        rt().block_on(async {
            let dir = tempdir().unwrap();
            let sock = dir.path().join("daemon.sock");
            let listener = Listener::bind(&sock).await.unwrap();

            let server = tokio::spawn(async move {
                let mut framed = listener.accept().await.unwrap();
                // Read one frame and then drop everything.
                let _ = framed.next().await;
            });

            let mut client = connect(&sock).await.unwrap();
            client
                .send(WireMessage::Request {
                    id: RequestId::new(1),
                    request: Request::ListSessions,
                })
                .await
                .unwrap();

            server.await.unwrap();

            // After the server drops the framed stream, the next read
            // returns None (EOF) rather than hanging.
            let next = client.next().await;
            assert!(next.is_none(), "expected EOF, got {next:?}");
        });
    }
}
