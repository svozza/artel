//! [`Client`] — stateless RPC over the artel IPC transport.
//!
//! Architecture: one connection, two background tasks.
//!
//! - **reader**: pulls frames from the framed transport and dispatches
//!   them to either a per-request oneshot (responses, correlated by
//!   [`RequestId`]) or the events mpsc.
//! - **writer**: drains an outbound mpsc into the framed transport.
//!
//! [`Client::request`] allocates a [`RequestId`], registers a oneshot,
//! pushes the [`WireMessage::Request`] onto the writer mpsc, and awaits
//! the oneshot.
//!
//! When the reader exits (EOF, transport error, drop), it flips a
//! shared `closed` watch and clears the pending map. `request` selects
//! between its oneshot and `closed.changed()`, so callers never wait
//! forever for a response that will not come.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use artel_protocol::transport::{self, Framed, client::connect as transport_connect};
use artel_protocol::{
    Event, PROTOCOL_VERSION, PeerId, ProtocolVersion, Request, RequestId, Response, WireMessage,
};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::UnixStream;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tracing::{debug, warn};

use crate::error::ClientError;

/// Capacity of the outbound writer queue.
///
/// Modest cap so a stuck daemon back-pressures `request` rather than
/// the client growing memory unboundedly.
const WRITER_QUEUE_CAPACITY: usize = 64;

/// Capacity of the events queue handed to the caller.
///
/// Caller-controlled draining: if the consumer is slow, `Event` frames
/// pile up here. The daemon's broadcast channel drops oldest events on
/// lag, so worst case the client surfaces a gap rather than blocking.
const EVENTS_QUEUE_CAPACITY: usize = 256;

type ResponseSenders = Arc<Mutex<HashMap<RequestId, oneshot::Sender<Response>>>>;

/// Stream of asynchronous events the daemon pushes after a successful
/// `Subscribe`.
pub type EventStream = mpsc::Receiver<Event>;

/// Async client for an `artel-daemon`. Multiplexes any number of
/// concurrent requests over one connection.
pub struct Client {
    /// Outbound queue. The writer task owns the receiver.
    out: mpsc::Sender<WireMessage>,
    /// Map of in-flight `RequestId`s to their oneshot senders.
    pending: ResponseSenders,
    /// Flips to `true` when the reader observes EOF / transport error.
    /// Concurrent `request` callers select on this so they don't block
    /// forever on a oneshot that will never resolve.
    closed_rx: watch::Receiver<bool>,
    /// Monotonic source for `RequestId`.
    next_id: AtomicU64,
    /// Daemon's reported protocol version, captured at handshake.
    daemon_version: ProtocolVersion,
    /// Daemon's reported peer id, captured at handshake.
    daemon_peer_id: PeerId,
    /// Socket path this client connected on. Retained so recovery
    /// loops (e.g. `artel-fs`'s cap-listener EOF recovery) can open a
    /// fresh connection to the same daemon without the caller having
    /// to thread the path separately.
    socket_path: PathBuf,
    /// Holds the events receiver until [`Client::take_events`] hands
    /// it out. Single-consumer.
    events_rx: Mutex<Option<EventStream>>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("daemon_version", &self.daemon_version)
            .field("daemon_peer_id", &self.daemon_peer_id)
            .field("closed", &*self.closed_rx.borrow())
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Connect to a daemon at `path`, perform the version handshake,
    /// and return a ready client.
    ///
    /// The handshake exchange happens inside this call; callers do not
    /// (and should not) issue [`Request::Hello`] manually.
    ///
    /// # Errors
    ///
    /// - [`ClientError::Transport`] if the socket cannot be reached
    ///   (missing, no listener, framing/I/O failure).
    /// - [`ClientError::Protocol`] if the daemon answers the `Hello`
    ///   with a [`Response::Error`].
    /// - [`ClientError::ConnectionClosed`] if the connection EOFs
    ///   before the handshake response arrives.
    /// - [`ClientError::UnexpectedResponse`] if the daemon answers
    ///   with anything other than a `Hello` response for the handshake.
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self, ClientError> {
        let path = path.as_ref();
        let framed = transport_connect(path)
            .await
            .map_err(|err| ClientError::Transport(transport::TransportError::Io(err)))?;
        Self::handshake(framed, path.to_path_buf()).await
    }

    /// Connect to a daemon, launching one if it isn't running.
    ///
    /// Tries [`Self::connect`] first. If the socket is missing or
    /// nothing is listening, the daemon binary at
    /// [`SpawnOptions::daemon_binary`] is spawned detached, the client
    /// waits for the socket to come up, and a fresh `connect` is
    /// retried. See [`SpawnOptions`](crate::SpawnOptions) for the
    /// lifecycle details.
    ///
    /// [`SpawnOptions::daemon_binary`]: crate::SpawnOptions::daemon_binary
    ///
    /// # Errors
    ///
    /// - Anything [`Self::connect`] surfaces, for the initial attempt
    ///   and the post-spawn retry, when the failure is not the
    ///   recoverable "no daemon yet" kind.
    /// - [`ClientError::Spawn`] wrapping [`SpawnError::Launch`] if the
    ///   daemon binary cannot be executed, or [`SpawnError::Timeout`]
    ///   if the socket never becomes connectable within
    ///   [`SpawnOptions::spawn_timeout`].
    ///
    /// [`SpawnError::Launch`]: crate::SpawnError::Launch
    /// [`SpawnError::Timeout`]: crate::SpawnError::Timeout
    /// [`SpawnOptions::spawn_timeout`]: crate::SpawnOptions::spawn_timeout
    pub async fn connect_or_spawn(opts: crate::SpawnOptions) -> Result<Self, ClientError> {
        crate::spawn::connect_or_spawn(opts).await
    }

    async fn handshake(
        framed: Framed<UnixStream>,
        socket_path: PathBuf,
    ) -> Result<Self, ClientError> {
        let (mut sink, mut stream) = framed.split();

        // Send Hello directly on the sink so we don't need the writer
        // task running yet.
        let hello_id = RequestId::ZERO;
        sink.send(WireMessage::Request {
            id: hello_id,
            request: Request::Hello {
                client_version: PROTOCOL_VERSION,
            },
        })
        .await?;

        let frame = stream.next().await.ok_or(ClientError::ConnectionClosed)??;
        let (daemon_version, daemon_peer_id) = match frame {
            WireMessage::Response {
                id,
                response:
                    Response::Hello {
                        daemon_version,
                        daemon_peer_id,
                    },
            } if id == hello_id => (daemon_version, daemon_peer_id),
            WireMessage::Response {
                id,
                response: Response::Error { error },
            } if id == hello_id => return Err(ClientError::Protocol(error)),
            other => return Err(unexpected(other)),
        };

        let framed = sink.reunite(stream).expect("split halves match");
        Ok(Self::spawn(
            framed,
            daemon_version,
            daemon_peer_id,
            socket_path,
        ))
    }

    fn spawn(
        framed: Framed<UnixStream>,
        daemon_version: ProtocolVersion,
        daemon_peer_id: PeerId,
        socket_path: PathBuf,
    ) -> Self {
        let (out_tx, out_rx) = mpsc::channel::<WireMessage>(WRITER_QUEUE_CAPACITY);
        let (events_tx, events_rx) = mpsc::channel(EVENTS_QUEUE_CAPACITY);
        let (closed_tx, closed_rx) = watch::channel(false);
        let pending: ResponseSenders = Arc::new(Mutex::new(HashMap::new()));

        let (sink, stream) = framed.split();
        spawn_writer(sink, out_rx);
        spawn_reader(stream, Arc::clone(&pending), events_tx, closed_tx);

        Self {
            out: out_tx,
            pending,
            closed_rx,
            // Hello used 0; start fresh requests at 1.
            next_id: AtomicU64::new(1),
            daemon_version,
            daemon_peer_id,
            socket_path,
            events_rx: Mutex::new(Some(events_rx)),
        }
    }

    /// Send `request` and await the matching response.
    ///
    /// `Response::Error { error }` is converted into
    /// [`ClientError::Protocol`] so the happy path always sees a
    /// non-error variant.
    ///
    /// # Errors
    ///
    /// - [`ClientError::ConnectionClosed`] if the connection is already
    ///   known closed, the writer queue is gone, or the reader exits
    ///   (EOF / transport error) before the response is delivered.
    /// - [`ClientError::Protocol`] if the daemon answers with a
    ///   [`Response::Error`].
    pub async fn request(&self, request: Request) -> Result<Response, ClientError> {
        // Cheap fast path: if the connection is already known closed,
        // don't bother allocating a oneshot or pushing onto the queue.
        if *self.closed_rx.borrow() {
            return Err(ClientError::ConnectionClosed);
        }

        let id = self.alloc_id();
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);

        if self
            .out
            .send(WireMessage::Request { id, request })
            .await
            .is_err()
        {
            self.pending.lock().await.remove(&id);
            return Err(ClientError::ConnectionClosed);
        }

        // Race the response oneshot against the closed signal so we
        // wake up if the reader exits while we're waiting.
        let mut closed = self.closed_rx.clone();
        let response = tokio::select! {
            // Bias toward the response so we don't spuriously fail when
            // the reader has just observed EOF but the response has
            // already been delivered into our oneshot.
            biased;
            r = rx => r.map_err(|_| ClientError::ConnectionClosed)?,
            res = closed.changed() => {
                // Either flipped to true, or the sender dropped.
                let _ = res;
                self.pending.lock().await.remove(&id);
                return Err(ClientError::ConnectionClosed);
            }
        };
        match response {
            Response::Error { error } => Err(ClientError::Protocol(error)),
            other => Ok(other),
        }
    }

    /// Take ownership of the event stream. Returns `None` on the
    /// second call — there is one consumer per `Client`.
    pub async fn take_events(&self) -> Option<EventStream> {
        self.events_rx.lock().await.take()
    }

    /// Daemon's protocol version, as reported in the Hello response.
    #[must_use]
    pub const fn daemon_version(&self) -> ProtocolVersion {
        self.daemon_version
    }

    /// Daemon's peer id, as reported in the Hello response.
    #[must_use]
    pub const fn daemon_peer_id(&self) -> PeerId {
        self.daemon_peer_id
    }

    /// Socket path this client connected on.
    ///
    /// Exposed so recovery loops can reconnect to the same daemon by
    /// opening a fresh [`Client::connect`] — see `artel-fs`'s
    /// cap-listener EOF recovery. (Subscriber lag itself no longer
    /// closes the connection: the daemon sends an in-band
    /// `Event::Gap` and the subscriber re-`Subscribe`s on the same
    /// connection.)
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Whether the underlying connection has been observed closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        *self.closed_rx.borrow()
    }

    fn alloc_id(&self) -> RequestId {
        // Wrap-around at u64::MAX is a 584-year problem.
        RequestId::new(self.next_id.fetch_add(1, Ordering::Relaxed))
    }
}

fn unexpected(frame: WireMessage) -> ClientError {
    match frame {
        WireMessage::Response { response, .. } => ClientError::UnexpectedResponse(response),
        _ => ClientError::ConnectionClosed,
    }
}

fn spawn_writer(
    mut sink: SplitSink<Framed<UnixStream>, WireMessage>,
    mut out: mpsc::Receiver<WireMessage>,
) {
    tokio::spawn(async move {
        while let Some(frame) = out.recv().await {
            if let Err(err) = sink.send(frame).await {
                warn!(error = %err, "client writer: send failed; closing");
                break;
            }
        }
        debug!("client writer: exited");
    });
}

fn spawn_reader(
    mut stream: SplitStream<Framed<UnixStream>>,
    pending: ResponseSenders,
    events: mpsc::Sender<Event>,
    closed: watch::Sender<bool>,
) {
    tokio::spawn(async move {
        while let Some(frame) = stream.next().await {
            let frame = match frame {
                Ok(f) => f,
                Err(err) => {
                    warn!(error = %err, "client reader: transport error; closing");
                    break;
                }
            };
            match frame {
                WireMessage::Response { id, response } => {
                    let tx = pending.lock().await.remove(&id);
                    if let Some(tx) = tx {
                        // If the caller dropped its future, this fails
                        // silently and the response is discarded.
                        let _ = tx.send(response);
                    } else {
                        warn!(?id, "client reader: response with no pending request");
                    }
                }
                WireMessage::Event { event } => {
                    if events.send(event).await.is_err() {
                        // Caller dropped the events stream. Keep
                        // running so in-flight requests can still
                        // complete.
                    }
                }
                WireMessage::Request { .. } => {
                    warn!("client reader: ignoring stray request frame from daemon");
                }
            }
        }
        // Stream ended: signal closure to any waiting requesters and
        // drop their oneshots.
        let _ = closed.send(true);
        pending.lock().await.clear();
        debug!("client reader: exited");
    });
}
