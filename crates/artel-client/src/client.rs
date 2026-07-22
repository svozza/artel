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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as SyncMutex};

use artel_protocol::transport::{self, Framed, client::connect as transport_connect};
use artel_protocol::{
    Event, PROTOCOL_VERSION, PeerId, ProtocolVersion, Request, RequestId, Response, WireMessage,
};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
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
/// pile up here. Once full, the reader drops the newest event rather
/// than blocking (see [`spawn_reader`]) — the events stream is
/// advisory and must never stall response demultiplexing, which
/// shares the same reader task. The daemon's own broadcast channel
/// has an analogous drop-on-lag policy one layer up, surfaced to the
/// consumer as [`Event::Gap`](artel_protocol::Event::Gap); this local
/// queue is a second, independent place the same kind of loss can
/// happen, purely from the consumer not draining fast enough.
const EVENTS_QUEUE_CAPACITY: usize = 256;

type ResponseSenders = Arc<SyncMutex<HashMap<RequestId, oneshot::Sender<Response>>>>;

/// Owns a `pending` map entry for the lifetime of a `request()` call
/// and removes it on drop.
///
/// Without this, cancelling a `request()` future mid-flight — e.g. a
/// caller wrapping the call in `tokio::time::timeout` that fires —
/// left the entry in `pending` forever: nothing removes it except a
/// matching `Response` frame arriving (which never happens for the
/// op that hung) or the whole connection closing. A retry loop around
/// a slow-but-alive daemon operation would leak one entry per
/// cancelled attempt. The guard makes removal happen on every exit
/// path — normal completion, early return, or the future simply being
/// dropped — without `request()` having to enumerate them.
struct PendingGuard<'a> {
    pending: &'a ResponseSenders,
    id: RequestId,
}

impl<'a> PendingGuard<'a> {
    fn insert(pending: &'a ResponseSenders, id: RequestId, tx: oneshot::Sender<Response>) -> Self {
        pending.lock().expect("pending mutex").insert(id, tx);
        Self { pending, id }
    }
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.pending.lock().expect("pending mutex").remove(&self.id);
    }
}

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

    async fn handshake<IO>(framed: Framed<IO>, socket_path: PathBuf) -> Result<Self, ClientError>
    where
        IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
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

    fn spawn<IO>(
        framed: Framed<IO>,
        daemon_version: ProtocolVersion,
        daemon_peer_id: PeerId,
        socket_path: PathBuf,
    ) -> Self
    where
        IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static,
    {
        let (out_tx, out_rx) = mpsc::channel::<WireMessage>(WRITER_QUEUE_CAPACITY);
        let (events_tx, events_rx) = mpsc::channel(EVENTS_QUEUE_CAPACITY);
        let (closed_tx, closed_rx) = watch::channel(false);
        let pending: ResponseSenders = Arc::new(SyncMutex::new(HashMap::new()));

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
        // Guard's Drop removes the `pending` entry on every exit path
        // below — early return, normal completion, or the whole
        // `request()` future being cancelled by an external timeout —
        // so none of those paths need their own explicit `.remove()`.
        let _guard = PendingGuard::insert(&self.pending, id, tx);

        if self
            .out
            .send(WireMessage::Request { id, request })
            .await
            .is_err()
        {
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

fn spawn_writer<IO>(
    mut sink: SplitSink<Framed<IO>, WireMessage>,
    mut out: mpsc::Receiver<WireMessage>,
) where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static,
{
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

fn spawn_reader<IO>(
    mut stream: SplitStream<Framed<IO>>,
    pending: ResponseSenders,
    events: mpsc::Sender<Event>,
    closed: watch::Sender<bool>,
) where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static,
{
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
                    let tx = pending.lock().expect("pending mutex").remove(&id);
                    if let Some(tx) = tx {
                        // If the caller dropped its future, this fails
                        // silently and the response is discarded.
                        let _ = tx.send(response);
                    } else {
                        warn!(?id, "client reader: response with no pending request");
                    }
                }
                WireMessage::Event { event } => {
                    // `try_send`, never `.await`: this task also
                    // demultiplexes `Response` frames below, so a
                    // blocking send here — with a slow or absent
                    // events consumer — would stall `stream.next()`
                    // and wedge every in-flight and future
                    // `Client::request` on this connection, which has
                    // no timeout of its own to escape it. Dropping the
                    // event on a full queue (or a closed stream) keeps
                    // the reader always making progress; the events
                    // stream is documented as advisory, not
                    // guaranteed delivery.
                    if let Err(err) = events.try_send(event) {
                        match err {
                            mpsc::error::TrySendError::Full(_) => {
                                warn!("client reader: events queue full; dropping event");
                            }
                            mpsc::error::TrySendError::Closed(_) => {
                                // Caller dropped the events stream. Keep
                                // running so in-flight requests can still
                                // complete.
                            }
                        }
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
        pending.lock().expect("pending mutex").clear();
        debug!("client reader: exited");
    });
}

#[cfg(test)]
mod tests {
    //! Unit tests for `spawn_reader`'s event/response demultiplexing,
    //! driven directly over a `tokio::io::duplex` pipe (no real socket
    //! or daemon needed — mirrors the pattern
    //! `artel_protocol::transport::framed`'s own tests use).

    use artel_protocol::{RequestId, SessionId};
    use tokio::time::{Duration, timeout};

    use super::*;

    /// Wire up a `spawn_reader` against one end of a duplex pipe,
    /// returning the other end (to feed frames in) plus the channels
    /// the reader populates.
    fn reader_harness(
        events_capacity: usize,
    ) -> (
        Framed<tokio::io::DuplexStream>,
        ResponseSenders,
        mpsc::Receiver<Event>,
        watch::Receiver<bool>,
    ) {
        let (a, b) = tokio::io::duplex(64 * 1024);
        let feed = transport::new(a);
        let (_sink, stream) = transport::new(b).split();
        let pending: ResponseSenders = Arc::new(SyncMutex::new(HashMap::new()));
        let (events_tx, events_rx) = mpsc::channel(events_capacity);
        let (closed_tx, closed_rx) = watch::channel(false);
        spawn_reader(stream, Arc::clone(&pending), events_tx, closed_tx);
        (feed, pending, events_rx, closed_rx)
    }

    fn dummy_event() -> Event {
        Event::SessionClosed {
            session: SessionId::from_bytes([7; 16]),
        }
    }

    /// The bug this module's fix closes: with the events queue at
    /// capacity and nobody draining it, a `Response` frame for an
    /// unrelated in-flight request must still be demultiplexed
    /// promptly — the reader must never block on the events send.
    #[tokio::test]
    async fn response_is_demuxed_even_when_events_queue_is_full() {
        let (mut feed, pending, mut events_rx, _closed_rx) = reader_harness(1);

        // Fill the events queue to capacity without draining it.
        feed.send(WireMessage::Event {
            event: dummy_event(),
        })
        .await
        .unwrap();
        // Give the reader task a moment to pull the frame and fill the
        // (capacity-1) events channel.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Register a pending request the way `Client::request` does,
        // then feed its response. Pre-fix, this would never arrive:
        // the reader would be stuck on the full events channel's
        // `.await` send from a *second* queued Event frame processed
        // ahead of it. To reproduce that ordering deterministically,
        // send another Event first, then the Response, both before
        // awaiting anything on the response side.
        let id = RequestId::new(1);
        let (tx, rx) = oneshot::channel();
        pending.lock().expect("pending mutex").insert(id, tx);

        feed.send(WireMessage::Event {
            event: dummy_event(),
        })
        .await
        .unwrap();
        feed.send(WireMessage::Response {
            id,
            response: Response::ListSessions { sessions: vec![] },
        })
        .await
        .unwrap();

        let response = timeout(Duration::from_secs(2), rx)
            .await
            .expect("reader must demux the Response without blocking on the full events queue")
            .expect("oneshot must not be dropped");
        assert!(matches!(response, Response::ListSessions { sessions } if sessions.is_empty()));

        // The events queue is advisory: of the 2 Event frames fed in
        // against a capacity-1 channel, only 1 must have been
        // delivered — the second was dropped rather than buffered or
        // blocking the reader.
        let mut delivered = 0;
        while timeout(Duration::from_millis(50), events_rx.recv())
            .await
            .is_ok_and(|item| item.is_some())
        {
            delivered += 1;
        }
        assert_eq!(
            delivered, 1,
            "exactly one of the two Event frames should have fit in the capacity-1 queue",
        );
    }

    /// Once the caller drops the events receiver entirely, the reader
    /// must keep demultiplexing responses rather than treating the
    /// closed channel as fatal.
    #[tokio::test]
    async fn response_is_demuxed_after_events_receiver_is_dropped() {
        let (mut feed, pending, events_rx, _closed_rx) = reader_harness(4);
        drop(events_rx);

        let id = RequestId::new(1);
        let (tx, rx) = oneshot::channel();
        pending.lock().expect("pending mutex").insert(id, tx);

        feed.send(WireMessage::Event {
            event: dummy_event(),
        })
        .await
        .unwrap();
        feed.send(WireMessage::Response {
            id,
            response: Response::ListSessions { sessions: vec![] },
        })
        .await
        .unwrap();

        let response = timeout(Duration::from_secs(2), rx)
            .await
            .expect("reader must keep running after the events receiver is dropped")
            .expect("oneshot must not be dropped");
        assert!(matches!(response, Response::ListSessions { sessions } if sessions.is_empty()));
    }

    /// A full `Client` wired directly over a duplex pipe, plus the
    /// peer end — kept alive by the caller for as long as the
    /// connection should stay open (silent, never answering) rather
    /// than `EOF`ing. `request()` can then be cancelled by an external
    /// `tokio::time::timeout` while the connection is genuinely still
    /// live — the exact pattern a caller reaches for to bound an op
    /// against a daemon that hangs rather than disconnects.
    fn client_with_silent_peer() -> (Client, tokio::io::DuplexStream) {
        let (a, b_never_answers) = tokio::io::duplex(64 * 1024);
        let framed = transport::new(a);
        let client = Client::spawn(
            framed,
            PROTOCOL_VERSION,
            PeerId::from_bytes([0; 32]),
            PathBuf::from("/nonexistent"),
        );
        (client, b_never_answers)
    }

    /// The bug this module's fix closes: cancelling `request()` via an
    /// external timeout must not leak its `pending` entry. Pre-fix,
    /// only a matching `Response` or the whole connection closing ever
    /// removed an entry — a cancelled call against an otherwise-alive
    /// connection left it there forever.
    #[tokio::test]
    async fn cancelling_request_removes_its_pending_entry() {
        let (client, _silent_peer) = client_with_silent_peer();

        let result = timeout(
            Duration::from_millis(50),
            client.request(Request::ListSessions),
        )
        .await;
        assert!(result.is_err(), "the silent peer must never answer");

        assert_eq!(
            client.pending.lock().unwrap().len(),
            0,
            "the cancelled request's pending entry must be cleaned up by PendingGuard's Drop, \
             not left behind for the connection's eventual close to reap",
        );
    }

    /// Repeated cancelled calls against the same live connection must
    /// not accumulate — each `PendingGuard` drop cleans up its own
    /// entry independently of the others.
    #[tokio::test]
    async fn repeated_cancelled_requests_do_not_accumulate() {
        let (client, _silent_peer) = client_with_silent_peer();

        for _ in 0..10 {
            let _ = timeout(
                Duration::from_millis(10),
                client.request(Request::ListSessions),
            )
            .await;
        }

        assert_eq!(
            client.pending.lock().unwrap().len(),
            0,
            "10 cancelled requests must leave 0 entries behind, not 10",
        );
    }
}
