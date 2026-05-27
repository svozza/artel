//! Daemon server: accept loop, per-connection task, RPC dispatch.
//!
//! [`Daemon::start`] sets up the shared [`Registry`], binds the IPC
//! socket, and acquires the PID file. [`Daemon::run`] drives the accept
//! loop until shutdown is triggered, then joins all outstanding
//! connection tasks before returning.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use artel_protocol::transport::{self, Framed, server::Listener};
use artel_protocol::{
    Event, PROTOCOL_VERSION, PeerInfo, ProtocolError, ProtocolVersion, Request, Response,
    SendPayload, SessionId, SessionMessage, VersionMismatch, WireMessage,
};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use tokio::net::UnixStream;
use tokio::sync::{Mutex as AsyncMutex, broadcast};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::pidfile::{PidError, PidFile};
use crate::session::{Registry, SessionError, Subscription};
use crate::shutdown::{Shutdown, ShutdownToken};

/// Configuration for [`Daemon::start`].
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    /// Path the daemon binds for IPC.
    pub socket_path: PathBuf,
    /// Path of the PID file. Acquired before binding to refuse a second
    /// daemon on the same path.
    pub pid_path: PathBuf,
    /// Directory holding per-session subdirectories. Loaded at startup
    /// so sessions outlive the daemon process; created if missing.
    pub sessions_dir: PathBuf,
    /// Fallback peer id, used when no iroh key path is supplied
    /// (or when the `iroh` feature is disabled at compile time).
    /// Tests and headless embeds set this directly to avoid spinning
    /// up an iroh `Endpoint`.
    pub daemon_peer_id: artel_protocol::PeerId,
    /// Path to the persisted iroh secret key. When `Some` and the
    /// `iroh` feature is enabled, the daemon loads (or generates) the
    /// key, binds an `Endpoint`, and uses the resulting `EndpointId`
    /// in place of [`Self::daemon_peer_id`]. When `None`, the daemon
    /// stays local-only and advertises the synthetic peer id.
    pub iroh_key_path: Option<PathBuf>,
    /// Optional override for the iroh endpoint's address-lookup
    /// system. Real deployments leave this `None` and let iroh
    /// discover peers via DNS/relay; integration tests that run two
    /// in-process daemons on `localhost` use it to seed each
    /// daemon with the other's [`iroh::EndpointAddr`] and skip
    /// network discovery.
    ///
    /// Only meaningful when the `iroh` feature is on and an
    /// [`Self::iroh_key_path`] is supplied. The field is
    /// unconditionally present so callers can construct
    /// [`DaemonConfig`] with the same struct literal regardless of
    /// feature flags; without `iroh`, the inner type is a unit
    /// placeholder and the option must be `None`.
    pub address_lookup: Option<AddressLookupOverride>,
}

/// Iroh-feature-conditional payload for
/// [`DaemonConfig::address_lookup`].
///
/// With the `iroh` feature on, this wraps a
/// [`iroh::address_lookup::memory::MemoryLookup`]; without the
/// feature it's an uninhabited type, so the option always
/// serialises to `None`.
#[cfg(feature = "iroh")]
#[derive(Debug, Clone)]
pub struct AddressLookupOverride(pub iroh::address_lookup::memory::MemoryLookup);

/// Iroh-feature-off placeholder — uninhabited.
#[cfg(not(feature = "iroh"))]
#[derive(Debug, Clone)]
pub enum AddressLookupOverride {}

/// Errors returned from [`Daemon::start`].
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// Could not acquire the PID file.
    #[error(transparent)]
    Pid(#[from] PidError),

    /// Could not bind the IPC socket.
    #[error("bind {path}: {source}")]
    Bind {
        /// Path the daemon tried to bind.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Could not install signal handlers.
    #[error("install signal handlers: {0}")]
    Signal(#[source] io::Error),

    /// Could not load persisted sessions from disk.
    #[error("load sessions: {0}")]
    LoadSessions(#[source] io::Error),

    /// Could not load or persist the iroh secret key, or could not
    /// bind the iroh `Endpoint`.
    #[cfg(feature = "iroh")]
    #[error("iroh: {0}")]
    Iroh(String),
}

/// iroh-side state held for the daemon's lifetime: the `Endpoint`
/// (network identity), a `Gossip` instance attached to it, and a
/// `Router` accepting `iroh_gossip::ALPN`.
///
/// Held as a unit because the three are constructed together at
/// startup and torn down together at shutdown via
/// [`iroh::protocol::Router::shutdown`], which closes the underlying
/// `Endpoint` for us. Keeping them together also means the
/// daemon doesn't have to thread three separate options through
/// every codepath that wants to know "is iroh on?".
#[cfg(feature = "iroh")]
#[derive(Debug, Clone)]
#[allow(dead_code)] // endpoint + gossip are read by Phase 2c-2 onwards.
pub struct IrohRuntime {
    /// QUIC endpoint owning the daemon's ed25519 identity.
    pub endpoint: iroh::Endpoint,
    /// Gossip handle. `Clone` is cheap (it's an `Arc` inside).
    pub gossip: iroh_gossip::net::Gossip,
    /// Protocol router; calling `.shutdown().await` cleans up both
    /// the accept loop and the endpoint.
    pub router: iroh::protocol::Router,
}

/// A running daemon. Hold the value to keep the daemon alive; drop it
/// to release the PID file and unbind the socket.
#[derive(Debug)]
pub struct Daemon {
    registry: Arc<Registry>,
    listener: Listener,
    pid: PidFile,
    shutdown: Arc<Shutdown>,
    /// iroh state when the feature is on and an
    /// [`DaemonConfig::iroh_key_path`] was supplied. We hold it for
    /// the daemon's lifetime; teardown happens in [`Self::run`] before
    /// returning.
    #[cfg(feature = "iroh")]
    iroh: Option<IrohRuntime>,
}

impl Daemon {
    /// Acquire the PID file, bind the socket, install signal handlers.
    /// Returns immediately; call [`Self::run`] to drive the accept loop.
    pub async fn start(config: DaemonConfig) -> Result<Self, StartError> {
        let pid = PidFile::acquire(config.pid_path)?;
        // Holding the PID file lock means no other daemon owns this
        // state dir, so any leftover socket file is from a crashed
        // predecessor and is safe to remove. Without this, a hard kill
        // would block the next start with AddrInUse.
        if let Err(err) = std::fs::remove_file(&config.socket_path)
            && err.kind() != io::ErrorKind::NotFound
        {
            return Err(StartError::Bind {
                path: config.socket_path,
                source: err,
            });
        }
        let listener = Listener::bind(&config.socket_path)
            .await
            .map_err(|source| StartError::Bind {
                path: config.socket_path,
                source,
            })?;

        // Resolve the daemon's network identity. Two paths:
        //
        // - `iroh` feature on AND an iroh_key_path supplied: load (or
        //   generate-and-persist) the secret, bind an Endpoint, spawn
        //   a Gossip handle, and start a protocol Router accepting
        //   the gossip ALPN. EndpointId becomes the daemon peer id.
        // - Otherwise: use the caller-supplied peer id and don't
        //   stand up any iroh runtime. Keeps tests fast and lets
        //   embeds opt out of the network stack entirely.
        // Keep a clone of the MemoryLookup (if any) so the gossip
        // bridge can reach into it later when joiners arrive with
        // out-of-band addrs to seed.
        #[cfg(feature = "iroh")]
        let address_lookup_for_bridge = config.address_lookup.as_ref().map(|o| o.0.clone());
        #[cfg(feature = "iroh")]
        let (daemon_peer_id, iroh) = resolve_iroh_runtime(
            config.iroh_key_path.as_deref(),
            config.daemon_peer_id,
            config.address_lookup,
        )
        .await?;
        #[cfg(not(feature = "iroh"))]
        let daemon_peer_id = config.daemon_peer_id;

        // Snapshot the daemon's network addr so the registry can
        // stamp it into outbound tickets. Without iroh (or before
        // the endpoint has discovered any addrs), we fall back to
        // an id-only addr — joiners will need an address-lookup
        // service of their own to dial us.
        #[cfg(feature = "iroh")]
        let daemon_addr = iroh.as_ref().map_or_else(
            || artel_protocol::WireEndpointAddr::id_only(daemon_peer_id),
            |rt| iroh_endpoint_to_wire(&rt.endpoint.addr()),
        );
        #[cfg(not(feature = "iroh"))]
        let daemon_addr = artel_protocol::WireEndpointAddr::id_only(daemon_peer_id);

        // Build the gossip bridge once we have the runtime. Lives
        // for the daemon's lifetime; sessions register themselves
        // with it as they're hosted/joined.
        #[cfg(feature = "iroh")]
        let bridge = iroh.as_ref().map(|rt| {
            Arc::new(crate::gossip_bridge::GossipBridge::new(
                rt.gossip.clone(),
                address_lookup_for_bridge,
            ))
        });

        let store: crate::store::DynStore = Arc::new(
            crate::store::FsLogStore::open(&config.sessions_dir)
                .map_err(StartError::LoadSessions)?,
        );
        let registry = Arc::new(
            Registry::load(
                daemon_peer_id,
                daemon_addr,
                store,
                #[cfg(feature = "iroh")]
                bridge.clone(),
            )
            .await
            .map_err(StartError::LoadSessions)?,
        );

        // Inject the back-reference now that the registry is in an
        // Arc. The bridge holds it as a Weak so we don't form a
        // cycle. Without this the host-side forwarder has no way to
        // call back into `Registry::send` for inbound SendRequests.
        #[cfg(feature = "iroh")]
        if let Some(bridge) = &bridge {
            bridge.attach_registry(Arc::downgrade(&registry)).await;
        }

        let shutdown = Arc::new(Shutdown::new());
        shutdown
            .install_signal_handlers()
            .map_err(StartError::Signal)?;

        let session_count = registry.list().await.len();
        info!(
            socket = %listener.path().display(),
            pid = pid.pid(),
            sessions_dir = %config.sessions_dir.display(),
            sessions_loaded = session_count,
            peer_id = %daemon_peer_id,
            "daemon started"
        );

        Ok(Self {
            registry,
            listener,
            pid,
            shutdown,
            #[cfg(feature = "iroh")]
            iroh,
        })
    }

    /// iroh runtime if the daemon stood one up. `None` when the
    /// feature is off or no `iroh_key_path` was supplied.
    ///
    /// Exposed so embedders and integration tests can talk to the
    /// daemon's `Endpoint` and `Gossip` directly. Phase 2c-2 will
    /// keep this surface but route most session traffic through
    /// `Registry`.
    #[cfg(feature = "iroh")]
    #[must_use]
    pub const fn iroh(&self) -> Option<&IrohRuntime> {
        self.iroh.as_ref()
    }

    /// Path of the bound IPC socket.
    #[must_use]
    pub fn socket_path(&self) -> &std::path::Path {
        self.listener.path()
    }

    /// PID recorded in the PID file.
    #[must_use]
    pub const fn pid(&self) -> i32 {
        self.pid.pid()
    }

    /// Cheap clonable cancellation token. Tests trigger shutdown via
    /// the parent [`Shutdown`] handle, but they can also wait on the
    /// token to know when shutdown has occurred.
    #[must_use]
    pub fn shutdown_token(&self) -> ShutdownToken {
        self.shutdown.token()
    }

    /// Clone the shutdown handle. The handle survives [`Self::run`]
    /// consuming the daemon, so embedders (CLI, tests) can trigger
    /// shutdown from outside.
    #[must_use]
    pub fn shutdown_handle(&self) -> Arc<Shutdown> {
        Arc::clone(&self.shutdown)
    }

    /// Trigger graceful shutdown without waiting for [`Self::run`] to
    /// observe it.
    pub fn trigger_shutdown(&self) {
        self.shutdown.trigger();
    }

    /// Drive the accept loop until shutdown. Returns once every
    /// outstanding connection task has finished.
    pub async fn run(self) -> io::Result<()> {
        let Self {
            registry,
            listener,
            pid,
            shutdown,
            #[cfg(feature = "iroh")]
            iroh,
        } = self;

        let mut connections = JoinSet::new();
        let mut shutdown_tok = shutdown.token();

        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    match accepted {
                        Ok(framed) => {
                            let registry = Arc::clone(&registry);
                            let token = shutdown.token();
                            connections.spawn(async move {
                                if let Err(err) = serve_connection(framed, registry, token).await {
                                    warn!(error = %err, "connection ended with error");
                                }
                            });
                        }
                        Err(err) => {
                            // EBADF after the socket is unbound during shutdown
                            // is normal; anything else we surface.
                            if shutdown_tok.is_triggered() {
                                break;
                            }
                            warn!(error = %err, "accept failed");
                        }
                    }
                }
                () = shutdown_tok.cancelled() => {
                    info!("shutdown signal observed, stopping accept loop");
                    break;
                }
            }
        }

        // Drain outstanding connection tasks so we don't leave clients
        // half-served.
        while connections.join_next().await.is_some() {}

        // Tear down the iroh stack cleanly so peers see a graceful
        // shutdown rather than a hung connection. Router::shutdown
        // closes the underlying Endpoint for us. Best-effort: even
        // if it errors, we still want to release the PID file.
        #[cfg(feature = "iroh")]
        if let Some(IrohRuntime { router, .. }) = iroh
            && let Err(err) = router.shutdown().await
        {
            warn!(error = %err, "iroh router shutdown failed");
        }

        // Explicit release lets us surface I/O errors from PID-file
        // removal; drop would swallow them.
        if let Err(err) = pid.release() {
            warn!(error = %err, "failed to release pid file");
        }
        info!("daemon stopped");
        Ok(())
    }
}

/// Drive a single client connection.
async fn serve_connection(
    framed: Framed<UnixStream>,
    registry: Arc<Registry>,
    mut shutdown: ShutdownToken,
) -> Result<(), transport::TransportError> {
    let (sink, mut stream) = framed.split();
    // Wrap the sink in an async mutex so the request loop and any
    // event-forwarding tasks can both push frames into the same
    // connection without interleaving bytes.
    let sink = Arc::new(AsyncMutex::new(sink));

    // First message must be Hello.
    let first = tokio::select! {
        f = stream.next() => f,
        () = shutdown.cancelled() => return Ok(()),
    };
    let first = match first {
        Some(Ok(frame)) => frame,
        Some(Err(err)) => return Err(err),
        None => return Ok(()),
    };

    let (hello_id, hello_result) = match first {
        WireMessage::Request {
            id,
            request: Request::Hello { client_version },
        } => (id, handle_hello(client_version)),
        WireMessage::Request { id, request } => {
            // Speak the protocol back even if the client is rude.
            warn!(?request, "first request was not Hello");
            send_frame(
                &sink,
                WireMessage::Response {
                    id,
                    response: Response::Error {
                        error: ProtocolError::Internal(
                            "expected Hello as first request".to_string(),
                        ),
                    },
                },
            )
            .await?;
            return Ok(());
        }
        other => {
            warn!(?other, "first frame was not a request");
            return Ok(());
        }
    };

    let response = match hello_result {
        Ok(()) => Response::Hello {
            daemon_version: PROTOCOL_VERSION,
            daemon_peer_id: registry.daemon_peer_id(),
        },
        Err(err) => Response::Error { error: err },
    };
    send_frame(
        &sink,
        WireMessage::Response {
            id: hello_id,
            response: response.clone(),
        },
    )
    .await?;
    if matches!(response, Response::Error { .. }) {
        return Ok(());
    }

    // Per-connection state: which sessions has this client joined,
    // and as which peer? Populated by Host/Join, consulted by Send/
    // Leave so we don't need the client to re-send peer info.
    let mut memberships: HashMap<SessionId, PeerInfo> = HashMap::new();

    // Main request loop.
    loop {
        let frame = tokio::select! {
            f = stream.next() => f,
            () = shutdown.cancelled() => return Ok(()),
        };
        let frame = match frame {
            Some(Ok(frame)) => frame,
            Some(Err(err)) => return Err(err),
            None => return Ok(()),
        };

        let WireMessage::Request { id, request } = frame else {
            warn!(?frame, "ignoring unexpected non-request frame");
            continue;
        };
        let response = dispatch(
            &registry,
            request,
            &sink,
            shutdown.clone(),
            &mut memberships,
        )
        .await;
        send_frame(&sink, WireMessage::Response { id, response }).await?;
    }
}

/// Dispatch a non-Hello request to the registry.
async fn dispatch(
    registry: &Registry,
    request: Request,
    sink: &Arc<AsyncMutex<SplitSink<Framed<UnixStream>, WireMessage>>>,
    shutdown: ShutdownToken,
    memberships: &mut HashMap<SessionId, PeerInfo>,
) -> Response {
    match request {
        Request::Hello { .. } => Response::Error {
            error: ProtocolError::Internal("Hello sent twice on one connection".into()),
        },
        Request::HostSession { peer, session } => {
            match registry.host(peer.clone(), session).await {
                Ok((session, ticket)) => {
                    memberships.insert(session, peer);
                    Response::HostSession { session, ticket }
                }
                Err(err) => Response::Error {
                    error: session_error_to_protocol(&err),
                },
            }
        }
        Request::JoinSession { peer, ticket } => match registry.join(&ticket, peer.clone()).await {
            Ok((session, head)) => {
                memberships.insert(session, peer);
                Response::JoinSession { session, head }
            }
            Err(err) => Response::Error {
                error: session_error_to_protocol(&err),
            },
        },
        Request::ListSessions => Response::ListSessions {
            sessions: registry.list().await,
        },
        Request::Subscribe { session, since } => match registry.subscribe(session, since).await {
            Ok(sub) => {
                spawn_subscription_forwarder(session, sub, Arc::clone(sink), shutdown);
                Response::Subscribed { session }
            }
            Err(err) => Response::Error {
                error: session_error_to_protocol(&err),
            },
        },
        Request::Send {
            session,
            payload:
                SendPayload {
                    kind,
                    action,
                    payload,
                },
        } => {
            let Some(peer) = memberships.get(&session).cloned() else {
                return Response::Error {
                    error: ProtocolError::NotSubscribed(session),
                };
            };
            match registry
                .send(session, peer, kind, action, payload, now_ms())
                .await
            {
                Ok(message) => Response::Sent {
                    session,
                    seq: message.seq,
                },
                Err(err) => Response::Error {
                    error: session_error_to_protocol(&err),
                },
            }
        }
        Request::LeaveSession { session } => {
            let Some(peer) = memberships.remove(&session) else {
                return Response::Error {
                    error: ProtocolError::NotSubscribed(session),
                };
            };
            match registry.leave(session, peer.id).await {
                Ok(()) => Response::Left { session },
                Err(err) => {
                    // Re-insert: the leave failed so the client is still
                    // a member. This is mostly defensive — registry
                    // errors here are for unknown-session, in which case
                    // the membership lookup wouldn't have had it
                    // anyway.
                    memberships.insert(session, peer);
                    Response::Error {
                        error: session_error_to_protocol(&err),
                    }
                }
            }
        }
        Request::RegisterAttachment { .. }
        | Request::ListAttachments { .. }
        | Request::ForgetAttachment { .. } => Response::Error {
            error: ProtocolError::Internal(
                "attachment RPCs require daemon support — see slice 2b".into(),
            ),
        },
    }
}

/// Stand up the daemon's iroh runtime: load (or generate) the secret
/// key, bind an `Endpoint`, spawn a `Gossip` instance attached to it,
/// and start a protocol `Router` accepting the gossip ALPN. Returns
/// the resulting `EndpointId` (cast to [`PeerId`]) plus the bundle.
///
/// When no key path is supplied, returns the caller-supplied fallback
/// peer id and `None` — the daemon stays local-only.
#[cfg(feature = "iroh")]
async fn resolve_iroh_runtime(
    key_path: Option<&std::path::Path>,
    fallback_peer_id: artel_protocol::PeerId,
    address_lookup: Option<AddressLookupOverride>,
) -> Result<(artel_protocol::PeerId, Option<IrohRuntime>), StartError> {
    let Some(path) = key_path else {
        return Ok((fallback_peer_id, None));
    };
    let secret =
        crate::iroh_key::load_or_create(path).map_err(|e| StartError::Iroh(e.to_string()))?;
    let mut builder = iroh::Endpoint::builder(iroh::endpoint::presets::N0).secret_key(secret);
    if let Some(AddressLookupOverride(lookup)) = address_lookup {
        builder = builder.address_lookup(lookup);
    }
    let endpoint = builder
        .bind()
        .await
        .map_err(|e| StartError::Iroh(format!("bind endpoint: {e}")))?;
    let peer_id = artel_protocol::PeerId::from_bytes(*endpoint.id().as_bytes());

    // Gossip needs a clone of the endpoint to register itself for the
    // ALPN; the Router does the actual accepting.
    let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
    let router = iroh::protocol::Router::builder(endpoint.clone())
        .accept(iroh_gossip::ALPN, gossip.clone())
        .spawn();

    Ok((
        peer_id,
        Some(IrohRuntime {
            endpoint,
            gossip,
            router,
        }),
    ))
}

/// Convert an `iroh::EndpointAddr` into the wire-friendly form used
/// in tickets. Direct UDP addresses become `SocketAddr`; the (zero
/// or one) home relay URL becomes a `String`.
#[cfg(feature = "iroh")]
fn iroh_endpoint_to_wire(addr: &iroh::EndpointAddr) -> artel_protocol::WireEndpointAddr {
    let direct_addrs = addr.ip_addrs().copied().collect();
    let relay_url = addr
        .relay_urls()
        .next()
        .map(ToString::to_string)
        .unwrap_or_default();
    artel_protocol::WireEndpointAddr {
        peer_id: artel_protocol::PeerId::from_bytes(*addr.id.as_bytes()),
        relay_url,
        direct_addrs,
    }
}

/// Wall-clock milliseconds since the Unix epoch. Used for stamping
/// outgoing [`SessionMessage`]s. Returns 0 if the clock is before the
/// epoch (impossible on a sanely-configured machine, but we don't
/// panic).
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Spawn a task that forwards events from `sub.events` as
/// [`WireMessage::Event`] frames into `sink`. Backfills `sub.replay`
/// first.
fn spawn_subscription_forwarder(
    session: SessionId,
    sub: Subscription,
    sink: Arc<AsyncMutex<SplitSink<Framed<UnixStream>, WireMessage>>>,
    mut shutdown: ShutdownToken,
) {
    let Subscription { replay, mut events } = sub;
    tokio::spawn(async move {
        for message in replay {
            if push_message(&sink, session, message).await.is_err() {
                return;
            }
        }
        loop {
            let next = tokio::select! {
                r = events.recv() => r,
                () = shutdown.cancelled() => return,
            };
            let event = match next {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Closed) => return,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(missed = n, "subscriber lagged; dropping {n} events");
                    continue;
                }
            };
            if send_frame(&sink, WireMessage::Event { event })
                .await
                .is_err()
            {
                return;
            }
        }
    });
}

async fn push_message(
    sink: &Arc<AsyncMutex<SplitSink<Framed<UnixStream>, WireMessage>>>,
    session: SessionId,
    message: SessionMessage,
) -> Result<(), transport::TransportError> {
    send_frame(
        sink,
        WireMessage::Event {
            event: Event::Message { session, message },
        },
    )
    .await
}

async fn send_frame(
    sink: &Arc<AsyncMutex<SplitSink<Framed<UnixStream>, WireMessage>>>,
    frame: WireMessage,
) -> Result<(), transport::TransportError> {
    let mut guard = sink.lock().await;
    guard.send(frame).await
}

/// Translate session-layer errors into wire errors.
fn session_error_to_protocol(err: &SessionError) -> ProtocolError {
    match err {
        SessionError::UnknownSession(s) => ProtocolError::UnknownSession(*s),
        SessionError::NotMember(_) => ProtocolError::Internal("not a member".into()),
        SessionError::AlreadyJoined(s) => ProtocolError::AlreadyJoined(*s),
        SessionError::InvalidTicket => ProtocolError::InvalidTicket,
        SessionError::Storage(io_err) => ProtocolError::Internal(format!("storage: {io_err}")),
        SessionError::InvalidAddr(msg) => ProtocolError::Internal(format!("invalid addr: {msg}")),
        SessionError::Internal(msg) => ProtocolError::Internal(msg.clone()),
        SessionError::NotHost => ProtocolError::NotHost,
        SessionError::SessionConflict(s) => ProtocolError::SessionConflict(*s),
        // Forward the host's verdict verbatim so the IPC client
        // sees the actual reason (e.g., UnknownSession after a
        // session close) instead of a generic Internal.
        SessionError::HostRejected(err) => err.clone(),
    }
}

/// Validate the client's `Hello`. Returns `Ok` when versions match.
fn handle_hello(client_version: ProtocolVersion) -> Result<(), ProtocolError> {
    if client_version != PROTOCOL_VERSION {
        debug!(
            client = %client_version,
            daemon = %PROTOCOL_VERSION,
            "rejecting client with unsupported version"
        );
        return Err(ProtocolError::VersionMismatch(VersionMismatch {
            client: client_version,
            daemon: PROTOCOL_VERSION,
        }));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use artel_protocol::transport::client::connect;
    use artel_protocol::{PeerId, PeerInfo, RequestId};
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;
    use tokio::time::timeout;

    use super::*;

    fn unused_socket() -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf) {
        let dir = tempdir().unwrap();
        let sock = dir.path().join("daemon.sock");
        let pid = dir.path().join("daemon.pid");
        let sessions = dir.path().join("sessions");
        (dir, sock, pid, sessions)
    }

    fn config(sock: PathBuf, pid: PathBuf, sessions_dir: PathBuf) -> DaemonConfig {
        DaemonConfig {
            socket_path: sock,
            pid_path: pid,
            sessions_dir,
            daemon_peer_id: PeerId::from_bytes([0xee; 32]),
            iroh_key_path: None,
            address_lookup: None,
        }
    }

    #[tokio::test]
    async fn start_then_immediate_shutdown_is_clean() {
        let (_dir, sock, pid, sessions) = unused_socket();
        let daemon = Daemon::start(config(sock.clone(), pid.clone(), sessions.clone()))
            .await
            .unwrap();
        daemon.trigger_shutdown();
        let run = tokio::spawn(daemon.run());
        timeout(Duration::from_secs(2), run)
            .await
            .expect("daemon did not exit")
            .unwrap()
            .unwrap();

        // PID file removed, socket file removed.
        assert!(!pid.exists(), "pid file should be removed on shutdown");
        assert!(!sock.exists(), "socket file should be removed on shutdown");
    }

    #[tokio::test]
    async fn hello_succeeds_against_running_daemon() {
        let (_dir, sock, pid, sessions) = unused_socket();
        let daemon = Daemon::start(config(sock.clone(), pid, sessions.clone()))
            .await
            .unwrap();
        let shutdown_handle = Arc::clone(&daemon.shutdown);
        let run = tokio::spawn(daemon.run());

        // Connect and send Hello.
        let mut framed = connect(&sock).await.unwrap();
        framed
            .send(WireMessage::Request {
                id: RequestId::new(1),
                request: Request::Hello {
                    client_version: PROTOCOL_VERSION,
                },
            })
            .await
            .unwrap();
        let resp = timeout(Duration::from_secs(2), framed.next())
            .await
            .expect("response")
            .expect("frame")
            .unwrap();
        match resp {
            WireMessage::Response {
                id,
                response: Response::Hello { daemon_peer_id, .. },
            } => {
                assert_eq!(id, RequestId::new(1));
                assert_eq!(daemon_peer_id, PeerId::from_bytes([0xee; 32]));
            }
            other => panic!("expected Hello response, got {other:?}"),
        }

        drop(framed);
        shutdown_handle.trigger();
        timeout(Duration::from_secs(2), run)
            .await
            .expect("daemon did not exit")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn version_mismatch_returns_error_then_closes() {
        let (_dir, sock, pid, sessions) = unused_socket();
        let daemon = Daemon::start(config(sock.clone(), pid, sessions.clone()))
            .await
            .unwrap();
        let shutdown_handle = Arc::clone(&daemon.shutdown);
        let run = tokio::spawn(daemon.run());

        let mut framed = connect(&sock).await.unwrap();
        framed
            .send(WireMessage::Request {
                id: RequestId::new(1),
                request: Request::Hello {
                    client_version: ProtocolVersion::new(99),
                },
            })
            .await
            .unwrap();

        let resp = timeout(Duration::from_secs(2), framed.next())
            .await
            .expect("response")
            .expect("frame")
            .unwrap();
        match resp {
            WireMessage::Response {
                response:
                    Response::Error {
                        error: ProtocolError::VersionMismatch(_),
                    },
                ..
            } => {}
            other => panic!("expected version-mismatch error, got {other:?}"),
        }

        // Daemon should close the connection after the rejection.
        // Either clean EOF (None) or a transport error counts as
        // "closed"; only a delivered frame or a timeout indicates the
        // daemon is still alive on this connection.
        let after = timeout(Duration::from_secs(2), framed.next())
            .await
            .expect("connection did not close");
        match after {
            None | Some(Err(_)) => {}
            Some(Ok(other)) => panic!("expected EOF, got frame {other:?}"),
        }

        shutdown_handle.trigger();
        timeout(Duration::from_secs(2), run)
            .await
            .expect("daemon did not exit")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn host_then_list_round_trip() {
        let (_dir, sock, pid, sessions) = unused_socket();
        let daemon = Daemon::start(config(sock.clone(), pid, sessions.clone()))
            .await
            .unwrap();
        let shutdown_handle = Arc::clone(&daemon.shutdown);
        let run = tokio::spawn(daemon.run());

        let mut framed = connect(&sock).await.unwrap();
        // Hello.
        framed
            .send(WireMessage::Request {
                id: RequestId::new(1),
                request: Request::Hello {
                    client_version: PROTOCOL_VERSION,
                },
            })
            .await
            .unwrap();
        let _hello = framed.next().await.unwrap().unwrap();

        // HostSession.
        framed
            .send(WireMessage::Request {
                id: RequestId::new(2),
                request: Request::HostSession {
                    peer: PeerInfo::new(PeerId::from_bytes([1; 32]), "alice"),
                    session: None,
                },
            })
            .await
            .unwrap();
        let host_resp = framed.next().await.unwrap().unwrap();
        let session_id = match host_resp {
            WireMessage::Response {
                response: Response::HostSession { session, .. },
                ..
            } => session,
            other => panic!("expected HostSession response, got {other:?}"),
        };

        // ListSessions.
        framed
            .send(WireMessage::Request {
                id: RequestId::new(3),
                request: Request::ListSessions,
            })
            .await
            .unwrap();
        let list_resp = framed.next().await.unwrap().unwrap();
        match list_resp {
            WireMessage::Response {
                response: Response::ListSessions { sessions },
                ..
            } => {
                assert_eq!(sessions.len(), 1);
                assert_eq!(sessions[0].id, session_id);
            }
            other => panic!("expected ListSessions response, got {other:?}"),
        }

        drop(framed);
        shutdown_handle.trigger();
        timeout(Duration::from_secs(2), run)
            .await
            .expect("daemon did not exit")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn stale_socket_file_is_replaced_on_start() {
        // A crashed predecessor can leave the socket file behind. Once
        // we have the PID lock, the daemon should overwrite it rather
        // than fail with AddrInUse.
        let (_dir, sock, pid, sessions) = unused_socket();
        std::fs::create_dir_all(sock.parent().unwrap()).unwrap();
        std::fs::write(&sock, b"junk").unwrap();
        assert!(sock.exists());

        let daemon = Daemon::start(config(sock.clone(), pid, sessions))
            .await
            .unwrap();
        assert!(sock.exists(), "fresh socket should now exist");
        // Should be a real listening socket: a client can connect.
        let _ = artel_protocol::transport::client::connect(&sock)
            .await
            .unwrap();
        daemon.trigger_shutdown();
        let run = tokio::spawn(daemon.run());
        timeout(Duration::from_secs(2), run)
            .await
            .expect("daemon did not exit")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn second_daemon_on_same_pid_path_errors() {
        let (_dir, sock, pid, sessions) = unused_socket();
        let _first = Daemon::start(config(sock.clone(), pid.clone(), sessions.clone()))
            .await
            .unwrap();
        // Use a different socket path so we hit the PID check, not bind.
        let other_sock = sock.with_extension("other");
        let err = Daemon::start(config(other_sock, pid, sessions))
            .await
            .unwrap_err();
        assert!(
            matches!(err, StartError::Pid(PidError::AlreadyRunning { .. })),
            "{err:?}"
        );
    }
}
