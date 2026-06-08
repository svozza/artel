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
use std::time::Duration;

use artel_protocol::transport::{self, Framed, server::Listener};
use artel_protocol::{
    Capability, Event, PROTOCOL_VERSION, PeerInfo, ProtocolError, ProtocolVersion, Request,
    Response, SendPayload, SessionId, SessionMessage, VersionMismatch, WireMessage,
};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use tokio::net::UnixStream;
use tokio::sync::{Mutex as AsyncMutex, broadcast};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

#[cfg(feature = "iroh")]
use crate::endpoint_setup::EndpointSetup;
use crate::pidfile::{PidError, PidFile};
use crate::session::{Registry, SessionError, Subscription};
use crate::shutdown::{Shutdown, ShutdownToken};

/// How long the daemon waits for the home-relay handshake
/// (`endpoint.online()`) before surfacing
/// [`StartError::RelayUnreachable`]. Mirrors
/// `artel_fs::node::HOME_RELAY_BUDGET` — keep them in sync until
/// the [`EndpointSetup`] duplication (handoff finding #11) is
/// resolved and the constant can move alongside the shared enum.
#[cfg(feature = "iroh")]
const HOME_RELAY_BUDGET: Duration = Duration::from_secs(30);

/// Non-routable, non-authenticated id advertised in `Hello` when the
/// `iroh` feature is disabled at compile time.
///
/// Equal to `[0u8; 32]`; outbound gossip is impossible in that mode,
/// so the bytes serve only as a stable, obviously-synthetic
/// placeholder (better than per-process drift for embedders that
/// talk only to a local registry). Defined unconditionally so
/// intra-doc links resolve in either feature mode; only read in the
/// no-iroh `Daemon::start` path.
pub const SYNTHETIC_LOCAL_PEER_ID: artel_protocol::PeerId =
    artel_protocol::PeerId::from_bytes([0; 32]);

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
    /// Path to the persisted iroh secret key. When the `iroh` feature
    /// is enabled this is the only knob — the daemon loads (or
    /// generates) the key and uses the resulting `EndpointId` as its
    /// [`artel_protocol::PeerId`]. When `None` and `iroh` is on,
    /// `Daemon::start` returns [`StartError::Iroh`] (a daemon with no
    /// network identity is a configuration bug). When `iroh` is off,
    /// the field is ignored and the daemon advertises
    /// [`SYNTHETIC_LOCAL_PEER_ID`] (an all-zero, non-routable id) in
    /// `Hello`.
    pub iroh_key_path: Option<PathBuf>,
    /// Pick the iroh endpoint's discovery layer when the `iroh`
    /// feature is on and an [`Self::iroh_key_path`] is supplied.
    /// Real deployments use [`EndpointSetup::Production`] (default
    /// — `presets::N0`, pkarr publish + DNS resolve via n0
    /// infrastructure). Integration tests use
    /// [`EndpointSetup::Testing`] with a shared
    /// `Arc<DnsPkarrServer>` so two in-process daemons share a
    /// localhost pkarr+DNS pair instead of paying n0's external
    /// rate limits.
    ///
    /// Without the `iroh` feature this field is unconditionally
    /// present so callers can construct [`DaemonConfig`] with the
    /// same struct literal regardless of feature flags; the
    /// `Production` default works in either case (the value is
    /// just ignored in the no-iroh build).
    #[cfg(feature = "iroh")]
    pub endpoint_setup: EndpointSetup,
    /// Placeholder so the struct shape doesn't drift between
    /// feature flags. Always `()` without `iroh`.
    #[cfg(not(feature = "iroh"))]
    pub endpoint_setup: (),
}

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

    /// The home-relay handshake (`iroh::Endpoint::online`) didn't
    /// resolve within the configured budget. Surfaces when the
    /// configured relay is unreachable. The daemon fails fast
    /// instead of hanging in [`Daemon::start`] forever.
    #[cfg(feature = "iroh")]
    #[error("relay unreachable: home-relay handshake did not complete within {0:?}")]
    RelayUnreachable(std::time::Duration),
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
    /// the accept loop and the endpoint. `None` only briefly during
    /// construction (between `resolve_iroh_runtime` and `Daemon::start`
    /// spawning the router with all protocols registered).
    pub router: Option<iroh::protocol::Router>,
    /// In-memory address-lookup service the gossip bridge populates
    /// with each inbound ticket's wire-form addr before subscribing.
    /// Same instance lives in `endpoint.address_lookup()` so the
    /// inserts are visible to iroh's resolver chain immediately.
    /// Sidesteps the pkarr/DNS propagation race that otherwise
    /// pushes the joiner-side gossip subscribe to
    /// `JOIN_READY_TIMEOUT` whenever a fresh joiner dials a host
    /// whose pkarr publish hasn't propagated yet. See
    /// `crate::gossip_bridge::GossipBridge::join_session`.
    pub addr_hint: iroh::address_lookup::memory::MemoryLookup,
    /// Set of `EndpointId`s that have ever been seeded into
    /// [`Self::addr_hint`] in this daemon incarnation, plus those
    /// loaded from disk at startup. Drives the
    /// shutdown-snapshot: at graceful shutdown the daemon walks
    /// this set, looks up each id's current `RemoteInfo` from the
    /// endpoint, and persists the result to
    /// [`Self::peer_addr_cache`]. iroh 0.98.2 has no public
    /// `remote_info_iter`, so the daemon maintains this shadow.
    ///
    /// **Invariant**: every `addr_hint.add_endpoint_info(addr)` call
    /// must be paired with a `tracked_peer_ids.insert(addr.id)` so
    /// the snapshot path can find the peer at shutdown. The
    /// gossip-bridge upholds the pairing in `join_session`.
    pub tracked_peer_ids: Arc<std::sync::Mutex<std::collections::BTreeSet<iroh::EndpointId>>>,
    /// On-disk peer-addr cache. Populated at startup (entries
    /// seeded into [`Self::addr_hint`]) and overwritten at graceful
    /// shutdown with a fresh snapshot of [`Self::tracked_peer_ids`].
    pub(crate) peer_addr_cache: Arc<crate::peer_addr_cache::PeerAddrCache>,
    /// Cloned `iroh::SecretKey` retained alongside the Endpoint so
    /// the daemon can sign session messages (Auth Slice B). Held as
    /// `Arc` so the registry + gossip bridge each hold a cheap
    /// refcounted handle and the bytes never have to be re-loaded
    /// from disk. iroh's `SecretKey` is a thin wrapper around
    /// `[u8; 32]` and is `Clone` for free; `Arc` is here only to
    /// flatten the lifetime to "as long as the daemon is up".
    pub(crate) signing_key: Arc<iroh::SecretKey>,
}

#[cfg(feature = "iroh")]
impl IrohRuntime {
    /// Borrow the daemon's signing key. Used by [`Registry::send`]
    /// and the gossip bridge to produce per-message ed25519
    /// signatures (Auth Slice B). The same bytes are inside
    /// `endpoint.secret_key()`; we mirror them here so the registry
    /// doesn't have to depend on the iroh `Endpoint` type.
    pub(crate) fn signing_key(&self) -> Arc<iroh::SecretKey> {
        self.signing_key.clone()
    }
}

/// A running daemon. Hold the value to keep the daemon alive; drop it
/// to release the PID file and unbind the socket.
#[derive(Debug)]
pub struct Daemon {
    registry: Arc<Registry>,
    listener: Listener,
    pid: PidFile,
    shutdown: Arc<Shutdown>,
    /// iroh state. Required when the feature is on; populated by
    /// [`resolve_iroh_runtime`] in [`Self::start`], which fails fast
    /// if no [`DaemonConfig::iroh_key_path`] was supplied. We hold it
    /// for the daemon's lifetime; teardown happens in [`Self::run`]
    /// before returning.
    #[cfg(feature = "iroh")]
    iroh: IrohRuntime,
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

        // Iroh on: load key, bind endpoint, EndpointId -> PeerId.
        // Off: synthetic id, no runtime.
        #[cfg(feature = "iroh")]
        let (daemon_peer_id, mut iroh) =
            resolve_iroh_runtime(config.iroh_key_path.as_deref(), &config.endpoint_setup).await?;
        #[cfg(not(feature = "iroh"))]
        let daemon_peer_id = SYNTHETIC_LOCAL_PEER_ID;

        // Snapshot the daemon's network addr so the registry can
        // stamp it into outbound tickets.
        #[cfg(feature = "iroh")]
        let daemon_addr = iroh_endpoint_to_wire(&iroh.endpoint.addr());
        #[cfg(not(feature = "iroh"))]
        let daemon_addr = artel_protocol::WireEndpointAddr::id_only(daemon_peer_id);

        // Build the gossip bridge once we have the runtime. Lives
        // for the daemon's lifetime; sessions register themselves
        // with it as they're hosted/joined. The `addr_hint`
        // [`MemoryLookup`] is shared by reference: same instance
        // lives in `endpoint.address_lookup()` so adds via the
        // bridge are visible to iroh's resolver chain immediately.
        #[cfg(feature = "iroh")]
        let bridge = Arc::new(crate::gossip_bridge::GossipBridge::new(
            iroh.gossip.clone(),
            iroh.addr_hint.clone(),
            Arc::clone(&iroh.tracked_peer_ids),
            iroh.endpoint.id(),
            iroh.signing_key(),
        ));

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
                Some(Arc::clone(&bridge)),
                #[cfg(feature = "iroh")]
                Some(iroh.signing_key()),
                #[cfg(feature = "iroh")]
                Some(iroh.endpoint.clone()),
            )
            .await
            .map_err(StartError::LoadSessions)?,
        );

        // Inject the back-reference now that the registry is in an
        // Arc. The bridge holds it as a Weak so we don't form a
        // cycle. Without this the host-side forwarder has no way to
        // call back into `Registry::send` for inbound SendRequests.
        #[cfg(feature = "iroh")]
        bridge.attach_registry(Arc::downgrade(&registry)).await;

        // Build and spawn the Router now that the Registry is available.
        // UpgradeProtocol needs the Registry to emit upgrade events into
        // session broadcast channels; the gossip ALPN is the only other
        // protocol on the daemon's endpoint.
        #[cfg(feature = "iroh")]
        {
            let upgrade_proto =
                crate::upgrade_protocol::UpgradeProtocol::new(Arc::clone(&registry));
            let router = iroh::protocol::Router::builder(iroh.endpoint.clone())
                .accept(iroh_gossip::ALPN, iroh.gossip.clone())
                .accept(crate::upgrade_protocol::UpgradeProtocol::alpn(), upgrade_proto)
                .spawn();
            iroh.router = Some(router);
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

    /// iroh runtime backing the daemon. Always present under the
    /// `iroh` feature: [`Self::start`] fails fast if no
    /// [`DaemonConfig::iroh_key_path`] was supplied.
    ///
    /// Exposed so embedders and integration tests can talk to the
    /// daemon's `Endpoint` and `Gossip` directly. Phase 2c-2 will
    /// keep this surface but route most session traffic through
    /// `Registry`.
    #[cfg(feature = "iroh")]
    #[must_use]
    pub const fn iroh(&self) -> &IrohRuntime {
        &self.iroh
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
        //
        // Before tearing down: snapshot the per-peer addrs iroh has
        // learned this incarnation so the next daemon startup can
        // seed `addr_hint` and skip the pkarr/DNS race that
        // otherwise breaks post-restart sync (handoff finding #5c).
        // The snapshot must run BEFORE `router.shutdown()` because
        // that closes the endpoint and `remote_info` returns `None`
        // afterwards.
        #[cfg(feature = "iroh")]
        {
            let IrohRuntime {
                router,
                endpoint,
                tracked_peer_ids,
                peer_addr_cache,
                ..
            } = iroh;
            snapshot_peer_addrs(&endpoint, &tracked_peer_ids, &peer_addr_cache).await;
            if let Some(router) = router
                && let Err(err) = router.shutdown().await
            {
                warn!(error = %err, "iroh router shutdown failed");
            }
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
#[allow(clippy::too_many_lines)]
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
        Request::HostSession {
            display_name,
            session,
        } => dispatch_host(registry, display_name, session, memberships).await,
        Request::JoinSession {
            display_name,
            ticket,
        } => dispatch_join(registry, display_name, ticket, memberships).await,
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
                .send(
                    session,
                    peer,
                    kind,
                    action,
                    payload,
                    crate::session::Authoring::Local,
                )
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
        req @ (Request::RegisterAttachment { .. }
        | Request::ListAttachments { .. }
        | Request::ForgetAttachment { .. }) => dispatch_attachment(registry, req).await,
        Request::IssueTicket {
            session,
            granted_cap,
            expiry_ms,
        } => {
            if !memberships.contains_key(&session) {
                return Response::Error {
                    error: ProtocolError::NotSubscribed(session),
                };
            }
            match registry.issue_ticket(session, granted_cap, expiry_ms).await {
                Ok(ticket) => Response::IssuedTicket { ticket },
                Err(err) => Response::Error {
                    error: session_error_to_protocol(&err),
                },
            }
        }
        #[cfg(feature = "iroh")]
        Request::DeliverUpgrade {
            session,
            target_peer,
            namespace_secret,
        } => {
            if !memberships.contains_key(&session) {
                return Response::Error {
                    error: ProtocolError::NotSubscribed(session),
                };
            }
            dispatch_deliver_upgrade(registry, session, target_peer, namespace_secret).await
        }
        #[cfg(not(feature = "iroh"))]
        Request::DeliverUpgrade { .. } => Response::Error {
            error: ProtocolError::Internal(
                "DeliverUpgrade requires the iroh feature".into(),
            ),
        },
    }
}

/// Stamp the daemon's authenticated `PeerId` and route to
/// `Registry::host`. Pulled out of `dispatch` so the parent stays
/// under clippy's `too_many_lines` cap (auth L1 fix #3,
/// `PROTOCOL_VERSION` 5).
async fn dispatch_host(
    registry: &Registry,
    display_name: String,
    session: Option<SessionId>,
    memberships: &mut HashMap<SessionId, PeerInfo>,
) -> Response {
    let peer = PeerInfo {
        id: registry.daemon_peer_id(),
        display_name,
    };
    match registry.host(peer.clone(), session, Capability::ReadWrite, 0).await {
        Ok((session, ticket)) => {
            memberships.insert(session, peer);
            Response::HostSession { session, ticket }
        }
        Err(err) => Response::Error {
            error: session_error_to_protocol(&err),
        },
    }
}

/// Stamp the daemon's authenticated `PeerId` and route to
/// `Registry::join`. Counterpart to `dispatch_host`.
async fn dispatch_join(
    registry: &Registry,
    display_name: String,
    ticket: artel_protocol::JoinTicket,
    memberships: &mut HashMap<SessionId, PeerInfo>,
) -> Response {
    let peer = PeerInfo {
        id: registry.daemon_peer_id(),
        display_name,
    };
    match registry.join(&ticket, peer.clone()).await {
        Ok((session, head)) => {
            memberships.insert(session, peer);
            Response::JoinSession { session, head }
        }
        Err(err) => Response::Error {
            error: session_error_to_protocol(&err),
        },
    }
}

/// Deliver the `NamespaceSecret` to a target peer over a direct QUIC
/// stream. Only the host of a Local session may call this. The daemon
/// dials the target via `Endpoint::connect`, sends a length-prefixed
/// `UpgradeFrame`, and reads a 1-byte ACK.
#[cfg(feature = "iroh")]
async fn dispatch_deliver_upgrade(
    registry: &Registry,
    session: SessionId,
    target_peer: artel_protocol::PeerId,
    namespace_secret: [u8; 32],
) -> Response {
    use artel_protocol::upgrade::{UPGRADE_ACK, UPGRADE_ALPN, UpgradeFrame};

    // Verify session exists and is Local (we are the host).
    match registry.is_local_session(session).await {
        Some(true) => {}
        Some(false) => {
            return Response::Error {
                error: ProtocolError::NotHost,
            };
        }
        None => {
            return Response::Error {
                error: ProtocolError::UnknownSession(session),
            };
        }
    }

    // Dial the target peer. PeerId IS EndpointId (same 32-byte key).
    let target_endpoint_id =
        iroh::EndpointId::from_bytes(target_peer.as_bytes()).expect("PeerId is 32 bytes");
    let Some(endpoint) = registry.endpoint() else {
        return Response::Error {
            error: ProtocolError::Internal("no iroh endpoint available".into()),
        };
    };

    let connection = match endpoint.connect(target_endpoint_id, UPGRADE_ALPN).await {
        Ok(conn) => conn,
        Err(e) => {
            warn!(error = %e, %target_peer, "deliver_upgrade: connect failed");
            return Response::Error {
                error: ProtocolError::Internal(format!("connect to target peer failed: {e}")),
            };
        }
    };

    let (mut send, mut recv) = match connection.open_bi().await {
        Ok(pair) => pair,
        Err(e) => {
            warn!(error = %e, "deliver_upgrade: open_bi failed");
            return Response::Error {
                error: ProtocolError::Internal(format!("open_bi failed: {e}")),
            };
        }
    };

    // Send length-prefixed UpgradeFrame.
    let frame = UpgradeFrame {
        session_id: session,
        namespace_secret,
    };
    let frame_bytes = postcard::to_allocvec(&frame).expect("UpgradeFrame is infallible");
    #[allow(clippy::cast_possible_truncation)] // UpgradeFrame is ~48 bytes, never exceeds u32
    let len = (frame_bytes.len() as u32).to_le_bytes();

    if let Err(e) = send.write_all(&len).await {
        warn!(error = %e, "deliver_upgrade: write length failed");
        return Response::Error {
            error: ProtocolError::Internal(format!("write frame length failed: {e}")),
        };
    }
    if let Err(e) = send.write_all(&frame_bytes).await {
        warn!(error = %e, "deliver_upgrade: write frame failed");
        return Response::Error {
            error: ProtocolError::Internal(format!("write frame failed: {e}")),
        };
    }
    let _ = send.finish();

    // Read ACK.
    let mut ack_buf = [0u8; 1];
    match recv.read_exact(&mut ack_buf).await {
        Ok(()) if ack_buf[0] == UPGRADE_ACK => Response::UpgradeDelivered,
        Ok(()) => {
            warn!(byte = ack_buf[0], "deliver_upgrade: unexpected ACK byte");
            Response::Error {
                error: ProtocolError::Internal("unexpected ACK byte from target".into()),
            }
        }
        Err(e) => {
            warn!(error = %e, "deliver_upgrade: read ACK failed");
            Response::Error {
                error: ProtocolError::Internal(format!("read ACK failed: {e}")),
            }
        }
    }
}

/// Dispatch the three attachment RPCs. Pulled out of `dispatch` so
/// the parent function stays under clippy's `too_many_lines` cap;
/// the attachment surface is independent of session membership and
/// has no business sharing the membership-tracking machinery.
async fn dispatch_attachment(registry: &Registry, request: Request) -> Response {
    match request {
        Request::RegisterAttachment {
            session,
            kind,
            payload,
        } => match registry.register_attachment(session, kind, payload).await {
            Ok(()) => Response::AttachmentRegistered,
            Err(err) => Response::Error {
                error: session_error_to_protocol(&err),
            },
        },
        Request::ListAttachments { kind } => {
            match registry.list_attachments(kind.as_deref()).await {
                Ok(stored) => Response::Attachments {
                    entries: stored
                        .into_iter()
                        .map(|s| artel_protocol::Attachment {
                            session: s.session,
                            kind: s.kind,
                            payload: s.payload,
                        })
                        .collect(),
                },
                Err(err) => Response::Error {
                    error: session_error_to_protocol(&err),
                },
            }
        }
        Request::ForgetAttachment { session, kind } => {
            match registry.forget_attachment(session, kind).await {
                Ok(()) => Response::AttachmentForgotten,
                Err(err) => Response::Error {
                    error: session_error_to_protocol(&err),
                },
            }
        }
        // dispatch_attachment is only called from `dispatch` with one
        // of the three attachment variants. Other variants would be a
        // routing bug — surface as Internal so the client gets a clear
        // error instead of a panic.
        other => Response::Error {
            error: ProtocolError::Internal(format!(
                "dispatch_attachment called with non-attachment variant: {other:?}",
            )),
        },
    }
}

/// Stand up the daemon's iroh runtime: load (or generate) the secret
/// key, bind an `Endpoint`, spawn a `Gossip` instance attached to it,
/// and start a protocol `Router` accepting the gossip ALPN. Returns
/// the resulting `EndpointId` (cast to [`PeerId`]) plus the bundle.
///
/// `key_path` is required: a daemon with the `iroh` feature on but
/// no key path is a configuration bug (no network identity, no way
/// to host or join sessions). [`StartError::Iroh`] surfaces it as a
/// fail-fast.
#[cfg(feature = "iroh")]
async fn resolve_iroh_runtime(
    key_path: Option<&std::path::Path>,
    setup: &EndpointSetup,
) -> Result<(artel_protocol::PeerId, IrohRuntime), StartError> {
    let path = key_path
        .ok_or_else(|| StartError::Iroh("iroh feature is on but no iroh_key_path given".into()))?;
    let secret =
        crate::iroh_key::load_or_create(path).map_err(|e| StartError::Iroh(e.to_string()))?;
    // `iroh::SecretKey` is a 32-byte wrapper that is `Clone`; we keep
    // a copy on `IrohRuntime` so the registry / gossip bridge can
    // sign session messages (Auth Slice B) without the
    // `Endpoint::secret_key` round-trip. The Endpoint takes the other
    // copy by value via `secret_key()`.
    let signing_key = Arc::new(secret.clone());
    // Start from `presets::Empty` so the `EndpointSetup::apply` chain
    // has full control over which discovery preset gets layered in.
    let endpoint = setup
        .apply(iroh::Endpoint::builder(iroh::endpoint::presets::Empty).secret_key(secret))
        .bind()
        .await
        .map_err(|e| StartError::Iroh(format!("bind endpoint: {e}")))?;
    let peer_id = artel_protocol::PeerId::from_bytes(*endpoint.id().as_bytes());

    // Install a per-daemon `MemoryLookup` alongside the configured
    // pkarr/DNS chain. The bridge holds a clone and populates it
    // with each inbound ticket's wire-form addr before subscribing
    // to a session's gossip topic — bypassing the propagation race
    // that otherwise leaves a fresh joiner waiting on pkarr+DNS for
    // up to `JOIN_READY_TIMEOUT`. The lookup adds zero cost when
    // it's empty (iroh's resolver chain just falls through to the
    // next service) so installing it unconditionally is safe across
    // both `EndpointSetup::Production` and `EndpointSetup::Testing`.
    let addr_hint = iroh::address_lookup::memory::MemoryLookup::with_provenance("artel-ticket");
    endpoint
        .address_lookup()
        .map_err(|e| StartError::Iroh(format!("address_lookup: {e}")))?
        .add(addr_hint.clone());

    // Restore peer addrs the previous incarnation of this daemon
    // learned. Without this, a host restart loses every peer addr
    // iroh was holding in memory — iroh-docs's persistent doc store
    // keeps id-only `EndpointAddr`s and the post-restart dial races
    // pkarr/DNS to find peers (handoff finding #5c). Loading is
    // best-effort: a missing or corrupt cache yields an empty seed,
    // identical to pre-fix behaviour.
    let tracked_peer_ids = Arc::new(std::sync::Mutex::new(std::collections::BTreeSet::<
        iroh::EndpointId,
    >::new()));
    let peer_addr_cache = Arc::new(crate::peer_addr_cache::PeerAddrCache::new(
        peer_addr_cache_path(path),
    ));
    for entry in peer_addr_cache.load() {
        match crate::peer_addr_cache::iroh_addr_from_entry(&entry) {
            Ok(iroh_addr) => {
                tracked_peer_ids
                    .lock()
                    .expect("poisoned")
                    .insert(iroh_addr.id);
                addr_hint.add_endpoint_info(iroh_addr);
            }
            Err(err) => {
                warn!(error = %err, "peer_addr_cache: skipping invalid entry");
            }
        }
    }

    // Mirror `WorkspaceNode::spawn`: block on home-relay readiness
    // when the configured setup uses one, so the daemon doesn't
    // accept IPC and start broadcasting on gossip before its first
    // dial out can complete. The timeout exists for the same
    // reason it does on the workspace side — failing fast on an
    // unreachable relay (offline laptop, captive portal, n0
    // outage, or the `TestingUnreachableRelay` fixture) instead of
    // hanging `Daemon::start` forever.
    if setup.awaits_relay()
        && tokio::time::timeout(HOME_RELAY_BUDGET, endpoint.online())
            .await
            .is_err()
    {
        return Err(StartError::RelayUnreachable(HOME_RELAY_BUDGET));
    }

    // Gossip needs a clone of the endpoint to register itself for the
    // ALPN; the Router is built later in Daemon::start (after the
    // Registry exists) so UpgradeProtocol can be registered on it.
    let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());

    Ok((
        peer_id,
        IrohRuntime {
            endpoint,
            gossip,
            router: None,
            addr_hint,
            tracked_peer_ids,
            peer_addr_cache,
            signing_key,
        },
    ))
}

/// Path the peer-addr cache lives at, derived from the daemon's
/// `iroh_key` path. Same parent dir as the secret key — convention
/// matches the rest of the daemon's per-state-dir files.
#[cfg(feature = "iroh")]
fn peer_addr_cache_path(iroh_key_path: &std::path::Path) -> PathBuf {
    iroh_key_path.parent().map_or_else(
        || PathBuf::from("peer_addrs.postcard"),
        |p| p.join("peer_addrs.postcard"),
    )
}

/// Walk every tracked `EndpointId`, ask iroh's endpoint for its
/// current addr info, and persist the result. Called once at
/// graceful shutdown, BEFORE `router.shutdown()` (which closes the
/// endpoint).
///
/// Best-effort throughout: a peer the endpoint has no info for is
/// silently skipped, persistence errors log via `tracing::warn!`
/// but never block daemon exit.
#[cfg(feature = "iroh")]
async fn snapshot_peer_addrs(
    endpoint: &iroh::Endpoint,
    tracked_peer_ids: &Arc<std::sync::Mutex<std::collections::BTreeSet<iroh::EndpointId>>>,
    peer_addr_cache: &crate::peer_addr_cache::PeerAddrCache,
) {
    let ids: Vec<iroh::EndpointId> = tracked_peer_ids
        .lock()
        .expect("poisoned")
        .iter()
        .copied()
        .collect();
    debug!(
        count = ids.len(),
        "peer_addr_cache: snapshotting tracked peers"
    );
    let mut entries = Vec::with_capacity(ids.len());
    for id in ids {
        let Some(info) = endpoint.remote_info(id).await else {
            debug!(peer = %id, "peer_addr_cache: no remote_info, skipping");
            continue;
        };
        // Reconstruct an `EndpointAddr` from the per-peer
        // `RemoteInfo`. The doc-comment example on
        // `RemoteInfo::into_addrs` shows this exact pattern.
        let iroh_addr = iroh::EndpointAddr::from_parts(
            info.id(),
            info.into_addrs()
                .map(iroh::endpoint::TransportAddrInfo::into_addr),
        );
        entries.push(crate::peer_addr_cache::entry_from_iroh(&iroh_addr));
    }
    peer_addr_cache.save(entries);
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
        SessionError::InvalidTicket | SessionError::TicketExpired => ProtocolError::InvalidTicket,
        SessionError::Storage(io_err) => ProtocolError::Internal(format!("storage: {io_err}")),
        SessionError::InvalidAddr(msg) => ProtocolError::Internal(format!("invalid addr: {msg}")),
        SessionError::Internal(msg) => ProtocolError::Internal(msg.clone()),
        SessionError::NotHost => ProtocolError::NotHost,
        SessionError::SessionConflict(s) => ProtocolError::SessionConflict(*s),
        // Forward the host's verdict verbatim so the IPC client
        // sees the actual reason (e.g., UnknownSession after a
        // session close) instead of a generic Internal.
        SessionError::HostRejected(err) => err.clone(),
        SessionError::SignatureRejected { peer_id, reason } => {
            ProtocolError::Signature(format!("{peer_id}: {reason}"))
        }
        // L2 capability denial (Auth Slice C): the joiner sees this as
        // `ProtocolError::Capability` so they can distinguish a cap
        // failure from a signature failure or a generic Internal.
        SessionError::CapabilityDenied {
            peer_id,
            had,
            needed,
        } => ProtocolError::Capability(format!("{peer_id}: had {had:?}, needs {needed:?}")),
        SessionError::InvalidCapClaim(reason) => {
            ProtocolError::Internal(format!("invalid cap claim: {reason}"))
        }
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

// Most in-module tests were removed in auth-L1/A2 — they relied on
// `iroh_key_path: None` + synthetic peer id, a path that no longer
// exists under the iroh feature. Coverage now lives in the
// integration suite (`tests/{sessions,attachments,identity,gossip,auth_l1_spoofing}.rs`,
// `artel-fs/tests/`).

#[cfg(all(test, feature = "iroh", feature = "test-utils"))]
mod tests {
    use std::sync::Arc;

    use iroh::test_utils::DnsPkarrServer;
    use pretty_assertions::assert_eq;

    use super::*;

    /// Pin the load-bearing invariant for Auth Slice B: the bytes
    /// retained on `IrohRuntime` for signing match the bytes the
    /// `Endpoint` is using as its identity. If these ever drift,
    /// daemons would sign with one key and authenticate as another —
    /// every receiver would reject the body as `BadSig` and the
    /// failure would only surface in flaky end-to-end traffic.
    #[tokio::test]
    async fn iroh_runtime_signing_key_matches_endpoint() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("iroh.key");
        // Hermetic relay/pkarr so we don't reach n0; the test is
        // about the *key bytes*, not network traffic.
        let dns_pkarr = Arc::new(DnsPkarrServer::run().await.expect("dns_pkarr server"));
        let setup = EndpointSetup::Testing {
            dns_pkarr: Arc::clone(&dns_pkarr),
        };
        let (_peer_id, runtime) = resolve_iroh_runtime(Some(&key_path), &setup)
            .await
            .expect("runtime");

        // The Arc on IrohRuntime holds the same 32 bytes as the
        // Endpoint's secret_key — both are clones of the bytes loaded
        // from disk.
        let on_runtime = runtime.signing_key().to_bytes();
        let on_endpoint = runtime.endpoint.secret_key().to_bytes();
        assert_eq!(on_runtime, on_endpoint);
        // And both equal what was written to disk; load_or_create is
        // identity for an existing key file.
        let on_disk = crate::iroh_key::load_or_create(&key_path).unwrap();
        assert_eq!(on_runtime, on_disk.to_bytes());

        // Tear down to avoid leaking the iroh router's accept loop.
        if let Some(router) = runtime.router {
            router.shutdown().await.expect("router shutdown");
        }
    }
}
