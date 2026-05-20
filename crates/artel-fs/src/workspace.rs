//! `Workspace` — an attached, replicating workspace over an artel
//! session.
//!
//! Two modes, one type:
//! - [`Workspace::host`] creates a fresh `iroh-docs` document, scans
//!   the supplied directory, publishes its files into the doc, and
//!   broadcasts the resulting [`DocTicket`] over the artel session
//!   as a [`MessageKind::System`] message with action
//!   [`TICKET_ACTION`].
//! - [`Workspace::join`] (added in 3a-4) listens for the system
//!   message, imports the ticket, and bulk-exports the doc to its
//!   own copy of the directory.
//!
//! The watcher / applier that turn this into live two-way sync land
//! in 3a-5. This file gets us as far as "host stands up, ticket lands
//! on the session."

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use artel_client::Client;
use artel_protocol::{Event, MessageKind, Request, Response, SendPayload, SessionId};
use bytes::Bytes;
use futures_util::StreamExt;
use iroh_docs::AuthorId;
use iroh_docs::DocTicket;
use iroh_docs::api::Doc;
use iroh_docs::api::protocol::{AddrInfoOptions, ShareMode};
use iroh_docs::engine::LiveEvent;
use iroh_docs::store::Query;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use walkdir::WalkDir;

use crate::echo_guard::EchoGuard;
use crate::error::WorkspaceError;
use crate::filter::{FilterDecision, SkipReason, WorkspaceFilter};
use crate::keys;
use crate::node::WorkspaceNode;

/// Action stamped on the `MessageKind::System` ticket-handout
/// message. Joiners filter on this to find the ticket inside the
/// session's event stream.
pub const TICKET_ACTION: &str = "workspace.ticket";

/// Capacity of the [`Workspace::events`] channel. Modest cap so a
/// stuck consumer back-pressures the watcher rather than letting
/// events queue without bound.
const EVENT_BUFFER: usize = 64;

/// Out-of-band signal surfaced to the consumer of a [`Workspace`].
///
/// Live-loop errors land here rather than as `Result` returns from
/// constructors; the constructor either succeeded or it didn't, and
/// once we're running we don't want a single stat failure to take
/// the whole workspace down.
#[derive(Clone, Debug)]
pub enum WorkspaceEvent {
    /// A peer's write landed on disk under [`Workspace::root`].
    PeerWrote {
        /// Absolute path under the workspace root.
        path: PathBuf,
    },
    /// A peer-driven delete removed a file from the workspace.
    PeerDeleted {
        /// Absolute path under the workspace root.
        path: PathBuf,
    },
    /// A file was skipped because it exceeded the size cap.
    /// Surfaces both directions: outgoing scans and incoming peer
    /// writes.
    SkippedTooLarge {
        /// Absolute path that was too big.
        path: PathBuf,
        /// Actual size in bytes.
        size: u64,
    },
    /// Non-fatal error in the live loop. Logged for the consumer;
    /// the workspace keeps running.
    Error(String),
}

/// A live, attached filesystem workspace.
///
/// Construct via [`Self::host`] or [`Self::join`]. Hold the value
/// to keep the underlying iroh node alive; drop it (or call
/// [`Self::shutdown`]) to tear down.
///
/// Spawn the watcher + applier with [`Self::run`].
#[derive(Debug)]
pub struct Workspace {
    /// Absolute path of the directory being mirrored.
    pub root: PathBuf,
    /// Doc handle. The watcher writes to it; the applier reads from
    /// `doc.subscribe()`.
    pub(crate) doc: Doc,
    /// Author id used to stamp our writes.
    pub(crate) author: AuthorId,
    /// Blobs API for fetching content the doc references. Cloned
    /// out of the iroh node so the applier can `get_bytes` without
    /// taking the node lock.
    pub(crate) blobs: iroh_blobs::BlobsProtocol,
    /// Echo guard shared between the watcher and applier.
    pub(crate) echo_guard: EchoGuard,
    /// Sender side of the [`WorkspaceEvent`] mpsc. Held by the
    /// workspace so background tasks can clone it cheaply.
    pub(crate) events: mpsc::Sender<WorkspaceEvent>,
    /// Cancellation token tripped by [`Self::shutdown`] to stop the
    /// background tasks.
    pub(crate) shutdown_token: CancellationToken,
    /// The per-workspace iroh runtime. Owned so its `Drop` runs when
    /// the workspace goes out of scope. `Mutex<Option<...>>` so
    /// [`Self::shutdown`] can take it and consume it without
    /// requiring `&mut self`.
    pub(crate) node: tokio::sync::Mutex<Option<WorkspaceNode>>,
}

impl Workspace {
    /// Stand the workspace up as the host on `session`.
    ///
    /// Steps:
    /// 1. Spawn a fresh iroh node (Endpoint + Gossip + Docs/Blobs +
    ///    Router) — see [`WorkspaceNode`].
    /// 2. Create a writeable `iroh-docs` document and an author id.
    /// 3. Walk `root`, publish every non-skipped file into the doc.
    /// 4. Share the doc as a `DocTicket` and broadcast it over the
    ///    artel session as a [`MessageKind::System`] message with
    ///    action [`TICKET_ACTION`].
    ///
    /// Returns the [`Workspace`] handle plus the receiver side of
    /// the [`WorkspaceEvent`] stream. The watcher / applier are not
    /// running yet — that's slice 3a-5.
    pub async fn host(
        client: &Client,
        session: SessionId,
        root: PathBuf,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
        let root = canonicalise(&root);
        let node = WorkspaceNode::spawn().await?;

        let author = node
            .docs
            .author_create()
            .await
            .map_err(|e| WorkspaceError::Doc(format!("author_create: {e}")))?;
        let doc = node
            .docs
            .create()
            .await
            .map_err(|e| WorkspaceError::Doc(format!("doc create: {e}")))?;

        let (tx, rx) = mpsc::channel(EVENT_BUFFER);
        let echo_guard = EchoGuard::new();

        // Pre-populate the doc from disk *before* we share the
        // ticket — joiners that import after this scan see the
        // current snapshot via initial sync.
        scan_and_publish_existing(&root, &doc, author, &node.blobs, &echo_guard, &tx).await?;

        // Share with full addressing info so the ticket is enough
        // for joiners to dial without out-of-band addr seeding (the
        // bet 3a-1 verified).
        let ticket = doc
            .share(ShareMode::Write, AddrInfoOptions::default())
            .await
            .map_err(|e| WorkspaceError::Doc(format!("share doc: {e}")))?;

        publish_ticket(client, session, &ticket).await?;

        let blobs = node.blobs.clone();
        Ok((
            Self {
                root,
                doc,
                author,
                blobs,
                echo_guard,
                events: tx,
                shutdown_token: CancellationToken::new(),
                node: tokio::sync::Mutex::new(Some(node)),
            },
            rx,
        ))
    }

    /// Attach to an existing workspace as a joiner.
    ///
    /// `client` must already be a member of `session` (via
    /// [`Request::JoinSession`] or [`Request::HostSession`]). The
    /// workspace internally:
    ///
    /// 1. Spawns its own iroh node.
    /// 2. Issues `Subscribe { since: None }` so the joiner backfills
    ///    the existing session log via Phase 2 follow-up (c)'s
    ///    replay path. The replay surfaces the host's
    ///    `workspace.ticket` system message even if the joiner
    ///    arrived after it was originally broadcast.
    /// 3. Drains events until the ticket arrives (15 s ceiling).
    /// 4. Imports the ticket into the joiner's local doc, runs
    ///    `bulk_export` to seed `root` with whatever's already in
    ///    the doc, and returns.
    ///
    /// Watcher / applier are *not* started yet — that's slice 3a-5.
    ///
    /// **Side effect:** consumes the client's [`Client::take_events`]
    /// channel. Callers that need to observe other session events
    /// from the same connection should open a second [`Client`].
    pub async fn join(
        client: &Client,
        session: SessionId,
        root: PathBuf,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
        let root = canonicalise(&root);
        tokio::fs::create_dir_all(&root).await?;

        let node = WorkspaceNode::spawn().await?;
        let author = node
            .docs
            .author_create()
            .await
            .map_err(|e| WorkspaceError::Doc(format!("author_create: {e}")))?;

        // Subscribe and drain until the ticket arrives. Phase 2
        // follow-up (c) (subscribe replay) means a fresh subscriber
        // sees historical messages on subscribe, so a joiner that
        // hosts after the ticket was published still picks it up.
        match client
            .request(Request::Subscribe {
                session,
                since: None,
            })
            .await?
        {
            Response::Subscribed { .. } => {}
            other => {
                return Err(WorkspaceError::Iroh(format!(
                    "unexpected response to Subscribe: {other:?}",
                )));
            }
        }
        let mut events = client
            .take_events()
            .await
            .ok_or_else(|| WorkspaceError::Iroh("client events already taken".into()))?;

        let ticket_bytes = wait_for_ticket(&mut events, session).await?;
        let ticket_str = std::str::from_utf8(&ticket_bytes)
            .map_err(|e| WorkspaceError::Doc(format!("ticket payload not utf-8: {e}")))?;
        let ticket = DocTicket::from_str(ticket_str)
            .map_err(|e| WorkspaceError::Doc(format!("ticket parse: {e}")))?;

        let (doc, live) = node
            .docs
            .import_and_subscribe(ticket)
            .await
            .map_err(|e| WorkspaceError::Doc(format!("doc import: {e}")))?;

        // Drain live events until the first sync round has finished
        // and pending content has settled. Without this, `get_many`
        // returns an empty result and the bulk export is a no-op
        // because the doc state hasn't replicated yet.
        wait_for_initial_sync(live).await?;

        let (tx, rx) = mpsc::channel(EVENT_BUFFER);
        let echo_guard = EchoGuard::new();

        bulk_export(&root, &doc, &node.blobs, &echo_guard, &tx).await?;

        let blobs = node.blobs.clone();
        Ok((
            Self {
                root,
                doc,
                author,
                blobs,
                echo_guard,
                events: tx,
                shutdown_token: CancellationToken::new(),
                node: tokio::sync::Mutex::new(Some(node)),
            },
            rx,
        ))
    }

    /// Borrow the underlying `iroh-docs` document.
    ///
    /// Exposed for diagnostics and tests — the watcher / applier
    /// drive it internally. Apps shouldn't normally need to write
    /// to it directly; use the filesystem path instead.
    #[must_use]
    pub const fn doc(&self) -> &Doc {
        &self.doc
    }

    /// Spawn the watcher + applier background tasks.
    ///
    /// - The watcher debounces filesystem events under [`Self::root`]
    ///   and publishes them into the doc.
    /// - The applier subscribes to [`Doc::subscribe`] and applies
    ///   `InsertRemote` / `ContentReady` events to disk.
    ///
    /// Both tasks honour the workspace's shutdown token. The
    /// returned `JoinHandle` resolves once both have exited.
    #[must_use]
    pub fn run(self: std::sync::Arc<Self>) -> tokio::task::JoinHandle<()> {
        let watcher_ws = std::sync::Arc::clone(&self);
        let applier_ws = std::sync::Arc::clone(&self);
        tokio::spawn(async move {
            let watcher = tokio::spawn(crate::watcher::run(watcher_ws));
            let applier = tokio::spawn(crate::applier::run(applier_ws));
            let _ = tokio::join!(watcher, applier);
        })
    }

    /// Trigger graceful shutdown. The node is taken out of its slot
    /// and torn down; subsequent calls are no-ops.
    pub async fn shutdown(&self) {
        self.shutdown_token.cancel();
        let mut slot = self.node.lock().await;
        if let Some(node) = slot.take() {
            node.shutdown().await;
        }
    }
}

/// Walk `root` and publish every non-skipped file to `doc`. Errors
/// on a single file surface as [`WorkspaceEvent::Error`]; we do not
/// abort the scan.
async fn scan_and_publish_existing(
    root: &Path,
    doc: &Doc,
    author: AuthorId,
    _blobs: &iroh_blobs::BlobsProtocol,
    echo_guard: &EchoGuard,
    events: &mpsc::Sender<WorkspaceEvent>,
) -> Result<(), WorkspaceError> {
    let filter = WorkspaceFilter::new(root);
    for entry in WalkDir::new(root).follow_links(false).into_iter().flatten() {
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        match filter.check(path) {
            FilterDecision::Skip(SkipReason::TooLarge { size }) => {
                let _ = events
                    .send(WorkspaceEvent::SkippedTooLarge {
                        path: path.to_path_buf(),
                        size,
                    })
                    .await;
                continue;
            }
            FilterDecision::Skip(_) => continue,
            FilterDecision::Include => {}
        }

        let bytes = match tokio::fs::read(path).await {
            Ok(b) => b,
            Err(err) => {
                let _ = events
                    .send(WorkspaceEvent::Error(format!(
                        "scan read {} failed: {err}",
                        path.display(),
                    )))
                    .await;
                continue;
            }
        };
        let key = match keys::path_to_key(root, path) {
            Ok(k) => k,
            Err(err) => {
                let _ = events
                    .send(WorkspaceEvent::Error(format!(
                        "scan key {} failed: {err}",
                        path.display(),
                    )))
                    .await;
                continue;
            }
        };
        if let Err(err) = doc.set_bytes(author, key, Bytes::from(bytes.clone())).await {
            let _ = events
                .send(WorkspaceEvent::Error(format!(
                    "scan publish {} failed: {err}",
                    path.display(),
                )))
                .await;
            continue;
        }
        echo_guard.record_local_publish(path, &bytes).await;
    }
    Ok(())
}

/// Broadcast `ticket` over `session` as a `MessageKind::System`
/// message with [`TICKET_ACTION`].
///
/// Wire shape: `payload = ticket.to_string().into_bytes()`. `DocTicket`
/// has a stable base32 string representation (its `Display` impl) so
/// joiners just call [`DocTicket::from_str`] on the bytes.
pub(crate) async fn publish_ticket(
    client: &Client,
    session: SessionId,
    ticket: &DocTicket,
) -> Result<(), WorkspaceError> {
    let resp = client
        .request(Request::Send {
            session,
            payload: SendPayload {
                kind: MessageKind::System,
                action: TICKET_ACTION.to_string(),
                payload: ticket.to_string().into_bytes(),
            },
        })
        .await?;
    match resp {
        Response::Sent { .. } => Ok(()),
        other => Err(WorkspaceError::Iroh(format!(
            "unexpected response to ticket Send: {other:?}",
        ))),
    }
}

/// Wait for `iroh-docs` to finish its first reconciliation pass and
/// download all pending content. Returns when [`LiveEvent::SyncFinished`]
/// has been observed *and* a subsequent
/// [`LiveEvent::PendingContentReady`] has arrived (or 30 s elapse, in
/// which case we surface a [`WorkspaceError::Doc`] — the alternative
/// is silently writing an empty bulk export).
async fn wait_for_initial_sync<S, E>(live: S) -> Result<(), WorkspaceError>
where
    S: futures_util::Stream<Item = Result<LiveEvent, E>> + Send + 'static,
{
    let mut live = Box::pin(live);
    timeout(Duration::from_secs(30), async {
        let mut sync_done = false;
        while let Some(ev) = live.next().await {
            let Ok(ev) = ev else { continue };
            match ev {
                LiveEvent::SyncFinished(_) => {
                    sync_done = true;
                }
                LiveEvent::PendingContentReady if sync_done => {
                    return Ok::<(), WorkspaceError>(());
                }
                _ => {}
            }
        }
        Err(WorkspaceError::Doc(
            "live event stream ended before initial sync".into(),
        ))
    })
    .await
    .map_err(|_| WorkspaceError::Doc("initial sync did not complete in 30s".into()))?
}

/// Drain `events` until a `MessageKind::System` event with
/// [`TICKET_ACTION`] for `session` arrives, returning its payload.
/// 15 s ceiling so a misconfigured session can't hang the joiner
/// forever.
async fn wait_for_ticket(
    events: &mut artel_client::EventStream,
    session: SessionId,
) -> Result<Vec<u8>, WorkspaceError> {
    timeout(Duration::from_secs(15), async {
        loop {
            let ev = events.recv().await.ok_or_else(|| {
                WorkspaceError::Iroh("event stream closed before ticket arrived".into())
            })?;
            if let Event::Message {
                session: ev_session,
                message,
            } = ev
                && ev_session == session
                && message.kind == MessageKind::System
                && message.action == TICKET_ACTION
            {
                return Ok::<_, WorkspaceError>(message.payload);
            }
        }
    })
    .await
    .map_err(|_| WorkspaceError::Iroh("timed out waiting for workspace.ticket".into()))?
}

/// Walk the doc and write every entry to disk under `root`.
///
/// Drives:
/// - tombstones (zero-length entries) → `remove_file`,
/// - too-large or filtered-out keys → skipped (with a
///   `SkippedTooLarge` event for size),
/// - invalid keys → logged and skipped,
/// - bytes not yet locally available → skipped (the applier in
///   3a-5 retries on `ContentReady`).
///
/// Pending-set entries are inserted via the echo guard so the (yet
/// to be wired) watcher won't republish what we just wrote. They
/// are released after [`PENDING_RELEASE_GRACE`].
async fn bulk_export(
    root: &Path,
    doc: &Doc,
    blobs: &iroh_blobs::BlobsProtocol,
    echo_guard: &EchoGuard,
    events: &mpsc::Sender<WorkspaceEvent>,
) -> Result<(), WorkspaceError> {
    let stream = doc
        .get_many(Query::all())
        .await
        .map_err(|e| WorkspaceError::Doc(format!("get_many: {e}")))?;
    tokio::pin!(stream);
    let filter = WorkspaceFilter::new(root);

    while let Some(res) = stream.next().await {
        let Ok(entry) = res else { continue };

        let path = match keys::key_to_path(root, entry.key()) {
            Ok(p) => p,
            Err(err) => {
                let _ = events
                    .send(WorkspaceEvent::Error(format!("invalid key: {err}")))
                    .await;
                continue;
            }
        };

        if entry.content_len() == 0 {
            let _ = tokio::fs::remove_file(&path).await;
            let _ = events.send(WorkspaceEvent::PeerDeleted { path }).await;
            continue;
        }

        match filter.check(&path) {
            FilterDecision::Skip(SkipReason::TooLarge { size }) => {
                let _ = events
                    .send(WorkspaceEvent::SkippedTooLarge {
                        path: path.clone(),
                        size,
                    })
                    .await;
                continue;
            }
            FilterDecision::Skip(_) => continue,
            FilterDecision::Include => {}
        }

        // Bytes not yet available locally → skip; the applier
        // (3a-5) retries on the next ContentReady. Bulk export is
        // best-effort.
        let Ok(bytes) = blobs.blobs().get_bytes(entry.content_hash()).await else {
            continue;
        };

        echo_guard.mark_remote_write(&path, &bytes).await;
        if let Some(parent) = path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        if let Err(err) = tokio::fs::write(&path, &bytes).await {
            let _ = events
                .send(WorkspaceEvent::Error(format!(
                    "bulk export write {} failed: {err}",
                    path.display(),
                )))
                .await;
            continue;
        }
        echo_guard.release_after(path.clone(), PENDING_RELEASE_GRACE);
        let _ = events.send(WorkspaceEvent::PeerWrote { path }).await;
    }
    Ok(())
}

/// Grace period before a path is removed from the echo guard's
/// pending set. The watcher's debouncer is 300 ms; 250 ms covers
/// the common case of "watcher fires while we're still writing"
/// without holding the path indefinitely.
const PENDING_RELEASE_GRACE: Duration = Duration::from_millis(250);

/// Best-effort canonicalisation. Falls back to the input path if
/// `canonicalize` fails (e.g., the dir doesn't exist yet) — callers
/// are expected to pass an existing dir.
fn canonicalise(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}
