//! `Workspace` — an attached, replicating workspace over an artel
//! session.
//!
//! Two modes, one type:
//! - [`Workspace::host`] creates a fresh `iroh-docs` document, scans
//!   the supplied directory, publishes its files into the doc, and
//!   broadcasts the resulting [`DocTicket`] over the artel session
//!   as a [`MessageKind::System`] message with action
//!   [`TICKET_ACTION`].
//! - [`Workspace::join`] listens for the system message, imports the
//!   ticket, and bulk-exports the doc to its own copy of the
//!   directory.
//!
//! Live two-way sync is wired up by [`Workspace::run`], which spawns
//! the watcher + applier background tasks.

use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use artel_client::Client;
use artel_protocol::{Event, MessageKind, Request, Response, SendPayload, SessionId};
use bytes::Bytes;
use futures_util::StreamExt;
use iroh_docs::AuthorId;
use iroh_docs::DocTicket;
use iroh_docs::NamespaceId;
use iroh_docs::api::Doc;
use iroh_docs::api::protocol::{AddrInfoOptions, ShareMode};
use iroh_docs::engine::LiveEvent;
use iroh_docs::store::Query;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use walkdir::WalkDir;

use crate::echo_guard::{EchoGuard, PENDING_RELEASE_GRACE};
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

/// Default name of the workspace's per-`root` state directory.
///
/// Lives inside the workspace so it travels with `root` for free;
/// added to the hardcoded filter skip list so the watcher never
/// tries to publish iroh's own redb / blob files into the doc.
pub const DEFAULT_STATE_SUBDIR: &str = ".artel-fs";

/// File inside `state_dir` that stores the host's persisted
/// `NamespaceId`. 32 raw bytes — namespaces aren't secret so no
/// special permissions are required.
const DOC_ID_FILE: &str = "doc-id";

/// Configurable knobs for [`Workspace::host_with`] /
/// [`Workspace::join_with`].
///
/// The default ([`WorkspaceConfig::default`]) puts state under
/// `<root>/.artel-fs/`. Override via [`Self::with_state_dir`] when
/// the workspace lives in a directory the user wouldn't want a
/// dotfile inside (e.g. a read-only export root).
#[derive(Clone, Debug, Default)]
pub struct WorkspaceConfig {
    /// Where the workspace persists its iroh secret, doc replica,
    /// and blob store. `None` resolves to `<root>/.artel-fs/`.
    pub state_dir: Option<PathBuf>,
}

impl WorkspaceConfig {
    /// Set an explicit state directory.
    #[must_use]
    pub fn with_state_dir(mut self, dir: PathBuf) -> Self {
        self.state_dir = Some(dir);
        self
    }

    /// Resolve the configured `state_dir` against `root`, applying
    /// the `<root>/.artel-fs/` default if unset.
    fn resolve(&self, root: &Path) -> PathBuf {
        self.state_dir
            .clone()
            .unwrap_or_else(|| root.join(DEFAULT_STATE_SUBDIR))
    }
}

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
    /// the [`WorkspaceEvent`] stream. Call [`Self::run`] to start
    /// the watcher + applier.
    pub async fn host(
        client: &Client,
        session: SessionId,
        root: PathBuf,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
        Self::host_with(client, session, root, WorkspaceConfig::default()).await
    }

    /// [`Self::host`], but with an explicit [`WorkspaceConfig`] so
    /// callers can override the state directory.
    ///
    /// On a fresh `state_dir`: creates a new doc and persists its
    /// `NamespaceId` to `state_dir/doc-id`. On a populated
    /// `state_dir`: opens the previously-published doc, **runs a
    /// reconcile pass** (tombstones doc entries whose backing files
    /// disappeared while we were down), and then re-publishes the
    /// remaining on-disk files. The resulting ticket is byte-stable
    /// across restarts so any joiner with the old ticket can
    /// resume.
    pub async fn host_with(
        client: &Client,
        session: SessionId,
        root: PathBuf,
        config: WorkspaceConfig,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
        let root = canonicalise(&root);
        // Materialise the workspace dir before the state dir so the
        // (default) `<root>/.artel-fs/` placement doesn't fail.
        tokio::fs::create_dir_all(&root).await?;
        let state_dir = config.resolve(&root);
        ensure_state_dir(&state_dir)?;

        let node = WorkspaceNode::spawn(&state_dir).await?;

        // Persistent docs store; default-author is managed by
        // iroh-docs at `state_dir/docs/default-author`.
        let author = node
            .docs
            .author_default()
            .await
            .map_err(|e| WorkspaceError::Doc(format!("author_default: {e}")))?;

        let doc_id_path = state_dir.join(DOC_ID_FILE);
        let (doc, returning) = open_or_create_doc(&node, &doc_id_path).await?;

        let (tx, rx) = mpsc::channel(EVENT_BUFFER);
        let echo_guard = EchoGuard::new();

        // Returning host: prune entries that no longer exist on
        // disk *before* we re-publish the current scan. Order
        // matters — tombstoning after re-publishing would erase
        // legitimate entries laid down by the scan.
        if returning {
            reconcile_doc_against_disk(&root, &doc, author, &tx).await?;
        }

        // Pre-populate the doc from disk *before* we share the
        // ticket — joiners that import after this scan see the
        // current snapshot via initial sync.
        scan_and_publish_existing(&root, &doc, author, &echo_guard, &tx).await?;

        // Share with full addressing info so the ticket is enough
        // for joiners to dial without out-of-band addr seeding.
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
    /// 2. Issues `Subscribe { since: None }` so the daemon's replay
    ///    path surfaces the host's `workspace.ticket` system
    ///    message even if the joiner arrived after it was
    ///    originally broadcast.
    /// 3. Drains events until the ticket arrives (15 s ceiling).
    /// 4. Imports the ticket into the joiner's local doc, runs
    ///    `bulk_export` to seed `root` with whatever's already in
    ///    the doc, and returns. Call [`Self::run`] to start the
    ///    watcher + applier.
    ///
    /// **Side effect:** consumes the client's [`Client::take_events`]
    /// channel. Callers that need to observe other session events
    /// from the same connection should open a second [`Client`].
    pub async fn join(
        client: &Client,
        session: SessionId,
        root: PathBuf,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
        Self::join_with(client, session, root, WorkspaceConfig::default()).await
    }

    /// [`Self::join`], but with an explicit [`WorkspaceConfig`].
    ///
    /// The joiner persists its iroh secret + doc replica + blobs
    /// store under `state_dir`. On restart, `iroh-docs` resumes from
    /// the existing replica; if the host's namespace has changed
    /// since last time, the joiner imports the new ticket alongside
    /// the old one without losing what it already had on disk.
    pub async fn join_with(
        client: &Client,
        session: SessionId,
        root: PathBuf,
        config: WorkspaceConfig,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
        let root = canonicalise(&root);
        tokio::fs::create_dir_all(&root).await?;
        let state_dir = config.resolve(&root);
        ensure_state_dir(&state_dir)?;

        let node = WorkspaceNode::spawn(&state_dir).await?;
        // Joiners don't persist a per-workspace `doc-id` — they
        // import the host's namespace from the ticket each time.
        // The default author is still useful for stamping our own
        // writes once live sync starts.
        let author = node
            .docs
            .author_default()
            .await
            .map_err(|e| WorkspaceError::Doc(format!("author_default: {e}")))?;

        // Subscribe and drain until the ticket arrives. Subscribe
        // replays historical messages, so a joiner that arrives
        // after the ticket was published still picks it up.
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

    /// Spawn the watcher + applier background tasks, **awaiting**
    /// the watcher's OS-level attach before returning.
    ///
    /// - The watcher debounces filesystem events under [`Self::root`]
    ///   and publishes them into the doc.
    /// - The applier subscribes to [`Doc::subscribe`] and applies
    ///   `InsertRemote` / `ContentReady` events to disk.
    ///
    /// When this future resolves, the OS-level filesystem watch is
    /// guaranteed to be attached — any subsequent write under
    /// [`Self::root`] will reach the watcher. This ordering matters
    /// because callers (most often a peer-driven test or production
    /// flow) typically write to [`Self::root`] right after `run()`
    /// completes; without the attach guarantee, those writes can
    /// race ahead of the watcher and be silently dropped on macOS
    /// `FSEvents`.
    ///
    /// If the watcher fails to initialise or attach, the readiness
    /// channel is dropped and `run` still returns the `JoinHandle`
    /// — the underlying error is surfaced via the [`WorkspaceEvent`]
    /// stream. The applier continues either way.
    ///
    /// Both tasks honour the workspace's shutdown token. The
    /// returned `JoinHandle` resolves once both have exited.
    #[must_use]
    pub async fn run(self: std::sync::Arc<Self>) -> tokio::task::JoinHandle<()> {
        let watcher_ws = std::sync::Arc::clone(&self);
        let applier_ws = std::sync::Arc::clone(&self);
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
        let join = tokio::spawn(async move {
            let watcher = tokio::spawn(crate::watcher::run(watcher_ws, ready_tx));
            let applier = tokio::spawn(crate::applier::run(applier_ws));
            let _ = tokio::join!(watcher, applier);
        });
        // Wait for the watcher to attach. A `RecvError` here means
        // the watcher dropped the sender on its early-return error
        // path (init failed / watch failed); the WorkspaceEvent::Error
        // already surfaces the cause, and we still hand back the
        // JoinHandle so callers can shut down cleanly.
        let _ = ready_rx.await;
        join
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
        let bytes = Bytes::from(bytes);
        if let Err(err) = doc.set_bytes(author, key, bytes.clone()).await {
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
/// - bytes not yet locally available → skipped (the applier
///   retries on `ContentReady`).
///
/// Pending-set entries are inserted via the echo guard so the
/// watcher won't republish what we just wrote. They are released
/// after [`PENDING_RELEASE_GRACE`].
async fn bulk_export(
    root: &Path,
    doc: &Doc,
    blobs: &iroh_blobs::BlobsProtocol,
    echo_guard: &EchoGuard,
    events: &mpsc::Sender<WorkspaceEvent>,
) -> Result<(), WorkspaceError> {
    // `include_empty()` is load-bearing for the returning-joiner
    // path: a tombstone the host emitted while we were offline only
    // shows up here if we ask for empty entries, otherwise sync
    // updates the replica but disk state silently drifts.
    let stream = doc
        .get_many(Query::single_latest_per_key().include_empty())
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
        // retries on the next ContentReady. Bulk export is
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

/// Best-effort canonicalisation. Falls back to the input path if
/// `canonicalize` fails (e.g., the dir doesn't exist yet) — callers
/// are expected to pass an existing dir.
fn canonicalise(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

/// Create the state dir (and any missing parents) if it doesn't
/// already exist. Idempotent — re-runs on every workspace startup.
fn ensure_state_dir(state_dir: &Path) -> Result<(), WorkspaceError> {
    // chmod 0700 on first create — the dir holds the workspace's
    // iroh secret key, so default-deny on traversal matches the
    // keystore's own threat model.
    crate::keystore::ensure_dir(state_dir)
        .map_err(|e| WorkspaceError::Iroh(format!("create state_dir {}: {e}", state_dir.display())))
}

/// Open the host's persisted doc, or create a fresh one and stamp
/// `doc_id_path` with its `NamespaceId`. Returns the doc plus a
/// flag: `true` means we opened an existing doc (the caller must
/// reconcile it against disk), `false` means we created a fresh
/// one (no reconcile needed).
async fn open_or_create_doc(
    node: &WorkspaceNode,
    doc_id_path: &Path,
) -> Result<(Doc, bool), WorkspaceError> {
    match std::fs::read(doc_id_path) {
        Ok(bytes) => {
            let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                WorkspaceError::Doc(format!(
                    "doc-id at {} is corrupt: expected 32 bytes, got {}",
                    doc_id_path.display(),
                    bytes.len(),
                ))
            })?;
            let id = NamespaceId::from(&arr);
            // `Docs::open` returns "Replica not found" if the redb
            // commit for the namespace hasn't durably landed yet —
            // `iroh-docs` batches writes with a 500 ms delay, so a
            // crash between `Docs::create` returning and the commit
            // firing can leave a `doc-id` pointing at a namespace
            // that doesn't exist on disk. Self-heal by recreating;
            // joiners with the prior ticket lose the ability to
            // resume, which is acceptable since they wouldn't have
            // synced anything pre-crash anyway.
            if let Ok(Some(doc)) = node.docs.open(id).await {
                Ok((doc, true))
            } else {
                tracing::warn!(
                    ?id,
                    "stale doc-id at {}: namespace not in store, recreating",
                    doc_id_path.display(),
                );
                create_and_persist(node, doc_id_path).await
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            create_and_persist(node, doc_id_path).await
        }
        Err(err) => Err(WorkspaceError::Doc(format!(
            "read doc-id at {}: {err}",
            doc_id_path.display(),
        ))),
    }
}

/// Create a fresh namespace and persist its id.
async fn create_and_persist(
    node: &WorkspaceNode,
    doc_id_path: &Path,
) -> Result<(Doc, bool), WorkspaceError> {
    let doc = node
        .docs
        .create()
        .await
        .map_err(|e| WorkspaceError::Doc(format!("doc create: {e}")))?;
    // No chmod — namespace ids aren't secret.
    crate::keystore::write_atomic(doc_id_path, &doc.id().to_bytes(), None).map_err(|e| {
        WorkspaceError::Doc(format!("persist doc-id at {}: {e}", doc_id_path.display()))
    })?;
    Ok((doc, false))
}

/// Walk the doc and tombstone any entry whose key maps to a path
/// that no longer exists on disk under `root`.
///
/// **Must** run before `scan_and_publish_existing` so a
/// stale-on-disk entry is removed before the rescan re-asserts
/// it; it must also run before the watcher / applier are spawned
/// and before the ticket is re-broadcast (otherwise a peer
/// importing the ticket would observe flapping state mid-pass).
async fn reconcile_doc_against_disk(
    root: &Path,
    doc: &Doc,
    author: AuthorId,
    events: &mpsc::Sender<WorkspaceEvent>,
) -> Result<(), WorkspaceError> {
    // One entry per key — we only need to know the *latest* state of
    // each path. Already-tombstoned entries are filtered out via the
    // `include_empty=false` default.
    let stream = doc
        .get_many(Query::single_latest_per_key())
        .await
        .map_err(|e| WorkspaceError::Doc(format!("reconcile get_many: {e}")))?;
    tokio::pin!(stream);

    while let Some(res) = stream.next().await {
        let Ok(entry) = res else { continue };
        // Defensive: should be unreachable given the default query.
        if entry.content_len() == 0 {
            continue;
        }

        let path = match keys::key_to_path(root, entry.key()) {
            Ok(p) => p,
            Err(err) => {
                let _ = events
                    .send(WorkspaceEvent::Error(format!(
                        "reconcile invalid key: {err}"
                    )))
                    .await;
                continue;
            }
        };
        if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
            // `Doc::del` writes a tombstone for `prefix`. The key
            // we hand it is exact, so it tombstones just this
            // entry.
            if let Err(err) = doc.del(author, entry.key().to_vec()).await {
                let _ = events
                    .send(WorkspaceEvent::Error(format!(
                        "reconcile tombstone {}: {err}",
                        path.display(),
                    )))
                    .await;
            }
        }
    }
    Ok(())
}
