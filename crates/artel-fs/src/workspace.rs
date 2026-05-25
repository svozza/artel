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

use std::ffi::OsString;
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
use crate::error::{PolicyViolation, WorkspaceError};
use crate::filter::{FilterDecision, SkipReason, WorkspaceFilter};
use crate::keys;
use crate::node::WorkspaceNode;
use crate::rules::PathRules;
use crate::ticket::{self, WorkspaceTicketEnvelope};

/// Maximum number of offending entries surfaced in a
/// [`PolicyViolation::DirNotEmpty`] error before truncation. Five is
/// enough to be diagnostic without flooding terminals on a
/// disastrously-wrong dir (e.g. `~`).
const POLICY_OFFENDING_LIMIT: usize = 5;

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

/// How a [`Workspace::host`] / [`Workspace::join`] call may attach
/// to its workspace root.
///
/// There is **deliberately no [`Default`]**. The brainstorm fixing
/// the home-dir-publish hazard (2026-05-20) decided every caller
/// must specify the policy explicitly so wrong-dir risk is visible
/// at every call site. If always specifying turns out to be
/// annoying we add a default later — easier to relax than to
/// tighten.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachPolicy {
    /// Refuse to attach if the workspace root is non-empty.
    ///
    /// "Empty" is computed at the **top level only**. The state
    /// directory (default `<root>/.artel-fs/`, or whatever
    /// [`WorkspaceConfig::with_state_dir`] resolves to when it lives
    /// under `root`) does not count toward non-emptiness, nor do
    /// hardcoded-skip paths like `.git/`, `target/`, `node_modules/`,
    /// `.DS_Store`, `*.swp`, `*.tmp`. Top-level symlinks **do**
    /// count: we never follow them, and a symlinked tree shouldn't
    /// trick us into thinking the dir is empty.
    ///
    /// This is the safe default for fresh hosts and joiners.
    RequireEmpty,
    /// Attach regardless of the root's existing contents.
    ///
    /// On host, the existing files are scanned and published into
    /// the doc. On join, [`bulk_export`](Workspace::join_with) may
    /// overwrite local files whose paths collide with synced doc
    /// entries. Use when you know the dir is yours and you accept
    /// that local edits may be clobbered.
    AllowExisting,
    /// Originate-only: adopt the existing dir's contents into a
    /// fresh workspace.
    ///
    /// Today this behaves identically to [`Self::AllowExisting`] on
    /// host: the scan runs and publishes whatever's there. The
    /// distinct variant exists so a future slice (snapshot/init
    /// step distinct from the live scan) can diverge without a
    /// breaking change. **Rejected on join** — a joiner has no
    /// canonical tree to seed from; use [`Self::AllowExisting`]
    /// there instead.
    InitFromExisting,
}

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

    /// How long [`Workspace::join_with`] waits for the host's
    /// `workspace.ticket` to arrive on the artel session before
    /// giving up. `None` (the default) means wait forever — useful
    /// for long-lived joiners that may attach minutes or hours
    /// after the host first published. Set [`Some(duration)`] when
    /// you'd rather fail fast on a misconfigured session (e.g.
    /// wrong ticket, daemons that can't reach each other).
    pub join_ticket_timeout: Option<Duration>,

    /// Override the workspace's iroh-side discovery layer. Real
    /// deployments leave this `None` — the workspace endpoint runs
    /// `iroh::endpoint::presets::N0` and discovers peers via n0's
    /// pkarr publish + DNS resolve. Integration tests that spin up
    /// many workspace nodes in rapid succession set
    /// [`Some(MemoryLookup)`] to swap discovery for an in-process
    /// lookup table; this also disables the relay path (`Minimal`
    /// preset has no relay) so n0's externally-rate-limited
    /// services are entirely off the critical path.
    ///
    /// Mirrors the daemon's
    /// [`artel_daemon::AddressLookupOverride`] knob — daemons and
    /// workspaces share the same shape so test fixtures can seed
    /// both with one cross-seeded lookup. See `tests/common/mod.rs`
    /// (`spawn_pair`) for the canonical use.
    pub address_lookup_override: Option<iroh::address_lookup::memory::MemoryLookup>,

    /// Per-path read/write rules for this workspace.
    ///
    /// Bound at originate-time (host side), travel with the
    /// `workspace.ticket` envelope, decoded and stored on the joiner
    /// side. **Ignored on join**: a joiner's `rules` field is
    /// dropped on the floor — the host's rules win. `None` resolves
    /// to [`PathRules::read_write`] (default-permissive).
    ///
    /// Note: in v1 only the **originator** decides the rules. There
    /// is no negotiation, no peer-side override. Once issued, the
    /// rules are fixed for the lifetime of the doc; restart the host
    /// with the same `WorkspaceConfig::rules` to keep them stable
    /// across resumes (see plan §"persistence-first constraint").
    pub rules: Option<PathRules>,
}

impl WorkspaceConfig {
    /// Set an explicit state directory.
    #[must_use]
    pub fn with_state_dir(mut self, dir: PathBuf) -> Self {
        self.state_dir = Some(dir);
        self
    }

    /// Set how long the joiner waits for the host's ticket to
    /// arrive over the artel session. `None` waits forever.
    #[must_use]
    pub const fn with_join_ticket_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.join_ticket_timeout = timeout;
        self
    }

    /// Set an in-process address lookup to substitute for n0's
    /// pkarr/DNS discovery. See [`Self::address_lookup_override`]
    /// for when this is appropriate (essentially: tests only).
    #[must_use]
    pub fn with_address_lookup_override(
        mut self,
        lookup: iroh::address_lookup::memory::MemoryLookup,
    ) -> Self {
        self.address_lookup_override = Some(lookup);
        self
    }

    /// Bind a [`PathRules`] set to the workspace at originate-time.
    ///
    /// Honoured on host (rides the `workspace.ticket` envelope to
    /// joiners). **Ignored on join** — see [`Self::rules`].
    #[must_use]
    pub fn with_rules(mut self, rules: PathRules) -> Self {
        self.rules = Some(rules);
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
    /// Path rules bound at originate-time.
    ///
    /// On the host: the configured (or default-permissive)
    /// [`PathRules`]. On the joiner: the rules decoded from the
    /// `workspace.ticket` envelope — the host's rules, not whatever
    /// the joiner configured. Surfaced via [`Self::rules`] for
    /// inspection; enforcement (watcher / applier consultation)
    /// lands in a follow-up slice.
    pub(crate) rules: PathRules,
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
        policy: AttachPolicy,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
        Self::host_with(client, session, root, policy, WorkspaceConfig::default()).await
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
        policy: AttachPolicy,
        config: WorkspaceConfig,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
        let root = canonicalise(&root);
        // Materialise the workspace dir before the state dir so the
        // (default) `<root>/.artel-fs/` placement doesn't fail.
        tokio::fs::create_dir_all(&root).await?;
        let state_dir = config.resolve(&root);

        // Enforce the policy *before* any state-dir or iroh-node
        // creation so a `Policy` error guarantees no on-disk
        // artefacts were left behind.
        enforce_attach_policy(&root, &state_dir, policy, AttachSide::Host)?;

        // Resolve and validate rules *before* iroh-node spawn too —
        // a malformed rule set is a configuration error, not a
        // runtime one.
        let rules = config.rules.unwrap_or_else(PathRules::read_write);
        rules.validate()?;

        ensure_state_dir(&state_dir)?;

        let node = WorkspaceNode::spawn(&state_dir, config.address_lookup_override.clone()).await?;

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

        publish_ticket(client, session, &ticket, &rules).await?;

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
                rules,
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
        policy: AttachPolicy,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
        Self::join_with(client, session, root, policy, WorkspaceConfig::default()).await
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
        policy: AttachPolicy,
        config: WorkspaceConfig,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
        let root = canonicalise(&root);
        tokio::fs::create_dir_all(&root).await?;
        let state_dir = config.resolve(&root);

        // Enforce the policy before any state-dir / iroh-node /
        // subscribe work so a `Policy` error leaves zero on-disk
        // and zero IPC state.
        enforce_attach_policy(&root, &state_dir, policy, AttachSide::Join)?;

        ensure_state_dir(&state_dir)?;

        let node = WorkspaceNode::spawn(&state_dir, config.address_lookup_override.clone()).await?;
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

        let envelope = wait_for_ticket(&mut events, session, config.join_ticket_timeout).await?;
        // The host's rules are authoritative — `config.rules` on the
        // joiner side is dropped on the floor here. Documented on
        // `WorkspaceConfig::rules`.
        let rules = envelope.rules;
        let ticket = DocTicket::from_str(&envelope.doc_ticket)
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
                rules,
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

    /// Borrow the workspace's [`PathRules`].
    ///
    /// On the host: the configured (or default-permissive) rules.
    /// On the joiner: the rules decoded from the host's
    /// `workspace.ticket` envelope (the host's, not whatever the
    /// joiner configured). Surfaced for tests and future consumers;
    /// enforcement (watcher / applier consultation) lands in a
    /// follow-up slice.
    #[must_use]
    pub const fn rules(&self) -> &PathRules {
        &self.rules
    }

    /// Spawn the watcher + applier background tasks, **awaiting**
    /// both halves' readiness before returning.
    ///
    /// - The watcher debounces filesystem events under [`Self::root`]
    ///   and publishes them into the doc.
    /// - The applier subscribes to [`Doc::subscribe`] and applies
    ///   `InsertRemote` / `ContentReady` events to disk.
    ///
    /// When this future resolves, both halves are wired up:
    /// - the OS-level filesystem watch is attached, so any
    ///   subsequent write under [`Self::root`] reaches the watcher
    ///   (without this, writes can race ahead of the watcher and be
    ///   silently dropped on macOS `FSEvents`);
    /// - the applier's `doc.subscribe()` has returned, so any
    ///   `InsertRemote` / `ContentReady` fired against this
    ///   workspace's doc reaches the applier (iroh-docs subscribers
    ///   are push-to-vec — events fired before `subscribe()`
    ///   completes are not replayed).
    ///
    /// If either half fails to start (watcher init / attach, or
    /// applier subscribe), the corresponding readiness signal is
    /// dropped and `run` still returns the `JoinHandle` — the
    /// underlying error is surfaced via the [`WorkspaceEvent`]
    /// stream. The other half continues.
    ///
    /// Both tasks honour the workspace's shutdown token. The
    /// returned `JoinHandle` resolves once both have exited.
    #[must_use]
    pub async fn run(self: std::sync::Arc<Self>) -> tokio::task::JoinHandle<()> {
        let watcher_ws = std::sync::Arc::clone(&self);
        let applier_ws = std::sync::Arc::clone(&self);
        let (watcher_ready_tx, watcher_ready_rx) = tokio::sync::oneshot::channel::<()>();
        let (applier_ready_tx, applier_ready_rx) = tokio::sync::oneshot::channel::<()>();
        let join = tokio::spawn(async move {
            let watcher = tokio::spawn(crate::watcher::run(watcher_ws, watcher_ready_tx));
            let applier = tokio::spawn(crate::applier::run(applier_ws, applier_ready_tx));
            let _ = tokio::join!(watcher, applier);
        });
        // Wait for both halves to come up. A `RecvError` on either
        // channel means that half hit its early-return error path
        // (watcher init / attach failure, or applier subscribe
        // failure); the WorkspaceEvent::Error already surfaces the
        // cause, and we still hand back the JoinHandle so callers
        // can shut down cleanly.
        let (_, _) = tokio::join!(watcher_ready_rx, applier_ready_rx);
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
///
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

/// Broadcast `ticket` + `rules` over `session` as a
/// `MessageKind::System` message with [`TICKET_ACTION`].
///
/// Wire shape: postcard-encoded [`WorkspaceTicketEnvelope`]. The
/// legacy pre-envelope shape (raw `DocTicket::to_string().into_bytes()`)
/// is hard-rejected by the joiner with
/// [`TicketEnvelopeError::Malformed`] — see [`crate::ticket`] module
/// docs for the wire-compat decision.
pub(crate) async fn publish_ticket(
    client: &Client,
    session: SessionId,
    ticket: &DocTicket,
    rules: &PathRules,
) -> Result<(), WorkspaceError> {
    let envelope = WorkspaceTicketEnvelope::new(ticket.to_string(), rules.clone());
    let payload = ticket::encode(&envelope)?;
    let resp = client
        .request(Request::Send {
            session,
            payload: SendPayload {
                kind: MessageKind::System,
                action: TICKET_ACTION.to_string(),
                payload,
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
/// [`TICKET_ACTION`] for `session` arrives, then decode the payload
/// as a [`WorkspaceTicketEnvelope`]. `deadline` caps the wait —
/// `None` means wait indefinitely (the right shape for long-lived
/// joiners that may arrive minutes or hours after the host).
async fn wait_for_ticket(
    events: &mut artel_client::EventStream,
    session: SessionId,
    deadline: Option<Duration>,
) -> Result<WorkspaceTicketEnvelope, WorkspaceError> {
    let drain = async {
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
                let envelope = ticket::decode(&message.payload)?;
                return Ok::<_, WorkspaceError>(envelope);
            }
        }
    };
    match deadline {
        Some(d) => timeout(d, drain)
            .await
            .map_err(|_| WorkspaceError::Iroh("timed out waiting for workspace.ticket".into()))?,
        None => drain.await,
    }
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

/// Which side of an attach is being checked. Drives the
/// `InitFromExisting`-on-join rejection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AttachSide {
    Host,
    Join,
}

/// Enforce `policy` against the given workspace `root`.
///
/// `state_dir` is the resolved state directory path (after
/// [`WorkspaceConfig::resolve`]). When it lives under `root` (the
/// common case), its top-level component is excluded from the
/// emptiness check so a returning host/joiner with persisted state
/// can still pass `RequireEmpty`.
///
/// Hardcoded-skip paths (`.git`, `target`, etc.) are also excluded —
/// see [`WorkspaceFilter::is_hardcoded_skip`].
fn enforce_attach_policy(
    root: &Path,
    state_dir: &Path,
    policy: AttachPolicy,
    side: AttachSide,
) -> Result<(), WorkspaceError> {
    if matches!(policy, AttachPolicy::InitFromExisting) && side == AttachSide::Join {
        return Err(WorkspaceError::Policy(
            PolicyViolation::InitFromExistingNotMeaningfulOnJoin,
        ));
    }

    if !matches!(policy, AttachPolicy::RequireEmpty) {
        return Ok(());
    }

    // Resolve the state dir's top-level component name relative to
    // `root`, when the state dir lives under `root`. Used to exempt
    // `<root>/.artel-fs` (or whatever override placed the state dir
    // inside the workspace) from the emptiness check.
    let exempt_state_component: Option<OsString> = state_dir
        .strip_prefix(root)
        .ok()
        .and_then(|rel| rel.components().next())
        .map(|c| c.as_os_str().to_owned());

    let read_dir = match std::fs::read_dir(root) {
        Ok(rd) => rd,
        // The policy check runs before `create_dir_all` (we want
        // canonicalisation to happen before either, and that
        // requires the path to already exist on disk). A
        // not-yet-created `root` is, by definition, empty — no user
        // data to clobber.
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(WorkspaceError::Io(err)),
    };

    let mut offending = Vec::with_capacity(POLICY_OFFENDING_LIMIT);
    let mut more = false;
    for entry in read_dir {
        let entry = entry.map_err(WorkspaceError::Io)?;
        let name = entry.file_name();

        // Exempt the workspace's own state dir.
        if let Some(exempt) = exempt_state_component.as_ref()
            && &name == exempt
        {
            continue;
        }

        // Exempt hardcoded-skip names by treating each top-level
        // entry as a single-component relative path. `.git/`,
        // `target/`, `node_modules/`, `.DS_Store`, `*.swp`, `*.tmp`.
        if WorkspaceFilter::is_hardcoded_skip(Path::new(&name)) {
            continue;
        }

        if offending.len() < POLICY_OFFENDING_LIMIT {
            offending.push(entry.path());
        } else {
            more = true;
            break;
        }
    }

    if offending.is_empty() {
        return Ok(());
    }

    let _ = more; // Reserved: a future variant could distinguish
    // truncated-vs-complete; today the error message already names
    // "first N entries" and that's good enough.

    Err(WorkspaceError::Policy(PolicyViolation::DirNotEmpty {
        root: root.to_path_buf(),
        offending_entries: offending,
    }))
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

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn workspace_config_with_join_ticket_timeout_sets_field() {
        let cfg = WorkspaceConfig::default().with_join_ticket_timeout(Some(Duration::from_secs(7)));
        assert_eq!(cfg.join_ticket_timeout, Some(Duration::from_secs(7)));

        let cfg = WorkspaceConfig::default().with_join_ticket_timeout(None);
        assert_eq!(cfg.join_ticket_timeout, None);
    }

    #[test]
    fn workspace_config_default_has_no_join_ticket_timeout() {
        // Long-lived joiners are the common case — default to "wait
        // forever" so a misconfigured single-process test has to opt
        // into a deadline.
        assert_eq!(WorkspaceConfig::default().join_ticket_timeout, None);
    }

    #[test]
    fn workspace_config_default_has_no_rules() {
        // The plan calls for default `None`, resolving to
        // `PathRules::read_write()` inside `host_with`. Test the
        // configuration default explicitly so a future patch can't
        // change the default rules without also flipping this test.
        assert!(WorkspaceConfig::default().rules.is_none());
    }

    #[test]
    fn workspace_config_with_rules_sets_field() {
        use crate::rules::{Mode, PathRule, PathRules};
        let rules = PathRules {
            default: Mode::ReadOnly,
            rules: vec![PathRule {
                glob: "shared/**".into(),
                mode: Mode::ReadWrite,
            }],
        };
        let cfg = WorkspaceConfig::default().with_rules(rules.clone());
        assert_eq!(cfg.rules, Some(rules));
    }

    /// Helper: build (`root`, `state_dir`) for a default-layout
    /// workspace inside a fresh tempdir. State dir is the standard
    /// `<root>/.artel-fs/` placement — its parent (`root`) doesn't
    /// have it pre-created so we can exercise both the
    /// already-exists and not-yet-exists branches.
    fn default_layout() -> (TempDir, PathBuf, PathBuf) {
        let dir = TempDir::new().expect("tempdir");
        let root = dir.path().to_path_buf();
        let state_dir = root.join(DEFAULT_STATE_SUBDIR);
        (dir, root, state_dir)
    }

    #[test]
    fn enforce_attach_policy_require_empty_accepts_truly_empty_dir() {
        let (_t, root, state_dir) = default_layout();
        let res = enforce_attach_policy(
            &root,
            &state_dir,
            AttachPolicy::RequireEmpty,
            AttachSide::Host,
        );
        assert!(res.is_ok(), "{res:?}");
    }

    #[test]
    fn enforce_attach_policy_require_empty_accepts_dir_with_only_artel_fs() {
        let (_t, root, state_dir) = default_layout();
        // Materialise the state dir before the check — this is the
        // returning-host / returning-joiner case.
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(state_dir.join("iroh.key"), b"x").unwrap();
        fs::write(state_dir.join("doc-id"), b"x").unwrap();
        let res = enforce_attach_policy(
            &root,
            &state_dir,
            AttachPolicy::RequireEmpty,
            AttachSide::Host,
        );
        assert!(res.is_ok(), "{res:?}");
    }

    #[test]
    fn enforce_attach_policy_require_empty_accepts_dir_with_only_filtered_paths() {
        let (_t, root, state_dir) = default_layout();
        // All hardcoded-skip names should be exempted from the
        // emptiness check.
        fs::create_dir_all(root.join(".git")).unwrap();
        fs::create_dir_all(root.join("target")).unwrap();
        fs::create_dir_all(root.join("node_modules")).unwrap();
        fs::write(root.join(".DS_Store"), b"x").unwrap();
        fs::write(root.join("foo.swp"), b"x").unwrap();
        fs::write(root.join("scratch.tmp"), b"x").unwrap();
        let res = enforce_attach_policy(
            &root,
            &state_dir,
            AttachPolicy::RequireEmpty,
            AttachSide::Host,
        );
        assert!(res.is_ok(), "{res:?}");
    }

    #[test]
    fn enforce_attach_policy_require_empty_accepts_dir_with_overridden_state_dir() {
        // State dir lives *inside* `root` but under a non-default
        // name. Its top-level component should still be exempted.
        let dir = TempDir::new().unwrap();
        let root = dir.path().to_path_buf();
        let state_dir = root.join("custom-state");
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(state_dir.join("iroh.key"), b"x").unwrap();
        let res = enforce_attach_policy(
            &root,
            &state_dir,
            AttachPolicy::RequireEmpty,
            AttachSide::Host,
        );
        assert!(res.is_ok(), "{res:?}");
    }

    #[test]
    fn enforce_attach_policy_require_empty_rejects_dir_with_user_file() {
        let (_t, root, state_dir) = default_layout();
        fs::write(root.join("a.txt"), b"hello").unwrap();
        let err = enforce_attach_policy(
            &root,
            &state_dir,
            AttachPolicy::RequireEmpty,
            AttachSide::Host,
        )
        .expect_err("should reject non-empty dir");
        match err {
            WorkspaceError::Policy(PolicyViolation::DirNotEmpty {
                offending_entries, ..
            }) => {
                assert_eq!(offending_entries.len(), 1);
                assert!(offending_entries[0].ends_with("a.txt"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn enforce_attach_policy_require_empty_rejects_dir_with_subdirectory() {
        let (_t, root, state_dir) = default_layout();
        fs::create_dir_all(root.join("src")).unwrap();
        let err = enforce_attach_policy(
            &root,
            &state_dir,
            AttachPolicy::RequireEmpty,
            AttachSide::Join,
        )
        .expect_err("should reject");
        assert!(matches!(
            err,
            WorkspaceError::Policy(PolicyViolation::DirNotEmpty { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn enforce_attach_policy_require_empty_rejects_top_level_symlink() {
        // Symlinks at the top level count as non-empty: the filter
        // never follows them, so a symlinked tree shouldn't trick
        // emptiness into a pass.
        let (_t, root, state_dir) = default_layout();
        let target = TempDir::new().unwrap();
        std::os::unix::fs::symlink(target.path(), root.join("link")).unwrap();
        let err = enforce_attach_policy(
            &root,
            &state_dir,
            AttachPolicy::RequireEmpty,
            AttachSide::Host,
        )
        .expect_err("symlink at top level should reject");
        assert!(matches!(
            err,
            WorkspaceError::Policy(PolicyViolation::DirNotEmpty { .. })
        ));
    }

    #[test]
    fn enforce_attach_policy_allow_existing_passes_anything() {
        let (_t, root, state_dir) = default_layout();
        fs::write(root.join("a.txt"), b"x").unwrap();
        fs::create_dir_all(root.join("nested/dir")).unwrap();
        for side in [AttachSide::Host, AttachSide::Join] {
            let res = enforce_attach_policy(&root, &state_dir, AttachPolicy::AllowExisting, side);
            assert!(res.is_ok(), "AllowExisting {side:?} should pass: {res:?}");
        }
    }

    #[test]
    fn enforce_attach_policy_init_from_existing_passes_on_host() {
        let (_t, root, state_dir) = default_layout();
        fs::write(root.join("a.txt"), b"x").unwrap();
        let res = enforce_attach_policy(
            &root,
            &state_dir,
            AttachPolicy::InitFromExisting,
            AttachSide::Host,
        );
        assert!(res.is_ok(), "{res:?}");
    }

    #[test]
    fn enforce_attach_policy_init_from_existing_rejected_on_join() {
        let (_t, root, state_dir) = default_layout();
        let err = enforce_attach_policy(
            &root,
            &state_dir,
            AttachPolicy::InitFromExisting,
            AttachSide::Join,
        )
        .expect_err("InitFromExisting must reject on join");
        assert!(matches!(
            err,
            WorkspaceError::Policy(PolicyViolation::InitFromExistingNotMeaningfulOnJoin)
        ));
    }
}
