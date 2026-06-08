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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use artel_client::{Client, ClientError};
use artel_protocol::{
    Event, JoinTicket, MessageKind, PeerId, ProtocolError, Request, Response, SendPayload,
    SessionId,
};
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
use tracing::{debug, warn};
use walkdir::WalkDir;

use crate::echo_guard::{EchoGuard, PENDING_RELEASE_GRACE};
use crate::endpoint_setup::EndpointSetup;
use crate::error::{PolicyViolation, WorkspaceError};
use crate::filter::{FilterDecision, SkipReason, WorkspaceFilter};
use crate::keys;
use crate::node::WorkspaceNode;
use crate::peer_map::PeerMap;
use crate::rules::{CompiledPathRules, Mode, PathRules};
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

/// Action stamped on the `MessageKind::System` message a joiner sends
/// to announce its workspace `EndpointId` to the host. Payload is the
/// raw 32-byte `EndpointId`.
pub const NODE_ID_ACTION: &str = "workspace.node_id";

use artel_protocol::UPGRADE_ACTION;

/// Payload for the `workspace.upgrade` system message.
#[derive(serde::Serialize, serde::Deserialize)]
struct UpgradePayload {
    target_peer: PeerId,
    namespace_secret: [u8; 32],
}

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

    /// Pick the workspace endpoint's discovery layer. Real
    /// deployments use [`EndpointSetup::Production`] (the default —
    /// `presets::N0`, pkarr publish + DNS resolve via n0
    /// infrastructure, with home-relay readiness on `online()`).
    /// Integration tests that spin up many workspace nodes in
    /// rapid succession use [`EndpointSetup::Testing`] with a
    /// shared `Arc<DnsPkarrServer>` so discovery runs against a
    /// localhost pkarr+DNS pair — deterministic, fast, no n0
    /// rate-limit exposure.
    ///
    /// Mirrors the daemon's [`DaemonConfig::endpoint_setup`] —
    /// daemons and workspaces share the same shape so test fixtures
    /// can hand one `Arc<DnsPkarrServer>` to both. See
    /// `tests/common/mod.rs` (`spawn_pair`) for the canonical use.
    pub endpoint_setup: EndpointSetup,

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

    /// Path to the daemon's IPC socket. When set, the workspace
    /// opens a **second** [`Client`] connection to subscribe to
    /// session events (capability grants/revokes, node-id
    /// announcements) and project them into the [`PeerMap`] that
    /// backs the docs gate. Without this, the gate still rejects
    /// connections from already-revoked peers (seeded at
    /// construction) but cannot observe revocations that happen
    /// after the workspace is up.
    pub daemon_socket: Option<PathBuf>,
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

    /// Pick the workspace endpoint's discovery layer. See
    /// [`Self::endpoint_setup`] for variant semantics. Production
    /// is the default; tests pass an [`EndpointSetup::Testing`]
    /// constructed from a shared `Arc<DnsPkarrServer>`.
    #[allow(clippy::missing_const_for_fn)]
    #[must_use]
    pub fn with_endpoint_setup(mut self, setup: EndpointSetup) -> Self {
        self.endpoint_setup = setup;
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

    /// Set the daemon socket path for the cap-listener. See
    /// [`Self::daemon_socket`].
    #[must_use]
    pub fn with_daemon_socket(mut self, path: PathBuf) -> Self {
        self.daemon_socket = Some(path);
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

/// Which side of the sync a [`WorkspaceEvent::SkippedReadOnly`] fired
/// on.
///
/// `Outgoing` covers a local change the watcher / scan refused to
/// publish. `Incoming` covers a peer-driven event the applier /
/// bulk-export refused to apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// A peer's change was refused at the applier or bulk-export
    /// boundary.
    Incoming,
    /// A local change was refused at the watcher or scan boundary.
    Outgoing,
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
    /// A path-event was skipped because the workspace's
    /// [`PathRules`] classified it [`crate::Mode::ReadOnly`].
    ///
    /// One event per skipped path-event — no coalescing. Mirrors
    /// [`Self::SkippedTooLarge`]'s shape; consumers that find this
    /// noisy (e.g. for a `target/**: ReadOnly` rule with chatty
    /// editor saves) should dedupe themselves.
    SkippedReadOnly {
        /// Absolute path under the workspace root that was skipped.
        path: PathBuf,
        /// Whether the skip happened on the publish side (`Outgoing`)
        /// or the apply side (`Incoming`).
        direction: Direction,
    },
    /// Non-fatal error in the live loop. Logged for the consumer;
    /// the workspace keeps running.
    Error(String),
}

/// A live, attached filesystem workspace.
///
/// Construct via [`Self::host`] or [`Self::join`]. Hold the value
/// to keep the underlying iroh node alive.
///
/// # Shutdown contract
///
/// Callers **must** call [`Self::shutdown`] (and `await` it) before
/// dropping. Drop alone does not close the underlying iroh
/// `Endpoint`: the workspace's QUIC + n0 relay session leaks until
/// the relay's stale-session timeout expires (typically minutes).
/// Because `iroh.key` is persisted, the next host of the same state
/// dir spawns a node with the **same** `EndpointId`; n0's relay
/// rejects the second connection with "Another endpoint connected
/// with the same endpoint id. No more messages will be received." —
/// [`Self::host_with`] then hangs in [`iroh::Endpoint::online`]
/// waiting for relay confirmation that never arrives. Symptom in
/// production runs (e.g. the chat harness): post-restart writes
/// from the host stop reaching peers because outbound gossip can't
/// fan out.
///
/// `Drop` emits a loud `tracing::error!` *and* an `eprintln!` when
/// the workspace is dropped without `shutdown` so a violator notices
/// even without a tracing subscriber. The error is loud on purpose;
/// silencing it means accepting that the next host of this state
/// dir will hang.
///
/// Spawn the watcher + applier with [`Self::run`].
#[derive(Debug)]
pub struct Workspace {
    /// Absolute path of the directory being mirrored.
    pub root: PathBuf,
    /// Doc handle. The watcher writes to it; the applier reads from
    /// `doc.subscribe()`. Borrowed via [`Self::doc`].
    pub(crate) doc: Doc,
    /// Author id used to stamp our writes. Borrowed via
    /// [`Self::author`]. Exposed so integration tests can inject
    /// `InsertRemote`-shaped events that bypass the watcher's rule
    /// check — required by the applier-side defence-in-depth test for
    /// [`crate::Mode::ReadOnly`].
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
    /// Path rules bound at originate-time. Two forms:
    /// `rules` is the wire-shape (kept for [`Self::rules`]
    /// inspection); `compiled_rules` is the precompiled
    /// [`globset::GlobSet`]-backed form consulted on every event
    /// (watcher, applier, scan, bulk-export).
    ///
    /// On the host: the configured (or default-permissive) rules.
    /// On the joiner: the rules decoded from the `workspace.ticket`
    /// envelope — the host's rules, not whatever the joiner
    /// configured.
    pub(crate) rules: PathRules,
    /// Precompiled rules; built once from `rules` at construction
    /// and read per event. See [`Self::rules`].
    pub(crate) compiled_rules: CompiledPathRules,
    /// Session id this workspace is attached to.
    ///
    /// On the host: derived from the workspace's persisted
    /// [`NamespaceId`] via [`crate::session_id_for`] so a re-host of
    /// the same dir under a fresh daemon recovers the same id.
    /// On the joiner: whatever id the daemon's [`Request::JoinSession`]
    /// reply returned. Borrow via [`Self::session_id`].
    pub(crate) session_id: SessionId,
    /// Artel-session [`JoinTicket`] that the daemon issued when this
    /// workspace registered as host. `None` on joiners — joiners
    /// already had a ticket to get here, the workspace doesn't need
    /// to round-trip it. Read via [`Self::join_ticket`].
    pub(crate) join_ticket: Option<JoinTicket>,
    /// Background task draining session events into the [`PeerMap`]
    /// for the docs gate. Aborted on [`Self::shutdown`].
    _cap_listener: tokio::task::JoinHandle<()>,
    /// Drop-bomb sentinel. Flipped to `true` by [`Self::shutdown`];
    /// [`Drop`] checks it and screams if it's still `false` so a
    /// caller that drops without shutting down notices in any
    /// logged run. See struct docs for why this matters.
    pub(crate) did_shutdown: AtomicBool,
}

impl Workspace {
    /// Stand the workspace up as the host.
    ///
    /// Steps:
    /// 1. Spawn a fresh iroh node (Endpoint + Gossip + Docs/Blobs +
    ///    Router) — see [`WorkspaceNode`].
    /// 2. Open (or create) the workspace's `iroh-docs` document and
    ///    derive a stable [`SessionId`] from its `NamespaceId` via
    ///    [`crate::session_id_for`].
    /// 3. Register with the daemon by issuing
    ///    `Request::HostSession { display_name, session: Some(derived_id) }`.
    ///    First-time hosts mint the session at the derived id;
    ///    subsequent restarts resume the existing record verbatim
    ///    (members, log, head preserved). The daemon stamps its own
    ///    authenticated `PeerId` server-side; the IPC caller cannot
    ///    influence it (auth L1, `PROTOCOL_VERSION` 5).
    /// 4. Walk `root`, publish every non-skipped file into the doc.
    /// 5. Share the doc as a `DocTicket` and broadcast it over the
    ///    artel session as a [`MessageKind::System`] message with
    ///    action [`TICKET_ACTION`].
    ///
    /// Returns the [`Workspace`] handle plus the receiver side of
    /// the [`WorkspaceEvent`] stream. Call [`Self::run`] to start
    /// the watcher + applier. Read the workspace's session id via
    /// [`Self::session_id`].
    pub async fn host(
        client: &Client,
        display_name: impl Into<String>,
        root: PathBuf,
        policy: AttachPolicy,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
        Self::host_with(
            client,
            display_name,
            root,
            policy,
            WorkspaceConfig::default(),
        )
        .await
    }

    /// [`Self::host`], but with an explicit [`WorkspaceConfig`] so
    /// callers can override the state directory.
    ///
    /// On a fresh `state_dir`: creates a new doc, derives a
    /// [`SessionId`] from its [`NamespaceId`], and registers with the
    /// daemon at that derived id. Persists the namespace bytes to
    /// `state_dir/doc-id`. On a populated `state_dir`: opens the
    /// previously-published doc, derives the same id again
    /// (deterministic from the persisted namespace), and asks the
    /// daemon to resume the existing host record at that id. **Runs
    /// a reconcile pass** (tombstones doc entries whose backing files
    /// disappeared while we were down) before re-publishing the
    /// remaining on-disk files. The resulting ticket is byte-stable
    /// across restarts so any joiner with the old ticket can resume.
    ///
    /// Errors with [`WorkspaceError::SessionConflict`] when the daemon
    /// already owns the derived id with a different host peer or as a
    /// remote-mirror session — see [`ProtocolError::SessionConflict`].
    pub async fn host_with(
        client: &Client,
        display_name: impl Into<String>,
        root: PathBuf,
        policy: AttachPolicy,
        config: WorkspaceConfig,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
        let display_name = display_name.into();
        // Materialise the workspace dir *before* canonicalising so the
        // canonical form is stable across first and subsequent attaches
        // (canonicalize errors on a non-existent path; the previous
        // shape silently fell back to the raw input, registering a
        // different `local_path` shape between phase 1 and phase 2 of
        // the same workspace's lifecycle). Materialise root first;
        // canonicalise; then create state_dir.
        tokio::fs::create_dir_all(&root).await?;
        let root = canonicalise(&root).await;
        let state_dir = config.resolve(&root);

        // Enforce the policy *before* any state-dir or iroh-node
        // creation so a `Policy` error guarantees no on-disk
        // artefacts were left behind.
        enforce_attach_policy(&root, &state_dir, policy, AttachSide::Host)?;

        // Resolve and compile rules *before* iroh-node spawn too —
        // `compile` validates as a side-effect, so a malformed rule
        // set is rejected as a configuration error, not a runtime one.
        let rules = config.rules.unwrap_or_else(PathRules::read_write);
        let compiled_rules = rules.compile()?;

        ensure_state_dir(&state_dir)?;
        // Canonicalise after ensure_state_dir so the attachment payload
        // (and every consumer of `state_dir` below) carries an absolute
        // canonical path. Without this, a user passing
        // `with_state_dir(PathBuf::from("./state"))` would register a
        // cwd-relative path the daemon can't resolve from another
        // process.
        let state_dir = canonicalise(&state_dir).await;

        // From here on, any failure must roll back the daemon-side
        // session and the iroh node we acquire. Build the rollback
        // guard up-front; populate as we go; disarm on success.
        let mut rb = WorkspaceRollback::default();
        match Self::host_with_inner(
            client,
            display_name,
            root,
            state_dir,
            rules,
            compiled_rules,
            &config.endpoint_setup,
            config.daemon_socket.as_deref(),
            &mut rb,
        )
        .await
        {
            Ok(out) => Ok(out),
            Err(err) => {
                rb.rollback(client).await;
                Err(err)
            }
        }
    }

    /// Inner half of [`Self::host_with`] that runs everything past the
    /// "no rollback needed yet" point. Populates `rb` as fallible
    /// state is acquired so the outer fn can undo on Err.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn host_with_inner(
        client: &Client,
        display_name: String,
        root: PathBuf,
        state_dir: PathBuf,
        rules: PathRules,
        compiled_rules: CompiledPathRules,
        endpoint_setup: &EndpointSetup,
        daemon_socket: Option<&Path>,
        rb: &mut WorkspaceRollback,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
        let daemon_peer_id = client.daemon_peer_id();
        let peer_map = Arc::new(PeerMap::new(daemon_peer_id));
        let node = WorkspaceNode::spawn(&state_dir, endpoint_setup, Arc::clone(&peer_map)).await?;
        rb.node = Some(node);
        // Borrow the node back for the rest of the constructor — it
        // moves into `Self` at the end via `rb.disarm()`.
        let node = rb.node.as_ref().expect("just stored");

        // Register the host's own workspace EndpointId in the peer
        // map so the gate never accidentally blocks the host itself.
        peer_map.register(node.endpoint_id, daemon_peer_id);

        // Persistent docs store; default-author is managed by
        // iroh-docs at `state_dir/docs/default-author`.
        let author = node
            .docs
            .author_default()
            .await
            .map_err(|e| WorkspaceError::Doc(format!("author_default: {e}")))?;

        let doc_id_path = state_dir.join(DOC_ID_FILE);
        let (doc, returning) = open_or_create_doc(node, &doc_id_path).await?;
        debug!(
            target: "artel_fs::workspace",
            namespace = %doc.id(),
            returning,
            "host_with: doc opened"
        );

        // Derive the session id from the persisted NamespaceId
        // *before* registering with the daemon. First host and every
        // subsequent restart land on the same id — that's what
        // gives us resume across daemon restarts.
        let session_id = crate::session_id::session_id_for(doc.id());

        // Register with the daemon. `Some(session_id)` either creates
        // the session at this id (first host) or resumes the existing
        // local-host record (returning host). A `SessionConflict`
        // means a different peer already owns this id locally — the
        // user is pointing two daemons at the same state dir or
        // started two workspaces from one daemon.
        let join_ticket = register_host(client, display_name, session_id).await?;
        // Arm the LeaveSession rollback the moment we own this
        // session in the daemon.
        rb.leave_on_rollback = Some(session_id);

        // Register a typed workspace attachment so a CLI / GUI can
        // enumerate this workspace without reading `~/.artel/`
        // directly. The daemon stores it opaquely (ADR-001 § "Daemon
        // scope: medium"); the schema lives in `crate::attachment`.
        //
        // Order matters: register *after* `register_host` (we need
        // the session id) and *before* reconcile / scan /
        // publish_ticket so the workspace becomes visible to
        // discovery as soon as the session exists, not only after a
        // potentially-slow scan finishes. The 2b cascade clears the
        // attachment when LeaveSession fires (including from our
        // rollback path), so we don't need a separate
        // `forget_attachment` arming for the host.
        register_workspace_attachment(
            client,
            session_id,
            &root,
            &state_dir,
            crate::attachment::WorkspaceRole::Host,
        )
        .await?;

        let (tx, rx) = mpsc::channel(EVENT_BUFFER);
        let echo_guard = EchoGuard::new();

        // Returning host: prune entries that no longer exist on
        // disk *before* we re-publish the current scan. Order
        // matters — tombstoning after re-publishing would erase
        // legitimate entries laid down by the scan.
        if returning {
            debug!(target: "artel_fs::workspace", root = %root.display(), "host_with: reconciling doc against disk");
            reconcile_doc_against_disk(&root, &doc, author, &tx).await?;
        }

        // Pre-populate the doc from disk *before* we share the
        // ticket — joiners that import after this scan see the
        // current snapshot via initial sync.
        debug!(target: "artel_fs::workspace", root = %root.display(), "host_with: scan_and_publish_existing");
        scan_and_publish_existing(&root, &doc, author, &compiled_rules, &echo_guard, &tx).await?;

        // Share with full addressing info (relay URL + direct
        // addrs) so the ticket carries everything a joiner needs
        // to dial on the first try. `AddrInfoOptions::default()`
        // is `Id` — id-only — which forces the joiner's iroh-docs
        // engine to fall back to pkarr/DNS lookup, racing the
        // host's publish-propagate window. iroh-docs does NOT
        // retry a failed first dial, so an id-only ticket on a
        // fresh peering reproducibly stalls `bulk_export` and
        // makes the host's pre-existing entries silently miss
        // the joiner. `RelayAndAddresses` lets iroh-docs's own
        // memory_lookup (populated in `engine::live::join_peers`
        // from `DocTicket.nodes`) seed the addr-book before the
        // first dial — same shape as the daemon-side fix in
        // `bac631f` for `Registry::join`, applied at the right
        // layer (the workspace's iroh-docs node, not the daemon's
        // gossip node).
        // Extract the NamespaceSecret for later delivery to RW joiners.
        // share(Write) returns a DocTicket whose capability contains
        // the secret.
        let write_ticket = doc
            .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
            .await
            .map_err(|e| WorkspaceError::Doc(format!("share doc (write): {e}")))?;
        let namespace_secret = match write_ticket.capability {
            iroh_docs::Capability::Write(ref secret) => secret.to_bytes(),
            iroh_docs::Capability::Read(_) => unreachable!("host always has Write capability"),
        };

        // Broadcast a Read-only ticket. The NamespaceSecret is never
        // included in the broadcast; RW joiners receive it via the
        // targeted upgrade path (see spawn_cap_listener / 3D).
        let ticket = doc
            .share(ShareMode::Read, AddrInfoOptions::RelayAndAddresses)
            .await
            .map_err(|e| WorkspaceError::Doc(format!("share doc: {e}")))?;

        publish_ticket(client, session_id, &ticket, &rules).await?;

        // Spawn the cap-listener on a second Client connection so we
        // don't consume the caller's event stream.
        let shutdown_token = CancellationToken::new();
        let host_ctx = if let Some(socket) = daemon_socket {
            let upgrade_client = Client::connect(socket)
                .await
                .map_err(|e| WorkspaceError::Iroh(format!("upgrade-client connect: {e}")))?;
            // The upgrade client needs a session membership on this
            // connection so the daemon's `Send` handler recognises it.
            // Re-host with the existing session_id (resume path) to
            // populate the per-connection memberships map.
            upgrade_client
                .request(Request::HostSession {
                    display_name: String::new(),
                    session: Some(session_id),
                })
                .await
                .map_err(|e| WorkspaceError::Iroh(format!("upgrade-client host: {e}")))?;
            Some(HostUpgradeCtx {
                client: Arc::new(upgrade_client),
                session: session_id,
                namespace_secret,
            })
        } else {
            None
        };
        let cap_listener = spawn_cap_listener_from_socket(
            daemon_socket,
            session_id,
            Arc::clone(&peer_map),
            shutdown_token.child_token(),
            host_ctx,
            None,
        )
        .await?;

        // All fallible work is done — pull the node out of the
        // rollback guard so it lives in the constructed Workspace.
        let node = std::mem::take(rb)
            .disarm()
            .expect("rb.node populated above");
        let blobs = node.blobs.clone();
        Ok((
            Self {
                root,
                doc,
                author,
                blobs,
                echo_guard,
                events: tx,
                shutdown_token,
                node: tokio::sync::Mutex::new(Some(node)),
                rules,
                compiled_rules,
                session_id,
                join_ticket: Some(join_ticket),
                _cap_listener: cap_listener,
                did_shutdown: AtomicBool::new(false),
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
        // Materialise root before canonicalising — see host_with for
        // the same reasoning (canonicalize errors on a non-existent
        // path; raw-input fallback registers a different shape across
        // attaches).
        tokio::fs::create_dir_all(&root).await?;
        let root = canonicalise(&root).await;
        let state_dir = config.resolve(&root);

        // Enforce the policy before any state-dir / iroh-node /
        // subscribe work so a `Policy` error leaves zero on-disk
        // and zero IPC state.
        enforce_attach_policy(&root, &state_dir, policy, AttachSide::Join)?;

        ensure_state_dir(&state_dir)?;
        // Canonicalise — see host_with for the same reasoning. The
        // attachment payload + iroh node + every later use of
        // state_dir now sees the canonical absolute form.
        let state_dir = canonicalise(&state_dir).await;

        // From here on, any failure must shut down the iroh node and
        // (once registered) forget the joiner-side attachment.
        // Unlike the host, we do NOT issue LeaveSession on rollback —
        // caller's session membership is a precondition, not
        // something the constructor acquired.
        let mut rb = WorkspaceRollback::default();
        match Self::join_with_inner(
            client,
            session,
            root,
            state_dir,
            config.join_ticket_timeout,
            &config.endpoint_setup,
            config.daemon_socket.as_deref(),
            &mut rb,
        )
        .await
        {
            Ok(out) => Ok(out),
            Err(err) => {
                rb.rollback(client).await;
                Err(err)
            }
        }
    }

    /// Inner half of [`Self::join_with`] — see `host_with_inner` for
    /// the same rollback-tracking pattern.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn join_with_inner(
        client: &Client,
        session: SessionId,
        root: PathBuf,
        state_dir: PathBuf,
        join_ticket_timeout: Option<Duration>,
        endpoint_setup: &EndpointSetup,
        join_daemon_socket: Option<&Path>,
        rb: &mut WorkspaceRollback,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
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

        let ticket_result = wait_for_ticket(&mut events, session, join_ticket_timeout).await?;
        let host_daemon_peer_id = ticket_result.host_daemon_peer_id;
        // The host's rules are authoritative — `config.rules` on the
        // joiner side is dropped on the floor here. Documented on
        // `WorkspaceConfig::rules`. Compile here too so the joiner's
        // hot path matches against a precompiled `GlobSet`.
        let rules = ticket_result.envelope.rules;
        let compiled_rules = rules.compile()?;
        let ticket = DocTicket::from_str(&ticket_result.envelope.doc_ticket)
            .map_err(|e| WorkspaceError::Doc(format!("ticket parse: {e}")))?;

        let peer_map = Arc::new(PeerMap::new(host_daemon_peer_id));
        // Register the host's workspace EndpointId from the ticket's
        // first node entry.
        if let Some(node_info) = ticket.nodes.first() {
            peer_map.register(node_info.id, host_daemon_peer_id);
        }

        let node = WorkspaceNode::spawn(&state_dir, endpoint_setup, Arc::clone(&peer_map)).await?;
        rb.node = Some(node);
        let node = rb.node.as_ref().expect("just stored");
        // Joiners don't persist a per-workspace `doc-id` — they
        // import the host's namespace from the ticket each time.
        // The default author is still useful for stamping our own
        // writes once live sync starts.
        let author = node
            .docs
            .author_default()
            .await
            .map_err(|e| WorkspaceError::Doc(format!("author_default: {e}")))?;

        let (doc, live) = node
            .docs
            .import_and_subscribe(ticket)
            .await
            .map_err(|e| WorkspaceError::Doc(format!("doc import: {e}")))?;

        // Register the typed attachment as soon as the doc handle is
        // alive — *before* `wait_for_initial_sync` (a 30 s blocking
        // wait) and *before* `bulk_export`. A joiner whose host is
        // offline would otherwise sit invisible to discovery for the
        // full sync timeout — exactly the case where local
        // enumeration is most useful. Symmetric with the host side's
        // register-before-scan ordering: the workspace becomes
        // visible the moment the session is wired up, not after a
        // potentially-slow seeding step.
        register_workspace_attachment(
            client,
            session,
            &root,
            &state_dir,
            crate::attachment::WorkspaceRole::Joiner,
        )
        .await?;
        // Arm attachment-forget rollback now that the entry exists.
        rb.forget_attachment = Some(session);

        // Drain live events until the first sync round has finished
        // and pending content has settled. Without this, `get_many`
        // returns an empty result and the bulk export is a no-op
        // because the doc state hasn't replicated yet.
        wait_for_initial_sync(live).await?;

        let (tx, rx) = mpsc::channel(EVENT_BUFFER);
        let echo_guard = EchoGuard::new();

        bulk_export(&root, &doc, &node.blobs, &compiled_rules, &echo_guard, &tx).await?;

        // Spawn the cap-listener. On the joiner path, the existing
        // event stream (from wait_for_ticket) is still subscribed —
        // reuse it. If a daemon_socket is configured, prefer a
        // dedicated connection so the caller can reuse their client.
        let shutdown_token = CancellationToken::new();
        let joiner_ctx = Some(JoinerUpgradeCtx {
            my_peer_id: client.daemon_peer_id(),
            docs: node.docs.clone(),
        });
        let cap_listener = if join_daemon_socket.is_some() {
            spawn_cap_listener_from_socket(
                join_daemon_socket,
                session,
                Arc::clone(&peer_map),
                shutdown_token.child_token(),
                None,
                joiner_ctx,
            )
            .await?
        } else {
            spawn_cap_listener(
                events,
                session,
                Arc::clone(&peer_map),
                shutdown_token.child_token(),
                None,
                joiner_ctx,
            )
        };

        // Announce our workspace EndpointId to the host so it can
        // register our mapping in its own PeerMap.
        let announce_resp = client
            .request(Request::Send {
                session,
                payload: SendPayload {
                    kind: MessageKind::System,
                    action: NODE_ID_ACTION.to_string(),
                    payload: node.endpoint_id.as_bytes().to_vec(),
                },
            })
            .await;
        if let Err(err) = &announce_resp {
            debug!(
                target: "artel_fs::workspace",
                %err,
                "join_with: failed to announce workspace node_id"
            );
        }

        let node = std::mem::take(rb)
            .disarm()
            .expect("rb.node populated above");
        let blobs = node.blobs.clone();
        Ok((
            Self {
                root,
                doc,
                author,
                blobs,
                echo_guard,
                events: tx,
                shutdown_token,
                node: tokio::sync::Mutex::new(Some(node)),
                rules,
                compiled_rules,
                session_id: session,
                join_ticket: None,
                _cap_listener: cap_listener,
                did_shutdown: AtomicBool::new(false),
            },
            rx,
        ))
    }

    /// The artel session id this workspace is attached to.
    ///
    /// On the host: derived from the local `NamespaceId` via
    /// [`crate::session_id_for`] — re-hosting the same workspace dir
    /// always lands on the same id, so a joiner who held the old
    /// ticket can keep receiving messages across the host's daemon
    /// restart.
    ///
    /// On the joiner: whatever the daemon's `JoinSession` reply
    /// returned (the host's id, propagated through the artel
    /// session).
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// The artel-session [`JoinTicket`] the daemon issued when this
    /// workspace registered as host. `None` on joiners — they
    /// already had a ticket to get here.
    ///
    /// Hand this ticket to a peer's [`Request::JoinSession`] to add
    /// them to the underlying artel session. Joiners then call
    /// [`Self::join`] (or [`Self::join_with`]) and the workspace's
    /// `workspace.ticket` system message brings the doc-side ticket
    /// over for them.
    #[must_use]
    pub const fn join_ticket(&self) -> Option<&JoinTicket> {
        self.join_ticket.as_ref()
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

    /// Borrow the workspace's [`PathRules`] in wire form.
    ///
    /// On the host: the configured (or default-permissive) rules.
    /// On the joiner: the rules decoded from the host's
    /// `workspace.ticket` envelope (the host's, not whatever the
    /// joiner configured). Surfaced for tests and consumers that
    /// want to inspect the configured rules; the hot path
    /// (watcher / applier / scan / bulk-export) uses
    /// [`Self::compiled_rules`] internally for matcher-once
    /// performance.
    #[must_use]
    pub const fn rules(&self) -> &PathRules {
        &self.rules
    }

    /// The [`AuthorId`] this workspace stamps on its outgoing writes.
    ///
    /// Exposed so integration tests can call
    /// `workspace.doc().set_bytes(workspace.author(), ...)` to inject
    /// peer-driven events that bypass the watcher — required by the
    /// applier-side defence-in-depth test for [`crate::Mode::ReadOnly`].
    /// Production code should not need this; the watcher publishes
    /// outbound writes directly.
    #[must_use]
    pub const fn author(&self) -> AuthorId {
        self.author
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
        debug!(target: "artel_fs::workspace", root = %self.root.display(), "run: spawning watcher + applier");
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
        let (watcher_res, applier_res) = tokio::join!(watcher_ready_rx, applier_ready_rx);
        debug!(
            target: "artel_fs::workspace",
            watcher_ready = watcher_res.is_ok(),
            applier_ready = applier_res.is_ok(),
            "run: both halves signalled (or errored)"
        );
        join
    }

    /// Trigger graceful shutdown.
    ///
    /// Cancels the shutdown token (stopping the watcher + applier
    /// loops) and consumes the underlying [`WorkspaceNode`] —
    /// `Router::shutdown` walks down to `Endpoint::close`, which is
    /// the load-bearing call: without it the iroh QUIC + n0 relay
    /// session leaks until n0's stale-session timeout fires, and
    /// the next host of the same state dir is rejected by the relay
    /// because `iroh.key` is persisted (same `EndpointId`).
    ///
    /// **Must** be `await`ed before the workspace is dropped. See
    /// the struct docs for the full failure mode and why `Drop`
    /// alone isn't enough.
    ///
    /// # Concurrency
    ///
    /// Holds the node mutex across the `await`, so two concurrent
    /// `shutdown` callers serialise: caller B blocks until caller A
    /// has finished tearing the iroh router down, then sees an empty
    /// slot and returns `Ok(())` immediately. This is the contract a
    /// fresh same-state-dir host depends on — without it, B could
    /// return before A's router actually closed and the next
    /// `Endpoint::online` would race A's lingering relay session.
    ///
    /// # Errors
    ///
    /// Returns the first router-shutdown failure verbatim; only the
    /// caller that actually consumed the node can observe it.
    /// Subsequent callers (and any caller arriving after the node was
    /// already taken) return `Ok(())`. The Drop bomb stays armed when
    /// this method returns `Err`, so a violator who logged-and-ignored
    /// a failed shutdown still sees the loud message on Drop.
    // Holding the lock across the await is deliberate: it serialises
    // shutdown so a second caller can't observe completion before the
    // first call's router has actually closed. See struct docs.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn shutdown(&self) -> Result<(), WorkspaceError> {
        debug!(target: "artel_fs::workspace", "shutdown: cancelling token");
        self.shutdown_token.cancel();
        let mut slot = self.node.lock().await;
        let Some(node) = slot.take() else {
            debug!(target: "artel_fs::workspace", "shutdown: node already taken");
            return Ok(());
        };
        debug!(target: "artel_fs::workspace", "shutdown: tearing down iroh node");
        node.shutdown().await?;
        debug!(target: "artel_fs::workspace", "shutdown: iroh node torn down");
        // Only arm the Drop-bomb sentinel after the router actually
        // closed cleanly. `Release` so a thread that observes the
        // flag in `Drop` (after the Workspace's owning Arc moves
        // into the destructor) sees every shutdown effect.
        self.did_shutdown.store(true, Ordering::Release);
        Ok(())
    }

    /// Test-only: arm the next [`Self::shutdown`] on this specific
    /// workspace to fail. Returns `Ok(())` if the node is still in
    /// the slot (the common case — call before any other shutdown
    /// path has consumed it), `Err(())` if the node was already
    /// taken (shutdown ran, or rollback consumed it).
    ///
    /// Per-instance, by design: a process-wide static would let two
    /// parallel tests in the same integration binary trip each
    /// other's fault injection. Sole consumer is
    /// `tests/workspace_shutdown_contract.rs`.
    #[cfg(feature = "test-utils")]
    pub async fn test_arm_shutdown_failure(&self) -> Result<(), ()> {
        // Clone the flag handle out from under the lock so the
        // store happens lock-free — the `Arc<AtomicBool>` is
        // designed to outlive the node's own lifetime.
        let flag = self
            .node
            .lock()
            .await
            .as_ref()
            .map(|node| std::sync::Arc::clone(&node.shutdown_failure_flag))
            .ok_or(())?;
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

impl Drop for Workspace {
    /// Drop bomb: scream if the workspace was dropped without an
    /// `await`ed [`Self::shutdown`]. We can't run async cleanup from
    /// `Drop` (no `await`, no safe `block_on` from inside a tokio
    /// runtime), so all we can do is make the misuse loud.
    ///
    /// Two channels because tracing might not be subscribed in some
    /// runtime configurations (CLI binaries, test harnesses,
    /// throwaway examples like `chat-harness`); `eprintln!` is the
    /// belt-and-braces fallback.
    fn drop(&mut self) {
        if !self.did_shutdown.load(Ordering::Acquire) {
            let msg = "Workspace dropped without calling shutdown(). \
                The iroh Endpoint will not close cleanly; if iroh.key is \
                persisted (the default), the next host of this state dir \
                will be rejected by the n0 relay (\"Another endpoint \
                connected with the same endpoint id\") until the \
                relay's stale-session timeout fires. \
                Call workspace.shutdown().await before dropping.";
            tracing::error!(target: "artel_fs::workspace", "{msg}");
            eprintln!("[artel-fs] {msg}");
        }
    }
}

/// Walk `root` and publish every non-skipped file to `doc`. Errors
/// on a single file surface as [`WorkspaceEvent::Error`]; we do not
/// abort the scan.
///
/// `rules` consultation: any file whose workspace-relative path
/// resolves to [`Mode::ReadOnly`] is skipped with a
/// [`WorkspaceEvent::SkippedReadOnly`] (`Outgoing`). A
/// `default: ReadOnly` workspace publishes nothing here — by design;
/// see plan §"Bulk-export `ReadOnly`: yes, honour it".
async fn scan_and_publish_existing(
    root: &Path,
    doc: &Doc,
    author: AuthorId,
    rules: &CompiledPathRules,
    echo_guard: &EchoGuard,
    events: &mpsc::Sender<WorkspaceEvent>,
) -> Result<(), WorkspaceError> {
    let filter = WorkspaceFilter::new(root);
    let mut published = 0usize;
    let mut skipped = 0usize;
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
            FilterDecision::Skip(_) => {
                skipped += 1;
                continue;
            }
            FilterDecision::Include => {}
        }

        let rel = path.strip_prefix(root).unwrap_or(path);
        if rules.mode_for(rel) == Mode::ReadOnly {
            skipped += 1;
            let _ = events
                .send(WorkspaceEvent::SkippedReadOnly {
                    path: path.to_path_buf(),
                    direction: Direction::Outgoing,
                })
                .await;
            continue;
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
        let len = bytes.len();
        if let Err(err) = doc.set_bytes(author, key, bytes.clone()).await {
            warn!(target: "artel_fs::workspace", path = %path.display(), len, %err, "scan: set_bytes failed");
            let _ = events
                .send(WorkspaceEvent::Error(format!(
                    "scan publish {} failed: {err}",
                    path.display(),
                )))
                .await;
            continue;
        }
        published += 1;
        debug!(target: "artel_fs::workspace", path = %path.display(), len, "scan: published");
        echo_guard.record_local_publish(path, &bytes).await;
    }
    debug!(target: "artel_fs::workspace", root = %root.display(), published, skipped, "scan_and_publish_existing complete");
    Ok(())
}

/// Tracks daemon-side and iroh-side state acquired during a workspace
/// constructor (`Workspace::host_with` / `Workspace::join_with`) so
/// that an error after the fallible-but-rollback-able point can
/// undo what's been done.
///
/// Why explicit rather than `Drop`: rollback requires async work
/// (`LeaveSession` IPC, `WorkspaceNode::shutdown`), and `Drop` is
/// sync. The constructor calls `rollback().await` on its error
/// paths; on success it `disarm`s and extracts the node back so the
/// `Workspace` can own it.
///
/// Joiner-side note: `leave_on_rollback` is intentionally `None` on
/// the joiner. Joiners didn't issue `JoinSession` from inside
/// `Workspace::join_with` — caller membership is a precondition, so
/// rolling back our partial workspace stand-up shouldn't unmember
/// them from a session they joined externally. We only forget the
/// joiner-side attachment.
#[derive(Default)]
struct WorkspaceRollback {
    /// Send `LeaveSession` for this session on rollback (cascades the
    /// attachment via the 2b `delete(session)` cascade). Set after
    /// `register_host` succeeds; left `None` on the joiner.
    leave_on_rollback: Option<SessionId>,
    /// Forget this `(session, KIND_V1)` attachment on rollback.
    /// Used on the joiner side, where `leave_on_rollback` is None.
    forget_attachment: Option<SessionId>,
    /// Shut this node down on rollback. Set after
    /// `WorkspaceNode::spawn` succeeds.
    node: Option<WorkspaceNode>,
}

impl WorkspaceRollback {
    /// Successful path: return the node so the caller can hand it to
    /// the constructed `Workspace`. After this, no rollback fires.
    fn disarm(mut self) -> Option<WorkspaceNode> {
        self.leave_on_rollback = None;
        self.forget_attachment = None;
        self.node.take()
    }

    /// Best-effort cleanup. Errors are logged and swallowed — the
    /// caller is already returning a different `WorkspaceError` and
    /// rollback shouldn't mask it.
    async fn rollback(mut self, client: &Client) {
        if let Some(session) = self.leave_on_rollback.take()
            && let Err(err) = client.request(Request::LeaveSession { session }).await
        {
            tracing::warn!(
                %session,
                error = %err,
                "rollback: LeaveSession failed; daemon retains orphan session",
            );
        }
        if let Some(session) = self.forget_attachment.take() {
            let req = Request::ForgetAttachment {
                session,
                kind: crate::attachment::KIND_V1.to_string(),
            };
            if let Err(err) = client.request(req).await {
                tracing::warn!(
                    %session,
                    error = %err,
                    "rollback: ForgetAttachment failed; daemon retains orphan attachment",
                );
            }
        }
        if let Some(node) = self.node.take()
            && let Err(err) = node.shutdown().await
        {
            tracing::warn!(
                error = %err,
                "rollback: WorkspaceNode shutdown failed; iroh router may not have closed cleanly",
            );
        }
    }
}

/// Issue `Request::HostSession { display_name, session: Some(session_id) }`
/// against the daemon and return the [`JoinTicket`] from the reply.
/// Maps the daemon's resume-conflict variant to
/// [`WorkspaceError::SessionConflict`] so callers can distinguish
/// "wrong peer at this state dir" from generic IPC failures.
///
/// The daemon stamps its authenticated `PeerId` server-side; the
/// `display_name` we pass here only labels this peer in events.
async fn register_host(
    client: &Client,
    display_name: String,
    session_id: SessionId,
) -> Result<JoinTicket, WorkspaceError> {
    match client
        .request(Request::HostSession {
            display_name,
            session: Some(session_id),
        })
        .await
    {
        Ok(Response::HostSession { ticket, .. }) => Ok(ticket),
        Ok(other) => Err(WorkspaceError::Iroh(format!(
            "unexpected response to HostSession: {other:?}",
        ))),
        Err(ClientError::Protocol(ProtocolError::SessionConflict(id))) => {
            Err(WorkspaceError::SessionConflict(id))
        }
        Err(err) => Err(WorkspaceError::Client(err)),
    }
}

/// Register a [`crate::attachment::WorkspaceAttachmentV1`] against
/// `session` via [`Request::RegisterAttachment`].
///
/// Failure propagates as [`WorkspaceError`] — both the host- and
/// joiner-side constructors treat a missing attachment as a
/// stand-up failure rather than degrading silently. A workspace
/// that's invisible to discovery is a real bug we want surfaced;
/// the brainstorm + plan §"Risks" explicitly chose this over
/// graceful degradation.
async fn register_workspace_attachment(
    client: &Client,
    session: SessionId,
    local_path: &Path,
    state_dir: &Path,
    role: crate::attachment::WorkspaceRole,
) -> Result<(), WorkspaceError> {
    let payload = crate::attachment::WorkspaceAttachmentV1 {
        local_path: local_path.to_path_buf(),
        state_dir: state_dir.to_path_buf(),
        role,
    }
    .encode()?;
    match client
        .request(Request::RegisterAttachment {
            session,
            kind: crate::attachment::KIND_V1.to_string(),
            payload,
        })
        .await
    {
        Ok(Response::AttachmentRegistered) => Ok(()),
        Ok(other) => Err(WorkspaceError::Iroh(format!(
            "unexpected response to RegisterAttachment: {other:?}",
        ))),
        // Distinguish "session vanished mid-stand-up" from generic
        // IPC failure so callers can retry the whole flow vs surface
        // a transport error. The cascade-via-LeaveSession from
        // another handle, or a registry eviction race, both surface
        // here as ProtocolError::UnknownSession.
        Err(ClientError::Protocol(ProtocolError::UnknownSession(id))) => {
            Err(WorkspaceError::SessionVanished(id))
        }
        Err(err) => Err(WorkspaceError::Client(err)),
    }
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

async fn publish_upgrade(
    client: &Client,
    session: SessionId,
    target_peer: PeerId,
    namespace_secret: [u8; 32],
) -> Result<(), ClientError> {
    client
        .request(Request::DeliverUpgrade {
            session,
            target_peer,
            namespace_secret,
        })
        .await?;
    Ok(())
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

/// Result of [`wait_for_ticket`]: the decoded envelope plus the host's
/// daemon `PeerId` extracted from the message that carried the ticket.
struct TicketResult {
    envelope: WorkspaceTicketEnvelope,
    host_daemon_peer_id: PeerId,
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
) -> Result<TicketResult, WorkspaceError> {
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
                let host_daemon_peer_id = message.peer.id;
                let envelope = ticket::decode(&message.payload)?;
                return Ok::<_, WorkspaceError>(TicketResult {
                    envelope,
                    host_daemon_peer_id,
                });
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
/// `rules` consultation: any entry whose workspace-relative path
/// resolves to [`Mode::ReadOnly`] is skipped with
/// [`WorkspaceEvent::SkippedReadOnly`] (`Incoming`), tombstones
/// included. A `default: ReadOnly` workspace bulk-exports nothing on
/// join — by design; see plan §"Bulk-export `ReadOnly`: yes, honour it".
///
/// Pending-set entries are inserted via the echo guard so the
/// watcher won't republish what we just wrote. They are released
/// after [`PENDING_RELEASE_GRACE`].
async fn bulk_export(
    root: &Path,
    doc: &Doc,
    blobs: &iroh_blobs::BlobsProtocol,
    rules: &CompiledPathRules,
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

        // Filter + rules sit ABOVE the tombstone branch on both
        // sides (here and in `applier::handle_entry`). A
        // peer-published tombstone whose key resolves to a path the
        // local filter rejects — asymmetric ignore globs across
        // peers, version drift, an attacker-crafted key targeting a
        // hardcoded-skip path like `.git/HEAD` — would otherwise
        // reach `tokio::fs::remove_file` regardless. The `ReadOnly`
        // rule is gated for the same reason: a `ReadOnly` path's
        // incoming tombstone must not trigger `remove_file`, and a
        // `ReadOnly` path's incoming write must not be applied
        // even if the filter would have let it through.
        let rel = path.strip_prefix(root).unwrap_or(&path);
        if rules.mode_for(rel) == Mode::ReadOnly {
            let _ = events
                .send(WorkspaceEvent::SkippedReadOnly {
                    path,
                    direction: Direction::Incoming,
                })
                .await;
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

        if entry.content_len() == 0 {
            let _ = tokio::fs::remove_file(&path).await;
            let _ = events.send(WorkspaceEvent::PeerDeleted { path }).await;
            continue;
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

/// Best-effort canonicalisation. Falls back to the input path on
/// any error — callers must `create_dir_all` first if they need a
/// canonical form. Async to keep the constructors off blocking
/// `std::fs::canonicalize` (network/slow mounts can stall the
/// reactor thread for tens of seconds).
async fn canonicalise(p: &Path) -> PathBuf {
    tokio::fs::canonicalize(p)
        .await
        .unwrap_or_else(|_| p.to_path_buf())
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

    let mut scanned = 0usize;
    let mut tombstoned = 0usize;
    while let Some(res) = stream.next().await {
        let Ok(entry) = res else { continue };
        scanned += 1;
        // Defensive: should be unreachable given the default query.
        if entry.content_len() == 0 {
            continue;
        }

        let path = match keys::key_to_path(root, entry.key()) {
            Ok(p) => p,
            Err(err) => {
                warn!(
                    target: "artel_fs::workspace",
                    key = %String::from_utf8_lossy(entry.key()),
                    %err,
                    "reconcile: invalid key"
                );
                let _ = events
                    .send(WorkspaceEvent::Error(format!(
                        "reconcile invalid key: {err}"
                    )))
                    .await;
                continue;
            }
        };
        if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
            debug!(target: "artel_fs::workspace", path = %path.display(), "reconcile: tombstoning entry not on disk");
            // `Doc::del` writes a tombstone for `prefix`. The key
            // we hand it is exact, so it tombstones just this
            // entry.
            if let Err(err) = doc.del(author, entry.key().to_vec()).await {
                warn!(target: "artel_fs::workspace", path = %path.display(), %err, "reconcile: tombstone failed");
                let _ = events
                    .send(WorkspaceEvent::Error(format!(
                        "reconcile tombstone {}: {err}",
                        path.display(),
                    )))
                    .await;
            } else {
                tombstoned += 1;
            }
        }
    }
    debug!(
        target: "artel_fs::workspace",
        root = %root.display(),
        scanned,
        tombstoned,
        "reconcile_doc_against_disk complete"
    );
    Ok(())
}

/// Open a second [`Client`] connection, subscribe to `session`, and
/// spawn the cap-listener task on that independent event stream.
///
/// Returns a no-op handle if `socket` is `None` (the gate still
/// rejects based on the seed state populated at construction).
/// Host-side context for delivering `NamespaceSecret` upgrades when a
/// peer is granted RW. `None` on the joiner side.
struct HostUpgradeCtx {
    client: Arc<Client>,
    session: SessionId,
    namespace_secret: [u8; 32],
}

/// Joiner-side context for receiving `NamespaceSecret` upgrades.
struct JoinerUpgradeCtx {
    my_peer_id: PeerId,
    docs: iroh_docs::protocol::Docs,
}

async fn spawn_cap_listener_from_socket(
    socket: Option<&Path>,
    session: SessionId,
    peer_map: Arc<PeerMap>,
    cancel: CancellationToken,
    host_ctx: Option<HostUpgradeCtx>,
    joiner_ctx: Option<JoinerUpgradeCtx>,
) -> Result<tokio::task::JoinHandle<()>, WorkspaceError> {
    let Some(socket) = socket else {
        return Ok(tokio::spawn(async move {
            cancel.cancelled().await;
        }));
    };
    let cap_client = Client::connect(socket)
        .await
        .map_err(|e| WorkspaceError::Iroh(format!("cap-listener connect: {e}")))?;
    match cap_client
        .request(Request::Subscribe {
            session,
            since: None,
        })
        .await?
    {
        Response::Subscribed { .. } => {}
        other => {
            return Err(WorkspaceError::Iroh(format!(
                "cap-listener subscribe: unexpected response: {other:?}",
            )));
        }
    }
    let events = cap_client
        .take_events()
        .await
        .ok_or_else(|| WorkspaceError::Iroh("cap-listener: events already taken".into()))?;
    Ok(spawn_cap_listener(
        events, session, peer_map, cancel, host_ctx, joiner_ctx,
    ))
}

/// Spawn a background task that drains session events into `peer_map`.
///
/// Processes three event types:
/// - `MessageKind::Capability`: applies grant/revoke to the cap-set
///   projection so the docs gate starts rejecting revoked peers.
///   On the host side, an RW grant also triggers delivery of the
///   `NamespaceSecret` to the promoted peer.
/// - `MessageKind::System` with `NODE_ID_ACTION`: registers the mapping
///   from a joiner's workspace `EndpointId` to their daemon `PeerId`.
/// - `MessageKind::System` with `UPGRADE_ACTION`: on the joiner side,
///   imports the `NamespaceSecret` to gain Write capability.
///
/// Runs until `cancel` is triggered (from `Workspace::shutdown`).
fn spawn_cap_listener(
    mut events: artel_client::EventStream,
    session: SessionId,
    peer_map: Arc<PeerMap>,
    cancel: CancellationToken,
    host_ctx: Option<HostUpgradeCtx>,
    joiner_ctx: Option<JoinerUpgradeCtx>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                ev = events.recv() => {
                    let Some(ev) = ev else { break };
                    match ev {
                        Event::Message {
                            session: ev_session,
                            message,
                        } if ev_session == session => {
                            match message.kind {
                                MessageKind::Capability => {
                                    peer_map.apply_capability(
                                        message.peer.id,
                                        &message.payload,
                                    );
                                    // Host: on RW grant, deliver the
                                    // NamespaceSecret to the promoted peer.
                                    // Check has_rw AFTER apply so a grant
                                    // whose peer was later revoked (during
                                    // replay) is suppressed.
                                    if let Some(ref ctx) = host_ctx
                                        && let Ok(artel_protocol::capability::CapabilityAction::Grant {
                                            peer,
                                            cap: artel_protocol::capability::Capability::ReadWrite,
                                        }) = artel_protocol::capability::CapabilityAction::decode(&message.payload)
                                        && peer_map.has_rw(peer)
                                    {
                                        let client = Arc::clone(&ctx.client);
                                        let sess = ctx.session;
                                        let secret = ctx.namespace_secret;
                                        tokio::spawn(async move {
                                            if let Err(e) = publish_upgrade(
                                                &client, sess, peer, secret,
                                            ).await {
                                                warn!(?e, ?peer, "upgrade delivery failed");
                                            }
                                        });
                                    }
                                }
                                MessageKind::System if message.action == NODE_ID_ACTION => {
                                    if let Ok(bytes) = <[u8; 32]>::try_from(message.payload.as_slice())
                                        && let Ok(workspace_id) = iroh::EndpointId::from_bytes(&bytes)
                                    {
                                        peer_map.register(workspace_id, message.peer.id);
                                    }
                                }
                                MessageKind::System if message.action == UPGRADE_ACTION => {
                                    if let Some(ref ctx) = joiner_ctx
                                        && message.peer.id == peer_map.host_peer_id()
                                        && let Ok(payload) = postcard::from_bytes::<UpgradePayload>(&message.payload)
                                        && payload.target_peer == ctx.my_peer_id
                                    {
                                        let secret = iroh_docs::NamespaceSecret::from_bytes(&payload.namespace_secret);
                                        let cap = iroh_docs::Capability::Write(secret);
                                        if let Err(e) = ctx.docs.import_namespace(cap).await {
                                            warn!("workspace.upgrade import_namespace failed: {e}");
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        // Host: re-deliver the upgrade to a peer that
                        // (re-)joins while already holding RW. Covers
                        // the case where the original broadcast was
                        // missed due to a network blip.
                        Event::PeerJoined {
                            session: ev_session,
                            peer: joined_peer,
                        } if ev_session == session => {
                            if let Some(ref ctx) = host_ctx
                                && peer_map.has_rw(joined_peer.id)
                            {
                                let client = Arc::clone(&ctx.client);
                                let sess = ctx.session;
                                let secret = ctx.namespace_secret;
                                let peer = joined_peer.id;
                                tokio::spawn(async move {
                                    if let Err(e) = publish_upgrade(
                                        &client, sess, peer, secret,
                                    ).await {
                                        warn!(?e, ?peer, "upgrade re-delivery on rejoin failed");
                                    }
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    })
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
