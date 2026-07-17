//! `Workspace` — an attached, replicating workspace over an artel
//! session.
//!
//! Two modes, one type:
//! - [`Workspace::host`] creates a fresh `iroh-docs` document, scans
//!   the supplied directory, publishes its files into the doc, and
//!   hands the resulting [`DocTicket`] to its daemon
//!   (`PublishWorkspaceTicket`), which unicasts it host→peer to each
//!   admitted member — never over the gossip topic.
//! - [`Workspace::join`] waits for the daemon's synthetic
//!   [`TICKET_ACTION`] system message carrying the envelope, imports
//!   the ticket, and bulk-exports the doc to its own copy of the
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
    Event, JoinTicket, MessageKind, PeerId, ProtocolError, Request, Response, SendPayload, Seq,
    SessionId, SessionMessage, UpgradePayload,
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
use crate::error::{PolicyViolation, WorkspaceError};
use crate::filter::{ExcludeRules, FilterDecision, SkipReason, WorkspaceFilter};
use crate::keys;
use crate::node::WorkspaceNode;
use crate::peer_map::PeerMap;
use crate::rules::{CompiledPathRules, Mode, PathRules};
use crate::ticket::{self, WorkspaceTicketEnvelope};
use artel_iroh_setup::EndpointSetup;

/// Maximum number of offending entries surfaced in a
/// [`PolicyViolation::DirNotEmpty`] error before truncation. Five is
/// enough to be diagnostic without flooding terminals on a
/// disastrously-wrong dir (e.g. `~`).
const POLICY_OFFENDING_LIMIT: usize = 5;

/// Action stamped on the synthetic `MessageKind::System`
/// ticket-handout message.
///
/// The joiner's daemon injects it from the unicast-delivered
/// envelope; joiners filter on this to find the ticket inside the
/// session's event stream. Re-exported from `artel-protocol` so the
/// daemon's injector and this consumer can't drift.
pub const TICKET_ACTION: &str = artel_protocol::TICKET_ACTION;

/// Action stamped on the `MessageKind::System` message a joiner sends
/// to announce its workspace `EndpointId` to the host. Payload is the
/// raw 32-byte `EndpointId`.
pub const NODE_ID_ACTION: &str = "workspace.node_id";

use artel_protocol::{
    DOWNGRADE_ACTION, DowngradePayload, ROTATE_ACTION, RotatePayload, UPGRADE_ACTION,
};

/// Capacity of the [`Workspace::events`] channel. Modest cap so a
/// stuck consumer back-pressures the watcher rather than letting
/// events queue without bound.
const EVENT_BUFFER: usize = 64;

/// First retry delay for the cap-listener's reconnect loop. Subsequent
/// attempts double this (see [`cap_reconnect_backoff`]) up to
/// [`CAP_RECONNECT_MAX_DELAY`].
const CAP_RECONNECT_BASE_DELAY: Duration = Duration::from_millis(100);

/// Ceiling on the cap-listener reconnect backoff — once the doubling
/// delay reaches this, it stays here for the remaining attempts.
const CAP_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(5);

/// How many reconnect attempts the cap-listener makes after an EOF
/// before giving up and surfacing a [`WorkspaceError`]. A genuinely
/// dead daemon shouldn't keep a listener task spinning indefinitely.
/// The total wait before giving up is the sum of the backoff series
/// (~0.1s + 0.2s + … capped at 5s) — roughly a minute at the default,
/// long enough to ride out a daemon restart but bounded.
const CAP_RECONNECT_MAX_ATTEMPTS: u32 = 16;

/// Default name of the workspace's per-`root` state directory.
///
/// Lives inside the workspace so it travels with `root` for free;
/// added to the hardcoded filter skip list so the watcher never
/// tries to publish iroh's own redb / blob files into the doc.
pub const DEFAULT_STATE_SUBDIR: &str = ".artel-fs";

/// File inside `state_dir` that stores the host's **genesis**
/// `NamespaceId` — the namespace the session was *born* with. 32 raw
/// bytes; namespaces aren't secret so no special permissions are
/// required. **Write-once: never rewritten on namespace rotation.**
/// It is the stable root the `SessionId` derivation reads, so a
/// rotation changes the document holding content (see
/// [`CURRENT_NAMESPACE_FILE`]) without changing the session id, gossip
/// topic, or any issued ticket. See CONTEXT.md "Genesis namespace".
const DOC_ID_FILE: &str = "doc-id";

/// File inside `state_dir` that stores the host's **current**
/// `NamespaceId` — the document the workspace is *currently* writing
/// to. 32 raw bytes. Absent until the first rotation, in which case the
/// current namespace *is* the genesis ([`DOC_ID_FILE`]); written (and
/// rewritten) on each rotation. Decoupled from `DOC_ID_FILE` so the
/// `SessionId` derivation stays pinned to genesis. See CONTEXT.md
/// "Current namespace".
const CURRENT_NAMESPACE_FILE: &str = "current-namespace";

/// File inside `state_dir` that stores the host's **current**
/// `namespace_epoch` — the monotonic rotation counter, as a little-endian
/// `u64`. Absent until the first rotation (absent ⇒ epoch 0, i.e.
/// genesis); rewritten alongside [`CURRENT_NAMESPACE_FILE`] on each
/// rotation/reimport.
///
/// **Load-bearing across restart (C2):** the epoch lives in an in-memory
/// `AtomicU64` at runtime, but a returning host must recover it from
/// disk. Without persistence the counter resets to 0 and a second
/// eviction re-mints epoch 1, which any survivor already at epoch 1
/// ignores as stale/duplicate — silently stranding it on the
/// pre-rotation namespace. Epoch ids aren't secret, so no chmod.
const NAMESPACE_EPOCH_FILE: &str = "namespace-epoch";

/// File inside `state_dir` that stores the highest `Revoke` log seq the
/// host has already rotated for, as a little-endian `u64` (C3).
///
/// On a host restart the cap-listener re-subscribes with `since: None`,
/// so the daemon replays the whole session log — including historical
/// `Revoke` messages. Without a high-water mark, every past eviction
/// would re-fire the rotation on each restart (re-mint a namespace,
/// re-distribute, churn survivors). The rotation task skips any
/// `HostEvict` whose `revoke_seq <=` this water mark, and advances +
/// persists it after a successful rotation. Seqs aren't secret, so no
/// chmod.
const ROTATED_REVOKE_SEQ_FILE: &str = "rotated-revoke-seq";

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
    /// after the host first published. Set `Some(duration)` when
    /// you'd rather fail fast on a misconfigured session (e.g.
    /// wrong ticket, daemons that can't reach each other).
    pub join_ticket_timeout: Option<Duration>,

    /// Pick the workspace endpoint's discovery layer. Real
    /// deployments use [`EndpointSetup::Production`] (the default —
    /// `presets::N0`, pkarr publish + DNS resolve via n0
    /// infrastructure, with home-relay readiness on `online()`).
    /// Integration tests that spin up many workspace nodes in
    /// rapid succession use `EndpointSetup::Testing` with a
    /// shared `Arc<DnsPkarrServer>` so discovery runs against a
    /// localhost pkarr+DNS pair — deterministic, fast, no n0
    /// rate-limit exposure.
    ///
    /// Mirrors the daemon's `DaemonConfig::endpoint_setup` —
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
    /// announcements) and project them into the `PeerMap` that
    /// backs the docs gate. Without this, the gate still rejects
    /// connections from already-revoked peers (seeded at
    /// construction) but cannot observe revocations that happen
    /// after the workspace is up.
    pub daemon_socket: Option<PathBuf>,

    /// Consumer-owned sync exclusions (filter layer 3).
    ///
    /// - `None` (default): hidden (dot-prefixed) files and
    ///   directories don't sync — a filesystem convention, not a
    ///   policy interpretation.
    /// - `Some(globs)`: **exactly** that list, replace not merge.
    ///   `Some(vec![])` syncs everything, dotfiles included. Globs
    ///   use the same workspace-relative shape as [`PathRules`]
    ///   globs and are validated identically.
    ///
    /// **Local to this node, not ticket-borne.** Unlike
    /// [`Self::rules`], the exclude list does not travel to joiners
    /// — it is each node's own hygiene, and nothing about it rides
    /// the synced tree. A host and a joiner may run different
    /// excludes; each filters both its outgoing publishes and its
    /// incoming applies by its own list, and every such skip is
    /// surfaced as [`WorkspaceEvent::SkippedExcluded`].
    ///
    /// An app with a **hidden state dir** (say a per-peer event log
    /// under `<root>/.myapp/log/`) must opt back in or its subtree
    /// won't sync: pass `Some(vec![])`, or restate the exclusions it
    /// wants minus its own dir. Anything beyond this knob — secrets
    /// scanning, `.gitignore` semantics — is consumer policy: read
    /// the sources yourself and pass equivalent globs.
    pub exclude: Option<Vec<String>>,
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
    /// is the default; tests pass an `EndpointSetup::Testing`
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

    /// Set the sync-exclusion globs. `Some(vec![])` syncs everything
    /// (dotfiles included); an explicit list **replaces** the dotfile
    /// default. See [`Self::exclude`].
    #[must_use]
    pub fn with_exclude(mut self, exclude: Option<Vec<String>>) -> Self {
        self.exclude = exclude;
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

/// Which side a directional [`WorkspaceEvent`] fired on.
///
/// For [`WorkspaceEvent::SkippedReadOnly`]: `Outgoing` covers a local
/// change the watcher / scan refused to publish; `Incoming` covers a
/// peer-driven event the applier / bulk-export refused to apply.
///
/// For [`WorkspaceEvent::RevokedPeerBlocked`]: `Incoming` is an inbound
/// connection from the revoked peer rejected post-handshake; `Outgoing`
/// is an outbound dial to the revoked peer refused before connecting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// A peer-originated action was refused on the receiving boundary.
    Incoming,
    /// A locally-originated action was refused on the sending boundary.
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
    /// A path-event was skipped because the path matched this node's
    /// sync exclusions ([`WorkspaceConfig::exclude`] — the dotfile
    /// default or an explicit glob list).
    ///
    /// Advisory, one event per skipped path-event, both directions.
    /// A workspace scan over a tree with many hidden files emits many
    /// of these; treat them as noise, not errors. Exists so an
    /// excluded path never vanishes *silently* — an app whose state
    /// dir isn't syncing finds the answer on this stream instead of
    /// in a debug log.
    SkippedExcluded {
        /// Absolute path under the workspace root that was skipped.
        path: PathBuf,
        /// Whether the skip happened on the publish side (`Outgoing`)
        /// or the apply side (`Incoming`).
        direction: Direction,
    },
    /// The host cooperatively demoted this node (RW → Read). The
    /// watcher has stopped publishing local changes (a voluntary
    /// write-stop); the node keeps reading peer writes. Surfaced so a
    /// consumer (e.g. a TUI send-gate) can reflect the demotion.
    Demoted,
    /// The host revoked `peer`'s capability and this workspace's
    /// cap-listener has applied it: from this moment the transport
    /// gates (`PeerFilter` / `DocsGate`) block that peer. Fires on
    /// every application, including session-log replay after a
    /// reconnect or workspace restart — consumers should treat it as
    /// idempotent state ("peer is out"), not an edge.
    PeerRevoked {
        /// The revoked peer's daemon-level id.
        peer: PeerId,
    },
    /// A connection involving a revoked peer was blocked at the
    /// transport layer. `Incoming`: the peer dialed us and the
    /// post-handshake filter rejected it. `Outgoing`: our own iroh
    /// node tried to dial the revoked peer (e.g. iroh-docs sync
    /// addressing a stale member) and the dial was refused.
    ///
    /// Advisory: the block itself is already enforced when this
    /// event fires. Surfaced so an orchestrator or UI can observe
    /// that a revoked peer is still attempting to sync.
    RevokedPeerBlocked {
        /// The revoked peer's daemon-level id.
        peer: PeerId,
        /// Which side of the connection was blocked.
        direction: Direction,
    },
    /// Non-fatal error in the live loop. Logged for the consumer;
    /// the workspace keeps running.
    Error(String),
}

/// Emit a [`WorkspaceEvent`] from a live loop **without ever
/// blocking** on the channel.
///
/// The applier drives `doc.subscribe()`; if it parks in
/// `events.send().await` waiting for a slow or absent
/// [`WorkspaceEvent`] consumer, the bounded subscribe channel
/// back-pressures into iroh-docs' single live actor and **all sync +
/// gossip for the namespace freezes** — every peer goes silent until
/// the consumer drains, not just this workspace. This was observed as
/// a multi-second runtime-wide stall when a between-restart edit
/// produced a CRDT conflict storm whose `PeerWrote` / `PeerDeleted`
/// events overran [`EVENT_BUFFER`] against a non-draining receiver.
///
/// `WorkspaceEvent`s are advisory (diagnostics + skip notices), so
/// dropping one when the consumer can't keep up is strictly better
/// than wedging replication. Used by the live loops (applier,
/// watcher); the construction-time emitters (scan / bulk-export /
/// reconcile) keep their blocking `send` — they run bounded by the
/// directory size, not a feedback loop, and aren't coupled to the
/// live actor.
pub(crate) fn emit_event(events: &mpsc::Sender<WorkspaceEvent>, ev: WorkspaceEvent) {
    use tokio::sync::mpsc::error::TrySendError;
    // `Ok` (delivered) and `Closed` (consumer dropped its stream) both
    // need no action — the workspace keeps replicating regardless.
    // Only a full channel is worth a line, and only at debug.
    if let Err(TrySendError::Full(ev)) = events.try_send(ev) {
        debug!(
            target: "artel_fs::workspace",
            ?ev,
            "event channel full; dropping advisory event to keep sync live",
        );
    }
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
    /// Doc handle for the **current** namespace. The watcher writes to
    /// it; the applier reads from `doc.subscribe()`. Cloned via
    /// [`Self::doc`]. Behind a `Mutex` so namespace rotation (Slice 3d
    /// re-import) can swap in the new namespace's handle while the
    /// consumer's `Arc<Workspace>` stays stable; `Doc` is a cheap
    /// `Clone` handle, so readers clone it out and drop the lock.
    pub(crate) doc: std::sync::Mutex<Doc>,
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
    /// Cancellation token tripped by [`Self::shutdown`] to stop *all*
    /// background tasks at workspace end. Parent of both
    /// [`Self::doc_token`] and the cap-listener's token, so cancelling
    /// it takes everything down.
    pub(crate) shutdown_token: CancellationToken,
    /// Doc-scoped cancellation token — a child of [`Self::shutdown_token`]
    /// that scopes only the watcher / applier / iroh node, **not** the
    /// cap-listener. Stored behind a `Mutex` so namespace rotation
    /// (Slice 3 re-import) can cancel it and install a fresh child to
    /// tear down + respawn the doc machinery against the new namespace
    /// *without* killing the cap-listener that carries the
    /// rotation-bump signal. Watcher/applier select on the value here
    /// at spawn time (see [`Self::doc_token`]).
    pub(crate) doc_token: std::sync::Mutex<CancellationToken>,
    /// Set when the host has cooperatively demoted this node (RW →
    /// Read) via a `DOWNGRADE_ACTION` notification. The watcher checks
    /// it before every publish/delete and skips when halted — a
    /// *voluntary* write-stop (cooperative threat model only; the
    /// cryptographic write cut-off is namespace rotation on Evict).
    /// `Relaxed` is sufficient: the only correctness requirement is
    /// that the watcher eventually observes the flip, and a stray
    /// publish in the race window is exactly the cooperative-trust
    /// assumption this slice rests on.
    ///
    /// Shared (`Arc`) with the joiner's `cap_listener`, which spawns
    /// before the `Workspace` is constructed and flips the flag on a
    /// `DOWNGRADE_ACTION`. The watcher reads it via [`Self::write_halted`].
    pub(crate) write_halted: Arc<std::sync::atomic::AtomicBool>,
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
    /// Compiled sync exclusions (filter layer 3), built once from
    /// [`WorkspaceConfig::exclude`] at construction. Local to this
    /// node — never ticket-borne. Cloned into the watcher's and
    /// applier's [`WorkspaceFilter`]s.
    pub(crate) exclude: ExcludeRules,
    /// The workspace's state directory (holds `iroh.key`, `doc-id`,
    /// `current-namespace`, the docs/blobs stores). Retained so
    /// namespace rotation can persist the new `current-namespace`.
    pub(crate) state_dir: PathBuf,
    /// Session id this workspace is attached to.
    ///
    /// On the host: derived from the workspace's persisted **genesis**
    /// [`NamespaceId`] via [`crate::session_id_for`] so a re-host of
    /// the same dir under a fresh daemon recovers the same id, and a
    /// namespace rotation doesn't change it.
    /// On the joiner: whatever id the daemon's [`Request::JoinSession`]
    /// reply returned. Borrow via [`Self::session_id`].
    pub(crate) session_id: SessionId,
    /// Artel-session [`JoinTicket`] that the daemon issued when this
    /// workspace registered as host. `None` on joiners — joiners
    /// already had a ticket to get here, the workspace doesn't need
    /// to round-trip it. Read via [`Self::join_ticket`].
    pub(crate) join_ticket: Option<JoinTicket>,
    /// Host-side context for distributing a rotated namespace's ticket
    /// to survivors (Slice 3e). `Some` on the host (carries the
    /// daemon `Client` + session), `None` on joiners (a survivor never
    /// distributes — it only receives). Reuses the same client the
    /// host's cap-listener upgrade path uses.
    pub(crate) rotation_distribute_ctx: Option<RotationDistributeCtx>,
    /// Current `namespace_epoch` (Slice 3e). Starts at 0 (genesis).
    /// Bumped by the host on each rotation and by a survivor when it
    /// reimports onto a higher epoch. A survivor ignores a rotation
    /// whose epoch is not strictly greater (idempotent re-delivery /
    /// out-of-order). `Relaxed` — the rotation task is the only writer
    /// and reads its own writes.
    pub(crate) namespace_epoch: std::sync::atomic::AtomicU64,
    /// Highest `Revoke` log seq already rotated for (C3). Seeded from
    /// [`ROTATED_REVOKE_SEQ_FILE`] on host construction; the rotation
    /// task skips any `HostEvict` whose `revoke_seq` is `<=` this and
    /// advances + persists it on a successful rotation. Defeats spurious
    /// re-rotation when the cap-listener replays historical revokes on
    /// restart. `Relaxed` — the rotation task is the sole writer/reader.
    pub(crate) rotated_revoke_seq: std::sync::atomic::AtomicU64,
    /// Receiver for namespace-rotation signals (Slice 3e). The
    /// cap-listener (which has no `Arc<Workspace>`) sends a
    /// [`RotationSignal`] here; the rotation task spawned in
    /// [`Self::run`] drains it and performs the actual rotate / reimport
    /// (which need the `Arc<Self>`). `Mutex<Option<...>>` so `run` can
    /// `take` it exactly once even though `run` is re-entrant (reimport
    /// calls it again — the second take yields `None` and skips
    /// respawning a duplicate rotation task). Absent ⇒ no rotation
    /// wiring (e.g. socket-less test workspaces).
    ///
    /// **Unbounded (C4):** the sender is called from the cap-listener's
    /// sync context, so it can't `await` back-pressure. A bounded
    /// `try_send` silently dropped signals on a full buffer — and a
    /// dropped `HostEvict` leaves the evicted peer cryptographically
    /// un-cut (no rotation). Rotations are rare and the signal is tiny,
    /// so an unbounded channel trades a negligible memory ceiling for
    /// guaranteed delivery.
    pub(crate) rotation_rx: std::sync::Mutex<Option<mpsc::UnboundedReceiver<RotationSignal>>>,
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
    ///    Router) — see `WorkspaceNode`.
    /// 2. Open (or create) the workspace's `iroh-docs` document and
    ///    derive a stable [`SessionId`] from its `NamespaceId` via
    ///    [`crate::session_id_for`].
    /// 3. Register with the daemon by issuing
    ///    `Request::HostSession { display_name, session: Some(derived_id) }`.
    ///    First-time hosts mint the session at the derived id;
    ///    subsequent restarts resume the existing record verbatim
    ///    (members, log, head preserved). The daemon stamps its own
    ///    authenticated `PeerId` server-side; the IPC caller cannot
    ///    influence it (auth L1, since protocol version 5).
    /// 4. Walk `root`, publish every non-skipped file into the doc.
    /// 5. Share the doc as a `DocTicket` and publish it to the daemon
    ///    via `PublishWorkspaceTicket`; the daemon persists the
    ///    envelope and unicasts it host→peer to each admitted member
    ///    (joiners see it as a synthetic [`TICKET_ACTION`] message).
    ///
    /// Returns the [`Workspace`] handle plus the receiver side of
    /// the [`WorkspaceEvent`] stream. Call [`Self::run`] to start
    /// the watcher + applier. Read the workspace's session id via
    /// [`Self::session_id`].
    ///
    /// # Errors
    ///
    /// Propagates every failure of [`Self::host_with`], which it calls
    /// with [`WorkspaceConfig::default`].
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
    /// # Errors
    ///
    /// - [`WorkspaceError::Policy`] if `policy` rejects the existing
    ///   `root` / `state_dir` (e.g. a non-empty dir under a strict
    ///   policy); checked before any on-disk artefacts are created.
    /// - [`WorkspaceError::PathRules`] / the rules error type if the
    ///   configured [`PathRules`] fail to compile.
    /// - [`WorkspaceError::Io`] / [`WorkspaceError::Iroh`] if the state
    ///   directory or iroh node can't be created, or [`WorkspaceError::Doc`]
    ///   for iroh-docs failures (doc open/create, sync, share, import).
    /// - [`WorkspaceError::SessionConflict`] when the daemon already
    ///   owns the derived id with a different host peer or as a
    ///   remote-mirror session — see [`ProtocolError::SessionConflict`].
    ///
    /// On any failure the rollback guard tears down the daemon-side
    /// session and the acquired iroh node before returning.
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
        // Same treatment for the exclude globs: reject a malformed
        // list as a configuration error before any state is created.
        let exclude = ExcludeRules::compile(config.exclude.as_deref())?;

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
            exclude,
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
        exclude: ExcludeRules,
        endpoint_setup: &EndpointSetup,
        daemon_socket: Option<&Path>,
        rb: &mut WorkspaceRollback,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
        let daemon_peer_id = client.daemon_peer_id();
        let peer_map = Arc::new(PeerMap::new(daemon_peer_id));
        // Create the event channel before the node spawns: the node's
        // transport-layer filters (PeerFilter / DocsGate) emit
        // `RevokedPeerBlocked` events on it.
        let (tx, rx) = mpsc::channel(EVENT_BUFFER);
        let node = WorkspaceNode::spawn(
            &state_dir,
            endpoint_setup,
            Arc::clone(&peer_map),
            tx.clone(),
        )
        .await?;
        rb.node = Some(node);
        // Borrow the node back for the rest of the constructor — it
        // moves into `Self` at the end via `rb.disarm()`.
        let node = rb.node.as_ref().expect("just stored");

        // Register the host's own workspace EndpointId in the peer
        // map so the gate never accidentally blocks the host itself.
        peer_map.register(node.endpoint_id, daemon_peer_id);

        // Same-seed author: bound to this node's endpoint key by
        // `WorkspaceNode::spawn`, so `AuthorId == endpoint_id`.
        let author = node.author;

        let doc_id_path = state_dir.join(DOC_ID_FILE);
        let current_ns_path = state_dir.join(CURRENT_NAMESPACE_FILE);
        let epoch_path = state_dir.join(NAMESPACE_EPOCH_FILE);
        let OpenedDoc {
            doc,
            genesis,
            returning,
            epoch,
        } = open_or_create_doc(node, &doc_id_path, &current_ns_path, &epoch_path).await?;
        // Recover the rotated-revoke-seq high-water mark (C3) so replayed
        // historical revokes don't re-rotate. A fresh/recreated genesis
        // (`!returning`) starts a new session log with no history, so
        // reset the mark and clear any stale file.
        let rotated_revoke_seq_path = state_dir.join(ROTATED_REVOKE_SEQ_FILE);
        let rotated_revoke_seq = if returning {
            read_u64_file(&rotated_revoke_seq_path, "rotated-revoke-seq")?
        } else {
            if let Err(err) = std::fs::remove_file(&rotated_revoke_seq_path)
                && err.kind() != io::ErrorKind::NotFound
            {
                return Err(WorkspaceError::Doc(format!(
                    "reset rotated-revoke-seq at {}: {err}",
                    rotated_revoke_seq_path.display(),
                )));
            }
            0
        };
        doc.start_sync(vec![])
            .await
            .map_err(|e| WorkspaceError::Doc(format!("start_sync: {e}")))?;
        debug!(
            target: "artel_fs::workspace",
            namespace = %doc.id(),
            %genesis,
            returning,
            "host_with: doc opened + sync registered"
        );

        // Derive the session id from the **genesis** NamespaceId
        // *before* registering with the daemon. Genesis is write-once,
        // so the session id is stable across both daemon restarts and
        // (future) namespace rotations — that's what gives us resume.
        let session_id = crate::session_id::session_id_for(genesis);

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

        let echo_guard = EchoGuard::new();

        // Returning host: prune entries that no longer exist on
        // disk *before* we re-publish the current scan. Order
        // matters — tombstoning after re-publishing would erase
        // legitimate entries laid down by the scan.
        if returning {
            debug!(target: "artel_fs::workspace", root = %root.display(), "host_with: reconciling doc against disk");
            reconcile_doc_against_disk(&root, &doc, author, &exclude, &tx).await?;
        }

        // Pre-populate the doc from disk *before* we share the
        // ticket — joiners that import after this scan see the
        // current snapshot via initial sync.
        debug!(target: "artel_fs::workspace", root = %root.display(), "host_with: scan_and_publish_existing");
        scan_and_publish_existing(
            &root,
            &doc,
            author,
            &compiled_rules,
            &exclude,
            &echo_guard,
            &tx,
        )
        .await?;

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

        // Publish a Read-only ticket to the daemon (unicast
        // distribution — never broadcast). The NamespaceSecret is
        // never included in the envelope; RW joiners receive it via
        // the targeted upgrade path (see spawn_cap_listener / 3D).
        let ticket = doc
            .share(ShareMode::Read, AddrInfoOptions::RelayAndAddresses)
            .await
            .map_err(|e| WorkspaceError::Doc(format!("share doc: {e}")))?;

        publish_ticket(client, session_id, &ticket, &rules).await?;

        // Spawn the cap-listener on a second Client connection so we
        // don't consume the caller's event stream.
        let shutdown_token = CancellationToken::new();
        // Rotation signal channel (Slice 3e): the host cap-listener
        // sends HostEvict here; the rotation task in `run` drains it.
        // Unbounded so a burst of evictions never drops a signal (C4).
        let (rotation_tx, rotation_rx) = mpsc::unbounded_channel::<RotationSignal>();
        let (host_ctx, rotation_distribute_ctx) = if let Some(socket) = daemon_socket {
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
            let upgrade_client = Arc::new(upgrade_client);
            // The current upgrade secret is shared, mutable state (C1):
            // on rotation the host re-mints it and the cap-listener must
            // start delivering the NEW secret to any peer promoted
            // afterwards. Both the cap-listener's `HostUpgradeCtx` and the
            // rotation task's `RotationDistributeCtx` hold the same cell.
            let upgrade_secret = Arc::new(std::sync::Mutex::new(namespace_secret));
            // Seed the shared rotated-ticket cell with the CURRENT write
            // ticket + the epoch recovered from disk (C2), not a hard-coded
            // 0. On a fresh host these coincide (genesis ticket, epoch 0);
            // on a returning host that had rotated, `write_ticket` is the
            // rotated namespace's ticket and `epoch` is its rotation count.
            // Hard-coding 0 here re-opened the gap Part 2 closes: a
            // returning RW member (offline across the rotation, so at a
            // lower epoch) would receive `publish_rotate(epoch = 0)`, which
            // its monotonic-epoch guard drops as stale — leaving it writing
            // to the abandoned namespace with no live re-delivery to swap
            // it onto the rotated one. The cell is overwritten on each
            // subsequent rotation by `refresh_distribution_state`.
            let current_write_ticket =
                Arc::new(std::sync::Mutex::new((write_ticket.to_string(), epoch)));
            let host_ctx = Some(HostUpgradeCtx {
                client: Arc::clone(&upgrade_client),
                session: session_id,
                namespace_secret: Arc::clone(&upgrade_secret),
                current_write_ticket: Arc::clone(&current_write_ticket),
                redelivered_epoch: Arc::new(
                    std::sync::Mutex::new(std::collections::HashMap::new()),
                ),
                rotation_tx: rotation_tx.clone(),
            });
            // Rotation distribution reuses the same upgrade client +
            // session (the host distributes the rotated ticket to
            // survivors over the same direct-stream path) and shares the
            // upgrade-secret + rotated-ticket cells + rules so it can
            // refresh the promotion secret, the published read envelope,
            // and the returning-member re-delivery ticket on rotate.
            let distribute = Some(RotationDistributeCtx {
                client: upgrade_client,
                session: session_id,
                upgrade_secret,
                current_write_ticket,
                rules: rules.clone(),
            });
            (host_ctx, distribute)
        } else {
            (None, None)
        };
        let cap_listener = spawn_cap_listener_from_socket(
            daemon_socket,
            session_id,
            Arc::clone(&peer_map),
            shutdown_token.child_token(),
            host_ctx,
            None,
            tx.clone(),
        )
        .await?;

        // All fallible work is done — pull the node out of the
        // rollback guard so it lives in the constructed Workspace.
        let node = std::mem::take(rb)
            .disarm()
            .expect("rb.node populated above");
        let blobs = node.blobs.clone();
        // Doc-scoped token: a child of shutdown_token. The cap-listener
        // above is on a *separate* shutdown_token child so it survives
        // a doc_token reset (namespace rotation re-import).
        let doc_token = std::sync::Mutex::new(shutdown_token.child_token());
        Ok((
            Self {
                root,
                doc: std::sync::Mutex::new(doc),
                author,
                blobs,
                echo_guard,
                events: tx,
                shutdown_token,
                doc_token,
                write_halted: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                node: tokio::sync::Mutex::new(Some(node)),
                rules,
                compiled_rules,
                exclude,
                state_dir,
                session_id,
                join_ticket: Some(join_ticket),
                rotation_distribute_ctx,
                // Seed from disk so a returning host that had rotated
                // resumes at its last epoch instead of resetting to 0 (C2).
                namespace_epoch: std::sync::atomic::AtomicU64::new(epoch),
                // Seed the rotated-revoke high-water mark from disk (C3)
                // so replayed historical revokes don't re-rotate.
                rotated_revoke_seq: std::sync::atomic::AtomicU64::new(rotated_revoke_seq),
                rotation_rx: std::sync::Mutex::new(Some(rotation_rx)),
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
    /// 2. Issues `Subscribe { since: None }` so the daemon injects
    ///    the persisted `workspace.ticket` envelope into the replay —
    ///    a joiner that attaches after the unicast delivery still
    ///    picks it up.
    /// 3. Drains events until the ticket arrives, bounded by
    ///    [`WorkspaceConfig::join_ticket_timeout`] (default `None` =
    ///    wait forever).
    /// 4. Imports the ticket into the joiner's local doc, runs
    ///    `bulk_export` to seed `root` with whatever's already in
    ///    the doc, and returns. Call [`Self::run`] to start the
    ///    watcher + applier.
    ///
    /// **Side effect:** consumes the client's [`Client::take_events`]
    /// channel. Callers that need to observe other session events
    /// from the same connection should open a second [`Client`].
    ///
    /// # Errors
    ///
    /// Propagates every failure of [`Self::join_with`], which it calls
    /// with [`WorkspaceConfig::default`].
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
    ///
    /// # Errors
    ///
    /// - [`WorkspaceError::Policy`] if `policy` rejects the existing
    ///   `root` / `state_dir`; checked before any on-disk or IPC state.
    /// - [`WorkspaceError::Io`] / [`WorkspaceError::Iroh`] if the state
    ///   directory or iroh node can't be created.
    /// - [`WorkspaceError::Iroh`] if the daemon's event stream closes
    ///   (or [`WorkspaceConfig::join_ticket_timeout`] elapses, when
    ///   set) before the `workspace.ticket` envelope arrives, or if
    ///   [`Client::take_events`] was already consumed.
    /// - [`WorkspaceError::Doc`] for iroh-docs failures importing the
    ///   ticket or bulk-exporting the doc into `root`.
    ///
    /// On any failure the rollback guard shuts down the iroh node and
    /// forgets the joiner-side attachment; session membership (a
    /// precondition) is left untouched.
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

        // Compile the exclude globs up front — a malformed list is a
        // configuration error, rejected before any state is created.
        // Unlike `rules` (host-authoritative, ticket-borne), the
        // exclude list is honoured on the joiner: it is local node
        // hygiene, not workspace policy.
        let exclude = ExcludeRules::compile(config.exclude.as_deref())?;

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
            exclude,
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
        exclude: ExcludeRules,
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

        // Create the event channel before the node spawns: the node's
        // transport-layer filters (PeerFilter / DocsGate) emit
        // `RevokedPeerBlocked` events on it.
        let (tx, rx) = mpsc::channel(EVENT_BUFFER);
        let node = WorkspaceNode::spawn(
            &state_dir,
            endpoint_setup,
            Arc::clone(&peer_map),
            tx.clone(),
        )
        .await?;
        rb.node = Some(node);
        let node = rb.node.as_ref().expect("just stored");
        // Joiners don't persist a per-workspace `doc-id` — they
        // import the host's namespace from the ticket each time.
        // Same-seed author (bound to the endpoint key in
        // `WorkspaceNode::spawn`) stamps our own writes once live sync
        // starts, so `AuthorId == endpoint_id`.
        let author = node.author;

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

        let echo_guard = EchoGuard::new();

        bulk_export(
            &root,
            &doc,
            &node.blobs,
            &compiled_rules,
            &exclude,
            &echo_guard,
            &tx,
        )
        .await?;

        // Spawn the cap-listener on its own dedicated connection so it
        // owns the `Client` it drains — required for M3 recovery, which
        // both reconnects on EOF and re-`Subscribe`s in-band on a gap.
        // Prefer an explicitly-configured `daemon_socket`; otherwise
        // dial the same socket the caller's client is on
        // (`client.socket_path()`). The caller's own event stream
        // (consumed by `wait_for_ticket` above) is dropped — we never
        // reuse it for the listener.
        let shutdown_token = CancellationToken::new();
        let write_halted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        // Rotation signal channel (Slice 3e): the survivor cap-listener
        // sends SurvivorRotate here; the rotation task in `run` drains it.
        // Unbounded so a burst of deliveries never drops a signal (C4).
        let (rotation_tx, rotation_rx) = mpsc::unbounded_channel::<RotationSignal>();
        let joiner_ctx = Some(JoinerUpgradeCtx {
            my_peer_id: client.daemon_peer_id(),
            docs: node.docs.clone(),
            write_halted: Arc::clone(&write_halted),
            rotation_tx: rotation_tx.clone(),
        });
        let listener_socket = join_daemon_socket.unwrap_or_else(|| client.socket_path());
        let cap_listener = spawn_cap_listener_from_socket(
            Some(listener_socket),
            session,
            Arc::clone(&peer_map),
            shutdown_token.child_token(),
            None,
            joiner_ctx,
            tx.clone(),
        )
        .await?;

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
        // Doc-scoped token: a child of shutdown_token, separate from the
        // cap-listener's token so a rotation re-import can reset it
        // without killing the cap-listener.
        let doc_token = std::sync::Mutex::new(shutdown_token.child_token());
        Ok((
            Self {
                root,
                doc: std::sync::Mutex::new(doc),
                author,
                blobs,
                echo_guard,
                events: tx,
                shutdown_token,
                doc_token,
                write_halted,
                node: tokio::sync::Mutex::new(Some(node)),
                rules,
                compiled_rules,
                exclude,
                state_dir,
                session_id: session,
                join_ticket: None,
                // Joiners never distribute a rotation — they only receive.
                rotation_distribute_ctx: None,
                namespace_epoch: std::sync::atomic::AtomicU64::new(0),
                // Joiners never act as the rotating host, so the
                // rotated-revoke mark is unused on this side (C3).
                rotated_revoke_seq: std::sync::atomic::AtomicU64::new(0),
                rotation_rx: std::sync::Mutex::new(Some(rotation_rx)),
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

    /// The current namespace-rotation epoch (Slice 3). `0` at genesis;
    /// bumped on each write-revocation rotation (host) or on reimport
    /// onto a rotated namespace (survivor). Lets a consumer surface
    /// "we are on namespace epoch N" so a rotation is visible — e.g. a
    /// chat-harness status line confirming an Evict took effect.
    #[must_use]
    pub fn namespace_epoch(&self) -> u64 {
        self.namespace_epoch
            .load(std::sync::atomic::Ordering::Relaxed)
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

    /// A clone of the current `iroh-docs` document handle.
    ///
    /// Exposed for diagnostics and tests — the watcher / applier
    /// drive it internally. Apps shouldn't normally need to write
    /// to it directly; use the filesystem path instead. `Doc` is a
    /// cheap `Clone` handle; callers get a snapshot of the *current*
    /// namespace (rotation may swap the underlying handle).
    ///
    /// # Panics
    ///
    /// Panics if the internal `doc` mutex is poisoned — i.e. another
    /// thread panicked while holding it.
    #[must_use]
    pub fn doc(&self) -> Doc {
        self.doc.lock().expect("doc mutex").clone()
    }

    /// Borrow the workspace's [`PathRules`] in wire form.
    ///
    /// On the host: the configured (or default-permissive) rules.
    /// On the joiner: the rules decoded from the host's
    /// `workspace.ticket` envelope (the host's, not whatever the
    /// joiner configured). Surfaced for tests and consumers that
    /// want to inspect the configured rules; the hot path
    /// (watcher / applier / scan / bulk-export) uses
    /// `Self::compiled_rules` internally for matcher-once
    /// performance.
    #[must_use]
    pub const fn rules(&self) -> &PathRules {
        &self.rules
    }

    /// Whether this node has been cooperatively demoted (RW → Read) and
    /// should stop publishing local writes. The watcher consults this
    /// before every publish/delete; flipped by a `DOWNGRADE_ACTION`
    /// notification. A *voluntary* write-stop — cooperative threat
    /// model only.
    #[must_use]
    pub(crate) fn is_write_halted(&self) -> bool {
        self.write_halted.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// A clone of the current doc-scoped cancellation token. The
    /// watcher and applier select on this (rather than
    /// [`Self::shutdown_token`]) so namespace rotation can stop and
    /// respawn them without taking down the cap-listener. Cloning a
    /// `CancellationToken` shares the same cancellation state.
    #[must_use]
    pub(crate) fn doc_token(&self) -> CancellationToken {
        self.doc_token.lock().expect("doc_token mutex").clone()
    }

    /// Snapshot the current doc and split its latest-per-key entries into
    /// the carry-forward survivor set and per-reason drop counts (D2).
    ///
    /// Tombstones are **included** in the survivors (len 0) so the
    /// re-author loop can carry deletes forward (D3); the author-filter
    /// classifies each non-host entry via [`PeerMap::classify_author`].
    /// An [`AuthorDisposition::Unresolvable`] author is dropped
    /// fail-closed but logged loudly: it is a survivor whose
    /// `NODE_ID_ACTION` mapping hadn't reached the host's `peer_map` when
    /// rotation fired, so dropping it silently loses a trusted peer's data.
    ///
    /// `node` is the caller's already-locked node guard (the lock is held
    /// across the whole rotation for a consistent snapshot).
    async fn collect_rotation_survivors(
        &self,
        node: &WorkspaceNode,
    ) -> Result<(Vec<(Vec<u8>, iroh_blobs::Hash, u64)>, RotationDropCounts), WorkspaceError> {
        use crate::peer_map::AuthorDisposition;

        // Latest entry per key, tombstones included (a delete must carry
        // forward as a delete — D3).
        let old_doc = self.doc();
        let stream = old_doc
            .get_many(Query::single_latest_per_key().include_empty())
            .await
            .map_err(|e| WorkspaceError::Doc(format!("rotate: snapshot get_many: {e}")))?;
        tokio::pin!(stream);

        let mut survivors: Vec<(Vec<u8>, iroh_blobs::Hash, u64)> = Vec::new();
        let mut drops = RotationDropCounts::default();
        while let Some(res) = stream.next().await {
            let entry =
                res.map_err(|e| WorkspaceError::Doc(format!("rotate: snapshot entry: {e}")))?;
            let author_endpoint = iroh::EndpointId::from_bytes(entry.author().as_bytes())
                .map_err(|e| WorkspaceError::Doc(format!("rotate: author not an endpoint: {e}")))?;
            // The host's own writes always survive (host is RW by
            // construction); other authors are classified via the binding.
            let keep = author_endpoint == node.endpoint_id
                || match node.peer_map.classify_author(author_endpoint) {
                    AuthorDisposition::Rw => true,
                    AuthorDisposition::Revoked => {
                        drops.revoked += 1;
                        false
                    }
                    AuthorDisposition::NotRw => {
                        drops.not_rw += 1;
                        false
                    }
                    AuthorDisposition::Unresolvable => {
                        drops.unresolvable += 1;
                        warn!(
                            target: "artel_fs::workspace",
                            %author_endpoint,
                            key = %String::from_utf8_lossy(entry.key()),
                            "rotate: dropping entry with unresolvable author (mapping not yet \
                             known); if this is a survivor, its NODE_ID_ACTION race lost data",
                        );
                        false
                    }
                };
            if keep {
                survivors.push((
                    entry.key().to_vec(),
                    entry.content_hash(),
                    entry.content_len(),
                ));
            }
        }
        Ok((survivors, drops))
    }

    /// Rotate the workspace namespace (Slice 3 — the cryptographic
    /// write cut-off behind Evict).
    ///
    /// Mints a **fresh** `NamespaceSecret`, re-publishes the current
    /// doc's latest-per-key snapshot into it **under the host's own
    /// author**, keeping only entries whose author is a still-RW peer
    /// (the revoked author's entries are dropped at the snapshot). File
    /// bytes never move — only `path → content-hash` pairs are copied
    /// (`set_hash`), and survivors already hold the blobs. The new
    /// namespace is persisted as the current namespace; the genesis
    /// (`doc-id`) and therefore the `SessionId` are untouched.
    ///
    /// Returns the new secret (for host→survivor distribution) and the
    /// bumped epoch. The caller is responsible for quiescing writes
    /// before calling (the freeze-drain barrier) and for distributing
    /// the secret afterward.
    ///
    /// Host-only: requires the workspace node and its `peer_map`
    /// (cap projection). Entries authored by a peer the `peer_map`
    /// can't resolve to a current RW cap are dropped (fail-closed).
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn rotate_namespace(
        &self,
        prev_epoch: u64,
    ) -> Result<RotationOutcome, WorkspaceError> {
        // Hold the node lock across the whole rotation: the snapshot
        // read, the new-namespace create + re-author, and the share
        // must see a consistent node (no concurrent teardown).
        let guard = self.node.lock().await;
        let node = guard
            .as_ref()
            .ok_or_else(|| WorkspaceError::Doc("rotate: node already torn down".into()))?;

        // Snapshot the current doc and classify each entry's author
        // (D2: split drops by reason; an unresolvable-author drop is a
        // possible survivor-data-loss race and is surfaced loudly).
        let (survivors, drops) = self.collect_rotation_survivors(node).await?;
        let dropped = drops.total();
        if drops.unresolvable > 0 {
            emit_event(
                &self.events,
                WorkspaceEvent::Error(format!(
                    "rotation dropped {} entr{} with an unresolvable author \
                     (possible survivor data loss from a NODE_ID mapping race)",
                    drops.unresolvable,
                    if drops.unresolvable == 1 { "y" } else { "ies" },
                )),
            );
        }

        // Mint the fresh namespace and re-author the snapshot into it
        // under the host's own author. `set_hash` re-points keys at
        // hashes that already exist locally — no blob bytes move.
        let new_doc = node
            .docs
            .create()
            .await
            .map_err(|e| WorkspaceError::Doc(format!("rotate: create namespace: {e}")))?;
        for (key, hash, len) in &survivors {
            if *len == 0 {
                // Tombstone (D3): carry the delete forward by re-authoring
                // an empty entry under the host. A `set_hash` of a 0-len
                // entry would be rejected, but `del` writes a syncable
                // deletion marker — so a survivor that was OFFLINE across
                // the rotation reimports the new namespace and still sees
                // the delete, rather than resurrecting its stale local
                // copy (and republishing it via its respawned watcher).
                new_doc
                    .del(self.author, key.clone())
                    .await
                    .map_err(|e| WorkspaceError::Doc(format!("rotate: carry-forward del: {e}")))?;
                continue;
            }
            new_doc
                .set_hash(self.author, key.clone(), *hash, *len)
                .await
                .map_err(|e| WorkspaceError::Doc(format!("rotate: set_hash: {e}")))?;
        }

        // Build the Write ticket: capability + the host's relay/direct
        // addresses. Survivors need the addresses to sync a namespace
        // they've never seen (RelayAndAddresses seeds their addr-book on
        // the first dial — same rationale as the host's read ticket in
        // `host_with_inner`). The bare secret is extracted separately for
        // the host's own reimport, which needs no addresses.
        let write_ticket = new_doc
            .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
            .await
            .map_err(|e| WorkspaceError::Doc(format!("rotate: share new doc: {e}")))?;
        let new_secret = match write_ticket.capability {
            iroh_docs::Capability::Write(ref secret) => secret.to_bytes(),
            iroh_docs::Capability::Read(_) => {
                unreachable!("freshly created namespace always has Write capability")
            }
        };
        let write_ticket = write_ticket.to_string();

        // Build the Read ticket too (capability + addresses). The host
        // re-publishes this as the new `workspace.ticket` envelope (C1)
        // so a peer that joins AFTER the rotation imports the rotated
        // namespace, not the abandoned genesis. Mirrors the host's
        // construction-time read ticket in `host_with_inner`.
        let read_ticket = new_doc
            .share(ShareMode::Read, AddrInfoOptions::RelayAndAddresses)
            .await
            .map_err(|e| WorkspaceError::Doc(format!("rotate: share new doc (read): {e}")))?
            .to_string();
        let new_namespace = new_doc.id();
        let new_epoch = prev_epoch + 1;

        // Persist the new current namespace (genesis/doc-id untouched,
        // so SessionId is stable). store-before-anyone-uses-it.
        let current_ns_path = self.state_dir.join(CURRENT_NAMESPACE_FILE);
        crate::keystore::write_atomic(&current_ns_path, &new_namespace.to_bytes(), None).map_err(
            |e| {
                WorkspaceError::Doc(format!(
                    "rotate: persist current-namespace at {}: {e}",
                    current_ns_path.display(),
                ))
            },
        )?;
        // Persist the bumped epoch alongside the namespace so a returning
        // host recovers it instead of resetting to 0 (C2). Order:
        // namespace first, then epoch — a crash between the two leaves a
        // recorded namespace at a stale (lower) epoch, which is the
        // recoverable direction (a survivor re-delivery with a higher
        // epoch still wins); the reverse would advance the epoch past a
        // namespace the host never durably switched to.
        persist_u64(
            &self.state_dir.join(NAMESPACE_EPOCH_FILE),
            new_epoch,
            "namespace-epoch",
        )?;

        debug!(
            target: "artel_fs::workspace",
            %new_namespace,
            new_epoch,
            survivors = survivors.len(),
            dropped,
            dropped_revoked = drops.revoked,
            dropped_not_rw = drops.not_rw,
            dropped_unresolvable = drops.unresolvable,
            "rotate_namespace: minted new namespace",
        );

        Ok(RotationOutcome {
            new_secret,
            write_ticket,
            read_ticket,
            new_namespace,
            new_epoch,
            survivor_entries: survivors.len(),
            dropped_entries: dropped,
        })
    }

    /// Re-import the workspace onto a rotated namespace (Slice 3d — the
    /// survivor side of rotation).
    ///
    /// Imports `new_secret` as a Write capability, persists it as the
    /// current namespace, swaps the live doc handle, then **resets the
    /// doc-scoped token** — cancelling the watcher/applier bound to the
    /// old namespace — and respawns them against the new one. The
    /// cap-listener (on a *separate* `shutdown_token` child) is left
    /// running, so the signal channel that drove this re-import stays
    /// alive (Slice 3a token split).
    ///
    /// Blobs are namespace-agnostic and already local, so nothing
    /// re-downloads; the new doc reconciles only the `path → hash`
    /// entry set. Idempotent-ish: importing a secret already present is
    /// a no-op at the docs layer.
    ///
    /// Takes `Arc<Self>` because it respawns the background tasks, which
    /// capture the workspace.
    ///
    /// `source` selects how the namespace is obtained:
    /// - [`ReimportSource::HostLocal`] — the host owns the namespace
    ///   locally (it just minted it); import the bare secret, no peer
    ///   addresses needed.
    /// - [`ReimportSource::SurvivorTicket`] — a survivor receives the
    ///   host's Write `DocTicket`; `start_sync(ticket.nodes)` seeds the
    ///   addr-book so the brand-new namespace can sync from the host.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn reimport_namespace(
        self: &Arc<Self>,
        source: ReimportSource,
    ) -> Result<(), WorkspaceError> {
        // The host re-importing a namespace it just minted owns the
        // authoritative on-disk state and must run a catch-up scan after
        // the respawn (see the scan call below). A survivor must not.
        let is_host_local = matches!(source, ReimportSource::HostLocal { .. });

        // Resolve (capability, sync-nodes, expected namespace id, epoch)
        // from the source.
        let (capability, sync_nodes, new_namespace, new_epoch_hint) = match source {
            ReimportSource::HostLocal {
                new_secret,
                new_namespace,
                new_epoch_hint,
            } => {
                let secret = iroh_docs::NamespaceSecret::from_bytes(&new_secret);
                (
                    iroh_docs::Capability::Write(secret),
                    Vec::new(),
                    new_namespace,
                    new_epoch_hint,
                )
            }
            ReimportSource::SurvivorTicket {
                doc_ticket,
                new_epoch_hint,
            } => {
                let ticket: DocTicket = doc_ticket.parse().map_err(|e| {
                    WorkspaceError::Doc(format!("reimport: parse rotation ticket: {e}"))
                })?;
                let ns = ticket.capability.id();
                (ticket.capability, ticket.nodes, ns, new_epoch_hint)
            }
        };

        // Import the new namespace and open its doc handle.
        let new_doc = {
            let guard = self.node.lock().await;
            let node = guard
                .as_ref()
                .ok_or_else(|| WorkspaceError::Doc("reimport: node torn down".into()))?;
            node.docs
                .import_namespace(capability)
                .await
                .map_err(|e| WorkspaceError::Doc(format!("reimport import_namespace: {e}")))?;
            node.docs
                .open(new_namespace)
                .await
                .map_err(|e| WorkspaceError::Doc(format!("reimport open: {e}")))?
                .ok_or_else(|| {
                    WorkspaceError::Doc("reimport: namespace absent after import".into())
                })?
        };
        // Seed the addr-book from the ticket's nodes (empty for the
        // host's own reimport) so a survivor can dial the host for the
        // brand-new namespace on the first try.
        new_doc
            .start_sync(sync_nodes)
            .await
            .map_err(|e| WorkspaceError::Doc(format!("reimport start_sync: {e}")))?;

        // Persist the new current namespace (genesis/doc-id untouched).
        let current_ns_path = self.state_dir.join(CURRENT_NAMESPACE_FILE);
        crate::keystore::write_atomic(&current_ns_path, &new_namespace.to_bytes(), None).map_err(
            |e| {
                WorkspaceError::Doc(format!(
                    "reimport: persist current-namespace at {}: {e}",
                    current_ns_path.display(),
                ))
            },
        )?;
        // Persist the epoch alongside the namespace (C2) so a returning
        // host/survivor recovers it rather than resetting to 0. Same
        // namespace-then-epoch ordering as `rotate_namespace`.
        persist_u64(
            &self.state_dir.join(NAMESPACE_EPOCH_FILE),
            new_epoch_hint,
            "namespace-epoch",
        )?;

        // Swap the live doc handle, then reset the doc token to tear
        // down the old watcher/applier. Order: install the new doc
        // BEFORE cancelling, so respawned tasks (which clone via
        // `doc()`) pick up the new namespace.
        {
            let mut doc_slot = self.doc.lock().expect("doc mutex");
            *doc_slot = new_doc;
        }
        {
            let mut tok = self.doc_token.lock().expect("doc_token mutex");
            tok.cancel(); // stop the old watcher/applier
            *tok = self.shutdown_token.child_token();
        }

        // The last-published hashes describe what we published into the
        // OLD namespace; against the rotated doc they are stale. A path
        // published during the rotation lag (after the host's snapshot,
        // so not carried forward) would otherwise have every identical-
        // bytes re-write swallowed as an echo forever — the respawned
        // watcher's events hash equal to the stale entry. Clearing is
        // safe on both the host and survivor paths: it publishes
        // nothing by itself, so it cannot resurrect a host-tombstoned
        // path (a real filesystem event is still required, and
        // `peer_deleted` markers survive the clear).
        self.echo_guard.forget_all_published().await;

        // Respawn watcher + applier against the new namespace. They
        // select on the fresh doc token (via `doc_token()`) and clone
        // the swapped doc. We respawn only the doc tasks (not full
        // `run`) so we don't re-enter the rotation-task spawn — the
        // rotation task is already running and survives the doc_token
        // reset (it lives on `shutdown_token`). `spawn_doc_tasks`
        // awaits the watcher's `ready` signal, so the OS-level watch is
        // attached by the time it returns.
        drop(Arc::clone(self).spawn_doc_tasks().await);

        // Catch-up scan (host only). Between the old watcher exiting on
        // the doc-token cancel above and the new watcher's recursive
        // watch attaching, there is a window in which a local write to
        // the host's tree produces no filesystem event for *either*
        // watcher — `notify` does no initial scan on attach, so a file
        // created in that gap is never published to the rotated
        // namespace and is lost (the auto-rotation write cut-off tests
        // hit this under load: the host's post-rotation write never
        // landed). Mirror `host_with`'s construction-time
        // `scan_and_publish_existing` to republish the current tree into
        // the new namespace; re-publishing a carried-forward survivor is
        // a no-op at the doc layer (same host author, same content hash),
        // so the scan only adds entries the watcher missed. Host-only:
        // the host's disk is authoritative for its own writes, whereas a
        // survivor scanning here could resurrect a path the host
        // tombstoned in the rotation but whose delete the survivor's
        // applier hasn't yet laid down.
        if is_host_local {
            debug!(
                target: "artel_fs::workspace",
                %new_namespace,
                "reimport_namespace: catch-up scan of host tree into rotated namespace",
            );
            let scan_doc = self.doc();
            scan_and_publish_existing(
                &self.root,
                &scan_doc,
                self.author,
                &self.compiled_rules,
                &self.exclude,
                &self.echo_guard,
                &self.events,
            )
            .await?;
        }

        // Reflect the new epoch locally so a future stale rotation is
        // ignored. The host sets this before distributing; a survivor
        // sets it here on reimport.
        self.namespace_epoch
            .store(new_epoch_hint, std::sync::atomic::Ordering::Relaxed);

        debug!(
            target: "artel_fs::workspace",
            %new_namespace,
            "reimport_namespace: swapped onto rotated namespace",
        );
        Ok(())
    }

    /// Handle one [`RotationSignal`] (Slice 3e). Host: rotate → reimport
    /// locally → refresh distribution state → distribute to survivors
    /// (host-first ordering, D1, so the host's write-loss window is just
    /// the local doc swap and it is syncing the new namespace before any
    /// survivor dials). Survivor: reimport onto the delivered namespace if
    /// its epoch is newer than what we hold. Errors are logged + surfaced as a
    /// [`WorkspaceEvent::Error`]; rotation is best-effort at this layer
    /// (a failed distribution leaves a survivor on the old namespace,
    /// recovered on its next epoch-bearing delivery).
    async fn handle_rotation_signal(self: &Arc<Self>, signal: RotationSignal) {
        match signal {
            RotationSignal::HostEvict {
                revoked_peer,
                revoke_seq,
            } => self.handle_host_evict(revoked_peer, revoke_seq).await,
            RotationSignal::SurvivorRotate {
                namespace_epoch,
                doc_ticket,
            } => {
                let prev = self
                    .namespace_epoch
                    .load(std::sync::atomic::Ordering::Relaxed);
                if namespace_epoch <= prev {
                    debug!(
                        namespace_epoch,
                        prev, "rotation: ignoring stale/duplicate survivor rotation",
                    );
                    return;
                }
                if let Err(e) = self
                    .reimport_namespace(ReimportSource::SurvivorTicket {
                        doc_ticket,
                        new_epoch_hint: namespace_epoch,
                    })
                    .await
                {
                    warn!(?e, "rotation: survivor reimport failed");
                    emit_event(
                        &self.events,
                        WorkspaceEvent::Error(format!("survivor reimport failed: {e}")),
                    );
                }
            }
        }
    }

    /// Host side of a [`RotationSignal::HostEvict`]: rotate the namespace,
    /// reimport the host locally (D1, host-first), refresh durable
    /// distribution state (C1), then distribute the new ticket to
    /// survivors. Skips replayed revokes via the rotated-revoke high-water
    /// mark (C3).
    async fn handle_host_evict(self: &Arc<Self>, revoked_peer: PeerId, revoke_seq: Seq) {
        // Idempotency guard (C3): skip a Revoke we've already rotated for.
        // On a host restart the cap-listener re-subscribes `since: None`,
        // so every historical Revoke replays; without this, each past
        // eviction would re-rotate (churn the namespace, re-distribute). A
        // genuinely new live Revoke carries a strictly higher seq (the
        // daemon assigns monotonic seqs), so it still fires.
        let watermark = self
            .rotated_revoke_seq
            .load(std::sync::atomic::Ordering::Relaxed);
        if revoke_seq.get() <= watermark {
            debug!(
                revoke_seq = revoke_seq.get(),
                watermark,
                ?revoked_peer,
                "rotation: skipping already-rotated (replayed) revoke",
            );
            return;
        }
        let prev = self
            .namespace_epoch
            .load(std::sync::atomic::Ordering::Relaxed);
        let outcome = match self.rotate_namespace(prev).await {
            Ok(o) => o,
            Err(e) => {
                warn!(?e, ?revoked_peer, "rotation: rotate_namespace failed");
                emit_event(
                    &self.events,
                    WorkspaceEvent::Error(format!("rotation failed: {e}")),
                );
                return;
            }
        };
        // Set the epoch before distributing so a concurrent signal sees
        // the bump.
        self.namespace_epoch
            .store(outcome.new_epoch, std::sync::atomic::Ordering::Relaxed);

        // Advance + persist the rotated-revoke high-water mark (C3): the
        // rotation succeeded, so this Revoke (and every earlier one) must
        // never re-rotate on a future restart. Persist before
        // distribution — distribution failure is recoverable (survivors
        // re-sync on their next epoch-bearing delivery), but a re-rotation
        // of this same revoke is exactly the churn we're preventing. A
        // persist failure only logs: the in-memory mark still advances, so
        // this process won't re-rotate; worst case a crash before the next
        // durable write replays one extra rotation, which is safe
        // (idempotent at the epoch gate).
        self.rotated_revoke_seq
            .store(revoke_seq.get(), std::sync::atomic::Ordering::Relaxed);
        if let Err(e) = persist_u64(
            &self.state_dir.join(ROTATED_REVOKE_SEQ_FILE),
            revoke_seq.get(),
            "rotated-revoke-seq",
        ) {
            warn!(?e, "rotation: persist rotated-revoke-seq failed");
        }

        // Reimport the host onto the namespace it just minted, BEFORE
        // distributing to survivors (D1). Two reasons to go host-first:
        //   1. Write-loss window: until the host swaps its live doc it
        //      keeps writing to the *old* namespace. If distribution ran
        //      first, that window would span the whole serial survivor
        //      loop (each delivery bounded by the daemon's ~10s
        //      DELIVER_FRAME_TIMEOUT, so seconds-to-minutes with an
        //      unreachable survivor) instead of just the local swap.
        //   2. Sync readiness: `rotate_namespace` mints the new namespace
        //      but only the host's reimport calls `start_sync` on it. A
        //      survivor told to dial before the host is syncing would just
        //      retry; reimporting first means the host is ready when the
        //      first survivor dials.
        if let Err(e) = self
            .reimport_namespace(ReimportSource::HostLocal {
                new_secret: outcome.new_secret,
                new_namespace: outcome.new_namespace,
                new_epoch_hint: outcome.new_epoch,
            })
            .await
        {
            warn!(?e, "rotation: host reimport failed");
            emit_event(
                &self.events,
                WorkspaceEvent::Error(format!("host reimport failed: {e}")),
            );
        }

        // Refresh the host's durable distribution state (C1) so peers that
        // join or are promoted AFTER this rotation land on the rotated
        // namespace, not the abandoned genesis.
        self.refresh_distribution_state(&outcome).await;

        // Distribute the new ticket to every surviving RW peer (the
        // revoked peer is already gone from the cap set, so
        // `rw_peers_except_host` excludes it).
        let survivors = {
            let guard = self.node.lock().await;
            guard
                .as_ref()
                .map_or_else(Vec::new, |node| node.peer_map.rw_peers_except_host())
        };
        if let Some(ctx) = &self.rotation_distribute_ctx {
            for peer in survivors {
                if let Err(e) = publish_rotate(
                    &ctx.client,
                    ctx.session,
                    peer,
                    outcome.new_epoch,
                    outcome.write_ticket.clone(),
                )
                .await
                {
                    warn!(?e, ?peer, "rotation: deliver to survivor failed");
                }
            }
        }
    }

    /// Refresh the host's durable distribution state after a rotation
    /// (C1): overwrite the shared upgrade-secret cell (so a later RW grant
    /// delivers the rotated secret) and re-publish the `workspace.ticket`
    /// envelope at the new namespace + epoch (so a later joiner imports
    /// the rotated namespace; the daemon re-delivers it to current members
    /// — inert for them). Both are best-effort: a failure leaves the
    /// steady-state cut-off intact (survivors already followed) but logs +
    /// surfaces a [`WorkspaceEvent::Error`] so the gap is visible.
    async fn refresh_distribution_state(self: &Arc<Self>, outcome: &RotationOutcome) {
        let Some(ctx) = &self.rotation_distribute_ctx else {
            return;
        };
        *ctx.upgrade_secret.lock().expect("upgrade_secret mutex") = outcome.new_secret;
        // Refresh the returning-member re-delivery cell with the rotated
        // Write ticket + epoch (read-plane analogue of the secret above).
        *ctx.current_write_ticket
            .lock()
            .expect("current_write_ticket mutex") =
            (outcome.write_ticket.clone(), outcome.new_epoch);
        let envelope = WorkspaceTicketEnvelope::at_epoch(
            outcome.read_ticket.clone(),
            ctx.rules.clone(),
            outcome.new_epoch,
        );
        let envelope_bytes = match ticket::encode(&envelope) {
            Ok(b) => b,
            Err(e) => {
                warn!(?e, "rotation: encode rotated envelope failed");
                emit_event(
                    &self.events,
                    WorkspaceEvent::Error(format!("rotation: encode rotated envelope failed: {e}")),
                );
                return;
            }
        };
        if let Err(e) = ctx
            .client
            .request(Request::PublishWorkspaceTicket {
                session: ctx.session,
                envelope_bytes,
            })
            .await
        {
            warn!(?e, "rotation: re-publish workspace ticket failed");
            emit_event(
                &self.events,
                WorkspaceEvent::Error(format!("rotation: re-publish ticket failed: {e}")),
            );
        }
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
    ///
    /// # Panics
    ///
    /// Panics if the internal `rotation_rx` mutex is poisoned — i.e.
    /// another thread panicked while holding it.
    #[must_use]
    pub async fn run(self: std::sync::Arc<Self>) -> tokio::task::JoinHandle<()> {
        debug!(target: "artel_fs::workspace", root = %self.root.display(), "run: spawning watcher + applier");
        // Spawn the rotation task ONCE. `run` is re-entrant — reimport
        // calls it again to respawn watcher/applier after a doc swap —
        // so guard on taking `rotation_rx`: the first `run` takes it
        // (Some → drives the task), later `run`s get `None` and skip
        // (the existing task keeps running, on `shutdown_token` so the
        // doc_token reset during reimport doesn't kill it).
        // Take the receiver in a tight scope so the std MutexGuard is
        // dropped before any `.await` below (else `run`'s future is
        // !Send and can't be awaited by the rotation task itself).
        let taken_rotation_rx = self.rotation_rx.lock().expect("rotation_rx mutex").take();
        if let Some(mut rotation_rx) = taken_rotation_rx {
            let rotation_ws = std::sync::Arc::clone(&self);
            let cancel = self.shutdown_token.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        () = cancel.cancelled() => break,
                        signal = rotation_rx.recv() => match signal {
                            Some(s) => rotation_ws.handle_rotation_signal(s).await,
                            None => break,
                        },
                    }
                }
            });
        }

        self.spawn_doc_tasks().await
    }

    /// Spawn the watcher + applier against the **current** doc, awaiting
    /// both halves' readiness. Extracted from [`Self::run`] so namespace
    /// re-import can respawn just the doc tasks without re-entering
    /// `run` (which also owns the once-only rotation task) — that
    /// re-entry would make `reimport_namespace`'s future recursively
    /// reference `run`'s, defeating `Send`.
    async fn spawn_doc_tasks(self: std::sync::Arc<Self>) -> tokio::task::JoinHandle<()> {
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
    /// loops) and consumes the underlying `WorkspaceNode` —
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
    ///
    /// # Errors
    ///
    /// Returns `Err(())` if the node slot is already empty — shutdown
    /// has run or rollback consumed the node — so there is no
    /// `shutdown_failure_flag` left to arm.
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

    /// Drive the applier's tombstone application for `path` directly,
    /// exactly as an incoming `InsertRemote` with `content_len() == 0`
    /// would after passing the rule/filter gates. For tests that need
    /// to deliver a *duplicate* tombstone at a controlled moment —
    /// e.g. the straggler-tombstone race, where a second tombstone for
    /// an already-deleted path arrives after a genuine local
    /// re-creation of the file (macOS `FSEvents` fans one unlink into
    /// two published tombstones; see the 2026-07-02 case study in
    /// `docs/diagnosing-flaky-tests.md`).
    #[cfg(feature = "test-utils")]
    pub async fn test_apply_peer_tombstone(&self, path: std::path::PathBuf) {
        crate::applier::apply_tombstone(&self.echo_guard, &self.events, path).await;
    }

    /// This workspace node's `EndpointId` bytes, for tests asserting the
    /// same-seed author binding (`AuthorId == endpoint_id`). Returns
    /// `None` if the node has already been torn down.
    #[cfg(feature = "test-utils")]
    pub async fn test_endpoint_id_bytes(&self) -> Option<[u8; 32]> {
        self.node
            .lock()
            .await
            .as_ref()
            .map(|node| *node.endpoint_id.as_bytes())
    }

    /// Drive a namespace rotation directly (Slice 3). Returns the new
    /// epoch, the new `NamespaceId` bytes, and (survivor, dropped)
    /// entry counts. For tests that exercise the rotation core without
    /// the full freeze-drain orchestration (the live doc swap is
    /// Slice 3d).
    ///
    /// # Errors
    ///
    /// Returns the stringified error if the underlying
    /// `rotate_namespace` fails — e.g. the node has been torn down or
    /// an iroh-docs operation (snapshot, create namespace, `set_hash`,
    /// share) errors.
    #[cfg(feature = "test-utils")]
    pub async fn test_rotate_namespace(
        &self,
        prev_epoch: u64,
    ) -> Result<(u64, [u8; 32], usize, usize), String> {
        self.rotate_namespace(prev_epoch)
            .await
            .map(|o| {
                (
                    o.new_epoch,
                    o.new_namespace.to_bytes(),
                    o.survivor_entries,
                    o.dropped_entries,
                )
            })
            .map_err(|e| e.to_string())
    }

    /// Rotate, then re-import this same workspace onto the rotated
    /// namespace (host self-rotation). Returns the new `NamespaceId`
    /// bytes. For tests exercising the rotate→reimport round-trip
    /// without the full host→survivor distribution wiring (Slice 3e).
    ///
    /// # Errors
    ///
    /// Returns the stringified error if either half fails: the
    /// `rotate_namespace` step (see [`Self::test_rotate_namespace`]) or
    /// the subsequent `reimport_namespace` (e.g. node torn down,
    /// `import_namespace` / open / `start_sync` errors).
    #[cfg(feature = "test-utils")]
    pub async fn test_rotate_and_reimport(
        self: &Arc<Self>,
        prev_epoch: u64,
    ) -> Result<[u8; 32], String> {
        let outcome = self
            .rotate_namespace(prev_epoch)
            .await
            .map_err(|e| e.to_string())?;
        let new_namespace = outcome.new_namespace;
        self.reimport_namespace(ReimportSource::HostLocal {
            new_secret: outcome.new_secret,
            new_namespace,
            new_epoch_hint: outcome.new_epoch,
        })
        .await
        .map_err(|e| e.to_string())?;
        Ok(new_namespace.to_bytes())
    }

    /// Rotate WITHOUT reimporting, returning the new epoch and the
    /// Write `DocTicket` string a survivor would receive. Leaves the
    /// live doc on the old namespace — for tests that need to publish
    /// into the old doc *after* the rotation snapshot (the survivor
    /// rotation-lag window) before driving the reimport themselves via
    /// [`Self::test_reimport_from_survivor_ticket`].
    ///
    /// # Errors
    ///
    /// Returns the stringified error if `rotate_namespace` fails (node
    /// torn down, iroh-docs snapshot/create/share errors).
    #[cfg(feature = "test-utils")]
    pub async fn test_rotate_for_survivor_reimport(
        &self,
        prev_epoch: u64,
    ) -> Result<(u64, String), String> {
        self.rotate_namespace(prev_epoch)
            .await
            .map(|o| (o.new_epoch, o.write_ticket))
            .map_err(|e| e.to_string())
    }

    /// Drive the **survivor-side** reimport directly: swap onto the
    /// rotated namespace named by `doc_ticket` exactly as a delivered
    /// `RotationSignal::SurvivorRotate` (private) would — including the
    /// deliberate absence of the host-only catch-up scan. Pairs with
    /// [`Self::test_rotate_for_survivor_reimport`].
    ///
    /// # Errors
    ///
    /// Returns the stringified error if `reimport_namespace` fails
    /// (ticket parse, import/open/`start_sync` errors, node torn down).
    #[cfg(feature = "test-utils")]
    pub async fn test_reimport_from_survivor_ticket(
        self: &Arc<Self>,
        doc_ticket: String,
        new_epoch_hint: u64,
    ) -> Result<(), String> {
        self.reimport_namespace(ReimportSource::SurvivorTicket {
            doc_ticket,
            new_epoch_hint,
        })
        .await
        .map_err(|e| e.to_string())
    }

    /// The `NamespaceId` bytes of the workspace's **current** doc.
    #[cfg(feature = "test-utils")]
    pub fn test_current_namespace_bytes(&self) -> [u8; 32] {
        self.doc().id().to_bytes()
    }

    /// The set of file keys (UTF-8 lossy) at the latest entry per key
    /// (excluding tombstones) in the namespace `namespace_id_bytes`,
    /// opened through this node's docs store. For tests asserting which
    /// entries survived a rotation into a freshly minted namespace.
    ///
    /// # Panics
    ///
    /// Panics if the node has been torn down (`node live`), or if the
    /// namespace can't be opened or is absent in this node's docs
    /// store, or if the `get_many` query errors. A test-only helper —
    /// these are bugs in the test setup, not recoverable conditions.
    #[cfg(feature = "test-utils")]
    #[allow(clippy::significant_drop_tightening)]
    pub async fn test_namespace_keys(&self, namespace_id_bytes: [u8; 32]) -> Vec<String> {
        let guard = self.node.lock().await;
        let node = guard.as_ref().expect("node live");
        let id = NamespaceId::from(&namespace_id_bytes);
        let doc = node
            .docs
            .open(id)
            .await
            .expect("open namespace")
            .expect("namespace present");
        let stream = doc
            .get_many(Query::single_latest_per_key())
            .await
            .expect("get_many");
        tokio::pin!(stream);
        let mut keys = Vec::new();
        while let Some(res) = stream.next().await {
            if let Ok(entry) = res
                && entry.content_len() > 0
            {
                keys.push(String::from_utf8_lossy(entry.key()).into_owned());
            }
        }
        keys
    }

    /// The set of keys whose **latest** entry in `namespace_id_bytes` is a
    /// tombstone (zero-length deletion marker). For tests asserting that a
    /// delete was carried forward into a rotated namespace (D3) rather
    /// than silently dropped (which would let an offline survivor
    /// resurrect the deleted path on reimport).
    ///
    /// # Panics
    ///
    /// Panics if the node has been torn down (`node live`), or if the
    /// namespace can't be opened or is absent in this node's docs
    /// store, or if the `get_many` query errors. A test-only helper —
    /// these are bugs in the test setup, not recoverable conditions.
    #[cfg(feature = "test-utils")]
    #[allow(clippy::significant_drop_tightening)]
    pub async fn test_namespace_tombstone_keys(&self, namespace_id_bytes: [u8; 32]) -> Vec<String> {
        let guard = self.node.lock().await;
        let node = guard.as_ref().expect("node live");
        let id = NamespaceId::from(&namespace_id_bytes);
        let doc = node
            .docs
            .open(id)
            .await
            .expect("open namespace")
            .expect("namespace present");
        let stream = doc
            .get_many(Query::single_latest_per_key().include_empty())
            .await
            .expect("get_many");
        tokio::pin!(stream);
        let mut keys = Vec::new();
        while let Some(res) = stream.next().await {
            if let Ok(entry) = res
                && entry.content_len() == 0
            {
                keys.push(String::from_utf8_lossy(entry.key()).into_owned());
            }
        }
        keys
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
    exclude: &ExcludeRules,
    echo_guard: &EchoGuard,
    events: &mpsc::Sender<WorkspaceEvent>,
) -> Result<(), WorkspaceError> {
    let filter = WorkspaceFilter::new(root, exclude.clone());
    let mut published = 0usize;
    let mut skipped = 0usize;
    for entry in WalkDir::new(root).follow_links(false).into_iter().flatten() {
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        // Advisory skips use `emit_event` (try_send, drop-on-full),
        // NOT a blocking `send`: this scan runs inside the
        // constructor, before the caller ever receives the event
        // receiver, so nothing is draining yet. A blocking send here
        // deadlocks construction as soon as the walk produces more
        // than EVENT_BUFFER skips — trivial under the dotfile default
        // (any `.venv/`-sized hidden tree).
        match filter.check(path) {
            FilterDecision::Skip(SkipReason::TooLarge { size }) => {
                emit_event(
                    events,
                    WorkspaceEvent::SkippedTooLarge {
                        path: path.to_path_buf(),
                        size,
                    },
                );
                continue;
            }
            FilterDecision::Skip(SkipReason::Excluded) => {
                skipped += 1;
                emit_event(
                    events,
                    WorkspaceEvent::SkippedExcluded {
                        path: path.to_path_buf(),
                        direction: Direction::Outgoing,
                    },
                );
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
            emit_event(
                events,
                WorkspaceEvent::SkippedReadOnly {
                    path: path.to_path_buf(),
                    direction: Direction::Outgoing,
                },
            );
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

/// Publish `ticket` + `rules` to the daemon via
/// [`Request::PublishWorkspaceTicket`] (revoked-lurker fix). The
/// daemon persists the envelope on the session record and owns its
/// distribution — host→peer unicast over the direct delivery stream,
/// to current members on publish and to each peer at admission.
/// Nothing capability-bearing rides the gossip topic; joiners see the
/// envelope as the daemon's synthetic [`TICKET_ACTION`] System
/// message (live + replayed on `Subscribe`).
///
/// Wire shape inside the envelope bytes: postcard-encoded
/// [`WorkspaceTicketEnvelope`], byte-stable across host restarts (the
/// restart contract depends on identical bytes). The legacy
/// pre-envelope shape (raw `DocTicket::to_string().into_bytes()`) is
/// hard-rejected by the joiner with
/// [`TicketEnvelopeError::Malformed`] — see [`crate::ticket`] module
/// docs for the wire-compat decision.
pub(crate) async fn publish_ticket(
    client: &Client,
    session: SessionId,
    ticket: &DocTicket,
    rules: &PathRules,
) -> Result<(), WorkspaceError> {
    let envelope = WorkspaceTicketEnvelope::new(ticket.to_string(), rules.clone());
    let envelope_bytes = ticket::encode(&envelope)?;
    let resp = client
        .request(Request::PublishWorkspaceTicket {
            session,
            envelope_bytes,
        })
        .await?;
    match resp {
        Response::WorkspaceTicketPublished => Ok(()),
        other => Err(WorkspaceError::Iroh(format!(
            "unexpected response to PublishWorkspaceTicket: {other:?}",
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

/// Host-side: ask the daemon to deliver a cooperative-downgrade
/// notification to `target_peer` over the direct stream. Mirror of
/// [`publish_upgrade`] with no key material — a demotion only tells the
/// peer to stop writing.
async fn publish_downgrade(
    client: &Client,
    session: SessionId,
    target_peer: PeerId,
) -> Result<(), ClientError> {
    client
        .request(Request::DeliverDowngrade {
            session,
            target_peer,
        })
        .await?;
    Ok(())
}

/// Host-side context for distributing a rotated namespace's ticket to
/// survivors (Slice 3e) and refreshing the host's durable distribution
/// state on rotation (C1).
pub(crate) struct RotationDistributeCtx {
    pub(crate) client: Arc<Client>,
    pub(crate) session: SessionId,
    /// Shared with the cap-listener's [`HostUpgradeCtx`]. On rotation the
    /// rotation task overwrites this with the freshly minted secret so a
    /// peer promoted to RW *after* the rotation receives the new secret,
    /// not the stale genesis one (C1).
    pub(crate) upgrade_secret: Arc<std::sync::Mutex<[u8; 32]>>,
    /// Shared with the cap-listener's [`HostUpgradeCtx::current_write_ticket`].
    /// On rotation the rotation task overwrites this with the rotated
    /// Write ticket + epoch so the cap-listener can re-deliver the rotated
    /// namespace to a *returning* RW member whose mirror reloaded a stale
    /// envelope across a restart.
    pub(crate) current_write_ticket: Arc<std::sync::Mutex<(String, u64)>>,
    /// The host's path rules, needed to rebuild the `workspace.ticket`
    /// envelope re-published on rotation so late joiners import the
    /// rotated namespace (C1).
    pub(crate) rules: PathRules,
}

impl std::fmt::Debug for RotationDistributeCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RotationDistributeCtx")
            .field("session", &self.session)
            .finish_non_exhaustive()
    }
}

/// Host-side: ask the daemon to deliver a rotated namespace's Write
/// `DocTicket` + epoch to `target_peer` over the direct stream.
async fn publish_rotate(
    client: &Client,
    session: SessionId,
    target_peer: PeerId,
    namespace_epoch: u64,
    doc_ticket: String,
) -> Result<(), ClientError> {
    client
        .request(Request::DeliverRotate {
            session,
            target_peer,
            namespace_epoch,
            doc_ticket,
        })
        .await?;
    Ok(())
}

/// Host-side: ask the daemon to drop `target_peer` from the session's
/// durable membership (the host evicting an evicted peer). Issued when
/// the cap-listener observes a capability `Revoke`, so the host stops
/// serving the evicted peer gossip — most importantly the
/// membership-gated log `Replay` an announce-less re-subscribe would
/// otherwise still hand it on reattach. The daemon is told only to drop
/// a member; the capability semantics stay artel-fs-side (ADR-003).
async fn publish_remove_member(
    client: &Client,
    session: SessionId,
    target_peer: PeerId,
) -> Result<(), ClientError> {
    client
        .request(Request::RemoveSessionMember {
            session,
            target_peer,
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
    exclude: &ExcludeRules,
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
    let filter = WorkspaceFilter::new(root, exclude.clone());

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
        // local filter rejects — asymmetric exclude lists across
        // peers, version drift, an attacker-crafted key targeting a
        // hardcoded-skip path like `.git/HEAD` — would otherwise
        // reach `tokio::fs::remove_file` regardless. The `ReadOnly`
        // rule is gated for the same reason: a `ReadOnly` path's
        // incoming tombstone must not trigger `remove_file`, and a
        // `ReadOnly` path's incoming write must not be applied
        // even if the filter would have let it through.
        // Advisory skips use `emit_event` (try_send, drop-on-full),
        // NOT a blocking `send`: bulk_export runs inside `join_with`,
        // before the caller ever receives the event receiver, so
        // nothing is draining yet — a blocking send deadlocks the
        // constructor once skips exceed EVENT_BUFFER (trivial under
        // the dotfile default against a doc with a large hidden tree).
        //
        // Filter BEFORE rules, matching the watcher/scan outgoing
        // order, so a path that is both excluded and `ReadOnly`
        // reports the same skip reason in both directions.
        match filter.check(&path) {
            FilterDecision::Skip(SkipReason::TooLarge { size }) => {
                emit_event(
                    events,
                    WorkspaceEvent::SkippedTooLarge {
                        path: path.clone(),
                        size,
                    },
                );
                continue;
            }
            FilterDecision::Skip(SkipReason::Excluded) => {
                emit_event(
                    events,
                    WorkspaceEvent::SkippedExcluded {
                        path,
                        direction: Direction::Incoming,
                    },
                );
                continue;
            }
            FilterDecision::Skip(_) => continue,
            FilterDecision::Include => {}
        }

        let rel = path.strip_prefix(root).unwrap_or(&path);
        if rules.mode_for(rel) == Mode::ReadOnly {
            emit_event(
                events,
                WorkspaceEvent::SkippedReadOnly {
                    path,
                    direction: Direction::Incoming,
                },
            );
            continue;
        }

        if entry.content_len() == 0 {
            // Same marking as the applier's tombstone branch: the
            // guard is handed to the watcher/applier right after this
            // bulk pass, and an unlink of a pre-existing file here can
            // surface as a watcher event after the watch attaches.
            // The Fresh/Duplicate mark is safe to discard here: the
            // guard is empty at bulk-export time and the query is
            // single_latest_per_key, so no key yields two tombstones
            // in one pass — every mark is Fresh, and nothing local
            // can have raced onto the path before the watcher exists.
            let _ = echo_guard.mark_remote_delete(&path).await;
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

/// Per-reason drop counts from [`Workspace::collect_rotation_survivors`]
/// (D2). All three are dropped from the rotated namespace, but they are
/// counted separately so the caller can surface the *alarming* one
/// (`unresolvable` — possible survivor data loss) distinctly from the
/// *expected* one (`revoked` — the intended effect of an Evict).
#[derive(Default)]
struct RotationDropCounts {
    /// Author resolves to an explicitly-revoked peer (intended drop).
    revoked: usize,
    /// Author resolves to a known non-RW peer (Read / dropped to floor).
    not_rw: usize,
    /// Author's `EndpointId` has no mapping yet — fail-closed drop, but a
    /// liveness race that may have silently lost a trusted peer's data.
    unresolvable: usize,
}

impl RotationDropCounts {
    const fn total(&self) -> usize {
        self.revoked + self.not_rw + self.unresolvable
    }
}

/// Result of [`Workspace::rotate_namespace`].
pub(crate) struct RotationOutcome {
    /// The freshly minted `NamespaceSecret` (32 bytes). Used by the
    /// **host's own** reimport ([`Workspace::reimport_namespace`]) — the
    /// host owns the namespace locally so it needs no peer addresses.
    pub(crate) new_secret: [u8; 32],
    /// The Write [`iroh_docs::DocTicket`] string (capability **+** the
    /// host's relay/direct addresses) for **survivor** distribution: a
    /// survivor importing a brand-new namespace has no addr-book entry
    /// for the host, so a bare secret can't sync — it needs
    /// `start_sync(ticket.nodes)`. Shipped over the `DeliverDowngrade`-
    /// style rotation unicast (see [`DeliveryFrame::Rotate`]). Never
    /// given to the revoked peer.
    pub(crate) write_ticket: String,
    /// The Read [`iroh_docs::DocTicket`] string (capability + addresses)
    /// for the rotated namespace. The host re-publishes this as the new
    /// `workspace.ticket` envelope so a peer that joins AFTER the
    /// rotation imports the rotated namespace, not the abandoned genesis
    /// (C1). Carries no write capability — RW is delivered separately via
    /// the refreshed upgrade secret.
    pub(crate) read_ticket: String,
    /// The new current `NamespaceId`.
    pub(crate) new_namespace: NamespaceId,
    /// The bumped `namespace_epoch` (prev + 1).
    pub(crate) new_epoch: u64,
    /// How many latest-per-key entries were carried into the new
    /// namespace (still-RW authors). Read only by the test-utils
    /// rotation accessor; the production path logs the local counts.
    #[cfg_attr(not(feature = "test-utils"), allow(dead_code))]
    pub(crate) survivor_entries: usize,
    /// How many entries were dropped because their author was no longer
    /// RW (the revoked peer's, plus any unresolvable authors). Read only
    /// by the test-utils rotation accessor.
    #[cfg_attr(not(feature = "test-utils"), allow(dead_code))]
    pub(crate) dropped_entries: usize,
}

/// A namespace-rotation signal sent from the cap-listener (no
/// `Arc<Workspace>`) to the rotation task in [`Workspace::run`].
pub(crate) enum RotationSignal {
    /// Host side: a peer was evicted (`Revoke`); rotate the namespace,
    /// distribute the new ticket to survivors, and reimport locally.
    /// `revoke_seq` is the sequence number of the `Revoke` log message,
    /// used as a monotonic high-water idempotency key so a replayed
    /// historical revoke (re-delivered on every host restart's
    /// `Subscribe { since: None }`) doesn't re-fire the rotation (C3).
    HostEvict {
        revoked_peer: PeerId,
        revoke_seq: Seq,
    },
    /// Survivor side: the host delivered a rotated namespace; reimport
    /// onto it if `namespace_epoch` is newer than what we hold.
    SurvivorRotate {
        namespace_epoch: u64,
        doc_ticket: String,
    },
}

/// How [`Workspace::reimport_namespace`] obtains the rotated namespace.
pub(crate) enum ReimportSource {
    /// The host re-importing a namespace it just minted locally — it
    /// already owns the replica, so no peer addresses are needed.
    HostLocal {
        new_secret: [u8; 32],
        new_namespace: NamespaceId,
        /// Epoch to record locally after the swap.
        new_epoch_hint: u64,
    },
    /// A survivor re-importing from the host's Write `DocTicket` string
    /// (capability + relay/direct addresses). The addresses seed the
    /// addr-book so the brand-new namespace can sync from the host.
    SurvivorTicket {
        doc_ticket: String,
        /// Epoch to record locally after the swap.
        new_epoch_hint: u64,
    },
}

/// What [`open_or_create_doc`] resolved for a host workspace.
struct OpenedDoc {
    /// The doc handle for the **current** namespace (the one the
    /// workspace writes to and syncs).
    doc: Doc,
    /// The **genesis** `NamespaceId` (`doc-id`), the stable root the
    /// `SessionId` derivation reads. Equals `doc.id()` until a rotation
    /// has happened.
    genesis: NamespaceId,
    /// `true` if we opened an existing doc (caller must reconcile
    /// against disk); `false` if we created a fresh one.
    returning: bool,
    /// The persisted `namespace_epoch` recovered from disk (C2). `0` for
    /// a fresh host or one that never rotated; the last persisted epoch
    /// for a returning host that had rotated. Seeds the workspace's
    /// in-memory `AtomicU64` so a second eviction doesn't re-mint an
    /// epoch survivors already passed.
    epoch: u64,
}

/// Read a 32-byte `NamespaceId` from a state-dir file. `Ok(None)` if
/// the file is absent; `Err` if present-but-corrupt or unreadable.
fn read_namespace_file(path: &Path) -> Result<Option<NamespaceId>, WorkspaceError> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                WorkspaceError::Doc(format!(
                    "namespace file at {} is corrupt: expected 32 bytes, got {}",
                    path.display(),
                    bytes.len(),
                ))
            })?;
            Ok(Some(NamespaceId::from(&arr)))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(WorkspaceError::Doc(format!(
            "read namespace file at {}: {err}",
            path.display(),
        ))),
    }
}

/// Read a persisted little-endian `u64` from a state-dir file.
/// `Ok(0)` if the file is absent; `Err` if present-but-corrupt or
/// unreadable. `label` names the value in error messages. Shared by the
/// `namespace_epoch` (C2) and `rotated-revoke-seq` (C3) recovery paths.
fn read_u64_file(path: &Path, label: &str) -> Result<u64, WorkspaceError> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let arr: [u8; 8] = bytes.as_slice().try_into().map_err(|_| {
                WorkspaceError::Doc(format!(
                    "{label} file at {} is corrupt: expected 8 bytes, got {}",
                    path.display(),
                    bytes.len(),
                ))
            })?;
            Ok(u64::from_le_bytes(arr))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(WorkspaceError::Doc(format!(
            "read {label} file at {}: {err}",
            path.display(),
        ))),
    }
}

/// Atomically persist a little-endian `u64` to `path`. `label` names the
/// value in error messages. Used for `namespace_epoch` (C2) and
/// `rotated-revoke-seq` (C3).
fn persist_u64(path: &Path, value: u64, label: &str) -> Result<(), WorkspaceError> {
    crate::keystore::write_atomic(path, &value.to_le_bytes(), None)
        .map_err(|e| WorkspaceError::Doc(format!("persist {label} at {}: {e}", path.display())))
}

/// Open the host's persisted doc, or create a fresh one.
///
/// Identity decoupling (Slice 2): `doc-id` holds the **genesis**
/// namespace (write-once, drives `SessionId`); `current-namespace`
/// holds the namespace actually opened/synced (absent ⇒ equals
/// genesis). The two diverge once a rotation lands
/// ([`Workspace::rotate_namespace`] rewrites `current-namespace`;
/// joiners follow via [`Workspace::reimport_namespace`]); the split is
/// what lets rotation change the current namespace without disturbing
/// the session id.
async fn open_or_create_doc(
    node: &WorkspaceNode,
    doc_id_path: &Path,
    current_ns_path: &Path,
    epoch_path: &Path,
) -> Result<OpenedDoc, WorkspaceError> {
    let Some(genesis) = read_namespace_file(doc_id_path)? else {
        // No genesis yet ⇒ first host. Create a fresh namespace; it is
        // both genesis and current.
        return create_and_persist(node, doc_id_path, epoch_path).await;
    };

    // The namespace to actually open is the current one if recorded,
    // else the genesis.
    let current = read_namespace_file(current_ns_path)?.unwrap_or(genesis);

    // `Docs::open` returns "Replica not found" if the redb commit for
    // the namespace hasn't durably landed yet — `iroh-docs` batches
    // writes with a 500 ms delay, so a crash between `Docs::create`
    // returning and the commit firing can leave a recorded id pointing
    // at a namespace that doesn't exist on disk. Self-heal by
    // recreating; joiners with the prior ticket lose the ability to
    // resume, which is acceptable since they wouldn't have synced
    // anything pre-crash anyway.
    if let Ok(Some(doc)) = node.docs.open(current).await {
        // Recover the persisted epoch (C2): absent ⇒ 0 (never rotated).
        let epoch = read_u64_file(epoch_path, "namespace-epoch")?;
        Ok(OpenedDoc {
            doc,
            genesis,
            returning: true,
            epoch,
        })
    } else {
        tracing::warn!(
            ?current,
            "stale namespace at {}: not in store, recreating",
            current_ns_path.display(),
        );
        create_and_persist(node, doc_id_path, epoch_path).await
    }
}

/// Create a fresh namespace and persist its id as the genesis. The
/// fresh namespace is both genesis and current, so no
/// `current-namespace` file is written (absent ⇒ equals genesis).
async fn create_and_persist(
    node: &WorkspaceNode,
    doc_id_path: &Path,
    epoch_path: &Path,
) -> Result<OpenedDoc, WorkspaceError> {
    let doc = node
        .docs
        .create()
        .await
        .map_err(|e| WorkspaceError::Doc(format!("doc create: {e}")))?;
    let genesis = doc.id();
    // No chmod — namespace ids aren't secret.
    crate::keystore::write_atomic(doc_id_path, &genesis.to_bytes(), None).map_err(|e| {
        WorkspaceError::Doc(format!("persist doc-id at {}: {e}", doc_id_path.display()))
    })?;
    // A fresh genesis is epoch 0. Clear any stale epoch file so a
    // self-heal recreate (new genesis at the same state dir) doesn't
    // inherit a higher rotation count from the abandoned namespace.
    if let Err(err) = std::fs::remove_file(epoch_path)
        && err.kind() != io::ErrorKind::NotFound
    {
        return Err(WorkspaceError::Doc(format!(
            "reset namespace-epoch at {}: {err}",
            epoch_path.display(),
        )));
    }
    Ok(OpenedDoc {
        doc,
        genesis,
        returning: false,
        epoch: 0,
    })
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
    exclude: &ExcludeRules,
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

    let filter = WorkspaceFilter::new(root, exclude.clone());
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
                emit_event(
                    events,
                    WorkspaceEvent::Error(format!("reconcile invalid key: {err}")),
                );
                continue;
            }
        };
        // A path this node's filter refuses is expected to be absent
        // from disk — it was never applied here. Tombstoning it would
        // destroy a peer's live entry just because our local hygiene
        // hides it (e.g. a returning host whose exclude list covers a
        // dotfile another peer legitimately publishes). "Missing from
        // disk" is only evidence of deletion for paths this node
        // actually mirrors. No event: nothing was skipped that this
        // node would ever have synced.
        if !matches!(filter.check(&path), FilterDecision::Include) {
            continue;
        }
        if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
            debug!(target: "artel_fs::workspace", path = %path.display(), "reconcile: tombstoning entry not on disk");
            // `Doc::del` writes a tombstone for `prefix`. The key
            // we hand it is exact, so it tombstones just this
            // entry.
            if let Err(err) = doc.del(author, entry.key().to_vec()).await {
                warn!(target: "artel_fs::workspace", path = %path.display(), %err, "reconcile: tombstone failed");
                // try_send, not send: reconcile runs inside the
                // constructor before the receiver is handed back —
                // a blocking send with no drainer deadlocks it.
                emit_event(
                    events,
                    WorkspaceEvent::Error(format!("reconcile tombstone {}: {err}", path.display())),
                );
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

/// Host-side context for delivering `NamespaceSecret` upgrades when a
/// peer is granted RW. `None` on the joiner side.
struct HostUpgradeCtx {
    client: Arc<Client>,
    session: SessionId,
    /// The *current* namespace secret to deliver on an RW grant. Shared
    /// (and mutated on rotation) with [`RotationDistributeCtx`] so a peer
    /// promoted after a rotation receives the rotated secret, not the
    /// stale genesis one (C1).
    namespace_secret: Arc<std::sync::Mutex<[u8; 32]>>,
    /// The *current* rotated Write ticket + `namespace_epoch`, shared
    /// (and mutated on rotation) with [`RotationDistributeCtx`]. Used to
    /// re-deliver the rotated namespace to a *returning* RW member whose
    /// daemon reloaded a stale (genesis-or-older) `workspace.ticket`
    /// across a restart: the live-only unicast it would have received on
    /// rotation was lost while it was offline, and its mirror's replayed
    /// envelope is its own stale copy, so without this re-delivery the
    /// returner writes to the abandoned namespace. Genesis value is the
    /// genesis Write ticket at epoch 0; the joiner's monotonic-epoch
    /// guard ([`RotationSignal::SurvivorRotate`]) drops it as a no-op when
    /// the returner is already current.
    current_write_ticket: Arc<std::sync::Mutex<(String, u64)>>,
    /// Per-peer high-water mark of the namespace epoch we have already
    /// re-delivered (secret + rotated ticket) to on a `NODE_ID` announce.
    ///
    /// `NODE_ID` is a logged, replayed message, so on a host cap-listener
    /// restart every historical announce replays and would re-fan-out a
    /// delivery to each RW peer that ever announced — wasteful chatter
    /// (each a direct-stream unicast), most of it to peers not even online
    /// now. This map suppresses the storm: a `(peer, epoch)` already
    /// delivered is skipped. It does NOT suppress a genuine recovery — a
    /// returning member that was offline across a rotation has no entry at
    /// the current epoch, so it still gets the re-delivery.
    ///
    /// Robustness: the entry is advanced only when delivery *succeeds*,
    /// and rolled back (CAS-guarded) on failure, so a transient delivery
    /// failure never durably suppresses a later retry — that would
    /// recreate the silent-stuck-on-stale-namespace bug this whole feature
    /// exists to prevent.
    redelivered_epoch: Arc<std::sync::Mutex<std::collections::HashMap<PeerId, u64>>>,
    /// Sender to the rotation task: on a `Revoke` the host cap-listener
    /// sends [`RotationSignal::HostEvict`] here (the cap-listener has no
    /// `Arc<Workspace>`, so it can't rotate directly). Slice 3e.
    rotation_tx: mpsc::UnboundedSender<RotationSignal>,
}

/// Joiner-side context for receiving `NamespaceSecret` upgrades.
struct JoinerUpgradeCtx {
    my_peer_id: PeerId,
    docs: iroh_docs::protocol::Docs,
    /// Shared with [`Workspace::write_halted`]; the `DOWNGRADE_ACTION`
    /// handler flips it so the watcher stops publishing.
    write_halted: Arc<std::sync::atomic::AtomicBool>,
    /// Sender to the rotation task: on a `ROTATE_ACTION` the survivor
    /// cap-listener sends [`RotationSignal::SurvivorRotate`] here. Slice 3e.
    rotation_tx: mpsc::UnboundedSender<RotationSignal>,
}

/// Exponential backoff for the cap-listener's reconnect loop.
///
/// `attempt` is zero-based: attempt 0 is the first retry delay. The
/// delay doubles from [`CAP_RECONNECT_BASE_DELAY`] each attempt, capped
/// at [`CAP_RECONNECT_MAX_DELAY`]. Returns `None` once `attempt`
/// reaches [`CAP_RECONNECT_MAX_ATTEMPTS`] — the signal to give up so a
/// genuinely-dead daemon doesn't keep a task spinning forever.
fn cap_reconnect_backoff(attempt: u32) -> Option<Duration> {
    if attempt >= CAP_RECONNECT_MAX_ATTEMPTS {
        return None;
    }
    // Saturating doubling: `BASE << attempt`, clamped to the cap. Guard
    // the shift so a large `attempt` can't overflow before the clamp.
    let delay = if attempt >= 32 {
        CAP_RECONNECT_MAX_DELAY
    } else {
        CAP_RECONNECT_BASE_DELAY
            .saturating_mul(1u32 << attempt)
            .min(CAP_RECONNECT_MAX_DELAY)
    };
    Some(delay)
}

/// Fold a freshly-seen message `seq` into the cap-listener's last-seen
/// watermark. The watermark only ever advances — a `Seq::ZERO`
/// synthetic (the daemon re-injects the workspace ticket at
/// `Seq::ZERO` on every `Subscribe`) or a replayed lower seq can't pull
/// it back, so a resume from `since: Some(prev)` never re-requests
/// already-processed log entries.
fn max_seq(prev: Option<Seq>, seq: Seq) -> Seq {
    prev.map_or(seq, |p| p.max(seq))
}

/// Open a fresh connection to `socket`, `Subscribe { since }`, and
/// return the owning [`Client`] plus its event stream.
///
/// The cap-listener **keeps** the returned `Arc<Client>` (it is not
/// dropped) so it can issue an in-band re-`Subscribe` on the same
/// connection when the daemon signals a gap (M3 Part B). The held
/// client also carries its [`Client::socket_path`], which the listener
/// uses to reconnect from scratch on a full EOF (Part A).
///
/// A `since` of `last_seq` makes the daemon replay every logged message
/// past the watermark, so resumption is gap-free for log-borne events.
async fn cap_resubscribe(
    socket: &Path,
    session: SessionId,
    since: Option<Seq>,
) -> Result<(Arc<Client>, artel_client::EventStream), WorkspaceError> {
    let client = Client::connect(socket)
        .await
        .map_err(|e| WorkspaceError::Iroh(format!("cap-listener reconnect: {e}")))?;
    match client
        .request(Request::Subscribe { session, since })
        .await?
    {
        Response::Subscribed { .. } => {}
        other => {
            return Err(WorkspaceError::Iroh(format!(
                "cap-listener resubscribe: unexpected response: {other:?}",
            )));
        }
    }
    let events = client
        .take_events()
        .await
        .ok_or_else(|| WorkspaceError::Iroh("cap-listener: events already taken".into()))?;
    Ok((Arc::new(client), events))
}

/// Open a second [`Client`] connection, subscribe to `session`, and
/// spawn the cap-listener task on that independent event stream.
///
/// Returns a no-op handle if `socket` is `None` (the gate still
/// rejects based on the seed state populated at construction).
async fn spawn_cap_listener_from_socket(
    socket: Option<&Path>,
    session: SessionId,
    peer_map: Arc<PeerMap>,
    cancel: CancellationToken,
    host_ctx: Option<HostUpgradeCtx>,
    joiner_ctx: Option<JoinerUpgradeCtx>,
    events_tx: mpsc::Sender<WorkspaceEvent>,
) -> Result<tokio::task::JoinHandle<()>, WorkspaceError> {
    let Some(socket) = socket else {
        return Ok(tokio::spawn(async move {
            cancel.cancelled().await;
        }));
    };
    let (client, events) = cap_resubscribe(socket, session, None).await?;
    Ok(spawn_cap_listener(
        client, events, session, peer_map, cancel, host_ctx, joiner_ctx, events_tx,
    ))
}

/// What one [`Event`] meant to the cap-listener loop.
enum CapOutcome {
    /// A session [`Event::Message`] was applied; carries its `seq` so
    /// the loop can advance its last-seen watermark.
    Advanced(Seq),
    /// The daemon signalled [`Event::Gap`] for this session — the
    /// per-subscriber buffer overflowed. The loop must re-`Subscribe`
    /// from its watermark to recover the dropped log entries.
    Gap,
    /// Nothing the loop needs to react to (live-only membership events,
    /// or an event for another session).
    Ignored,
}

/// Handle a `NODE_ID_ACTION` System message: register the announcing
/// peer's workspace `EndpointId` -> daemon `PeerId` mapping, and — on the
/// host — re-deliver BOTH the current `NamespaceSecret` AND the current
/// rotated Write ticket to that peer if it holds RW.
///
/// This is the recovery path for a member that was offline across a
/// namespace rotation. Two things were lost while it was down, with the
/// same root cause (no re-delivery trigger fires for a reloaded mirror —
/// `PeerJoined`/`ensure_member` only run on a *fresh* gossip announce):
/// 1. the new secret (live-only upgrade broadcast, long gone), and
/// 2. the rotated *namespace* — its mirror replays its own stale
///    `workspace.ticket`, so without re-delivery it imports the abandoned
///    genesis namespace and writes where the host never sees it.
///
/// `NODE_ID` is the correct trigger because a joiner emits it only
/// *after* its own cap-listener is live (see `join_with`), so neither
/// re-delivery can outrun its receiver. The rotated ticket rides the
/// existing `publish_rotate`/`SurvivorRotate` path the joiner already
/// consumes (`reimport_namespace`).
///
/// Fires on every reattach. Idempotent: the secret import is a monotonic
/// Read->Write merge; the rotated ticket is dropped by the joiner's
/// monotonic-epoch guard when already current (and is inert at genesis
/// epoch 0). Gated on `has_rw` *after* `register`, so a since-revoked
/// peer gets nothing. Best-effort spawn.
fn handle_node_id_message(
    message: &SessionMessage,
    peer_map: &Arc<PeerMap>,
    host_ctx: Option<&HostUpgradeCtx>,
) {
    let Ok(bytes) = <[u8; 32]>::try_from(message.payload.as_slice()) else {
        return;
    };
    let Ok(workspace_id) = iroh::EndpointId::from_bytes(&bytes) else {
        return;
    };
    peer_map.register(workspace_id, message.peer.id);

    if let Some(ctx) = host_ctx
        && peer_map.has_rw(message.peer.id)
    {
        let client = Arc::clone(&ctx.client);
        let sess = ctx.session;
        let peer = message.peer.id;
        // Read the live secret (refreshed on rotation, C1).
        let secret = *ctx.namespace_secret.lock().expect("upgrade_secret mutex");
        // Read the live rotated ticket + epoch. A returning member that
        // was offline across a rotation reloaded a stale `workspace.ticket`
        // (its mirror's own genesis-or-older copy) and would otherwise
        // write to the abandoned namespace. Re-delivering the current
        // Write ticket here — alongside the secret — swaps it onto the
        // rotated namespace via the joiner's `SurvivorRotate` consumer,
        // which drops it as a no-op (monotonic-epoch guard) when already
        // current. At genesis the epoch is 0, so this is inert for a
        // never-rotated session.
        let (write_ticket, namespace_epoch) = ctx
            .current_write_ticket
            .lock()
            .expect("current_write_ticket mutex")
            .clone();

        // De-storm (finding #4): `NODE_ID` is logged + replayed, so a host
        // cap-listener restart replays every historical announce and would
        // re-fan-out a unicast to each RW peer that ever announced. Skip if
        // we have already delivered this peer the current (or a newer)
        // epoch. We claim the high-water mark up front so concurrent
        // replays of the same announce collapse to one delivery, then roll
        // it back if the delivery fails — a transient failure must never
        // durably suppress a genuine later re-delivery (that would recreate
        // the silent-stuck-on-stale-namespace bug).
        let redelivered = Arc::clone(&ctx.redelivered_epoch);
        if !claim_redelivery_epoch(&redelivered, peer, namespace_epoch) {
            debug!(
                ?peer,
                namespace_epoch, "node_id re-delivery: already current, skipping",
            );
            return;
        }
        tokio::spawn(async move {
            // Run both deliveries (don't short-circuit — the rotate is
            // useful even if the upgrade failed and vice versa), tracking
            // whether either failed.
            let upgrade_ok = match publish_upgrade(&client, sess, peer, secret).await {
                Ok(()) => true,
                Err(e) => {
                    warn!(?e, ?peer, "upgrade re-delivery on node_id failed");
                    false
                }
            };
            let rotate_ok =
                match publish_rotate(&client, sess, peer, namespace_epoch, write_ticket).await {
                    Ok(()) => true,
                    Err(e) => {
                        warn!(?e, ?peer, "rotated-ticket re-delivery on node_id failed");
                        false
                    }
                };
            if !(upgrade_ok && rotate_ok) {
                // Roll the high-water mark back so the next announce
                // retries (only if no later delivery overtook us).
                rollback_redelivery_epoch(&redelivered, peer, namespace_epoch);
            }
        });
    }
}

/// Claim the per-peer `NODE_ID` re-delivery high-water mark for
/// `epoch` (finding #4 de-storm). Returns `true` if the caller should
/// proceed with delivery — i.e. this peer had not already been delivered
/// `epoch` or newer — and records `epoch` as claimed. Returns `false`
/// (no mutation) when a delivery at `epoch` or higher already happened,
/// so a replayed `NODE_ID` is suppressed. A peer with no entry is treated
/// as never-delivered and always proceeds.
fn claim_redelivery_epoch(
    redelivered: &std::sync::Mutex<std::collections::HashMap<PeerId, u64>>,
    peer: PeerId,
    epoch: u64,
) -> bool {
    let mut seen = redelivered.lock().expect("redelivered_epoch mutex");
    if seen.get(&peer).is_some_and(|&e| e >= epoch) {
        return false;
    }
    seen.insert(peer, epoch);
    true
}

/// Roll back a [`claim_redelivery_epoch`] claim after a failed delivery,
/// so the next `NODE_ID` announce retries. Only clears the entry if it is
/// still exactly `epoch` — a concurrent higher-epoch delivery that
/// overtook us must keep its progress, never be clobbered back.
fn rollback_redelivery_epoch(
    redelivered: &std::sync::Mutex<std::collections::HashMap<PeerId, u64>>,
    peer: PeerId,
    epoch: u64,
) {
    let mut seen = redelivered.lock().expect("redelivered_epoch mutex");
    if seen.get(&peer) == Some(&epoch) {
        seen.remove(&peer);
    }
}

/// Apply a `Capability` message to `peer_map`, then fire host-side unicast.
///
/// On the host an RW grant delivers the `NamespaceSecret` (upgrade),
/// and a cooperative demote from RW to Read delivers a downgrade
/// notification so the peer halts its own watcher. Both side-effects
/// are best-effort spawns.
fn handle_capability_message(
    message: &SessionMessage,
    peer_map: &Arc<PeerMap>,
    host_ctx: Option<&HostUpgradeCtx>,
    events_tx: &mpsc::Sender<WorkspaceEvent>,
) {
    use artel_protocol::capability::{Capability, CapabilityAction};

    // Capture the affected peer's cap *before* applying, so we can tell
    // a cooperative demote (was RW, now Read) from a fresh Read grant —
    // the downgrade notification fires only on the former.
    let demote_target = if host_ctx.is_some() {
        match CapabilityAction::decode(&message.payload) {
            Ok(CapabilityAction::Grant {
                peer,
                cap: Capability::Read,
            }) if peer_map.has_rw(peer) => Some(peer),
            _ => None,
        }
    } else {
        None
    };

    peer_map.apply_capability(message.peer.id, &message.payload);

    // Surface the applied revoke to the events stream. Gate on host
    // authorship — `apply_capability` ignored anything else, so a
    // non-host `Revoke` must not signal "peer is out" when the
    // transport gates won't actually block it.
    if message.peer.id == peer_map.host_peer_id()
        && let Ok(CapabilityAction::Revoke { peer }) = CapabilityAction::decode(&message.payload)
    {
        emit_event(events_tx, WorkspaceEvent::PeerRevoked { peer });
    }

    // Host: on RW grant, deliver the NamespaceSecret to the promoted
    // peer. Check has_rw AFTER apply so a grant whose peer was later
    // revoked (during replay) is suppressed.
    if let Some(ctx) = host_ctx
        && let Ok(CapabilityAction::Grant {
            peer,
            cap: Capability::ReadWrite,
        }) = CapabilityAction::decode(&message.payload)
        && peer_map.has_rw(peer)
    {
        let client = Arc::clone(&ctx.client);
        let sess = ctx.session;
        // Read the *current* secret (refreshed on rotation, C1).
        let secret = *ctx.namespace_secret.lock().expect("upgrade_secret mutex");
        tokio::spawn(async move {
            if let Err(e) = publish_upgrade(&client, sess, peer, secret).await {
                warn!(?e, ?peer, "upgrade delivery failed");
            }
        });
    }

    // Host: on a cooperative demote (RW → Read), notify the peer so its
    // daemon halts its own watcher. Carries no key material; the
    // cryptographic write cut-off is rotation on Revoke/Evict.
    if let Some(ctx) = host_ctx
        && let Some(peer) = demote_target
    {
        let client = Arc::clone(&ctx.client);
        let sess = ctx.session;
        tokio::spawn(async move {
            if let Err(e) = publish_downgrade(&client, sess, peer).await {
                warn!(?e, ?peer, "downgrade delivery failed");
            }
        });
    }

    // Host: on an Evict (`Revoke`), signal the rotation task to rotate
    // the namespace (cryptographic write cut-off). The cap-listener has
    // no `Arc<Workspace>`, so it hands the rotation off via the channel;
    // the apply above has already removed the revoked peer from the cap
    // set, so the rotation's survivor set excludes it.
    if let Some(ctx) = host_ctx
        && let Ok(CapabilityAction::Revoke { peer }) = CapabilityAction::decode(&message.payload)
    {
        if let Err(e) = ctx.rotation_tx.send(RotationSignal::HostEvict {
            revoked_peer: peer,
            // Carry the Revoke's log seq as the idempotency key: the
            // rotation task skips any HostEvict whose seq it has already
            // rotated for, so a replayed historical revoke doesn't
            // re-rotate (C3).
            revoke_seq: message.seq,
        }) {
            // Unbounded send (C4) only errors if the rotation task is gone
            // (workspace shutting down) — never on back-pressure.
            warn!(?e, ?peer, "rotation: failed to enqueue HostEvict signal");
        }

        // Drop the evicted peer from the substrate's durable membership
        // so the host stops serving it gossip — notably the
        // membership-gated log `Replay` that an announce-less
        // re-subscribe (the reload re-delivery path) would otherwise
        // still hand it on reattach. The crypto cut (rotation, above) and
        // the transport block (`PeerFilter`) already deny it reads and
        // writes; this closes the residual gossip-chatter aperture. The
        // daemon is told only to drop a member — the decision that a
        // `Revoke` means "drop membership" stays here (ADR-003).
        // Idempotent + host-only daemon-side; best-effort spawn.
        let client = Arc::clone(&ctx.client);
        let sess = ctx.session;
        tokio::spawn(async move {
            if let Err(e) = publish_remove_member(&client, sess, peer).await {
                warn!(?e, ?peer, "evict: remove_member IPC failed");
            }
        });
    }
}

/// Apply one cap-listener [`Event`] to `peer_map` and trigger any
/// host-side upgrade delivery. See [`CapOutcome`] for what the return
/// value tells the loop.
///
/// Processes these message types:
/// - `MessageKind::Capability`: applies grant/revoke to the cap-set
///   projection so the docs gate starts rejecting revoked peers.
///   On the host side, an RW grant also triggers delivery of the
///   `NamespaceSecret` to the promoted peer, and a demote to Read a
///   downgrade notification (see [`handle_capability_message`]).
/// - `MessageKind::System` with `NODE_ID_ACTION`: registers the mapping
///   from a joiner's workspace `EndpointId` to their daemon `PeerId`. On
///   the host side, if the (re)announcing peer holds RW, also re-delivers
///   the current `NamespaceSecret` — the recovery path for a member that
///   was offline across a namespace rotation.
/// - `MessageKind::System` with `UPGRADE_ACTION`: on the joiner side,
///   imports the `NamespaceSecret` to gain Write capability.
/// - `MessageKind::System` with `DOWNGRADE_ACTION`: on the joiner side,
///   halts the watcher and emits [`WorkspaceEvent::Demoted`].
/// - `MessageKind::System` with `ROTATE_ACTION`: on the joiner side,
///   signals the rotation task to re-import the rotated namespace.
async fn handle_cap_event(
    ev: Event,
    session: SessionId,
    peer_map: &Arc<PeerMap>,
    host_ctx: Option<&HostUpgradeCtx>,
    joiner_ctx: Option<&JoinerUpgradeCtx>,
    events_tx: &mpsc::Sender<WorkspaceEvent>,
) -> CapOutcome {
    match ev {
        Event::Message {
            session: ev_session,
            message,
        } if ev_session == session => {
            let seq = message.seq;
            match message.kind {
                MessageKind::Capability => {
                    handle_capability_message(&message, peer_map, host_ctx, events_tx);
                }
                MessageKind::System if message.action == NODE_ID_ACTION => {
                    handle_node_id_message(&message, peer_map, host_ctx);
                }
                MessageKind::System if message.action == UPGRADE_ACTION => {
                    if let Some(ctx) = joiner_ctx
                        && message.peer.id == peer_map.host_peer_id()
                        && let Ok(payload) =
                            postcard::from_bytes::<UpgradePayload>(&message.payload)
                        && payload.target_peer == ctx.my_peer_id
                    {
                        let secret =
                            iroh_docs::NamespaceSecret::from_bytes(&payload.namespace_secret);
                        let cap = iroh_docs::Capability::Write(secret);
                        if let Err(e) = ctx.docs.import_namespace(cap).await {
                            warn!("workspace.upgrade import_namespace failed: {e}");
                        }
                    }
                }
                MessageKind::System if message.action == DOWNGRADE_ACTION => {
                    // Joiner: the host cooperatively demoted us (RW →
                    // Read). Halt our own watcher (voluntary write-stop)
                    // and surface a Demoted event. Verify host origin
                    // and that we are the target before acting.
                    if let Some(ctx) = joiner_ctx
                        && message.peer.id == peer_map.host_peer_id()
                        && let Ok(payload) =
                            postcard::from_bytes::<DowngradePayload>(&message.payload)
                        && payload.target_peer == ctx.my_peer_id
                    {
                        ctx.write_halted
                            .store(true, std::sync::atomic::Ordering::Relaxed);
                        emit_event(events_tx, WorkspaceEvent::Demoted);
                    }
                }
                MessageKind::System if message.action == ROTATE_ACTION => {
                    // Survivor: the host rotated the namespace and
                    // delivered the new Write ticket. Verify host origin
                    // and that we are the target, then hand off to the
                    // rotation task (the cap-listener has no
                    // `Arc<Workspace>`). Slice 3e.
                    if let Some(ctx) = joiner_ctx
                        && message.peer.id == peer_map.host_peer_id()
                        && let Ok(payload) = postcard::from_bytes::<RotatePayload>(&message.payload)
                        && payload.target_peer == ctx.my_peer_id
                        && let Err(e) = ctx.rotation_tx.send(RotationSignal::SurvivorRotate {
                            namespace_epoch: payload.namespace_epoch,
                            doc_ticket: payload.doc_ticket,
                        })
                    {
                        // Unbounded send (C4): only errors if the rotation
                        // task is gone, never on back-pressure.
                        warn!(?e, "rotation: failed to enqueue SurvivorRotate signal");
                    }
                }
                _ => {}
            }
            CapOutcome::Advanced(seq)
        }
        // Host: re-deliver the upgrade to a peer that
        // (re-)joins while already holding RW. Covers
        // the case where the original broadcast was
        // missed due to a network blip.
        Event::PeerJoined {
            session: ev_session,
            peer: joined_peer,
        } if ev_session == session => {
            if let Some(ctx) = host_ctx
                && peer_map.has_rw(joined_peer.id)
            {
                let client = Arc::clone(&ctx.client);
                let sess = ctx.session;
                // Read the *current* secret (refreshed on rotation, C1).
                let secret = *ctx.namespace_secret.lock().expect("upgrade_secret mutex");
                let peer = joined_peer.id;
                tokio::spawn(async move {
                    if let Err(e) = publish_upgrade(&client, sess, peer, secret).await {
                        warn!(?e, ?peer, "upgrade re-delivery on rejoin failed");
                    }
                });
            }
            CapOutcome::Ignored
        }
        // The daemon dropped events for this subscriber but kept the
        // connection open (M3 Part B). Tell the loop to re-Subscribe
        // from its watermark.
        Event::Gap {
            session: ev_session,
        } if ev_session == session => CapOutcome::Gap,
        _ => CapOutcome::Ignored,
    }
}

/// Fire-and-forget an in-band re-`Subscribe { since }` on `client` to
/// recover from an [`Event::Gap`] (M3 Part B).
///
/// **Must not be awaited from the loop.** A Gap arrives precisely
/// because the subscriber was draining slowly, so the client's events
/// channel may be near-full. The daemon answers a `Subscribe` by
/// spawning the replacement forwarder (which immediately pushes replay
/// frames) *and* a `Subscribed` response on the same connection; if the
/// loop blocked on `request().await` while being the sole drainer, the
/// reader could wedge on a full events channel with the response queued
/// behind replay frames. Spawning the request keeps the loop draining,
/// so replay flows and the response resolves on the spawned task.
///
/// The replayed events arrive on the *same* [`artel_client::EventStream`]
/// the loop already drains: the daemon's M4 forwarder-dedup replaces the
/// prior forwarder for this session on re-`Subscribe`, so there is no
/// new stream to swap in — unlike the EOF path.
fn spawn_gap_resubscribe(client: &Arc<Client>, session: SessionId, since: Option<Seq>) {
    let client = Arc::clone(client);
    tokio::spawn(async move {
        match client.request(Request::Subscribe { session, since }).await {
            Ok(Response::Subscribed { .. }) => {
                debug!(
                    target: "artel_fs::workspace",
                    ?session, ?since,
                    "cap-listener re-subscribed in-band after gap",
                );
            }
            Ok(other) => warn!(
                ?session,
                ?other,
                "cap-listener gap re-subscribe: unexpected reply"
            ),
            Err(e) => warn!(?session, %e, "cap-listener gap re-subscribe failed"),
        }
    });
}

/// Spawn a background task that drains session events into `peer_map`.
///
/// See [`handle_cap_event`] for the event types processed. The task owns
/// `client` (the connection it drains) so it can both reconnect on EOF
/// and re-`Subscribe` in-band on a gap.
///
/// **Lag recovery (M3):** when this subscriber falls more than the
/// broadcast capacity behind, the daemon makes the loss loud. Two
/// recovery paths, both resuming from the last-seen `seq` so the daemon
/// replays every logged message past the watermark:
///
/// - **Gap (Part B):** the daemon sends [`Event::Gap`] and keeps the
///   connection open. The loop re-`Subscribe`s in-band on the same
///   `client` (see [`spawn_gap_resubscribe`]); replayed + live events
///   resume on the same stream. The common case — no reconnect.
/// - **EOF (Part A):** if the connection actually drops (e.g. the daemon
///   restarted, or the Part-B gap send failed and it closed), `recv()`
///   yields `None`. The loop reconnects via [`cap_resubscribe`] on the
///   client's [`Client::socket_path`] with bounded
///   [`cap_reconnect_backoff`], swapping in the fresh client + stream.
///
/// Recovery is gap-free for the log-borne events this loop acts on:
/// `Capability` and `NODE_ID_ACTION` are replayed, and both
/// [`PeerMap::apply_capability`] and [`PeerMap::register`] are
/// idempotent. `UPGRADE_ACTION` / `DOWNGRADE_ACTION` / `ROTATE_ACTION`
/// (also handled here) are live-only synthetics, never store-appended
/// (the daemon's reserved-action invariant) — a delivery this loop
/// missed is recovered by host-side re-delivery (admission /
/// re-announce), not by replay. `PeerJoined`/`PeerLeft`/
/// `SessionClosed` are live-only and not replayed, but the only one
/// this loop acts on (`PeerJoined`, host-side) merely re-fires an
/// upgrade that the replayed `Capability::Grant` already re-fires — so
/// no separate membership reconciliation RPC is needed here.
///
/// After [`CAP_RECONNECT_MAX_ATTEMPTS`] failed reconnects the task
/// surfaces a [`WorkspaceError`] as a [`WorkspaceEvent::Error`] and
/// stops, so a genuinely-dead daemon doesn't spin forever.
///
/// Runs until `cancel` is triggered (from `Workspace::shutdown`).
#[allow(clippy::too_many_arguments)]
fn spawn_cap_listener(
    mut client: Arc<Client>,
    mut events: artel_client::EventStream,
    session: SessionId,
    peer_map: Arc<PeerMap>,
    cancel: CancellationToken,
    host_ctx: Option<HostUpgradeCtx>,
    joiner_ctx: Option<JoinerUpgradeCtx>,
    events_tx: mpsc::Sender<WorkspaceEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_seq: Option<Seq> = None;
        'outer: loop {
            // Drain the current stream until EOF or cancellation.
            loop {
                tokio::select! {
                    () = cancel.cancelled() => break 'outer,
                    ev = events.recv() => {
                        let Some(ev) = ev else {
                            // EOF — the connection actually dropped
                            // (daemon restart, or a Part-B gap send that
                            // failed and closed). Fall through to the
                            // reconnect loop.
                            break;
                        };
                        match handle_cap_event(ev, session, &peer_map, host_ctx.as_ref(), joiner_ctx.as_ref(), &events_tx)
                            .await
                        {
                            CapOutcome::Advanced(seq) => last_seq = Some(max_seq(last_seq, seq)),
                            // In-band gap: re-Subscribe on the SAME
                            // connection without dropping it. Replayed
                            // events arrive on this same stream.
                            CapOutcome::Gap => spawn_gap_resubscribe(&client, session, last_seq),
                            CapOutcome::Ignored => {}
                        }
                    }
                }
            }

            // Reconnect + re-Subscribe { since: last_seq } with bounded
            // backoff. Retry on BOTH connect and subscribe failure so a
            // transient error (e.g. UnknownSession while the host
            // re-hosts) is rideable.
            let mut attempt = 0u32;
            loop {
                let Some(delay) = cap_reconnect_backoff(attempt) else {
                    let msg = format!(
                        "cap-listener: giving up after {CAP_RECONNECT_MAX_ATTEMPTS} reconnect \
                         attempts; peer/capability-change events will no longer be observed",
                    );
                    warn!(?session, "{msg}");
                    emit_event(&events_tx, WorkspaceEvent::Error(msg));
                    break 'outer;
                };
                tokio::select! {
                    () = cancel.cancelled() => break 'outer,
                    () = tokio::time::sleep(delay) => {}
                }
                match cap_resubscribe(client.socket_path(), session, last_seq).await {
                    Ok((fresh_client, fresh_events)) => {
                        debug!(
                            target: "artel_fs::workspace",
                            ?session, ?last_seq, attempt,
                            "cap-listener reconnected after stream close",
                        );
                        client = fresh_client;
                        events = fresh_events;
                        continue 'outer;
                    }
                    Err(e) => {
                        debug!(
                            target: "artel_fs::workspace",
                            ?session, attempt, %e,
                            "cap-listener reconnect attempt failed; backing off",
                        );
                        attempt += 1;
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

    /// Token hierarchy (Slice 3a): the doc-scoped token is a child of
    /// the shutdown token, so cancelling the parent (workspace
    /// shutdown) cancels the doc token — but cancelling the doc token
    /// (namespace-rotation re-import) leaves a *sibling* cap-listener
    /// token, also a child of the parent, untouched. This is what lets
    /// re-import tear down the watcher/applier without killing the
    /// cap-listener that carries the rotation-bump signal.
    #[test]
    fn doc_token_is_child_of_shutdown_but_sibling_of_cap_listener() {
        let shutdown = CancellationToken::new();
        let doc_token = shutdown.child_token();
        let cap_token = shutdown.child_token();

        // Cancelling the doc token must NOT cancel the cap-listener
        // sibling or the parent.
        doc_token.cancel();
        assert!(doc_token.is_cancelled());
        assert!(
            !cap_token.is_cancelled(),
            "cap-listener token must survive a doc-token reset (rotation re-import)",
        );
        assert!(!shutdown.is_cancelled(), "parent must survive a doc reset");

        // Cancelling the parent (workspace shutdown) cancels the
        // cap-listener sibling too.
        shutdown.cancel();
        assert!(
            cap_token.is_cancelled(),
            "workspace shutdown must take the cap-listener down",
        );
    }

    #[test]
    fn max_seq_tracks_highest_and_ignores_synthetic_zero() {
        // Starting empty, the first real seq becomes the watermark.
        assert_eq!(max_seq(None, Seq::new(5)), Seq::new(5));
        // A higher seq advances it.
        assert_eq!(max_seq(Some(Seq::new(5)), Seq::new(9)), Seq::new(9));
        // A lower (or replayed) seq never regresses the watermark.
        assert_eq!(max_seq(Some(Seq::new(9)), Seq::new(3)), Seq::new(9));
        // A `Seq::ZERO` synthetic (e.g. the re-injected ticket) can't
        // pull an established watermark back to zero.
        assert_eq!(max_seq(Some(Seq::new(9)), Seq::ZERO), Seq::new(9));
    }

    #[test]
    fn cap_reconnect_backoff_grows_then_caps_then_gives_up() {
        // Attempt 0 is the first retry delay; delays grow monotonically
        // until they saturate at the cap, then the ceiling returns None
        // so a genuinely-dead daemon doesn't spin forever.
        let mut prev = Duration::ZERO;
        let mut saw_cap = false;
        for attempt in 0..CAP_RECONNECT_MAX_ATTEMPTS {
            let d = cap_reconnect_backoff(attempt).expect("under ceiling => Some");
            assert!(
                d >= prev,
                "backoff must be monotonic non-decreasing: attempt {attempt} gave {d:?} < {prev:?}",
            );
            assert!(
                d <= CAP_RECONNECT_MAX_DELAY,
                "backoff must never exceed the cap: {d:?}",
            );
            if d == CAP_RECONNECT_MAX_DELAY {
                saw_cap = true;
            }
            prev = d;
        }
        assert!(
            saw_cap,
            "backoff should saturate at the cap before the ceiling"
        );
        // At and past the ceiling: give up.
        assert_eq!(cap_reconnect_backoff(CAP_RECONNECT_MAX_ATTEMPTS), None);
        assert_eq!(cap_reconnect_backoff(CAP_RECONNECT_MAX_ATTEMPTS + 7), None);
    }

    #[tokio::test]
    async fn handle_cap_event_classifies_gap() {
        // M3 Part B: an Event::Gap for our session asks the loop to
        // re-Subscribe (CapOutcome::Gap); a Gap for a different session
        // is ignored; a Message advances the watermark.
        let session = SessionId::from_bytes([1; 16]);
        let other = SessionId::from_bytes([2; 16]);
        let peer_map = Arc::new(PeerMap::new(PeerId::from_bytes([0; 32])));
        let (events_tx, _events_rx) = mpsc::channel(EVENT_BUFFER);

        let out = handle_cap_event(
            Event::Gap { session },
            session,
            &peer_map,
            None,
            None,
            &events_tx,
        )
        .await;
        assert!(matches!(out, CapOutcome::Gap));

        let out = handle_cap_event(
            Event::Gap { session: other },
            session,
            &peer_map,
            None,
            None,
            &events_tx,
        )
        .await;
        assert!(matches!(out, CapOutcome::Ignored));

        let out = handle_cap_event(
            Event::SessionClosed { session },
            session,
            &peer_map,
            None,
            None,
            &events_tx,
        )
        .await;
        assert!(matches!(out, CapOutcome::Ignored));
    }

    /// `handle_node_id_message` registers the announcing peer's
    /// workspace-id -> daemon-id mapping. With `host_ctx = None` (the
    /// joiner side, or any non-host) there is no secret re-delivery — we
    /// pin the registration half here, which is observable without a live
    /// `Client`. The host-side RW-gated re-delivery is exercised
    /// end-to-end in the real-n0 integration suite (a spawned unicast
    /// needs a real daemon).
    #[test]
    fn node_id_message_registers_mapping() {
        let host = PeerId::from_bytes([0xa0; 32]);
        let peer_map = Arc::new(PeerMap::new(host));

        // A returning peer's daemon id + its workspace endpoint id.
        // The workspace id must be a valid ed25519 public key, so derive
        // it from a signing key rather than using arbitrary bytes.
        let peer_daemon = PeerId::from_bytes([0xb0; 32]);
        let ws_key = artel_protocol::signing::SigningKey::from_bytes(&[0xc0; 32]);
        let workspace_id =
            iroh::EndpointId::from_bytes(&ws_key.verifying_key().to_bytes()).unwrap();

        // Before the NODE_ID announce the workspace id is unresolvable.
        assert_eq!(
            peer_map.classify_author(workspace_id),
            crate::peer_map::AuthorDisposition::Unresolvable,
        );

        let msg = SessionMessage::new(
            Seq::new(1),
            1,
            artel_protocol::PeerInfo::new(peer_daemon, "bob"),
            MessageKind::System,
            NODE_ID_ACTION,
            workspace_id.as_bytes().to_vec(),
            artel_protocol::message::SIGNATURE_UNSIGNED,
            artel_protocol::message::SIGNATURE_UNSIGNED,
        );

        // host_ctx None: registers the mapping, no delivery side-effect.
        handle_node_id_message(&msg, &peer_map, None);

        // Mapping now resolves. The peer holds no cap yet, so it's NotRw
        // (resolvable but not RW) — proving the register landed without a
        // grant having to arrive first.
        assert_eq!(
            peer_map.classify_author(workspace_id),
            crate::peer_map::AuthorDisposition::NotRw,
        );

        // Grant RW, re-announce: now classified RW — the precise
        // precondition the host-side re-delivery gates on (`has_rw` after
        // register).
        peer_map.apply_capability(host, &grant_rw_payload(peer_daemon));
        handle_node_id_message(&msg, &peer_map, None);
        assert_eq!(
            peer_map.classify_author(workspace_id),
            crate::peer_map::AuthorDisposition::Rw,
        );
        assert!(peer_map.has_rw(peer_daemon));
    }

    /// Encode a host-authored `Grant{peer, ReadWrite}` capability payload.
    fn grant_rw_payload(peer: PeerId) -> Vec<u8> {
        use artel_protocol::capability::{Capability, CapabilityAction};
        CapabilityAction::Grant {
            peer,
            cap: Capability::ReadWrite,
        }
        .encode()
    }

    /// Build a `MessageKind::Capability` session message authored by
    /// `author` carrying `payload`.
    fn capability_message(author: PeerId, payload: Vec<u8>) -> SessionMessage {
        SessionMessage::new(
            Seq::new(1),
            1,
            artel_protocol::PeerInfo::new(author, "test"),
            MessageKind::Capability,
            "capability",
            payload,
            artel_protocol::message::SIGNATURE_UNSIGNED,
            artel_protocol::message::SIGNATURE_UNSIGNED,
        )
    }

    // ---- PeerRevoked event emission ----

    #[test]
    fn host_revoke_emits_peer_revoked_event() {
        use artel_protocol::capability::CapabilityAction;
        let host = PeerId::from_bytes([0xa0; 32]);
        let peer = PeerId::from_bytes([0xb0; 32]);
        let peer_map = Arc::new(PeerMap::new(host));
        let (events_tx, mut events_rx) = mpsc::channel(EVENT_BUFFER);

        peer_map.apply_capability(host, &grant_rw_payload(peer));
        let revoke = capability_message(host, CapabilityAction::Revoke { peer }.encode());
        handle_capability_message(&revoke, &peer_map, None, &events_tx);

        match events_rx.try_recv() {
            Ok(WorkspaceEvent::PeerRevoked { peer: got }) => assert_eq!(got, peer),
            other => panic!("expected PeerRevoked, got {other:?}"),
        }
        // And the projection actually blocks: the peer is revoked.
        assert!(!peer_map.has_rw(peer));
    }

    #[test]
    fn non_host_revoke_emits_nothing() {
        use artel_protocol::capability::CapabilityAction;
        let host = PeerId::from_bytes([0xa0; 32]);
        let peer = PeerId::from_bytes([0xb0; 32]);
        let impostor = PeerId::from_bytes([0x99; 32]);
        let peer_map = Arc::new(PeerMap::new(host));
        let (events_tx, mut events_rx) = mpsc::channel(EVENT_BUFFER);

        peer_map.apply_capability(host, &grant_rw_payload(peer));
        let revoke = capability_message(impostor, CapabilityAction::Revoke { peer }.encode());
        handle_capability_message(&revoke, &peer_map, None, &events_tx);

        // The impostor's revoke was ignored by the projection, so no
        // "peer is out" signal may fire — the gates won't block them.
        assert!(events_rx.try_recv().is_err());
        assert!(peer_map.has_rw(peer));
    }

    #[test]
    fn host_grant_emits_no_peer_revoked_event() {
        let host = PeerId::from_bytes([0xa0; 32]);
        let peer = PeerId::from_bytes([0xb0; 32]);
        let peer_map = Arc::new(PeerMap::new(host));
        let (events_tx, mut events_rx) = mpsc::channel(EVENT_BUFFER);

        let grant = capability_message(host, grant_rw_payload(peer));
        handle_capability_message(&grant, &peer_map, None, &events_tx);

        assert!(events_rx.try_recv().is_err());
        assert!(peer_map.has_rw(peer));
    }

    // ---- NODE_ID re-delivery de-storm (finding #4) ----

    #[test]
    fn claim_redelivery_first_time_proceeds_then_dedups() {
        let map = std::sync::Mutex::new(std::collections::HashMap::new());
        let peer = PeerId::from_bytes([0xd0; 32]);

        // First announce at epoch 2: proceed (no prior delivery).
        assert!(claim_redelivery_epoch(&map, peer, 2));
        // A replayed announce at the same epoch: suppressed.
        assert!(!claim_redelivery_epoch(&map, peer, 2));
        // And at a *lower* epoch (a stale replay): also suppressed.
        assert!(!claim_redelivery_epoch(&map, peer, 1));
    }

    #[test]
    fn claim_redelivery_proceeds_on_higher_epoch() {
        // A genuine recovery: the peer was delivered epoch 1, then a
        // rotation bumped to epoch 2. The next announce must proceed so the
        // returning member gets the new namespace — the dedup must not
        // mistake "delivered epoch 1" for "current".
        let map = std::sync::Mutex::new(std::collections::HashMap::new());
        let peer = PeerId::from_bytes([0xd1; 32]);
        assert!(claim_redelivery_epoch(&map, peer, 1));
        assert!(claim_redelivery_epoch(&map, peer, 2));
        assert!(!claim_redelivery_epoch(&map, peer, 2));
    }

    #[test]
    fn rollback_redelivery_restores_retry_after_failure() {
        // A failed delivery rolls the claim back so the next announce
        // retries — a transient failure must never durably suppress a
        // genuine re-delivery (that would strand the member on a stale
        // namespace, the bug this feature prevents).
        let map = std::sync::Mutex::new(std::collections::HashMap::new());
        let peer = PeerId::from_bytes([0xd2; 32]);
        assert!(claim_redelivery_epoch(&map, peer, 3));
        rollback_redelivery_epoch(&map, peer, 3);
        // Retry now proceeds again.
        assert!(claim_redelivery_epoch(&map, peer, 3));
    }

    #[test]
    fn rollback_redelivery_does_not_clobber_a_newer_delivery() {
        // If a higher-epoch delivery overtook us between claim and the
        // failed-delivery rollback, the rollback must leave that newer
        // progress intact rather than wiping the entry.
        let map = std::sync::Mutex::new(std::collections::HashMap::new());
        let peer = PeerId::from_bytes([0xd3; 32]);
        assert!(claim_redelivery_epoch(&map, peer, 1));
        // A concurrent newer delivery lands at epoch 2.
        assert!(claim_redelivery_epoch(&map, peer, 2));
        // The epoch-1 task now fails and tries to roll back — must be a
        // no-op, since the live mark is the newer epoch 2.
        rollback_redelivery_epoch(&map, peer, 1);
        // Epoch 2 is still claimed: a replay at 2 stays suppressed.
        assert!(!claim_redelivery_epoch(&map, peer, 2));
    }

    /// End-to-end recovery proof (M3): a cap-listener whose connection
    /// is torn down (here: the daemon stops and a fresh one resumes the
    /// hosted session on the same socket + sessions dir) reconnects,
    /// re-`Subscribe`s from its last-seen seq, and applies a capability
    /// grant that was issued *after* the disconnect.
    ///
    /// Without the reconnect loop this is red: the listener's
    /// `events.recv()` yields `None` on the daemon stop and the task
    /// exits, so the post-restart grant is never observed and
    /// `has_rw(promoted_after_restart)` stays false until the timeout.
    ///
    /// Pure IPC — no iroh data-plane sync — so it's deterministic: the
    /// daemon reloads the hosted session from disk, the post-restart
    /// grant lands in the log, and the reconnect's
    /// `Subscribe { since: last_seq }` replays it. The peer-map is
    /// idempotent, so even a replay of the pre-restart grant is benign.
    #[tokio::test(flavor = "multi_thread")]
    #[allow(clippy::too_many_lines)]
    async fn cap_listener_recovers_grant_after_daemon_restart() {
        use std::sync::Arc;

        use artel_client::Client;
        use artel_daemon::shutdown::Shutdown;
        use artel_daemon::{Daemon, DaemonConfig, EndpointSetup as DaemonEndpointSetup};
        use artel_protocol::capability::{Capability, CapabilityAction};
        use iroh::test_utils::DnsPkarrServer;

        async fn spawn_daemon(
            socket: &Path,
            pid: &Path,
            sessions: &Path,
            iroh_key: &Path,
            dns_pkarr: Arc<DnsPkarrServer>,
        ) -> (Arc<Shutdown>, tokio::task::JoinHandle<std::io::Result<()>>) {
            let daemon = Daemon::start(DaemonConfig {
                socket_path: socket.to_path_buf(),
                pid_path: pid.to_path_buf(),
                sessions_dir: sessions.to_path_buf(),
                iroh_key_path: Some(iroh_key.to_path_buf()),
                endpoint_setup: DaemonEndpointSetup::Testing { dns_pkarr },
            })
            .await
            .expect("daemon start");
            let shutdown = daemon.shutdown_handle();
            let join = tokio::spawn(daemon.run());
            (shutdown, join)
        }

        // Poll `has_rw(peer)` until true or a bounded deadline.
        async fn wait_has_rw(peer_map: &Arc<PeerMap>, peer: PeerId, what: &str) {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            while !peer_map.has_rw(peer) {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "cap-listener never observed RW for {what}",
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        let dns_pkarr = Arc::new(
            DnsPkarrServer::run_with_origin(artel_fs::TEST_DNS_ORIGIN.to_string())
                .await
                .expect("DnsPkarrServer::run"),
        );
        let dir = TempDir::new().unwrap();
        let socket = dir.path().join("daemon.sock");
        let pid = dir.path().join("daemon.pid");
        let sessions = dir.path().join("sessions");
        let iroh_key = dir.path().join("iroh.key");

        // --- Phase 1: daemon up, host a session, attach a cap-listener.
        let (shutdown1, join1) =
            spawn_daemon(&socket, &pid, &sessions, &iroh_key, Arc::clone(&dns_pkarr)).await;

        let client = Client::connect(&socket).await.expect("connect");
        let session = match client
            .request(Request::HostSession {
                display_name: "host".into(),
                session: None,
            })
            .await
            .expect("host")
        {
            Response::HostSession { session, .. } => session,
            other => panic!("unexpected HostSession reply: {other:?}"),
        };
        let host_peer = client.daemon_peer_id();

        let peer_map = Arc::new(PeerMap::new(host_peer));
        let cancel = CancellationToken::new();
        // Sink for the give-up WorkspaceError; we don't expect one here.
        let (ev_tx, _ev_rx) = mpsc::channel(EVENT_BUFFER);
        let listener = spawn_cap_listener_from_socket(
            Some(socket.as_path()),
            session,
            Arc::clone(&peer_map),
            cancel.child_token(),
            None,
            None,
            ev_tx,
        )
        .await
        .expect("cap-listener spawn");

        // Grant RW to a first peer; the listener must apply it.
        let peer_before = PeerId::from_bytes([7; 32]);
        grant_rw(&client, session, peer_before).await;
        wait_has_rw(&peer_map, peer_before, "peer granted before restart").await;

        // --- Phase 2: stop the daemon. The listener sees EOF and
        // enters its reconnect loop.
        drop(client);
        shutdown1.trigger();
        timeout(Duration::from_secs(10), join1)
            .await
            .expect("daemon 1 exit")
            .expect("daemon 1 join")
            .expect("daemon 1 io");

        // --- Phase 3: bring a fresh daemon up on the same socket +
        // sessions dir. It reloads the hosted session from disk.
        let (shutdown2, join2) =
            spawn_daemon(&socket, &pid, &sessions, &iroh_key, Arc::clone(&dns_pkarr)).await;

        // A fresh client re-hosts (resume) to gain send membership on
        // its connection, then grants RW to a second peer — issued
        // entirely after the disconnect.
        let client2 = Client::connect(&socket).await.expect("reconnect client");
        match client2
            .request(Request::HostSession {
                display_name: "host".into(),
                session: Some(session),
            })
            .await
            .expect("re-host")
        {
            Response::HostSession { session: s, .. } => assert_eq!(s, session),
            other => panic!("unexpected re-host reply: {other:?}"),
        }
        let peer_after = PeerId::from_bytes([9; 32]);
        let grant = CapabilityAction::Grant {
            peer: peer_after,
            cap: Capability::ReadWrite,
        };
        match client2
            .request(Request::Send {
                session,
                payload: SendPayload {
                    kind: MessageKind::Capability,
                    action: grant.action_str().to_string(),
                    payload: grant.encode(),
                },
            })
            .await
            .expect("post-restart grant")
        {
            Response::Sent { .. } => {}
            other => panic!("unexpected Send reply: {other:?}"),
        }

        // The payoff: the reconnected listener applies the
        // post-restart grant. Red without the reconnect loop.
        wait_has_rw(&peer_map, peer_after, "peer granted after restart").await;

        // Cleanup.
        cancel.cancel();
        let _ = timeout(Duration::from_secs(5), listener).await;
        drop(client2);
        shutdown2.trigger();
        let _ = timeout(Duration::from_secs(10), join2).await;
    }

    /// Helper mirroring the test-suite `grant_rw`: host authors a
    /// `Capability::Grant` for `target`.
    async fn grant_rw(client: &artel_client::Client, session: SessionId, target: PeerId) {
        use artel_protocol::capability::{Capability, CapabilityAction};
        let grant = CapabilityAction::Grant {
            peer: target,
            cap: Capability::ReadWrite,
        };
        match client
            .request(Request::Send {
                session,
                payload: SendPayload {
                    kind: MessageKind::Capability,
                    action: grant.action_str().to_string(),
                    payload: grant.encode(),
                },
            })
            .await
            .expect("grant send")
        {
            Response::Sent { .. } => {}
            other => panic!("unexpected Send reply: {other:?}"),
        }
    }

    /// Drain `events` until the next session [`Event::Message`] (with
    /// `seq > after` when `after` is set), returning its seq. Bounded so
    /// a missing message fails loudly rather than hanging the test.
    async fn next_message_seq(
        events: &mut artel_client::EventStream,
        after: Option<Seq>,
        what: &str,
    ) -> Seq {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Event::Message { message, .. } = events.recv().await.expect("stream open")
                    && after.is_none_or(|a| message.seq > a)
                {
                    break message.seq;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what} did not arrive in time"))
    }

    /// In-band gap recovery (M3 Part B): `spawn_gap_resubscribe` issues a
    /// `Subscribe { since: last_seq }` on the *same* connection the
    /// listener already holds, and the daemon replays every logged
    /// message past the watermark back onto that same event stream — no
    /// reconnect, no new socket.
    ///
    /// We drive it directly (rather than forcing a real 256-event lag,
    /// which is inherently racy) because the daemon's Gap *emission* is
    /// already covered deterministically by
    /// `server::forwarder_set_tests::forwarder_sends_gap_on_subscriber_lag`,
    /// and the loop's Gap *classification* by
    /// `handle_cap_event_classifies_gap`. This test pins the third leg:
    /// that the in-band re-Subscribe actually backfills the gap.
    #[tokio::test(flavor = "multi_thread")]
    async fn gap_resubscribe_replays_missed_messages_in_band() {
        use std::sync::Arc;

        use artel_client::Client;
        use artel_daemon::{Daemon, DaemonConfig, EndpointSetup as DaemonEndpointSetup};
        use iroh::test_utils::DnsPkarrServer;

        let dns_pkarr = Arc::new(
            DnsPkarrServer::run_with_origin(artel_fs::TEST_DNS_ORIGIN.to_string())
                .await
                .expect("DnsPkarrServer::run"),
        );
        let dir = TempDir::new().unwrap();
        let socket = dir.path().join("daemon.sock");
        let daemon = Daemon::start(DaemonConfig {
            socket_path: socket.clone(),
            pid_path: dir.path().join("daemon.pid"),
            sessions_dir: dir.path().join("sessions"),
            iroh_key_path: Some(dir.path().join("iroh.key")),
            endpoint_setup: DaemonEndpointSetup::Testing { dns_pkarr },
        })
        .await
        .expect("daemon start");
        let shutdown = daemon.shutdown_handle();
        let join = tokio::spawn(daemon.run());

        let client = Client::connect(&socket).await.expect("connect");
        let session = match client
            .request(Request::HostSession {
                display_name: "host".into(),
                session: None,
            })
            .await
            .expect("host")
        {
            Response::HostSession { session, .. } => session,
            other => panic!("unexpected HostSession reply: {other:?}"),
        };

        // The listener's connection: subscribe and take the stream.
        let (cap_client, mut events) = cap_resubscribe(&socket, session, None)
            .await
            .expect("subscribe");

        // Drain a first grant live; pin the watermark *at* its seq. This
        // is the "last seq processed before the gap" — the resume point.
        grant_rw(&client, session, PeerId::from_bytes([7; 32])).await;
        let watermark = next_message_seq(&mut events, None, "first grant").await;

        // A second grant. We drain its LIVE copy here so it's no longer
        // pending on the stream — modelling a message that, in a real
        // gap, would have been dropped. The watermark stays at the first
        // seq, so re-Subscribe { since: watermark } must REPLAY this one.
        grant_rw(&client, session, PeerId::from_bytes([9; 32])).await;
        let second_seq = next_message_seq(&mut events, Some(watermark), "second grant live").await;

        // Gap-recovery action: re-Subscribe in-band from the watermark on
        // the SAME connection. With the live copy already drained, the
        // only way the second grant reappears is the daemon's replay of
        // `seq > watermark` — which is exactly the gap-recovery contract.
        spawn_gap_resubscribe(&cap_client, session, Some(watermark));

        let replayed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Event::Message { message, .. } = events.recv().await.expect("stream open")
                    && message.seq == second_seq
                {
                    break message;
                }
            }
        })
        .await
        .expect("in-band replay must re-deliver the missed message past the watermark");

        let decoded = artel_protocol::capability::CapabilityAction::decode(&replayed.payload)
            .expect("capability action");
        assert!(
            matches!(
                decoded,
                artel_protocol::capability::CapabilityAction::Grant { peer, .. }
                    if peer == PeerId::from_bytes([9; 32])
            ),
            "replayed message should be the post-watermark grant, got {decoded:?}",
        );

        drop(client);
        drop(cap_client);
        shutdown.trigger();
        let _ = tokio::time::timeout(Duration::from_secs(10), join).await;
    }

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

    /// `emit_event` delivers to a receiver with spare capacity.
    #[tokio::test]
    async fn emit_event_delivers_when_capacity_available() {
        let (tx, mut rx) = mpsc::channel(4);
        emit_event(
            &tx,
            WorkspaceEvent::PeerWrote {
                path: PathBuf::from("/w/a.txt"),
            },
        );
        match rx.recv().await {
            Some(WorkspaceEvent::PeerWrote { path }) => {
                assert_eq!(path, PathBuf::from("/w/a.txt"));
            }
            other => panic!("expected PeerWrote, got {other:?}"),
        }
    }

    /// The load-bearing property: a full channel makes `emit_event`
    /// drop the event and return immediately rather than block. This
    /// is what stops the applier from parking in `send().await` and
    /// back-pressuring iroh-docs' live actor into a sync-wide freeze.
    /// The synchronous body asserts non-blocking: if `emit_event`
    /// awaited, this test would never reach the assertion.
    #[tokio::test]
    async fn emit_event_drops_when_channel_full_without_blocking() {
        let (tx, mut rx) = mpsc::channel(1);
        // Fill the single slot.
        emit_event(&tx, WorkspaceEvent::Error("first".into()));
        // Channel is now full; this must drop, not block.
        emit_event(&tx, WorkspaceEvent::Error("dropped".into()));
        // Drain: only the first event is present, the second was
        // dropped on the floor.
        assert!(matches!(
            rx.try_recv(),
            Ok(WorkspaceEvent::Error(ref m)) if m == "first"
        ));
        assert!(
            rx.try_recv().is_err(),
            "second event must have been dropped, not queued",
        );
    }

    /// A closed receiver makes `emit_event` a no-op — the workspace
    /// keeps replicating after the consumer drops its stream.
    #[tokio::test]
    async fn emit_event_is_noop_when_receiver_dropped() {
        let (tx, rx) = mpsc::channel(4);
        drop(rx);
        // Must not panic or block.
        emit_event(
            &tx,
            WorkspaceEvent::PeerDeleted {
                path: PathBuf::from("/w/gone.txt"),
            },
        );
    }
}
