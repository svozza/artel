//! In-memory session registry and RPC handlers.
//!
//! A [`Registry`] owns every active session by ID. Each [`Session`]
//! holds an ordered message log (host-sequenced), a member set, and a
//! `broadcast::Sender<Event>` for live subscribers. RPC handlers are
//! methods on `Registry`; they take peer info as an argument so the
//! transport layer can supply it (peer identity comes from the IPC
//! handshake rather than the message).
//!
//! [`JoinTicket`]s emitted here use the `artel:` text format defined
//! in [`artel_protocol::ticket`]. Phase 2c will extend the payload
//! with iroh `NodeAddr` and topic info; today the ticket carries the
//! session id and the host daemon's peer id, which is enough for a
//! local-only daemon to route a join request and rejects all
//! pre-2b ticket forms.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use artel_protocol::capability::{Capability, CapabilityAction};
use artel_protocol::ids::TicketId;
use artel_protocol::message::{MESSAGE_FORMAT, SIGNATURE_UNSIGNED, SigBytes};
use artel_protocol::signing::{self, verify_reason};
use artel_protocol::ticket::{self, SessionTicket, TicketEntry, TicketStatus, WireEndpointAddr};
use artel_protocol::{
    Event, JoinTicket, MessageKind, PeerId, PeerInfo, ProtocolError, Seq, SessionId,
    SessionMessage, SessionSummary, UpgradePayload,
};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, broadcast};

use crate::store::{DynStore, SessionKind, SessionRecord, StoredAttachment};

/// Capacity of the per-session broadcast channel.
///
/// Slow subscribers that lag by more than this lose old events; the
/// transport surfaces that to the client as a message gap (which the
/// client can recover from with a `Subscribe { since }`). This is the
/// right shape — we do not want to back-pressure publishers because of
/// one slow subscriber.
const EVENT_CHANNEL_CAPACITY: usize = 256;

use artel_protocol::{TICKET_ACTION, UPGRADE_ACTION};

/// Wall-clock millis since the Unix epoch. The `Local` arm of
/// [`Authoring`] stamps this onto every body it signs.
fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Errors the registry may return from RPC handlers.
#[derive(Debug, Error)]
pub enum SessionError {
    /// The referenced session does not exist.
    #[error("unknown session: {0}")]
    UnknownSession(SessionId),

    /// The peer is not a member of the session.
    #[error("not a member of session: {0}")]
    NotMember(SessionId),

    /// Join ticket malformed or revoked.
    #[error("invalid join ticket")]
    InvalidTicket,

    /// Backing storage failed. The in-memory state was not changed.
    #[error("storage: {0}")]
    Storage(#[source] std::io::Error),

    /// Ticket carried a host addr the daemon couldn't parse.
    #[error("invalid host address in ticket: {0}")]
    InvalidAddr(String),

    /// Internal failure inside iroh gossip plumbing. Surfaces over
    /// the wire as `ProtocolError::Internal` — the joiner gets a
    /// generic error rather than iroh-specific detail.
    #[error("internal: {0}")]
    Internal(String),

    /// `Send` issued for a session whose host is a different
    /// daemon AND the daemon is built without the `iroh` feature
    /// (so there's no transport to forward through). With `iroh`
    /// on, joiner sends are routed through the gossip bridge.
    #[error("send is only supported on the host side in this build")]
    NotHost,

    /// `Registry::host(peer, Some(id))` was issued for an `id`
    /// that exists locally but with a different host or as a
    /// remote-mirror session. The caller is asking to resume a
    /// session they don't own. Maps to
    /// [`artel_protocol::ProtocolError::SessionConflict`] over
    /// the wire.
    #[error("session id {0} already exists with a different host or kind")]
    SessionConflict(SessionId),

    /// A joiner-side `Send` that we forwarded to the host over
    /// gossip came back with a wire-form rejection. The wrapped
    /// [`artel_protocol::ProtocolError`] is what the host
    /// authoritatively decided; we forward it verbatim to the
    /// IPC client so they see the host's actual reason rather than
    /// a flattened `Internal` shrug.
    #[error("host rejected send: {0}")]
    HostRejected(#[source] artel_protocol::ProtocolError),

    /// A remote-authored `SendRequest` reached `Registry::send` with
    /// a signature that does not verify against the body. The host
    /// (or any other receiver) must drop the message rather than
    /// append; the joiner sees this as
    /// [`artel_protocol::ProtocolError::Signature`] in their
    /// `SendAck`. See `crate::session::Authoring::Remote` and Auth
    /// Slice B2.
    #[error("signature rejected for peer {peer_id}: {reason}")]
    SignatureRejected {
        /// The body's `peer.id` — i.e. who claimed authorship.
        peer_id: PeerId,
        /// Diagnostic reason. Names the failure mode (sentinel /
        /// bad key / bad sig). Never includes the bytes of the
        /// rejected signature.
        reason: String,
    },

    /// The authoring peer lacked the capability required to author a
    /// message at its seq (Auth Slice C / L2). For a non-`Capability`
    /// message that means `peer` did not hold `ReadWrite`; for a
    /// `Capability` grant/revoke it means the author did not hold
    /// `ReadWrite` (the right to grant rides on `ReadWrite`, brainstorm
    /// Q2). The host rejects at `send` (the message never gets a seq);
    /// the joiner mirror drops+logs the same way. Maps to
    /// [`artel_protocol::ProtocolError::Capability`] over the wire on
    /// the host-side `SendRequest` rejection path. `had`/`needed` name
    /// the cap gap for telemetry (Q5) and never leak payload bytes.
    #[error("capability denied for peer {peer_id}: had {had:?}, needs {needed:?}")]
    CapabilityDenied {
        /// The author whose write was denied.
        peer_id: PeerId,
        /// The capability they held at that seq (`None` = absent ⇒
        /// the `Read` floor).
        had: Option<Capability>,
        /// The capability the action required.
        needed: Capability,
    },

    /// The ticket's `expiry_ms` is in the past at admission time.
    #[error("ticket expired")]
    TicketExpired,

    /// The ticket's `cap_sig` did not verify against the host's pubkey,
    /// or the claim was otherwise malformed (sentinel signature, bad
    /// key). The string carries a diagnostic reason.
    #[error("invalid cap claim: {0}")]
    InvalidCapClaim(String),

    /// The claim's `ticket_id` is not admissible against the host's
    /// issued-ticket ledger: either explicitly revoked, or absent
    /// (issued-only, fail closed — a sig-valid claim whose id the
    /// ledger never saw means a pre-cutover ticket, a rolled-back
    /// ledger, or a forge with a stolen signing key; all reject), or
    /// present but disagreeing with the claimed cap/expiry. One
    /// variant for all three on purpose: it maps to the joiner-opaque
    /// [`artel_protocol::ProtocolError::InvalidTicket`], and
    /// distinguishing them for the bearer would oracle ledger
    /// contents to anyone holding a leaked ticket.
    #[error("ticket not admissible")]
    TicketNotAdmissible,

    /// `revoke_ticket` named an id that was never issued for the
    /// session. Host-operator-facing (maps to
    /// [`artel_protocol::ProtocolError::UnknownTicket`]): reporting
    /// success would falsely reassure the caller a leaked ticket is
    /// dead.
    #[error("ticket {0} was never issued for this session")]
    UnknownTicket(TicketId),

    /// A `Send` carried a reserved daemon-injected System action
    /// (`workspace.ticket` / `workspace.upgrade`). These actions are
    /// minted only by the daemon as synthetic, unsigned, off-log
    /// messages from unicast-delivered capability material — they are
    /// never authored by a member. Rejecting at the single sequencing
    /// chokepoint (`Registry::send`) keeps a forged broadcast off the
    /// log entirely, so it can't reach the host's live IPC fan-out (a
    /// co-located joiner-mode workspace would otherwise import it) nor
    /// any log-derived replay surface. The `&'static str` is the
    /// rejected action for diagnostics.
    #[error("reserved system action may not be sent by a member: {0}")]
    ReservedAction(&'static str),
}

// io::Error doesn't impl PartialEq, so we hand-roll one for the
// Storage-free variants tests rely on.
impl PartialEq for SessionError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::UnknownSession(a), Self::UnknownSession(b))
            | (Self::NotMember(a), Self::NotMember(b))
            | (Self::SessionConflict(a), Self::SessionConflict(b)) => a == b,
            (Self::Storage(a), Self::Storage(b)) => a.kind() == b.kind(),
            (Self::InvalidAddr(a), Self::InvalidAddr(b))
            | (Self::Internal(a), Self::Internal(b))
            | (Self::InvalidCapClaim(a), Self::InvalidCapClaim(b)) => a == b,
            (Self::HostRejected(a), Self::HostRejected(b)) => a == b,
            (
                Self::SignatureRejected {
                    peer_id: a_peer,
                    reason: a_reason,
                },
                Self::SignatureRejected {
                    peer_id: b_peer,
                    reason: b_reason,
                },
            ) => a_peer == b_peer && a_reason == b_reason,
            (
                Self::CapabilityDenied {
                    peer_id: a_peer,
                    had: a_had,
                    needed: a_needed,
                },
                Self::CapabilityDenied {
                    peer_id: b_peer,
                    had: b_had,
                    needed: b_needed,
                },
            ) => a_peer == b_peer && a_had == b_had && a_needed == b_needed,
            (Self::UnknownTicket(a), Self::UnknownTicket(b)) => a == b,
            (Self::ReservedAction(a), Self::ReservedAction(b)) => a == b,
            (Self::InvalidTicket, Self::InvalidTicket)
            | (Self::NotHost, Self::NotHost)
            | (Self::TicketExpired, Self::TicketExpired)
            | (Self::TicketNotAdmissible, Self::TicketNotAdmissible) => true,
            _ => false,
        }
    }
}
impl Eq for SessionError {}

/// The one session→wire error translation, used by both the IPC
/// server (`Response::Error`) and the gossip bridge (`SendAck`).
/// Living here, next to the error type, keeps the two surfaces
/// identical by construction — they used to be hand-synced mirrors,
/// which is one missed edit away from a bearer-visible divergence.
impl From<&SessionError> for ProtocolError {
    fn from(err: &SessionError) -> Self {
        match err {
            SessionError::UnknownSession(s) => Self::UnknownSession(*s),
            SessionError::NotMember(_) => Self::Internal("not a member".into()),
            // TicketNotAdmissible is joiner-opaque on purpose
            // (revocation slice): revoked, never-issued, and
            // mint-mismatch all collapse to InvalidTicket so a bearer
            // can't oracle the host's ledger.
            SessionError::InvalidTicket
            | SessionError::TicketExpired
            | SessionError::TicketNotAdmissible => Self::InvalidTicket,
            SessionError::Storage(io_err) => Self::Internal(format!("storage: {io_err}")),
            // Generic Internal so ticket-parser detail doesn't leak.
            SessionError::InvalidAddr(msg) => Self::Internal(format!("invalid addr: {msg}")),
            SessionError::Internal(msg) => Self::Internal(msg.clone()),
            SessionError::NotHost => Self::NotHost,
            SessionError::SessionConflict(s) => Self::SessionConflict(*s),
            // Forward the host's verdict verbatim so the caller sees
            // the actual reason (e.g., UnknownSession after a session
            // close) instead of a generic Internal. Host-side this
            // should never occur (only joiners receive HostRejected
            // from `send_remote`) — surfaced defensively.
            SessionError::HostRejected(err) => err.clone(),
            // Distinguishable from a generic Internal so the client
            // can tell a sig failure from a cap failure.
            SessionError::SignatureRejected { peer_id, reason } => {
                Self::Signature(format!("{peer_id}: {reason}"))
            }
            // L2 capability denial (Auth Slice C). `had`/`needed`
            // name the cap gap; never payload bytes.
            SessionError::CapabilityDenied {
                peer_id,
                had,
                needed,
            } => Self::Capability(format!("{peer_id}: had {had:?}, needs {needed:?}")),
            SessionError::InvalidCapClaim(reason) => {
                Self::Internal(format!("invalid cap claim: {reason}"))
            }
            // Host-operator-facing (RevokeTicket of a never-issued
            // id); never reaches the gossip wire today but mapped
            // faithfully rather than panicking on a future code
            // motion.
            SessionError::UnknownTicket(t) => Self::UnknownTicket(*t),
            // A member tried to author a reserved daemon-injected
            // action. It's a protocol misuse (or a forge attempt),
            // not a capability tier issue — surface it as Internal
            // with the offending action named.
            SessionError::ReservedAction(action) => {
                Self::Internal(format!("reserved system action: {action}"))
            }
        }
    }
}

/// Signed capability claim extracted from a `JoinAnnouncement`. The
/// host verifies this against its own pubkey at admission to grant the
/// ticket-specified capability tier rather than unconditional RW.
#[derive(Clone, Debug)]
#[cfg(feature = "iroh")]
pub(crate) struct CapClaim {
    pub ticket_id: TicketId,
    pub granted_cap: Capability,
    pub expiry_ms: u64,
    pub cap_sig: SigBytes,
}

/// Outcome of a successful `subscribe`: a snapshot of the log to
/// replay, plus a live event receiver for everything that follows.
#[derive(Debug)]
pub struct Subscription {
    /// Log entries with `seq > since` at the moment of subscription.
    /// Empty if the caller already had everything.
    pub replay: Vec<SessionMessage>,
    /// Live event stream. The first event is whatever happens *after*
    /// the last entry in `replay`.
    pub events: broadcast::Receiver<Event>,
}

/// One active session.
#[derive(Debug)]
pub struct Session {
    id: SessionId,
    host: PeerId,
    kind: SessionKind,
    members: HashSet<PeerId>,
    log: Vec<SessionMessage>,
    head: Seq,
    /// Host incarnation epoch. On a `Local` session this is the host's
    /// own incarnation counter (bumped on resume); on a `Remote` mirror
    /// it is the highest beacon-verified host epoch (the watermark).
    /// See [`SessionRecord::host_epoch`].
    host_epoch: u64,
    /// Projected capability set (Auth Slice C / L2): the current cap
    /// each peer holds, derived by replaying every `Capability` message
    /// in the log in seq order. **Derived state — never persisted.** It
    /// is seeded in [`Session::new`] (host ⇒ `ReadWrite`), rebuilt from
    /// `record.log` in [`Session::from_record`], and advanced
    /// incrementally as messages are appended on both the host-send and
    /// joiner-mirror paths. A peer absent from this map is treated as
    /// `Read`-only ("absent ⇒ Read" floor). See
    /// `artel_protocol::capability`.
    caps: HashMap<PeerId, Capability>,
    /// Issued-ticket ledger (revocation slice). `Local` sessions:
    /// every ticket minted for this session, mint order — the
    /// authoritative set for issued-only admission. `Remote` mirrors
    /// never mint; stays empty. **Persisted state** (unlike the
    /// derived `caps`): round-trips through `record`/`from_record`
    /// and is rewritten via `SessionStore::put_tickets` on every
    /// mutation, store-before-memory.
    tickets: Vec<TicketEntry>,
    /// Workspace ticket envelope (revoked-lurker fix). Opaque bytes,
    /// **persisted state** like `tickets`: round-trips through
    /// `record`/`from_record` and is rewritten via
    /// `SessionStore::put_workspace_ticket`, store-before-memory.
    /// One slot, kind-dependent meaning — see
    /// [`SessionRecord::workspace_ticket`].
    workspace_ticket: Option<Vec<u8>>,
    events_tx: broadcast::Sender<Event>,
}

impl Session {
    fn new(id: SessionId, host: &PeerInfo, kind: SessionKind) -> Self {
        let mut members = HashSet::new();
        members.insert(host.id);
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        // Seed the host as the cap-log root: it holds `ReadWrite` by
        // construction. This is the equivalent of the originator's
        // first implicit `Grant(self, ReadWrite)` — we never emit a
        // literal self-grant; the host simply starts at the ceiling, so
        // its first real `Grant(joiner, ..)` passes the
        // author-holds-RW authority check (Auth Slice C / L2, Q2).
        let mut caps = HashMap::new();
        caps.insert(host.id, Capability::ReadWrite);
        Self {
            id,
            host: host.id,
            kind,
            members,
            log: Vec::new(),
            head: Seq::ZERO,
            host_epoch: 0,
            caps,
            tickets: Vec::new(),
            workspace_ticket: None,
            events_tx,
        }
    }

    /// Hydrate from a persisted [`SessionRecord`]. The record's
    /// `kind` is authoritative — a `Remote` mirror rehydrates as
    /// `Remote` so it doesn't try to assign seqs locally after a
    /// daemon restart.
    fn from_record(record: SessionRecord) -> Self {
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        // Rebuild the derived cap-set from the persisted log (Auth Slice
        // C, Q4): caps are never persisted, so on every daemon restart /
        // resume we replay the log in seq order to reconstruct them. The
        // host starts at `ReadWrite` (the cap-log root), then every
        // appended `Capability` message projects on top.
        let caps = project_caps(record.host, &record.log);
        Self {
            id: record.id,
            host: record.host,
            kind: record.kind,
            members: record.members,
            log: record.log,
            head: record.head,
            host_epoch: record.host_epoch,
            caps,
            tickets: record.tickets,
            workspace_ticket: record.workspace_ticket,
            events_tx,
        }
    }

    /// Take a snapshot suitable for [`crate::store::SessionStore::create`].
    fn record(&self) -> SessionRecord {
        SessionRecord {
            id: self.id,
            host: self.host,
            members: self.members.clone(),
            head: self.head,
            log: self.log.clone(),
            kind: self.kind,
            host_epoch: self.host_epoch,
            tickets: self.tickets.clone(),
            workspace_ticket: self.workspace_ticket.clone(),
        }
    }

    /// Summary suitable for [`Registry::list`].
    fn summary(&self, daemon_peer_id: PeerId) -> SessionSummary {
        SessionSummary {
            id: self.id,
            is_host: self.host == daemon_peer_id,
            peer_count: u32::try_from(self.members.len()).unwrap_or(u32::MAX),
            last_seq: if self.head == Seq::ZERO {
                None
            } else {
                Some(self.head)
            },
        }
    }

    /// Whether `peer` may author a (non-capability) message right now —
    /// i.e. holds `ReadWrite` in the current projected cap set. The
    /// single source of the "can write" rule. "Absent ⇒ Read" is the
    /// floor, so an unknown peer cannot write.
    ///
    /// **v1 is host-only enforcement** (Auth Slice C delivery rethink,
    /// `docs/brainstorms/2026-06-04-auth-slice-c-l2-delivery-rethink-brainstorm.md`):
    /// this is consulted on the host's `Registry::send` path, which is
    /// the sole sequencer and thus the mandatory enforcement chokepoint.
    /// Joiner mirrors do **not** re-enforce — see the loud note at
    /// `apply_inbound_mirror_message`.
    fn can_write(&self, peer: PeerId) -> bool {
        self.caps
            .get(&peer)
            .is_some_and(|c| Capability::permits_write(*c))
    }

    /// Issued-only ledger gate (revocation slice): may a bearer of
    /// `claim` be admitted? The claim's id must be present in the
    /// ledger and `Active`, and the claimed cap/expiry must match
    /// what was minted — absence, revocation, and mismatch are all
    /// inadmissible, and the caller maps all three to ONE
    /// joiner-opaque error (the bearer must not learn which). Only
    /// `Local` sessions enforce: a Remote mirror has no ledger (it
    /// never mints) and is not the admission authority for anyone.
    #[cfg(feature = "iroh")]
    fn ticket_admissible(&self, claim: &CapClaim) -> bool {
        if self.kind != SessionKind::Local {
            return true;
        }
        self.tickets.iter().any(|t| {
            t.ticket_id == claim.ticket_id
                && t.status == TicketStatus::Active
                && t.granted_cap == claim.granted_cap
                && t.expiry_ms == claim.expiry_ms
        })
    }

    /// Apply a single `Capability` message to the incremental cap set,
    /// enforcing the authority rule (Q2): the author (`msg.peer.id`)
    /// must currently hold `ReadWrite`, else the action is a no-op on
    /// `caps`. The host appends in seq order, so "currently" already
    /// *is* at-seq. Call this *after* the message is pushed to the log,
    /// exactly once per appended `Capability`-kind message.
    ///
    /// A malformed payload is ignored (the message stays in the log as
    /// an inert artifact, but never mutates caps); this only happens for
    /// a `Capability`-kind message whose payload isn't a valid
    /// [`CapabilityAction`], which the author signed — they get nothing.
    fn apply_capability(&mut self, msg: &SessionMessage) {
        debug_assert_eq!(msg.kind, MessageKind::Capability);
        apply_capability_to(&mut self.caps, self.host, msg);
    }
}

/// Fold a log into a capability set, in seq order, starting from the
/// host's root `ReadWrite`. The log is assumed sorted by seq (both the
/// host log and the joiner mirror maintain that invariant); we sort a
/// scratch index defensively so a caller passing an unsorted slice
/// still gets at-seq-correct output.
fn project_caps(host: PeerId, log: &[SessionMessage]) -> HashMap<PeerId, Capability> {
    let mut caps = HashMap::new();
    caps.insert(host, Capability::ReadWrite);
    // The log is maintained in seq order on every path that feeds this;
    // iterate by ascending seq to be robust regardless.
    let mut ordered: Vec<&SessionMessage> = log.iter().collect();
    ordered.sort_by_key(|m| m.seq);
    for msg in ordered {
        if msg.kind == MessageKind::Capability {
            apply_capability_to(&mut caps, host, msg);
        }
    }
    caps
}

/// Core projection step shared by the incremental and full-replay
/// paths. Only the host may grant/revoke — tightened from the original
/// "any RW holder" (Q2) to prevent malicious joiners from revoking
/// peers. For P2P (no single host) this must become a quorum or
/// causal-authority rule; see
/// `docs/brainstorms/2026-06-04-auth-slice-c-l2-delivery-rethink-brainstorm.md`.
fn apply_capability_to(caps: &mut HashMap<PeerId, Capability>, host: PeerId, msg: &SessionMessage) {
    // Authority check: only the host may issue grants/revokes.
    if msg.peer.id != host {
        return;
    }
    let Ok(action) = CapabilityAction::decode(&msg.payload) else {
        // Author held RW but shipped a malformed payload — inert.
        return;
    };
    match action {
        CapabilityAction::Grant { peer, cap } => {
            caps.insert(peer, cap);
        }
        CapabilityAction::Revoke { peer } => {
            // Removal = back to the "absent ⇒ Read" floor.
            caps.remove(&peer);
        }
    }
}

/// Host-side L2 capability gate (Auth Slice C). Returns
/// [`SessionError::CapabilityDenied`] if `peer_id` does not currently
/// hold `ReadWrite` in the session's projected cap set, leaving the
/// caller to reject *before* the message is signed, sequenced, or
/// appended — so an unauthorized write (or an unauthorized grant/revoke,
/// which rides on the same `ReadWrite` requirement, Q2) never occupies a
/// seq in the log (open item O1: drop-before-append). The host's cap set
/// is at-seq by construction (it appends strictly in order). `Capability`
/// and ordinary messages share one rule: the author must hold
/// `ReadWrite`. Pulled out of [`Registry::send`] so the lock guard's
/// scope is tight (no live `MutexGuard` held across the rest of `send`'s
/// await points) and `send` stays under clippy's line cap.
async fn ensure_can_write(
    session_arc: &Arc<Mutex<Session>>,
    peer_id: PeerId,
) -> Result<(), SessionError> {
    let (can_write, had) = {
        let s = session_arc.lock().await;
        (s.can_write(peer_id), s.caps.get(&peer_id).copied())
    };
    if can_write {
        return Ok(());
    }
    Err(SessionError::CapabilityDenied {
        peer_id,
        had,
        needed: Capability::ReadWrite,
    })
}

/// In-memory session registry, backed by a [`crate::store::SessionStore`]
/// for durability.
#[derive(Debug)]
pub struct Registry {
    daemon_peer_id: PeerId,
    /// The daemon's own [`WireEndpointAddr`], stamped into every
    /// outbound `host` ticket so joiners can dial back. Either a
    /// snapshot of the live iroh `Endpoint::addr()` or an
    /// `id_only` placeholder when the daemon is local-only.
    daemon_addr: WireEndpointAddr,
    sessions: RwLock<HashMap<SessionId, Arc<Mutex<Session>>>>,
    store: DynStore,
    /// Plumbing to the iroh gossip substrate. `Some` when the daemon
    /// is running with the `iroh` feature on and an `iroh_key_path`
    /// supplied; `None` for local-only embeds and unit tests.
    #[cfg(feature = "iroh")]
    bridge: Option<Arc<crate::gossip_bridge::GossipBridge>>,
    /// Daemon's iroh secret key, used to sign every locally-authored
    /// `SessionMessage` (Auth Slice B). `None` only for unit tests
    /// that build a registry via the test-only [`Registry::new`]
    /// without an iroh runtime in scope; every production path
    /// populates it from [`crate::server::IrohRuntime::signing_key`]
    /// (see [`Registry::load`]'s call in `Daemon::start`).
    ///
    /// When the key is `None`, the `Local` arm of [`Authoring`] stamps
    /// [`SIGNATURE_UNSIGNED`] rather than panicking: that sentinel is
    /// the lit fuse. Any real receive/load path verifies and rejects
    /// it ([`signing::VerifyError::SentinelUnsigned`]), so a wiring
    /// bug that left the key unset surfaces as a loud, total verify
    /// failure rather than silently shipping forgeable messages. It is
    /// *not* silently treated as "signed".
    #[cfg(feature = "iroh")]
    signing_key: Option<Arc<iroh::SecretKey>>,
    /// The daemon's iroh `Endpoint`, needed by the direct-stream
    /// upgrade delivery path (`DeliverUpgrade`) to dial target peers.
    /// `None` only for unit tests that don't wire up a full iroh
    /// runtime.
    #[cfg(feature = "iroh")]
    endpoint: Option<iroh::Endpoint>,
}

/// Where the body handed to [`Registry::send`] was authored.
///
/// Drives the sign-vs-verify decision: [`Authoring::Local`] stamps
/// `timestamp_ms` and signs with the daemon's own
/// [`Registry::signing_key`]; [`Authoring::Remote`] trusts the
/// joiner's timestamp + signature and verifies against the body's
/// `peer.id` before assigning seq + appending.
///
/// The two arms differ in one bit ("did someone else already author
/// this body?"). A typed enum forecloses "I forgot which arm re-signs"
/// bugs the plain-arguments shape allowed: it is impossible to call
/// `Registry::send` without committing to one or the other.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Authoring {
    /// This daemon authored the body. Production IPC `Send` callers
    /// and the unit-test fan-out use this arm. Stamps
    /// `timestamp_ms = now_ms()` and signs before append.
    Local,
    /// A remote joiner authored the body, signed it, and the host
    /// is now appending it to its log. Preserves the joiner's
    /// `timestamp_ms` + `signature` verbatim and **verifies** before
    /// assigning seq + appending. Only [`crate::gossip_bridge`]'s
    /// `run_host_send` constructs this variant.
    #[cfg(feature = "iroh")]
    Remote {
        /// Authoring time, stamped by the joiner's daemon at the
        /// moment the body was signed. Inside the signed scope
        /// (canonical bytes) so a malicious host cannot rewrite it.
        timestamp_ms: u64,
        /// Joiner's ed25519 signature over
        /// [`signing::canonical_bytes`]. Verified against the body's
        /// `peer.id` before append; on failure
        /// [`SessionError::SignatureRejected`] is returned and no
        /// state mutates.
        signature: SigBytes,
    },
}

impl Registry {
    /// Create a registry backed by `store`. The store is consulted only
    /// for mutations; in-memory state holds the live runtime view
    /// (broadcast channels, subscribers).
    ///
    /// Used by unit tests; production code goes through
    /// [`Registry::load`] which also rehydrates from the store.
    #[cfg(test)]
    pub(crate) fn new(daemon_peer_id: PeerId, store: DynStore) -> Self {
        Self {
            daemon_peer_id,
            daemon_addr: WireEndpointAddr::id_only(daemon_peer_id),
            sessions: RwLock::new(HashMap::new()),
            store,
            #[cfg(feature = "iroh")]
            bridge: None,
            #[cfg(feature = "iroh")]
            signing_key: None,
            #[cfg(feature = "iroh")]
            endpoint: None,
        }
    }

    /// Test-only constructor that wires a signing key into the
    /// registry. Production code uses [`Registry::load`] (which
    /// receives the key from
    /// [`crate::server::IrohRuntime::signing_key`]); tests that need
    /// the `Local` arm of [`Authoring`] to actually sign go through
    /// this.
    #[cfg(all(test, feature = "iroh"))]
    pub(crate) fn new_with_signing_key(
        daemon_peer_id: PeerId,
        store: DynStore,
        signing_key: Arc<iroh::SecretKey>,
    ) -> Self {
        Self {
            daemon_peer_id,
            daemon_addr: WireEndpointAddr::id_only(daemon_peer_id),
            sessions: RwLock::new(HashMap::new()),
            store,
            bridge: None,
            signing_key: Some(signing_key),
            endpoint: None,
        }
    }

    /// Build a registry whose initial state is the records the store
    /// returned from `load_all`. Called once at daemon startup.
    pub(crate) async fn load(
        daemon_peer_id: PeerId,
        daemon_addr: WireEndpointAddr,
        store: DynStore,
        #[cfg(feature = "iroh")] bridge: Option<Arc<crate::gossip_bridge::GossipBridge>>,
        #[cfg(feature = "iroh")] signing_key: Option<Arc<iroh::SecretKey>>,
        #[cfg(feature = "iroh")] endpoint: Option<iroh::Endpoint>,
    ) -> std::io::Result<Self> {
        let records = store.load_all().await?;
        let mut sessions = HashMap::with_capacity(records.len());
        for record in records {
            let id = record.id;
            sessions.insert(id, Arc::new(Mutex::new(Session::from_record(record))));
        }
        Ok(Self {
            daemon_peer_id,
            daemon_addr,
            sessions: RwLock::new(sessions),
            store,
            #[cfg(feature = "iroh")]
            bridge,
            #[cfg(feature = "iroh")]
            signing_key,
            #[cfg(feature = "iroh")]
            endpoint,
        })
    }

    /// The daemon's own peer id, returned in the `Hello` response.
    #[must_use]
    pub const fn daemon_peer_id(&self) -> PeerId {
        self.daemon_peer_id
    }

    /// Returns `Some(true)` if the session exists and is `Local` (we
    /// are the host), `Some(false)` if it exists but is `Remote`, or
    /// `None` if the session is unknown.
    pub(crate) async fn is_local_session(&self, session: SessionId) -> Option<bool> {
        let guard = self.sessions.read().await;
        let session_arc = guard.get(&session)?.clone();
        drop(guard);
        let s = session_arc.lock().await;
        Some(s.kind == SessionKind::Local)
    }

    /// Borrow the iroh `Endpoint` (if wired). Used by the
    /// `DeliverUpgrade` dispatch to dial target peers.
    #[cfg(feature = "iroh")]
    pub(crate) const fn endpoint(&self) -> Option<&iroh::Endpoint> {
        self.endpoint.as_ref()
    }

    /// Host or resume a session. Returns the session's id and a
    /// fresh join ticket stamped with this daemon's current
    /// [`WireEndpointAddr`].
    ///
    /// `requested_id` controls the session id and the resume path:
    ///
    /// - `None` (today's behaviour): mint a fresh random
    ///   [`SessionId`] and create a new session record.
    /// - `Some(id)` and **no existing local entry**: create a new
    ///   session record at `id`. Lets a caller (e.g. `artel-fs`)
    ///   pin the id to local state so a re-host always lands on
    ///   the same id.
    /// - `Some(id)` and **existing local entry whose host is
    ///   `host_peer.id` and whose kind is `Local`**: resume
    ///   verbatim. Members, log, head, and broadcast channel are
    ///   preserved. The returned ticket is re-stamped from the
    ///   *current* `daemon_addr`, which may differ from the addr
    ///   in the persisted record after a daemon restart.
    /// - `Some(id)` and **existing entry that doesn't match**
    ///   (different host, or `kind == Remote`): rejected with
    ///   [`SessionError::SessionConflict`]. The in-memory state
    ///   is not modified.
    ///
    /// On store-write failure the in-memory state is not modified
    /// and the error propagates — the create path doesn't insert the
    /// session, and the resume path keeps its old epoch and ledger.
    /// This keeps "registry thinks it has session X but disk
    /// doesn't" from happening.
    pub async fn host(
        &self,
        host_peer: PeerInfo,
        requested_id: Option<SessionId>,
        granted_cap: Capability,
        expiry_ms: u64,
    ) -> Result<(SessionId, JoinTicket, TicketId), SessionError> {
        // Resume path: caller supplied an id and we already have a
        // matching local-host record. Reuse the in-memory session
        // verbatim and re-stamp the ticket with the current addr.
        if let Some(id) = requested_id {
            let existing = {
                let guard = self.sessions.read().await;
                guard.get(&id).cloned()
            };
            if let Some(arc) = existing {
                // Bump this host's incarnation epoch (Auth Slice B.5,
                // D3) and persist it before returning the ticket or
                // re-subscribing. This re-subscribe of an existing
                // local-host record IS the incarnation boundary: a
                // `SessionClosed` signed at the old epoch is rejected
                // against a joiner whose beacon-advanced watermark has
                // moved past it. A fresh create (below) leaves epoch 0.
                let (ticket, ticket_id) = self.mint_ticket(id, granted_cap, expiry_ms);
                let host_epoch = {
                    let mut s = arc.lock().await;
                    if s.host != host_peer.id || s.kind != SessionKind::Local {
                        return Err(SessionError::SessionConflict(id));
                    }
                    let host_epoch = s.host_epoch.saturating_add(1);
                    // Every resume re-mints, so every resume appends a
                    // ledger entry (deliberate — the bearer string just
                    // handed back genuinely admits and must be
                    // revocable; entries are tiny and session-scoped).
                    let mut tickets = s.tickets.clone();
                    tickets.push(Self::ledger_entry(ticket_id, granted_cap, expiry_ms));
                    // Store-before-memory, with the lock held across
                    // the writes like every other ledger mutation
                    // (write_tickets' deterministic tmp name relies on
                    // it). On store failure, surface it and leave
                    // memory untouched: a resume that can't durably
                    // record its new epoch would re-emit a stale epoch
                    // after the next restart, and an unpersisted
                    // ledger entry would brick the ticket we are about
                    // to return (issued-only).
                    self.store
                        .bump_host_epoch(id, host_epoch)
                        .await
                        .map_err(SessionError::Storage)?;
                    self.store
                        .put_tickets(id, &tickets)
                        .await
                        .map_err(SessionError::Storage)?;
                    s.host_epoch = host_epoch;
                    s.tickets = tickets;
                    host_epoch
                };

                // Re-open the gossip topic. The bridge tracks per-
                // session state by id; if the daemon was restarted
                // since the original `host` call, the topic is gone
                // and we need to re-subscribe. If it's still around
                // (same-process resume), the existing entry is left
                // in place and we just ignore the re-host. Best-
                // effort: a bridge failure is non-fatal — the local
                // session still works; we just won't reach the
                // network until something else triggers a reattach.
                // After (re)subscribing, broadcast a signed EpochBeacon
                // so already-joined joiners learn the new epoch
                // immediately, independent of session activity.
                #[cfg(feature = "iroh")]
                if let Some(bridge) = &self.bridge {
                    if let Err(err) = bridge.host_session(id).await {
                        tracing::warn!(?err, ?id, "gossip host_session failed on resume");
                    }
                    bridge.publish_epoch_beacon(id, host_epoch).await;
                }

                return Ok((id, ticket, ticket_id));
            }
        }

        // Create path. Either no `requested_id` (mint random) or
        // `Some(id)` whose entry doesn't exist locally yet.
        let session_id = requested_id.unwrap_or_else(SessionId::new_random);
        let (ticket, ticket_id) = self.mint_ticket(session_id, granted_cap, expiry_ms);
        let mut session = Session::new(session_id, &host_peer, SessionKind::Local);
        // Seed the ledger before record(): create() persists the
        // initial entry together with the session, so there is no
        // window where the ticket exists but its ledger entry doesn't.
        session
            .tickets
            .push(Self::ledger_entry(ticket_id, granted_cap, expiry_ms));
        let record = session.record();
        self.store
            .create(&record)
            .await
            .map_err(SessionError::Storage)?;
        self.sessions
            .write()
            .await
            .insert(session_id, Arc::new(Mutex::new(session)));

        // If iroh is wired up, open a gossip topic for this session
        // so future Sends can fan out to remote joiners. Bridge
        // failure is non-fatal: the local session still works; we
        // just won't reach the network. Surface as a warn for ops.
        #[cfg(feature = "iroh")]
        if let Some(bridge) = &self.bridge
            && let Err(err) = bridge.host_session(session_id).await
        {
            tracing::warn!(?err, ?session_id, "gossip host_session failed");
        }

        Ok((session_id, ticket, ticket_id))
    }

    /// Acquire the per-session lock for a locally-hosted session —
    /// the shared authority gate of the ticket-ledger operations
    /// (`issue_ticket` / `revoke_ticket` / `list_tickets`).
    ///
    /// `SessionError::UnknownSession` if the id isn't known,
    /// `SessionError::NotHost` if the session is a `Remote` mirror.
    /// `kind == Local` is the whole check: the L1 invariant (the IPC
    /// server stamps every caller with the daemon's own identity)
    /// already guarantees a Local session is one this daemon hosts —
    /// there is no separate host-id comparison here, so if ledger
    /// authority ever needs to be finer than "this daemon" (e.g.
    /// sub-RW members), this is the single place to grow it.
    async fn lock_local_session(
        &self,
        session: SessionId,
    ) -> Result<tokio::sync::OwnedMutexGuard<Session>, SessionError> {
        let arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };
        let s = arc.lock_owned().await;
        if s.kind != SessionKind::Local {
            return Err(SessionError::NotHost);
        }
        Ok(s)
    }

    /// Issue an additional ticket for an existing locally-hosted session.
    ///
    /// Authority: [`Self::lock_local_session`]. Returns
    /// `SessionError::NotHost` if the session is a remote mirror, and
    /// `SessionError::UnknownSession` if it doesn't exist.
    pub async fn issue_ticket(
        &self,
        session: SessionId,
        granted_cap: Capability,
        expiry_ms: u64,
    ) -> Result<(JoinTicket, TicketId), SessionError> {
        let mut s = self.lock_local_session(session).await?;
        let (ticket, ticket_id) = self.mint_ticket(session, granted_cap, expiry_ms);
        // Store-before-memory: if the ledger write fails, the
        // mint fails and no unrevocable ticket leaves the daemon
        // (issued-only would reject it anyway — fail loudly here
        // instead of handing out a dead ticket).
        let mut tickets = s.tickets.clone();
        tickets.push(Self::ledger_entry(ticket_id, granted_cap, expiry_ms));
        self.store
            .put_tickets(session, &tickets)
            .await
            .map_err(SessionError::Storage)?;
        s.tickets = tickets;
        // Lint-mandated (significant_drop_tightening): release before
        // building the return value.
        drop(s);
        Ok((ticket, ticket_id))
    }

    /// Revoke a previously issued ticket so it no longer admits
    /// bearers (revocation slice). Authority mirrors
    /// [`Registry::issue_ticket`]: only the hosting daemon of a
    /// `SessionKind::Local` session. Idempotent on an
    /// already-revoked ticket; an id never issued for this session is
    /// [`SessionError::UnknownTicket`] (success would falsely
    /// reassure the operator a leaked ticket is dead).
    ///
    /// Ticket-only: an already-admitted bearer keeps membership and
    /// caps — revocation gates *future admissions*. Use a capability
    /// revoke for the peer; `list_tickets`' `used_by` names them.
    pub async fn revoke_ticket(
        &self,
        session: SessionId,
        ticket_id: TicketId,
    ) -> Result<(), SessionError> {
        let mut s = self.lock_local_session(session).await?;
        let Some(idx) = s.tickets.iter().position(|t| t.ticket_id == ticket_id) else {
            return Err(SessionError::UnknownTicket(ticket_id));
        };
        if s.tickets[idx].status == TicketStatus::Revoked {
            return Ok(());
        }
        // Store-before-memory, same shape as every other mutation.
        let mut tickets = s.tickets.clone();
        tickets[idx].status = TicketStatus::Revoked;
        self.store
            .put_tickets(session, &tickets)
            .await
            .map_err(SessionError::Storage)?;
        s.tickets = tickets;
        // Lint-mandated (significant_drop_tightening).
        drop(s);
        Ok(())
    }

    /// Snapshot of the issued-ticket ledger (revocation slice).
    /// Metadata only — the encoded bearer strings are never stored,
    /// so they cannot be returned. Authority as `revoke_ticket`.
    pub async fn list_tickets(&self, session: SessionId) -> Result<Vec<TicketEntry>, SessionError> {
        let s = self.lock_local_session(session).await?;
        Ok(s.tickets.clone())
    }

    /// Mint a bearer ticket. Pure: generates the id, signs, encodes —
    /// does NOT touch the ledger. Every caller must record a
    /// [`TicketEntry`] for the returned id (issued-only admission
    /// rejects ledger-absent ids, so a mint whose entry is never
    /// recorded produces a ticket that can never admit).
    fn mint_ticket(
        &self,
        session_id: SessionId,
        granted_cap: Capability,
        expiry_ms: u64,
    ) -> (JoinTicket, TicketId) {
        let tid = TicketId::new_random();
        let cap_sig = self.signing_key.as_ref().map_or(SIGNATURE_UNSIGNED, |key| {
            signing::sign_ticket_cap(
                key.as_signing_key(),
                tid,
                session_id,
                granted_cap,
                expiry_ms,
            )
        });
        let ticket = JoinTicket::from(ticket::encode(&SessionTicket {
            ticket_id: tid,
            session_id,
            host_peer_id: self.daemon_peer_id,
            host_addr: self.daemon_addr.clone(),
            granted_cap,
            expiry_ms,
            cap_sig,
        }));
        (ticket, tid)
    }

    /// Build the ledger entry for a just-minted ticket.
    fn ledger_entry(ticket_id: TicketId, granted_cap: Capability, expiry_ms: u64) -> TicketEntry {
        TicketEntry {
            ticket_id,
            granted_cap,
            expiry_ms,
            issued_at_ms: now_ms(),
            status: TicketStatus::Active,
            used_by: Vec::new(),
        }
    }

    /// Join an existing session via its ticket. Returns the session id
    /// and the head seq at join time.
    ///
    /// Two cases:
    ///
    /// - **Local session.** The session is already in `self.sessions`
    ///   (we're the host or an earlier joiner-on-the-same-daemon).
    ///   Just adds the peer to membership and emits `PeerJoined`.
    /// - **Remote session** (`host_peer_id != self.daemon_peer_id`).
    ///   The session doesn't exist locally yet; we materialise a
    ///   mirror, ask the bridge to subscribe to the host's gossip
    ///   topic, and feed inbound messages into the mirror. Without
    ///   the iroh feature this is rejected as `InvalidTicket`.
    pub async fn join(
        &self,
        ticket: &JoinTicket,
        peer: PeerInfo,
    ) -> Result<(SessionId, Option<Seq>), SessionError> {
        let parsed = parse_ticket(ticket)?;
        let session_id = parsed.session_id;

        let session = {
            let guard = self.sessions.read().await;
            guard.get(&session_id).cloned()
        };

        let session = if let Some(existing) = session {
            existing
        } else {
            if parsed.host_peer_id == self.daemon_peer_id {
                // Same-daemon ticket but the session id isn't
                // registered locally — that's a stale or forged
                // ticket, not a "join a remote" request.
                return Err(SessionError::UnknownSession(session_id));
            }
            // Remote session: not yet known locally. Materialise
            // a mirror and wire up the gossip bridge so the host's
            // messages start flowing in.
            self.materialise_remote_session(
                session_id,
                &parsed.host_peer_id,
                &parsed.host_addr,
                &peer,
                parsed.ticket_id,
                parsed.granted_cap,
                parsed.expiry_ms,
                parsed.cap_sig,
            )
            .await?
        };

        // Hold the session lock across the store write so a concurrent
        // join with the same peer doesn't race past the membership
        // check. This is the simplest correct shape; the store is fast
        // and uncontended in practice.
        let head;
        {
            let mut s = session.lock().await;
            head = if s.head == Seq::ZERO {
                None
            } else {
                Some(s.head)
            };
            if s.members.contains(&peer.id) {
                // Self-rejoin: caller's authenticated id is already
                // a member. Daemon-side membership is per-
                // authenticated-identity (persistent across consumer
                // remounts); a re-host or re-join from the same
                // daemon is a no-op. No second PeerJoined fires.
                // See `docs/plans/2026-06-01-auth-l1-fix3-plan.md`.
                return Ok((session_id, head));
            }
            self.store
                .add_member(session_id, &peer)
                .await
                .map_err(SessionError::Storage)?;
            s.members.insert(peer.id);

            // Notify other peers of the join. broadcast::send
            // returns Err when there are no receivers; that's fine,
            // we treat it as a "nobody listening" no-op.
            let _ = s.events_tx.send(Event::PeerJoined {
                session: session_id,
                peer,
            });
        }

        Ok((session_id, head))
    }

    /// Stand up a local mirror of a session whose authoritative log
    /// lives on another daemon. Inserts a new [`Session`] keyed by
    /// `session_id`, persists it, and asks the bridge to subscribe
    /// to the host's gossip topic. Inbound gossip messages land in
    /// the mirror's `log` and `events_tx`.
    #[allow(clippy::too_many_arguments)]
    async fn materialise_remote_session(
        &self,
        session_id: SessionId,
        host_peer_id: &PeerId,
        host_addr: &WireEndpointAddr,
        joiner: &PeerInfo,
        ticket_id: TicketId,
        granted_cap: Capability,
        expiry_ms: u64,
        cap_sig: SigBytes,
    ) -> Result<Arc<Mutex<Session>>, SessionError> {
        // Without the `iroh` feature, there's no way to actually
        // reach the host; refuse cleanly rather than silently
        // creating an unreachable session.
        #[cfg(not(feature = "iroh"))]
        {
            let _ = (
                host_peer_id,
                host_addr,
                joiner,
                ticket_id,
                granted_cap,
                expiry_ms,
                cap_sig,
            );
            tracing::debug!(
                ?session_id,
                "remote ticket received but iroh feature is off",
            );
            return Err(SessionError::InvalidTicket);
        }

        #[cfg(feature = "iroh")]
        {
            let bridge = self
                .bridge
                .as_ref()
                .ok_or(SessionError::InvalidTicket)?
                .clone();

            // Persist the new session so a later daemon restart
            // doesn't lose the membership / log we're about to start
            // populating from the host. Host field is the *remote*
            // peer's id, which lets `summary` distinguish remote
            // sessions in `list` output.
            let mut session_obj = Session::new(
                session_id,
                &PeerInfo::new(*host_peer_id, "remote-host"),
                SessionKind::Remote,
            );
            // The constructor adds the host to `members`; for a
            // remote session that's right (the host is a member of
            // its own session) but we'll never see local Send from
            // them — Sends arrive via gossip and route through the
            // forwarder.
            session_obj.host = *host_peer_id;
            self.store
                .create(&session_obj.record())
                .await
                .map_err(SessionError::Storage)?;
            // Seed the host-epoch watermark from the persisted mirror
            // (0 for a fresh join). The bridge's EpochBeacon arm
            // advances this AtomicU64 on each host-signed beacon and
            // persists the advance via `advance_host_epoch_watermark`;
            // the SessionClosed arm reads it to gate replayed closes.
            let host_epoch_watermark =
                Arc::new(std::sync::atomic::AtomicU64::new(session_obj.host_epoch));

            let arc = Arc::new(Mutex::new(session_obj));
            self.sessions
                .write()
                .await
                .insert(session_id, Arc::clone(&arc));

            // Hand the bridge a callback that writes into this very
            // mirror. We deliberately keep a strong Arc in the
            // closure so the session outlives the forwarder task
            // until forget_session aborts it. The store handle is
            // cloned so the callback can persist each message —
            // without that, a daemon restart loses the entire
            // remote-mirror log (`Subscribe { since: None }` replays
            // nothing on bob's restart, so a joiner that re-runs
            // `Workspace::join_with` hangs in `wait_for_ticket`
            // forever waiting for the host's `workspace.ticket`
            // System message that was never persisted).
            let mirror = Arc::clone(&arc);
            let store = self.store.clone();
            let session_for_log = session_id;
            let on_message = move |msg: SessionMessage| {
                let mirror = Arc::clone(&mirror);
                let store = store.clone();
                let session_for_log = session_for_log;
                // Spawn so the gossip forwarder doesn't block on
                // each message. Acceptable for now; if ordering
                // ever matters we can replace with a per-session
                // mpsc.
                tokio::spawn(async move {
                    apply_inbound_mirror_message(&store, &mirror, session_for_log, msg).await;
                });
            };

            // Wire `host_addr` is used as a synchronous addr hint to
            // sidestep pkarr propagation: the bridge feeds it into
            // its `MemoryLookup` before subscribing so the very
            // first dial finds the host's relay url + direct addrs
            // without waiting on n0 DNS / `DnsPkarrServer`. Falling
            // back on pkarr alone produced a ~500ms-to-15s race in
            // production where a fresh joiner would hit
            // `JOIN_READY_TIMEOUT` before the host's record reached
            // their resolver. The wire format is re-validated at
            // the bridge boundary; a bad addr surfaces as
            // [`SessionError::InvalidAddr`].
            bridge
                .join_session(
                    session_id,
                    joiner.clone(),
                    *host_peer_id,
                    host_addr,
                    host_epoch_watermark,
                    on_message,
                    ticket_id,
                    granted_cap,
                    expiry_ms,
                    cap_sig,
                )
                .await
                .map_err(|e| match e {
                    crate::gossip_bridge::BridgeError::InvalidAddr(msg) => {
                        SessionError::InvalidAddr(msg)
                    }
                    other => SessionError::Internal(other.to_string()),
                })?;

            Ok(arc)
        }
    }

    /// Remove `peer` from `session`. Three cases, distinguished by
    /// session [`SessionKind`] and whether the leaver is the host:
    ///
    /// 1. **Host of a `Local` session leaves** → the entire session is
    ///    closed: `store.delete(session)` (cascades any consumer
    ///    attachments via the store contract), in-memory entry
    ///    removed, [`Event::SessionClosed`] emitted, gossip topic
    ///    torn down with a final `SessionClosed` broadcast so remote
    ///    mirrors see the close.
    ///
    /// 2. **Joiner of a `Local` session leaves** → the session keeps
    ///    going (other members are still in it). Just `remove_member`
    ///    in the store and emit [`Event::PeerLeft`].
    ///
    /// 3. **Joiner of a `Remote` mirror leaves** → the local mirror
    ///    has no purpose without its only local consumer, so drop it
    ///    fully: `store.delete(session)` (cascading attachments),
    ///    in-memory entry removed, [`Event::SessionClosed`] emitted,
    ///    bridge per-session state forgotten. Symmetric with
    ///    [`Self::host_closed_session`] — same teardown shape, just
    ///    triggered by a local IPC leave instead of a gossip
    ///    `SessionClosed` from the host.
    pub async fn leave(&self, session: SessionId, peer: PeerId) -> Result<(), SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };

        // Decide the disposition under the per-session lock so a
        // concurrent `register_attachment` (which holds the same
        // lock) cannot land between our existence check and the
        // store cascade.
        let host;
        let kind;
        let drop_session;
        let host_epoch;
        {
            let mut s = session_arc.lock().await;
            if !s.members.contains(&peer) {
                return Err(SessionError::NotMember(session));
            }
            host = s.host;
            kind = s.kind;
            host_epoch = s.host_epoch;
            // The session's local lifetime ends in two cases: the
            // host is leaving a Local session (case 1), or the
            // (sole local) joiner is leaving a Remote mirror
            // (case 3). Both run the same store-side cascade.
            drop_session = peer == host || kind == SessionKind::Remote;

            // Persist before mutating in-memory state. If this fails,
            // the registry stays consistent with the store.
            if drop_session {
                self.store
                    .delete(session)
                    .await
                    .map_err(SessionError::Storage)?;
            } else {
                self.store
                    .remove_member(session, peer)
                    .await
                    .map_err(SessionError::Storage)?;
            }

            s.members.remove(&peer);
            if drop_session {
                let _ = s.events_tx.send(Event::SessionClosed { session });
            } else {
                let _ = s.events_tx.send(Event::PeerLeft { session, peer });
            }
        }

        if drop_session {
            self.sessions.write().await.remove(&session);
            // Bridge teardown:
            // - Local-host leave (case 1): publish a final
            //   `SessionClosed` over gossip so remote mirrors see the
            //   close, then drop our topic state.
            // - Remote-mirror leave (case 3): we are NOT the host, so
            //   we do not publish anything — the host's mirror is
            //   none of our business. Just drop our local topic
            //   state so the forwarder task exits.
            #[cfg(feature = "iroh")]
            if let Some(bridge) = &self.bridge {
                if peer == host && kind == SessionKind::Local {
                    bridge.publish_session_closed(session, host_epoch).await;
                }
                bridge.forget_session(session).await;
            }
        }
        Ok(())
    }

    /// Snapshot of every active session as a [`SessionSummary`] list.
    pub async fn list(&self) -> Vec<SessionSummary> {
        // Take a cheap snapshot of the Arc handles, then release the
        // top-level lock before per-session locking. This keeps `host`/
        // `join`/`leave` callers from blocking on `list`.
        let arcs: Vec<Arc<Mutex<Session>>> = self.sessions.read().await.values().cloned().collect();
        let mut out = Vec::with_capacity(arcs.len());
        for arc in arcs {
            out.push(arc.lock().await.summary(self.daemon_peer_id));
        }
        out
    }

    /// Ensure `peer` is a member of `session`. Used by the gossip
    /// bridge on the host side to admit a remote joiner, driven by an
    /// inbound `JoinAnnouncement` frame — the *sole* admission path
    /// (the old `SendRequest` lazy-admission backstop was removed:
    /// admitting without a claim would bypass ticket verification;
    /// see `run_host_send` in the bridge). Persists the membership
    /// change and emits [`Event::PeerJoined`] when the peer is newly
    /// added.
    ///
    /// Mostly idempotent for an existing member: re-announcing with a
    /// still-admissible claim is a no-op `Ok`. The ledger gate runs
    /// BEFORE the already-member early return, though, so a duplicate
    /// announcement carrying a since-revoked claim returns
    /// `TicketNotAdmissible` even while the peer keeps its membership
    /// (revocation is ticket-only) — the bridge logs it and moves on.
    ///
    /// When `cap_claim` is `Some`, the host verifies expiry, the
    /// ticket's capability signature, and the issued-ticket ledger,
    /// then grants the claimed tier. When `None`, **all three checks
    /// are skipped and the auto-grant defaults to `ReadWrite`** — a
    /// pre-tiered-tickets compatibility shape that no production
    /// caller uses (the bridge always passes `Some`; only tests pass
    /// `None`). Any future admission path MUST carry a claim; wiring
    /// one through `None` is privilege escalation with zero ticket
    /// verification and zero revocation.
    ///
    /// Returns `Err(UnknownSession)` if `session` doesn't exist on
    /// this daemon. Other failures surface the underlying
    /// [`SessionError`].
    #[cfg(feature = "iroh")]
    pub(crate) async fn ensure_member(
        &self,
        session: SessionId,
        peer: PeerInfo,
        cap_claim: Option<CapClaim>,
    ) -> Result<(), SessionError> {
        // Verify the claim BEFORE any state mutation so a forged or
        // expired ticket never touches the member set.
        if let Some(ref claim) = cap_claim {
            if claim.expiry_ms != 0 && claim.expiry_ms <= now_ms() {
                return Err(SessionError::TicketExpired);
            }
            // Skip sig verification when the host has no signing key
            // (test-only path where host() stamped SIGNATURE_UNSIGNED).
            // Production always has a signing key.
            if self.signing_key.is_some()
                && let Err(err) = signing::verify_ticket_cap(
                    &self.daemon_peer_id,
                    claim.ticket_id,
                    session,
                    claim.granted_cap,
                    claim.expiry_ms,
                    &claim.cap_sig,
                )
            {
                return Err(SessionError::InvalidCapClaim(
                    signing::verify_reason(&err).to_string(),
                ));
            }
        }

        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };
        let session_kind;
        let newly_admitted;
        let workspace_ticket;
        {
            let mut s = session_arc.lock().await;
            // Issued-only ledger gate (revocation slice), after expiry
            // and cap-sig above so an unauthenticated forger can't
            // oracle ledger contents, and BEFORE the already-member
            // early return, same as the other claim checks.
            if let Some(ref claim) = cap_claim
                && !s.ticket_admissible(claim)
            {
                return Err(SessionError::TicketNotAdmissible);
            }
            session_kind = s.kind;
            // Snapshot for the post-admission envelope delivery
            // (revoked-lurker fix) — taken for both the fresh and the
            // re-announce path: a joiner whose local state was wiped
            // re-announces while still in our member set and needs the
            // envelope re-delivered (its receive path is idempotent).
            workspace_ticket = s.workspace_ticket.clone();
            if s.members.contains(&peer.id) {
                newly_admitted = false;
            } else {
                newly_admitted = true;
                // Persist before mutating in-memory state — same shape as
                // `Registry::join`. If the store fails, the registry stays
                // consistent with disk.
                self.store
                    .add_member(session, &peer)
                    .await
                    .map_err(SessionError::Storage)?;
                s.members.insert(peer.id);
                // Record the admission on the ticket's ledger entry
                // (advisory `used_by` metadata — names peers an operator
                // may also want to cap-revoke after revoking a used
                // ticket). Persist failure must NOT fail the admission:
                // membership is already durable, and used_by is a hint,
                // not a gate.
                if let Some(ref claim) = cap_claim
                    && session_kind == SessionKind::Local
                    && let Some(entry) = s
                        .tickets
                        .iter_mut()
                        .find(|t| t.ticket_id == claim.ticket_id)
                    && !entry.used_by.contains(&peer.id)
                {
                    entry.used_by.push(peer.id);
                    let tickets = s.tickets.clone();
                    if let Err(err) = self.store.put_tickets(session, &tickets).await {
                        tracing::warn!(?session, ?err, "persisting used_by failed (advisory)");
                    }
                }
                let events_tx = s.events_tx.clone();
                // Drop the session guard BEFORE the PeerJoined send and,
                // crucially, before the auto-grant `Registry::send` below —
                // `send` re-acquires this same per-session lock, so holding
                // it here would deadlock. Mirrors the existing drop(s)
                // discipline. (Auth Slice C, the one non-obvious hazard.)
                drop(s);
                let _ = events_tx.send(Event::PeerJoined {
                    session,
                    peer: peer.clone(),
                });
            }
        }

        // Auto-grant on join (Auth Slice C / L2): grant the capability
        // tier the ticket claims (or RW if no claim is present — the
        // `SendRequest` backstop path for pre-tiered-ticket joiners).
        // Emit only for a `Local` session — we are its host, and only
        // on a fresh admission — re-announces must not spam the cap
        // log with duplicate grants. See the original comment block in
        // the pre-tiered version for the full rationale re:
        // HOST-PRIVATE (D3) delivery.
        if session_kind == SessionKind::Local && newly_admitted {
            let granted_cap = cap_claim
                .as_ref()
                .map_or(Capability::ReadWrite, |c| c.granted_cap);
            self.auto_grant_on_join(session, peer.id, granted_cap).await;
        }

        // Workspace ticket delivery at admission (revoked-lurker
        // fix): a peer the host just admitted (or re-admitted via
        // re-announce) gets the persisted envelope over the direct
        // stream. This trigger — not a joiner-side retry or timer —
        // is what closes the publish-before-admission race: the
        // moment a peer becomes a member, the host backfills it.
        // Best-effort: a failed dial is covered by the peer's next
        // re-announce.
        if session_kind == SessionKind::Local
            && let Some(envelope_bytes) = workspace_ticket
        {
            self.deliver_workspace_ticket_to(session, peer.id, envelope_bytes)
                .await;
        }
        Ok(())
    }

    /// Emit the host's auto-grant `Capability` message for a freshly
    /// admitted peer. Pulled out of [`Self::ensure_member`] so it
    /// stays under clippy's line cap; warn-and-continue on failure
    /// (the admission already landed).
    #[cfg(feature = "iroh")]
    async fn auto_grant_on_join(&self, session: SessionId, peer: PeerId, cap: Capability) {
        let host_author = PeerInfo::new(self.daemon_peer_id, "host");
        let action = CapabilityAction::Grant { peer, cap };
        if let Err(err) = self
            .send(
                session,
                host_author,
                MessageKind::Capability,
                action.action_str().to_string(),
                action.encode(),
                Authoring::Local,
            )
            .await
        {
            tracing::warn!(?session, %peer, ?err, "auto-grant on join failed");
        }
    }

    /// Best-effort unicast of `envelope_bytes` to one peer over the
    /// direct delivery stream. Shared by the admission trigger
    /// ([`Self::ensure_member`]) and the publish fan-out
    /// ([`Self::publish_workspace_ticket`]). No-ops on a self-target
    /// or when no iroh endpoint is wired (unit tests); failures warn —
    /// admission redelivery covers the peer's next re-announce.
    #[cfg(feature = "iroh")]
    async fn deliver_workspace_ticket_to(
        &self,
        session: SessionId,
        target: PeerId,
        envelope_bytes: Vec<u8>,
    ) {
        if target == self.daemon_peer_id {
            return;
        }
        let Some(endpoint) = self.endpoint() else {
            return;
        };
        let frame = artel_protocol::upgrade::DeliveryFrame::WorkspaceTicket {
            session_id: session,
            envelope_bytes,
        };
        if let Err(err) = crate::server::deliver_frame(endpoint, target, &frame).await {
            tracing::warn!(
                ?session,
                peer = %target,
                %err,
                "workspace ticket delivery failed; admission redelivery covers re-announce",
            );
        }
    }

    /// Snapshot every message in `session`'s log with `seq > since`,
    /// **excluding `Capability` messages**. Used by the host's gossip
    /// bridge to answer a joiner's `Replay` request — we re-broadcast
    /// each entry as a `Message` frame and the joiner's mirror dedups by
    /// seq.
    ///
    /// `Capability` messages are host-private in v1 (Auth Slice C
    /// delivery rethink, D3): joiners neither enforce nor project caps,
    /// so they have no use for a Grant/Revoke, and keeping them off the
    /// Replay path (as well as off live fanout in `send`) means the cap
    /// log adds zero joiner-visible gossip traffic. ⚠️ FOR P2P: caps
    /// MUST sync to peers — drop this filter and deliver capability
    /// events as part of the (causal) log sync. See
    /// `docs/brainstorms/2026-06-04-auth-slice-c-l2-delivery-rethink-brainstorm.md`.
    ///
    /// Returns `Err(UnknownSession)` if `session` doesn't exist on
    /// this daemon. Returns an empty Vec if the joiner is already
    /// caught up.
    #[cfg(feature = "iroh")]
    pub(crate) async fn log_since(
        &self,
        session: SessionId,
        since: Seq,
    ) -> Result<Vec<SessionMessage>, SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };
        let s = session_arc.lock().await;
        Ok(s.log
            .iter()
            .filter(|m| {
                // TICKET_ACTION never rides the gossip replay
                // (revoked-lurker fix): the host doesn't log the
                // envelope (it is unicast + IPC-replay only), so a
                // log entry carrying the action is a peer-authored
                // impostor — re-serving it would put
                // capability-shaped bytes back on the topic.
                m.seq > since
                    && m.kind != MessageKind::Capability
                    && !(m.kind == MessageKind::System
                        && (m.action == UPGRADE_ACTION || m.action == TICKET_ACTION))
            })
            .cloned()
            .collect())
    }

    /// Drop a remote-mirror session because the host has signalled
    /// (via [`GossipBody::SessionClosed`]) that they're closing it.
    /// Deletes the persisted record, removes the in-memory session,
    /// emits [`Event::SessionClosed`] to local IPC subscribers, and
    /// tears down the bridge's per-session topic state. Idempotent:
    /// if the session is already gone (or was never `Remote`)
    /// returns `Ok(())` so a duplicate close broadcast doesn't
    /// surface as an error.
    ///
    /// Only meaningful for `Remote` sessions — the host's own
    /// close path is `Registry::leave(session, host_peer)`. We
    /// guard against the wrong kind defensively so a misrouted
    /// frame from a hostile peer can't poison a local session.
    ///
    /// [`GossipBody::SessionClosed`]: artel_protocol::gossip::GossipBody::SessionClosed
    #[cfg(feature = "iroh")]
    pub(crate) async fn host_closed_session(&self, session: SessionId) -> Result<(), SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard.get(&session).cloned()
        };
        let Some(session_arc) = session_arc else {
            // Already closed (or never present). Nothing to do.
            return Ok(());
        };
        // Hold the per-session lock across the store cascade so a
        // concurrent `register_attachment` (which also takes this
        // lock) cannot land an attachment after the cascade runs.
        // Same shape as `leave`'s critical section.
        let events_tx = {
            let s = session_arc.lock().await;
            if s.kind != SessionKind::Remote {
                tracing::warn!(?session, "ignoring SessionClosed for a non-remote session",);
                return Ok(());
            }
            self.store
                .delete(session)
                .await
                .map_err(SessionError::Storage)?;
            s.events_tx.clone()
        };

        self.sessions.write().await.remove(&session);
        let _ = events_tx.send(Event::SessionClosed { session });

        if let Some(bridge) = &self.bridge {
            bridge.forget_session(session).await;
        }
        Ok(())
    }

    /// Persist a beacon-advanced `host_epoch` watermark on a `Remote`
    /// mirror record (Auth Slice B.5.3). Called from the bridge's
    /// `EpochBeacon` arm after a host-signed beacon advances the
    /// in-memory `AtomicU64` watermark, so the watermark survives a
    /// daemon restart and keeps gating replayed closes. Monotonic: only
    /// writes when `host_epoch` exceeds the stored value. A no-op for
    /// an unknown or non-`Remote` session.
    #[cfg(feature = "iroh")]
    pub(crate) async fn advance_host_epoch_watermark(
        &self,
        session: SessionId,
        host_epoch: u64,
    ) -> Result<(), SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard.get(&session).cloned()
        };
        let Some(session_arc) = session_arc else {
            return Ok(());
        };
        // Advance the in-memory watermark under the lock, then release
        // it before the (monotonic, idempotent) store write. A
        // concurrent advance is harmless: both write a value >= what
        // they read, and `bump_host_epoch` is a last-writer-wins
        // targeted set.
        {
            let mut s = session_arc.lock().await;
            if s.kind != SessionKind::Remote || host_epoch <= s.host_epoch {
                return Ok(());
            }
            s.host_epoch = host_epoch;
        }
        self.store
            .bump_host_epoch(session, host_epoch)
            .await
            .map_err(SessionError::Storage)?;
        Ok(())
    }

    /// Append a message to a session. Returns the freshly-built
    /// [`SessionMessage`] (with its host-assigned `seq`). Also
    /// broadcasts an [`Event::Message`] to local IPC subscribers and
    /// (when the bridge is wired up and this is a `Local` session)
    /// fans out over gossip.
    ///
    /// Remote-mirror sessions return [`SessionError::NotHost`] —
    /// the joiner-side path goes through
    /// [`crate::gossip_bridge::GossipBridge::send_remote`] which
    /// publishes a [`GossipBody::SendRequest`] and awaits a
    /// host-published [`GossipBody::SendAck`]. The outer registry
    /// caller (the IPC dispatch) is responsible for choosing the
    /// right path based on the session kind.
    ///
    /// [`GossipBody::SendRequest`]: artel_protocol::gossip::GossipBody::SendRequest
    /// [`GossipBody::SendAck`]: artel_protocol::gossip::GossipBody::SendAck
    pub(crate) async fn send(
        &self,
        session: SessionId,
        peer: PeerInfo,
        kind: MessageKind,
        action: String,
        payload: Vec<u8>,
        authoring: Authoring,
    ) -> Result<SessionMessage, SessionError> {
        // Reserved daemon-injected actions are never member-authored:
        // the genuine `workspace.ticket` / `workspace.upgrade` messages
        // are synthetic, unsigned, off-log frames the daemon mints from
        // unicast-delivered capability material. Reject at this single
        // sequencing chokepoint — shared by the IPC `Send` path and the
        // gossip `run_host_send` path — so a forged broadcast never
        // enters the log, never reaches the host's live IPC fan-out,
        // and never rides a log-derived replay surface. The downstream
        // log-filter sites stay as defense-in-depth against a stale or
        // mixed-build host that sequenced one before this gate existed.
        if kind == MessageKind::System
            && let Some(reserved) = reserved_system_action(&action)
        {
            return Err(SessionError::ReservedAction(reserved));
        }

        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };

        let kind_snapshot;
        {
            let s = session_arc.lock().await;
            if !s.members.contains(&peer.id) {
                return Err(SessionError::NotMember(session));
            }
            kind_snapshot = s.kind;
        }

        // Remote-mirror sessions can't append locally — the host is
        // the sequencer. Forward via gossip (`SendRequest`) and wait
        // for the host's `SendAck`. The host's local `Registry::send`
        // will produce the broadcast `Message` frame we'll see on
        // our own forwarder; the IPC reply uses the assigned
        // `SessionMessage` from the ack. Authoring on this arm is
        // always `Local` — the joiner's daemon owns the body and
        // signs it inside `bridge.send_remote`; reaching here with
        // `Remote` is a wiring bug.
        #[cfg(feature = "iroh")]
        if kind_snapshot == SessionKind::Remote {
            debug_assert!(
                matches!(authoring, Authoring::Local),
                "Authoring::Remote is only valid for the host arm",
            );
            let bridge = self.bridge.as_ref().ok_or_else(|| {
                SessionError::Internal("remote send requires gossip bridge".into())
            })?;
            let send_payload = artel_protocol::rpc::SendPayload {
                kind,
                action,
                payload,
            };
            return match bridge.send_remote(session, peer, send_payload).await {
                Ok(message) => Ok(message),
                Err(crate::gossip_bridge::BridgeError::HostRejected(err)) => {
                    Err(SessionError::HostRejected(err))
                }
                Err(crate::gossip_bridge::BridgeError::SendTimeout) => Err(SessionError::Internal(
                    "send_remote: timed out waiting for host ack".into(),
                )),
                Err(crate::gossip_bridge::BridgeError::UnknownSession(_)) => Err(
                    SessionError::Internal("send_remote: bridge missing session topic".into()),
                ),
                Err(crate::gossip_bridge::BridgeError::Iroh(msg)) => {
                    Err(SessionError::Internal(format!("gossip: {msg}")))
                }
                // `send_remote` doesn't take a wire-form addr, so
                // `InvalidAddr` shouldn't surface here today; map it
                // defensively the same way [`Self::join`] does so a
                // future refactor that funnels addr-validation
                // through `send_remote` doesn't silently flatten it.
                Err(crate::gossip_bridge::BridgeError::InvalidAddr(msg)) => {
                    Err(SessionError::InvalidAddr(msg))
                }
            };
        }
        #[cfg(not(feature = "iroh"))]
        if kind_snapshot == SessionKind::Remote {
            // Without the iroh feature we have no transport; remote
            // sessions can't even be materialised here, but keep the
            // arm for completeness.
            return Err(SessionError::NotHost);
        }

        // L2 capability enforcement (Auth Slice C). Rejects an author who
        // can't write *here* — before the message is signed, sequenced,
        // or appended (O1: drop-before-append). See `ensure_can_write`.
        ensure_can_write(&session_arc, peer.id).await?;

        // Local-host arm. Either we authored the body ourselves
        // (`Authoring::Local`: stamp now_ms + sign with our key) or a
        // joiner authored it and we're appending on their behalf
        // (`Authoring::Remote`: verify their signature against the
        // body's peer.id BEFORE assigning seq + appending). The two
        // arms split here so the typing makes "did we sign vs did
        // they sign" explicit.
        let (timestamp_ms, signature) =
            match self.resolve_authoring(authoring, &peer, kind, &action, &payload, session) {
                Ok(pair) => pair,
                Err(err) => return Err(err),
            };

        let mut s = session_arc.lock().await;
        // Build the message under the session lock (so seq is stable),
        // then persist before bumping in-memory state and fanning out.
        // If the store fails, head and log are unchanged; the request
        // is rejected, the client gets a Storage error.
        //
        // We compute the prospective seq without committing it. If the
        // store write succeeds we commit; if not, we leave head alone.
        let prospective = s.head.next().expect("seq overflow");
        // Host sequencing signature (Auth Slice B.5, D1): bind *this seq*
        // to *this author signature* under our (the host's) key. Stamped
        // for BOTH authoring arms — our own `Authoring::Local` sends and
        // the joiner `Authoring::Remote` re-sequences — since this is the
        // single logged seq-assigning site. Under no-iroh / no-key we
        // emit the sentinel (lit-fuse posture, matching `author_local`).
        let host_sig = self.sign_seq_for(session, prospective, &signature);
        let message = SessionMessage::new(
            prospective,
            timestamp_ms,
            peer,
            kind,
            action,
            payload,
            signature,
            host_sig,
        );
        if let Err(err) = self.store.append(session, &message).await {
            return Err(SessionError::Storage(err));
        }
        s.head = prospective;
        s.log.push(message.clone());
        // Advance the projected cap set if this was a grant/revoke
        // (Auth Slice C). Order matches the seq-order discipline:
        // append to the log first, then project. The author already
        // passed the can_write gate above, so the authority check
        // inside `apply_capability` is satisfied for a legitimate grant.
        if message.kind == MessageKind::Capability {
            s.apply_capability(&message);
        }

        // Snapshot the broadcast handle so we can drop the per-session
        // lock before fanning out — `broadcast::send` is cheap but
        // there's no reason to hold the session mutex across it.
        let events_tx = s.events_tx.clone();
        drop(s);
        let _ = events_tx.send(Event::Message {
            session,
            message: message.clone(),
        });

        // Forward to remote joiners over gossip. Best-effort: if the
        // bridge isn't available (no iroh, or it errored), the local
        // fan-out has already happened so IPC clients are served.
        //
        // `Capability` messages are HOST-PRIVATE in v1 and never leave
        // the host (Auth Slice C delivery rethink, D3). v1 enforces caps
        // host-only — joiners neither enforce nor project a cap-set — so
        // a joiner has no use for a Grant/Revoke. Keeping them off the
        // wire entirely (not just off live fanout, but off the Replay
        // path too — see `log_since`) means Slice C adds ZERO
        // joiner-visible gossip traffic, so it cannot perturb iroh-gossip's
        // plumtree eager/lazy tree and cannot expose the latent
        // close-vs-teardown race. The grant still lives in the host log
        // (persisted, projected, replayed on resume, audit / P2P-ready).
        // ⚠️ FOR P2P: caps MUST propagate to peers — remove this
        // host-private gate and deliver capability events as part of the
        // (causal) log sync. See
        // docs/brainstorms/2026-06-04-auth-slice-c-l2-delivery-rethink-brainstorm.md
        #[cfg(feature = "iroh")]
        if message.kind != MessageKind::Capability
            && let Some(bridge) = &self.bridge
        {
            bridge.publish_message(session, message.clone()).await;
        }

        Ok(message)
    }

    /// Branchpoint between the [`Authoring::Local`] sign-now and
    /// [`Authoring::Remote`] verify-now arms. Builds canonical bytes
    /// once and either signs (Local) or verifies (Remote); returns
    /// the `(timestamp_ms, signature)` pair the [`SessionMessage`]
    /// will carry.
    fn resolve_authoring(
        &self,
        authoring: Authoring,
        peer: &PeerInfo,
        kind: MessageKind,
        action: &str,
        payload: &[u8],
        session: SessionId,
    ) -> Result<(u64, SigBytes), SessionError> {
        // Take by value because `Authoring::Remote` carries a 64-byte
        // signature we move into the verify path; passing by reference
        // would force a clone on every joiner-authored body.
        match authoring {
            Authoring::Local => Ok(self.author_local(peer, kind, action, payload, session)),
            #[cfg(feature = "iroh")]
            Authoring::Remote {
                timestamp_ms,
                signature,
            } => Self::author_remote(
                timestamp_ms,
                signature,
                peer,
                kind,
                action,
                payload,
                session,
            ),
        }
    }

    /// Host sequencing signature over `"artel/seq-v1" || session_id ||
    /// seq || author_sig` (Auth Slice B.5, D1). Called once per
    /// host-sequenced message at the prospective seq, for both
    /// authoring arms. Under no-iroh / no-key returns the sentinel —
    /// the same lit-fuse posture as [`Self::author_local`]: a joiner's
    /// `verify_seq` rejects the sentinel loudly once enforcement is on.
    fn sign_seq_for(&self, session: SessionId, seq: Seq, author_sig: &SigBytes) -> SigBytes {
        #[cfg(feature = "iroh")]
        {
            self.signing_key.as_ref().map_or(SIGNATURE_UNSIGNED, |key| {
                signing::sign_seq(key.as_signing_key(), session, seq, author_sig)
            })
        }
        #[cfg(not(feature = "iroh"))]
        {
            let _ = (session, seq, author_sig);
            SIGNATURE_UNSIGNED
        }
    }

    /// Local-arm authoring: stamp `now_ms()`, sign with the daemon's
    /// own key. Under `cfg(not(feature = "iroh"))` the registry has
    /// no signing key — there's no wire surface to defend, so we
    /// emit the unsigned sentinel. Under `cfg(feature = "iroh")` a
    /// `None` signing key is a wiring bug: production paths feed
    /// [`crate::server::IrohRuntime::signing_key`] in, and only the
    /// test-only `Registry::new` constructor leaves it `None`. We
    /// keep that test path working by falling back to the sentinel,
    /// which the verifier rejects loudly the moment any receive
    /// path actually verifies.
    fn author_local(
        &self,
        peer: &PeerInfo,
        kind: MessageKind,
        action: &str,
        payload: &[u8],
        session: SessionId,
    ) -> (u64, SigBytes) {
        let timestamp_ms = now_ms();
        #[cfg(feature = "iroh")]
        let signature = self.signing_key.as_ref().map_or(SIGNATURE_UNSIGNED, |key| {
            signing::sign_body(
                key.as_signing_key(),
                session,
                MESSAGE_FORMAT,
                timestamp_ms,
                peer,
                kind,
                action,
                payload,
            )
        });
        #[cfg(not(feature = "iroh"))]
        let signature = {
            // No iroh feature → no wire surface; signing has nothing
            // to defend at this layer. L4 will close this gap when
            // it lands.
            let _ = (session, peer, kind, action, payload);
            SIGNATURE_UNSIGNED
        };
        (timestamp_ms, signature)
    }

    /// Remote-arm authoring: trust the joiner's stamp + signature
    /// but verify before append. Failure returns
    /// [`SessionError::SignatureRejected`] so `run_host_send` can
    /// translate it into a [`artel_protocol::ProtocolError::Signature`]
    /// in the `SendAck` (rather than the joiner timing out).
    #[cfg(feature = "iroh")]
    fn author_remote(
        timestamp_ms: u64,
        signature: SigBytes,
        peer: &PeerInfo,
        kind: MessageKind,
        action: &str,
        payload: &[u8],
        session: SessionId,
    ) -> Result<(u64, SigBytes), SessionError> {
        // Mock up a SessionMessage shape so we can reuse
        // `verify_message` — seq is excluded from the canonical
        // bytes so any value works. This candidate is discarded after
        // the author-sig check, so host_sig is irrelevant here
        // (SIGNATURE_UNSIGNED); the real host_sig is stamped in
        // `Registry::send` once the host assigns the seq.
        let candidate = SessionMessage::new(
            Seq::ZERO,
            timestamp_ms,
            peer.clone(),
            kind,
            action.to_string(),
            payload.to_vec(),
            signature,
            SIGNATURE_UNSIGNED,
        );
        match signing::verify_message(session, &candidate, &signature) {
            Ok(()) => Ok((timestamp_ms, signature)),
            Err(err) => Err(SessionError::SignatureRejected {
                peer_id: peer.id,
                reason: verify_reason(&err).to_string(),
            }),
        }
    }

    /// Persist (or overwrite) a consumer-tagged attachment against
    /// `session`. The daemon never inspects `payload`; consumers
    /// (e.g. `artel-fs`) namespace `kind` and ship a postcard-encoded
    /// blob.
    ///
    /// Returns [`SessionError::UnknownSession`] when `session` is not
    /// known to the daemon. Idempotent within a `(session, kind)`
    /// pair: re-registering overwrites. Attachments cascade-delete
    /// with their session — see [`crate::store::SessionStore::delete`].
    ///
    /// Holds the per-session `Mutex<Session>` across the store write
    /// so a concurrent [`Self::leave`] (host) or
    /// [`Self::host_closed_session`] (remote mirror) cannot run its
    /// cascade between our existence check and the put — that would
    /// orphan the attachment. This is the synchronization point the
    /// store's [`crate::store::SessionStore::put_attachment`] doc
    /// references.
    pub(crate) async fn register_attachment(
        &self,
        session: SessionId,
        kind: String,
        payload: Vec<u8>,
    ) -> Result<(), SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };
        let _s = session_arc.lock().await;
        match self.store.put_attachment(session, &kind, &payload).await {
            Ok(true) => Ok(()),
            // The session existed when we took the lock but the store
            // disagrees — treat as UnknownSession; this is the only
            // way `Ok(false)` is reachable now.
            Ok(false) => Err(SessionError::UnknownSession(session)),
            Err(err) => Err(SessionError::Storage(err)),
        }
    }

    /// List every attachment matching `kind_filter`. `None` returns
    /// all kinds across all sessions; `Some(k)` returns only those
    /// tagged with `k`. Order unspecified.
    ///
    /// Does not take per-session locks — a concurrent register or
    /// cascade may shift the result by one entry but cannot produce
    /// a torn read of any individual attachment (each store op is
    /// itself atomic at the file/map-entry granularity).
    pub(crate) async fn list_attachments(
        &self,
        kind_filter: Option<&str>,
    ) -> Result<Vec<StoredAttachment>, SessionError> {
        self.store
            .list_attachments(kind_filter)
            .await
            .map_err(SessionError::Storage)
    }

    /// Remove the attachment at `(session, kind)` without removing
    /// the session itself. Idempotent: missing session OR missing
    /// attachment returns `Ok(())`.
    ///
    /// Holds the per-session lock when the session is known so a
    /// concurrent register cannot resurrect the attachment between
    /// our delete and the caller observing the empty list. If the
    /// session is already gone (cascade ran), the store's idempotent
    /// `delete_attachment` returns `Ok(())` directly.
    pub(crate) async fn forget_attachment(
        &self,
        session: SessionId,
        kind: String,
    ) -> Result<(), SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard.get(&session).cloned()
        };
        let _maybe_lock = match &session_arc {
            Some(arc) => Some(arc.lock().await),
            None => None,
        };
        self.store
            .delete_attachment(session, &kind)
            .await
            .map_err(SessionError::Storage)
    }

    /// Subscribe to live events for `session`, optionally backfilling
    /// every message with `seq > since` first.
    ///
    /// When the session holds a persisted workspace ticket envelope
    /// (revoked-lurker fix), a synthetic `TICKET_ACTION` System
    /// message reconstructed from it is **prepended** to the replay
    /// set — this is what makes late attach and joiner-daemon restart
    /// work (`Workspace::join_with` issues `Subscribe { since: None }`
    /// and drains for `TICKET_ACTION`; the live unicast may have
    /// happened before the workspace attached). IPC-subscribe only:
    /// the envelope never enters [`Self::log_since`] (the gossip
    /// replay surface) — re-broadcasting the capability on the topic
    /// is exactly the leak this slice closes.
    pub async fn subscribe(
        &self,
        session: SessionId,
        since: Option<Seq>,
    ) -> Result<Subscription, SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };

        let s = session_arc.lock().await;
        let cutoff = since.unwrap_or(Seq::ZERO);
        let mut replay: Vec<SessionMessage> = Vec::with_capacity(s.log.len() + 1);
        if let Some(envelope) = &s.workspace_ticket {
            replay.push(synthetic_ticket_message(s.host, envelope.clone()));
        }
        // Log-borne TICKET_ACTION entries are filtered: the synthetic
        // message above (from the persisted unicast copy) is the only
        // sanctioned source. A log entry carrying the action is a
        // peer-authored broadcast — surfacing it would let any RW
        // member drive a joiner's `wait_for_ticket` with a forged
        // envelope (revoked-lurker fix, legacy-broadcast hard-reject).
        replay.extend(
            s.log
                .iter()
                .filter(|m| {
                    m.seq > cutoff && !(m.kind == MessageKind::System && m.action == TICKET_ACTION)
                })
                .cloned(),
        );
        let events = s.events_tx.subscribe();
        drop(s);
        Ok(Subscription { replay, events })
    }

    /// Emit a synthetic upgrade event into a session's broadcast channel.
    ///
    /// Called by [`crate::upgrade_protocol::UpgradeProtocol`] when the
    /// host delivers a `NamespaceSecret` over a direct stream. The
    /// synthetic event has `kind: System`, `action: UPGRADE_ACTION`, and
    /// a postcard-encoded payload matching the `UpgradePayload` shape
    /// that `artel-fs`'s `cap_listener` already processes.
    ///
    /// Validates:
    /// - Session exists locally.
    /// - Session is `Remote` (we are a joiner, not the host).
    /// - `sender_peer` matches the session's host.
    #[cfg(feature = "iroh")]
    pub(crate) async fn emit_upgrade(
        &self,
        session: SessionId,
        sender_peer: PeerId,
        namespace_secret: [u8; 32],
    ) -> Result<(), SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };

        let s = session_arc.lock().await;

        // Only joiners (Remote sessions) should accept upgrades. A
        // Local session IS the host, so it has nothing to receive —
        // surface that directly rather than the host-side `NotHost`,
        // whose message is the semantic opposite of what happened.
        if s.kind != SessionKind::Remote {
            return Err(SessionError::Internal(
                "host sessions cannot receive upgrades".into(),
            ));
        }

        // The sender must be the session's host.
        if sender_peer != s.host {
            return Err(SessionError::Internal(format!(
                "upgrade sender {sender_peer} is not the session host {}",
                s.host,
            )));
        }

        let payload = postcard::to_allocvec(&UpgradePayload {
            target_peer: self.daemon_peer_id,
            namespace_secret,
        })
        .expect("UpgradePayload is infallible to serialize");

        // Synthetic, non-persisted event: `Seq::ZERO`, no `store.append`,
        // and excluded from replay (see the `UPGRADE_ACTION` skip in the
        // subscribe path). It is delivered live only.
        //
        // INVARIANT: upgrades are *not* replayable. A joiner that misses
        // this live event (e.g. restarts between broadcast and process)
        // does not recover it from the log — it relies on the host
        // re-delivering on reconnect (the `Event::PeerJoined` handler in
        // `artel-fs`'s `cap_listener` re-issues `DeliverUpgrade`). If
        // upgrades ever need to survive a joiner restart on their own
        // (e.g. offline promotion), this must become a sequenced,
        // persisted message instead.
        let message = SessionMessage::new(
            Seq::ZERO,
            0,
            PeerInfo::new(s.host, "host"),
            MessageKind::System,
            UPGRADE_ACTION,
            payload,
            SIGNATURE_UNSIGNED,
            SIGNATURE_UNSIGNED,
        );

        let events_tx = s.events_tx.clone();
        drop(s);

        // Best-effort: if no subscribers are listening, that's fine.
        let _ = events_tx.send(Event::Message { session, message });

        Ok(())
    }

    /// Accept a workspace ticket envelope delivered host→peer over
    /// the direct stream (revoked-lurker fix): persist it on the
    /// mirror record, then emit a **live** synthetic `TICKET_ACTION`
    /// System message — the same construction [`Self::emit_upgrade`]
    /// uses for `UPGRADE_ACTION`, but persisted, so [`Self::subscribe`]
    /// also replays it (late attach / joiner restart).
    ///
    /// Validates like `emit_upgrade`:
    /// - Session exists locally.
    /// - Session is `Remote` (we are a joiner, not the host).
    /// - `sender_peer` matches the session's host.
    ///
    /// Idempotent on a re-delivery of identical bytes (admission
    /// redelivery, leave-then-rejoin): the persist and the re-emit
    /// are both skipped, so the joiner's event stream isn't spammed.
    #[cfg(feature = "iroh")]
    pub(crate) async fn emit_workspace_ticket(
        &self,
        session: SessionId,
        sender_peer: PeerId,
        envelope_bytes: Vec<u8>,
    ) -> Result<(), SessionError> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard
                .get(&session)
                .cloned()
                .ok_or(SessionError::UnknownSession(session))?
        };

        let mut s = session_arc.lock().await;

        if s.kind != SessionKind::Remote {
            return Err(SessionError::Internal(
                "host sessions cannot receive workspace tickets".into(),
            ));
        }

        if sender_peer != s.host {
            return Err(SessionError::Internal(format!(
                "workspace ticket sender {sender_peer} is not the session host {}",
                s.host,
            )));
        }

        if s.workspace_ticket.as_deref() == Some(envelope_bytes.as_slice()) {
            // Identical re-delivery; already persisted and already
            // surfaced (live or via Subscribe replay). Ack quietly.
            return Ok(());
        }

        // Store-before-memory, holding the per-session lock across
        // the write like every other record mutation.
        self.store
            .put_workspace_ticket(session, &envelope_bytes)
            .await
            .map_err(SessionError::Storage)?;
        s.workspace_ticket = Some(envelope_bytes.clone());

        let message = synthetic_ticket_message(s.host, envelope_bytes);
        let events_tx = s.events_tx.clone();
        drop(s);

        // Best-effort: if no subscribers are listening (workspace
        // not attached yet), the persisted copy serves them on their
        // eventual Subscribe.
        let _ = events_tx.send(Event::Message { session, message });

        Ok(())
    }

    /// Persist the host workspace's ticket envelope and deliver it to
    /// every current member over the direct stream (revoked-lurker
    /// fix, host side). `Local`-only — a mirror returns `NotHost`.
    ///
    /// Per-member delivery is best-effort (warn on failure): an
    /// offline member is covered by admission-redelivery the moment
    /// its re-announce runs [`Self::ensure_member`].
    #[cfg(feature = "iroh")]
    pub(crate) async fn publish_workspace_ticket(
        &self,
        session: SessionId,
        envelope_bytes: Vec<u8>,
    ) -> Result<(), SessionError> {
        let members = {
            let mut s = self.lock_local_session(session).await?;
            // Store-before-memory under the session lock.
            self.store
                .put_workspace_ticket(session, &envelope_bytes)
                .await
                .map_err(SessionError::Storage)?;
            s.workspace_ticket = Some(envelope_bytes.clone());
            let members: Vec<PeerId> = s.members.iter().copied().collect();
            drop(s);
            members
        };

        for member in members {
            self.deliver_workspace_ticket_to(session, member, envelope_bytes.clone())
                .await;
        }
        Ok(())
    }

    /// Whether `peer` is currently a member of `session`. `None` if
    /// the session is unknown. Read-only accessor for the gossip
    /// bridge's membership-gated `Replay` (mirrors
    /// [`Self::is_local_session`]'s lock discipline).
    #[cfg(feature = "iroh")]
    pub(crate) async fn is_member(&self, session: SessionId, peer: PeerId) -> Option<bool> {
        let session_arc = {
            let guard = self.sessions.read().await;
            guard.get(&session)?.clone()
        };
        let s = session_arc.lock().await;
        Some(s.members.contains(&peer))
    }
}

/// Returns the canonical `&'static str` for `action` if it is a
/// reserved daemon-injected System action, else `None`.
///
/// `workspace.ticket` and `workspace.upgrade` are minted only by the
/// daemon (synthetic, unsigned, off-log messages built from unicast
/// capability material); a member must never author them. Returning
/// the `'static` form lets the rejection name the action without
/// allocating. This is the single source of truth for "reserved
/// action" — the log-filter sites match the same two constants.
fn reserved_system_action(action: &str) -> Option<&'static str> {
    match action {
        TICKET_ACTION => Some(TICKET_ACTION),
        UPGRADE_ACTION => Some(UPGRADE_ACTION),
        _ => None,
    }
}

/// Build the synthetic `TICKET_ACTION` System message that surfaces a
/// unicast-delivered workspace ticket envelope to the IPC subscriber.
/// Host-stamped (`PeerInfo::new(host, "host")` — `wait_for_ticket`
/// reads `message.peer.id` as the host's daemon id), `Seq::ZERO`,
/// unsigned: it is not a log entry, exists only on the IPC surface,
/// and must never ride the gossip topic. Shared by the live emit
/// (`emit_workspace_ticket`) and the `Subscribe` replay injection.
fn synthetic_ticket_message(host: PeerId, envelope_bytes: Vec<u8>) -> SessionMessage {
    SessionMessage::new(
        Seq::ZERO,
        0,
        PeerInfo::new(host, "host"),
        MessageKind::System,
        TICKET_ACTION,
        envelope_bytes,
        SIGNATURE_UNSIGNED,
        SIGNATURE_UNSIGNED,
    )
}

/// Parse an artel join ticket. Phase 2b: returns the session id; the
/// host peer id is decoded but not yet used (Phase 2c will route on
/// it). Any decode failure surfaces as [`SessionError::InvalidTicket`]
/// so the daemon doesn't leak parser internals over the wire.
fn parse_ticket(ticket: &JoinTicket) -> Result<SessionTicket, SessionError> {
    ticket::decode(ticket.as_str()).map_err(|err| {
        // Log the underlying TicketError at debug; the wire-facing
        // error stays generic so version-mismatch doesn't double as
        // an oracle.
        tracing::debug!(?err, "ticket decode failed");
        SessionError::InvalidTicket
    })
}

/// Apply an inbound `Message` to a `Remote` mirror, with the full
/// joiner-side acceptance pipeline (Auth Slice B.5.3): **dedup →
/// author signature → host sequencing signature → persist → emit**.
///
/// Extracted from `materialise_remote_session`'s `on_message` closure
/// so the ordering invariant is unit-testable without a live gossip
/// bridge. The order is load-bearing:
/// - **Dedup first** so routine at-least-once / replay-backfill
///   duplicates don't re-pay ed25519 (review-fix #5).
/// - **Author sig** (`verify_message`) against the body's `peer.id`.
/// - **Host seq-sig** (`verify_seq`) against `s.host` (= the ticket's
///   `host_peer_id`), binding *this seq* to *this author sig* — so a
///   genuine frame replayed under a different seq (finding #1) drops.
///
/// **No watermark interaction here** — the `host_epoch` watermark is
/// moved only by the bridge's `EpochBeacon` arm. A genuine `Message`
/// replayed on an unseen seq is dropped by `verify_seq` and never
/// touches the watermark (`replayed_message_cannot_poison_watermark`).
#[cfg(feature = "iroh")]
async fn apply_inbound_mirror_message(
    store: &DynStore,
    mirror: &Arc<Mutex<Session>>,
    session: SessionId,
    msg: SessionMessage,
) {
    let mut s = mirror.lock().await;
    // Dedup BEFORE verifying. iroh-gossip delivers a frame
    // at-least-once (mesh redundancy) and the host re-broadcasts the
    // whole log on `Replay`, so duplicate seqs are routine. The
    // signature checks are ed25519 scalar-mults; running them on a
    // frame we already hold is wasted crypto. We still verify every
    // *new* frame below, before it touches disk or the in-memory log.
    let pos = s.log.partition_point(|m| m.seq < msg.seq);
    if pos < s.log.len() && s.log[pos].seq == msg.seq {
        // Duplicate seq; drop quietly. Already on disk from the first
        // delivery.
        return;
    }
    // Verify the body's author signature against its claimed peer.id
    // before any state mutation. A tampered `Message` drops here with
    // a warn and never touches the mirror's log. seq is excluded from
    // the author signed scope so the host's stamp doesn't matter for
    // *this* check; we verify the joiner's authorship directly.
    if let Err(err) = signing::verify_message(session, &msg, &msg.signature) {
        tracing::warn!(
            ?session,
            seq = ?msg.seq,
            peer = %msg.peer.id,
            ?err,
            "dropping inbound Message: signature verify failed",
        );
        return;
    }
    // Then verify the HOST's sequencing signature over `(session, seq,
    // author_sig)` against the host pubkey (`s.host` = the ticket's
    // host_peer_id). This binds *this seq* to *this body* under the
    // host key, so a genuine frame replayed under a different seq
    // (finding #1) is dropped — the captured host_sig is bound to the
    // original seq.
    if let Err(err) = signing::verify_seq(&s.host, session, msg.seq, &msg.signature, &msg.host_sig)
    {
        tracing::warn!(
            ?session,
            seq = ?msg.seq,
            host = %s.host,
            ?err,
            "dropping inbound Message: host seq-sig verify failed",
        );
        return;
    }
    // ───────────────────────────────────────────────────────────────
    // NO L2 CAPABILITY ENFORCEMENT HERE — THIS IS DELIBERATE (v1).
    //
    // An earlier cut of Slice C re-enforced the cap rule on every
    // joiner mirror ("every peer enforces locally"). That was removed:
    // in v1's star topology the HOST is the sole sequencer, so the host's
    // `Registry::send` cap-gate is the mandatory enforcement chokepoint —
    // a joiner cannot get a message into the log without the host
    // sequencing it. Re-enforcing here only defended against a malicious
    // host forging a write, which v1 puts out of scope ("host-as-sequencer
    // trust", parent threat model). It also required deriving the cap-set
    // at-seq from Grant/Revoke events arriving over iroh-gossip's epidemic
    // delivery — which is neither timely nor ordered — and that fragility
    // caused a lost-SessionClosed flake and a real grant-ordering race.
    //
    // ⚠️ FOR SYMMETRIC-P2P: per-peer enforcement becomes MANDATORY (no
    // host to delegate to). Re-add it here ONLY against causal history
    // (hash-linked DAG + project-at-merge), NEVER against this live
    // host-sequenced stream — a message causally depending on its
    // authorizing Grant makes the ordering race impossible by
    // construction. See
    // docs/brainstorms/2026-06-04-auth-slice-c-l2-delivery-rethink-brainstorm.md
    // ───────────────────────────────────────────────────────────────

    // Persist BEFORE mutating in-memory state so a crash mid-callback
    // doesn't leave the mirror's in-memory log ahead of disk. Persist
    // while holding the lock so a concurrent remote-mirror cascade
    // (`leave` of the joiner) can't race us. Failure: log, drop the
    // message; the host re-broadcasts on Replay if the joiner asks
    // again, and the in-memory state stays consistent with disk.
    if let Err(err) = store.append(session, &msg).await {
        tracing::warn!(
            ?session,
            seq = ?msg.seq,
            error = %err,
            "remote-mirror log persist failed; dropping message",
        );
        return;
    }
    s.log.insert(pos, msg.clone());
    if msg.seq > s.head {
        s.head = msg.seq;
    }
    // Log-borne TICKET_ACTION is never surfaced live to IPC
    // subscribers (revoked-lurker fix): the genuine envelope arrives
    // over the host→peer unicast and is injected from the persisted
    // copy only; a gossip Message carrying the action is a stale or
    // forged broadcast and must not drive a joiner's
    // `wait_for_ticket`. The entry stays in the mirror log (seq
    // continuity for dedup); `subscribe` applies the same filter, so
    // it is inert on every joiner-visible surface.
    if msg.kind == MessageKind::System && msg.action == TICKET_ACTION {
        tracing::warn!(
            ?session,
            seq = ?msg.seq,
            peer = %msg.peer.id,
            "suppressing log-borne TICKET_ACTION broadcast (unicast is the only sanctioned source)",
        );
        return;
    }
    let _ = s.events_tx.send(Event::Message {
        session,
        message: msg,
    });
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use artel_protocol::Event;
    use pretty_assertions::assert_eq;
    use tokio::time::timeout;

    use super::*;

    fn peer(byte: u8, name: &str) -> PeerInfo {
        PeerInfo::new(PeerId::from_bytes([byte; 32]), name)
    }

    fn registry() -> Registry {
        registry_with_peer(PeerId::from_bytes([0xff; 32]))
    }

    fn registry_with_peer(daemon_peer_id: PeerId) -> Registry {
        let store: DynStore = Arc::new(crate::store::MemoryStore::new());
        Registry::new(daemon_peer_id, store)
    }

    impl Registry {
        async fn host_rw(
            &self,
            host_peer: PeerInfo,
            requested_id: Option<SessionId>,
        ) -> Result<(SessionId, JoinTicket), SessionError> {
            self.host(host_peer, requested_id, Capability::ReadWrite, 0)
                .await
                .map(|(id, ticket, _tid)| (id, ticket))
        }

        /// Push a message straight onto a session's in-memory log,
        /// bypassing `send`. Simulates an entry that reached the log
        /// without passing the ingress gate — a peer-authored
        /// broadcast a mixed-build/stale host sequenced, or a pre-fix
        /// relic — which is exactly what the downstream log filters
        /// (`log_since`, `subscribe`, `apply_inbound_mirror_message`)
        /// exist to neutralise. `send` now rejects reserved actions at
        /// ingress, so tests for those defense-in-depth filters can no
        /// longer plant their fixtures through `send`.
        async fn inject_log_entry(&self, session: SessionId, message: SessionMessage) {
            let session_arc = {
                let guard = self.sessions.read().await;
                guard.get(&session).cloned().expect("session exists")
            };
            let mut s = session_arc.lock().await;
            s.head = message.seq;
            s.log.push(message);
        }
    }

    /// Build an unsigned `SessionMessage` fixture for `inject_log_entry`.
    fn log_fixture(
        seq: u64,
        author: &PeerInfo,
        kind: MessageKind,
        action: &str,
        payload: Vec<u8>,
    ) -> SessionMessage {
        SessionMessage::new(
            Seq::new(seq),
            seq,
            author.clone(),
            kind,
            action,
            payload,
            SIGNATURE_UNSIGNED,
            SIGNATURE_UNSIGNED,
        )
    }

    // ---- host ----

    #[tokio::test]
    async fn host_creates_session_and_returns_artel_ticket() {
        let daemon_peer = PeerId::from_bytes([0xff; 32]);
        let r = registry_with_peer(daemon_peer);
        let (id, ticket) = r.host_rw(peer(1, "alice"), None).await.unwrap();
        assert!(ticket.as_str().starts_with("artel:"));
        // The ticket round-trips and embeds this daemon's identity.
        let decoded = ticket::decode(ticket.as_str()).unwrap();
        assert_eq!(decoded.session_id, id);
        assert_eq!(decoded.host_peer_id, daemon_peer);
        // Without an iroh runtime the addr is id-only — the daemon
        // is local-only in this test path.
        assert_eq!(decoded.host_addr.peer_id, daemon_peer);
        assert!(decoded.host_addr.relay_url.is_empty());
        assert!(decoded.host_addr.direct_addrs.is_empty());
        let summaries = r.list().await;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, id);
        assert_eq!(summaries[0].peer_count, 1);
        assert_eq!(summaries[0].last_seq, None);
    }

    #[tokio::test]
    async fn host_with_read_cap_produces_read_ticket() {
        let r = registry();
        let (_, ticket, _) = r
            .host(peer(1, "alice"), None, Capability::Read, 0)
            .await
            .unwrap();
        let decoded = ticket::decode(ticket.as_str()).unwrap();
        assert_eq!(decoded.granted_cap, Capability::Read);
        assert_eq!(decoded.expiry_ms, 0);
    }

    #[tokio::test]
    async fn host_with_expiry_produces_ticket_with_expiry_ms() {
        let r = registry();
        let expiry = 1_700_000_000_000u64;
        let (_, ticket, _) = r
            .host(peer(1, "alice"), None, Capability::ReadWrite, expiry)
            .await
            .unwrap();
        let decoded = ticket::decode(ticket.as_str()).unwrap();
        assert_eq!(decoded.granted_cap, Capability::ReadWrite);
        assert_eq!(decoded.expiry_ms, expiry);
    }

    #[tokio::test]
    async fn host_with_some_id_creates_session_at_that_id() {
        // First-time host with a caller-supplied id and no
        // pre-existing record. The id propagates verbatim and is
        // persisted at that id.
        let r = registry();
        let alice = peer(1, "alice");
        let chosen = SessionId::from_bytes([0xab; 16]);
        let (id, _ticket) = r.host_rw(alice.clone(), Some(chosen)).await.unwrap();
        assert_eq!(id, chosen);
        let summaries = r.list().await;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, chosen);
    }

    #[tokio::test]
    async fn host_with_some_id_resumes_existing_local_session() {
        // Pre-seed a Local-host record (mimicking what daemon
        // restart would rehydrate from disk), then resume via
        // host(peer, Some(id)). Members, log, and head should be
        // preserved verbatim; the ticket should re-stamp from the
        // current daemon_addr.
        let daemon_peer = PeerId::from_bytes([0xff; 32]);
        let alice = peer(1, "alice");
        let bob = peer(2, "bob");
        let session_id = SessionId::from_bytes([0xcd; 16]);

        let store = Arc::new(crate::store::MemoryStore::new());
        let log = vec![SessionMessage::new(
            Seq::new(1),
            42,
            alice.clone(),
            MessageKind::Chat,
            String::from("hello"),
            b"world".to_vec(),
            artel_protocol::message::SIGNATURE_UNSIGNED,
            artel_protocol::message::SIGNATURE_UNSIGNED,
        )];
        let record = SessionRecord {
            id: session_id,
            host: alice.id,
            members: HashSet::from([alice.id, bob.id]),
            head: Seq::new(1),
            log: log.clone(),
            kind: SessionKind::Local,
            host_epoch: 0,
            tickets: Vec::new(),
            workspace_ticket: None,
        };
        store.create(&record).await.unwrap();
        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            store,
            #[cfg(feature = "iroh")]
            None,
            #[cfg(feature = "iroh")]
            None,
            #[cfg(feature = "iroh")]
            None,
        )
        .await
        .unwrap();

        let (id, ticket) = r.host_rw(alice.clone(), Some(session_id)).await.unwrap();
        assert_eq!(id, session_id);

        // Ticket re-stamped with this daemon's current addr.
        let decoded = ticket::decode(ticket.as_str()).unwrap();
        assert_eq!(decoded.session_id, session_id);
        assert_eq!(decoded.host_peer_id, daemon_peer);

        // Resumed session keeps members, log, head verbatim. Replay
        // the log via Subscribe to confirm.
        let sub = r.subscribe(session_id, None).await.unwrap();
        assert_eq!(sub.replay, log);

        let summaries = r.list().await;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].peer_count, 2);
        assert_eq!(summaries[0].last_seq, Some(Seq::new(1)));
    }

    #[tokio::test]
    async fn host_with_some_id_rejects_when_host_differs() {
        // Existing local-host record at `id` belongs to alice; bob
        // tries to resume it. Must reject with SessionConflict and
        // leave the in-memory state alone.
        let daemon_peer = PeerId::from_bytes([0xff; 32]);
        let alice = peer(1, "alice");
        let bob = peer(2, "bob");
        let session_id = SessionId::from_bytes([0xcd; 16]);

        let store = Arc::new(crate::store::MemoryStore::new());
        let record = SessionRecord {
            id: session_id,
            host: alice.id,
            members: HashSet::from([alice.id]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Local,
            host_epoch: 0,
            tickets: Vec::new(),
            workspace_ticket: None,
        };
        store.create(&record).await.unwrap();
        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            store,
            #[cfg(feature = "iroh")]
            None,
            #[cfg(feature = "iroh")]
            None,
            #[cfg(feature = "iroh")]
            None,
        )
        .await
        .unwrap();

        let err = r.host_rw(bob, Some(session_id)).await.unwrap_err();
        assert_eq!(err, SessionError::SessionConflict(session_id));

        // Existing session is still present and still alice's.
        let summaries = r.list().await;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].peer_count, 1);
    }

    #[tokio::test]
    async fn host_with_some_id_rejects_when_kind_is_remote() {
        // Existing record at `id` is a Remote mirror — somebody
        // tries to "host" it locally. Must reject regardless of
        // whether the supplied peer matches the recorded host.
        let daemon_peer = PeerId::from_bytes([0xff; 32]);
        let remote_host = peer(1, "alice");
        let session_id = SessionId::from_bytes([0xcd; 16]);

        let store = Arc::new(crate::store::MemoryStore::new());
        let record = SessionRecord {
            id: session_id,
            host: remote_host.id,
            members: HashSet::from([remote_host.id]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Remote,
            host_epoch: 0,
            tickets: Vec::new(),
            workspace_ticket: None,
        };
        store.create(&record).await.unwrap();
        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            store,
            #[cfg(feature = "iroh")]
            None,
            #[cfg(feature = "iroh")]
            None,
            #[cfg(feature = "iroh")]
            None,
        )
        .await
        .unwrap();

        let err = r.host_rw(remote_host, Some(session_id)).await.unwrap_err();
        assert_eq!(err, SessionError::SessionConflict(session_id));
    }

    // ---- issue_ticket ----

    #[tokio::test]
    async fn issue_ticket_for_hosted_session_returns_ticket_with_requested_cap() {
        let r = registry();
        let (id, _) = r.host_rw(peer(1, "alice"), None).await.unwrap();

        let (ticket, _) = r.issue_ticket(id, Capability::Read, 0).await.unwrap();
        let decoded = ticket::decode(ticket.as_str()).unwrap();
        assert_eq!(decoded.session_id, id);
        assert_eq!(decoded.granted_cap, Capability::Read);
        assert_eq!(decoded.expiry_ms, 0);
    }

    #[tokio::test]
    async fn issue_ticket_with_expiry_propagates() {
        let r = registry();
        let (id, _) = r.host_rw(peer(1, "alice"), None).await.unwrap();

        let expiry = 1_700_000_000_000u64;
        let (ticket, _) = r
            .issue_ticket(id, Capability::ReadWrite, expiry)
            .await
            .unwrap();
        let decoded = ticket::decode(ticket.as_str()).unwrap();
        assert_eq!(decoded.granted_cap, Capability::ReadWrite);
        assert_eq!(decoded.expiry_ms, expiry);
    }

    #[tokio::test]
    async fn issue_ticket_for_unknown_session_returns_error() {
        let r = registry();
        let fake = SessionId::from_bytes([0xde; 16]);
        let err = r.issue_ticket(fake, Capability::Read, 0).await.unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(fake));
    }

    #[tokio::test]
    async fn issue_ticket_for_remote_session_returns_not_host() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();

        // Create a second registry (different daemon_peer_id) that joins
        // the same session — simulates a remote mirror.
        let r2 = registry_with_peer(PeerId::from_bytes([0xaa; 32]));
        // Manually seed a remote-mirror session in r2.
        {
            let session = Session::new(id, &host, SessionKind::Remote);
            r2.sessions
                .write()
                .await
                .insert(id, Arc::new(Mutex::new(session)));
        }
        let err = r2.issue_ticket(id, Capability::Read, 0).await.unwrap_err();
        assert_eq!(err, SessionError::NotHost);
    }

    // ---- join ----

    #[tokio::test]
    async fn join_artel_ticket_succeeds_and_emits_peer_joined() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, ticket) = r.host_rw(host, None).await.unwrap();

        // Subscribe before second peer joins so we observe the event.
        let mut sub = r.subscribe(id, None).await.unwrap();

        let joiner = peer(2, "bob");
        let (got_id, head) = r.join(&ticket, joiner.clone()).await.unwrap();
        assert_eq!(got_id, id);
        assert_eq!(head, None);

        let event = timeout(Duration::from_millis(100), sub.events.recv())
            .await
            .expect("event")
            .unwrap();
        match event {
            Event::PeerJoined { session, peer } => {
                assert_eq!(session, id);
                assert_eq!(peer, joiner);
            }
            other => panic!("expected PeerJoined, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn join_invalid_prefix_errors() {
        let r = registry();
        let err = r
            .join(&JoinTicket::from("iroh-fake:abc"), peer(2, "bob"))
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::InvalidTicket);
    }

    #[tokio::test]
    async fn join_legacy_artel_local_ticket_errors() {
        // Pre-2b strings are no longer accepted. We surface them as
        // InvalidTicket rather than UnknownSession so users get a
        // crisper signal when they paste old data.
        let r = registry();
        let bogus = SessionId::new_random();
        let err = r
            .join(
                &JoinTicket::from(format!("artel-local:{bogus}")),
                peer(2, "bob"),
            )
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::InvalidTicket);
    }

    #[tokio::test]
    async fn join_unknown_session_errors() {
        let r = registry();
        let bogus = SessionId::new_random();
        let host_peer_id = PeerId::from_bytes([0xff; 32]);
        let ticket = JoinTicket::from(ticket::encode(&SessionTicket {
            ticket_id: TicketId::new_random(),
            session_id: bogus,
            host_peer_id,
            host_addr: WireEndpointAddr::id_only(host_peer_id),
            granted_cap: Capability::ReadWrite,
            expiry_ms: 0,
            cap_sig: SIGNATURE_UNSIGNED,
        }));
        let err = r.join(&ticket, peer(2, "bob")).await.unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    #[tokio::test]
    async fn join_twice_is_idempotent() {
        let r = registry();
        let (id, ticket) = r.host_rw(peer(1, "alice"), None).await.unwrap();
        let (got_first, head_first) = r.join(&ticket, peer(2, "bob")).await.unwrap();
        // Second call must NOT error — it's a no-op for the same
        // authenticated id (auth L1 fix #3, idempotent self-rejoin).
        let (got_second, head_second) = r.join(&ticket, peer(2, "bob")).await.unwrap();
        assert_eq!(got_first, id);
        assert_eq!(got_second, id);
        assert_eq!(head_first, head_second);

        // Bob remains a single member of the session — the
        // idempotent path neither duplicates the entry nor races
        // through the store.
        let bob_id = peer(2, "bob").id;
        let session_arc = {
            let sessions = r.sessions.read().await;
            sessions.get(&id).expect("session exists").clone()
        };
        let (members, bob_count) = {
            let session = session_arc.lock().await;
            let count = session.members.iter().filter(|m| **m == bob_id).count();
            (session.members.clone(), count)
        };
        assert_eq!(bob_count, 1, "members: {members:?}");
    }

    #[tokio::test]
    async fn host_then_self_join_via_same_id_is_idempotent() {
        // Alice is the daemon's own peer (matches `registry()`'s
        // [0xff; 32]), so re-joining via her own ticket is the
        // self-rejoin case.
        let daemon_peer = PeerId::from_bytes([0xff; 32]);
        let r = registry();
        let alice = PeerInfo::new(daemon_peer, "alice");
        let (id, ticket) = r.host_rw(alice.clone(), None).await.unwrap();
        // Same authenticated id rejoining via the host's own ticket.
        let (got, head) = r.join(&ticket, alice.clone()).await.unwrap();
        assert_eq!(got, id);
        assert_eq!(head, None, "no messages yet, head should be None");

        // Membership unchanged: alice is still a single member.
        let session_arc = {
            let sessions = r.sessions.read().await;
            sessions.get(&id).expect("session exists").clone()
        };
        let (members, alice_count) = {
            let session = session_arc.lock().await;
            let count = session
                .members
                .iter()
                .filter(|m| **m == daemon_peer)
                .count();
            (session.members.clone(), count)
        };
        assert_eq!(alice_count, 1, "members: {members:?}");
    }

    // ---- send / sequencing ----

    #[tokio::test]
    async fn send_assigns_strictly_monotonic_seq() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();

        let s1 = r
            .send(
                id,
                host.clone(),
                MessageKind::Chat,
                "a".into(),
                vec![],
                Authoring::Local,
            )
            .await
            .unwrap();
        let s2 = r
            .send(
                id,
                host.clone(),
                MessageKind::Chat,
                "b".into(),
                vec![],
                Authoring::Local,
            )
            .await
            .unwrap();
        let s3 = r
            .send(
                id,
                host,
                MessageKind::Chat,
                "c".into(),
                vec![],
                Authoring::Local,
            )
            .await
            .unwrap();

        assert!(s1.seq < s2.seq);
        assert!(s2.seq < s3.seq);
        // First real seq is 1 (Seq::ZERO is reserved as "no messages").
        assert_eq!(s1.seq, Seq::new(1));
    }

    #[tokio::test]
    async fn send_by_non_member_errors() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host, None).await.unwrap();
        let err = r
            .send(
                id,
                peer(9, "intruder"),
                MessageKind::Chat,
                "x".into(),
                vec![],
                Authoring::Local,
            )
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::NotMember(id));
    }

    #[tokio::test]
    async fn send_to_unknown_session_errors() {
        let r = registry();
        let bogus = SessionId::new_random();
        let err = r
            .send(
                bogus,
                peer(1, "alice"),
                MessageKind::Chat,
                "x".into(),
                vec![],
                Authoring::Local,
            )
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    #[tokio::test]
    async fn send_emits_message_event() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();

        let mut sub = r.subscribe(id, None).await.unwrap();
        let sent = r
            .send(
                id,
                host.clone(),
                MessageKind::Chat,
                "hello".into(),
                b"world".to_vec(),
                Authoring::Local,
            )
            .await
            .unwrap();

        let event = timeout(Duration::from_millis(100), sub.events.recv())
            .await
            .expect("event")
            .unwrap();
        match event {
            Event::Message { session, message } => {
                assert_eq!(session, id);
                assert_eq!(message.seq, sent.seq);
                assert_eq!(message.action, "hello");
                assert_eq!(message.payload, b"world");
                assert_eq!(message.peer, host);
            }
            other => panic!("expected Message event, got {other:?}"),
        }
    }

    // ---- send / signing (Auth Slice B2) ----

    /// Build a registry with a known iroh `SecretKey` so the
    /// `Local` arm of [`Authoring`] actually signs. The matching
    /// `PeerId` is the daemon's own iroh public key — we use it as
    /// both the daemon peer id and the host of the test session so
    /// membership and signing-identity line up.
    #[cfg(feature = "iroh")]
    fn registry_with_signing_seed(seed: u8) -> (Registry, PeerInfo, Arc<iroh::SecretKey>) {
        let store: DynStore = Arc::new(crate::store::MemoryStore::new());
        registry_with_signing_seed_over(seed, store)
    }

    /// Like [`registry_with_signing_seed`] but over a caller-supplied
    /// `store`, so a test can drop the registry and `Registry::load` a
    /// fresh one over the SAME store to exercise the real cold-start
    /// replay path (the only path that reconstructs `caps` from the
    /// persisted log — Auth Slice C.3, the two-resume-path nuance: an
    /// in-process re-host leaves caps untouched in memory and proves
    /// nothing about replay).
    #[cfg(feature = "iroh")]
    fn registry_with_signing_seed_over(
        seed: u8,
        store: DynStore,
    ) -> (Registry, PeerInfo, Arc<iroh::SecretKey>) {
        let secret = Arc::new(iroh::SecretKey::from_bytes(&[seed; 32]));
        let endpoint_id = secret.public();
        let peer_id = PeerId::from_bytes(*endpoint_id.as_bytes());
        let r = Registry::new_with_signing_key(peer_id, store, Arc::clone(&secret));
        (r, PeerInfo::new(peer_id, "alice"), secret)
    }

    #[tokio::test]
    #[cfg(feature = "iroh")]
    async fn registry_send_local_signs_with_daemon_key() {
        // Pin the load-bearing signing path: a `Local`-arm send
        // must produce a `SessionMessage` whose signature verifies
        // against the body's peer.id (= the daemon's own pubkey).
        let (r, host, _secret) = registry_with_signing_seed(0x41);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let sent = r
            .send(
                id,
                host.clone(),
                MessageKind::Chat,
                "hello".into(),
                b"world".to_vec(),
                Authoring::Local,
            )
            .await
            .unwrap();
        // The signature must NOT be the sentinel — the local arm
        // signed with the registry's iroh key.
        assert_ne!(
            sent.signature,
            artel_protocol::message::SIGNATURE_UNSIGNED,
            "Local arm must replace the sentinel with a real signature",
        );
        // And it verifies against the body's peer.id over the
        // canonical bytes.
        signing::verify_message(id, &sent, &sent.signature)
            .expect("locally-signed message must verify");
    }

    #[tokio::test]
    #[cfg(feature = "iroh")]
    async fn registry_send_remote_authoring_verifies_signature_first() {
        // A `Remote`-arm body whose signature does NOT verify must
        // be rejected with `SignatureRejected`, not appended.
        let (r, host, _) = registry_with_signing_seed(0x41);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();

        // Build a body whose claimed peer is a real ed25519 key but
        // sign with a different key — that's the "BadSig" failure
        // mode.
        let real = artel_protocol::signing::SigningKey::from_bytes(&[0xaa; 32]);
        let other = artel_protocol::signing::SigningKey::from_bytes(&[0xbb; 32]);
        let claimed_peer = PeerInfo::new(
            PeerId::from_bytes(real.verifying_key().to_bytes()),
            "joiner",
        );
        let timestamp_ms = 1_700_000_000_000u64;
        let bad_sig = artel_protocol::signing::sign_body(
            &other,
            id,
            artel_protocol::message::MESSAGE_FORMAT,
            timestamp_ms,
            &claimed_peer,
            MessageKind::Chat,
            "x",
            b"hi",
        );
        // Admit the joiner first so the membership check passes —
        // this test pins the verify-before-append property, not the
        // membership path.
        r.ensure_member(id, claimed_peer.clone(), None)
            .await
            .unwrap();

        let err = r
            .send(
                id,
                claimed_peer.clone(),
                MessageKind::Chat,
                "x".into(),
                b"hi".to_vec(),
                Authoring::Remote {
                    timestamp_ms,
                    signature: bad_sig,
                },
            )
            .await
            .unwrap_err();
        match err {
            SessionError::SignatureRejected { peer_id, reason } => {
                assert_eq!(peer_id, claimed_peer.id);
                assert!(
                    reason.contains("does not verify"),
                    "unexpected reason: {reason}",
                );
            }
            other => panic!("expected SignatureRejected, got {other:?}"),
        }

        // The rejected Chat body must not append. The log may contain
        // the auto-grant Capability message `ensure_member` emitted when
        // it admitted the joiner (Auth Slice C) — but no Chat.
        let sub = r.subscribe(id, None).await.unwrap();
        assert!(
            sub.replay.iter().all(|m| m.kind != MessageKind::Chat),
            "rejected Chat body must not append; log: {:?}",
            sub.replay.iter().map(|m| m.kind).collect::<Vec<_>>(),
        );
    }

    #[tokio::test]
    #[cfg(feature = "iroh")]
    async fn registry_send_remote_authoring_preserves_timestamp_and_signature() {
        // The host trusts the joiner's `timestamp_ms` + `signature`
        // verbatim. Sign off-registry, drive the `Remote` arm, and
        // assert the appended message carries exactly those bytes.
        let (r, host, _) = registry_with_signing_seed(0x41);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        // The joiner signs with their own key; the body's peer.id is
        // their own pubkey.
        let joiner_signing = artel_protocol::signing::SigningKey::from_bytes(&[0x77; 32]);
        let joiner = PeerInfo::new(
            PeerId::from_bytes(joiner_signing.verifying_key().to_bytes()),
            "bob",
        );
        let timestamp_ms = 1_700_000_001_234u64;
        let signature = artel_protocol::signing::sign_body(
            &joiner_signing,
            id,
            artel_protocol::message::MESSAGE_FORMAT,
            timestamp_ms,
            &joiner,
            MessageKind::Chat,
            "joiner.send",
            b"payload",
        );
        r.ensure_member(id, joiner.clone(), None).await.unwrap();
        let appended = r
            .send(
                id,
                joiner.clone(),
                MessageKind::Chat,
                "joiner.send".into(),
                b"payload".to_vec(),
                Authoring::Remote {
                    timestamp_ms,
                    signature,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            appended.timestamp_ms, timestamp_ms,
            "timestamp must be preserved verbatim",
        );
        assert_eq!(
            appended.signature, signature,
            "signature must be preserved verbatim",
        );
        // And it still verifies on the receiver side over the same
        // canonical bytes.
        signing::verify_message(id, &appended, &appended.signature).unwrap();
    }

    // ---- Host sequencing signature + epoch (Auth Slice B.5.2) ----

    #[tokio::test]
    #[cfg(feature = "iroh")]
    async fn host_send_local_stamps_verifiable_host_sig() {
        // A `Local` send must stamp a `host_sig` that `verify_seq`
        // accepts under the daemon's own pubkey, bound to (session,
        // seq, author_sig).
        let (r, host, _secret) = registry_with_signing_seed(0x41);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let sent = r
            .send(
                id,
                host.clone(),
                MessageKind::Chat,
                "hello".into(),
                b"world".to_vec(),
                Authoring::Local,
            )
            .await
            .unwrap();
        assert_ne!(
            sent.host_sig,
            artel_protocol::message::SIGNATURE_UNSIGNED,
            "Local send must stamp a real host_sig",
        );
        // host.id is the daemon's own pubkey (signing identity = host).
        signing::verify_seq(&host.id, id, sent.seq, &sent.signature, &sent.host_sig)
            .expect("host_sig must verify under the host pubkey");
        // Bound to this seq: a bumped seq fails.
        assert!(
            signing::verify_seq(
                &host.id,
                id,
                sent.seq.next().unwrap(),
                &sent.signature,
                &sent.host_sig,
            )
            .is_err(),
            "host_sig must be bound to its seq",
        );
    }

    #[tokio::test]
    #[cfg(feature = "iroh")]
    async fn host_send_remote_stamps_host_sig_over_joiner_author_sig() {
        // When the host re-sequences a joiner-authored body, it stamps
        // its own host_sig over the *joiner's* author signature — so
        // the seq binding holds against the joiner's sig, not the
        // host's.
        let (r, host, _) = registry_with_signing_seed(0x41);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();

        let joiner_signing = artel_protocol::signing::SigningKey::from_bytes(&[0x77; 32]);
        let joiner = PeerInfo::new(
            PeerId::from_bytes(joiner_signing.verifying_key().to_bytes()),
            "bob",
        );
        let timestamp_ms = 1_700_000_009_000u64;
        let author_sig = artel_protocol::signing::sign_body(
            &joiner_signing,
            id,
            artel_protocol::message::MESSAGE_FORMAT,
            timestamp_ms,
            &joiner,
            MessageKind::Chat,
            "joiner.send",
            b"payload",
        );
        r.ensure_member(id, joiner.clone(), None).await.unwrap();
        let appended = r
            .send(
                id,
                joiner.clone(),
                MessageKind::Chat,
                "joiner.send".into(),
                b"payload".to_vec(),
                Authoring::Remote {
                    timestamp_ms,
                    signature: author_sig,
                },
            )
            .await
            .unwrap();
        // The author sig is the joiner's, preserved verbatim.
        assert_eq!(appended.signature, author_sig);
        // The host_sig is over the joiner's author sig at the assigned
        // seq, verifiable under the HOST's pubkey (not the joiner's).
        signing::verify_seq(
            &host.id,
            id,
            appended.seq,
            &appended.signature,
            &appended.host_sig,
        )
        .expect("host_sig must verify under the host pubkey");
    }

    #[tokio::test]
    #[cfg(feature = "iroh")]
    async fn fresh_host_starts_at_epoch_zero() {
        // A brand-new hosted session has host_epoch 0 persisted.
        let (r, host, _) = registry_with_signing_seed(0x41);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let records = r.store.load_all().await.unwrap();
        let rec = records.iter().find(|rr| rr.id == id).unwrap();
        assert_eq!(rec.host_epoch, 0, "fresh host starts at epoch 0");
    }

    #[tokio::test]
    #[cfg(feature = "iroh")]
    async fn resume_bumps_host_epoch_and_persists() {
        // First host creates at epoch 0. A restart (Registry::load over
        // the same store) followed by host(Some(id)) resumes and bumps
        // to 1; a second resume bumps to 2. Each bump is persisted.
        let secret = Arc::new(iroh::SecretKey::from_bytes(&[0x41; 32]));
        let peer_id = PeerId::from_bytes(*secret.public().as_bytes());
        let host = PeerInfo::new(peer_id, "alice");
        let store: DynStore = Arc::new(crate::store::MemoryStore::new());

        let r = Registry::new_with_signing_key(peer_id, Arc::clone(&store), Arc::clone(&secret));
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        assert_eq!(
            store.load_all().await.unwrap()[0].host_epoch,
            0,
            "fresh create is epoch 0",
        );
        drop(r);

        // Restart: load from the same store, then resume.
        let r2 = Registry::load(
            peer_id,
            WireEndpointAddr::id_only(peer_id),
            Arc::clone(&store),
            None,
            Some(Arc::clone(&secret)),
            None,
        )
        .await
        .unwrap();
        let (_id, _) = r2.host_rw(host.clone(), Some(id)).await.unwrap();
        assert_eq!(
            store.load_all().await.unwrap()[0].host_epoch,
            1,
            "first resume bumps to epoch 1",
        );
        // Second resume (same process) bumps to 2.
        let (_id, _) = r2.host_rw(host.clone(), Some(id)).await.unwrap();
        assert_eq!(
            store.load_all().await.unwrap()[0].host_epoch,
            2,
            "second resume bumps to epoch 2",
        );

        // The bumped epoch produces a verify_ctrl-valid signature —
        // the same bytes the resume EpochBeacon and a SessionClosed
        // would carry. (The broadcast itself is driven end-to-end by
        // the B5.3 e2e; no real bridge exists in a MemoryStore registry.)
        let sig = artel_protocol::signing::sign_ctrl(secret.as_signing_key(), id, 2);
        signing::verify_ctrl(&peer_id, id, 2, &sig).expect("ctrl sig at bumped epoch must verify");
    }

    // ---- Joiner mirror acceptance pipeline (Auth Slice B.5.3) ----
    //
    // These drive `apply_inbound_mirror_message` directly — the same
    // dedup → author sig → host seq-sig pipeline the live `on_message`
    // closure runs — so the ordering + drop invariants are pinned
    // without a live gossip bridge. The watermark is exercised
    // separately (it lives in the bridge's EpochBeacon arm); these
    // assert the load-bearing invariant that the Message path NEVER
    // touches it.

    /// A signed host + a Remote mirror keyed to it. Returns the host's
    /// signing key (so tests can stamp `host_sig`), the session id, the
    /// joiner author key, and the mirror arc.
    #[cfg(feature = "iroh")]
    fn remote_mirror_fixture() -> (
        artel_protocol::signing::SigningKey,
        SessionId,
        artel_protocol::signing::SigningKey,
        Arc<Mutex<Session>>,
        DynStore,
    ) {
        let host_key = artel_protocol::signing::SigningKey::from_bytes(&[0x31; 32]);
        let host_id = PeerId::from_bytes(host_key.verifying_key().to_bytes());
        let session_id = SessionId::from_bytes([0xcc; 16]);
        let mut session_obj = Session::new(
            session_id,
            &PeerInfo::new(host_id, "remote-host"),
            SessionKind::Remote,
        );
        session_obj.host = host_id;
        let author_key = artel_protocol::signing::SigningKey::from_bytes(&[0x32; 32]);
        // No cap-seed needed: v1 mirrors do not enforce or project caps
        // (host-only enforcement). These B.5.3 tests exercise the
        // *signature* pipeline (dedup → author sig → host seq-sig), which
        // is independent of L2.
        let store: DynStore = Arc::new(crate::store::MemoryStore::new());
        (
            host_key,
            session_id,
            author_key,
            Arc::new(Mutex::new(session_obj)),
            store,
        )
    }

    /// Build a fully-signed `SessionMessage` as the host would emit it:
    /// the author signs the body, the host stamps the seq-sig over
    /// `(session, seq, author_sig)`.
    #[cfg(feature = "iroh")]
    fn host_signed_message(
        host_key: &artel_protocol::signing::SigningKey,
        author_key: &artel_protocol::signing::SigningKey,
        session: SessionId,
        seq: Seq,
        payload: &[u8],
    ) -> SessionMessage {
        let author = PeerInfo::new(
            PeerId::from_bytes(author_key.verifying_key().to_bytes()),
            "bob",
        );
        let timestamp_ms = 1_700_000_000_000u64;
        let author_sig = artel_protocol::signing::sign_body(
            author_key,
            session,
            artel_protocol::message::MESSAGE_FORMAT,
            timestamp_ms,
            &author,
            MessageKind::Chat,
            "chat.message",
            payload,
        );
        let host_sig = artel_protocol::signing::sign_seq(host_key, session, seq, &author_sig);
        SessionMessage::new(
            seq,
            timestamp_ms,
            author,
            MessageKind::Chat,
            "chat.message",
            payload.to_vec(),
            author_sig,
            host_sig,
        )
    }

    #[tokio::test]
    #[cfg(feature = "iroh")]
    async fn mirror_accepts_message_with_valid_host_sig() {
        let (host_key, session, author_key, mirror, store) = remote_mirror_fixture();
        store.create(&mirror.lock().await.record()).await.unwrap();
        let msg = host_signed_message(&host_key, &author_key, session, Seq::new(1), b"hi");
        apply_inbound_mirror_message(&store, &mirror, session, msg).await;
        let s = mirror.lock().await;
        assert_eq!(s.log.len(), 1, "valid message must be appended");
        assert_eq!(s.head, Seq::new(1));
    }

    #[tokio::test]
    #[cfg(feature = "iroh")]
    async fn mirror_drops_message_with_bad_host_sig() {
        let (host_key, session, author_key, mirror, store) = remote_mirror_fixture();
        store.create(&mirror.lock().await.record()).await.unwrap();
        let mut msg = host_signed_message(&host_key, &author_key, session, Seq::new(1), b"hi");
        // Corrupt the host seq-sig: author sig stays valid, host_sig
        // doesn't verify under the host pubkey.
        msg.host_sig[0] ^= 0xff;
        apply_inbound_mirror_message(&store, &mirror, session, msg).await;
        assert!(
            mirror.lock().await.log.is_empty(),
            "bad host_sig must be dropped",
        );
    }

    #[tokio::test]
    #[cfg(feature = "iroh")]
    async fn mirror_drops_message_signed_by_wrong_host() {
        let (_host_key, session, author_key, mirror, store) = remote_mirror_fixture();
        store.create(&mirror.lock().await.record()).await.unwrap();
        // A different "host" key stamps the seq-sig — verify_seq must
        // reject it against the mirror's real host pubkey.
        let impostor = artel_protocol::signing::SigningKey::from_bytes(&[0x99; 32]);
        let msg = host_signed_message(&impostor, &author_key, session, Seq::new(1), b"hi");
        apply_inbound_mirror_message(&store, &mirror, session, msg).await;
        assert!(mirror.lock().await.log.is_empty(), "wrong-host sig dropped");
    }

    #[tokio::test]
    #[cfg(feature = "iroh")]
    async fn mirror_drops_replayed_message_under_new_seq() {
        // Finding #1, the live attack: capture a valid (message,
        // host_sig) at seq 1, re-feed it with a bumped seq. The host
        // seq-sig is bound to the original seq, so verify_seq rejects
        // the replay under the new seq.
        let (host_key, session, author_key, mirror, store) = remote_mirror_fixture();
        store.create(&mirror.lock().await.record()).await.unwrap();
        let genuine = host_signed_message(&host_key, &author_key, session, Seq::new(1), b"hi");

        // Replay the genuine frame's bytes but on seq 5 (a gap the
        // mirror hasn't seen). host_sig still binds seq 1.
        let mut replayed = genuine.clone();
        replayed.seq = Seq::new(5);
        apply_inbound_mirror_message(&store, &mirror, session, replayed).await;
        assert!(
            mirror.lock().await.log.is_empty(),
            "replay under a new seq must be dropped by verify_seq",
        );

        // The genuine frame at its real seq still applies.
        apply_inbound_mirror_message(&store, &mirror, session, genuine).await;
        assert_eq!(mirror.lock().await.log.len(), 1, "genuine frame applies");
    }

    #[tokio::test]
    #[cfg(feature = "iroh")]
    async fn mirror_suppresses_log_borne_ticket_action_from_live_events() {
        // A fully-signed System/TICKET_ACTION gossip Message (an RW
        // member broadcasting a forged envelope) is appended to the
        // mirror log (seq continuity) but never reaches the live IPC
        // event stream — the unicast-delivered synthetic message is
        // the only sanctioned TICKET_ACTION source.
        let (host_key, session, author_key, mirror, store) = remote_mirror_fixture();
        store.create(&mirror.lock().await.record()).await.unwrap();

        let mut events_rx = mirror.lock().await.events_tx.subscribe();

        // Sign a System/TICKET_ACTION message the same way the host
        // would sequence a member's broadcast.
        let author = PeerInfo::new(
            PeerId::from_bytes(author_key.verifying_key().to_bytes()),
            "mallory",
        );
        let timestamp_ms = 1_700_000_000_000u64;
        let payload = vec![0xAB; 32];
        let author_sig = artel_protocol::signing::sign_body(
            &author_key,
            session,
            artel_protocol::message::MESSAGE_FORMAT,
            timestamp_ms,
            &author,
            MessageKind::System,
            TICKET_ACTION,
            &payload,
        );
        let host_sig =
            artel_protocol::signing::sign_seq(&host_key, session, Seq::new(1), &author_sig);
        let msg = SessionMessage::new(
            Seq::new(1),
            timestamp_ms,
            author,
            MessageKind::System,
            TICKET_ACTION,
            payload,
            author_sig,
            host_sig,
        );

        apply_inbound_mirror_message(&store, &mirror, session, msg).await;
        assert_eq!(
            mirror.lock().await.log.len(),
            1,
            "entry persists for seq continuity",
        );
        assert!(
            matches!(
                events_rx.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ),
            "log-borne TICKET_ACTION must not surface as a live event",
        );
    }

    #[tokio::test]
    #[cfg(feature = "iroh")]
    async fn mirror_dedups_duplicate_seq_without_reverifying() {
        // A duplicate seq is dropped at the dedup gate before any
        // crypto runs — so even a duplicate whose host_sig we corrupt
        // (after the first genuine append) is a no-op, not a drop-warn.
        let (host_key, session, author_key, mirror, store) = remote_mirror_fixture();
        store.create(&mirror.lock().await.record()).await.unwrap();
        let msg = host_signed_message(&host_key, &author_key, session, Seq::new(1), b"hi");
        apply_inbound_mirror_message(&store, &mirror, session, msg.clone()).await;
        assert_eq!(mirror.lock().await.log.len(), 1);
        // Re-feed the same seq; dedup drops it, log unchanged.
        apply_inbound_mirror_message(&store, &mirror, session, msg).await;
        assert_eq!(mirror.lock().await.log.len(), 1, "duplicate seq deduped");
    }

    #[tokio::test]
    #[cfg(feature = "iroh")]
    async fn replayed_message_cannot_poison_watermark() {
        // The review-blocker regression: feeding a genuine (message,
        // host_sig) on an unseen seq must NOT move any host-epoch
        // watermark — the Message path has no watermark interaction at
        // all (only the bridge's EpochBeacon arm moves it). We model
        // the watermark as the mirror's persisted `host_epoch` and
        // assert it stays at its seed value across message application.
        let (host_key, session, author_key, mirror, store) = remote_mirror_fixture();
        // Seed a watermark of 0 (fresh join).
        store.create(&mirror.lock().await.record()).await.unwrap();
        assert_eq!(mirror.lock().await.host_epoch, 0);

        // Apply a genuine message on an unseen seq.
        let msg = host_signed_message(&host_key, &author_key, session, Seq::new(7), b"hi");
        apply_inbound_mirror_message(&store, &mirror, session, msg).await;

        // Watermark unchanged: a later legitimate SessionClosed at the
        // real epoch (0) would still satisfy `host_epoch >= watermark`.
        assert_eq!(
            mirror.lock().await.host_epoch,
            0,
            "Message application must never move the host_epoch watermark",
        );
        // And the persisted record agrees.
        let records = store.load_all().await.unwrap();
        assert_eq!(records[0].host_epoch, 0);
    }

    #[tokio::test]
    #[cfg(feature = "iroh")]
    async fn beacon_advances_watermark_only_when_host_signed() {
        // The EpochBeacon arm advances the watermark only on a
        // host-signed value. `advance_host_epoch_watermark` is the
        // persistence half of that arm; pin that a host-signed epoch
        // advances + persists, while we never call it for an unsigned
        // / wrong-key beacon (the bridge arm drops those before
        // reaching it). We assert the signed advance here and the
        // wrong-key drop via verify_ctrl directly.
        let (host_key, session, _author, mirror, store) = remote_mirror_fixture();
        store.create(&mirror.lock().await.record()).await.unwrap();
        let host_id = PeerId::from_bytes(host_key.verifying_key().to_bytes());

        // A host-signed beacon at epoch 3 verifies, and the registry
        // method persists the advance.
        let good = artel_protocol::signing::sign_ctrl(&host_key, session, 3);
        signing::verify_ctrl(&host_id, session, 3, &good).expect("host-signed beacon verifies");

        // A wrong-key beacon does NOT verify — the bridge arm would
        // drop it before any watermark move.
        let impostor = artel_protocol::signing::SigningKey::from_bytes(&[0x99; 32]);
        let bad = artel_protocol::signing::sign_ctrl(&impostor, session, 99);
        assert!(
            signing::verify_ctrl(&host_id, session, 99, &bad).is_err(),
            "wrong-key beacon must not verify",
        );

        // Build a registry over the same store so we can call the
        // persistence half against the mirror.
        let daemon_peer = PeerId::from_bytes([7; 32]);
        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            Arc::clone(&store),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        r.advance_host_epoch_watermark(session, 3).await.unwrap();
        assert_eq!(
            r.store.load_all().await.unwrap()[0].host_epoch,
            3,
            "host-signed beacon advances + persists the watermark",
        );
        // Monotonic: a lower epoch is a no-op.
        r.advance_host_epoch_watermark(session, 1).await.unwrap();
        assert_eq!(r.store.load_all().await.unwrap()[0].host_epoch, 3);
    }

    // ====================================================================
    // L2 capabilities (Auth Slice C)
    // ====================================================================

    /// Build an unsigned `Capability` message carrying `action`. The
    /// projection (`apply_capability`) enforces the cap-set authority
    /// rule, not signatures — those are checked at the send/mirror gates
    /// — so these projection-unit fixtures skip signing.
    fn cap_msg(seq: u64, author: &PeerInfo, action: &CapabilityAction) -> SessionMessage {
        SessionMessage::new(
            Seq::new(seq),
            1_700_000_000_000,
            author.clone(),
            MessageKind::Capability,
            action.action_str(),
            action.encode(),
            SIGNATURE_UNSIGNED,
            SIGNATURE_UNSIGNED,
        )
    }

    // ---- projection unit (no I/O) ----

    #[tokio::test]
    async fn new_session_seeds_host_read_write() {
        let host = peer(1, "alice");
        let s = Session::new(SessionId::from_bytes([1; 16]), &host, SessionKind::Local);
        assert_eq!(s.caps.get(&host.id), Some(&Capability::ReadWrite));
        assert!(s.can_write(host.id), "host is the cap-log root");
        // An unrelated peer is absent ⇒ Read floor ⇒ cannot write.
        assert!(!s.can_write(peer(2, "bob").id));
    }

    #[tokio::test]
    async fn apply_capability_grant_inserts_and_revoke_removes() {
        let host = peer(1, "alice");
        let bob = peer(2, "bob");
        let mut s = Session::new(SessionId::from_bytes([1; 16]), &host, SessionKind::Local);

        // Host grants bob ReadWrite.
        let grant = CapabilityAction::Grant {
            peer: bob.id,
            cap: Capability::ReadWrite,
        };
        s.log.push(cap_msg(1, &host, &grant));
        s.apply_capability(&s.log.last().unwrap().clone());
        assert!(s.can_write(bob.id), "granted peer can write");

        // Host revokes bob.
        let revoke = CapabilityAction::Revoke { peer: bob.id };
        s.log.push(cap_msg(2, &host, &revoke));
        s.apply_capability(&s.log.last().unwrap().clone());
        assert!(
            !s.can_write(bob.id),
            "revoked peer falls back to Read floor"
        );
        assert_eq!(s.caps.get(&bob.id), None, "revoke removes the entry");
    }

    #[tokio::test]
    async fn grant_from_non_host_is_a_no_op_on_caps() {
        // Only the host may issue grants/revokes. A non-host peer —
        // even one holding RW — cannot mutate the cap set. This pins
        // the host-only authority rule (tightened from "any RW holder"
        // to prevent malicious joiners from revoking peers; P2P will
        // need a quorum/causal-authority rule instead).
        let host = peer(1, "alice");
        let mallory = peer(9, "mallory");
        let mut s = Session::new(SessionId::from_bytes([1; 16]), &host, SessionKind::Local);

        // Even if mallory somehow held RW, her grant is inert.
        let self_grant = CapabilityAction::Grant {
            peer: mallory.id,
            cap: Capability::ReadWrite,
        };
        s.log.push(cap_msg(1, &mallory, &self_grant));
        s.apply_capability(&s.log.last().unwrap().clone());
        assert!(
            !s.can_write(mallory.id),
            "non-host cannot grant — even itself",
        );
    }

    #[tokio::test]
    async fn from_record_rebuilds_caps_from_log() {
        // Q4: caps are derived, never persisted. A record whose log
        // contains grant→revoke rebuilds to the identical cap set on
        // load — pinned store-free.
        let host = peer(1, "alice");
        let bob = peer(2, "bob");
        let carol = peer(3, "carol");
        let session_id = SessionId::from_bytes([0xcd; 16]);

        let grant_bob = CapabilityAction::Grant {
            peer: bob.id,
            cap: Capability::ReadWrite,
        };
        let grant_carol = CapabilityAction::Grant {
            peer: carol.id,
            cap: Capability::ReadWrite,
        };
        let revoke_bob = CapabilityAction::Revoke { peer: bob.id };
        let log = vec![
            cap_msg(1, &host, &grant_bob),
            cap_msg(2, &host, &grant_carol),
            cap_msg(3, &host, &revoke_bob),
        ];
        let record = SessionRecord {
            id: session_id,
            host: host.id,
            members: HashSet::from([host.id, bob.id, carol.id]),
            head: Seq::new(3),
            log,
            kind: SessionKind::Local,
            host_epoch: 0,
            tickets: Vec::new(),
            workspace_ticket: None,
        };
        let s = Session::from_record(record);
        assert!(s.can_write(host.id), "host root preserved");
        assert!(s.can_write(carol.id), "carol's grant replayed");
        assert!(
            !s.can_write(bob.id),
            "bob's grant-then-revoke nets to no write"
        );
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn fs_log_store_replays_caps_on_registry_load() {
        let dir = tempfile::tempdir().unwrap();
        let store: DynStore =
            Arc::new(crate::store::FsLogStore::open(dir.path().join("sessions")).unwrap());

        // 1. Build a registry with a signing key over the FsLogStore.
        let (r, host, secret) = registry_with_signing_seed_over(0x41, Arc::clone(&store));
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();

        // 2. Admit bob + carol (auto-grants both RW).
        let bob = peer(2, "bob");
        let carol = peer(3, "carol");
        r.ensure_member(id, bob.clone(), None).await.unwrap();
        r.ensure_member(id, carol.clone(), None).await.unwrap();

        // 3. Host revokes bob.
        let revoke = CapabilityAction::Revoke { peer: bob.id };
        r.send(
            id,
            host.clone(),
            MessageKind::Capability,
            revoke.action_str().to_string(),
            revoke.encode(),
            Authoring::Local,
        )
        .await
        .unwrap();

        // 4. Assert pre-resume state.
        {
            let arc = {
                let g = r.sessions.read().await;
                g.get(&id).unwrap().clone()
            };
            let s = arc.lock().await;
            let (host_rw, carol_rw, bob_rw) = (
                s.can_write(host.id),
                s.can_write(carol.id),
                s.can_write(bob.id),
            );
            drop(s);
            assert!(host_rw, "host RW before restart");
            assert!(carol_rw, "carol RW before restart");
            assert!(!bob_rw, "bob revoked before restart");
        }

        // 5. Drop the registry (in-memory state gone).
        drop(r);

        // 6. Registry::load from the SAME FsLogStore (cold start).
        let r2 = Registry::load(
            host.id,
            WireEndpointAddr::id_only(host.id),
            Arc::clone(&store),
            None,
            Some(secret),
            None,
        )
        .await
        .unwrap();

        // 7. Assert identical cap-set survived the cold reload.
        let arc = {
            let g = r2.sessions.read().await;
            g.get(&id).unwrap().clone()
        };
        let s = arc.lock().await;
        let (host_rw, carol_rw, bob_rw) = (
            s.can_write(host.id),
            s.can_write(carol.id),
            s.can_write(bob.id),
        );
        drop(s);
        assert!(host_rw, "host RW after reload");
        assert!(carol_rw, "carol RW after reload");
        assert!(!bob_rw, "bob still revoked after reload");
    }

    #[tokio::test]
    async fn revoke_after_grant_nets_to_no_write_in_projection() {
        // Projection is seq-ordered: grant at seq 1 then revoke at seq 3
        // leaves bob unable to write in the resulting cap-set. (v1 is
        // host-only enforcement, so this is the host's own projection;
        // there is no joiner-side at-seq path anymore.)
        let host = peer(1, "alice");
        let bob = peer(2, "bob");
        let session_id = SessionId::from_bytes([0xcd; 16]);

        let grant_bob = CapabilityAction::Grant {
            peer: bob.id,
            cap: Capability::ReadWrite,
        };
        let revoke_bob = CapabilityAction::Revoke { peer: bob.id };
        let log = vec![
            cap_msg(1, &host, &grant_bob),  // seq 1: bob granted
            cap_msg(3, &host, &revoke_bob), // seq 3: bob revoked
        ];
        let s = Session {
            caps: project_caps(host.id, &log),
            log,
            ..Session::new(session_id, &host, SessionKind::Local)
        };
        assert!(!s.can_write(bob.id), "grant-then-revoke nets to no write");
        assert!(s.can_write(host.id), "host root preserved");
    }

    // ---- Registry-via-MemoryStore enforcement ----

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn host_send_rejects_read_only_peer() {
        // A member who was never granted RW (we add them to membership
        // directly, bypassing the auto-grant path) is rejected at the
        // host's `send` with CapabilityDenied — never seq'd (O1).
        let (r, host, _) = registry_with_signing_seed(0x41);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();

        // Add bob to membership WITHOUT auto-granting: poke the session
        // directly so we isolate the enforcement from the auto-grant.
        let bob = peer(2, "bob");
        {
            let arc = {
                let g = r.sessions.read().await;
                g.get(&id).unwrap().clone()
            };
            let mut s = arc.lock().await;
            s.members.insert(bob.id);
        }

        let err = r
            .send(
                id,
                bob.clone(),
                MessageKind::Chat,
                "x".into(),
                b"hi".to_vec(),
                Authoring::Local,
            )
            .await
            .unwrap_err();
        assert_eq!(
            err,
            SessionError::CapabilityDenied {
                peer_id: bob.id,
                had: None,
                needed: Capability::ReadWrite,
            },
        );
        // Drop-before-append: no Chat occupies a seq.
        let sub = r.subscribe(id, None).await.unwrap();
        assert!(sub.replay.iter().all(|m| m.kind != MessageKind::Chat));
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn host_send_rejects_revoked_peer() {
        // grant → revoke → write must be denied at the host.
        let (r, host, _) = registry_with_signing_seed(0x41);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let bob = peer(2, "bob");

        // ensure_member auto-grants bob RW.
        r.ensure_member(id, bob.clone(), None).await.unwrap();
        assert!(
            {
                let arc = {
                    let g = r.sessions.read().await;
                    g.get(&id).unwrap().clone()
                };
                let s = arc.lock().await;
                s.can_write(bob.id)
            },
            "auto-grant gave bob RW",
        );

        // Host revokes bob.
        let revoke = CapabilityAction::Revoke { peer: bob.id };
        r.send(
            id,
            host.clone(),
            MessageKind::Capability,
            revoke.action_str().to_string(),
            revoke.encode(),
            Authoring::Local,
        )
        .await
        .unwrap();

        // Bob's next write is denied.
        let err = r
            .send(
                id,
                bob.clone(),
                MessageKind::Chat,
                "x".into(),
                b"hi".to_vec(),
                Authoring::Local,
            )
            .await
            .unwrap_err();
        assert_eq!(
            err,
            SessionError::CapabilityDenied {
                peer_id: bob.id,
                had: None,
                needed: Capability::ReadWrite,
            },
        );
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn auto_grant_on_ensure_member_lets_peer_write() {
        // The round-trip: ensure_member admits + auto-grants a peer,
        // who can then `send` successfully.
        let (r, host, _) = registry_with_signing_seed(0x41);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let bob = peer(2, "bob");
        r.ensure_member(id, bob.clone(), None).await.unwrap();

        // A Grant message now sits in the log, authored by the host.
        let sub = r.subscribe(id, None).await.unwrap();
        let grant = sub
            .replay
            .iter()
            .find(|m| m.kind == MessageKind::Capability)
            .expect("auto-grant message in log");
        assert_eq!(grant.peer.id, host.id, "grant authored by the host");
        let action = CapabilityAction::decode(&grant.payload).unwrap();
        assert_eq!(
            action,
            CapabilityAction::Grant {
                peer: bob.id,
                cap: Capability::ReadWrite,
            },
        );

        // And bob can now write.
        let sent = r
            .send(
                id,
                bob.clone(),
                MessageKind::Chat,
                "hi".into(),
                b"world".to_vec(),
                Authoring::Local,
            )
            .await;
        assert!(sent.is_ok(), "auto-granted peer can write: {sent:?}");
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn mirror_does_not_enforce_or_project_caps() {
        // v1 is host-only enforcement: a joiner mirror neither enforces
        // the cap rule nor projects a cap-set from inbound Capability
        // frames (see the loud note in `apply_inbound_mirror_message`).
        // Feed a validly host-signed `Grant` through the mirror and
        // assert: (a) it appends like any other signed frame (the mirror
        // does not gate on caps), and (b) the mirror's cap-set stays
        // host-only (mirrors don't project — the forged-self-grant
        // defense lives at the host's `send`, pinned by
        // `grant_from_non_holder_is_a_no_op_on_caps`).
        let (host_key, session, author_key, mirror, store) = remote_mirror_fixture();
        store.create(&mirror.lock().await.record()).await.unwrap();
        let author = PeerInfo::new(
            PeerId::from_bytes(author_key.verifying_key().to_bytes()),
            "bob",
        );
        let grant = CapabilityAction::Grant {
            peer: author.id,
            cap: Capability::ReadWrite,
        };
        let ts = 1_700_000_000_000u64;
        let author_sig = artel_protocol::signing::sign_body(
            &author_key,
            session,
            MESSAGE_FORMAT,
            ts,
            &author,
            MessageKind::Capability,
            grant.action_str(),
            &grant.encode(),
        );
        let host_sig =
            artel_protocol::signing::sign_seq(&host_key, session, Seq::new(1), &author_sig);
        let msg = SessionMessage::new(
            Seq::new(1),
            ts,
            author.clone(),
            MessageKind::Capability,
            grant.action_str(),
            grant.encode(),
            author_sig,
            host_sig,
        );
        apply_inbound_mirror_message(&store, &mirror, session, msg).await;
        let (log_len, caps_len, caps_has_host) = {
            let s = mirror.lock().await;
            (s.log.len(), s.caps.len(), s.caps.contains_key(&s.host))
        };
        assert_eq!(log_len, 1, "validly-signed frame appends; no cap gate");
        // The mirror never projects: only the host root is in caps.
        assert_eq!(caps_len, 1, "mirror does not project caps");
        assert!(caps_has_host, "only the host root seeded");
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn cross_session_grant_replay_is_rejected_by_signature() {
        // A grant signed for session A, fed to session B's mirror, fails
        // `verify_message` (session_id is in the signed scope, B.5) — so
        // it is dropped (never appended). Re-asserts the signature
        // property for the Capability kind.
        let (host_key, session_a, _author, _mirror_a, store) = remote_mirror_fixture();
        let session_b = SessionId::from_bytes([0xbb; 16]);
        // A mirror for session B with the same host.
        let host_id = PeerId::from_bytes(host_key.verifying_key().to_bytes());
        let mut mirror_b_obj = Session::new(
            session_b,
            &PeerInfo::new(host_id, "remote-host"),
            SessionKind::Remote,
        );
        mirror_b_obj.host = host_id;
        let mirror_b = Arc::new(Mutex::new(mirror_b_obj));
        store.create(&mirror_b.lock().await.record()).await.unwrap();

        let bob = peer(2, "bob");
        let grant = CapabilityAction::Grant {
            peer: bob.id,
            cap: Capability::ReadWrite,
        };
        // Host authors+signs the grant for session A.
        let ts = 1_700_000_000_000u64;
        let author_sig = artel_protocol::signing::sign_body(
            &host_key,
            session_a, // signed FOR A
            MESSAGE_FORMAT,
            ts,
            &PeerInfo::new(host_id, "remote-host"),
            MessageKind::Capability,
            grant.action_str(),
            &grant.encode(),
        );
        let host_sig =
            artel_protocol::signing::sign_seq(&host_key, session_b, Seq::new(1), &author_sig);
        let msg = SessionMessage::new(
            Seq::new(1),
            ts,
            PeerInfo::new(host_id, "remote-host"),
            MessageKind::Capability,
            grant.action_str(),
            grant.encode(),
            author_sig,
            host_sig,
        );
        // Fed to B's mirror: author-sig verify uses session_b, but the
        // sig was made for session_a → BadSig → dropped.
        let _ = bob;
        apply_inbound_mirror_message(&store, &mirror_b, session_b, msg).await;
        let log_is_empty = {
            let s = mirror_b.lock().await;
            s.log.is_empty()
        };
        assert!(log_is_empty, "cross-session grant must not append");
    }

    // ---- subscribe / replay ----

    #[tokio::test]
    async fn subscribe_replays_messages_after_since() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();

        let s1 = r
            .send(
                id,
                host.clone(),
                MessageKind::Chat,
                "1".into(),
                vec![],
                Authoring::Local,
            )
            .await
            .unwrap();
        let _s2 = r
            .send(
                id,
                host.clone(),
                MessageKind::Chat,
                "2".into(),
                vec![],
                Authoring::Local,
            )
            .await
            .unwrap();
        let _s3 = r
            .send(
                id,
                host,
                MessageKind::Chat,
                "3".into(),
                vec![],
                Authoring::Local,
            )
            .await
            .unwrap();

        // Subscribe with since = s1: replay should hold s2, s3 (in
        // order, no s1).
        let sub = r.subscribe(id, Some(s1.seq)).await.unwrap();
        let actions: Vec<&str> = sub.replay.iter().map(|m| m.action.as_str()).collect();
        assert_eq!(actions, vec!["2", "3"]);
    }

    #[tokio::test]
    async fn subscribe_with_no_since_replays_full_log() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        for n in 0..5 {
            r.send(
                id,
                host.clone(),
                MessageKind::Chat,
                format!("m{n}"),
                vec![],
                Authoring::Local,
            )
            .await
            .unwrap();
        }
        let sub = r.subscribe(id, None).await.unwrap();
        assert_eq!(sub.replay.len(), 5);
    }

    #[tokio::test]
    async fn subscribe_unknown_session_errors() {
        let r = registry();
        let bogus = SessionId::new_random();
        let err = r.subscribe(bogus, None).await.unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    // ---- leave ----

    #[tokio::test]
    async fn member_leave_emits_peer_left_and_keeps_session() {
        let r = registry();
        let host = peer(1, "alice");
        let bob = peer(2, "bob");
        let (id, ticket) = r.host_rw(host, None).await.unwrap();
        r.join(&ticket, bob.clone()).await.unwrap();

        let mut sub = r.subscribe(id, None).await.unwrap();
        r.leave(id, bob.id).await.unwrap();
        let event = timeout(Duration::from_millis(100), sub.events.recv())
            .await
            .expect("event")
            .unwrap();
        match event {
            Event::PeerLeft { session, peer } => {
                assert_eq!(session, id);
                assert_eq!(peer, bob.id);
            }
            other => panic!("expected PeerLeft, got {other:?}"),
        }
        // Session still exists.
        assert_eq!(r.list().await.len(), 1);
    }

    #[tokio::test]
    async fn host_leave_emits_session_closed_and_removes_session() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let mut sub = r.subscribe(id, None).await.unwrap();
        r.leave(id, host.id).await.unwrap();

        let event = timeout(Duration::from_millis(100), sub.events.recv())
            .await
            .expect("event")
            .unwrap();
        assert_eq!(event, Event::SessionClosed { session: id });

        assert!(r.list().await.is_empty());
    }

    #[tokio::test]
    async fn leave_non_member_errors() {
        let r = registry();
        let (id, _) = r.host_rw(peer(1, "alice"), None).await.unwrap();
        let err = r.leave(id, peer(9, "intruder").id).await.unwrap_err();
        assert_eq!(err, SessionError::NotMember(id));
    }

    #[tokio::test]
    async fn leave_unknown_session_errors() {
        let r = registry();
        let bogus = SessionId::new_random();
        let err = r.leave(bogus, peer(1, "alice").id).await.unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    // ---- list ----

    #[tokio::test]
    async fn list_summarises_each_session() {
        let r = registry();
        let host = peer(1, "alice");
        let bob = peer(2, "bob");
        let (id, ticket) = r.host_rw(host.clone(), None).await.unwrap();
        r.join(&ticket, bob).await.unwrap();
        r.send(
            id,
            host,
            MessageKind::Chat,
            "x".into(),
            vec![],
            Authoring::Local,
        )
        .await
        .unwrap();

        let mut summaries = r.list().await;
        assert_eq!(summaries.len(), 1);
        let s = summaries.pop().unwrap();
        assert_eq!(s.id, id);
        assert_eq!(s.peer_count, 2);
        assert_eq!(s.last_seq, Some(Seq::new(1)));
        // Daemon peer id is 0xff, host is 0x01, so this daemon is not
        // the host of this session.
        assert!(!s.is_host);
    }

    #[tokio::test]
    async fn list_marks_is_host_when_daemon_is_session_host() {
        let daemon_peer = PeerId::from_bytes([7; 32]);
        let r = registry_with_peer(daemon_peer);
        let host = PeerInfo::new(daemon_peer, "self");
        r.host_rw(host, None).await.unwrap();
        let summaries = r.list().await;
        assert!(summaries[0].is_host);
    }

    // ---- rehydrate with persisted SessionKind ----

    use crate::store::SessionStore;

    #[tokio::test]
    async fn load_rehydrates_remote_session_with_remote_kind() {
        // Pre-populate a store with a Remote-kind record (the shape
        // a daemon would have on disk after joining a remote
        // session and being restarted), load a registry on top of
        // it, and verify that local Send refuses to assign seqs —
        // i.e. the kind survived the round trip.
        let daemon_peer = PeerId::from_bytes([7; 32]);
        let remote_host = peer(1, "alice");
        let session_id = SessionId::from_bytes([0xaa; 16]);
        let me = peer(2, "bob");

        let store = Arc::new(crate::store::MemoryStore::new());
        let record = SessionRecord {
            id: session_id,
            host: remote_host.id,
            members: HashSet::from([remote_host.id, me.id]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Remote,
            host_epoch: 0,
            tickets: Vec::new(),
            workspace_ticket: None,
        };
        store.create(&record).await.unwrap();

        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            store,
            #[cfg(feature = "iroh")]
            None,
            #[cfg(feature = "iroh")]
            None,
            #[cfg(feature = "iroh")]
            None,
        )
        .await
        .unwrap();

        let err = r
            .send(
                session_id,
                me,
                MessageKind::Chat,
                "x".into(),
                vec![],
                Authoring::Local,
            )
            .await
            .unwrap_err();
        // Without the iroh feature the registry surfaces NotHost
        // directly. With iroh on but no bridge attached, the
        // Remote-send branch reports the missing bridge as Internal —
        // either way confirms the kind was persisted as Remote
        // (a Local rehydrate would have just appended locally).
        #[cfg(feature = "iroh")]
        assert!(
            matches!(&err, SessionError::Internal(msg) if msg.contains("remote send")),
            "expected internal-no-bridge, got {err:?}",
        );
        #[cfg(not(feature = "iroh"))]
        assert_eq!(err, SessionError::NotHost);
    }

    // ---- host_closed_session (joiner-side mirror teardown) ----

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn host_closed_session_drops_remote_mirror_and_emits_event() {
        // Stand up a Remote-kind mirror by hand (no live bridge —
        // host_closed_session only consults bridge if Some, so a
        // None bridge is the right shape for this unit test).
        let daemon_peer = PeerId::from_bytes([7; 32]);
        let remote_host = peer(1, "alice");
        let session_id = SessionId::from_bytes([0xaa; 16]);
        let me = peer(2, "bob");

        let store = Arc::new(crate::store::MemoryStore::new());
        let record = SessionRecord {
            id: session_id,
            host: remote_host.id,
            members: HashSet::from([remote_host.id, me.id]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Remote,
            host_epoch: 0,
            tickets: Vec::new(),
            workspace_ticket: None,
        };
        store.create(&record).await.unwrap();

        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            store.clone(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let mut sub = r.subscribe(session_id, None).await.unwrap();

        r.host_closed_session(session_id).await.unwrap();

        let event = timeout(Duration::from_millis(100), sub.events.recv())
            .await
            .expect("event")
            .unwrap();
        assert_eq!(
            event,
            Event::SessionClosed {
                session: session_id
            }
        );
        assert!(r.list().await.is_empty(), "mirror should be gone");
        assert!(
            store.load_all().await.unwrap().is_empty(),
            "persisted record should be deleted",
        );

        // Idempotency: a duplicate close broadcast (or one that
        // races with a manual leave) shouldn't surface as an error.
        r.host_closed_session(session_id).await.unwrap();
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn log_since_returns_only_messages_after_cursor() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();

        let s1 = r
            .send(
                id,
                host.clone(),
                MessageKind::Chat,
                "1".into(),
                vec![],
                Authoring::Local,
            )
            .await
            .unwrap();
        let _s2 = r
            .send(
                id,
                host.clone(),
                MessageKind::Chat,
                "2".into(),
                vec![],
                Authoring::Local,
            )
            .await
            .unwrap();
        let _s3 = r
            .send(
                id,
                host,
                MessageKind::Chat,
                "3".into(),
                vec![],
                Authoring::Local,
            )
            .await
            .unwrap();

        // since = ZERO returns the full log.
        let all = r.log_since(id, Seq::ZERO).await.unwrap();
        assert_eq!(all.len(), 3);

        // since = s1 skips the first.
        let after_s1 = r.log_since(id, s1.seq).await.unwrap();
        let actions: Vec<&str> = after_s1.iter().map(|m| m.action.as_str()).collect();
        assert_eq!(actions, vec!["2", "3"]);

        // since past head returns empty.
        let past = r.log_since(id, Seq::new(99)).await.unwrap();
        assert!(past.is_empty());
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn log_since_unknown_session_errors() {
        let r = registry();
        let bogus = SessionId::new_random();
        let err = r.log_since(bogus, Seq::ZERO).await.unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn log_since_excludes_upgrade_messages() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();

        r.send(
            id,
            host.clone(),
            MessageKind::Chat,
            "hello".into(),
            vec![],
            Authoring::Local,
        )
        .await
        .unwrap();

        // A reserved-action System entry can no longer be sent (the
        // ingress gate rejects it); inject it directly to exercise the
        // log_since filter against a stale/impostor log entry.
        r.inject_log_entry(
            id,
            log_fixture(
                2,
                &host,
                MessageKind::System,
                UPGRADE_ACTION,
                vec![0xAB; 32],
            ),
        )
        .await;

        r.send(
            id,
            host.clone(),
            MessageKind::Capability,
            "cap.grant".into(),
            vec![],
            Authoring::Local,
        )
        .await
        .unwrap();

        r.send(
            id,
            host,
            MessageKind::Chat,
            "world".into(),
            vec![],
            Authoring::Local,
        )
        .await
        .unwrap();

        let messages = r.log_since(id, Seq::ZERO).await.unwrap();
        let actions: Vec<&str> = messages.iter().map(|m| m.action.as_str()).collect();
        assert_eq!(actions, vec!["hello", "world"]);
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn log_since_never_emits_ticket_action() {
        // The gossip replay surface must never carry TICKET_ACTION:
        // the host doesn't log the envelope any more, so any log
        // entry with the action is a peer-authored impostor (or a
        // pre-fix relic) and re-serving it would put
        // capability-shaped bytes back on the topic.
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();

        // A peer-authored TICKET_ACTION can't pass the ingress gate;
        // inject it directly to simulate the impostor log entry the
        // log_since filter must drop.
        r.inject_log_entry(
            id,
            log_fixture(1, &host, MessageKind::System, TICKET_ACTION, vec![0xAB; 64]),
        )
        .await;
        r.send(
            id,
            host,
            MessageKind::Chat,
            "hello".into(),
            vec![],
            Authoring::Local,
        )
        .await
        .unwrap();

        let messages = r.log_since(id, Seq::ZERO).await.unwrap();
        let actions: Vec<&str> = messages.iter().map(|m| m.action.as_str()).collect();
        assert_eq!(actions, vec!["hello"]);
    }

    #[tokio::test]
    async fn subscribe_filters_log_borne_ticket_action() {
        // A TICKET_ACTION System message that somehow landed in the
        // log (peer broadcast) must not surface via Subscribe — the
        // synthetic injection from the persisted unicast copy is the
        // only sanctioned source.
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        // Plant the impostor entry directly — the ingress gate would
        // otherwise reject a member-authored TICKET_ACTION send.
        r.inject_log_entry(
            id,
            log_fixture(1, &host, MessageKind::System, TICKET_ACTION, vec![0xAB; 64]),
        )
        .await;
        r.send(
            id,
            host,
            MessageKind::Chat,
            "hello".into(),
            vec![],
            Authoring::Local,
        )
        .await
        .unwrap();

        let sub = r.subscribe(id, None).await.unwrap();
        let actions: Vec<&str> = sub.replay.iter().map(|m| m.action.as_str()).collect();
        assert_eq!(actions, vec!["hello"]);
    }

    // ---- workspace ticket: emit (joiner side) ----

    /// Remote-kind mirror registry: store pre-seeded with a Remote
    /// record, no bridge. Returns `(registry, session_id, host_peer)`.
    #[cfg(feature = "iroh")]
    async fn remote_mirror_registry() -> (Registry, SessionId, PeerInfo) {
        let daemon_peer = PeerId::from_bytes([7; 32]);
        let remote_host = peer(1, "alice");
        let session_id = SessionId::from_bytes([0xaa; 16]);
        let me = peer(2, "bob");

        let store = Arc::new(crate::store::MemoryStore::new());
        let record = SessionRecord {
            id: session_id,
            host: remote_host.id,
            members: HashSet::from([remote_host.id, me.id]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Remote,
            host_epoch: 0,
            tickets: Vec::new(),
            workspace_ticket: None,
        };
        store.create(&record).await.unwrap();

        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            store,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        (r, session_id, remote_host)
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn emit_workspace_ticket_persists_and_emits_live() {
        let (r, session_id, remote_host) = remote_mirror_registry().await;
        let mut sub = r.subscribe(session_id, None).await.unwrap();
        assert!(sub.replay.is_empty(), "no envelope yet");

        let envelope = vec![0xEE; 128];
        r.emit_workspace_ticket(session_id, remote_host.id, envelope.clone())
            .await
            .unwrap();

        // Live event arrives on the pre-existing subscription.
        let ev = timeout(Duration::from_secs(1), sub.events.recv())
            .await
            .expect("live event in time")
            .expect("channel open");
        match ev {
            Event::Message { message, .. } => {
                assert_eq!(message.kind, MessageKind::System);
                assert_eq!(message.action, TICKET_ACTION);
                assert_eq!(message.payload, envelope);
                assert_eq!(message.peer.id, remote_host.id, "host-stamped");
                assert_eq!(message.seq, Seq::ZERO);
            }
            other => panic!("expected Message, got {other:?}"),
        }

        // Persisted: a fresh Subscribe replays the envelope first.
        let sub2 = r.subscribe(session_id, None).await.unwrap();
        assert_eq!(sub2.replay.len(), 1);
        assert_eq!(sub2.replay[0].action, TICKET_ACTION);
        assert_eq!(sub2.replay[0].payload, envelope);
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn emit_workspace_ticket_rejects_non_host_sender() {
        let (r, session_id, _remote_host) = remote_mirror_registry().await;
        let imposter = peer(9, "mallory");
        let err = r
            .emit_workspace_ticket(session_id, imposter.id, vec![1, 2, 3])
            .await
            .unwrap_err();
        assert!(
            matches!(&err, SessionError::Internal(msg) if msg.contains("not the session host")),
            "got {err:?}",
        );
        // Nothing persisted.
        let sub = r.subscribe(session_id, None).await.unwrap();
        assert!(sub.replay.is_empty());
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn emit_workspace_ticket_rejects_local_session() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let err = r
            .emit_workspace_ticket(id, host.id, vec![1])
            .await
            .unwrap_err();
        assert!(
            matches!(&err, SessionError::Internal(msg) if msg.contains("host sessions")),
            "got {err:?}",
        );
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn emit_workspace_ticket_unknown_session_errors() {
        let (r, _session_id, remote_host) = remote_mirror_registry().await;
        let bogus = SessionId::new_random();
        let err = r
            .emit_workspace_ticket(bogus, remote_host.id, vec![1])
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn emit_workspace_ticket_identical_redelivery_is_idempotent() {
        let (r, session_id, remote_host) = remote_mirror_registry().await;
        let envelope = vec![0xEE; 64];
        r.emit_workspace_ticket(session_id, remote_host.id, envelope.clone())
            .await
            .unwrap();

        // Re-delivery of identical bytes: no second live emit.
        let mut sub = r.subscribe(session_id, None).await.unwrap();
        assert_eq!(sub.replay.len(), 1, "one replayed envelope");
        r.emit_workspace_ticket(session_id, remote_host.id, envelope.clone())
            .await
            .unwrap();
        let second = timeout(Duration::from_millis(300), sub.events.recv()).await;
        assert!(
            second.is_err(),
            "identical re-delivery must not re-emit: {second:?}",
        );

        // Changed bytes DO re-persist + re-emit (host re-published a
        // genuinely different envelope).
        let envelope2 = vec![0xDD; 64];
        r.emit_workspace_ticket(session_id, remote_host.id, envelope2.clone())
            .await
            .unwrap();
        let ev = timeout(Duration::from_secs(1), sub.events.recv())
            .await
            .expect("changed envelope re-emits")
            .expect("channel open");
        match ev {
            Event::Message { message, .. } => assert_eq!(message.payload, envelope2),
            other => panic!("expected Message, got {other:?}"),
        }
    }

    // ---- workspace ticket: subscribe replay injection ----

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn subscribe_replays_persisted_envelope_before_log() {
        // The synthetic ticket message must be FIRST in the replay
        // set so a draining joiner can't give up before reaching it.
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        r.send(
            id,
            host.clone(),
            MessageKind::Chat,
            "chatter".into(),
            vec![],
            Authoring::Local,
        )
        .await
        .unwrap();

        r.publish_workspace_ticket(id, vec![0xAB; 32])
            .await
            .unwrap();

        let sub = r.subscribe(id, None).await.unwrap();
        let actions: Vec<&str> = sub.replay.iter().map(|m| m.action.as_str()).collect();
        assert_eq!(actions, vec![TICKET_ACTION, "chatter"]);
        assert_eq!(sub.replay[0].payload, vec![0xAB; 32]);
        assert_eq!(sub.replay[0].peer.id, host.id, "host-stamped");
    }

    #[tokio::test]
    async fn subscribe_without_envelope_has_no_ticket_message() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host, None).await.unwrap();
        let sub = r.subscribe(id, None).await.unwrap();
        assert!(
            !sub.replay.iter().any(|m| m.action == TICKET_ACTION),
            "no envelope persisted ⇒ no synthetic ticket message",
        );
    }

    // ---- workspace ticket: publish (host side) ----

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn publish_workspace_ticket_is_local_only() {
        let (r, session_id, _host) = remote_mirror_registry().await;
        let err = r
            .publish_workspace_ticket(session_id, vec![1])
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::NotHost);
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn publish_workspace_ticket_unknown_session_errors() {
        let r = registry();
        let bogus = SessionId::new_random();
        let err = r
            .publish_workspace_ticket(bogus, vec![1])
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn publish_workspace_ticket_persists_on_host_record() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host, None).await.unwrap();
        r.publish_workspace_ticket(id, vec![0xCD; 48])
            .await
            .unwrap();
        // Round-trips through the record (restart shape).
        let sub = r.subscribe(id, None).await.unwrap();
        assert_eq!(sub.replay[0].action, TICKET_ACTION);
        assert_eq!(sub.replay[0].payload, vec![0xCD; 48]);
    }

    // ---- reserved-action ingress rejection (forged-envelope leak) ----

    #[tokio::test]
    async fn send_rejects_reserved_ticket_action_and_does_not_emit() {
        // A member's forged `workspace.ticket` System send must be
        // rejected at the sequencing chokepoint — never appended,
        // never live-emitted to the host's IPC subscribers (a
        // co-located joiner-mode workspace would otherwise import the
        // forged envelope from the live stream).
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let mut sub = r.subscribe(id, None).await.unwrap();

        let err = r
            .send(
                id,
                host.clone(),
                MessageKind::System,
                TICKET_ACTION.to_string(),
                b"forged-envelope".to_vec(),
                Authoring::Local,
            )
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::ReservedAction(TICKET_ACTION));

        // Nothing live-emitted.
        let got = timeout(Duration::from_millis(300), sub.events.recv()).await;
        assert!(got.is_err(), "forged TICKET_ACTION must not emit: {got:?}");
        // Nothing appended: a fresh subscribe sees no such entry.
        let sub2 = r.subscribe(id, None).await.unwrap();
        assert!(
            !sub2.replay.iter().any(|m| m.action == TICKET_ACTION),
            "forged TICKET_ACTION must not be appended to the log",
        );
    }

    #[tokio::test]
    async fn send_rejects_reserved_upgrade_action() {
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let err = r
            .send(
                id,
                host,
                MessageKind::System,
                UPGRADE_ACTION.to_string(),
                vec![0xAB; 32],
                Authoring::Local,
            )
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::ReservedAction(UPGRADE_ACTION));
    }

    #[tokio::test]
    async fn send_allows_non_reserved_system_action() {
        // Legitimate System sends (e.g. artel-fs's node-id announce)
        // must still pass — only the two reserved actions are blocked.
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let msg = r
            .send(
                id,
                host,
                MessageKind::System,
                "workspace.node_id".to_string(),
                vec![1, 2, 3],
                Authoring::Local,
            )
            .await
            .expect("non-reserved System action must be accepted");
        assert_eq!(msg.action, "workspace.node_id");
    }

    #[tokio::test]
    async fn send_allows_chat_action_named_like_reserved_with_non_system_kind() {
        // The gate keys on (System kind AND reserved action). A Chat
        // message that happens to carry the reserved action string is
        // not a capability frame and stays allowed — the reserved
        // names only matter on the System surface the daemon injects.
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let msg = r
            .send(
                id,
                host,
                MessageKind::Chat,
                TICKET_ACTION.to_string(),
                vec![1],
                Authoring::Local,
            )
            .await
            .expect("non-System kind must be accepted even with reserved action name");
        assert_eq!(msg.kind, MessageKind::Chat);
    }

    // ---- is_member accessor ----

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn is_member_reflects_membership() {
        let r = registry();
        let host = peer(1, "alice");
        let outsider = peer(9, "mallory");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        assert_eq!(r.is_member(id, host.id).await, Some(true));
        assert_eq!(r.is_member(id, outsider.id).await, Some(false));
        assert_eq!(r.is_member(SessionId::new_random(), host.id).await, None,);
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn host_closed_session_ignores_local_session() {
        // Defensive: a misrouted SessionClosed for a Local session
        // shouldn't take it down. The host's own close path is
        // `Registry::leave(session, host_peer)`, not this one.
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host, None).await.unwrap();

        r.host_closed_session(id).await.unwrap();

        // Session is still here.
        assert_eq!(r.list().await.len(), 1);
    }

    // ---- attachments ----

    const KIND_V1: &str = "artel-fs/workspace/v1";

    #[tokio::test]
    async fn register_attachment_persists_via_store() {
        let r = registry();
        let alice = peer(1, "alice");
        let (id, _) = r.host_rw(alice, None).await.unwrap();
        r.register_attachment(id, KIND_V1.into(), b"payload".to_vec())
            .await
            .unwrap();
        let listed = r.list_attachments(None).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].session, id);
        assert_eq!(listed[0].kind, KIND_V1);
        assert_eq!(listed[0].payload, b"payload");
    }

    #[tokio::test]
    async fn register_attachment_for_unknown_session_returns_unknown_session_error() {
        let r = registry();
        let bogus = SessionId::new_random();
        let err = r
            .register_attachment(bogus, KIND_V1.into(), b"x".to_vec())
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::UnknownSession(bogus));
    }

    #[tokio::test]
    async fn list_attachments_returns_entries_across_multiple_sessions() {
        let r = registry();
        let (id1, _) = r.host_rw(peer(1, "alice"), None).await.unwrap();
        let (id2, ticket2) = r.host_rw(peer(2, "bob"), None).await.unwrap();
        let _ = ticket2;
        r.register_attachment(id1, KIND_V1.into(), b"one".to_vec())
            .await
            .unwrap();
        r.register_attachment(id2, KIND_V1.into(), b"two".to_vec())
            .await
            .unwrap();

        let mut listed = r.list_attachments(None).await.unwrap();
        listed.sort_by_key(|s| s.session);
        let mut want = vec![id1, id2];
        want.sort();
        assert_eq!(listed.iter().map(|s| s.session).collect::<Vec<_>>(), want);
    }

    #[tokio::test]
    async fn forget_attachment_removes_entry() {
        let r = registry();
        let (id, _) = r.host_rw(peer(1, "alice"), None).await.unwrap();
        r.register_attachment(id, KIND_V1.into(), b"x".to_vec())
            .await
            .unwrap();
        r.forget_attachment(id, KIND_V1.into()).await.unwrap();
        assert!(r.list_attachments(None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn cascade_removes_attachments_when_host_leaves() {
        let r = registry();
        let alice = peer(1, "alice");
        let (id, _) = r.host_rw(alice.clone(), None).await.unwrap();
        r.register_attachment(id, KIND_V1.into(), b"x".to_vec())
            .await
            .unwrap();

        r.leave(id, alice.id).await.unwrap();

        // Session is gone and so is the attachment — list_attachments
        // returns empty rather than a dangling entry.
        assert!(r.list_attachments(None).await.unwrap().is_empty());
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn cascade_removes_attachments_when_remote_session_closes() {
        let daemon_peer = PeerId::from_bytes([7; 32]);
        let remote_host = peer(1, "alice");
        let session_id = SessionId::from_bytes([0xaa; 16]);
        let me = peer(2, "bob");

        let store = Arc::new(crate::store::MemoryStore::new());
        let record = SessionRecord {
            id: session_id,
            host: remote_host.id,
            members: HashSet::from([remote_host.id, me.id]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Remote,
            host_epoch: 0,
            tickets: Vec::new(),
            workspace_ticket: None,
        };
        store.create(&record).await.unwrap();
        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            store.clone(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        r.register_attachment(session_id, KIND_V1.into(), b"x".to_vec())
            .await
            .unwrap();
        r.host_closed_session(session_id).await.unwrap();

        assert!(r.list_attachments(None).await.unwrap().is_empty());
    }

    /// A joiner leaving a `Remote` mirror fully drops the mirror —
    /// store record gone, in-memory entry gone, attachment cascaded,
    /// `Event::SessionClosed` emitted. Symmetric with
    /// `host_closed_session`'s teardown but triggered by a local IPC
    /// leave instead of a gossip `SessionClosed` from the host.
    #[tokio::test]
    async fn joiner_leave_remote_drops_mirror_and_cascades_attachment() {
        let daemon_peer = PeerId::from_bytes([7; 32]);
        let remote_host = peer(1, "alice");
        let session_id = SessionId::from_bytes([0xaa; 16]);
        let me = peer(2, "bob");

        let store = Arc::new(crate::store::MemoryStore::new());
        let record = SessionRecord {
            id: session_id,
            host: remote_host.id,
            members: HashSet::from([remote_host.id, me.id]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Remote,
            host_epoch: 0,
            tickets: Vec::new(),
            workspace_ticket: None,
        };
        store.create(&record).await.unwrap();
        let r = Registry::load(
            daemon_peer,
            WireEndpointAddr::id_only(daemon_peer),
            store.clone(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        r.register_attachment(session_id, KIND_V1.into(), b"x".to_vec())
            .await
            .unwrap();
        let mut sub = r.subscribe(session_id, None).await.unwrap();

        r.leave(session_id, me.id).await.unwrap();

        let event = timeout(Duration::from_millis(100), sub.events.recv())
            .await
            .expect("event")
            .unwrap();
        assert_eq!(
            event,
            Event::SessionClosed {
                session: session_id,
            }
        );
        assert!(r.list().await.is_empty(), "mirror should be gone");
        assert!(
            store.load_all().await.unwrap().is_empty(),
            "persisted record should be deleted",
        );
        assert!(
            r.list_attachments(None).await.unwrap().is_empty(),
            "attachment should cascade-delete with the mirror",
        );
    }

    /// Joiner of a `Local` session leaving (i.e. another peer left
    /// our hosted session) keeps the session alive — the host and
    /// any other members are still in it. Just an unmember +
    /// `Event::PeerLeft`.
    #[tokio::test]
    async fn joiner_leave_local_session_keeps_session_alive() {
        let r = registry();
        let host = peer(1, "alice");
        let bob = peer(2, "bob");
        let charlie = peer(3, "charlie");
        let (id, ticket) = r.host_rw(host.clone(), None).await.unwrap();
        r.join(&ticket, bob.clone()).await.unwrap();
        r.join(&ticket, charlie.clone()).await.unwrap();

        // Bob (a joiner of our Local session) leaves.
        r.leave(id, bob.id).await.unwrap();

        // Session still alive: alice + charlie remain.
        let summaries = r.list().await;
        assert_eq!(summaries.len(), 1, "session must persist");
        assert_eq!(summaries[0].peer_count, 2);
    }

    /// Race-regression: `register_attachment` vs. `leave` on the same
    /// session. Without the per-session lock in `register_attachment`
    /// + the matching lock in `leave`'s critical section, the put
    /// could land *after* the cascade ran, orphaning the attachment.
    ///
    /// Drives the race deterministically: spawn a register task and
    /// a leave task, await both, then assert the cascade contract:
    /// either register won (attachment present, session still there)
    /// or leave won (no session, no attachment) — never both.
    /// Loops to give the scheduler many chances to interleave; any
    /// orphan is a hard failure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn race_register_attachment_vs_leave_does_not_orphan() {
        for _ in 0..200 {
            let r = Arc::new(registry());
            let alice = peer(1, "alice");
            let (id, _) = r.host_rw(alice.clone(), None).await.unwrap();

            let r1 = Arc::clone(&r);
            let r2 = Arc::clone(&r);
            let alice_id = alice.id;
            let register = tokio::spawn(async move {
                r1.register_attachment(id, KIND_V1.into(), b"x".to_vec())
                    .await
            });
            let leave = tokio::spawn(async move { r2.leave(id, alice_id).await });

            // Both tasks must complete (one will race-win, the other
            // may see UnknownSession or the cascade already-ran path).
            let _ = register.await.unwrap();
            let _ = leave.await.unwrap();

            // Cascade contract: if the session is gone, no attachment
            // for it may survive in the store.
            let listed = r.list_attachments(None).await.unwrap();
            for entry in &listed {
                assert_eq!(
                    entry.session, id,
                    "stray attachment for unknown session: {entry:?}"
                );
            }
            // The session may or may not still exist depending on
            // which task observably won. If it doesn't, list_attachments
            // must be empty.
            let session_present = r.list().await.iter().any(|s| s.id == id);
            if !session_present {
                assert!(
                    listed.is_empty(),
                    "session gone but attachment leaked: {listed:?}",
                );
            }
        }
    }

    // ---- cap_claim verification (tiered tickets Phase 2A) ----

    /// A sig-valid claim whose ticket id was NEVER minted by the
    /// registry. Pre-issued-only this admitted; now only the checks
    /// that fire before the ledger (expiry, cap-sig) can pass it.
    /// Tests that need an *admissible* claim use [`minted_cap_claim`].
    #[cfg(feature = "iroh")]
    fn valid_cap_claim(
        host_key: &iroh::SecretKey,
        session: SessionId,
        granted_cap: Capability,
        expiry_ms: u64,
    ) -> CapClaim {
        use artel_protocol::ids::TicketId;
        let ticket_id = TicketId::from_bytes([0xCC; 16]);
        let cap_sig = signing::sign_ticket_cap(
            host_key.as_signing_key(),
            ticket_id,
            session,
            granted_cap,
            expiry_ms,
        );
        CapClaim {
            ticket_id,
            granted_cap,
            expiry_ms,
            cap_sig,
        }
    }

    /// Decode a just-minted ticket into the `CapClaim` its bearer
    /// would announce at join time.
    #[cfg(feature = "iroh")]
    fn claim_of(ticket: &JoinTicket) -> CapClaim {
        let decoded = ticket::decode(ticket.as_str()).unwrap();
        CapClaim {
            ticket_id: decoded.ticket_id,
            granted_cap: decoded.granted_cap,
            expiry_ms: decoded.expiry_ms,
            cap_sig: decoded.cap_sig,
        }
    }

    /// Mint a real ticket through the registry (recording it in the
    /// ledger) and shape it into the `CapClaim` its bearer would
    /// announce. The issued-only admission path accepts exactly these.
    #[cfg(feature = "iroh")]
    async fn minted_cap_claim(
        r: &Registry,
        session: SessionId,
        granted_cap: Capability,
        expiry_ms: u64,
    ) -> CapClaim {
        let (ticket, ticket_id) = r
            .issue_ticket(session, granted_cap, expiry_ms)
            .await
            .unwrap();
        let claim = claim_of(&ticket);
        assert_eq!(claim.ticket_id, ticket_id);
        claim
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn ensure_member_with_valid_rw_claim_grants_rw() {
        let (r, host, _key) = registry_with_signing_seed(0x50);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let bob = peer(2, "bob");
        let claim = minted_cap_claim(&r, id, Capability::ReadWrite, 0).await;
        r.ensure_member(id, bob.clone(), Some(claim)).await.unwrap();

        // Bob can write.
        let sent = r
            .send(
                id,
                bob.clone(),
                MessageKind::Chat,
                "hi".into(),
                b"hello".to_vec(),
                Authoring::Local,
            )
            .await;
        assert!(sent.is_ok(), "RW-claimed peer can write: {sent:?}");
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn ensure_member_with_valid_read_claim_grants_read() {
        let (r, host, _key) = registry_with_signing_seed(0x51);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let bob = peer(2, "bob");
        let claim = minted_cap_claim(&r, id, Capability::Read, 0).await;
        r.ensure_member(id, bob.clone(), Some(claim)).await.unwrap();

        // Bob cannot write — cap is Read.
        let sent = r
            .send(
                id,
                bob.clone(),
                MessageKind::Chat,
                "hi".into(),
                b"hello".to_vec(),
                Authoring::Local,
            )
            .await;
        assert_eq!(
            sent.unwrap_err(),
            SessionError::CapabilityDenied {
                peer_id: bob.id,
                had: Some(Capability::Read),
                needed: Capability::ReadWrite,
            },
        );
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn ensure_member_rejects_forged_cap_sig() {
        let (r, host, _host_key) = registry_with_signing_seed(0x52);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let bob = peer(2, "bob");

        // Sign with a different key (attacker key) — sig won't verify
        // against the host's pubkey.
        let attacker_key = iroh::SecretKey::from_bytes(&[0xAA; 32]);
        let claim = valid_cap_claim(&attacker_key, id, Capability::ReadWrite, 0);

        let err = r
            .ensure_member(id, bob.clone(), Some(claim))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            SessionError::InvalidCapClaim("signature does not verify".into()),
        );
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn ensure_member_rejects_expired_ticket() {
        let (r, host, key) = registry_with_signing_seed(0x53);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let bob = peer(2, "bob");

        // Expiry set to 1ms — always in the past.
        let claim = valid_cap_claim(&key, id, Capability::ReadWrite, 1);

        let err = r
            .ensure_member(id, bob.clone(), Some(claim))
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::TicketExpired);
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn ensure_member_with_none_claim_grants_rw_backwards_compat() {
        // The `None` path (SendRequest backstop) still grants RW for
        // pre-tiered-ticket joiners.
        let (r, host, _) = registry_with_signing_seed(0x54);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let bob = peer(2, "bob");
        r.ensure_member(id, bob.clone(), None).await.unwrap();

        let sent = r
            .send(
                id,
                bob.clone(),
                MessageKind::Chat,
                "hi".into(),
                b"hello".to_vec(),
                Authoring::Local,
            )
            .await;
        assert!(sent.is_ok(), "None-claim peer gets RW: {sent:?}");
    }

    // ---- ticket revocation (issued-ticket ledger) ----

    #[tokio::test]
    async fn revoke_is_idempotent_and_persists_status() {
        let r = registry();
        let (id, _) = r.host_rw(peer(1, "alice"), None).await.unwrap();
        let (_, tid) = r.issue_ticket(id, Capability::Read, 0).await.unwrap();

        r.revoke_ticket(id, tid).await.unwrap();
        // Second revoke of the same id is Ok, not an error.
        r.revoke_ticket(id, tid).await.unwrap();

        let entry = r
            .list_tickets(id)
            .await
            .unwrap()
            .into_iter()
            .find(|t| t.ticket_id == tid)
            .unwrap();
        assert_eq!(entry.status, TicketStatus::Revoked);
        // The store saw the flip, not just memory.
        let persisted = r.store.load_all().await.unwrap()[0]
            .tickets
            .iter()
            .find(|t| t.ticket_id == tid)
            .unwrap()
            .status;
        assert_eq!(persisted, TicketStatus::Revoked);
    }

    #[tokio::test]
    async fn revoke_never_issued_ticket_is_unknown_ticket() {
        let r = registry();
        let (id, _) = r.host_rw(peer(1, "alice"), None).await.unwrap();
        let bogus = TicketId::from_bytes([0xEE; 16]);
        let err = r.revoke_ticket(id, bogus).await.unwrap_err();
        assert_eq!(err, SessionError::UnknownTicket(bogus));
    }

    #[tokio::test]
    async fn revoke_and_list_on_unknown_session_error() {
        let r = registry();
        let fake = SessionId::from_bytes([0xDE; 16]);
        let tid = TicketId::from_bytes([0xEE; 16]);
        assert_eq!(
            r.revoke_ticket(fake, tid).await.unwrap_err(),
            SessionError::UnknownSession(fake),
        );
        assert_eq!(
            r.list_tickets(fake).await.unwrap_err(),
            SessionError::UnknownSession(fake),
        );
    }

    #[tokio::test]
    async fn revoke_and_list_on_remote_mirror_return_not_host() {
        // Same mirror-seeding shape as
        // `issue_ticket_for_remote_session_returns_not_host`.
        let r = registry();
        let host = peer(1, "alice");
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let r2 = registry_with_peer(PeerId::from_bytes([0xAA; 32]));
        {
            let session = Session::new(id, &host, SessionKind::Remote);
            r2.sessions
                .write()
                .await
                .insert(id, Arc::new(Mutex::new(session)));
        }
        let tid = TicketId::from_bytes([0xEE; 16]);
        assert_eq!(
            r2.revoke_ticket(id, tid).await.unwrap_err(),
            SessionError::NotHost,
        );
        assert_eq!(
            r2.list_tickets(id).await.unwrap_err(),
            SessionError::NotHost
        );
    }

    #[tokio::test]
    async fn all_three_mint_sites_write_ledger_entries() {
        // Create, explicit issue, and resume must each append exactly
        // one entry — under issued-only a mint site that forgets its
        // ledger write produces a ticket that can never admit.
        let r = registry();
        let alice = peer(1, "alice");
        let (id, _ticket, create_tid) = r
            .host(alice.clone(), None, Capability::ReadWrite, 0)
            .await
            .unwrap();
        let ledger = r.list_tickets(id).await.unwrap();
        assert_eq!(ledger.len(), 1, "create path records its mint");
        assert_eq!(ledger[0].ticket_id, create_tid);

        let (_, issue_tid) = r.issue_ticket(id, Capability::Read, 0).await.unwrap();
        assert_eq!(r.list_tickets(id).await.unwrap().len(), 2);

        // Resume re-mints a fresh random id — a NEW entry, not a
        // dedup (the bearer string just handed back genuinely admits
        // and must be independently revocable).
        let (_, _ticket, resume_tid) = r
            .host(alice.clone(), Some(id), Capability::ReadWrite, 0)
            .await
            .unwrap();
        let ledger = r.list_tickets(id).await.unwrap();
        assert_eq!(ledger.len(), 3, "resume appends, never dedups");
        assert_ne!(resume_tid, create_tid);
        assert_ne!(resume_tid, issue_tid);

        // All three landed in the store, not just memory.
        assert_eq!(r.store.load_all().await.unwrap()[0].tickets.len(), 3);
    }

    /// Delegates to [`MemoryStore`] but lets a test park the resume
    /// path at its `bump_host_epoch` store write (only the resume
    /// branch of `host` calls it) and fail the next `put_tickets`.
    /// Pins the resume path's lock discipline and store-before-memory
    /// ordering against the other ledger writers.
    #[derive(Debug)]
    struct ResumeProbeStore {
        inner: crate::store::MemoryStore,
        park_epoch: std::sync::atomic::AtomicBool,
        epoch_gate: tokio::sync::Semaphore,
        fail_next_put_tickets: std::sync::atomic::AtomicBool,
    }

    impl ResumeProbeStore {
        fn new() -> Self {
            Self {
                inner: crate::store::MemoryStore::new(),
                park_epoch: std::sync::atomic::AtomicBool::new(false),
                epoch_gate: tokio::sync::Semaphore::new(0),
                fail_next_put_tickets: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn park_epoch_writes(&self) {
            self.park_epoch
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn release_epoch_writes(&self) {
            self.epoch_gate.add_permits(1);
        }

        fn fail_next_put_tickets(&self) {
            self.fail_next_put_tickets
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl crate::store::SessionStore for ResumeProbeStore {
        async fn create(&self, record: &SessionRecord) -> std::io::Result<()> {
            self.inner.create(record).await
        }

        async fn append(
            &self,
            session: SessionId,
            message: &SessionMessage,
        ) -> std::io::Result<()> {
            self.inner.append(session, message).await
        }

        async fn bump_host_epoch(&self, session: SessionId, epoch: u64) -> std::io::Result<()> {
            if self.park_epoch.load(std::sync::atomic::Ordering::SeqCst) {
                self.epoch_gate
                    .acquire()
                    .await
                    .expect("gate never closed")
                    .forget();
            }
            self.inner.bump_host_epoch(session, epoch).await
        }

        async fn put_tickets(
            &self,
            session: SessionId,
            tickets: &[TicketEntry],
        ) -> std::io::Result<()> {
            if self
                .fail_next_put_tickets
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(std::io::Error::other("injected put_tickets failure"));
            }
            self.inner.put_tickets(session, tickets).await
        }

        async fn put_workspace_ticket(
            &self,
            session: SessionId,
            envelope: &[u8],
        ) -> std::io::Result<()> {
            self.inner.put_workspace_ticket(session, envelope).await
        }

        async fn add_member(&self, session: SessionId, peer: &PeerInfo) -> std::io::Result<()> {
            self.inner.add_member(session, peer).await
        }

        async fn remove_member(&self, session: SessionId, peer: PeerId) -> std::io::Result<()> {
            self.inner.remove_member(session, peer).await
        }

        async fn delete(&self, session: SessionId) -> std::io::Result<()> {
            self.inner.delete(session).await
        }

        async fn load_all(&self) -> std::io::Result<Vec<SessionRecord>> {
            self.inner.load_all().await
        }

        async fn put_attachment(
            &self,
            session: SessionId,
            kind: &str,
            payload: &[u8],
        ) -> std::io::Result<bool> {
            self.inner.put_attachment(session, kind, payload).await
        }

        async fn list_attachments(
            &self,
            kind_filter: Option<&str>,
        ) -> std::io::Result<Vec<StoredAttachment>> {
            self.inner.list_attachments(kind_filter).await
        }

        async fn delete_attachment(&self, session: SessionId, kind: &str) -> std::io::Result<()> {
            self.inner.delete_attachment(session, kind).await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_revoke_during_resume_survives_on_disk() {
        // Lost-update pin: a revoke that commits while a resume is
        // mid-flight must not be overwritten on disk by the resume's
        // ledger snapshot. The probe parks the resume at its
        // bump_host_epoch store write; with the paused clock, the
        // sleep below only fires once both tasks are blocked, so the
        // interleaving is deterministic, not timing-luck.
        let probe = Arc::new(ResumeProbeStore::new());
        let r = Arc::new(Registry::new(
            PeerId::from_bytes([0xff; 32]),
            Arc::clone(&probe) as DynStore,
        ));
        let alice = peer(1, "alice");
        let (id, _, _) = r
            .host(alice.clone(), None, Capability::ReadWrite, 0)
            .await
            .unwrap();
        let (_, victim) = r.issue_ticket(id, Capability::Read, 0).await.unwrap();

        probe.park_epoch_writes();
        let resume = tokio::spawn({
            let r = Arc::clone(&r);
            let alice = alice.clone();
            async move { r.host(alice, Some(id), Capability::ReadWrite, 0).await }
        });
        let revoke = tokio::spawn({
            let r = Arc::clone(&r);
            async move { r.revoke_ticket(id, victim).await }
        });
        // Fires once the resume is parked on the gate and the revoke
        // has run as far as the lock discipline lets it.
        tokio::time::sleep(Duration::from_millis(10)).await;
        probe.release_epoch_writes();
        resume.await.unwrap().unwrap();
        revoke.await.unwrap().unwrap();

        let persisted = r.store.load_all().await.unwrap()[0]
            .tickets
            .iter()
            .find(|t| t.ticket_id == victim)
            .unwrap()
            .status;
        assert_eq!(
            persisted,
            TicketStatus::Revoked,
            "revocation must survive a concurrent resume's ledger rewrite",
        );
    }

    #[tokio::test]
    async fn failed_resume_store_write_leaves_no_phantom_ledger_entry() {
        // Store-before-memory pin for the resume path: if the ledger
        // write fails, the failed mint must leave no trace in memory
        // — no phantom Active entry, no epoch bump — and no later
        // successful write may resurrect one to disk.
        let probe = Arc::new(ResumeProbeStore::new());
        let r = Registry::new(
            PeerId::from_bytes([0xff; 32]),
            Arc::clone(&probe) as DynStore,
        );
        let alice = peer(1, "alice");
        let (id, _, create_tid) = r
            .host(alice.clone(), None, Capability::ReadWrite, 0)
            .await
            .unwrap();

        probe.fail_next_put_tickets();
        let err = r
            .host(alice.clone(), Some(id), Capability::ReadWrite, 0)
            .await
            .unwrap_err();
        assert!(matches!(err, SessionError::Storage(_)));

        // Memory: only the create-path entry, and no epoch bump.
        let in_memory = r.list_tickets(id).await.unwrap();
        assert_eq!(
            in_memory.iter().map(|t| t.ticket_id).collect::<Vec<_>>(),
            vec![create_tid],
            "failed resume must not leave a phantom entry in memory",
        );
        let epoch = {
            let arc = r.sessions.read().await.get(&id).cloned().unwrap();
            let s = arc.lock().await;
            s.host_epoch
        };
        assert_eq!(epoch, 0, "failed resume must not bump the in-memory epoch");

        // A later successful mutation must not resurrect the phantom.
        let (_, issued) = r.issue_ticket(id, Capability::Read, 0).await.unwrap();
        let persisted: Vec<TicketId> = r.store.load_all().await.unwrap()[0]
            .tickets
            .iter()
            .map(|t| t.ticket_id)
            .collect();
        assert_eq!(persisted, vec![create_tid, issued]);
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn revoked_ticket_rejected_while_sibling_ticket_admits() {
        let (r, host, _key) = registry_with_signing_seed(0x60);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let claim_a = minted_cap_claim(&r, id, Capability::ReadWrite, 0).await;
        let claim_b = minted_cap_claim(&r, id, Capability::ReadWrite, 0).await;

        r.revoke_ticket(id, claim_a.ticket_id).await.unwrap();

        let err = r
            .ensure_member(id, peer(2, "bob"), Some(claim_a))
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::TicketNotAdmissible);
        // Revocation is per-ticket: the sibling still admits.
        r.ensure_member(id, peer(3, "carol"), Some(claim_b))
            .await
            .unwrap();
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn expired_and_revoked_claim_reports_expiry() {
        // Check-order pin: expiry fires before the ledger gate, so a
        // claim that is both expired and revoked surfaces
        // TicketExpired (the pre-revocation behaviour an
        // unauthenticated bearer could already observe).
        let (r, host, _key) = registry_with_signing_seed(0x61);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let claim = minted_cap_claim(&r, id, Capability::ReadWrite, 1).await;
        r.revoke_ticket(id, claim.ticket_id).await.unwrap();

        let err = r
            .ensure_member(id, peer(2, "bob"), Some(claim))
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::TicketExpired);
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn admissions_append_used_by_in_order() {
        async fn used_by(r: &Registry, id: SessionId, tid: TicketId) -> Vec<PeerId> {
            r.list_tickets(id)
                .await
                .unwrap()
                .into_iter()
                .find(|t| t.ticket_id == tid)
                .unwrap()
                .used_by
        }

        let (r, host, _key) = registry_with_signing_seed(0x62);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let claim = minted_cap_claim(&r, id, Capability::ReadWrite, 0).await;
        let bob = peer(2, "bob");
        let carol = peer(3, "carol");

        r.ensure_member(id, bob.clone(), Some(claim.clone()))
            .await
            .unwrap();
        assert_eq!(used_by(&r, id, claim.ticket_id).await, vec![bob.id]);

        // Multi-use bearer token: a second peer on the same ticket is
        // legal and both admissions are listed, in order.
        r.ensure_member(id, carol.clone(), Some(claim.clone()))
            .await
            .unwrap();
        assert_eq!(
            used_by(&r, id, claim.ticket_id).await,
            vec![bob.id, carol.id],
        );

        // Re-announcement from an existing member doesn't duplicate.
        r.ensure_member(id, bob.clone(), Some(claim.clone()))
            .await
            .unwrap();
        assert_eq!(
            used_by(&r, id, claim.ticket_id).await,
            vec![bob.id, carol.id],
        );

        // used_by is persisted, not memory-only.
        let persisted = r.store.load_all().await.unwrap()[0]
            .tickets
            .iter()
            .find(|t| t.ticket_id == claim.ticket_id)
            .unwrap()
            .used_by
            .clone();
        assert_eq!(persisted, vec![bob.id, carol.id]);
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn revoke_is_ticket_only_admitted_peer_keeps_membership() {
        let (r, host, _key) = registry_with_signing_seed(0x63);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let claim = minted_cap_claim(&r, id, Capability::ReadWrite, 0).await;
        let bob = peer(2, "bob");
        r.ensure_member(id, bob.clone(), Some(claim.clone()))
            .await
            .unwrap();

        r.revoke_ticket(id, claim.ticket_id).await.unwrap();

        // Bob was admitted before the revoke: membership and caps are
        // untouched (ticket-only revocation gates future admissions).
        let sent = r
            .send(
                id,
                bob.clone(),
                MessageKind::Chat,
                "hi".into(),
                b"still here".to_vec(),
                Authoring::Local,
            )
            .await;
        assert!(sent.is_ok(), "admitted peer survives revoke: {sent:?}");

        // A NEW bearer of the same (leaked) ticket is rejected.
        let err = r
            .ensure_member(id, peer(3, "mallory"), Some(claim))
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::TicketNotAdmissible);
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn rejoin_after_leave_with_revoked_ticket_is_rejected() {
        // The rejoin hole: admission → leave → revoke. The peer's
        // re-join re-runs announcement → ensure_member, where the
        // ledger gate now rejects — the member-set early-return no
        // longer shields them.
        let (r, host, _key) = registry_with_signing_seed(0x64);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let claim = minted_cap_claim(&r, id, Capability::ReadWrite, 0).await;
        let bob = peer(2, "bob");
        r.ensure_member(id, bob.clone(), Some(claim.clone()))
            .await
            .unwrap();

        r.leave(id, bob.id).await.unwrap();
        r.revoke_ticket(id, claim.ticket_id).await.unwrap();

        let err = r
            .ensure_member(id, bob.clone(), Some(claim))
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::TicketNotAdmissible);
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn issued_only_rejects_ledger_absent_claim_even_with_valid_sig() {
        // The stolen-signing-key / ledger-rollback case: expiry and
        // cap-sig both pass, but the ledger never saw this id.
        // Fail closed, with the SAME error as a revoked ticket so the
        // bearer can't distinguish (both map to the joiner-opaque
        // ProtocolError::InvalidTicket).
        let (r, host, key) = registry_with_signing_seed(0x65);
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let forged = valid_cap_claim(&key, id, Capability::ReadWrite, 0);

        let err = r
            .ensure_member(id, peer(2, "mallory"), Some(forged))
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::TicketNotAdmissible);
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn claim_disagreeing_with_ledger_entry_is_rejected() {
        // Ledger cross-check of cap/expiry. The cap-sig already binds
        // these for a signing host, so exercise the ledger's own
        // check through an unsigned registry (sig verification
        // skipped when no signing key is configured).
        let r = registry();
        let (id, _) = r.host_rw(peer(1, "alice"), None).await.unwrap();
        let far_future = now_ms() + 3_600_000;
        let (_, tid) = r
            .issue_ticket(id, Capability::Read, far_future)
            .await
            .unwrap();

        // Cap escalation: ledger says Read, claim says ReadWrite.
        let escalated = CapClaim {
            ticket_id: tid,
            granted_cap: Capability::ReadWrite,
            expiry_ms: far_future,
            cap_sig: SIGNATURE_UNSIGNED,
        };
        assert_eq!(
            r.ensure_member(id, peer(2, "bob"), Some(escalated))
                .await
                .unwrap_err(),
            SessionError::TicketNotAdmissible,
        );

        // Expiry stretch: ledger says far_future, claim says never.
        let stretched = CapClaim {
            ticket_id: tid,
            granted_cap: Capability::Read,
            expiry_ms: 0,
            cap_sig: SIGNATURE_UNSIGNED,
        };
        assert_eq!(
            r.ensure_member(id, peer(2, "bob"), Some(stretched))
                .await
                .unwrap_err(),
            SessionError::TicketNotAdmissible,
        );

        // The honest claim still admits.
        let honest = CapClaim {
            ticket_id: tid,
            granted_cap: Capability::Read,
            expiry_ms: far_future,
            cap_sig: SIGNATURE_UNSIGNED,
        };
        r.ensure_member(id, peer(2, "bob"), Some(honest))
            .await
            .unwrap();
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn tickets_minted_at_each_site_admit() {
        // Mint → admit round-trip per site: guards against a mint
        // site forgetting its ledger write, which under issued-only
        // would brick that ticket.
        let (r, host, _key) = registry_with_signing_seed(0x66);

        // Site 1: create.
        let (id, ticket, _) = r
            .host(host.clone(), None, Capability::ReadWrite, 0)
            .await
            .unwrap();
        r.ensure_member(id, peer(2, "bob"), Some(claim_of(&ticket)))
            .await
            .unwrap();

        // Site 2: explicit issue.
        let (ticket, _) = r.issue_ticket(id, Capability::Read, 0).await.unwrap();
        r.ensure_member(id, peer(3, "carol"), Some(claim_of(&ticket)))
            .await
            .unwrap();

        // Site 3: resume.
        let (_, ticket, _) = r
            .host(host.clone(), Some(id), Capability::ReadWrite, 0)
            .await
            .unwrap();
        r.ensure_member(id, peer(4, "dave"), Some(claim_of(&ticket)))
            .await
            .unwrap();
    }

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn fs_store_revocation_survives_registry_reload() {
        // Persistence-first rule: a revoke that only lived in memory
        // would silently re-admit after a daemon restart.
        let dir = tempfile::tempdir().unwrap();
        let store: DynStore =
            Arc::new(crate::store::FsLogStore::open(dir.path().join("sessions")).unwrap());
        let (r, host, secret) = registry_with_signing_seed_over(0x67, Arc::clone(&store));
        let (id, _) = r.host_rw(host.clone(), None).await.unwrap();
        let claim = minted_cap_claim(&r, id, Capability::ReadWrite, 0).await;
        r.revoke_ticket(id, claim.ticket_id).await.unwrap();
        drop(r);

        let r2 = Registry::load(
            host.id,
            WireEndpointAddr::id_only(host.id),
            Arc::clone(&store),
            None,
            Some(secret),
            None,
        )
        .await
        .unwrap();
        let err = r2
            .ensure_member(id, peer(2, "bob"), Some(claim.clone()))
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::TicketNotAdmissible);
        let entry = r2
            .list_tickets(id)
            .await
            .unwrap()
            .into_iter()
            .find(|t| t.ticket_id == claim.ticket_id)
            .unwrap();
        assert_eq!(entry.status, TicketStatus::Revoked);
    }
}
