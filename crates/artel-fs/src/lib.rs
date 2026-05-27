//! Filesystem workspace sync built on top of artel sessions.
//!
//! ADR-001 § "Doc handles across IPC" picked the **ticket-handout**
//! shape for v1. Concretely:
//!
//! - `artel-daemon` knows nothing about `iroh-docs` or `iroh-blobs`.
//! - `artel-fs::Workspace` spawns its **own** iroh `Endpoint` +
//!   `Gossip` + `Docs` + `Blobs`, distinct from the daemon's.
//! - The host shares the resulting `DocTicket` over the artel session
//!   as a `MessageKind::System` with `action = "workspace.ticket"`.
//!   Joiners pick it up off the events stream.
//!
//! Public surface today:
//! - [`Workspace::host`] / [`Workspace::join`] — host or attach to a
//!   workspace on an existing artel session.
//! - [`Workspace::run`] — spawn the watcher + applier so live edits
//!   propagate.
//! - The pure-logic helpers ([`keys`], [`filter`], [`echo_guard`])
//!   are public so apps and tests can reuse them.

mod applier;
pub mod echo_guard;
pub mod error;
pub mod filter;
pub mod keys;
mod keystore;
mod node;
pub mod rules;
pub mod session_id;
pub mod ticket;
mod watcher;
pub mod workspace;

pub use echo_guard::EchoGuard;
pub use error::{PolicyViolation, WorkspaceError};
pub use filter::{FilterDecision, MAX_FILE_SIZE, SkipReason, WorkspaceFilter};
pub use keys::{KEY_PREFIX, key_to_path, path_to_key};
pub use rules::{CompiledPathRules, Mode, PathRule, PathRules, PathRulesError};
pub use session_id::session_id_for;
pub use ticket::{TicketEnvelopeError, WorkspaceTicketEnvelope};
pub use workspace::{
    AttachPolicy, Direction, TICKET_ACTION, Workspace, WorkspaceConfig, WorkspaceEvent,
};
