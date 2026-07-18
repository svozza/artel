//! Filesystem workspace sync built on top of artel sessions.
//!
//! ADR-001 § "Doc handles across IPC" picked the **ticket-handout**
//! shape for v1. Concretely:
//!
//! - `artel-daemon` knows nothing about `iroh-docs` or `iroh-blobs`.
//! - `artel-fs::Workspace` spawns its **own** iroh `Endpoint` +
//!   `Gossip` + `Docs` + `Blobs`, distinct from the daemon's.
//! - The host wraps the resulting `DocTicket` in a versioned
//!   `WorkspaceTicketEnvelope` (postcard) and publishes it via
//!   `Request::PublishWorkspaceTicket`; the daemon persists the
//!   envelope and unicasts it to each member over the direct stream.
//!   Joiners observe it as a synthetic `workspace.ticket` System
//!   message on the events stream (bare `DocTicket` payloads are
//!   hard-rejected).
//!
//! Public surface today:
//! - [`Workspace::host`] / [`Workspace::join`] — host or attach to a
//!   workspace on an existing artel session.
//! - [`Workspace::run`] — spawn the watcher + applier so live edits
//!   propagate.
//! - The pure-logic helpers ([`keys`], [`filter`], [`echo_guard`])
//!   are public so apps and tests can reuse them.

#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]

mod applier;
pub mod attachment;
mod docs_gate;
pub mod echo_guard;
pub mod error;
pub mod filter;
pub mod keys;
mod keystore;
mod node;
mod peer_filter;
pub(crate) mod peer_map;
mod progress;
pub mod rules;
pub mod session_id;
pub mod ticket;
mod watcher;
pub mod workspace;

pub use attachment::{
    KIND_V1, KnownWorkspace, WorkspaceAttachmentV1, WorkspaceRole, list_known_workspaces,
};
// Endpoint-discovery setup is shared with `artel-daemon` via
// `artel-iroh-setup` (the two are peer crates; neither may depend on
// the other). Re-exported so consumers keep the
// `artel_fs::EndpointSetup` path.
pub use artel_iroh_setup::EndpointSetup;
#[cfg(feature = "test-utils")]
pub use artel_iroh_setup::TEST_DNS_ORIGIN;
pub use echo_guard::EchoGuard;
pub use error::{PolicyViolation, WorkspaceError};
pub use filter::{ExcludeRules, FilterDecision, SkipReason, WorkspaceFilter};
pub use keys::{KEY_PREFIX, key_to_path, path_to_key};
pub use rules::{CompiledPathRules, Mode, PathRule, PathRules, PathRulesError};
pub use session_id::session_id_for;
pub use ticket::{TicketEnvelopeError, WorkspaceTicketEnvelope};
pub use workspace::{
    AttachPolicy, DEFAULT_MAX_FILE_SIZE, Direction, NODE_ID_ACTION, TICKET_ACTION, Workspace,
    WorkspaceConfig, WorkspaceEvent,
};
