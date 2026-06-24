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

#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]

mod applier;
pub mod attachment;
mod docs_gate;
pub mod echo_guard;
mod endpoint_setup;
pub mod error;
pub mod filter;
pub mod keys;
mod keystore;
mod node;
mod peer_filter;
pub(crate) mod peer_map;
pub mod rules;
pub mod session_id;
pub mod ticket;
mod watcher;
pub mod workspace;

pub use attachment::{
    KIND_V1, KnownWorkspace, WorkspaceAttachmentV1, WorkspaceRole, list_known_workspaces,
};
pub use echo_guard::EchoGuard;
pub use endpoint_setup::EndpointSetup;
#[cfg(feature = "test-utils")]
pub use endpoint_setup::TEST_DNS_ORIGIN;
pub use error::{PolicyViolation, WorkspaceError};
pub use filter::{FilterDecision, MAX_FILE_SIZE, SkipReason, WorkspaceFilter};
pub use keys::{KEY_PREFIX, key_to_path, path_to_key};
pub use rules::{CompiledPathRules, Mode, PathRule, PathRules, PathRulesError};
pub use session_id::session_id_for;
pub use ticket::{TicketEnvelopeError, WorkspaceTicketEnvelope};
pub use workspace::{
    AttachPolicy, Direction, NODE_ID_ACTION, TICKET_ACTION, Workspace, WorkspaceConfig,
    WorkspaceEvent,
};
