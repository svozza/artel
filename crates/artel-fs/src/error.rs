//! Errors surfaced by `artel-fs`.

use std::io;
use std::path::PathBuf;

use artel_client::ClientError;

use crate::rules::PathRulesError;
use crate::ticket::TicketEnvelopeError;

/// Workspace-side error type.
///
/// Callers see these out of [`crate::Workspace`]'s constructors and
/// helpers. Live-loop errors from the watcher / applier flow to
/// consumers as [`crate::WorkspaceEvent::Error`] instead.
#[derive(Debug, thiserror::Error)]
pub enum WorkspaceError {
    /// I/O on the workspace root.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// A path-to-key (or key-to-path) translation rejected its input.
    /// Carries the offending path or key plus a short reason.
    #[error("invalid path: {0}")]
    InvalidPath(String),

    /// Bringing up the workspace's own iroh node, doc, or blob store
    /// failed.
    #[error("iroh: {0}")]
    Iroh(String),

    /// A doc operation (set/get/share/import) failed.
    #[error("doc: {0}")]
    Doc(String),

    /// The supplied [`crate::AttachPolicy`] was violated by the state
    /// of the workspace root. Surfaces `RequireEmpty` rejections of
    /// non-empty dirs and the `InitFromExisting`-on-join error.
    #[error("attach policy: {0}")]
    Policy(#[from] PolicyViolation),

    /// `workspace.ticket` envelope encode/decode failed. Joiners see
    /// this against a host that publishes the legacy raw
    /// `DocTicket`-string payload — hard-rejected by design.
    #[error("ticket envelope: {0}")]
    TicketEnvelope(#[from] TicketEnvelopeError),

    /// [`crate::PathRules`] failed validation at originate-time.
    #[error("path rules: {0}")]
    PathRules(#[from] PathRulesError),

    /// IPC roundtrip via the artel client failed.
    #[error(transparent)]
    Client(#[from] ClientError),
}

/// Why an [`crate::AttachPolicy`] check failed.
///
/// Surfaces inside [`WorkspaceError::Policy`] from
/// [`crate::Workspace::host`] / [`crate::Workspace::join`] (and their
/// `_with` variants) before any disk state is created. A `Policy`
/// error guarantees no state directory was written.
#[derive(Debug, thiserror::Error)]
pub enum PolicyViolation {
    /// The workspace root was non-empty under [`crate::AttachPolicy::RequireEmpty`].
    ///
    /// `offending_entries` lists up to the first 5 top-level entries
    /// that don't qualify as empty (i.e. neither the workspace's own
    /// state directory nor a hardcoded-skip path like `.git`,
    /// `target`, etc.). Truncated for legibility — there may be more.
    #[error(
        "refused to attach to non-empty workspace root {}: contains {} (truncated to first {} entries)",
        root.display(),
        offending_entries
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", "),
        offending_entries.len(),
    )]
    DirNotEmpty {
        /// Canonicalised workspace root.
        root: PathBuf,
        /// Up to 5 top-level entries that caused the rejection.
        offending_entries: Vec<PathBuf>,
    },

    /// [`crate::AttachPolicy::InitFromExisting`] is meaningless on
    /// the joiner side — joiners don't have a canonical tree to seed
    /// from. Use [`crate::AttachPolicy::AllowExisting`] if you really
    /// want to layer the synced doc onto an existing dir.
    #[error("InitFromExisting is only meaningful when hosting; on join, use AllowExisting")]
    InitFromExistingNotMeaningfulOnJoin,
}
