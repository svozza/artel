//! Errors surfaced by `artel-fs`.

use std::io;

use artel_client::ClientError;

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

    /// IPC roundtrip via the artel client failed.
    #[error(transparent)]
    Client(#[from] ClientError),
}
