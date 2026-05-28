//! Workspace-side typed attachment shipped over [`Request::RegisterAttachment`].
//!
//! `artel-fs` attaches a small typed record to its session so a CLI
//! / GUI / future tool can enumerate the workspaces the daemon knows
//! about without reading `~/.artel/` filesystem state directly.
//!
//! The daemon stores the payload opaquely — see ADR-001 § "Daemon
//! scope: medium" and the workspace registry plan's "Why a typed
//! helper, and where it lives" section. The schema lives here, in
//! the consumer crate that owns it.
//!
//! Wire shape:
//! - [`KIND_V1`] is the namespaced tag the daemon indexes against.
//!   Bumping it is a breaking change; the v1 → v2 migration ships a
//!   parallel [`KIND_V1`]-style constant rather than mutating this
//!   one.
//! - [`WorkspaceAttachmentV1`] is the postcard-encoded payload. New
//!   fields land as `Option<>`-typed with `#[serde(default)]` until a
//!   required field forces a `KIND_V2` bump.
//!
//! Decode failures in [`list_known_workspaces`] are warn-and-skipped
//! rather than propagated — see the brainstorm + plan §"Risks" for
//! the reasoning.
//!
//! ## Failure mode
//!
//! [`Workspace::host_with`] / [`Workspace::join_with`] register their
//! attachment as a hard prerequisite of standing up. A failure here
//! propagates out of the constructor — a workspace whose attachment
//! never landed is invisible to discovery tooling, which is a real
//! bug we want surfaced rather than silently degrading.
//!
//! [`Workspace::host_with`]: crate::Workspace::host_with
//! [`Workspace::join_with`]: crate::Workspace::join_with

use std::path::PathBuf;

use artel_client::Client;
use artel_protocol::{Request, Response, SessionId};
use serde::{Deserialize, Serialize};

use crate::error::WorkspaceError;

/// Kind tag for the v1 schema.
///
/// Used as the [`Request::RegisterAttachment::kind`] (and the matching
/// [`Request::ListAttachments::kind`] filter) for every workspace
/// attachment. Stable across `artel-fs` versions; bumping is a
/// breaking change handled by introducing a parallel `KIND_V2`
/// constant and migrating consumers explicitly.
pub const KIND_V1: &str = "artel-fs/workspace/v1";

/// Whether the local side of the workspace was the host or a joiner.
///
/// Surfaced inside [`WorkspaceAttachmentV1`] so a discovery consumer
/// can show the right verb in its UI without re-deriving from disk
/// state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceRole {
    /// This side hosts the workspace.
    Host,
    /// This side joined an existing workspace.
    Joiner,
}

/// Wire-stable v1 payload for the `artel-fs/workspace/v1` attachment
/// kind. Postcard-encoded into the opaque [`Attachment::payload`].
///
/// Fields are deliberately conservative — see the brainstorm's
/// "fast-follow" note for `last_seen`. New fields land as
/// `Option<>`-typed with `#[serde(default)]` until a required field
/// forces a `KIND_V2` bump.
///
/// [`Attachment::payload`]: artel_protocol::Attachment::payload
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceAttachmentV1 {
    /// Canonicalised workspace root (where the user's files live).
    pub local_path: PathBuf,
    /// Resolved state directory (default `<local_path>/.artel-fs/`,
    /// or whatever [`crate::WorkspaceConfig::with_state_dir`] set).
    pub state_dir: PathBuf,
    /// Whether this side hosts or joined.
    pub role: WorkspaceRole,
}

impl WorkspaceAttachmentV1 {
    /// Postcard-encode for shipping inside [`Attachment::payload`].
    ///
    /// `pub(crate)` because only the workspace constructors should
    /// produce the payload — consumers use [`list_known_workspaces`]
    /// to read.
    ///
    /// [`Attachment::payload`]: artel_protocol::Attachment::payload
    pub(crate) fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        postcard::to_allocvec(self)
            .map_err(|e| WorkspaceError::Iroh(format!("attachment encode: {e}")))
    }

    /// Postcard-decode from an [`Attachment::payload`].
    ///
    /// Public so test fixtures and diagnostic consumers can decode
    /// raw payload bytes (mirrors [`crate::ticket::decode`]'s shape).
    ///
    /// [`Attachment::payload`]: artel_protocol::Attachment::payload
    pub fn decode(bytes: &[u8]) -> Result<Self, WorkspaceError> {
        postcard::from_bytes(bytes)
            .map_err(|e| WorkspaceError::Iroh(format!("attachment decode: {e}")))
    }
}

/// One workspace as the daemon knows it: the underlying session id
/// plus the decoded v1 payload.
///
/// Returned from [`list_known_workspaces`]. Shape kept distinct from
/// [`artel_protocol::Attachment`] so consumers don't have to learn
/// about opaque-`payload` / `kind`-tag plumbing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KnownWorkspace {
    /// Session id this workspace is attached to.
    pub session: SessionId,
    /// Decoded v1 payload.
    pub attachment: WorkspaceAttachmentV1,
}

/// List every `artel-fs/workspace/v1` workspace the daemon knows.
///
/// Issues [`Request::ListAttachments`] with a [`KIND_V1`] filter and
/// decodes each entry's payload into a [`WorkspaceAttachmentV1`].
///
/// Entries whose payload fails to decode are logged via
/// `tracing::warn!` and skipped — they could be stragglers from a
/// future `KIND_V2` schema this build doesn't speak, and a single bad
/// payload shouldn't take down enumeration. A future
/// `list_known_workspaces_strict` helper could surface decode errors
/// instead, additively.
pub async fn list_known_workspaces(client: &Client) -> Result<Vec<KnownWorkspace>, WorkspaceError> {
    let resp = client
        .request(Request::ListAttachments {
            kind: Some(KIND_V1.to_string()),
        })
        .await
        .map_err(WorkspaceError::Client)?;
    let entries = match resp {
        Response::Attachments { entries } => entries,
        other => {
            return Err(WorkspaceError::Iroh(format!(
                "unexpected response to ListAttachments: {other:?}",
            )));
        }
    };
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        // Defence-in-depth: filter is applied server-side, but a
        // future `kind != KIND_V1` slipping through (e.g. v2
        // alongside v1) shouldn't be force-decoded as v1.
        if entry.kind != KIND_V1 {
            continue;
        }
        match WorkspaceAttachmentV1::decode(&entry.payload) {
            Ok(att) => out.push(KnownWorkspace {
                session: entry.session,
                attachment: att,
            }),
            Err(err) => {
                tracing::warn!(
                    session = %entry.session,
                    error = %err,
                    "skipping undecodeable v1 workspace attachment",
                );
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn sample() -> WorkspaceAttachmentV1 {
        WorkspaceAttachmentV1 {
            local_path: PathBuf::from("/home/alice/projects/notes"),
            state_dir: PathBuf::from("/home/alice/projects/notes/.artel-fs"),
            role: WorkspaceRole::Host,
        }
    }

    #[test]
    fn workspace_attachment_v1_round_trips_postcard() {
        let original = sample();
        let bytes = original.encode().expect("encode");
        let back = WorkspaceAttachmentV1::decode(&bytes).expect("decode");
        assert_eq!(original, back);
    }

    #[test]
    fn workspace_attachment_v1_round_trips_with_joiner_role() {
        let original = WorkspaceAttachmentV1 {
            role: WorkspaceRole::Joiner,
            ..sample()
        };
        let bytes = original.encode().unwrap();
        assert_eq!(WorkspaceAttachmentV1::decode(&bytes).unwrap(), original);
    }

    #[test]
    fn workspace_attachment_v1_decode_rejects_garbage_bytes() {
        let err = WorkspaceAttachmentV1::decode(b"not a postcard payload")
            .expect_err("decode should fail");
        assert!(matches!(err, WorkspaceError::Iroh(_)), "got {err:?}");
    }

    #[test]
    fn workspace_attachment_v1_decode_rejects_truncated_bytes() {
        // Pin "no graceful partial decode": a few bytes off the end
        // is unrecoverable, not silently filled with defaults.
        let original = sample();
        let mut bytes = original.encode().unwrap();
        let drop = bytes.len().saturating_sub(3);
        bytes.truncate(drop);
        let err = WorkspaceAttachmentV1::decode(&bytes).expect_err("decode should fail");
        assert!(matches!(err, WorkspaceError::Iroh(_)), "got {err:?}");
    }

    #[test]
    fn kind_v1_string_is_pinned() {
        // Catches accidental edits to the wire-stable tag.
        assert_eq!(KIND_V1, "artel-fs/workspace/v1");
    }
}
