//! Pure-data record of a session, shared by all [`super::SessionStore`]
//! implementations.

#![allow(clippy::redundant_pub_crate)]

use std::collections::HashSet;

use artel_protocol::{PeerId, Seq, SessionId, SessionMessage};
use serde::{Deserialize, Serialize};

/// Whether this daemon owns the authoritative log for the session
/// or is mirroring another daemon's. Persisted so a daemon restart
/// rehydrates remote mirrors as `Remote` rather than mistaking them
/// for local-host sessions and trying to assign seqs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum SessionKind {
    /// Session was created here via `Registry::host` — we are the
    /// sequencer, our log is canonical. Default for backward
    /// compatibility with on-disk records that predate this field
    /// (pre-2c-2e there was no way for a remote mirror to reach
    /// disk, so the default is correct retroactively).
    #[default]
    Local,
    /// Session was materialised here via `Registry::join` for a
    /// ticket whose host lives elsewhere. Log entries flow in over
    /// gossip; we never assign seqs locally.
    Remote,
}

/// Everything a [`super::SessionStore`] needs to persist about a
/// session. Logic lives on `super::session::Session`; this is the
/// disk shape.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SessionRecord {
    /// Identifier.
    pub(crate) id: SessionId,
    /// Peer that hosts the session (assigns sequence numbers).
    pub(crate) host: PeerId,
    /// Active member set, including the host. Only the cryptographic
    /// id is persisted; display names are advisory and re-established
    /// when a peer reconnects via `JoinSession`.
    pub(crate) members: HashSet<PeerId>,
    /// Highest sequence number observed.
    pub(crate) head: Seq,
    /// Ordered message log.
    pub(crate) log: Vec<SessionMessage>,
    /// Whether this daemon is the host (`Local`) or a mirror of a
    /// remote host (`Remote`). See [`SessionKind`].
    #[serde(default)]
    pub(crate) kind: SessionKind,
}
