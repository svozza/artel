//! Pure-data record of a session, shared by all [`super::SessionStore`]
//! implementations.

#![allow(clippy::redundant_pub_crate)]

use std::collections::HashSet;

use artel_protocol::{PeerId, Seq, SessionId, SessionMessage};
use serde::{Deserialize, Serialize};

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
}
