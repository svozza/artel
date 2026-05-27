//! Persistence layer for sessions.
//!
//! [`SessionStore`] is the seam between the runtime
//! `super::session::Registry` and durable storage. The trait is
//! **crate-private** — it is shaped by the operations the registry
//! actually performs, not by speculation about future schemas. Two
//! implementations ship from day one so the trait stays grounded:
//!
//! - [`MemoryStore`]: in-memory, used by unit tests and as the
//!   no-persistence baseline.
//! - [`FsLogStore`]: on-disk, append-only postcard log + JSON meta.
//!   The shape ADR-001 calls for.
//!
//! A future third implementation (e.g. `SQLite`) lands as a drop-in if
//! its needs fit the trait. If they don't, we grow the trait then —
//! with two reference implementations to compare against.

// `redundant_pub_crate` fires because the parent module is already
// `pub(crate)`, so `pub(crate)` items inside are technically as visible
// as plain `pub`. But plain `pub` here trips `unreachable_pub` because
// nothing exports them publicly. The two lints are mutually
// contradictory inside crate-private modules; we allow the redundancy.
#![allow(clippy::redundant_pub_crate)]

pub(crate) mod fs;
mod record;

#[cfg(test)]
pub(crate) mod memory;

pub(crate) use fs::FsLogStore;
#[cfg(test)]
pub(crate) use memory::MemoryStore;
pub(crate) use record::{SessionKind, SessionRecord};

use std::io;
use std::sync::Arc;

use artel_protocol::{PeerId, PeerInfo, SessionId, SessionMessage};

/// Storage operations the [`super::session::Registry`] needs.
///
/// All methods are async because the on-disk implementation does I/O
/// that should cooperate with the tokio runtime. The in-memory impl
/// trivially satisfies the same shape.
#[async_trait::async_trait]
pub(crate) trait SessionStore: Send + Sync + std::fmt::Debug {
    /// Persist a brand-new session. Idempotent: writing over an
    /// existing record is fine (e.g. crash before the first append
    /// flushed).
    async fn create(&self, record: &SessionRecord) -> io::Result<()>;

    /// Append a single message to a session's log. Must be durable
    /// (fsync on disk-backed implementations) before returning.
    async fn append(&self, session: SessionId, message: &SessionMessage) -> io::Result<()>;

    /// Add a peer to a session's member set.
    async fn add_member(&self, session: SessionId, peer: &PeerInfo) -> io::Result<()>;

    /// Remove a peer from a session's member set.
    async fn remove_member(&self, session: SessionId, peer: PeerId) -> io::Result<()>;

    /// Forget the session entirely. Used when the host leaves.
    ///
    /// **Cascade invariant:** any attachments associated with `session`
    /// must be removed atomically with the session itself. The on-disk
    /// implementation gets this for free via `remove_dir_all`; the
    /// in-memory implementation must explicitly clear them.
    async fn delete(&self, session: SessionId) -> io::Result<()>;

    /// Load every session this store knows about. Called once at
    /// daemon startup. May skip sessions whose data is unrecoverable,
    /// emitting a warning, rather than failing the whole daemon.
    async fn load_all(&self) -> io::Result<Vec<SessionRecord>>;

    /// Persist (or overwrite) an attachment payload for `(session, kind)`.
    ///
    /// `kind` is opaque to the store — consumers namespace their tags
    /// (e.g. `"artel-fs/workspace/v1"`). The daemon never inspects
    /// `payload`; the store ships it back verbatim from
    /// [`Self::list_attachments`].
    ///
    /// Returns `Ok(false)` if the session is not known to the store
    /// — the caller maps this to
    /// [`artel_protocol::ProtocolError::UnknownSession`]. Returns
    /// `Ok(true)` on success. Disk-backed implementations must cap
    /// `payload` length at the same `MAX_FRAME_SIZE` the log uses;
    /// over-cap writes return `io::ErrorKind::InvalidData`.
    async fn put_attachment(
        &self,
        session: SessionId,
        kind: &str,
        payload: &[u8],
    ) -> io::Result<bool>;

    /// List every attachment matching `kind_filter`.
    ///
    /// `None` returns all kinds across all sessions; `Some(k)` returns
    /// only attachments tagged with `k`. Order is unspecified — callers
    /// that care should sort client-side. Skip-and-warn semantics on
    /// unparseable on-disk entries (mirrors [`Self::load_all`]).
    async fn list_attachments(
        &self,
        kind_filter: Option<&str>,
    ) -> io::Result<Vec<StoredAttachment>>;

    /// Remove the attachment at `(session, kind)`.
    ///
    /// Idempotent: a missing session OR a missing attachment file is
    /// `Ok(())`. The cascade in [`Self::delete`] makes "no such session"
    /// indistinguishable from "no such attachment", so we accept both.
    async fn delete_attachment(&self, session: SessionId, kind: &str) -> io::Result<()>;
}

/// Pure-data record returned by [`SessionStore::list_attachments`].
///
/// Distinct from [`artel_protocol::Attachment`] so the store stays free
/// of protocol types — same shape as `SessionRecord` vs. `Response`.
/// The server arm performs the one-line conversion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredAttachment {
    /// Session this attachment is bound to.
    pub(crate) session: SessionId,
    /// Consumer-namespaced tag.
    pub(crate) kind: String,
    /// Consumer-defined opaque bytes.
    pub(crate) payload: Vec<u8>,
}

/// Convenience alias for the type Registry holds.
pub(crate) type DynStore = Arc<dyn SessionStore>;
