//! Persistence layer for sessions.
//!
//! [`SessionStore`] is the seam between the runtime
//! `super::session::Registry` and durable storage. The trait is
//! **crate-private** — it is shaped by the operations the registry
//! actually performs, not by speculation about future schemas.
//!
//! - [`FsLogStore`]: on-disk, append-only postcard log + JSON meta.
//!   The shape ADR-001 calls for. Production daemons use this.
//! - [`MemoryStore`]: in-memory, gated `#[cfg(test)]`. Used by unit
//!   tests as the no-persistence baseline so the disk impl has a
//!   reference to compare against. Not present in release builds.
//!
//! A future production-second implementation (e.g. `SQLite`) lands as
//! a drop-in if its needs fit the trait. If they don't, we grow the
//! trait then — with the in-memory test-only impl as a parallel
//! check.

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

    /// Persist a new host incarnation `epoch` for `session` without
    /// rewriting the full record (Auth Slice B.5). Called from
    /// `Registry::host`'s resume branch after bumping the in-memory
    /// epoch. A no-op (returning `Ok(())`) for an unknown session.
    async fn bump_host_epoch(&self, session: SessionId, epoch: u64) -> io::Result<()>;

    /// Add a peer to a session's member set.
    async fn add_member(&self, session: SessionId, peer: &PeerInfo) -> io::Result<()>;

    /// Remove a peer from a session's member set.
    async fn remove_member(&self, session: SessionId, peer: PeerId) -> io::Result<()>;

    /// Forget the session entirely. Used when the host leaves.
    ///
    /// **Cascade invariant:** when this returns, any attachments
    /// associated with `session` must also be gone. The on-disk
    /// implementation gets this for free from `remove_dir_all`; the
    /// in-memory implementation bundles attachments into the session
    /// entry so a single map remove sweeps both.
    ///
    /// Concurrency: the *store* does NOT serialize `delete` against
    /// concurrent [`Self::put_attachment`] calls for the same session
    /// — a put that races a delete may land an attachment whose
    /// session has just been removed. Callers (today: only
    /// [`super::super::session::Registry`]) MUST hold the per-session
    /// `Mutex<Session>` across both calls so cascade and put cannot
    /// interleave at the store boundary.
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
    ///
    /// Concurrency: see [`Self::delete`] — the store does not
    /// serialize put-vs-delete races. The Registry holds its
    /// per-session `Mutex<Session>` across this call to keep the
    /// cascade invariant.
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
    /// that care should sort client-side.
    ///
    /// Two skip categories with different observability:
    /// - **Skip-and-warn** on unparseable on-disk entries — corruption,
    ///   filename that doesn't decode, oversized payloads (mirrors
    ///   [`Self::load_all`]). Logged via `tracing::warn!`.
    /// - **Skip-on-vanish** when an entry disappears mid-iteration
    ///   because [`Self::delete`]'s cascade is racing this call. Silent
    ///   — these are expected concurrency outcomes, not corruption,
    ///   and warning every cascade-race would flood logs with noise.
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
