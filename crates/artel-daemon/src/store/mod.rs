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
pub(crate) use record::SessionRecord;

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
    async fn delete(&self, session: SessionId) -> io::Result<()>;

    /// Load every session this store knows about. Called once at
    /// daemon startup. May skip sessions whose data is unrecoverable,
    /// emitting a warning, rather than failing the whole daemon.
    async fn load_all(&self) -> io::Result<Vec<SessionRecord>>;
}

/// Convenience alias for the type Registry holds.
pub(crate) type DynStore = Arc<dyn SessionStore>;
