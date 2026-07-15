//! On-disk [`super::SessionStore`].
//!
//! Layout under the configured `sessions_dir`:
//!
//! ```text
//! sessions_dir/
//!   <session-uuid>/
//!     meta.json            — host, members, head
//!     log                  — length-prefixed postcard frames of SessionMessage
//!     tickets.json         — issued-ticket ledger (host sessions; absent ⇒ empty)
//!     workspace-ticket.bin — workspace ticket envelope (absent ⇒ none)
//! ```
//!
//! `meta.json` is small enough to overwrite atomically (write to
//! `meta.json.tmp`, fsync, rename, fsync the parent dir) on every
//! membership or head change. The `log` is append-only with `fsync`
//! after each frame. Every rename/create that a write depends on is
//! followed by a parent-directory fsync (see [`fsync_dir`]) so the
//! durability holds across a power loss, not just a process crash —
//! `fsync(file)` alone leaves the directory entry unflushed.
//!
//! Crash-recovery: `meta.head` is a cache of the durable log's tail,
//! not the source of truth. `append` fsyncs the log frame before the
//! separate `meta.head` write, so a crash in between can leave
//! `meta.head` behind the log; [`load_one`] reconciles `head` up to the
//! max seq actually present in the log. `meta.head` is also written
//! monotonically so an out-of-order Remote-mirror append can't lower it.
//!
//! Recovery: on `load_all`, missing or unparseable `meta.json` makes
//! the daemon skip that session with a warning. A partial trailing
//! frame in `log` is truncated (we never acked it to a client).

#![allow(clippy::redundant_pub_crate)]

use std::collections::HashSet;
use std::io::{self, ErrorKind, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use artel_protocol::{PeerId, PeerInfo, Seq, SessionId, SessionMessage, TicketEntry};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::{SessionKind, SessionRecord, SessionStore, StoredAttachment};

/// Maximum size of one log frame's payload, in bytes. Same cap as the
/// IPC transport — a frame too big to send over IPC isn't worth
/// persisting either.
const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Per-session metadata file name.
const META_FILE: &str = "meta.json";
/// Per-session log file name.
const LOG_FILE: &str = "log";
/// Per-session issued-ticket ledger file name (ticket-revocation
/// slice). Written only when the session mints tickets; absent ⇒
/// empty ledger on load.
const TICKETS_FILE: &str = "tickets.json";
/// Quarantine name for an unreadable [`TICKETS_FILE`] (corruption or
/// schema skew). The rename preserves the bytes for manual repair
/// while letting the session load with an empty — fail-closed —
/// ledger. See [`load_one`].
const TICKETS_QUARANTINE_FILE: &str = "tickets.json.corrupt";
/// Per-session workspace ticket envelope sidecar (revoked-lurker
/// fix). Raw postcard `WorkspaceTicketEnvelope` bytes, opaque to the
/// daemon, `0600` (capability-bearing — a read `DocTicket` rides
/// inside, same sensitivity as `tickets.json`). Absent ⇒ no envelope.
const WORKSPACE_TICKET_FILE: &str = "workspace-ticket.bin";
/// Upper bound accepted when loading [`WORKSPACE_TICKET_FILE`], and
/// enforced symmetrically on write (see [`write_workspace_ticket`]).
/// The shared [`artel_protocol::upgrade::WORKSPACE_TICKET_ENVELOPE_MAX`]
/// — every site the envelope flows through (producer encode, publish
/// ingress, store write, unicast delivery) uses the one constant, so
/// a put the store accepts is one the loader and the wire accept too.
/// A larger sidecar can only be corruption.
const WORKSPACE_TICKET_MAX: u64 = artel_protocol::upgrade::WORKSPACE_TICKET_ENVELOPE_MAX as u64;
/// Root-level subdirectory holding one small JSON file per session id,
/// tracking the lowest `host_epoch` a fresh [`FsLogStore::create`] of
/// that id may use (session-ID-reuse replay finding). Deliberately a
/// **sibling** of the per-session directories, not inside one: `delete`
/// only `remove_dir_all`s a session's own directory, so a floor here
/// survives a full close-and-recreate of the same session id — the
/// exact case the floor exists to protect against. Entries are never
/// removed; the directory grows by one tiny file per session id ever
/// hosted, which is the intended permanent record.
const EPOCH_FLOORS_DIR: &str = "epoch_floors";
/// Per-session subdirectory for opaque consumer attachments.
const ATTACHMENTS_DIR: &str = "attachments";
/// Suffix on attachment files; the prefix is the kind, hex-encoded.
const ATTACHMENT_FILE_SUFFIX: &str = ".bin";

/// Maximum length of an attachment `kind` in UTF-8 bytes.
///
/// The on-disk filename is `lowercase-hex(<kind>) + ".bin"`, i.e.
/// `2 * kind.len() + 4` bytes. `NAME_MAX` is 255 on Linux ext4 and on
/// macOS APFS (the latter has been observed to surface `ENAMETOOLONG`
/// well below that in practice — likely from path normalisation
/// overhead). 100 bytes of kind → 204 bytes of filename, safely under
/// every Unix filesystem we ship on. Anything longer is rejected at
/// the store with `InvalidData` rather than failing inside
/// `write_attachment` with the OS-leaked `ENAMETOOLONG`.
const MAX_KIND_LEN: usize = 100;

/// Per-process counter feeding [`unique_tmp_path`].
///
/// Combined with the process pid, this makes each attachment write's
/// tmp filename unique even if the same `(session, kind)` is being
/// written by two concurrent callers. Sharing one tmp path between
/// writers would let `create+truncate` interleave their bytes, so the
/// final file (after rename) could be a corrupted mixture.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Permission applied to the sessions root and per-session subdirs.
const DIR_MODE: u32 = 0o700;
/// Permission applied to log + meta files.
const FILE_MODE: u32 = 0o600;

#[derive(Debug)]
pub(crate) struct FsLogStore {
    root: PathBuf,
}

impl FsLogStore {
    /// Open (and ensure exists) the sessions directory at `root`.
    ///
    /// Also reaps any orphaned `*.tmp` files left by a previous daemon
    /// that crashed between [`write_attachment`]'s `create_new` and
    /// the rename. They were already invisible to `list_attachments`
    /// (the suffix-skip), but without a sweep they'd accumulate
    /// across crash cycles. Best-effort: a removal failure is logged
    /// and the open continues.
    pub(crate) fn open(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        ensure_dir(&root, DIR_MODE)?;
        sweep_tmp_files(&root);
        Ok(Self { root })
    }

    fn session_dir(&self, id: SessionId) -> PathBuf {
        self.root.join(id.to_string())
    }

    /// Path to `id`'s epoch-floor sidecar under [`EPOCH_FLOORS_DIR`] —
    /// a sibling of the session directory, not inside it, so it
    /// survives that directory's removal. See [`EPOCH_FLOORS_DIR`].
    fn epoch_floor_path(&self, id: SessionId) -> PathBuf {
        self.root.join(EPOCH_FLOORS_DIR).join(format!("{id}.json"))
    }
}

#[async_trait]
impl SessionStore for FsLogStore {
    async fn create(&self, record: &SessionRecord) -> io::Result<()> {
        let dir = self.session_dir(record.id);
        let epoch_floor_path = self.epoch_floor_path(record.id);
        let record = record.clone();
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            ensure_dir(&dir, DIR_MODE)?;
            // Raise the epoch floor to cover the epoch this create is
            // about to persist, BEFORE writing meta.json — so a crash
            // between the two leaves the floor at least as high as
            // what's about to land on disk, never behind it (session-
            // ID-reuse replay finding). `record.host_epoch` is 0 for a
            // genuinely fresh session and the caller-resolved floor
            // value for one that reuses a previously-deleted id.
            read_and_raise_epoch_floor(&epoch_floor_path, record.host_epoch + 1)?;
            // Idempotent (trait contract): a second create for the same
            // id — a host(Some(id)) resume whose in-memory entry was
            // lost, or recovery of a dir left half-built by a crash
            // between this log-touch and the meta write — must succeed,
            // not wedge at AlreadyExists. `create` always (re)writes a
            // fresh meta below, so adopting an existing log is safe; we
            // only need the file to exist for later appends to open it.
            // Registry never reuses a uuid for a *different* session, so
            // there's no "stale log from an unrelated session" case to
            // guard against. `create` instead of `create_new` so an
            // existing (empty, or already-populated-then-recovered) log
            // is tolerated.
            let log_path = dir.join(LOG_FILE);
            std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&log_path)?;
            chmod(&log_path, FILE_MODE)?;

            write_meta(&dir.join(META_FILE), &Meta::from_record(&record))?;
            // Fold the initial ticket ledger into create so the
            // host's first mint and the session record land together
            // (no window where the ticket exists but the ledger
            // doesn't). Empty ledgers write no file — absent ⇒ empty
            // on load, and Remote mirrors never mint.
            if !record.tickets.is_empty() {
                write_tickets(&dir.join(TICKETS_FILE), &record.tickets)?;
            }
            // Same create-folds-the-sidecar shape for the workspace
            // ticket envelope: absent ⇒ no file.
            if let Some(envelope) = &record.workspace_ticket {
                write_workspace_ticket(&dir.join(WORKSPACE_TICKET_FILE), envelope)?;
            }
            // We don't write the in-memory log here; create() is for a
            // fresh session and Registry::host() always passes an empty
            // log.
            for msg in &record.log {
                append_log(&log_path, msg)?;
            }
            Ok(())
        })
        .await
        .map_err(|e| join_to_io(&e))?
    }

    async fn append(&self, session: SessionId, message: &SessionMessage) -> io::Result<()> {
        let log_path = self.session_dir(session).join(LOG_FILE);
        let meta_path = self.session_dir(session).join(META_FILE);
        let message = message.clone();
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            append_log(&log_path, &message)?;
            // Bump head in meta. Read-modify-write: cheap because
            // meta.json is tiny.
            //
            // Monotonic, not an unconditional set: a Remote mirror
            // applies gossip frames in arbitrary lock order (one task
            // per inbound frame), so a lower seq can be appended after a
            // higher one. Lowering `head` here would persist a watermark
            // behind the durable log — mirroring the in-memory guard
            // (`if msg.seq > s.head`) that `apply_inbound_mirror_message`
            // already applies. The host (Local) path only ever appends
            // an increasing prospective seq, so for it `max` is a no-op.
            let mut meta = read_meta(&meta_path)?;
            meta.head = meta.head.max(message.seq);
            // `write_meta` → `write_bytes_atomic` fsyncs the session
            // directory after the rename (H2), so both the appended log
            // frame (fsynced inside `append_log`) and the meta rename are
            // durable across a power loss, not just a process crash.
            write_meta(&meta_path, &meta)?;
            Ok(())
        })
        .await
        .map_err(|e| join_to_io(&e))?
    }

    async fn bump_host_epoch(&self, session: SessionId, epoch: u64) -> io::Result<()> {
        let meta_path = self.session_dir(session).join(META_FILE);
        let epoch_floor_path = self.epoch_floor_path(session);
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            // Read-modify-write the tiny meta.json. A missing meta means
            // the session is unknown to this store — a no-op per the
            // trait contract.
            let mut meta = match read_meta(&meta_path) {
                Ok(m) => m,
                Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
                Err(err) => return Err(err),
            };
            meta.host_epoch = epoch;
            // Raise the floor first (session-ID-reuse replay finding): a
            // crash between the two writes must never leave the floor
            // behind an epoch this call is about to durably commit.
            read_and_raise_epoch_floor(&epoch_floor_path, epoch + 1)?;
            write_meta(&meta_path, &meta)?;
            Ok(())
        })
        .await
        .map_err(|e| join_to_io(&e))?
    }

    async fn epoch_floor(&self, session: SessionId) -> io::Result<u64> {
        let epoch_floor_path = self.epoch_floor_path(session);
        tokio::task::spawn_blocking(move || read_and_raise_epoch_floor(&epoch_floor_path, 0))
            .await
            .map_err(|e| join_to_io(&e))?
    }

    async fn put_tickets(&self, session: SessionId, tickets: &[TicketEntry]) -> io::Result<()> {
        let dir = self.session_dir(session);
        let tickets = tickets.to_vec();
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            // Unknown session must surface (trait contract): the
            // ledger gates admission, so a write that lands nowhere
            // is a correctness bug, not a no-op. The dir check is the
            // same existence proxy `create` uses.
            if !dir.is_dir() {
                return Err(io::Error::new(
                    ErrorKind::NotFound,
                    format!("no session dir at {}", dir.display()),
                ));
            }
            write_tickets(&dir.join(TICKETS_FILE), &tickets)
        })
        .await
        .map_err(|e| join_to_io(&e))?
    }

    async fn put_workspace_ticket(&self, session: SessionId, envelope: &[u8]) -> io::Result<()> {
        let dir = self.session_dir(session);
        let envelope = envelope.to_vec();
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            // Unknown session must surface, same contract as
            // put_tickets: the envelope is what a joiner's late
            // attach depends on; a write that lands nowhere is a
            // correctness bug, not a no-op.
            if !dir.is_dir() {
                return Err(io::Error::new(
                    ErrorKind::NotFound,
                    format!("no session dir at {}", dir.display()),
                ));
            }
            write_workspace_ticket(&dir.join(WORKSPACE_TICKET_FILE), &envelope)
        })
        .await
        .map_err(|e| join_to_io(&e))?
    }

    async fn add_member(&self, session: SessionId, peer: &PeerInfo) -> io::Result<()> {
        let meta_path = self.session_dir(session).join(META_FILE);
        let peer_id = peer.id;
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let mut meta = read_meta(&meta_path)?;
            meta.members.insert(peer_id);
            write_meta(&meta_path, &meta)?;
            Ok(())
        })
        .await
        .map_err(|e| join_to_io(&e))?
    }

    async fn remove_member(&self, session: SessionId, peer: PeerId) -> io::Result<()> {
        let meta_path = self.session_dir(session).join(META_FILE);
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let mut meta = read_meta(&meta_path)?;
            meta.members.remove(&peer);
            write_meta(&meta_path, &meta)?;
            Ok(())
        })
        .await
        .map_err(|e| join_to_io(&e))?
    }

    async fn delete(&self, session: SessionId) -> io::Result<()> {
        let dir = self.session_dir(session);
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err),
            }
        })
        .await
        .map_err(|e| join_to_io(&e))?
    }

    async fn load_all(&self) -> io::Result<Vec<SessionRecord>> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || -> io::Result<Vec<SessionRecord>> {
            let mut out = Vec::new();
            let entries = match std::fs::read_dir(&root) {
                Ok(it) => it,
                Err(err) if err.kind() == ErrorKind::NotFound => return Ok(out),
                Err(err) => return Err(err),
            };
            for entry in entries {
                let entry = entry?;
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                match load_one(&path) {
                    Ok(record) => out.push(record),
                    Err(err) => {
                        warn!(
                            dir = %path.display(),
                            error = %err,
                            "skipping session: load failed"
                        );
                    }
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| join_to_io(&e))?
    }

    async fn put_attachment(
        &self,
        session: SessionId,
        kind: &str,
        payload: &[u8],
    ) -> io::Result<bool> {
        let session_dir = self.session_dir(session);
        let kind = kind.to_owned();
        let payload = payload.to_vec();
        if payload.len() > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("attachment payload too large: {} bytes", payload.len()),
            ));
        }
        if kind.len() > MAX_KIND_LEN {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "attachment kind too long: {} bytes (max {MAX_KIND_LEN})",
                    kind.len(),
                ),
            ));
        }
        if kind.is_empty() {
            // Empty kind hex-encodes to a zero-length stem, producing
            // the dotfile `.bin`. That round-trips through
            // `decode_attachment_filename` as `Some("")` and is
            // unfilterable from any `Some(non_empty_kind)` query, so
            // a misconfigured caller could persist an entry that no
            // typed reader ever sees. Reject up-front; consumers must
            // namespace.
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "attachment kind must be non-empty",
            ));
        }
        tokio::task::spawn_blocking(move || -> io::Result<bool> {
            // Existence test mirrors load_all's: only directories that
            // already pass `load_one` count as "known sessions". A bare
            // `is_dir` check is enough here because Registry won't call
            // us with an id whose dir is half-built — `create` runs
            // first and it lays down both meta and log atomically.
            if !session_dir.is_dir() {
                return Ok(false);
            }
            let attachments_dir = session_dir.join(ATTACHMENTS_DIR);
            ensure_dir(&attachments_dir, DIR_MODE)?;
            let path = attachments_dir.join(attachment_filename(&kind));
            write_attachment(&path, &payload)?;
            Ok(true)
        })
        .await
        .map_err(|e| join_to_io(&e))?
    }

    async fn list_attachments(
        &self,
        kind_filter: Option<&str>,
    ) -> io::Result<Vec<StoredAttachment>> {
        let root = self.root.clone();
        let kind_filter = kind_filter.map(str::to_owned);
        tokio::task::spawn_blocking(move || -> io::Result<Vec<StoredAttachment>> {
            let mut out = Vec::new();
            let entries = match std::fs::read_dir(&root) {
                Ok(it) => it,
                Err(err) if err.kind() == ErrorKind::NotFound => return Ok(out),
                Err(err) => return Err(err),
            };
            for entry in entries {
                let entry = entry?;
                let session_path = entry.path();
                if !session_path.is_dir() {
                    continue;
                }
                let Some(id_str) = session_path.file_name().and_then(|s| s.to_str()) else {
                    continue;
                };
                let session_id: SessionId = match id_str.parse() {
                    Ok(id) => id,
                    Err(_) => continue, // unparseable session dir — load_all warns on it
                };
                let attachments_dir = session_path.join(ATTACHMENTS_DIR);
                let attachment_entries = match std::fs::read_dir(&attachments_dir) {
                    Ok(it) => it,
                    Err(err) if err.kind() == ErrorKind::NotFound => continue,
                    Err(err) => return Err(err),
                };
                for att in attachment_entries {
                    // A concurrent `delete` (cascade via remove_dir_all)
                    // can unlink entries while we iterate. The DirEntry
                    // result, the metadata stat, and the read can each
                    // return NotFound; treat that as "the entry vanished
                    // mid-list" and skip rather than failing the whole
                    // call. This is the skip-on-vanish path the trait doc
                    // calls out — silent on purpose; cascade-races are
                    // expected concurrency, not corruption.
                    let att = match att {
                        Ok(a) => a,
                        Err(err) if err.kind() == ErrorKind::NotFound => continue,
                        Err(err) => return Err(err),
                    };
                    let path = att.path();
                    // In-flight or crashed write_attachment: skip
                    // silently rather than warn. is_attachment_tmp
                    // matches only the unique_tmp_path shape, so an
                    // unrelated `.tmp` file (operator scratch, editor
                    // backup) falls through to the kind-decode warn
                    // below.
                    if path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(is_attachment_tmp)
                    {
                        continue;
                    }
                    let Some(kind) = decode_attachment_filename(&path) else {
                        warn!(
                            file = %path.display(),
                            "skipping attachment: filename is not lowercase-hex(<kind>) + .bin",
                        );
                        continue;
                    };
                    if let Some(filter) = &kind_filter
                        && filter != &kind
                    {
                        continue;
                    }
                    let metadata = match std::fs::metadata(&path) {
                        Ok(m) => m,
                        Err(err) if err.kind() == ErrorKind::NotFound => continue,
                        Err(err) => return Err(err),
                    };
                    if metadata.len() > MAX_FRAME_SIZE as u64 {
                        warn!(
                            file = %path.display(),
                            size = metadata.len(),
                            "skipping attachment: payload exceeds MAX_FRAME_SIZE",
                        );
                        continue;
                    }
                    let payload = match std::fs::read(&path) {
                        Ok(p) => p,
                        Err(err) if err.kind() == ErrorKind::NotFound => continue,
                        Err(err) => return Err(err),
                    };
                    out.push(StoredAttachment {
                        session: session_id,
                        kind,
                        payload,
                    });
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| join_to_io(&e))?
    }

    async fn delete_attachment(&self, session: SessionId, kind: &str) -> io::Result<()> {
        let path = self
            .session_dir(session)
            .join(ATTACHMENTS_DIR)
            .join(attachment_filename(kind));
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            match std::fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err),
            }
        })
        .await
        .map_err(|e| join_to_io(&e))?
    }
}

/// On-disk meta document. Kept distinct from `SessionRecord` so the
/// disk schema can evolve independently of the in-memory shape.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Meta {
    /// Schema version for forward-compat. Increment on incompatible
    /// changes.
    version: u32,
    host: PeerId,
    members: HashSet<PeerId>,
    head: Seq,
    /// Whether the daemon hosts this session or mirrors it. Added
    /// in 2c-2e; old meta files without this field deserialise to
    /// `SessionKind::Local`, which is correct retroactively (pre-
    /// 2c-2e there was no way for a remote mirror to reach disk).
    #[serde(default)]
    kind: SessionKind,
    /// Host incarnation epoch (Auth Slice B.5). See
    /// [`SessionRecord::host_epoch`]. Old meta without this field
    /// deserialises to 0, which is the correct fresh-session default.
    #[serde(default)]
    host_epoch: u64,
}

impl Meta {
    /// On-disk schema version for `meta.json`.
    ///
    /// Bumped to `2` on 2026-06-02 (Auth Slice B1) when
    /// `MESSAGE_FORMAT` went 1 → 2 and the on-disk log frames started
    /// embedding signatures. A v1 directory is unreadable by a v2
    /// daemon: pre-Slice-B logs have no signature byte run, so even
    /// unverified replay would mis-decode.
    ///
    /// Bumped to `3` on 2026-06-03 (Auth Slice B.5) when
    /// `MESSAGE_FORMAT` went 2 → 3: each log frame gained a `host_sig`
    /// byte run and the record gained `host_epoch`. A v2 directory is
    /// unreadable by a v3 daemon (pre-cutover frames lack the `host_sig`
    /// run); `load_one` rejects it and `load_all` skips-and-logs.
    const CURRENT_VERSION: u32 = 3;

    fn from_record(r: &SessionRecord) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            host: r.host,
            members: r.members.clone(),
            head: r.head,
            kind: r.kind,
            host_epoch: r.host_epoch,
        }
    }
}

fn load_one(dir: &Path) -> io::Result<SessionRecord> {
    let id_str = dir
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "non-utf8 session dir"))?;
    let id: SessionId = id_str
        .parse()
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, format!("session id parse: {e}")))?;
    let meta = read_meta(&dir.join(META_FILE))?;
    if meta.version != Meta::CURRENT_VERSION {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "unsupported meta version {} (expected {})",
                meta.version,
                Meta::CURRENT_VERSION
            ),
        ));
    }
    // Verify signatures on load only when this build signs on write.
    // A no-iroh daemon writes `SIGNATURE_UNSIGNED`, so verifying would
    // drop its own log; an iroh daemon signs every frame and must
    // reject tampered / sentinel ones. See `read_log`.
    let log = read_log(&dir.join(LOG_FILE), id, cfg!(feature = "iroh"))?;
    // Ticket ledger: absent file is the empty ledger (fresh dir, or a
    // Remote mirror that never mints). An *unreadable* file
    // (corruption or schema skew) must neither be silently treated as
    // empty in place — the next mint's rewrite would destroy the
    // evidence — nor fail the whole session load: a skipped session
    // drops out of the registry, and the only way back is a resume
    // that falls through to the create path, which succeeds (`create`
    // is idempotent) but rewrites meta from a fresh record — resetting
    // head and host epoch and orphaning the log's existing entries.
    // That is a far larger blast radius than the ledger itself.
    // Instead, quarantine the
    // file (rename preserves the bytes for manual repair), warn, and
    // load with an empty ledger: fail closed — every outstanding
    // ticket stops admitting until the host re-mints — while the
    // session's log and membership stay reachable. Only InvalidData
    // (parse/schema) is quarantined; transient I/O errors still fail
    // the load so a flaky disk doesn't trigger a spurious rename.
    let tickets_path = dir.join(TICKETS_FILE);
    let tickets = match read_tickets(&tickets_path) {
        Ok(tickets) => tickets,
        Err(err) if err.kind() == ErrorKind::InvalidData => {
            let quarantine = dir.join(TICKETS_QUARANTINE_FILE);
            std::fs::rename(&tickets_path, &quarantine)?;
            warn!(
                session = %id,
                error = %err,
                quarantine = %quarantine.display(),
                "unreadable ticket ledger quarantined; loading with an \
                 empty (fail-closed) ledger — outstanding tickets stop \
                 admitting until re-minted"
            );
            Vec::new()
        }
        Err(err) => return Err(err),
    };
    // Workspace ticket envelope: absent ⇒ None (fresh dir, session
    // without a workspace). A present-but-unreadable sidecar fails
    // the session load loudly (Meta posture, not the ledger's
    // quarantine): silently loading `None` would leave a joiner's
    // late attach hanging in `wait_for_ticket` forever with no
    // recovery path — the capability the workspace depends on would
    // have vanished without a trace.
    let workspace_ticket = read_workspace_ticket(&dir.join(WORKSPACE_TICKET_FILE))?;
    // Reconcile `head` against the durable log tail (H1). `append`
    // fsyncs the log frame BEFORE the separate `meta.head` write, so a
    // crash (or an `ENOSPC`/`EIO` on the meta write) between the two
    // leaves the log holding seq N while `meta.head` is still N-1.
    // Trusting `meta.head` verbatim would make the next host send
    // compute `prospective = head.next() = N` and write a SECOND frame
    // at an already-used seq. The durable log is the source of truth
    // for the head watermark; `meta.head` is a cache of it. Take the
    // max so a (signature-dropped) sparse log can't lower a legitimately
    // higher recorded head either.
    let log_tail = log.iter().map(|m| m.seq).max().unwrap_or(Seq::ZERO);
    let head = meta.head.max(log_tail);
    Ok(SessionRecord {
        id,
        host: meta.host,
        members: meta.members,
        head,
        log,
        kind: meta.kind,
        host_epoch: meta.host_epoch,
        tickets,
        workspace_ticket,
    })
}

fn read_meta(path: &Path) -> io::Result<Meta> {
    read_json(path, "meta")
}

/// Filename for `(kind)`: lowercase-hex(utf8(kind)) + `.bin`.
///
/// Hex avoids case collisions on macOS's default filesystem, slashes
/// in `kind`, and other OS-illegal characters. These files are never
/// user-facing — direct readers go through `list_attachments`.
fn attachment_filename(kind: &str) -> String {
    let bytes = kind.as_bytes();
    let mut s = String::with_capacity(bytes.len() * 2 + ATTACHMENT_FILE_SUFFIX.len());
    for b in bytes {
        // No-alloc per-byte hex.
        s.push(char::from_digit(u32::from(b >> 4), 16).expect("nibble"));
        s.push(char::from_digit(u32::from(b & 0xF), 16).expect("nibble"));
    }
    s.push_str(ATTACHMENT_FILE_SUFFIX);
    s
}

/// Inverse of [`attachment_filename`]. Returns `None` if `path` does
/// not have the expected `<lowercase-hex>.bin` shape: non-`.bin`
/// suffix, odd-length stem, non-`[0-9a-f]` characters, or stem
/// that decodes to non-utf8 bytes.
fn decode_attachment_filename(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    let stem = name.strip_suffix(ATTACHMENT_FILE_SUFFIX)?;
    if !stem.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(stem.len() / 2);
    let mut chars = stem.chars();
    while let (Some(hi), Some(lo)) = (chars.next(), chars.next()) {
        // Require lowercase: keeps one canonical on-disk shape so a
        // dir written by one daemon and read by another (or the same
        // daemon after a code change) can never disagree on file
        // identity.
        let hi = decode_lower_hex_nibble(hi)?;
        let lo = decode_lower_hex_nibble(lo)?;
        bytes.push((hi << 4) | lo);
    }
    String::from_utf8(bytes).ok()
}

const fn decode_lower_hex_nibble(c: char) -> Option<u8> {
    match c {
        '0'..='9' => Some(c as u8 - b'0'),
        'a'..='f' => Some(c as u8 - b'a' + 10),
        _ => None,
    }
}

/// Atomic write of an attachment payload.
///
/// Same fsync + rename + chmod-0o600 dance as [`write_meta`], but the
/// tmp filename is unique per writer (pid + monotonic counter) rather
/// than deterministic per `(session, kind)`. Two concurrent writers
/// with the same `kind` would otherwise share one tmp file, where
/// `create+truncate` and `write_all` can interleave and corrupt the
/// final post-rename payload.
fn write_attachment(path: &Path, payload: &[u8]) -> io::Result<()> {
    let tmp = unique_tmp_path(path);
    let result = (|| -> io::Result<()> {
        {
            let mut f = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&tmp)?;
            f.write_all(payload)?;
            f.sync_all()?;
        }
        chmod(&tmp, FILE_MODE)?;
        std::fs::rename(&tmp, path)?;
        // Durable rename: fsync the attachments directory (H2).
        if let Some(parent) = path.parent() {
            fsync_dir(parent)?;
        }
        Ok(())
    })();
    if result.is_err() {
        // Best-effort: the unique-tmp path was ours alone; if rename
        // never landed, leave nothing dangling for `list_attachments`
        // to warn about on every call.
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// Build a tmp path for `path` that is unique within this process.
///
/// Format: `<basename>.<pid>.<counter>.tmp` next to the destination.
/// Lives in the same directory so the rename is on-device (no
/// cross-fs copy) and inherits the parent's permissions.
fn unique_tmp_path(path: &Path) -> PathBuf {
    let pid = std::process::id();
    let counter = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut name = path
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .unwrap_or_default();
    name.push(format!(".{pid}.{counter}.tmp"));
    path.with_file_name(name)
}

/// Whether `name` matches the [`unique_tmp_path`] shape:
/// `<lowercase-hex>.bin.<digits>.<digits>.tmp`. Stricter than a
/// blanket `*.tmp` so the sweep + the in-list skip won't touch
/// anything but our own atomic-write tmps. An admin's stray `.tmp`
/// scratch file alongside attachments is preserved.
fn is_attachment_tmp(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".tmp") else {
        return false;
    };
    // <hex>.bin.<digits>.<digits>
    let Some((before_counter, counter)) = stem.rsplit_once('.') else {
        return false;
    };
    if counter.is_empty() || !counter.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    let Some((before_pid, pid)) = before_counter.rsplit_once('.') else {
        return false;
    };
    if pid.is_empty() || !pid.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    // before_pid is the original `<hex>.bin` filename. Validate the
    // hex stem so a non-attachment file like `notes.txt.42.7.tmp`
    // doesn't qualify just because it happens to end in two
    // numeric segments.
    let Some(hex_stem) = before_pid.strip_suffix(ATTACHMENT_FILE_SUFFIX) else {
        return false;
    };
    if hex_stem.is_empty() || !hex_stem.len().is_multiple_of(2) {
        return false;
    }
    hex_stem
        .bytes()
        .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
}

/// Crash-safe write of raw `bytes` to `path`: write to
/// `<path minus extension>.<tmp_ext>`, fsync, chmod [`FILE_MODE`],
/// rename onto `path`, then fsync the parent directory. The rename is
/// atomic, so a reader sees either the old file or the fully-written
/// new one — never a torn write — and the parent-dir fsync makes that
/// rename durable across a power loss (see [`fsync_dir`]).
///
/// The deterministic tmp name is safe only while writes to one `path`
/// never race: every caller holds the per-session `Mutex<Session>`
/// across the write. A writer outside that lock needs
/// [`unique_tmp_path`] instead (see [`write_attachment`]).
fn write_bytes_atomic(path: &Path, bytes: &[u8], tmp_ext: &str) -> io::Result<()> {
    let tmp = path.with_extension(tmp_ext);
    {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    chmod(&tmp, FILE_MODE)?;
    std::fs::rename(&tmp, path)?;
    // fsync the parent directory so the rename is durable across a
    // power loss (H2). `f.sync_all()` flushed the tmp file's contents,
    // but the directory entry the rename creates is separate metadata:
    // without this a crash right after the call returns can revert
    // `path` to its old contents (or leave it absent) even though we
    // returned `Ok`.
    if let Some(parent) = path.parent() {
        fsync_dir(parent)?;
    }
    Ok(())
}

/// Atomic JSON write: pretty-serialize then [`write_bytes_atomic`] to
/// `<path minus extension>.json.tmp`. `label` names the document in
/// error messages ("meta", "tickets").
fn write_json_atomic<T: Serialize>(path: &Path, value: &T, label: &str) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, format!("{label} json: {e}")))?;
    write_bytes_atomic(path, &bytes, "json.tmp")
}

/// Counterpart of [`write_json_atomic`]: read + parse, mapping parse
/// failures to `InvalidData` tagged with `label`. I/O errors (notably
/// `NotFound`) pass through untouched so callers can layer their own
/// absent-file policy.
fn read_json<T: serde::de::DeserializeOwned>(path: &Path, label: &str) -> io::Result<T> {
    let bytes = std::fs::read(path)?;
    serde_json::from_slice(&bytes)
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, format!("{label} json: {e}")))
}

/// On-disk envelope for the ticket ledger — same idiom as [`Meta`]:
/// a schema version of its own, kept distinct from the wire
/// [`TicketEntry`] shape so the two can evolve independently and a
/// future daemon's ledger reads as explicit schema skew, not as
/// corruption or (worse) an empty ledger.
#[derive(Debug, Serialize, Deserialize)]
struct TicketsFile {
    /// Schema version for forward-compat. Increment on incompatible
    /// changes.
    version: u32,
    entries: Vec<TicketEntry>,
}

impl TicketsFile {
    /// On-disk schema version for `tickets.json`. `1` is the
    /// versioned-envelope cutover (ticket-revocation slice shipped a
    /// bare `Vec<TicketEntry>` for a few days; no released build wrote
    /// that shape).
    const CURRENT_VERSION: u32 = 1;
}

/// Atomic full rewrite of the ticket ledger.
fn write_tickets(path: &Path, tickets: &[TicketEntry]) -> io::Result<()> {
    /// Write-side view of [`TicketsFile`] — borrows the entries so a
    /// ledger rewrite doesn't clone the whole Vec just to serialize.
    #[derive(Serialize)]
    struct TicketsFileRef<'a> {
        version: u32,
        entries: &'a [TicketEntry],
    }
    let doc = TicketsFileRef {
        version: TicketsFile::CURRENT_VERSION,
        entries: tickets,
    };
    write_json_atomic(path, &doc, "tickets")
}

/// Read the ticket ledger sidecar. Absent ⇒ empty (see `load_one` for
/// why corrupt ≠ absent).
fn read_tickets(path: &Path) -> io::Result<Vec<TicketEntry>> {
    let doc: TicketsFile = match read_json(path, "tickets") {
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        other => other?,
    };
    if doc.version != TicketsFile::CURRENT_VERSION {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "unsupported tickets version {} (expected {})",
                doc.version,
                TicketsFile::CURRENT_VERSION
            ),
        ));
    }
    Ok(doc.entries)
}

/// On-disk envelope for one session id's [`EPOCH_FLOORS_DIR`] sidecar.
/// Same versioned-envelope idiom as [`Meta`] / [`TicketsFile`].
#[derive(Debug, Serialize, Deserialize)]
struct EpochFloorFile {
    /// Schema version for forward-compat.
    version: u32,
    /// Lowest `host_epoch` a fresh `create` of this session id may use.
    floor: u64,
}

impl EpochFloorFile {
    const CURRENT_VERSION: u32 = 1;
}

/// Read-modify-write the epoch-floor sidecar for `session`: returns the
/// floor to use *now* (the value before this call), then durably raises
/// the stored floor to `raise_to` — or leaves it unchanged if the
/// existing floor is already `>= raise_to` (monotonic, so a
/// lower-numbered concurrent caller's write can't regress a
/// higher-numbered one's).
///
/// Absent file ⇒ floor `0`, matching the trait's "never seen ⇒ 0"
/// contract. Shared by [`FsLogStore::epoch_floor`] (reads only, via
/// `raise_to = 0` — a no-op raise) and every site that persists a fresh
/// `host_epoch` ([`FsLogStore::create`], [`FsLogStore::bump_host_epoch`]),
/// so the floor can never be observed to be *behind* an epoch this store
/// has already durably committed for `session`.
fn read_and_raise_epoch_floor(path: &Path, raise_to: u64) -> io::Result<u64> {
    let current = match read_json::<EpochFloorFile>(path, "epoch_floor") {
        Ok(doc) => {
            if doc.version != EpochFloorFile::CURRENT_VERSION {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "unsupported epoch_floor version {} (expected {})",
                        doc.version,
                        EpochFloorFile::CURRENT_VERSION
                    ),
                ));
            }
            doc.floor
        }
        Err(err) if err.kind() == ErrorKind::NotFound => 0,
        Err(err) => return Err(err),
    };
    if raise_to > current {
        if let Some(parent) = path.parent() {
            ensure_dir(parent, DIR_MODE)?;
        }
        write_json_atomic(
            path,
            &EpochFloorFile {
                version: EpochFloorFile::CURRENT_VERSION,
                floor: raise_to,
            },
            "epoch_floor",
        )?;
    }
    Ok(current)
}

fn write_meta(path: &Path, meta: &Meta) -> io::Result<()> {
    write_json_atomic(path, meta, "meta")
}

/// Atomic full rewrite of the workspace ticket envelope sidecar via
/// [`write_bytes_atomic`] (single writer — the registry holds the
/// per-session lock across the call, same as `write_meta` /
/// `write_tickets`), `0600` because the envelope is capability-bearing.
fn write_workspace_ticket(path: &Path, envelope: &[u8]) -> io::Result<()> {
    // Reject at write time what the loader would reject at read time
    // (and the wire would reject at delivery). Persisting an envelope
    // larger than this would brick the whole session at the next
    // restart: `read_workspace_ticket` returns `InvalidData`,
    // `load_one` fails, and `load_all` skips-and-warns the entire
    // session dir — log, members, and ledger included.
    if envelope.len() > artel_protocol::upgrade::WORKSPACE_TICKET_ENVELOPE_MAX {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "workspace ticket envelope too large: {} bytes (max {WORKSPACE_TICKET_MAX})",
                envelope.len(),
            ),
        ));
    }
    write_bytes_atomic(path, envelope, "bin.tmp")
}

/// Read the workspace ticket envelope sidecar. Absent ⇒ `None`.
/// Oversized (> [`WORKSPACE_TICKET_MAX`]) is `InvalidData` — the
/// wire never produces it, so it can only be corruption, and the
/// caller fails the session load loudly rather than dropping the
/// capability silently.
fn read_workspace_ticket(path: &Path) -> io::Result<Option<Vec<u8>>> {
    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    if meta.len() > WORKSPACE_TICKET_MAX {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "workspace ticket sidecar too large: {} bytes (max {WORKSPACE_TICKET_MAX})",
                meta.len(),
            ),
        ));
    }
    Ok(Some(std::fs::read(path)?))
}

/// Append a [`SessionMessage`] as `[u32 BE length][postcard]`, then
/// fsync.
fn append_log(path: &Path, message: &SessionMessage) -> io::Result<()> {
    let payload = postcard::to_allocvec(message)
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, format!("postcard: {e}")))?;
    if payload.len() > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("log frame too large: {} bytes", payload.len()),
        ));
    }
    let len = u32::try_from(payload.len()).expect("checked above");
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)?;
    f.write_all(&len.to_be_bytes())?;
    f.write_all(&payload)?;
    f.sync_all()?;
    Ok(())
}

/// Read every complete frame in `path`. Corruption is handled by
/// whether the *framing* is still intact:
///
/// - **Torn framing** (length prefix not fully present, or a length
///   prefix that says N bytes but fewer follow, or an announced length
///   over [`MAX_FRAME_SIZE`]) — the next frame boundary is unknowable,
///   so this is treated as a torn trailing write: log and truncate the
///   file to the last good byte.
/// - **Intact framing, bad payload** (we read the full `len` bytes but
///   postcard decode fails — at-rest bit-rot of one frame) — skip that
///   frame and keep reading (L5). The on-disk log is **not** truncated:
///   later durable frames survive, leaving a non-contiguous `seq` gap.
///
/// When `verify` is `true`, each frame's signature is checked against
/// `session_id` and frames that fail are dropped with a `warn` and
/// **not** appended — same skip-and-continue, non-truncating shape.
/// That is intentional — the receiver has no truth for the missing
/// seq, and a future `Replay { since: head }` can fill it. Truncating
/// on a bad mid-log frame would amputate every valid frame after a
/// single tampered/corrupt one.
///
/// `verify` mirrors whether the daemon *signs* on write: a no-iroh
/// build emits `SIGNATURE_UNSIGNED` (no wire surface), so it loads
/// with `verify == false` — otherwise it would drop its own log on
/// every restart. See [`FsLogStore`]'s caller, which passes
/// `cfg!(feature = "iroh")`.
fn read_log(path: &Path, session_id: SessionId, verify: bool) -> io::Result<Vec<SessionMessage>> {
    let mut f = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(f) => f,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };

    let mut out = Vec::new();
    let mut last_good = 0u64;
    loop {
        let mut len_bytes = [0u8; 4];
        match f.read_exact(&mut len_bytes) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                let pos = f.stream_position()?;
                if pos != last_good {
                    warn!(
                        file = %path.display(),
                        partial_bytes = pos - last_good,
                        "truncating partial trailing frame in log"
                    );
                    f.set_len(last_good)?;
                }
                break;
            }
            Err(err) => return Err(err),
        }

        let len = u32::from_be_bytes(len_bytes) as usize;
        if len > MAX_FRAME_SIZE {
            warn!(
                file = %path.display(),
                announced = len,
                "log frame announces too-large size; truncating here"
            );
            f.set_len(last_good)?;
            break;
        }

        let mut buf = vec![0u8; len];
        match f.read_exact(&mut buf) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                warn!(
                    file = %path.display(),
                    "truncating partial trailing frame in log (incomplete payload)"
                );
                f.set_len(last_good)?;
                break;
            }
            Err(err) => return Err(err),
        }

        match postcard::from_bytes::<SessionMessage>(&buf) {
            Ok(msg) => {
                // Verify the frame's signature before including it.
                // A bad signature (tampered payload, sentinel from a
                // pre-Slice-B daemon, or a host that lied) drops the
                // frame: skip-and-continue, leaving a seq hole. We
                // do NOT truncate — a bad frame in the middle of a
                // long log shouldn't sever every good frame after
                // it.
                //
                // `verify` is `false` only for a no-iroh build, whose
                // `Registry::author_local` emits `SIGNATURE_UNSIGNED`
                // (no wire surface to sign for). Verifying there would
                // drop every frame the daemon wrote, silently wiping
                // the whole log on restart; the no-iroh log has a
                // single local writer, so there's nothing to reject.
                if verify {
                    if let Err(err) =
                        artel_protocol::signing::verify_message(session_id, &msg, &msg.signature)
                    {
                        warn!(
                            file = %path.display(),
                            seq = ?msg.seq,
                            peer = %msg.peer.id,
                            ?err,
                            "dropping log frame: signature verify failed",
                        );
                    } else {
                        out.push(msg);
                    }
                } else {
                    out.push(msg);
                }
                last_good = f.stream_position()?;
            }
            Err(err) => {
                // Malformed payload but INTACT framing (we read the full
                // `len` bytes, so the cursor is at the next frame
                // boundary). This is at-rest corruption of one frame, not
                // a torn trailing write — skip it and keep reading, like
                // the signature-fail branch above (L5). Truncating here
                // would amputate every durable frame that physically
                // follows a single mid-log bit-flip. Advance `last_good`
                // past the skipped frame so a genuine torn tail later
                // still truncates to the right place.
                warn!(
                    file = %path.display(),
                    error = %err,
                    "skipping malformed log frame (intact framing); preserving later frames"
                );
                last_good = f.stream_position()?;
            }
        }
    }

    debug!(
        file = %path.display(),
        frames = out.len(),
        "log loaded"
    );
    Ok(out)
}

/// Walk every `<session>/attachments/` under `root` and unlink any
/// orphaned attachment-write tmps. Called once from
/// [`FsLogStore::open`] to reap stragglers left by a previous daemon
/// that crashed between `write_attachment`'s `create_new` and the
/// rename.
///
/// Filter is [`is_attachment_tmp`] (the [`unique_tmp_path`] shape) —
/// stricter than `*.tmp` so an admin's scratch file or an editor
/// backup that happens to live alongside attachments is left alone.
///
/// Best-effort throughout: a missing root, a failing `read_dir` on any
/// session, or a failing `remove_file` is logged and skipped. Only the
/// `<session>/attachments/` layer is searched; we do not touch
/// `meta.json.tmp` (the meta write is atomic at single-writer scope —
/// only the registry calls it, serialised by per-session locking) or
/// any unrecognised paths.
fn sweep_tmp_files(root: &Path) {
    let session_iter = match std::fs::read_dir(root) {
        Ok(it) => it,
        Err(err) if err.kind() == ErrorKind::NotFound => return,
        Err(err) => {
            warn!(root = %root.display(), error = %err, "tmp sweep: read_dir root failed");
            return;
        }
    };
    for session_entry in session_iter {
        let Ok(session_entry) = session_entry else {
            continue;
        };
        let attachments_dir = session_entry.path().join(ATTACHMENTS_DIR);
        let att_iter = match std::fs::read_dir(&attachments_dir) {
            Ok(it) => it,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                warn!(
                    dir = %attachments_dir.display(),
                    error = %err,
                    "tmp sweep: read_dir attachments failed",
                );
                continue;
            }
        };
        for att in att_iter {
            let Ok(att) = att else { continue };
            let path = att.path();
            let matches_tmp = path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(is_attachment_tmp);
            if !matches_tmp {
                continue;
            }
            match std::fs::remove_file(&path) {
                Ok(()) => debug!(file = %path.display(), "tmp sweep: removed orphaned tmp"),
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => warn!(
                    file = %path.display(),
                    error = %err,
                    "tmp sweep: remove_file failed",
                ),
            }
        }
    }
}

/// Make sure `dir` exists at `mode`. Creates the chain if needed; if
/// the directory already exists, the mode is left as-is.
fn ensure_dir(dir: &Path, mode: u32) -> io::Result<()> {
    match std::fs::metadata(dir) {
        Ok(meta) if meta.is_dir() => Ok(()),
        Ok(_) => Err(io::Error::new(
            ErrorKind::AlreadyExists,
            format!("{} exists but is not a directory", dir.display()),
        )),
        Err(err) if err.kind() == ErrorKind::NotFound => {
            std::fs::create_dir_all(dir)?;
            chmod(dir, mode)?;
            // fsync the parent so the new directory's entry is durable
            // across a power loss (H2) — otherwise a crash can lose the
            // whole session dir even after a write into it returned Ok.
            // `create_dir_all` may have created intermediates too, but
            // the sessions root (the only multi-level case here) is
            // created once at daemon open; the parent of the leaf is the
            // entry a subsequent write depends on.
            if let Some(parent) = dir.parent() {
                fsync_dir(parent)?;
            }
            Ok(())
        }
        Err(err) => Err(err),
    }
}

/// Set Unix permissions; no-op on non-Unix (the `cfg(unix)` guard at
/// the daemon level prevents it but we still gate this for safety).
fn chmod(path: &Path, mode: u32) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)?.permissions();
    perms.set_mode(mode);
    std::fs::set_permissions(path, perms)
}

/// fsync the *directory* `dir` so a preceding `rename`/`create_new`
/// into it is durable across a crash (H2).
///
/// `fsync(file)` flushes a file's contents but NOT the directory entry
/// that names it: on ext4/xfs a crash right after `rename` returning
/// `Ok` can revert the destination to its old contents or lose a
/// freshly-created file even though the data was flushed. POSIX makes
/// the rename durable only once the *containing directory* is fsynced.
/// Open the dir read-only and `sync_all()` it.
fn fsync_dir(dir: &Path) -> io::Result<()> {
    // A directory must be opened read-only (O_RDONLY); opening it
    // writable fails with EISDIR. `File::open` is read-only, which is
    // exactly what fsync-on-a-dir needs.
    let result = std::fs::File::open(dir)?.sync_all();
    #[cfg(test)]
    if result.is_ok() {
        // Test-only call counter: lets the suite assert that every
        // durable write path actually fsyncs its parent directory
        // (the H2 regression guard). The durability guarantee itself
        // isn't observable in userspace — a SIGKILL leaves the page
        // cache intact and the OS flushes anyway; only a power loss
        // loses an un-fsynced rename — so this counts the call rather
        // than proving the flush.
        FSYNC_DIR_CALLS.fetch_add(1, Ordering::Relaxed);
    }
    result
}

/// Test-only counter of successful [`fsync_dir`] calls. nextest runs
/// each test in its own process, so this is effectively per-test.
#[cfg(test)]
static FSYNC_DIR_CALLS: AtomicU64 = AtomicU64::new(0);

fn join_to_io(err: &tokio::task::JoinError) -> io::Error {
    io::Error::other(format!("blocking task: {err}"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::io::Write as _;

    use artel_protocol::{MessageKind, PeerInfo};
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

    /// Deterministic ed25519 signing key derived from `seed`. The
    /// public key (i.e. the `PeerId` we advertise) is whatever
    /// dalek's `SigningKey::from_bytes` produces; tests that need
    /// to know the resulting `PeerId` go through [`peer_for_seed`].
    fn signing_key_for_seed(seed: u8) -> artel_protocol::signing::SigningKey {
        artel_protocol::signing::SigningKey::from_bytes(&[seed; 32])
    }

    fn peer_for_seed(seed: u8, name: &str) -> PeerInfo {
        let signing = signing_key_for_seed(seed);
        let pk = signing.verifying_key();
        PeerInfo::new(PeerId::from_bytes(pk.to_bytes()), name)
    }

    fn record(id_byte: u8) -> SessionRecord {
        let host = peer_for_seed(id_byte, "host").id;
        SessionRecord {
            id: SessionId::from_bytes([id_byte; 16]),
            host,
            members: HashSet::from([host]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Local,
            host_epoch: 0,
            tickets: Vec::new(),
            workspace_ticket: None,
        }
    }

    /// Build a `SessionMessage` with a real signature against
    /// `session_id`. Authoring peer is `peer_for_seed(1, "alice")`
    /// — that pubkey is the body's `peer.id`, and the matching
    /// `signing_key_for_seed(1)` produces the signature.
    fn message_for(session_id: SessionId, seq: u64) -> SessionMessage {
        let signing = signing_key_for_seed(1);
        let peer = peer_for_seed(1, "alice");
        let kind = MessageKind::Chat;
        let action = "x";
        let payload = vec![0xab; 8];
        let timestamp_ms = seq;
        let signature = artel_protocol::signing::sign_body(
            &signing,
            session_id,
            artel_protocol::message::MESSAGE_FORMAT,
            timestamp_ms,
            &peer,
            kind,
            action,
            &payload,
        );
        SessionMessage::new(
            Seq::new(seq),
            timestamp_ms,
            peer,
            kind,
            action,
            payload,
            signature,
            artel_protocol::message::SIGNATURE_UNSIGNED,
        )
    }

    /// Convenience for the legacy `message(seq)` test fixtures that
    /// didn't take a session id; uses session id `[id_byte=1; 16]`
    /// so it pairs with `record(1)` by default.
    fn message(seq: u64) -> SessionMessage {
        message_for(SessionId::from_bytes([1; 16]), seq)
    }

    #[tokio::test]
    async fn create_then_load_round_trips_empty_log() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();

        let store2 = FsLogStore::open(dir.path()).unwrap();
        let loaded = store2.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0], record(1));
    }

    #[tokio::test]
    async fn append_persists_messages_and_advances_head() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();

        store.append(record(1).id, &message(1)).await.unwrap();
        store.append(record(1).id, &message(2)).await.unwrap();

        let store2 = FsLogStore::open(dir.path()).unwrap();
        let loaded = store2.load_all().await.unwrap();
        assert_eq!(loaded[0].head, Seq::new(2));
        assert_eq!(loaded[0].log, vec![message(1), message(2)]);
    }

    // ---- H1: head/log durability reconciliation ----

    /// Simulate the crash window inside `append`: the log frame fsync
    /// landed (`append_log`) but the daemon died before the separate
    /// `meta.head` write. The durable log holds seq N while `meta.head`
    /// is still N-1. `load_one`/`load_all` MUST recover `head` from the
    /// log tail, not trust the stale meta verbatim — otherwise the next
    /// host send re-issues seq N and writes a second, distinct frame at
    /// an already-used seq.
    #[tokio::test]
    async fn load_reconciles_head_to_log_tail_when_meta_head_is_stale() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        // First append goes through the normal (atomic-enough) path.
        store.append(record(1).id, &message(1)).await.unwrap();

        // Now simulate the partial append: write the seq-2 frame to the
        // log durably, but DO NOT bump meta.head (crash before the meta
        // write). meta.head stays at 1.
        let session_dir = dir.path().join(record(1).id.to_string());
        append_log(&session_dir.join(LOG_FILE), &message(2)).unwrap();
        let meta = read_meta(&session_dir.join(META_FILE)).unwrap();
        assert_eq!(meta.head, Seq::new(1), "precondition: meta.head is stale");

        let store2 = FsLogStore::open(dir.path()).unwrap();
        let loaded = store2.load_all().await.unwrap();
        assert_eq!(
            loaded[0].log,
            vec![message(1), message(2)],
            "the durable seq-2 frame must load",
        );
        assert_eq!(
            loaded[0].head,
            Seq::new(2),
            "head must be reconciled up to the log tail, not left at the stale meta.head",
        );
    }

    /// `append` must keep `meta.head` monotonic. A Remote mirror applies
    /// gossip frames in arbitrary lock order (one task per inbound
    /// frame), so a lower seq can be appended after a higher one. The
    /// persisted head must not regress below an already-recorded seq
    /// (this is the M1 sub-bug; the in-memory path already guards with
    /// `if msg.seq > s.head`, the store does not).
    #[tokio::test]
    async fn append_does_not_regress_persisted_head_on_out_of_order_seq() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();

        // Higher seq lands first, then a lower seq (out-of-order mirror
        // delivery).
        store.append(record(1).id, &message(5)).await.unwrap();
        store.append(record(1).id, &message(3)).await.unwrap();

        let session_dir = dir.path().join(record(1).id.to_string());
        let meta = read_meta(&session_dir.join(META_FILE)).unwrap();
        assert_eq!(
            meta.head,
            Seq::new(5),
            "persisted head must stay at the max seq, not regress to the last-appended one",
        );
    }

    // ---- H2: parent-directory fsync after rename ----
    //
    // NOTE on coverage: true rename durability is a block-layer
    // property. `fsync(dir)` has no effect observable through the
    // filesystem API without power-loss injection — and a SIGKILL →
    // restart test (the shape of our `_n0` crash tests) CANNOT catch
    // this bug, because SIGKILL leaves the kernel page cache intact and
    // the OS flushes the dirty pages anyway; only a real power loss /
    // kernel panic loses an un-fsynced rename. So no userspace test at
    // any level proves the durability guarantee. What we CAN and do
    // assert behaviorally is that every durable write path actually
    // issues the directory fsync (via the test-only FSYNC_DIR_CALLS
    // counter) — that is the regression we care about: a future edit
    // dropping or missing a call site. nextest runs each test in its
    // own process, so the counter is per-test.

    fn fsync_dir_calls() -> u64 {
        FSYNC_DIR_CALLS.load(Ordering::Relaxed)
    }

    /// `fsync_dir` opens the directory read-only and syncs it. A
    /// writable open of a directory fails with EISDIR, which would
    /// poison every write path — this pins the helper's behavior.
    #[test]
    fn fsync_dir_syncs_a_real_directory_and_errors_on_missing() {
        let dir = tempdir().unwrap();
        let before = fsync_dir_calls();
        fsync_dir(dir.path()).expect("fsync of an existing dir should succeed");
        assert_eq!(fsync_dir_calls(), before + 1);

        let missing = dir.path().join("does-not-exist");
        assert!(
            fsync_dir(&missing).is_err(),
            "fsync of a missing directory should error, not silently pass",
        );
    }

    /// `append` must fsync the log's parent directory so the appended
    /// frame's bytes (already fsynced via `append_log`) AND the meta
    /// rewrite are durable across a power loss, not just a process
    /// crash. Red until the call sites are woven in.
    #[tokio::test]
    async fn append_fsyncs_the_session_directory() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();

        let before = fsync_dir_calls();
        store.append(record(1).id, &message(1)).await.unwrap();
        assert!(
            fsync_dir_calls() > before,
            "append must fsync the session directory after writing the log frame + meta",
        );
    }

    /// Every atomic full-rewrite (`write_meta` / `write_tickets` /
    /// `write_workspace_ticket`, all via `write_bytes_atomic`) must
    /// fsync the destination's parent directory after the rename, or
    /// the rename can revert on power loss. Red until woven in.
    #[tokio::test]
    async fn put_tickets_fsyncs_the_parent_directory() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();

        let before = fsync_dir_calls();
        store
            .put_tickets(
                record(1).id,
                &[entry(7, artel_protocol::TicketStatus::Active)],
            )
            .await
            .unwrap();
        assert!(
            fsync_dir_calls() > before,
            "put_tickets (write_bytes_atomic + rename) must fsync the parent directory",
        );
    }

    /// `create` lays down the new session directory and its files; the
    /// directory entry for the dir itself (in the sessions root) and
    /// the files within it must be durable. Red until woven in.
    #[tokio::test]
    async fn create_fsyncs_directories() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();

        let before = fsync_dir_calls();
        store.create(&record(1)).await.unwrap();
        assert!(
            fsync_dir_calls() > before,
            "create must fsync the new session dir (and the sessions root) after laying it down",
        );
    }

    /// Regression guard: weaving the parent-dir fsync in must not break
    /// the write path (a dir handle opened with the wrong flags fails
    /// with EISDIR). The files must still round-trip and no `.tmp` may
    /// survive a successful write.
    #[tokio::test]
    async fn atomic_writes_round_trip_with_parent_dir_fsync() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store.append(record(1).id, &message(1)).await.unwrap();
        store
            .put_tickets(
                record(1).id,
                &[entry(7, artel_protocol::TicketStatus::Active)],
            )
            .await
            .unwrap();

        let store2 = FsLogStore::open(dir.path()).unwrap();
        let loaded = store2.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].log, vec![message(1)]);
        assert_eq!(loaded[0].tickets.len(), 1);
        // No stray tmp files survive a successful write.
        let session_dir = dir.path().join(record(1).id.to_string());
        for ent in std::fs::read_dir(&session_dir).unwrap() {
            let name = ent.unwrap().file_name();
            let name = name.to_string_lossy();
            assert!(
                !name.ends_with(".tmp"),
                "no .tmp file should survive a successful atomic write, found {name}",
            );
        }
    }

    #[tokio::test]
    async fn delete_removes_session_dir() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        let session_dir = dir.path().join(record(1).id.to_string());
        assert!(session_dir.exists());

        store.delete(record(1).id).await.unwrap();
        assert!(!session_dir.exists());
    }

    // ---- ticket ledger (revocation slice) ----

    fn entry(id_byte: u8, status: artel_protocol::TicketStatus) -> TicketEntry {
        TicketEntry {
            ticket_id: artel_protocol::TicketId::from_bytes([id_byte; 16]),
            granted_cap: artel_protocol::Capability::ReadWrite,
            expiry_ms: 0,
            issued_at_ms: 1_700_000_000_000,
            status,
            used_by: Vec::new(),
        }
    }

    #[tokio::test]
    async fn put_tickets_then_load_round_trips() {
        use artel_protocol::TicketStatus;
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();

        let ledger = vec![
            entry(1, TicketStatus::Active),
            entry(2, TicketStatus::Revoked),
        ];
        store.put_tickets(record(1).id, &ledger).await.unwrap();

        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].tickets, ledger);
    }

    #[tokio::test]
    async fn put_tickets_rewrite_replaces_previous_ledger() {
        use artel_protocol::TicketStatus;
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();

        store
            .put_tickets(record(1).id, &[entry(1, TicketStatus::Active)])
            .await
            .unwrap();
        // Same entry flipped to Revoked + a second mint: full rewrite.
        let after = vec![
            entry(1, TicketStatus::Revoked),
            entry(2, TicketStatus::Active),
        ];
        store.put_tickets(record(1).id, &after).await.unwrap();

        assert_eq!(store.load_all().await.unwrap()[0].tickets, after);
    }

    #[tokio::test]
    async fn put_tickets_for_unknown_session_errors_not_found() {
        use artel_protocol::TicketStatus;
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        let err = store
            .put_tickets(
                SessionId::from_bytes([9; 16]),
                &[entry(1, TicketStatus::Active)],
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn absent_tickets_file_loads_as_empty_ledger() {
        // Pre-slice session dir: meta + log but no tickets.json.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        assert!(
            !dir.path()
                .join(record(1).id.to_string())
                .join(TICKETS_FILE)
                .exists()
        );

        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(loaded[0].tickets.is_empty());
    }

    #[tokio::test]
    async fn tickets_file_is_version_enveloped() {
        use artel_protocol::TicketStatus;
        // The ledger sidecar follows the Meta idiom: a versioned
        // envelope, not a bare serialization of the wire TicketEntry
        // type — so disk-schema skew is distinguishable from
        // corruption and the wire shape can evolve independently.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store
            .put_tickets(record(1).id, &[entry(1, TicketStatus::Active)])
            .await
            .unwrap();

        let raw =
            std::fs::read(dir.path().join(record(1).id.to_string()).join(TICKETS_FILE)).unwrap();
        let doc: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(
            doc.get("version").and_then(serde_json::Value::as_u64),
            Some(u64::from(TicketsFile::CURRENT_VERSION)),
            "tickets.json must carry a schema version",
        );
        assert!(
            doc.get("entries").is_some_and(serde_json::Value::is_array),
            "ledger entries must live under an envelope key",
        );
    }

    #[tokio::test]
    async fn tickets_file_with_unsupported_version_quarantines_and_fails_closed() {
        // A future-daemon ledger (or a corrupted version field) is
        // schema skew: quarantine like corruption — the bytes are
        // preserved for a re-upgrade, old tickets stop admitting,
        // and the session's primary data stays reachable.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        let session_dir = dir.path().join(record(1).id.to_string());
        std::fs::write(
            session_dir.join(TICKETS_FILE),
            br#"{"version": 99, "entries": []}"#,
        )
        .unwrap();

        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1, "schema skew must not drop the session");
        assert!(loaded[0].tickets.is_empty(), "skewed ledger fails closed");
        assert!(session_dir.join(TICKETS_QUARANTINE_FILE).exists());
        assert!(!session_dir.join(TICKETS_FILE).exists());
    }

    #[tokio::test]
    async fn corrupt_tickets_file_quarantines_and_fails_closed() {
        use artel_protocol::TicketStatus;
        // Issued-only admission makes the ledger load-bearing, but it
        // is a *sidecar*: corruption must not take the session's
        // intact log and membership down with it (a skipped session
        // drops out of the registry; a resume falls through to the
        // idempotent create path, which rewrites meta from a fresh
        // record and loses head + host epoch). Instead: quarantine
        // the corrupt file so the
        // bytes survive for manual repair, load the session with an
        // EMPTY ledger — fail closed, every outstanding ticket stops
        // admitting — and warn. The next mint writes a fresh ledger.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store
            .put_tickets(record(1).id, &[entry(1, TicketStatus::Active)])
            .await
            .unwrap();
        let session_dir = dir.path().join(record(1).id.to_string());
        std::fs::write(session_dir.join(TICKETS_FILE), b"{not json").unwrap();

        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1, "corrupt ledger must not drop the session");
        assert_eq!(loaded[0], {
            let mut expected = record(1);
            expected.tickets = Vec::new();
            expected
        });
        // The corrupt bytes are preserved, not destroyed.
        assert_eq!(
            std::fs::read(session_dir.join(TICKETS_QUARANTINE_FILE)).unwrap(),
            b"{not json",
        );
        assert!(!session_dir.join(TICKETS_FILE).exists());
    }

    #[tokio::test]
    async fn create_persists_initial_ledger_with_record() {
        use artel_protocol::TicketStatus;
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        let mut r = record(1);
        r.tickets = vec![entry(7, TicketStatus::Active)];
        store.create(&r).await.unwrap();

        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded[0].tickets, r.tickets);
    }

    // ---- H5: create idempotency / partial-failure recovery ----

    #[tokio::test]
    async fn create_is_idempotent_over_existing_record() {
        // The trait documents create as idempotent ("writing over an
        // existing record is fine"). A second create for the same id
        // must succeed, not fail with AlreadyExists — otherwise a
        // host(Some(id)) resume whose in-memory entry was lost wedges
        // forever (the create path is the only fallback).
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store
            .create(&record(1))
            .await
            .expect("create over an existing record must be idempotent");

        // Still exactly one well-formed session afterwards.
        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0], record(1));
    }

    #[tokio::test]
    async fn create_recovers_a_half_built_dir() {
        // Simulate a crash mid-create: the log file was touched but the
        // meta.json write never landed. The dir is unloadable
        // (read_meta NotFound → load_all skips it) AND, before the fix,
        // uncreatable (log_path.exists() → AlreadyExists), so it's
        // wedged through every API. create must recover it: lay down the
        // missing meta and succeed.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        let session_dir = dir.path().join(record(1).id.to_string());
        ensure_dir(&session_dir, DIR_MODE).unwrap();
        // A lone, empty log — the half-built state.
        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(session_dir.join(LOG_FILE))
            .unwrap();
        // Sanity: load skips the meta-less dir.
        assert!(store.load_all().await.unwrap().is_empty());

        store
            .create(&record(1))
            .await
            .expect("create must recover a half-built (meta-less) session dir");
        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0], record(1));
    }

    #[tokio::test]
    async fn delete_cascades_tickets_file_with_session_dir() {
        use artel_protocol::TicketStatus;
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store
            .put_tickets(record(1).id, &[entry(1, TicketStatus::Active)])
            .await
            .unwrap();
        let tickets_path = dir.path().join(record(1).id.to_string()).join(TICKETS_FILE);
        assert!(tickets_path.exists());

        store.delete(record(1).id).await.unwrap();
        assert!(!tickets_path.exists());
    }

    #[tokio::test]
    async fn tickets_file_mode_is_0600() {
        use artel_protocol::TicketStatus;
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store
            .put_tickets(record(1).id, &[entry(1, TicketStatus::Active)])
            .await
            .unwrap();
        let tickets_path = dir.path().join(record(1).id.to_string()).join(TICKETS_FILE);
        let mode = std::os::unix::fs::MetadataExt::mode(&std::fs::metadata(&tickets_path).unwrap());
        assert_eq!(mode & 0o777, FILE_MODE);
    }

    // ---- workspace ticket envelope sidecar (revoked-lurker fix) ----

    #[tokio::test]
    async fn put_workspace_ticket_then_load_round_trips() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        let envelope = vec![0xab; 256];
        store
            .put_workspace_ticket(record(1).id, &envelope)
            .await
            .unwrap();
        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert_eq!(loaded[0].workspace_ticket, Some(envelope));
    }

    #[tokio::test]
    async fn absent_workspace_ticket_file_loads_as_none() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded[0].workspace_ticket, None);
    }

    #[tokio::test]
    async fn put_workspace_ticket_rewrite_replaces_previous() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store
            .put_workspace_ticket(record(1).id, &[1, 2, 3])
            .await
            .unwrap();
        store
            .put_workspace_ticket(record(1).id, &[9, 9])
            .await
            .unwrap();
        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded[0].workspace_ticket, Some(vec![9, 9]));
    }

    #[tokio::test]
    async fn put_workspace_ticket_for_unknown_session_errors_not_found() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        let err = store
            .put_workspace_ticket(SessionId::from_bytes([9; 16]), &[1])
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn workspace_ticket_round_trips_through_create() {
        // create() folds a populated slot into the fresh session dir
        // (host-restart path persists via put_, but a future caller
        // creating with the field set must not silently drop it).
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        let mut r = record(1);
        r.workspace_ticket = Some(vec![0xcd; 64]);
        store.create(&r).await.unwrap();
        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded[0].workspace_ticket, r.workspace_ticket);
    }

    #[tokio::test]
    async fn delete_cascades_workspace_ticket_file_with_session_dir() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store
            .put_workspace_ticket(record(1).id, &[1, 2, 3])
            .await
            .unwrap();
        let sidecar = dir
            .path()
            .join(record(1).id.to_string())
            .join(WORKSPACE_TICKET_FILE);
        assert!(sidecar.exists());

        store.delete(record(1).id).await.unwrap();
        assert!(!sidecar.exists());
    }

    #[tokio::test]
    async fn workspace_ticket_file_mode_is_0600() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store
            .put_workspace_ticket(record(1).id, &[0xee; 32])
            .await
            .unwrap();
        let sidecar = dir
            .path()
            .join(record(1).id.to_string())
            .join(WORKSPACE_TICKET_FILE);
        let mode = std::os::unix::fs::MetadataExt::mode(&std::fs::metadata(&sidecar).unwrap());
        assert_eq!(mode & 0o777, FILE_MODE);
    }

    #[tokio::test]
    async fn put_workspace_ticket_rejects_over_cap_envelope() {
        // Write-side counterpart of the load cap: an envelope the
        // loader would refuse must be rejected at put time, not
        // persisted and discovered as "corruption" at the next
        // restart (which skips the whole session).
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        let oversized = vec![0u8; artel_protocol::upgrade::WORKSPACE_TICKET_ENVELOPE_MAX + 1];
        let err = store
            .put_workspace_ticket(record(1).id, &oversized)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        // A rejected put leaves no partial state behind.
        assert!(
            !dir.path()
                .join(record(1).id.to_string())
                .join(WORKSPACE_TICKET_FILE)
                .exists(),
        );
    }

    #[tokio::test]
    async fn accepted_workspace_ticket_put_never_bricks_the_session() {
        // Cap-mismatch repro: the publish ingress used to accept any
        // size (IPC frames go up to 16 MiB) while the loader rejects
        // > 64 KiB and load_all then skips the ENTIRE session — log,
        // members, ledger all gone at the next daemon restart. The
        // invariant: a put the store accepts must round-trip; a put
        // it could not load back must be rejected at write time.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        let oversized = vec![0u8; 64 * 1024 + 1];
        let put = store.put_workspace_ticket(record(1).id, &oversized).await;
        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert_eq!(
            loaded.len(),
            1,
            "session bricked: an accepted put failed the next load",
        );
        if put.is_ok() {
            assert_eq!(loaded[0].workspace_ticket, Some(oversized));
        }
    }

    #[tokio::test]
    async fn at_cap_workspace_ticket_round_trips_through_restart() {
        // The largest envelope the write path accepts must survive a
        // restart — pins write cap <= load cap.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        let max = vec![0xab; artel_protocol::upgrade::WORKSPACE_TICKET_ENVELOPE_MAX];
        store
            .put_workspace_ticket(record(1).id, &max)
            .await
            .unwrap();
        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert_eq!(loaded.len(), 1, "session must survive restart");
        assert_eq!(loaded[0].workspace_ticket, Some(max));
    }

    #[tokio::test]
    async fn oversized_workspace_ticket_sidecar_fails_session_load_loudly() {
        // An over-cap sidecar can only be corruption (the wire caps
        // delivery at 64 KiB). Posture matches Meta, not the ledger's
        // quarantine: silently loading None would strand a joiner's
        // late attach with no recovery, so the whole session load
        // fails and load_all skips-and-warns.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        let session_dir = dir.path().join(record(1).id.to_string());
        let oversized = vec![0u8; usize::try_from(WORKSPACE_TICKET_MAX).unwrap() + 1];
        std::fs::write(session_dir.join(WORKSPACE_TICKET_FILE), &oversized).unwrap();

        let loaded = store.load_all().await.unwrap();
        assert!(
            loaded.is_empty(),
            "session with corrupt envelope sidecar must be skipped loudly",
        );
    }

    #[tokio::test]
    async fn add_then_remove_member_persists() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        let bob = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
        store.add_member(record(1).id, &bob).await.unwrap();

        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert!(loaded[0].members.contains(&bob.id));

        store.remove_member(record(1).id, bob.id).await.unwrap();
        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert!(!loaded[0].members.contains(&bob.id));
    }

    #[tokio::test]
    async fn multiple_sessions_load_independently() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store.create(&record(2)).await.unwrap();
        store.append(record(1).id, &message(1)).await.unwrap();

        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert_eq!(loaded.len(), 2);
    }

    #[tokio::test]
    async fn partial_trailing_frame_is_truncated() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store.append(record(1).id, &message(1)).await.unwrap();

        // Append one truncated frame: just the length prefix saying
        // 100 bytes, then 5 bytes.
        let log_path = dir.path().join(record(1).id.to_string()).join(LOG_FILE);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap();
        f.write_all(&100u32.to_be_bytes()).unwrap();
        f.write_all(&[0u8; 5]).unwrap();
        f.sync_all().unwrap();

        // Reopen & load. The complete frame stays, the partial one is
        // truncated, no error.
        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert_eq!(loaded[0].log, vec![message(1)]);

        // The truncation should have shrunk the file; a second open
        // round-trips identically.
        let loaded_again = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert_eq!(loaded[0].log, loaded_again[0].log);
    }

    #[tokio::test]
    async fn read_log_drops_tampered_frame_and_keeps_surrounding() {
        // Three valid frames; corrupt the middle frame's payload
        // byte so postcard still decodes (we keep the
        // length+postcard prefix intact and only flip a payload
        // byte). Verify rejects, the surrounding two pass.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        for seq in 1..=3u64 {
            store.append(record(1).id, &message(seq)).await.unwrap();
        }

        // Tamper byte-by-byte on disk. We need the postcard prefix
        // to stay structurally valid, so we'll flip a single byte
        // in the middle of the second frame's payload. Easiest
        // approach: read the whole log, decode each frame, find
        // the second frame's start, flip one byte inside its
        // payload region, and write the buffer back.
        let log_path = dir.path().join(record(1).id.to_string()).join(LOG_FILE);
        let mut bytes = std::fs::read(&log_path).unwrap();
        // Frame 1: 4 length-prefix bytes + len_1 payload bytes.
        let len1 = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
        let frame2_start = 4 + len1;
        let len2 =
            u32::from_be_bytes(bytes[frame2_start..frame2_start + 4].try_into().unwrap()) as usize;
        // Flip a byte inside frame 2's author `signature` field so
        // `verify_message` rejects it. Postcard length-prefixes each
        // serde_bytes run, so the frame tail is
        // `[len=64][signature 64][len=64][host_sig 64]`. The last byte
        // is `host_sig[63]` (host-seq sig, not checked by this verify
        // path); the author `signature`'s last byte sits 65 bytes
        // earlier (64 host_sig bytes + 1 length-prefix byte).
        let target = frame2_start + 4 + len2 - 1 - 65;
        bytes[target] ^= 0xff;
        std::fs::write(&log_path, &bytes).unwrap();

        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert_eq!(loaded.len(), 1);
        // Two messages remain: seq 1 and seq 3. seq 2's hole is on
        // purpose — the receiver has no truth for it.
        let seqs: Vec<_> = loaded[0].log.iter().map(|m| m.seq).collect();
        assert_eq!(seqs, vec![Seq::new(1), Seq::new(3)]);
        // The on-disk file is NOT truncated — we left frame 3 on
        // disk too.
        let on_disk_size = std::fs::metadata(&log_path).unwrap().len();
        assert_eq!(
            on_disk_size,
            bytes.len() as u64,
            "tampered-frame skip must not truncate the log",
        );
    }

    #[tokio::test]
    async fn read_log_skips_malformed_mid_frame_and_keeps_later_frames() {
        // L5: a frame whose length prefix is intact but whose payload
        // fails to postcard-decode (at-rest bit-rot mid-log, not a torn
        // trailing write) must be SKIPPED — leaving the durable frames
        // after it intact — exactly like the signature-fail path. The
        // old behaviour set_len-truncated at the bad frame, destroying
        // every good frame that physically followed.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        for seq in 1..=3u64 {
            store.append(record(1).id, &message(seq)).await.unwrap();
        }

        let log_path = dir.path().join(record(1).id.to_string()).join(LOG_FILE);
        let original = std::fs::read(&log_path).unwrap();
        // Corrupt the START of frame 2's payload so postcard decode
        // fails outright (the leading bytes drive the variant/field
        // structure), while frame 2's 4-byte length prefix stays valid
        // so the reader can still find frame 3's boundary.
        let mut bytes = original.clone();
        let len1 = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
        let frame2_payload_start = 4 + len1 + 4;
        bytes[frame2_payload_start] ^= 0xff;
        bytes[frame2_payload_start + 1] ^= 0xff;
        std::fs::write(&log_path, &bytes).unwrap();

        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert_eq!(loaded.len(), 1);
        // Frame 2 is dropped (undecodable), but frames 1 AND 3 survive —
        // the bug was frame 3 being amputated.
        let seqs: Vec<_> = loaded[0].log.iter().map(|m| m.seq).collect();
        assert_eq!(
            seqs,
            vec![Seq::new(1), Seq::new(3)],
            "a malformed mid-log frame must not amputate later durable frames",
        );
        // The on-disk file is NOT truncated.
        let on_disk_size = std::fs::metadata(&log_path).unwrap().len();
        assert_eq!(
            on_disk_size,
            bytes.len() as u64,
            "malformed-frame skip must not truncate the log",
        );
    }

    #[tokio::test]
    async fn read_log_drops_unsigned_sentinel_when_verifying() {
        // A frame with `SIGNATURE_UNSIGNED` (a pre-Slice-B daemon's
        // wire shape, or a buggy/malicious peer that ships the
        // sentinel) is dropped when verification is on — the iroh
        // build's behaviour. Drives `read_log` with `verify = true`
        // directly so the assertion holds regardless of the test
        // binary's own feature set (dev-deps pull `iroh` in even under
        // `--no-default-features`).
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();

        // Hand-write a frame with the sentinel signature. Bypass
        // the normal `append` path so we can plant the unsigned
        // body without going through `Authoring::Local`.
        let unsigned = SessionMessage::new(
            Seq::new(1),
            42,
            peer_for_seed(1, "alice"),
            MessageKind::Chat,
            "x",
            vec![0xab; 8],
            artel_protocol::message::SIGNATURE_UNSIGNED,
            artel_protocol::message::SIGNATURE_UNSIGNED,
        );
        let log_path = dir.path().join(record(1).id.to_string()).join(LOG_FILE);
        append_log(&log_path, &unsigned).unwrap();

        let loaded = read_log(&log_path, record(1).id, true).unwrap();
        assert!(
            loaded.is_empty(),
            "unsigned-sentinel frame must be dropped when verifying",
        );
    }

    #[tokio::test]
    async fn read_log_keeps_unsigned_sentinel_when_not_verifying() {
        // Regression for the no-iroh data-loss bug: a local-only
        // daemon's `author_local` emits `SIGNATURE_UNSIGNED` (no wire
        // surface to sign for), so its own on-disk frames carry the
        // sentinel and it loads with `verify == false`. Verifying
        // there would drop every frame the daemon wrote, silently
        // wiping the entire log on restart; the no-iroh log has a
        // single local writer, so there's no forged-frame threat to
        // reject. The production wiring passes `cfg!(feature = "iroh")`
        // for this flag; the test drives `verify = false` directly.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();

        let unsigned = SessionMessage::new(
            Seq::new(1),
            42,
            peer_for_seed(1, "alice"),
            MessageKind::Chat,
            "x",
            vec![0xab; 8],
            artel_protocol::message::SIGNATURE_UNSIGNED,
            artel_protocol::message::SIGNATURE_UNSIGNED,
        );
        let log_path = dir.path().join(record(1).id.to_string()).join(LOG_FILE);
        append_log(&log_path, &unsigned).unwrap();

        let loaded = read_log(&log_path, record(1).id, false).unwrap();
        assert_eq!(
            loaded,
            vec![unsigned],
            "no-iroh build must keep its own unsigned frames on reload",
        );
    }

    #[tokio::test]
    async fn missing_meta_skips_session_with_warning() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        // Wipe the meta.
        std::fs::remove_file(dir.path().join(record(1).id.to_string()).join(META_FILE)).unwrap();

        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn corrupt_meta_skips_session_with_warning() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        std::fs::write(
            dir.path().join(record(1).id.to_string()).join(META_FILE),
            b"{this isn't json",
        )
        .unwrap();

        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn unsupported_meta_version_skips_session() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        let path = dir.path().join(record(1).id.to_string()).join(META_FILE);
        let mut meta = read_meta(&path).unwrap();
        meta.version = u32::MAX;
        write_meta(&path, &meta).unwrap();

        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn pre_cutover_v2_meta_skipped_on_load() {
        // Migration story (Auth Slice B.5.3): a pre-cutover session
        // directory carries `version: 2` meta (the Slice B shape, no
        // host_sig run on its log frames). `load_all` must skip it
        // with an operator warn — not crash — while OTHER sessions
        // still load. Reuses the existing version-rejection path
        // (Meta::CURRENT_VERSION == 3 from B5.2).
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        // A healthy v3 session...
        store.create(&record(1)).await.unwrap();
        // ...and a planted pre-cutover v2 session.
        store.create(&record(2)).await.unwrap();
        let v2_path = dir.path().join(record(2).id.to_string()).join(META_FILE);
        let mut meta = read_meta(&v2_path).unwrap();
        meta.version = 2;
        write_meta(&v2_path, &meta).unwrap();

        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        // The v2 dir is skipped; the v3 dir still loads.
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, record(1).id);
    }

    #[tokio::test]
    async fn load_from_empty_root_returns_empty() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        let loaded = store.load_all().await.unwrap();
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn session_kind_round_trips() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        let mut r = record(1);
        r.kind = SessionKind::Remote;
        store.create(&r).await.unwrap();

        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].kind, SessionKind::Remote);
    }

    #[test]
    fn meta_version_is_three() {
        // Auth Slice B.5 bumped the on-disk schema 2 → 3.
        assert_eq!(Meta::CURRENT_VERSION, 3);
    }

    #[tokio::test]
    async fn session_record_host_epoch_round_trips() {
        // A record created with a non-zero host_epoch reloads with it.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        let mut r = record(1);
        r.host_epoch = 7;
        store.create(&r).await.unwrap();

        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].host_epoch, 7);
    }

    #[tokio::test]
    async fn fs_persists_and_reloads_host_epoch() {
        // bump_host_epoch persists a new epoch without disturbing the
        // rest of the record, and a fresh open reloads it.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        // Fresh create is epoch 0.
        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert_eq!(loaded[0].host_epoch, 0);

        store.bump_host_epoch(record(1).id, 3).await.unwrap();
        let reloaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert_eq!(reloaded.len(), 1);
        assert_eq!(reloaded[0].host_epoch, 3);
        // Other fields intact.
        assert_eq!(reloaded[0].host, record(1).host);
        assert_eq!(reloaded[0].kind, SessionKind::Local);

        // bump on an unknown session is a no-op, not an error.
        store
            .bump_host_epoch(SessionId::from_bytes([0xee; 16]), 9)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn legacy_meta_without_host_epoch_defaults_to_zero() {
        // A v3 meta.json hand-written without the host_epoch field
        // (e.g. a future field-add forgot a backfill) deserialises to
        // 0 via #[serde(default)] — the correct fresh-session default.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        let r = record(1);
        store.create(&r).await.unwrap();

        let meta_path = dir.path().join(r.id.to_string()).join(META_FILE);
        let legacy = serde_json::json!({
            "version": Meta::CURRENT_VERSION,
            "host": r.host,
            "members": r.members,
            "head": r.head,
            "kind": "Local",
        });
        std::fs::write(&meta_path, serde_json::to_vec_pretty(&legacy).unwrap()).unwrap();

        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].host_epoch, 0);
    }

    #[tokio::test]
    async fn legacy_meta_without_kind_field_defaults_to_local() {
        // Pre-2c-2e meta.json files don't have a `kind` field. Old
        // records were always host-local in practice, so #[serde(default)]
        // → SessionKind::Local is correct retroactively.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        let r = record(1);
        store.create(&r).await.unwrap();

        // Strip the `kind` field by re-writing meta.json with the
        // legacy shape (just the four fields that existed pre-2c-2e).
        let meta_path = dir.path().join(r.id.to_string()).join(META_FILE);
        let legacy = serde_json::json!({
            "version": Meta::CURRENT_VERSION,
            "host": r.host,
            "members": r.members,
            "head": r.head,
        });
        std::fs::write(&meta_path, serde_json::to_vec_pretty(&legacy).unwrap()).unwrap();

        let loaded = FsLogStore::open(dir.path())
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].kind, SessionKind::Local);
    }

    #[tokio::test]
    async fn open_creates_root_dir_with_0700() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("a").join("b").join("sessions");
        let _store = FsLogStore::open(&nested).unwrap();
        let mode = std::os::unix::fs::MetadataExt::mode(&std::fs::metadata(&nested).unwrap());
        assert_eq!(mode & 0o777, DIR_MODE);
    }

    // ---- attachments ----

    const KIND_V1: &str = "artel-fs/workspace/v1";

    #[tokio::test]
    async fn put_then_list_attachment_round_trips() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();

        let payload = b"opaque-bytes".to_vec();
        let ok = store
            .put_attachment(record(1).id, KIND_V1, &payload)
            .await
            .unwrap();
        assert!(ok);

        let listed = store.list_attachments(None).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].session, record(1).id);
        assert_eq!(listed[0].kind, KIND_V1);
        assert_eq!(listed[0].payload, payload);
    }

    #[tokio::test]
    async fn put_attachment_overwrites_existing_at_same_kind() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();

        store
            .put_attachment(record(1).id, KIND_V1, b"first")
            .await
            .unwrap();
        store
            .put_attachment(record(1).id, KIND_V1, b"second")
            .await
            .unwrap();

        let listed = store.list_attachments(None).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].payload, b"second");
    }

    #[tokio::test]
    async fn put_attachment_for_unknown_session_returns_false() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        // No create() — session dir doesn't exist.
        let id = SessionId::from_bytes([0xaa; 16]);
        let ok = store.put_attachment(id, KIND_V1, b"x").await.unwrap();
        assert!(!ok);

        // Verify nothing landed on disk.
        assert!(!dir.path().join(id.to_string()).exists());
    }

    #[tokio::test]
    async fn list_attachments_filters_by_kind_exact_match() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store.create(&record(2)).await.unwrap();

        store
            .put_attachment(record(1).id, KIND_V1, b"a")
            .await
            .unwrap();
        store
            .put_attachment(record(1).id, "other/kind/v1", b"b")
            .await
            .unwrap();
        store
            .put_attachment(record(2).id, KIND_V1, b"c")
            .await
            .unwrap();

        let v1_only = store.list_attachments(Some(KIND_V1)).await.unwrap();
        assert_eq!(v1_only.len(), 2);
        for entry in &v1_only {
            assert_eq!(entry.kind, KIND_V1);
        }

        let others = store.list_attachments(Some("other/kind/v1")).await.unwrap();
        assert_eq!(others.len(), 1);
        assert_eq!(others[0].payload, b"b");
    }

    #[tokio::test]
    async fn list_attachments_returns_empty_when_no_attachments() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        let listed = store.list_attachments(None).await.unwrap();
        assert!(listed.is_empty());
    }

    #[tokio::test]
    async fn delete_attachment_is_idempotent() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store
            .put_attachment(record(1).id, KIND_V1, b"x")
            .await
            .unwrap();

        store
            .delete_attachment(record(1).id, KIND_V1)
            .await
            .unwrap();
        store
            .delete_attachment(record(1).id, KIND_V1)
            .await
            .unwrap();

        assert!(store.list_attachments(None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_attachment_on_unknown_session_is_ok() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        let id = SessionId::from_bytes([0xaa; 16]);
        store.delete_attachment(id, KIND_V1).await.unwrap();
    }

    #[tokio::test]
    async fn delete_session_cascades_attachments() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store
            .put_attachment(record(1).id, KIND_V1, b"a")
            .await
            .unwrap();
        store
            .put_attachment(record(1).id, "other/kind/v1", b"b")
            .await
            .unwrap();

        store.delete(record(1).id).await.unwrap();

        // No attachments remain — the cascade fell out of remove_dir_all.
        assert!(store.list_attachments(None).await.unwrap().is_empty());
        let attachments_dir = dir
            .path()
            .join(record(1).id.to_string())
            .join(ATTACHMENTS_DIR);
        assert!(!attachments_dir.exists());
    }

    #[tokio::test]
    async fn attachment_filename_is_hex_encoded() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store
            .put_attachment(record(1).id, KIND_V1, b"x")
            .await
            .unwrap();

        // lowercase-hex(b"artel-fs/workspace/v1") + ".bin"
        let expected =
            "61727465".to_string() + "6c2d66732f776f726b73706163652f7631" + ATTACHMENT_FILE_SUFFIX;
        let path = dir
            .path()
            .join(record(1).id.to_string())
            .join(ATTACHMENTS_DIR)
            .join(expected);
        assert!(
            path.exists(),
            "expected hex-encoded attachment at {}",
            path.display()
        );
    }

    #[tokio::test]
    async fn non_hex_attachment_filenames_are_skipped_with_warning() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store
            .put_attachment(record(1).id, KIND_V1, b"valid")
            .await
            .unwrap();

        // Drop a non-hex filename next to the valid one.
        let attachments_dir = dir
            .path()
            .join(record(1).id.to_string())
            .join(ATTACHMENTS_DIR);
        std::fs::write(attachments_dir.join("not-hex.bin"), b"junk").unwrap();

        let listed = store.list_attachments(None).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].kind, KIND_V1);
    }

    #[tokio::test]
    async fn oversized_attachment_payload_is_skipped() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();

        // Drop a too-large file directly. We bypass put_attachment
        // because that path would reject MAX_FRAME_SIZE-sized writes;
        // we want to exercise list_attachments's defence-in-depth
        // skip-and-warn path for files that somehow appeared on disk.
        let attachments_dir = dir
            .path()
            .join(record(1).id.to_string())
            .join(ATTACHMENTS_DIR);
        ensure_dir(&attachments_dir, DIR_MODE).unwrap();
        let path = attachments_dir.join(attachment_filename(KIND_V1));
        let f = std::fs::File::create(&path).unwrap();
        f.set_len(MAX_FRAME_SIZE as u64 + 1).unwrap();
        f.sync_all().unwrap();

        let listed = store.list_attachments(None).await.unwrap();
        assert!(listed.is_empty(), "expected oversized file to be skipped");
    }

    #[tokio::test]
    async fn put_attachment_rejects_oversized_payload() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();

        let payload = vec![0u8; MAX_FRAME_SIZE + 1];
        let err = store
            .put_attachment(record(1).id, KIND_V1, &payload)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    /// Race-regression: two concurrent writers of the same
    /// `(session, kind)` must not corrupt the final file. With a
    /// deterministic tmp path, both writers shared one `<hex>.bin.tmp`
    /// — `create+truncate` could interleave, and the renamed file
    /// would hold a mixture of both payloads. Unique tmp filenames
    /// (`<base>.<pid>.<counter>.tmp`) make the contention land on the
    /// final rename instead, where one of the two valid payloads wins
    /// atomically.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_put_attachment_same_kind_does_not_corrupt() {
        for _ in 0..50 {
            let dir = tempdir().unwrap();
            let store = std::sync::Arc::new(FsLogStore::open(dir.path()).unwrap());
            store.create(&record(1)).await.unwrap();

            let payload_a = vec![0xAAu8; 4096];
            let payload_b = vec![0xBBu8; 4096];
            let store_a = std::sync::Arc::clone(&store);
            let store_b = std::sync::Arc::clone(&store);
            let pa = payload_a.clone();
            let pb = payload_b.clone();
            let task_a = tokio::spawn(async move {
                store_a
                    .put_attachment(record(1).id, KIND_V1, &pa)
                    .await
                    .unwrap();
            });
            let task_b = tokio::spawn(async move {
                store_b
                    .put_attachment(record(1).id, KIND_V1, &pb)
                    .await
                    .unwrap();
            });
            task_a.await.unwrap();
            task_b.await.unwrap();

            let listed = store.list_attachments(None).await.unwrap();
            assert_eq!(listed.len(), 1);
            // The winning payload is whichever rename landed second.
            // It MUST be one of the two valid payloads — never a mix.
            let p = &listed[0].payload;
            assert!(
                p == &payload_a || p == &payload_b,
                "corrupted final payload: first 8 bytes = {:?}",
                &p[..8.min(p.len())],
            );

            // No `.tmp` files left dangling.
            let attachments_dir = dir
                .path()
                .join(record(1).id.to_string())
                .join(ATTACHMENTS_DIR);
            for entry in std::fs::read_dir(&attachments_dir).unwrap() {
                let name = entry.unwrap().file_name();
                let name = name.to_string_lossy();
                assert!(!name.ends_with(".tmp"), "left a tmp file behind: {name}");
            }
        }
    }

    #[tokio::test]
    async fn put_attachment_rejects_oversized_kind() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();

        // One byte over the cap. We reject before the OS gets a
        // chance, so the error stays typed (InvalidData) rather than
        // ENAMETOOLONG-leaking.
        let oversized = "k".repeat(MAX_KIND_LEN + 1);
        let err = store
            .put_attachment(record(1).id, &oversized, b"x")
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn put_attachment_accepts_kind_at_cap() {
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();

        let max_kind = "k".repeat(MAX_KIND_LEN);
        let ok = store
            .put_attachment(record(1).id, &max_kind, b"x")
            .await
            .unwrap();
        assert!(ok);
        let listed = store.list_attachments(None).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].kind, max_kind);
    }

    #[tokio::test]
    async fn put_attachment_rejects_empty_kind() {
        // An empty kind would hex-encode to a zero-length stem,
        // producing the dotfile `.bin`. decode_attachment_filename
        // round-trips it as `Some("")`, which is unfilterable from any
        // `Some(non-empty)` query — a misconfigured caller could
        // persist an entry no typed reader ever sees.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        let err = store
            .put_attachment(record(1).id, "", b"x")
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        // No file laid down on disk.
        let attachments_dir = dir
            .path()
            .join(record(1).id.to_string())
            .join(ATTACHMENTS_DIR);
        assert!(
            !attachments_dir.exists() || attachments_dir.read_dir().unwrap().next().is_none(),
            "rejected put must not create any attachment file",
        );
    }

    #[tokio::test]
    async fn open_sweeps_orphaned_tmp_files() {
        // Simulate a previous daemon crashing mid-write_attachment:
        // an `<base>.<pid>.<counter>.tmp` is left next to a real
        // attachment. Re-opening the store should reap it without
        // touching the real file.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store
            .put_attachment(record(1).id, KIND_V1, b"real")
            .await
            .unwrap();

        let attachments_dir = dir
            .path()
            .join(record(1).id.to_string())
            .join(ATTACHMENTS_DIR);
        let real_path = attachments_dir.join(attachment_filename(KIND_V1));
        // Plant an orphan with the EXACT shape unique_tmp_path
        // produces: `<hex>.bin.<pid>.<counter>.tmp`. The hex stem is
        // an arbitrary kind hex-encoded; pid + counter are arbitrary
        // digits. Any drift in the sweep filter that no longer
        // matches this real shape makes the test fail.
        let orphan_hex = attachment_filename("orphan-kind/v1");
        let orphan_path = attachments_dir.join(format!("{orphan_hex}.99.42.tmp"));
        std::fs::write(&orphan_path, b"orphan").unwrap();
        assert!(orphan_path.exists());
        assert!(real_path.exists());

        // Re-open: triggers the sweep.
        let _store2 = FsLogStore::open(dir.path()).unwrap();
        assert!(!orphan_path.exists(), "sweep should have removed tmp");
        assert!(real_path.exists(), "sweep must not touch real attachments");
    }

    #[tokio::test]
    async fn open_sweep_leaves_unrelated_tmp_files_alone() {
        // The sweep filter is is_attachment_tmp (the unique_tmp_path
        // shape `<hex>.bin.<pid>.<counter>.tmp`). An admin's stray
        // `.tmp` file or an editor backup that lives in the
        // attachments/ dir must be preserved across daemon restarts —
        // the sweep is OUR atomic-write cleanup, not a generic
        // dotfile reaper.
        let dir = tempdir().unwrap();
        let store = FsLogStore::open(dir.path()).unwrap();
        store.create(&record(1)).await.unwrap();
        store
            .put_attachment(record(1).id, KIND_V1, b"real")
            .await
            .unwrap();

        let attachments_dir = dir
            .path()
            .join(record(1).id.to_string())
            .join(ATTACHMENTS_DIR);
        // Generic .tmp files that don't match the unique_tmp_path
        // shape — the sweep must leave each of these alone.
        let unrelated = [
            attachments_dir.join("notes.tmp"),          // no hex stem
            attachments_dir.join("notes.txt.42.7.tmp"), // hex stem fails
            attachments_dir.join("a1b2.bin.foo.7.tmp"), // pid not digits
            attachments_dir.join("a1b2.bin.42..tmp"),   // empty counter
        ];
        for p in &unrelated {
            std::fs::write(p, b"unrelated").unwrap();
        }

        let _store2 = FsLogStore::open(dir.path()).unwrap();
        for p in &unrelated {
            assert!(p.exists(), "sweep wrongly reaped {}", p.display());
        }
    }

    #[tokio::test]
    async fn open_sweep_tolerates_missing_root() {
        // A fresh root that doesn't exist yet: open creates it and
        // the sweep finds nothing to do. No error.
        let dir = tempdir().unwrap();
        let nested = dir.path().join("brand-new");
        let _store = FsLogStore::open(&nested).unwrap();
        assert!(nested.is_dir());
    }

    /// Race-regression for the list-vs-cascade contract: while
    /// `list_attachments` iterates a session's attachments dir, a
    /// concurrent `delete` (cascade via `remove_dir_all`) can unlink
    /// entries out from under it. The store must skip-and-continue —
    /// per the trait doc — rather than propagating an `io::NotFound`
    /// up to the caller.
    ///
    /// Drives the race deterministically by spawning N attachments,
    /// then issuing list and delete concurrently many times. Any
    /// successful list must return either 0..=N entries with no
    /// torn entries; an `io::Error` is a hard failure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn list_attachments_skips_entries_unlinked_mid_iteration() {
        for _ in 0..50 {
            let dir = tempdir().unwrap();
            let store = std::sync::Arc::new(FsLogStore::open(dir.path()).unwrap());
            store.create(&record(1)).await.unwrap();
            // Seed many attachments so the iterator has real work to do.
            for i in 0..32u8 {
                let kind = format!("kind/{i}");
                store
                    .put_attachment(record(1).id, &kind, &[i; 64])
                    .await
                    .unwrap();
            }

            let store_list = std::sync::Arc::clone(&store);
            let store_del = std::sync::Arc::clone(&store);
            let list_task = tokio::spawn(async move { store_list.list_attachments(None).await });
            let delete_task = tokio::spawn(async move { store_del.delete(record(1).id).await });

            // Both must succeed; the list must NOT propagate NotFound.
            let listed = list_task.await.unwrap();
            delete_task.await.unwrap().unwrap();
            let listed = listed.expect("list_attachments must not error on concurrent cascade");
            // Whatever survived must be well-formed — no torn entries.
            for entry in &listed {
                assert!(entry.kind.starts_with("kind/"));
                assert_eq!(entry.payload.len(), 64);
            }
        }
    }

    #[test]
    fn is_attachment_tmp_pins_the_unique_tmp_path_shape() {
        // Real shapes unique_tmp_path produces: `<hex>.bin.<pid>.<counter>.tmp`.
        let real = format!("{}.42.7.tmp", attachment_filename("artel-fs/workspace/v1"));
        assert!(is_attachment_tmp(&real));
        assert!(is_attachment_tmp(&format!(
            "{}.0.0.tmp",
            attachment_filename("kind"),
        )));
        assert!(is_attachment_tmp(&format!(
            "{}.999999.123456789.tmp",
            attachment_filename("k"),
        )));

        // Things that look like tmps but aren't ours:
        assert!(!is_attachment_tmp("notes.tmp"));
        assert!(!is_attachment_tmp("notes.txt.42.7.tmp"));
        assert!(!is_attachment_tmp("ABCD.bin.42.7.tmp")); // uppercase hex
        assert!(!is_attachment_tmp("abc.bin.42.7.tmp")); // odd-length hex
        assert!(!is_attachment_tmp("a1b2.bin.foo.7.tmp")); // pid non-digit
        assert!(!is_attachment_tmp("a1b2.bin.42.bar.tmp")); // counter non-digit
        assert!(!is_attachment_tmp("a1b2.bin.42..tmp")); // empty counter
        assert!(!is_attachment_tmp("a1b2.bin..7.tmp")); // empty pid
        assert!(!is_attachment_tmp(".bin.42.7.tmp")); // empty hex
        assert!(!is_attachment_tmp("a1b2.bin.42.7")); // missing .tmp
        assert!(!is_attachment_tmp("a1b2.txt.42.7.tmp")); // wrong .bin slot
    }
}
