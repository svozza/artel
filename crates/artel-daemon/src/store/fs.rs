//! On-disk [`super::SessionStore`].
//!
//! Layout under the configured `sessions_dir`:
//!
//! ```text
//! sessions_dir/
//!   <session-uuid>/
//!     meta.json   — host, members, head
//!     log         — length-prefixed postcard frames of SessionMessage
//! ```
//!
//! `meta.json` is small enough to overwrite atomically (write to
//! `meta.json.tmp`, fsync, rename) on every membership or head change.
//! The `log` is append-only with `fsync` after each frame so a crash
//! between the response being acked and the OS flushing is impossible.
//!
//! Recovery: on `load_all`, missing or unparseable `meta.json` makes
//! the daemon skip that session with a warning. A partial trailing
//! frame in `log` is truncated (we never acked it to a client).

#![allow(clippy::redundant_pub_crate)]

use std::collections::HashSet;
use std::io::{self, ErrorKind, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use artel_protocol::{PeerId, PeerInfo, Seq, SessionId, SessionMessage};
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
}

#[async_trait]
impl SessionStore for FsLogStore {
    async fn create(&self, record: &SessionRecord) -> io::Result<()> {
        let dir = self.session_dir(record.id);
        let record = record.clone();
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            ensure_dir(&dir, DIR_MODE)?;
            // Always start with a meta.json. If a stale log exists from
            // a previous session at this id (shouldn't happen — uuid),
            // fail loudly.
            let log_path = dir.join(LOG_FILE);
            if log_path.exists() {
                return Err(io::Error::new(
                    ErrorKind::AlreadyExists,
                    format!("log already exists at {}", log_path.display()),
                ));
            }
            // Touch the log so subsequent appends find a file to open.
            std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&log_path)?;
            chmod(&log_path, FILE_MODE)?;

            write_meta(&dir.join(META_FILE), &Meta::from_record(&record))?;
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
            let mut meta = read_meta(&meta_path)?;
            meta.head = message.seq;
            write_meta(&meta_path, &meta)?;
            Ok(())
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
}

impl Meta {
    /// On-disk schema version for `meta.json`.
    ///
    /// Bumped to `2` on 2026-06-02 (Auth Slice B1) when
    /// `MESSAGE_FORMAT` went 1 → 2 and the on-disk log frames started
    /// embedding signatures. A v1 directory is unreadable by a v2
    /// daemon: pre-Slice-B logs have no signature byte run, so even
    /// unverified replay would mis-decode.
    const CURRENT_VERSION: u32 = 2;

    fn from_record(r: &SessionRecord) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            host: r.host,
            members: r.members.clone(),
            head: r.head,
            kind: r.kind,
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
    let log = read_log(&dir.join(LOG_FILE))?;
    Ok(SessionRecord {
        id,
        host: meta.host,
        members: meta.members,
        head: meta.head,
        log,
        kind: meta.kind,
    })
}

fn read_meta(path: &Path) -> io::Result<Meta> {
    let bytes = std::fs::read(path)?;
    serde_json::from_slice(&bytes)
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, format!("meta json: {e}")))
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

/// Atomic write: `path.tmp` + fsync + rename.
fn write_meta(path: &Path, meta: &Meta) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(meta)
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, format!("meta json: {e}")))?;
    let tmp = path.with_extension("json.tmp");
    {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    chmod(&tmp, FILE_MODE)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
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

/// Read every complete frame in `path`. A trailing partial frame
/// (length prefix not fully present, length prefix says N bytes but
/// fewer follow, or postcard parse fails) is logged and the file is
/// truncated to the last good byte.
fn read_log(path: &Path) -> io::Result<Vec<SessionMessage>> {
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
                out.push(msg);
                last_good = f.stream_position()?;
            }
            Err(err) => {
                warn!(
                    file = %path.display(),
                    error = %err,
                    "truncating malformed log frame"
                );
                f.set_len(last_good)?;
                break;
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

    fn record(id_byte: u8) -> SessionRecord {
        SessionRecord {
            id: SessionId::from_bytes([id_byte; 16]),
            host: PeerId::from_bytes([id_byte; 32]),
            members: HashSet::from([PeerId::from_bytes([id_byte; 32])]),
            head: Seq::ZERO,
            log: Vec::new(),
            kind: SessionKind::Local,
        }
    }

    fn message(seq: u64) -> SessionMessage {
        SessionMessage::new(
            Seq::new(seq),
            seq,
            PeerInfo::new(PeerId::from_bytes([1; 32]), "alice"),
            MessageKind::Chat,
            "x",
            vec![0xab; 8],
            artel_protocol::message::SIGNATURE_UNSIGNED,
        )
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
