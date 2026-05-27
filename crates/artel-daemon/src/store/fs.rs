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
    pub(crate) fn open(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        ensure_dir(&root, DIR_MODE)?;
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
                    let att = att?;
                    let path = att.path();
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
                    let metadata = std::fs::metadata(&path)?;
                    if metadata.len() > MAX_FRAME_SIZE as u64 {
                        warn!(
                            file = %path.display(),
                            size = metadata.len(),
                            "skipping attachment: payload exceeds MAX_FRAME_SIZE",
                        );
                        continue;
                    }
                    let payload = std::fs::read(&path)?;
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
    const CURRENT_VERSION: u32 = 1;

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
/// Same `<path>.tmp` + fsync + rename + chmod-0o600 dance as
/// [`write_meta`]; the only thing different is the file content
/// (raw bytes vs. JSON).
fn write_attachment(path: &Path, payload: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("bin.tmp");
    {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        f.write_all(payload)?;
        f.sync_all()?;
    }
    chmod(&tmp, FILE_MODE)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
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
}
