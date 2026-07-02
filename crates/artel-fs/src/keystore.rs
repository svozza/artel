//! Small atomic-write / private-dir helpers for `artel-fs` state
//! files.
//!
//! The workspace's persisted **iroh secret key** (`iroh.key`) loads
//! through the shared [`artel_iroh_setup::load_or_create`] — see
//! `node.rs`. What remains here are the plain file helpers the
//! workspace uses for its other state-dir slots (`doc-id`,
//! `current-namespace`, `namespace_epoch`, …): atomic tmp+rename
//! writes and 0700 directory creation.

#![allow(clippy::redundant_pub_crate)]

use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// Write `bytes` to `path` atomically: tmp-create + `write_all` +
/// fsync + (optional chmod) + rename. A partially-written tmp file
/// never replaces the real one.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8], mode: Option<u32>) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        if let Some(mode) = mode {
            let mut perms = f.metadata()?.permissions();
            perms.set_mode(mode);
            fs::set_permissions(&tmp, perms)?;
        }
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Same shape as artel-protocol's `transport::server::ensure_dir` —
/// chmod 0700 on create, leave existing dirs alone.
pub(crate) fn ensure_dir(dir: &Path) -> io::Result<()> {
    match fs::metadata(dir) {
        Ok(meta) if meta.is_dir() => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} exists but is not a directory", dir.display()),
        )),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(dir)?;
            let mut perms = fs::metadata(dir)?.permissions();
            perms.set_mode(0o700);
            fs::set_permissions(dir, perms)?;
            Ok(())
        }
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn write_atomic_round_trips_and_removes_tmp() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("slot");
        write_atomic(&path, b"payload", None).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"payload");
        assert!(!path.with_extension("tmp").exists());
    }

    #[test]
    fn write_atomic_applies_requested_mode() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempdir().unwrap();
        let path = dir.path().join("secretish");
        write_atomic(&path, b"x", Some(0o600)).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().mode() & 0o777, 0o600);
    }

    #[test]
    fn write_atomic_overwrites_existing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("slot");
        write_atomic(&path, b"old", None).unwrap();
        write_atomic(&path, b"new", None).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"new");
    }

    #[test]
    fn ensure_dir_creates_at_0700_and_tolerates_existing() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempdir().unwrap();
        let nested = dir.path().join("a").join("b");
        ensure_dir(&nested).unwrap();
        assert_eq!(fs::metadata(&nested).unwrap().mode() & 0o777, 0o700);
        // Second call is a no-op, not an error.
        ensure_dir(&nested).unwrap();
    }

    #[test]
    fn ensure_dir_rejects_non_directory() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("file");
        fs::write(&file, b"x").unwrap();
        assert!(ensure_dir(&file).is_err());
    }
}
