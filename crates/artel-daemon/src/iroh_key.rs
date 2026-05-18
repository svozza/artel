//! Persisted iroh secret key.
//!
//! The daemon's `EndpointId` is derived from a 32-byte ed25519 secret
//! key. That id is the daemon's network identity: it appears in
//! tickets and is what peers dial. For the identity to be stable
//! across daemon restarts, the secret has to live on disk between
//! invocations.
//!
//! [`load_or_create`] is the entry point. It reads `path` if it
//! exists, generates a fresh key via `OsRng` otherwise, and writes
//! the new key out atomically with mode `0600`.
//!
//! ## Threat model
//!
//! Anyone with read access to the key file can impersonate this
//! daemon's network identity. The file is owner-readable only, lives
//! under `~/.artel` (already chmod 0700), and is never logged or
//! transmitted. Same risk profile as `~/.ssh/id_ed25519`.

#![allow(clippy::redundant_pub_crate)]

use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use iroh::SecretKey;
use rand::TryRngCore;
use rand::rngs::OsRng;

/// Mode applied to the on-disk key file. Owner read+write only —
/// anyone else who can read it can impersonate this daemon.
const KEY_MODE: u32 = 0o600;

/// Errors `load_or_create` may surface.
#[derive(Debug, thiserror::Error)]
pub(crate) enum KeyError {
    /// Filesystem or syscall error while reading or writing the key
    /// file.
    #[error("iroh key {path}: {source}")]
    Io {
        /// Path that failed.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Key file exists but is not exactly 32 bytes.
    #[error("iroh key {path} is corrupt: expected 32 bytes, got {got}")]
    Corrupt {
        /// Path that was read.
        path: PathBuf,
        /// Number of bytes actually present.
        got: usize,
    },

    /// Could not draw entropy from the OS RNG.
    #[error("iroh key generation failed: {0}")]
    Rng(#[source] rand::rand_core::OsError),
}

/// Load the secret key from `path`, or generate and persist a fresh
/// one if the file does not yet exist.
///
/// The parent directory of `path` is created at mode `0700` if
/// missing — matches `transport::path` conventions.
pub(crate) fn load_or_create(path: &Path) -> Result<SecretKey, KeyError> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent).map_err(|source| KeyError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    match fs::read(path) {
        Ok(bytes) => parse_key(path, &bytes),
        Err(err) if err.kind() == io::ErrorKind::NotFound => generate_and_persist(path),
        Err(source) => Err(KeyError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn parse_key(path: &Path, bytes: &[u8]) -> Result<SecretKey, KeyError> {
    let arr: &[u8; 32] = bytes.try_into().map_err(|_| KeyError::Corrupt {
        path: path.to_path_buf(),
        got: bytes.len(),
    })?;
    Ok(SecretKey::from_bytes(arr))
}

fn generate_and_persist(path: &Path) -> Result<SecretKey, KeyError> {
    let mut bytes = [0u8; 32];
    OsRng.try_fill_bytes(&mut bytes).map_err(KeyError::Rng)?;
    write_atomic(path, &bytes).map_err(|source| KeyError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(SecretKey::from_bytes(&bytes))
}

/// Write `bytes` to `path` atomically: write to `path.tmp`, fsync,
/// chmod 0600, rename. Crash-safe: a partially-written tmp file
/// never replaces the real one.
fn write_atomic(path: &Path, bytes: &[u8; 32]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        let mut perms = f.metadata()?.permissions();
        perms.set_mode(KEY_MODE);
        fs::set_permissions(&tmp, perms)?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Same shape as `transport::server::ensure_dir` — chmod 0700 on
/// create, leave existing dirs alone.
fn ensure_dir(dir: &Path) -> io::Result<()> {
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
    use std::os::unix::fs::MetadataExt;

    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn missing_file_generates_persists_and_returns_a_key() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("iroh.key");
        assert!(!path.exists());

        let key = load_or_create(&path).unwrap();
        assert!(path.exists(), "key file should be written");
        assert_eq!(fs::read(&path).unwrap().len(), 32);

        // The persisted bytes match the in-memory key.
        let on_disk = fs::read(&path).unwrap();
        assert_eq!(on_disk, key.to_bytes());
    }

    #[test]
    fn second_call_reads_existing_key_unchanged() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("iroh.key");

        let first = load_or_create(&path).unwrap();
        let second = load_or_create(&path).unwrap();
        assert_eq!(
            first.to_bytes(),
            second.to_bytes(),
            "second load must reuse the persisted key",
        );
    }

    #[test]
    fn key_file_is_chmod_0600() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("iroh.key");
        let _ = load_or_create(&path).unwrap();

        let mode = fs::metadata(&path).unwrap().mode() & 0o777;
        assert_eq!(mode, KEY_MODE, "key file mode should be {KEY_MODE:o}");
    }

    #[test]
    fn parent_dir_is_created_at_0700() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("a").join("b");
        let path = nested.join("iroh.key");
        let _ = load_or_create(&path).unwrap();

        let mode = fs::metadata(&nested).unwrap().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[test]
    fn corrupt_file_too_short_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("iroh.key");
        fs::write(&path, b"short").unwrap();

        let err = load_or_create(&path).unwrap_err();
        match err {
            KeyError::Corrupt { got, .. } => assert_eq!(got, 5),
            other => panic!("expected Corrupt, got {other:?}"),
        }
    }

    #[test]
    fn corrupt_file_too_long_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("iroh.key");
        fs::write(&path, vec![0u8; 64]).unwrap();

        let err = load_or_create(&path).unwrap_err();
        assert!(matches!(err, KeyError::Corrupt { got: 64, .. }), "{err:?}");
    }

    #[test]
    fn empty_file_errors_as_corrupt() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("iroh.key");
        fs::write(&path, b"").unwrap();

        let err = load_or_create(&path).unwrap_err();
        assert!(matches!(err, KeyError::Corrupt { got: 0, .. }), "{err:?}");
    }

    #[test]
    fn load_then_use_for_endpoint_id_is_stable() {
        // The whole point: same key bytes -> same EndpointId.
        let dir = tempdir().unwrap();
        let path = dir.path().join("iroh.key");
        let a = load_or_create(&path).unwrap();
        let b = load_or_create(&path).unwrap();
        assert_eq!(a.public(), b.public());
    }
}
