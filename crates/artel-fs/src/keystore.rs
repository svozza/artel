//! Persisted ed25519 secret keys for an `artel-fs` workspace.
//!
//! A workspace has exactly one secret to keep stable across
//! restarts: its **iroh endpoint identity** (the 32-byte ed25519
//! seed that becomes the workspace's `EndpointId` / `NodeId`).
//! Without persistence, every restart rotates the network
//! identity and any peer that learned about the workspace via a
//! prior ticket has to re-learn it.
//!
//! The iroh-docs **author** identity is derived from this same
//! seed (`AuthorId` == endpoint id — see `node.rs`'s author
//! binding), so this file covers both. iroh-docs' own
//! `default-author` file still exists on disk but is deliberately
//! unused.
//!
//! [`load_or_create_secret`] is the entry point. It reads `path`
//! if it exists, otherwise generates a fresh key from `OsRng` and
//! writes it out atomically with mode `0600`.

#![allow(clippy::redundant_pub_crate)]

use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use iroh::SecretKey;
use rand::TryRngCore;
use rand::rngs::OsRng;

/// Mode applied to the on-disk secret file. Owner read/write only.
const KEY_MODE: u32 = 0o600;

/// Errors [`load_or_create_secret`] may surface.
#[derive(Debug, thiserror::Error)]
pub(crate) enum KeyError {
    /// Filesystem or syscall error while reading or writing the
    /// key file.
    #[error("workspace key {path}: {source}")]
    Io {
        /// Path that failed.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Key file exists but is not exactly 32 bytes.
    #[error("workspace key {path} is corrupt: expected 32 bytes, got {got}")]
    Corrupt {
        /// Path that was read.
        path: PathBuf,
        /// Number of bytes actually present.
        got: usize,
    },

    /// Could not draw entropy from the OS RNG.
    #[error("workspace key generation failed: {0}")]
    Rng(#[source] rand::rand_core::OsError),
}

/// Load the secret key at `path`, or generate and persist a fresh
/// one if the file does not yet exist.
///
/// The parent directory is created at mode `0700` if missing —
/// matches the daemon's `iroh_key.rs` and `transport::path`
/// conventions.
pub(crate) fn load_or_create_secret(path: &Path) -> Result<SecretKey, KeyError> {
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
    write_atomic(path, &bytes, Some(KEY_MODE)).map_err(|source| KeyError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(SecretKey::from_bytes(&bytes))
}

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

/// Same shape as `transport::server::ensure_dir` — chmod 0700 on
/// create, leave existing dirs alone.
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
    use std::os::unix::fs::MetadataExt;

    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn missing_file_generates_persists_and_returns_a_key() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("iroh.key");
        assert!(!path.exists());

        let key = load_or_create_secret(&path).unwrap();
        assert!(path.exists(), "key file should be written");
        assert_eq!(fs::read(&path).unwrap().len(), 32);

        let on_disk = fs::read(&path).unwrap();
        assert_eq!(on_disk, key.to_bytes());
    }

    #[test]
    fn second_call_reads_existing_key_unchanged() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("iroh.key");

        let first = load_or_create_secret(&path).unwrap();
        let second = load_or_create_secret(&path).unwrap();
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
        let _ = load_or_create_secret(&path).unwrap();

        let mode = fs::metadata(&path).unwrap().mode() & 0o777;
        assert_eq!(mode, KEY_MODE, "key file mode should be {KEY_MODE:o}");
    }

    #[test]
    fn parent_dir_is_created_at_0700() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("a").join("b");
        let path = nested.join("iroh.key");
        let _ = load_or_create_secret(&path).unwrap();

        let mode = fs::metadata(&nested).unwrap().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[test]
    fn corrupt_file_too_short_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("iroh.key");
        fs::write(&path, b"short").unwrap();

        let err = load_or_create_secret(&path).unwrap_err();
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

        let err = load_or_create_secret(&path).unwrap_err();
        assert!(matches!(err, KeyError::Corrupt { got: 64, .. }), "{err:?}");
    }

    #[test]
    fn empty_file_errors_as_corrupt() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("iroh.key");
        fs::write(&path, b"").unwrap();

        let err = load_or_create_secret(&path).unwrap_err();
        assert!(matches!(err, KeyError::Corrupt { got: 0, .. }), "{err:?}");
    }

    #[test]
    fn endpoint_id_stable_across_loads() {
        // Same key bytes -> same EndpointId / public key.
        let dir = tempdir().unwrap();
        let path = dir.path().join("iroh.key");
        let a = load_or_create_secret(&path).unwrap();
        let b = load_or_create_secret(&path).unwrap();
        assert_eq!(a.public(), b.public());
    }
}
