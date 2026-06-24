//! Daemon-side socket binding and accept loop.

use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use tokio::net::{UnixListener, UnixStream};

use super::framed::{Framed, new as new_framed};

/// Permissions applied to the parent state directory.
///
/// `0700` = owner read/write/execute, no group/other access. Prevents
/// other local users from listing or descending the directory.
const DIR_MODE: u32 = 0o700;

/// Permissions applied to the bound socket file.
///
/// `0600` = owner read/write, no group/other access. Without this, the
/// platform default would let any local user connect.
const SOCKET_MODE: u32 = 0o600;

/// A bound Unix-socket listener for the artel IPC.
///
/// On drop, the socket file is removed if it still points at this
/// listener's path. The parent directory is left in place — it is shared
/// with the daemon's PID file, log, etc.
#[derive(Debug)]
pub struct Listener {
    inner: UnixListener,
    path: PathBuf,
}

impl Listener {
    /// Bind to `path`, creating the parent directory at mode `0700` if
    /// missing and chmodding the socket to `0600` after binding.
    ///
    /// If a stale socket file already exists at `path`, it is *not*
    /// removed — callers detect that and decide whether to recover. That
    /// keeps the listener honest about double-bind: a second daemon
    /// trying to bind the same path will get `AddrInUse` rather than
    /// silently stealing the socket.
    ///
    /// # Errors
    ///
    /// Returns an [`io::Error`] if creating the parent directory fails,
    /// if binding the socket fails (notably [`io::ErrorKind::AddrInUse`]
    /// when a stale or live socket already occupies `path`), or if
    /// reading/setting the socket's permissions to `0600` fails.
    pub async fn bind(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();

        if let Some(parent) = path.parent() {
            ensure_dir(parent).await?;
        }

        let listener = UnixListener::bind(&path)?;

        // Tighten perms after bind. Failure here is fatal: a world-
        // readable socket would defeat the access control.
        let mut perms = fs::metadata(&path)?.permissions();
        perms.set_mode(SOCKET_MODE);
        fs::set_permissions(&path, perms)?;

        Ok(Self {
            inner: listener,
            path,
        })
    }

    /// Path the listener is bound to.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Accept the next incoming connection and wrap it in [`Framed`].
    ///
    /// # Errors
    ///
    /// Returns the [`io::Error`] from the underlying accept if the OS
    /// fails to hand back a connected stream.
    pub async fn accept(&self) -> io::Result<Framed<UnixStream>> {
        let (stream, _addr) = self.inner.accept().await?;
        Ok(new_framed(stream))
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        // Best-effort: if the socket file still exists at our path,
        // remove it. We don't surface errors — drop can't anyway, and
        // the daemon's next startup will recover.
        let _ = fs::remove_file(&self.path);
    }
}

/// Create `dir` (and parents) at mode `0700` if it does not yet exist.
///
/// If the directory already exists, this leaves its mode alone — the
/// user may have set their own preference, and overwriting it would be
/// surprising.
async fn ensure_dir(dir: &Path) -> io::Result<()> {
    match tokio::fs::metadata(dir).await {
        Ok(meta) if meta.is_dir() => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} exists but is not a directory", dir.display()),
        )),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            tokio::fs::create_dir_all(dir).await?;
            let mut perms = fs::metadata(dir)?.permissions();
            perms.set_mode(DIR_MODE);
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
    use tokio::runtime::Builder;

    use super::*;

    fn rt() -> tokio::runtime::Runtime {
        Builder::new_current_thread().enable_all().build().unwrap()
    }

    #[test]
    fn bind_creates_parent_dir_and_chmods_socket() {
        rt().block_on(async {
            let dir = tempdir().unwrap();
            let nested = dir.path().join("a").join("b");
            let sock = nested.join("daemon.sock");

            let _listener = Listener::bind(&sock).await.unwrap();

            // Parent directory was created.
            let dir_meta = fs::metadata(&nested).unwrap();
            assert!(dir_meta.is_dir());
            // Mode 0700 (the bottom 9 bits).
            assert_eq!(
                dir_meta.mode() & 0o777,
                DIR_MODE,
                "parent dir mode should be {DIR_MODE:o}"
            );

            // Socket exists and is mode 0600.
            let sock_meta = fs::metadata(&sock).unwrap();
            assert_eq!(
                sock_meta.mode() & 0o777,
                SOCKET_MODE,
                "socket mode should be {SOCKET_MODE:o}"
            );
        });
    }

    #[test]
    fn drop_unbinds_socket_file() {
        rt().block_on(async {
            let dir = tempdir().unwrap();
            let sock = dir.path().join("daemon.sock");
            {
                let _listener = Listener::bind(&sock).await.unwrap();
                assert!(sock.exists());
            }
            assert!(!sock.exists(), "drop should remove the socket file");
        });
    }

    #[test]
    fn second_bind_to_same_path_errors() {
        rt().block_on(async {
            let dir = tempdir().unwrap();
            let sock = dir.path().join("daemon.sock");

            let _first = Listener::bind(&sock).await.unwrap();
            let err = Listener::bind(&sock).await.unwrap_err();
            assert_eq!(
                err.kind(),
                io::ErrorKind::AddrInUse,
                "expected AddrInUse, got {err:?}"
            );
        });
    }

    #[test]
    fn bind_into_existing_dir_does_not_change_dir_mode() {
        rt().block_on(async {
            let dir = tempdir().unwrap();
            // Set parent to a different mode and confirm we don't touch it.
            let mut perms = fs::metadata(dir.path()).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(dir.path(), perms).unwrap();

            let sock = dir.path().join("daemon.sock");
            let _listener = Listener::bind(&sock).await.unwrap();

            let mode = fs::metadata(dir.path()).unwrap().mode() & 0o777;
            assert_eq!(mode, 0o755, "existing dir mode should be preserved");
        });
    }
}
