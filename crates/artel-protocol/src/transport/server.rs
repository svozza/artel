//! Daemon-side socket binding and accept loop.

use std::fs;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
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

/// Group- and other-writable permission bits (`0o022`).
///
/// A state directory carrying either bit lets a user who is not the
/// owner create, replace, or unlink files inside it — including the IPC
/// socket. That defeats the socket's own `0600` mode (the attacker
/// substitutes their own socket rather than connecting to ours), so we
/// refuse to run under such a directory.
const WORLD_GROUP_WRITABLE: u32 = 0o022;

/// A bound Unix-socket listener for the artel IPC.
///
/// On drop, the socket file is removed **iff** it still points at the
/// exact inode this listener bound — a replacement socket dropped in by
/// another process is left alone. The parent directory is left in place
/// — it is shared with the daemon's PID file, log, etc.
#[derive(Debug)]
pub struct Listener {
    inner: UnixListener,
    path: PathBuf,
    /// `(device, inode)` of the socket file as bound. Drop compares the
    /// on-disk file against this before unlinking, so a socket swapped
    /// in under our path (e.g. after a crash + restart race) is not
    /// removed out from under its real owner.
    identity: (u64, u64),
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
    /// Returns an [`io::Error`] if creating the parent directory fails;
    /// if an existing parent directory is not a private, owner-owned
    /// directory ([`io::ErrorKind::PermissionDenied`] — see `ensure_dir`
    /// for the exact ownership/permission rules); if binding the socket
    /// fails (notably
    /// [`io::ErrorKind::AddrInUse`] when a stale or live socket already
    /// occupies `path`); or if reading/setting the socket's permissions
    /// to `0600` fails.
    pub async fn bind(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();

        if let Some(parent) = path.parent() {
            ensure_dir(parent).await?;
        }

        let listener = UnixListener::bind(&path)?;

        // Tighten perms after bind. Failure here is fatal: a world-
        // readable socket would defeat the access control. The brief
        // window between bind and chmod is only reachable by someone who
        // can already write the parent directory — which `ensure_dir`
        // has just refused to run under (owner-only, not group/other
        // writable), so the window is not exploitable.
        let mut perms = fs::metadata(&path)?.permissions();
        perms.set_mode(SOCKET_MODE);
        fs::set_permissions(&path, perms)?;

        // Record the bound socket's identity so Drop only unlinks *this*
        // socket, never a replacement swapped in under our path.
        let meta = fs::metadata(&path)?;
        let identity = (meta.dev(), meta.ino());

        Ok(Self {
            inner: listener,
            path,
            identity,
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
        // Best-effort: remove the socket file, but ONLY if the file at
        // our path is still the exact inode we bound. If another process
        // has since replaced it (crash + restart race, or a hostile
        // swap), the `(dev, ino)` won't match and we leave it alone —
        // unlinking it would destroy a socket we don't own. We don't
        // surface errors: drop can't, and the next startup recovers a
        // genuinely stale file.
        match fs::metadata(&self.path) {
            Ok(meta) if (meta.dev(), meta.ino()) == self.identity => {
                let _ = fs::remove_file(&self.path);
            }
            _ => {}
        }
    }
}

/// Ensure `dir` is a private, owner-controlled directory, creating it at
/// mode `0700` if it does not yet exist.
///
/// When `dir` is created here it gets `0700` (owner-only). When it
/// already exists we do **not** rewrite its mode — the user may have a
/// legitimate preference — but we *validate* that it is safe to host the
/// IPC socket, and refuse otherwise:
///
/// - it must be owned by the current effective user, and
/// - it must not be group- or other-writable.
///
/// A directory another user can write lets them unlink our `0600` socket
/// and substitute their own, so the socket's mode alone is not enough —
/// the containing directory must also be locked down. This matters most
/// for headless deployments pointing `--state-dir` at a shared location;
/// the default `~/.artel` is already private via the home directory.
///
/// # Errors
///
/// - [`io::ErrorKind::AlreadyExists`] if `dir` exists but is not a
///   directory.
/// - [`io::ErrorKind::PermissionDenied`] if an existing `dir` is not
///   owned by the current user, or is group/other-writable.
/// - Any other [`io::Error`] from the underlying metadata / create /
///   chmod calls.
async fn ensure_dir(dir: &Path) -> io::Result<()> {
    match tokio::fs::metadata(dir).await {
        Ok(meta) if meta.is_dir() => validate_existing_dir(dir, &meta),
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

/// Reject an existing state directory that isn't safe to host the IPC
/// socket: it must be owned by the current effective user and must not
/// be group- or other-writable. See [`ensure_dir`].
fn validate_existing_dir(dir: &Path, meta: &fs::Metadata) -> io::Result<()> {
    let current_uid = nix::unistd::Uid::effective().as_raw();
    if meta.uid() != current_uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{} is owned by uid {}, not the current user (uid {current_uid}); \
                 refusing to bind the IPC socket in a directory we do not own",
                dir.display(),
                meta.uid(),
            ),
        ));
    }
    let mode = meta.mode();
    if mode & WORLD_GROUP_WRITABLE != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{} is group- or other-writable (mode {:o}); \
                 another user could replace the IPC socket. Restrict it to 0700",
                dir.display(),
                mode & 0o777,
            ),
        ));
    }
    Ok(())
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
            // Set parent to a different (but still owner-only, non-
            // group/other-writable) mode and confirm we don't touch it.
            let mut perms = fs::metadata(dir.path()).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(dir.path(), perms).unwrap();

            let sock = dir.path().join("daemon.sock");
            let _listener = Listener::bind(&sock).await.unwrap();

            let mode = fs::metadata(dir.path()).unwrap().mode() & 0o777;
            assert_eq!(mode, 0o755, "existing dir mode should be preserved");
        });
    }

    #[test]
    fn bind_rejects_group_writable_existing_dir() {
        rt().block_on(async {
            let dir = tempdir().unwrap();
            // Group- and world-writable: another user could unlink our
            // socket and drop in their own. bind must refuse.
            let mut perms = fs::metadata(dir.path()).unwrap().permissions();
            perms.set_mode(0o777);
            fs::set_permissions(dir.path(), perms).unwrap();

            let sock = dir.path().join("daemon.sock");
            let err = Listener::bind(&sock).await.unwrap_err();
            assert_eq!(
                err.kind(),
                io::ErrorKind::PermissionDenied,
                "expected PermissionDenied for a world-writable state dir, got {err:?}",
            );
            assert!(!sock.exists(), "no socket should be bound on rejection");
        });
    }

    #[test]
    fn bind_rejects_other_writable_existing_dir() {
        rt().block_on(async {
            let dir = tempdir().unwrap();
            // Owner rwx + other write bit only (0o702): still lets a
            // non-owner replace the socket, so it must be refused.
            let mut perms = fs::metadata(dir.path()).unwrap().permissions();
            perms.set_mode(0o702);
            fs::set_permissions(dir.path(), perms).unwrap();

            let sock = dir.path().join("daemon.sock");
            let err = Listener::bind(&sock).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied, "got {err:?}");
        });
    }

    #[test]
    fn drop_leaves_a_replacement_socket_in_place() {
        rt().block_on(async {
            let dir = tempdir().unwrap();
            let sock = dir.path().join("daemon.sock");

            // Bind, then replace the socket file with a different inode
            // (simulating a crash + restart race or a hostile swap). Drop
            // must NOT unlink the replacement — it isn't ours.
            let replacement_ino = {
                let listener = Listener::bind(&sock).await.unwrap();
                let bound_ino = fs::metadata(&sock).unwrap().ino();

                fs::remove_file(&sock).unwrap();
                let other = Listener::bind(&sock).await.unwrap();
                let replacement_ino = fs::metadata(&sock).unwrap().ino();
                assert_ne!(bound_ino, replacement_ino, "replacement is a new inode");

                // Keep `other`'s socket file alive past the first
                // listener's drop by forgetting it (so its own Drop
                // doesn't remove the file we're asserting on).
                std::mem::forget(other);
                drop(listener); // first listener drops here
                replacement_ino
            };

            assert!(
                sock.exists(),
                "the replacement socket must survive the original listener's drop",
            );
            assert_eq!(
                fs::metadata(&sock).unwrap().ino(),
                replacement_ino,
                "the surviving file must be the replacement, not a re-created stale one",
            );
        });
    }

    #[test]
    fn drop_removes_our_own_socket() {
        rt().block_on(async {
            let dir = tempdir().unwrap();
            let sock = dir.path().join("daemon.sock");
            {
                let _listener = Listener::bind(&sock).await.unwrap();
                assert!(sock.exists());
            }
            assert!(
                !sock.exists(),
                "drop should remove the socket it actually bound",
            );
        });
    }

    #[test]
    fn bind_rejects_parent_path_that_is_a_regular_file() {
        rt().block_on(async {
            let dir = tempdir().unwrap();
            // The socket's intended parent directory is actually a
            // regular file: ensure_dir must refuse rather than trying
            // to create a directory chain through it.
            let not_a_dir = dir.path().join("not-a-dir");
            fs::write(&not_a_dir, b"x").unwrap();
            let sock = not_a_dir.join("daemon.sock");

            let err = Listener::bind(&sock).await.unwrap_err();
            assert_eq!(
                err.kind(),
                io::ErrorKind::AlreadyExists,
                "expected AlreadyExists for a non-directory parent, got {err:?}",
            );
        });
    }
}
