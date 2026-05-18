//! PID file lifecycle: acquire, detect stale, release.
//!
//! [`PidFile::acquire`] is the entry point. On success, returns a guard
//! whose `Drop` removes the file if and only if it still names our PID
//! (i.e. a successor process didn't overwrite it). Failure modes:
//!
//! - [`PidError::AlreadyRunning`]: the file exists and contains a live
//!   PID. Refuse to start.
//! - [`PidError::Io`]: filesystem-level failure.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process;

use nix::errno::Errno;
use nix::sys::signal;
use nix::unistd::Pid;

/// Errors `PidFile::acquire` may return.
#[derive(Debug, thiserror::Error)]
pub enum PidError {
    /// The PID file exists and the recorded PID is a live process.
    #[error("daemon already running (pid {pid})")]
    AlreadyRunning {
        /// PID of the running daemon.
        pid: i32,
    },

    /// Filesystem or syscall error.
    #[error("pidfile io error: {0}")]
    Io(#[from] io::Error),

    /// PID file exists but its contents are not parseable as a PID.
    #[error("pidfile is corrupt: {0}")]
    Corrupt(String),
}

/// RAII handle for an acquired PID file.
///
/// On drop the file is removed only if it still records `our_pid`, which
/// avoids clobbering a successor that took over the slot after we
/// stopped.
#[derive(Debug)]
pub struct PidFile {
    path: PathBuf,
    our_pid: i32,
    /// `Some` while we still hold the file. Cleared by [`Self::release`].
    held: bool,
}

impl PidFile {
    /// Acquire the PID file at `path`.
    ///
    /// If the file exists and the recorded PID is alive, returns
    /// [`PidError::AlreadyRunning`]. If the recorded PID is dead, the
    /// stale file is overwritten and acquisition proceeds.
    pub fn acquire(path: impl Into<PathBuf>) -> Result<Self, PidError> {
        let path = path.into();
        let our_pid = i32::try_from(process::id()).expect("pid fits in i32");

        if path.exists() {
            let raw = fs::read_to_string(&path)?;
            let trimmed = raw.trim();
            let existing: i32 = trimmed
                .parse()
                .map_err(|_| PidError::Corrupt(format!("not a PID: {trimmed:?}")))?;
            if pid_is_alive(existing) {
                return Err(PidError::AlreadyRunning { pid: existing });
            }
            // Stale: fall through and overwrite.
        }

        write_atomic(&path, &format!("{our_pid}\n"))?;

        Ok(Self {
            path,
            our_pid,
            held: true,
        })
    }

    /// Path the PID file lives at.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// PID recorded in the file (always the current process).
    #[must_use]
    pub const fn pid(&self) -> i32 {
        self.our_pid
    }

    /// Explicitly release. Equivalent to dropping but lets callers
    /// surface I/O errors.
    pub fn release(mut self) -> io::Result<()> {
        self.do_release()
    }

    fn do_release(&mut self) -> io::Result<()> {
        if !self.held {
            return Ok(());
        }
        self.held = false;
        // Only remove if the file still names us, so we don't clobber a
        // process that took over the slot.
        match fs::read_to_string(&self.path) {
            Ok(raw) => {
                if raw.trim().parse::<i32>().ok() == Some(self.our_pid)
                    && let Err(err) = fs::remove_file(&self.path)
                    && err.kind() != io::ErrorKind::NotFound
                {
                    return Err(err);
                }
                Ok(())
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }
}

impl Drop for PidFile {
    fn drop(&mut self) {
        // Best-effort: surface nothing.
        let _ = self.do_release();
    }
}

/// Check if `pid` names a process this user can signal.
///
/// `kill(pid, 0)` returns:
/// - `Ok(())` if the process exists and we may signal it
/// - `Err(EPERM)` if the process exists but belongs to another user
/// - `Err(ESRCH)` if no such process
///
/// Both `Ok` and `EPERM` are treated as "alive" — we don't want to
/// proceed against a daemon we can't even signal.
fn pid_is_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    match signal::kill(Pid::from_raw(pid), None) {
        Ok(()) | Err(Errno::EPERM) => true,
        Err(_) => false,
    }
}

/// Write `content` to `path` atomically: write to `path.tmp`, then rename.
fn write_atomic(path: &Path, content: &str) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn acquire_writes_current_pid() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("daemon.pid");

        let pid = PidFile::acquire(&path).unwrap();
        let raw = fs::read_to_string(&path).unwrap();
        assert_eq!(raw.trim().parse::<i32>().unwrap(), pid.pid());
        assert_eq!(pid.pid(), i32::try_from(process::id()).unwrap());
    }

    #[test]
    fn drop_removes_file_when_we_still_own_it() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("daemon.pid");
        {
            let _pid = PidFile::acquire(&path).unwrap();
            assert!(path.exists());
        }
        assert!(!path.exists(), "drop should remove the file");
    }

    #[test]
    fn release_returns_ok_and_removes_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("daemon.pid");
        let pid = PidFile::acquire(&path).unwrap();
        pid.release().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn drop_does_not_remove_file_owned_by_another_pid() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("daemon.pid");

        let pid = PidFile::acquire(&path).unwrap();
        // Simulate a successor: overwrite the file with someone else's
        // PID. Use 1, which is always present (init / launchd).
        fs::write(&path, "1\n").unwrap();
        drop(pid);

        // The successor's record is preserved.
        let raw = fs::read_to_string(&path).unwrap();
        assert_eq!(raw.trim(), "1");
    }

    #[test]
    fn second_acquire_against_live_pid_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("daemon.pid");
        let _first = PidFile::acquire(&path).unwrap();

        let err = PidFile::acquire(&path).unwrap_err();
        match err {
            PidError::AlreadyRunning { pid } => {
                assert_eq!(pid, i32::try_from(process::id()).unwrap());
            }
            other => panic!("expected AlreadyRunning, got {other:?}"),
        }
    }

    #[test]
    fn stale_pid_file_is_overwritten() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("daemon.pid");

        // Spawn a short-lived process and capture its PID. By the time
        // we record it, `wait` has reaped it and the kernel has freed
        // the slot, so kill(pid, 0) returns ESRCH.
        let mut child = Command::new("true").spawn().unwrap();
        let dead_pid = i32::try_from(child.id()).unwrap();
        child.wait().unwrap();
        // A short sleep would reduce the (small) chance of the same PID
        // being reused, but on a quiet test runner this is fine.
        fs::write(&path, format!("{dead_pid}\n")).unwrap();

        let pid = PidFile::acquire(&path).unwrap();
        assert_eq!(pid.pid(), i32::try_from(process::id()).unwrap());
        // File now records us, not the dead process.
        let raw = fs::read_to_string(&path).unwrap();
        assert_eq!(raw.trim().parse::<i32>().unwrap(), pid.pid());
    }

    #[test]
    fn corrupt_pid_file_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("daemon.pid");
        fs::write(&path, "not-a-number\n").unwrap();

        let err = PidFile::acquire(&path).unwrap_err();
        assert!(matches!(err, PidError::Corrupt(_)), "got {err:?}");
    }

    #[test]
    fn pid_is_alive_self_is_true() {
        let me = i32::try_from(process::id()).unwrap();
        assert!(pid_is_alive(me));
    }

    #[test]
    fn pid_is_alive_zero_is_false() {
        // PID 0 is the kernel scheduler / process group, never a real
        // user process.
        assert!(!pid_is_alive(0));
    }

    #[test]
    fn pid_is_alive_negative_is_false() {
        assert!(!pid_is_alive(-1));
        assert!(!pid_is_alive(i32::MIN));
    }
}
