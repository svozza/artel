//! PID file lifecycle: acquire, detect stale, release.
//!
//! [`PidFile::acquire`] is the entry point. On success, returns a guard
//! whose `Drop` removes the file if and only if it still names our PID
//! (i.e. a successor process didn't overwrite it). Failure modes:
//!
//! - [`PidError::AlreadyRunning`]: another live process holds the slot.
//!   Refuse to start.
//! - [`PidError::Io`]: filesystem-level failure.
//!
//! ## Locking protocol
//!
//! Ownership is an OS-level exclusive lock ([`std::fs::File::try_lock`],
//! i.e. `flock`) held on the PID file for the daemon's lifetime — not
//! the file's existence or contents. The kernel drops the lock when the
//! holder exits, even on SIGKILL, so a crashed daemon can never wedge
//! the slot and PID-liveness probing is only a fallback (for files left
//! by pre-lock daemon versions).
//!
//! The previous check-then-write protocol (`path.exists()` → rename
//! over it) had a cold-start race: two daemons starting in parallel
//! could both observe "no file", both write, and the last rename won —
//! leaving the file naming one daemon while the other ran unrecorded
//! (and unkillable-by-pidfile). The lock makes acquisition atomic:
//! exactly one contender wins; the loser gets `AlreadyRunning`.

use std::fs;
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::process;

use nix::errno::Errno;
use nix::sys::signal;
use nix::unistd::Pid;

/// Errors `PidFile::acquire` may return.
#[derive(Debug, thiserror::Error)]
pub enum PidError {
    /// Another live process holds the PID-file lock (or, for files
    /// left by pre-lock daemon versions, the recorded PID is a live
    /// process). `pid` is the owner recorded in the file, or `0` when
    /// the lock is held but the contents don't name a readable PID.
    #[error("daemon already running (pid {pid})")]
    AlreadyRunning {
        /// PID of the running daemon (`0` = owner unknown).
        pid: i32,
    },

    /// Filesystem or syscall error.
    #[error("pidfile io error: {0}")]
    Io(#[from] io::Error),
}

/// RAII handle for an acquired PID file.
///
/// Holds the OS lock (`file`) for the daemon's lifetime. On drop the
/// file is removed only if it still records `our_pid`, which avoids
/// clobbering a successor that took over the slot after we stopped.
#[derive(Debug)]
pub struct PidFile {
    path: PathBuf,
    our_pid: i32,
    /// The locked file handle. Never read after acquisition — held
    /// only so the OS lock lives exactly as long as the guard; dropping
    /// it releases the `flock`.
    _lock: fs::File,
    /// `true` while we still hold the file. Cleared by [`Self::release`].
    held: bool,
}

impl PidFile {
    /// Acquire the PID file at `path`.
    ///
    /// Opens (creating if absent) and takes an exclusive non-blocking
    /// lock on the file, then writes our PID into it. If the lock is
    /// held by another process, returns [`PidError::AlreadyRunning`]
    /// with the PID recorded in the file. A leftover file from a
    /// crashed or pre-lock daemon is unlocked, so acquisition simply
    /// succeeds and overwrites it — no liveness probing required.
    pub fn acquire(path: impl Into<PathBuf>) -> Result<Self, PidError> {
        let path = path.into();
        let our_pid = i32::try_from(process::id()).expect("pid fits in i32");

        // O_CREAT without O_TRUNC: never destroy an owner's contents
        // before we know we hold the lock.
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        match file.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                // Locked by a live owner. Report who from the file
                // contents, best-effort: the lock alone proves a live
                // owner, so unreadable/corrupt contents still mean
                // AlreadyRunning — `0` is documented as "unknown pid".
                let mut raw = String::new();
                let pid = match file.read_to_string(&mut raw) {
                    Ok(_) => raw.trim().parse::<i32>().unwrap_or(0),
                    Err(_) => 0,
                };
                return Err(PidError::AlreadyRunning { pid });
            }
            Err(fs::TryLockError::Error(err)) => return Err(PidError::Io(err)),
        }

        // Lock held: the slot is ours. If a pre-lock daemon version
        // left contents naming a live process, honor them — we must
        // not steal the slot out from under a daemon that predates
        // the locking protocol. (The lock alone can't see it.)
        let mut raw = String::new();
        file.read_to_string(&mut raw)?;
        let trimmed = raw.trim();
        if !trimmed.is_empty()
            && let Ok(existing) = trimmed.parse::<i32>()
            && existing != our_pid
            && pid_is_alive(existing)
        {
            return Err(PidError::AlreadyRunning { pid: existing });
        }

        // Rewrite in place. We hold the lock, so nobody else writes.
        file.seek(io::SeekFrom::Start(0))?;
        file.set_len(0)?;
        file.write_all(format!("{our_pid}\n").as_bytes())?;
        file.sync_all()?;

        Ok(Self {
            path,
            our_pid,
            _lock: file,
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
            PidError::Io(other) => panic!("expected AlreadyRunning, got {other:?}"),
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
    fn corrupt_unlocked_pid_file_is_reclaimed() {
        // Pre-lock-protocol semantics surfaced garbage contents as a
        // hard `Corrupt` error. Under the lock protocol, an unlocked
        // file has no live owner by definition — garbage contents are
        // just debris from a crash and the slot is safely reclaimed.
        let dir = tempdir().unwrap();
        let path = dir.path().join("daemon.pid");
        fs::write(&path, "not-a-number\n").unwrap();

        let pid = PidFile::acquire(&path).unwrap();
        let raw = fs::read_to_string(&path).unwrap();
        assert_eq!(raw.trim().parse::<i32>().unwrap(), pid.pid());
    }

    /// Regression for the parallel-cold-start orphan leak: two
    /// contenders racing `acquire` on the same cold path must settle
    /// on exactly one winner, and the file must name that winner.
    ///
    /// Pre-lock, both contenders could pass the `exists()` check, both
    /// write, and the last rename won — so the file could name a LOSER
    /// (who then died), leaving the winner running but unrecorded and
    /// unkillable via the pidfile. `flock` locks conflict across file
    /// descriptors even within one process, so racing threads exercise
    /// the same kernel arbitration as racing daemon processes.
    #[test]
    fn concurrent_acquires_settle_on_exactly_one_winner() {
        use std::sync::{Arc, Barrier};

        let dir = tempdir().unwrap();
        let path = dir.path().join("daemon.pid");

        let contenders = 8;
        let barrier = Arc::new(Barrier::new(contenders));
        let results: Vec<_> = (0..contenders)
            .map(|_| {
                let path = path.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    PidFile::acquire(&path)
                })
            })
            // Collect first so all threads SPAWN before any join —
            // joining inside one chain would serialize the contenders
            // and destroy the race this test exists to exercise.
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();
        let winners: Vec<&PidFile> = results.iter().filter_map(|r| r.as_ref().ok()).collect();
        let losers = results.iter().filter(|r| r.is_err()).count();

        assert_eq!(winners.len(), 1, "exactly one contender must win");
        assert_eq!(losers, contenders - 1);
        // The file names the winner (here: this process) and every
        // loser saw AlreadyRunning.
        let raw = fs::read_to_string(&path).unwrap();
        assert_eq!(raw.trim().parse::<i32>().unwrap(), winners[0].pid());
        for r in &results {
            if let Err(err) = r {
                assert!(matches!(err, PidError::AlreadyRunning { .. }), "{err:?}");
            }
        }
    }

    #[test]
    fn loser_does_not_clobber_winner_contents() {
        // The losing contender opens the file with O_CREAT but must
        // not truncate it — the winner's PID record has to survive.
        let dir = tempdir().unwrap();
        let path = dir.path().join("daemon.pid");

        let winner = PidFile::acquire(&path).unwrap();
        let before = fs::read_to_string(&path).unwrap();

        let err = PidFile::acquire(&path).unwrap_err();
        assert!(matches!(err, PidError::AlreadyRunning { .. }), "{err:?}");

        let after = fs::read_to_string(&path).unwrap();
        assert_eq!(before, after, "loser must not alter the pidfile");
        drop(winner);
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
