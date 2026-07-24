//! Auto-spawning the artel daemon from a client.
//!
//! [`Client::connect_or_spawn`] is the public surface; this module
//! holds the launch + handshake-wait logic.
//!
//! [`Client::connect_or_spawn`]: crate::Client::connect_or_spawn
//!
//! ## Lifecycle
//!
//! 1. Try [`Client::connect`]. If it succeeds, return it.
//! 2. If the failure is recoverable ([`is_no_daemon_error`]):
//!    - If the PID file names a live process, the daemon is mid-boot.
//!      Wait for the socket to appear and retry connect.
//!    - Otherwise, spawn the daemon binary detached and wait for the
//!      socket to appear, then retry connect.
//! 3. Stale socket recovery is handled by the daemon itself: once it
//!    holds the PID file lock, it removes any leftover socket file
//!    before binding. The client never deletes files.
//!
//! ## Concurrency
//!
//! Two parallel `connect_or_spawn` calls against the same cold dir
//! both try to spawn the daemon. Only one wins the pidfile
//! contention (artel-daemon's `pidfile` module) — the other observes
//! `AlreadyRunning` and exits. Both clients then connect to the
//! survivor.
//!
//! [`Client::connect`]: crate::Client::connect

// Inside a crate-private module we reach for `pub(crate)` to be
// explicit; clippy's nursery `redundant_pub_crate` and rustc's
// `unreachable_pub` disagree about that. See
// `feedback_clippy_lint_conflict` in memory.
#![allow(clippy::redundant_pub_crate)]

use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use artel_protocol::transport::TransportError;
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::Client;
use crate::error::ClientError;

/// How long to wait for the daemon's socket to appear after spawn,
/// before giving up.
const DEFAULT_SPAWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Polling interval for the socket to appear / become connectable.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Configuration for [`Client::connect_or_spawn`].
///
/// Per-field defaults are conservative: the caller must supply the
/// daemon binary path explicitly so we never silently launch a
/// `$PATH`-resolved binary that may not be the one they meant.
///
/// [`Client::connect_or_spawn`]: crate::Client::connect_or_spawn
#[derive(Debug, Clone)]
pub struct SpawnOptions {
    /// IPC socket path the daemon will bind.
    pub socket_path: PathBuf,
    /// PID file path. Read (never written) by the client to
    /// distinguish "no daemon" from "daemon is still booting" —
    /// see `pidfile_names_live_process`.
    pub pid_path: PathBuf,
    /// Path to the `artel-daemon` binary. Required: the client refuses
    /// to guess.
    pub daemon_binary: PathBuf,
    /// Extra arguments passed to the daemon. The client always supplies
    /// `--socket` and `--state-dir`-equivalent flags via `socket_path`
    /// and `pid_path`; these args are appended after.
    pub extra_args: Vec<String>,
    /// Extra environment variables passed to the spawned daemon process.
    pub extra_envs: Vec<(String, String)>,
    /// How long to wait for the daemon to come up after spawn.
    pub spawn_timeout: Duration,
}

impl SpawnOptions {
    /// Build an options struct with the three required paths and
    /// default timeout / no extra args.
    #[must_use]
    pub fn new(
        socket_path: impl Into<PathBuf>,
        pid_path: impl Into<PathBuf>,
        daemon_binary: impl Into<PathBuf>,
    ) -> Self {
        Self {
            socket_path: socket_path.into(),
            pid_path: pid_path.into(),
            daemon_binary: daemon_binary.into(),
            extra_args: Vec::new(),
            extra_envs: Vec::new(),
            spawn_timeout: DEFAULT_SPAWN_TIMEOUT,
        }
    }

    /// Replace [`Self::extra_args`].
    #[must_use]
    pub fn with_args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.extra_args = args.into_iter().map(Into::into).collect();
        self
    }

    /// Replace [`Self::extra_envs`].
    #[must_use]
    pub fn with_envs<I, K, V>(mut self, envs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.extra_envs = envs
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        self
    }

    /// Replace [`Self::spawn_timeout`].
    #[must_use]
    pub const fn with_spawn_timeout(mut self, timeout: Duration) -> Self {
        self.spawn_timeout = timeout;
        self
    }
}

/// Errors specific to the auto-spawn path.
///
/// Wrapped into [`ClientError::Spawn`] so callers see one error type.
#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    /// The daemon binary did not exist or was not executable.
    #[error("daemon binary {path} could not be launched: {source}")]
    Launch {
        /// Binary path the client tried to run.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Spawned the daemon, but the socket never became connectable
    /// within [`SpawnOptions::spawn_timeout`].
    #[error("daemon did not become reachable within {timeout:?} (socket {socket})")]
    Timeout {
        /// Socket path that was being polled.
        socket: PathBuf,
        /// Timeout that elapsed.
        timeout: Duration,
    },
}

/// Top-level entry point. See [`Client::connect_or_spawn`].
pub(crate) async fn connect_or_spawn(opts: SpawnOptions) -> Result<Client, ClientError> {
    match Client::connect(&opts.socket_path).await {
        Ok(c) => return Ok(c),
        Err(err) if !is_no_daemon_error(&err) => return Err(err),
        Err(err) => debug!(error = %err, "initial connect failed; trying to spawn daemon"),
    }

    if pidfile_names_live_process(&opts.pid_path) {
        // Daemon is mid-boot or briefly unreachable. Don't spawn —
        // just wait for it to come up.
        debug!(
            pid = %opts.pid_path.display(),
            "pid file names a live process; waiting for socket",
        );
    } else {
        spawn_detached(&opts)?;
    }

    wait_for_socket(&opts.socket_path, opts.spawn_timeout).await?;
    Client::connect(&opts.socket_path).await
}

/// Whether `err` indicates "the daemon isn't there yet" (worth trying
/// to spawn) versus a hard failure (caller's problem).
///
/// The recoverable cases come back via [`TransportError::Io`]:
/// - [`io::ErrorKind::NotFound`] — socket file absent.
/// - [`io::ErrorKind::ConnectionRefused`] — socket exists, no
///   listener (daemon crashed without cleanup).
/// - `ENOTSOCK` (via `nix::errno::Errno`, so per-platform: 38 on
///   macOS, 88 on Linux) — a file exists at the socket path but
///   isn't a socket. Surfaces on the [`io::ErrorKind::Uncategorized`]
///   bucket on stable Rust. Daemon-side cleanup will replace it.
fn is_no_daemon_error(err: &ClientError) -> bool {
    let ClientError::Transport(TransportError::Io(io_err)) = err else {
        return false;
    };
    is_io_recoverable(io_err)
}

fn is_io_recoverable(err: &io::Error) -> bool {
    if matches!(
        err.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
    ) {
        return true;
    }
    err.raw_os_error() == Some(nix::errno::Errno::ENOTSOCK as i32)
}

/// Read `path`, parse a PID, return whether that PID is currently a
/// live process. Best-effort: any I/O or parse failure → `false`.
fn pidfile_names_live_process(path: &Path) -> bool {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return false;
    };
    let Ok(pid) = raw.trim().parse::<i32>() else {
        return false;
    };
    pid_is_alive(pid)
}

/// `kill(pid, 0)`-style probe. EPERM also counts as "alive", since the
/// process exists even if we can't signal it.
fn pid_is_alive(pid: i32) -> bool {
    use nix::errno::Errno;
    use nix::sys::signal;
    use nix::unistd::Pid;
    if pid <= 0 {
        return false;
    }
    matches!(
        signal::kill(Pid::from_raw(pid), None),
        Ok(()) | Err(Errno::EPERM)
    )
}

/// Spawn `daemon_binary` detached from the current process so it
/// outlives the caller.
///
/// Detachment uses [`Command::process_group`] (Unix-only, stable
/// since Rust 1.64) to put the child into its own process group, so
/// signals delivered to the parent's group do not reach the child.
/// stdio is redirected to `/dev/null` so the daemon never blocks on
/// the parent's terminal.
fn spawn_detached(opts: &SpawnOptions) -> Result<(), ClientError> {
    use std::os::unix::process::CommandExt;

    let mut cmd = Command::new(&opts.daemon_binary);
    cmd.arg("--socket")
        .arg(&opts.socket_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0);

    // The daemon resolves the PID path from --state-dir or its default.
    // We pass the parent of the requested PID path as state-dir so the
    // daemon writes its bookkeeping in the location the caller pinned,
    // even when that location is not under $HOME/.artel.
    if let Some(parent) = opts.pid_path.parent() {
        cmd.arg("--state-dir").arg(parent);
    }

    for a in &opts.extra_args {
        cmd.arg(a);
    }

    cmd.envs(opts.extra_envs.iter().map(|(k, v)| (k, v)));

    debug!(
        binary = %opts.daemon_binary.display(),
        socket = %opts.socket_path.display(),
        "spawning daemon",
    );
    let child = cmd.spawn().map_err(|source| {
        ClientError::Spawn(SpawnError::Launch {
            path: opts.daemon_binary.clone(),
            source,
        })
    })?;
    reap_in_background(child);
    Ok(())
}

/// Wait on `child` from a detached background thread so it never
/// becomes a zombie.
///
/// The daemon is meant to outlive this process — we don't want to
/// block `connect_or_spawn` on its exit, or hold any tokio resources
/// for however long it runs (hours to indefinitely). But `spawn()`'s
/// returned [`std::process::Child`] must still be waited on by
/// *someone*, or an exited child (e.g. a fast crash from a bad
/// `--state-dir` or a startup permission error, surfaced separately
/// to the caller as [`SpawnError::Timeout`] once `wait_for_socket`
/// gives up) leaves an entry in the process table for the rest of
/// this process's lifetime. A plain OS thread doing a blocking
/// `wait()` is the whole fix: it costs one thread for the life of the
/// child, exits the moment the child does, and needs no tokio
/// runtime or executor feature.
fn reap_in_background(mut child: std::process::Child) {
    std::thread::spawn(move || match child.wait() {
        Ok(status) => debug!(%status, "spawned daemon process exited; reaped"),
        Err(err) => warn!(%err, "failed to reap spawned daemon process"),
    });
}

/// Poll for `socket` to become connectable, up to `timeout`. We don't
/// just wait for the file to appear: `bind()` races with our
/// `UnixStream::connect`, and on macOS the socket can briefly exist
/// without accepting connections.
async fn wait_for_socket(socket: &Path, timeout: Duration) -> Result<(), ClientError> {
    let deadline = Instant::now() + timeout;
    loop {
        match tokio::net::UnixStream::connect(socket).await {
            Ok(_) => return Ok(()),
            Err(err) if is_socket_pending(&err) => {
                if Instant::now() >= deadline {
                    warn!(socket = %socket.display(), "daemon failed to come up in time");
                    return Err(ClientError::Spawn(SpawnError::Timeout {
                        socket: socket.to_path_buf(),
                        timeout,
                    }));
                }
                sleep(POLL_INTERVAL).await;
            }
            Err(err) => {
                return Err(ClientError::Transport(TransportError::Io(err)));
            }
        }
    }
}

/// Whether `err` from `UnixStream::connect` is "still booting" (we
/// should retry) versus a real failure (give up).
fn is_socket_pending(err: &io::Error) -> bool {
    is_io_recoverable(err)
}

#[cfg(test)]
mod tests {
    use std::process;

    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn pid_is_alive_self_is_true() {
        let me = i32::try_from(process::id()).unwrap();
        assert!(pid_is_alive(me));
    }

    #[test]
    fn pid_is_alive_zero_and_negative_are_false() {
        assert!(!pid_is_alive(0));
        assert!(!pid_is_alive(-1));
    }

    #[test]
    fn pidfile_names_live_process_missing_file_is_false() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("daemon.pid");
        assert!(!pidfile_names_live_process(&p));
    }

    #[test]
    fn pidfile_names_live_process_self_is_true() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("daemon.pid");
        std::fs::write(&p, format!("{}\n", process::id())).unwrap();
        assert!(pidfile_names_live_process(&p));
    }

    #[test]
    fn pidfile_names_live_process_bogus_contents_is_false() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("daemon.pid");
        std::fs::write(&p, b"not a pid\n").unwrap();
        assert!(!pidfile_names_live_process(&p));
    }

    #[test]
    fn pidfile_names_live_process_dead_pid_is_false() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("daemon.pid");
        // Spawn and immediately reap.
        let mut child = process::Command::new("true").spawn().unwrap();
        let dead = i32::try_from(child.id()).unwrap();
        child.wait().unwrap();
        std::fs::write(&p, format!("{dead}\n")).unwrap();
        assert!(!pidfile_names_live_process(&p));
    }

    /// The bug this module's fix closes: a spawned child that exits
    /// quickly must not sit in the process table as a zombie for the
    /// rest of this process's lifetime. `reap_in_background` waits on
    /// it from a dedicated thread; poll `ps`'s reported state for the
    /// child's pid until it's no longer `Z` (zombie) or the pid is
    /// gone entirely (already reaped and recycled).
    #[test]
    fn reap_in_background_clears_a_fast_exiting_child() {
        let child = process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        reap_in_background(child);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let state = ps_state(pid);
            match state.as_deref() {
                None => break,                        // pid no longer listed at all: reaped.
                Some(s) if !s.contains('Z') => break, // no longer a zombie.
                _ => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "pid {pid} was still a zombie after 5s — reap_in_background \
                         did not wait() it",
                    );
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            }
        }
    }

    /// `ps`'s reported process state for `pid`, or `None` if `ps`
    /// no longer lists it (already reaped by *some* waiter — the OS
    /// recycles pids, so absence is as conclusive as a non-`Z` state).
    fn ps_state(pid: u32) -> Option<String> {
        let output = process::Command::new("ps")
            .args(["-o", "state=", "-p", &pid.to_string()])
            .output()
            .expect("ps must be runnable in the test environment");
        if !output.status.success() {
            return None;
        }
        let state = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if state.is_empty() { None } else { Some(state) }
    }

    #[test]
    fn is_no_daemon_error_classifies_io_kinds() {
        let not_found =
            ClientError::Transport(TransportError::Io(io::Error::from(io::ErrorKind::NotFound)));
        let refused = ClientError::Transport(TransportError::Io(io::Error::from(
            io::ErrorKind::ConnectionRefused,
        )));
        let other = ClientError::Transport(TransportError::Io(io::Error::from(
            io::ErrorKind::PermissionDenied,
        )));
        let not_a_socket = ClientError::Transport(TransportError::Io(
            io::Error::from_raw_os_error(nix::errno::Errno::ENOTSOCK as i32),
        ));
        assert!(is_no_daemon_error(&not_found));
        assert!(is_no_daemon_error(&refused));
        assert!(is_no_daemon_error(&not_a_socket));
        assert!(!is_no_daemon_error(&other));
        assert!(!is_no_daemon_error(&ClientError::ConnectionClosed));
    }

    #[test]
    fn is_io_recoverable_classifies_error_kinds() {
        let not_found = io::Error::from(io::ErrorKind::NotFound);
        let refused = io::Error::from(io::ErrorKind::ConnectionRefused);
        let other = io::Error::from(io::ErrorKind::PermissionDenied);
        let not_a_socket = io::Error::from_raw_os_error(nix::errno::Errno::ENOTSOCK as i32);
        assert!(is_io_recoverable(&not_found));
        assert!(is_io_recoverable(&refused));
        assert!(is_io_recoverable(&not_a_socket));
        assert!(!is_io_recoverable(&other));
    }

    #[test]
    fn is_socket_pending_delegates_to_is_io_recoverable() {
        // `is_socket_pending` is a thin alias used at the `connect()`
        // retry site in `wait_for_socket` — pin that it shares
        // `is_io_recoverable`'s classification rather than drifting.
        let refused = io::Error::from(io::ErrorKind::ConnectionRefused);
        let other = io::Error::from(io::ErrorKind::PermissionDenied);
        assert!(is_socket_pending(&refused));
        assert!(!is_socket_pending(&other));
    }

    #[tokio::test]
    async fn wait_for_socket_returns_timeout_when_socket_never_appears() {
        let dir = tempdir().unwrap();
        let sock = dir.path().join("never.sock");
        let err = wait_for_socket(&sock, Duration::from_millis(100))
            .await
            .unwrap_err();
        match err {
            ClientError::Spawn(SpawnError::Timeout { socket, .. }) => {
                assert_eq!(socket, sock);
            }
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    #[test]
    fn spawn_options_builder_sets_fields() {
        // Use a real flag the daemon CLI accepts so anyone copying
        // this snippet into a live `Client::connect_or_spawn` call
        // doesn't trip a clap rejection. Daemon CLI flags as of HEAD:
        // --socket, --state-dir, --log.
        let opts = SpawnOptions::new("/tmp/sock", "/tmp/pid", "/usr/local/bin/artel-daemon")
            .with_args(["--log", "debug"])
            .with_spawn_timeout(Duration::from_secs(10));
        assert_eq!(opts.socket_path, PathBuf::from("/tmp/sock"));
        assert_eq!(opts.pid_path, PathBuf::from("/tmp/pid"));
        assert_eq!(
            opts.daemon_binary,
            PathBuf::from("/usr/local/bin/artel-daemon"),
        );
        assert_eq!(opts.extra_args, vec!["--log", "debug"]);
        assert_eq!(opts.spawn_timeout, Duration::from_secs(10));
    }
}
