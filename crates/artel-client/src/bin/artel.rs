//! `artel` — command-line client for the artel daemon.
//!
//! Three subcommands:
//!
//! - `status`: probe the daemon, print version + peer id.
//! - `stop`: signal the daemon to shut down (SIGTERM, or SIGKILL with
//!   `--force`).
//! - `list`: ask the daemon for active session summaries.
//!
//! All accept `--socket` / `--state-dir` for path overrides; `status`
//! and `list` accept `--json` for orchestrator-friendly output.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use artel_client::{Client, ClientError, SpawnOptions};
use artel_protocol::transport::path::{default_pid_path, default_socket_path};
use artel_protocol::{Request, Response, SessionSummary};
use clap::{Args, Parser, Subcommand};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(
    name = "artel",
    version,
    about = "Command-line client for the artel daemon"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Show whether the daemon is reachable.
    Status(StatusArgs),
    /// Signal the daemon to shut down.
    Stop(StopArgs),
    /// List active sessions.
    List(ListArgs),
}

#[derive(Debug, Clone, Args)]
struct ConnectionArgs {
    /// IPC socket path. Default: `~/.artel/daemon.sock` (or
    /// `$ARTEL_HOME/daemon.sock`).
    #[arg(long)]
    socket: Option<PathBuf>,

    /// State directory override. Implies socket = `<dir>/daemon.sock`
    /// and pid = `<dir>/daemon.pid`.
    #[arg(long)]
    state_dir: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct StatusArgs {
    #[command(flatten)]
    conn: ConnectionArgs,
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct StopArgs {
    #[command(flatten)]
    conn: ConnectionArgs,
    /// Send SIGKILL instead of SIGTERM.
    #[arg(long)]
    force: bool,
    /// Seconds to wait for the daemon to exit before reporting timeout.
    #[arg(long, default_value = "5")]
    timeout_secs: u64,
}

#[derive(Debug, Args)]
struct ListArgs {
    #[command(flatten)]
    conn: ConnectionArgs,
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
    #[command(flatten)]
    spawn: SpawnArgs,
}

#[derive(Debug, Clone, Args)]
struct SpawnArgs {
    /// If set and the daemon is not running, spawn one before
    /// connecting.
    #[arg(long)]
    auto_spawn: bool,

    /// Path to the `artel-daemon` binary used by `--auto-spawn`. When
    /// omitted, the CLI looks up `artel-daemon` on `$PATH`.
    #[arg(long, requires = "auto_spawn")]
    daemon_binary: Option<PathBuf>,
}

impl SpawnArgs {
    /// Resolve the daemon binary path from the CLI flags, falling
    /// back to `$PATH`. Returns `Ok(None)` when `--auto-spawn` was
    /// not requested.
    fn resolve_daemon_binary(&self) -> Result<Option<PathBuf>, String> {
        if !self.auto_spawn {
            return Ok(None);
        }
        if let Some(p) = &self.daemon_binary {
            return Ok(Some(p.clone()));
        }
        which("artel-daemon")
            .map(Some)
            .map_err(|e| format!("locate artel-daemon: {e}"))
    }
}

/// Find an executable on `$PATH` by name. Minimal, intentionally
/// dumb: walks `$PATH` and returns the first hit that exists and is
/// a file. Doesn't probe the exec bit; the spawn step will surface a
/// permission error if it isn't executable.
fn which(name: &str) -> Result<PathBuf, String> {
    let path = std::env::var_os("PATH").ok_or_else(|| "$PATH is not set".to_string())?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(format!("{name} not found on $PATH"))
}

fn resolve_paths(args: &ConnectionArgs) -> Result<(PathBuf, PathBuf), String> {
    if let Some(dir) = &args.state_dir {
        let socket = args
            .socket
            .clone()
            .unwrap_or_else(|| dir.join("daemon.sock"));
        return Ok((socket, dir.join("daemon.pid")));
    }
    let socket = match &args.socket {
        Some(p) => p.clone(),
        None => default_socket_path().map_err(|e| format!("resolve socket path: {e}"))?,
    };
    let pid = default_pid_path().map_err(|e| format!("resolve pid path: {e}"))?;
    Ok((socket, pid))
}

#[derive(Debug, Serialize)]
struct StatusJson {
    running: bool,
    socket: PathBuf,
    daemon_version: Option<u32>,
    daemon_peer_id: Option<String>,
    error: Option<String>,
}

async fn status(args: StatusArgs) -> ExitCode {
    let (socket, _pid) = match resolve_paths(&args.conn) {
        Ok(p) => p,
        Err(err) => {
            eprintln!("artel: {err}");
            return ExitCode::from(2);
        }
    };

    let outcome = Client::connect(&socket).await;

    if args.json {
        let json = match &outcome {
            Ok(client) => StatusJson {
                running: true,
                socket: socket.clone(),
                daemon_version: Some(client.daemon_version().get()),
                daemon_peer_id: Some(client.daemon_peer_id().to_hex()),
                error: None,
            },
            Err(err) => StatusJson {
                running: false,
                socket: socket.clone(),
                daemon_version: None,
                daemon_peer_id: None,
                error: Some(err.to_string()),
            },
        };
        let line = serde_json::to_string(&json).expect("serializable");
        println!("{line}");
    } else {
        match &outcome {
            Ok(client) => {
                println!("running");
                println!("socket          {}", socket.display());
                println!("daemon version  {}", client.daemon_version());
                println!("daemon peer id  {}", client.daemon_peer_id());
            }
            Err(err) => {
                println!("not running");
                println!("socket  {}", socket.display());
                println!("error   {err}");
            }
        }
    }
    if outcome.is_ok() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

async fn stop(args: StopArgs) -> ExitCode {
    let (socket, pid_path) = match resolve_paths(&args.conn) {
        Ok(p) => p,
        Err(err) => {
            eprintln!("artel: {err}");
            return ExitCode::from(2);
        }
    };

    let raw = match std::fs::read_to_string(&pid_path) {
        Ok(s) => s,
        Err(err) => {
            eprintln!("artel: read {}: {err}", pid_path.display());
            return ExitCode::from(1);
        }
    };
    let Ok(pid) = raw.trim().parse::<i32>() else {
        eprintln!("artel: pid file {} is corrupt", pid_path.display());
        return ExitCode::from(1);
    };

    // Verify the pidfile's flock is still held before signaling the
    // PID it names. `artel-daemon`'s `PidFile::acquire` takes an
    // exclusive OS lock on this file for its entire lifetime, and the
    // kernel releases that lock unconditionally on any exit —
    // including SIGKILL or a crash — with no cooperation from the
    // daemon required. If we can take the lock ourselves here, nobody
    // holds it: the recorded PID is stale. Signaling it anyway is
    // unsafe once the OS has recycled that PID for an unrelated
    // process — `--force`'s SIGKILL would kill whatever now happens
    // to hold that number. A plain liveness probe (`kill(pid, 0)`)
    // can't tell "recycled by someone else" from "still our daemon";
    // the flock can, because it's identity, not just existence.
    if pidfile_lock_is_free(&pid_path) {
        eprintln!(
            "artel: pid file {} names pid {pid}, but nothing holds its lock — \
             the daemon is already gone and this pid has likely been reused; refusing to signal it",
            pid_path.display(),
        );
        return ExitCode::from(1);
    }

    let signal = if args.force {
        Signal::SIGKILL
    } else {
        Signal::SIGTERM
    };
    if let Err(err) = kill(Pid::from_raw(pid), signal) {
        eprintln!("artel: signal pid {pid}: {err}");
        return ExitCode::from(1);
    }

    let deadline = Instant::now() + Duration::from_secs(args.timeout_secs);
    while Instant::now() < deadline {
        if !socket.exists() && !pid_path.exists() {
            println!("daemon stopped");
            return ExitCode::SUCCESS;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    eprintln!(
        "artel: daemon did not exit within {} seconds",
        args.timeout_secs
    );
    ExitCode::from(1)
}

/// Whether the OS-level exclusive lock on `pid_path` is free — i.e.
/// nobody currently holds it, meaning no live process owns this
/// pidfile.
///
/// Mirrors `artel-daemon`'s own `PidFile::acquire`: a live daemon
/// holds `try_lock` on this exact file for as long as it runs, and
/// the kernel drops that lock the instant the holder exits, by any
/// means. Taking the lock ourselves and immediately releasing it
/// (drop) is a pure probe — it never writes to or truncates the file,
/// and never removes it; only `artel-daemon`'s own guard does that.
/// Best-effort: any I/O error opening the file (e.g. it vanished
/// between our earlier read and this check) is reported as "free"
/// (lock not held) — the caller's read of the file already succeeded,
/// so a failure here is far more likely a genuinely-gone daemon than
/// a transient glitch, and refusing to signal is the safe default
/// either way.
fn pidfile_lock_is_free(pid_path: &Path) -> bool {
    let Ok(file) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(pid_path)
    else {
        return true;
    };
    file.try_lock().is_ok()
}

async fn list(args: ListArgs) -> ExitCode {
    let (socket, pid) = match resolve_paths(&args.conn) {
        Ok(p) => p,
        Err(err) => {
            eprintln!("artel: {err}");
            return ExitCode::from(2);
        }
    };

    let daemon_binary = match args.spawn.resolve_daemon_binary() {
        Ok(p) => p,
        Err(err) => {
            eprintln!("artel: {err}");
            return ExitCode::from(2);
        }
    };

    let client = match connect_with_spawn(&socket, &pid, daemon_binary.as_ref()).await {
        Ok(c) => c,
        Err(err) => {
            print_connect_error(&socket, args.spawn.auto_spawn, &err);
            return ExitCode::from(1);
        }
    };

    let summaries = match client.request(Request::ListSessions).await {
        Ok(Response::ListSessions { sessions }) => sessions,
        Ok(other) => {
            eprintln!("artel: unexpected response {other:?}");
            return ExitCode::from(1);
        }
        Err(err) => {
            eprintln!("artel: list-sessions: {err}");
            return ExitCode::from(1);
        }
    };

    if args.json {
        let line = serde_json::to_string(&summaries).expect("serializable");
        println!("{line}");
    } else if summaries.is_empty() {
        println!("no active sessions");
    } else {
        print_summaries(&summaries);
    }
    ExitCode::SUCCESS
}

/// Connect, optionally spawning the daemon. When `daemon_binary` is
/// `None`, this is just `Client::connect`; when supplied, it routes
/// through `Client::connect_or_spawn`.
async fn connect_with_spawn(
    socket: &Path,
    pid: &Path,
    daemon_binary: Option<&PathBuf>,
) -> Result<Client, ClientError> {
    if let Some(binary) = daemon_binary {
        Client::connect_or_spawn(SpawnOptions::new(socket, pid, binary)).await
    } else {
        Client::connect(socket).await
    }
}

/// Print a connect error with a hint about `--auto-spawn` when the
/// daemon was simply absent.
fn print_connect_error(socket: &Path, auto_spawn: bool, err: &ClientError) {
    eprintln!("artel: connect {}: {err}", socket.display());
    let recoverable = matches!(
        err,
        ClientError::Transport(artel_protocol::transport::TransportError::Io(io_err))
            if matches!(
                io_err.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused,
            )
    );
    if recoverable && !auto_spawn {
        eprintln!(
            "artel: hint: pass --auto-spawn to launch the daemon, or run `artel-daemon` first",
        );
    }
}

fn print_summaries(summaries: &[SessionSummary]) {
    for s in summaries {
        let last_seq = s
            .last_seq
            .map_or_else(|| "-".to_string(), |q| q.to_string());
        println!(
            "{} host={} peers={} last_seq={}",
            s.id, s.is_host, s.peer_count, last_seq
        );
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("artel: failed to build tokio runtime: {err}");
            return ExitCode::from(2);
        }
    };

    runtime.block_on(async move {
        match cli.command {
            Command::Status(args) => status(args).await,
            Command::Stop(args) => stop(args).await,
            Command::List(args) => list(args).await,
        }
    })
}

#[cfg(test)]
mod tests {
    //! Unit tests for `pidfile_lock_is_free`: the guard `stop` uses to
    //! refuse signaling a PID whose pidfile's OS lock nobody holds
    //! (see the doc comment on `pidfile_lock_is_free` for why liveness
    //! alone — `kill(pid, 0)` — isn't enough after PID reuse).

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn lock_is_not_free_while_a_holder_has_it_locked() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("daemon.pid");
        let held = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .unwrap();
        held.try_lock().unwrap();

        assert!(
            !pidfile_lock_is_free(&path),
            "a file locked by a live holder must report as NOT free",
        );

        drop(held);
    }

    /// The bug this module's fix closes: once the holder releases the
    /// lock (process exit, by any means, including SIGKILL — the
    /// kernel drops `flock`s unconditionally), the file must report
    /// as free even though its *contents* still name the old PID —
    /// that PID may since have been recycled by the OS for an
    /// unrelated live process.
    #[test]
    fn lock_is_free_after_the_holder_releases_it() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("daemon.pid");
        {
            let held = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)
                .unwrap();
            held.try_lock().unwrap();
            // Held drops here, releasing the OS lock — simulating the
            // daemon exiting while the pidfile's contents (never
            // rewritten by anyone) still name its now-stale PID.
        }

        assert!(
            pidfile_lock_is_free(&path),
            "a file with no live holder must report as free",
        );
    }

    #[test]
    fn missing_pidfile_reports_free() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("never-existed.pid");
        assert!(
            pidfile_lock_is_free(&path),
            "a pidfile that can't even be opened must not block on the safe-refusal path — \
             `stop`'s earlier read_to_string already succeeded, so this is belt-and-braces",
        );
    }

    #[test]
    fn probing_the_lock_does_not_modify_file_contents() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("daemon.pid");
        std::fs::write(&path, "12345\n").unwrap();

        assert!(pidfile_lock_is_free(&path));

        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "12345\n",
            "the probe must not truncate or rewrite the pidfile — only \
             artel-daemon's own PidFile guard ever touches its contents",
        );
    }
}
