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

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use artel_client::Client;
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

async fn list(args: ListArgs) -> ExitCode {
    let (socket, _pid) = match resolve_paths(&args.conn) {
        Ok(p) => p,
        Err(err) => {
            eprintln!("artel: {err}");
            return ExitCode::from(2);
        }
    };

    let client = match Client::connect(&socket).await {
        Ok(c) => c,
        Err(err) => {
            eprintln!("artel: connect {}: {err}", socket.display());
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
