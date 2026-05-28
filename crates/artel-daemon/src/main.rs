//! `artel-daemon` foreground entry point.
//!
//! Minimal v1: parse a few flags, init tracing, start the daemon, run
//! until shutdown. Status / stop / list will move into the
//! `artel-client` crate alongside the rest of the client surface.

use std::path::PathBuf;
use std::process::ExitCode;

use artel_daemon::{Daemon, DaemonConfig};
use artel_protocol::PeerId;
use artel_protocol::transport::path::{default_pid_path, default_socket_path};
use clap::Parser;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Parser)]
#[command(name = "artel-daemon", version, about = "artel collaboration daemon")]
struct Args {
    /// IPC socket path. Defaults to `~/.artel/daemon.sock` (or
    /// `$ARTEL_HOME/daemon.sock`).
    #[arg(long)]
    socket: Option<PathBuf>,

    /// State directory. When set, defaults `socket` to
    /// `<dir>/daemon.sock` and PID to `<dir>/daemon.pid`.
    #[arg(long)]
    state_dir: Option<PathBuf>,

    /// 32-byte peer id as 64 hex chars. For local-only testing without
    /// iroh; defaults to a deterministic pseudo id derived from PID.
    #[arg(long)]
    peer_id: Option<String>,

    /// Log filter. Falls back to `ARTEL_LOG`, then `RUST_LOG`, then
    /// `info`.
    #[arg(long, env = "ARTEL_LOG")]
    log: Option<String>,
}

fn main() -> ExitCode {
    let args = Args::parse();

    if let Err(err) = init_tracing(args.log.as_deref()) {
        eprintln!("artel-daemon: failed to init logging: {err}");
        return ExitCode::from(2);
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("artel-daemon: failed to build tokio runtime: {err}");
            return ExitCode::from(2);
        }
    };

    runtime.block_on(async move { run(args).await })
}

async fn run(args: Args) -> ExitCode {
    let config = match build_config(&args) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("artel-daemon: {err}");
            return ExitCode::from(2);
        }
    };

    let daemon = match Daemon::start(config).await {
        Ok(d) => d,
        Err(err) => {
            eprintln!("artel-daemon: {err}");
            return ExitCode::from(1);
        }
    };

    match daemon.run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("artel-daemon: {err}");
            ExitCode::from(1)
        }
    }
}

fn build_config(args: &Args) -> Result<DaemonConfig, String> {
    let (socket_path, pid_path, sessions_dir, state_dir) = if let Some(dir) = &args.state_dir {
        (
            args.socket
                .clone()
                .unwrap_or_else(|| dir.join("daemon.sock")),
            dir.join("daemon.pid"),
            dir.join("sessions"),
            dir.clone(),
        )
    } else {
        let socket = args.socket.clone().map_or_else(
            || default_socket_path().map_err(|e| format!("resolve socket path: {e}")),
            Ok,
        )?;
        let pid = default_pid_path().map_err(|e| format!("resolve pid path: {e}"))?;
        let state_dir = artel_protocol::transport::path::default_dir()
            .map_err(|e| format!("resolve state dir: {e}"))?;
        let sessions = state_dir.join("sessions");
        (socket, pid, sessions, state_dir)
    };

    // When the user pins --peer-id we honour it and skip iroh
    // entirely. That keeps the synthetic-id path useful for tests
    // and embeds. Otherwise: load (or generate) a real iroh key.
    let (daemon_peer_id, iroh_key_path) = match &args.peer_id {
        Some(hex) => (parse_peer_id_hex(hex)?, None),
        None => (derive_default_peer_id(), Some(state_dir.join("iroh.key"))),
    };

    Ok(DaemonConfig {
        socket_path,
        pid_path,
        sessions_dir,
        daemon_peer_id,
        iroh_key_path,
        #[cfg(feature = "iroh")]
        endpoint_setup: artel_daemon::EndpointSetup::default(),
        #[cfg(not(feature = "iroh"))]
        endpoint_setup: (),
    })
}

fn init_tracing(filter: Option<&str>) -> Result<(), String> {
    let env = if let Some(f) = filter {
        EnvFilter::try_new(f).map_err(|e| format!("invalid filter: {e}"))?
    } else {
        EnvFilter::try_from_env("ARTEL_LOG")
            .or_else(|_| EnvFilter::try_from_env("RUST_LOG"))
            .unwrap_or_else(|_| EnvFilter::new("info"))
    };
    fmt()
        .with_env_filter(env)
        .try_init()
        .map_err(|e| format!("init tracing: {e}"))
}

fn parse_peer_id_hex(hex: &str) -> Result<PeerId, String> {
    if hex.len() != 64 {
        return Err(format!("peer id must be 64 hex chars, got {}", hex.len()));
    }
    let mut out = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        let hi = decode_nibble(chunk[0]).ok_or_else(|| format!("invalid hex: {hex:?}"))?;
        let lo = decode_nibble(chunk[1]).ok_or_else(|| format!("invalid hex: {hex:?}"))?;
        out[i] = (hi << 4) | lo;
    }
    Ok(PeerId::from_bytes(out))
}

const fn decode_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Pseudo peer id used when `--peer-id` isn't supplied. PID-derived so
/// two local instances don't collide; sentinel byte 0xfe in the last
/// slot makes the synthetic source obvious.
fn derive_default_peer_id() -> PeerId {
    let pid = std::process::id();
    let mut bytes = [0u8; 32];
    let pid_bytes = pid.to_le_bytes();
    bytes[..pid_bytes.len()].copy_from_slice(&pid_bytes);
    bytes[31] = 0xfe;
    PeerId::from_bytes(bytes)
}
