//! `artel-daemon` foreground entry point.
//!
//! Minimal v1: parse a few flags, init tracing, start the daemon, run
//! until shutdown. Status / stop / list will move into the
//! `artel-client` crate alongside the rest of the client surface.

use std::path::PathBuf;
use std::process::ExitCode;

use artel_daemon::{Daemon, DaemonConfig};
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

    // The daemon's PeerId is its iroh EndpointId — full stop. The
    // key file under the state dir is loaded (or created on first
    // run); without the iroh feature, the daemon advertises a
    // documented all-zero `SYNTHETIC_LOCAL_PEER_ID` and can't
    // route anything.
    let iroh_key_path = state_dir.join("iroh.key");

    #[cfg(feature = "iroh")]
    let endpoint_setup = {
        #[cfg(feature = "test-utils")]
        {
            if let Ok(url) = std::env::var("ARTEL_RELAY_URL") {
                let relay_url = url.parse().expect("ARTEL_RELAY_URL invalid");
                artel_daemon::EndpointSetup::ProductionCustomRelay { relay_url }
            } else {
                artel_daemon::EndpointSetup::default()
            }
        }
        #[cfg(not(feature = "test-utils"))]
        {
            artel_daemon::EndpointSetup::default()
        }
    };

    Ok(DaemonConfig {
        socket_path,
        pid_path,
        sessions_dir,
        iroh_key_path: Some(iroh_key_path),
        #[cfg(feature = "iroh")]
        endpoint_setup,
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
