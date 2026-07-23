//! `artel-daemon` foreground entry point.
//!
//! Minimal v1: parse a few flags, init tracing, start the daemon, run
//! until shutdown. Status / stop / list live in the `artel-client`
//! crate (`bin/artel.rs`) alongside the rest of the client surface.

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
            resolve_endpoint_setup(std::env::var("ARTEL_RELAY_URL").ok().as_deref())?
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

/// Resolve the test-utils endpoint setup from an optional
/// `ARTEL_RELAY_URL` value. `None` ⇒ the default setup; `Some(url)` ⇒ a
/// custom-relay setup, or a config `Err` if the url is malformed (L11:
/// a bad value exits cleanly via exit code 2, not a panic). Pure +
/// env-free so it's unit-testable without mutating process globals.
#[cfg(all(feature = "iroh", feature = "test-utils"))]
fn resolve_endpoint_setup(relay_url: Option<&str>) -> Result<artel_daemon::EndpointSetup, String> {
    match relay_url {
        None => Ok(artel_daemon::EndpointSetup::default()),
        Some(url) => {
            let relay_url = url
                .parse()
                .map_err(|e| format!("ARTEL_RELAY_URL invalid: {e}"))?;
            Ok(artel_daemon::EndpointSetup::ProductionCustomRelay { relay_url })
        }
    }
}

#[cfg(test)]
mod build_config_tests {
    use super::*;

    fn args_with_state_dir(dir: PathBuf) -> Args {
        Args {
            socket: None,
            state_dir: Some(dir),
            log: None,
        }
    }

    #[test]
    fn state_dir_derives_socket_pid_and_sessions_under_it() {
        // The `Some(state_dir)` branch never touches process env — no
        // HOME/ARTEL_HOME mutation needed, so this is safe to run
        // alongside every other test in the same process/binary.
        let dir = PathBuf::from("/tmp/artel-daemon-test-state");
        let args = args_with_state_dir(dir.clone());
        let config = build_config(&args).expect("build_config");
        assert_eq!(config.socket_path, dir.join("daemon.sock"));
        assert_eq!(config.pid_path, dir.join("daemon.pid"));
        assert_eq!(config.sessions_dir, dir.join("sessions"));
        assert_eq!(config.iroh_key_path, Some(dir.join("iroh.key")));
    }

    #[test]
    fn state_dir_with_explicit_socket_override_wins() {
        let dir = PathBuf::from("/tmp/artel-daemon-test-state-2");
        let mut args = args_with_state_dir(dir.clone());
        args.socket = Some(PathBuf::from("/tmp/custom.sock"));
        let config = build_config(&args).expect("build_config");
        assert_eq!(
            config.socket_path,
            PathBuf::from("/tmp/custom.sock"),
            "an explicit --socket must override the state-dir default",
        );
        // pid and sessions are still derived from state_dir regardless.
        assert_eq!(config.pid_path, dir.join("daemon.pid"));
        assert_eq!(config.sessions_dir, dir.join("sessions"));
    }
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

#[cfg(all(test, feature = "iroh", feature = "test-utils"))]
mod tests {
    use super::*;

    #[test]
    fn malformed_relay_url_is_a_config_error_not_a_panic() {
        // L11: a bad ARTEL_RELAY_URL must surface as Err(String) (clean
        // exit 2), not abort the process via panic like the prior
        // `.expect()` did.
        let err =
            resolve_endpoint_setup(Some("not a url")).expect_err("malformed url must be rejected");
        assert!(
            err.contains("ARTEL_RELAY_URL invalid"),
            "error should name the offending config: {err}",
        );
    }

    #[test]
    fn absent_relay_url_resolves_to_default() {
        assert!(resolve_endpoint_setup(None).is_ok());
    }

    #[test]
    fn valid_relay_url_resolves_to_custom_relay() {
        let setup = resolve_endpoint_setup(Some("https://relay.example.com"))
            .expect("a valid url must resolve");
        assert!(matches!(
            setup,
            artel_daemon::EndpointSetup::ProductionCustomRelay { .. }
        ));
    }
}
