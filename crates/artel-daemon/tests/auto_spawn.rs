//! Integration tests for [`artel_client::Client::connect_or_spawn`].
//!
//! Lives in the `artel-daemon` crate because Cargo only exposes the
//! daemon binary path via `CARGO_BIN_EXE_artel-daemon` to integration
//! tests within that crate.
//!
//! Each test spawns its own short-lived daemon under a tempdir, waits
//! for it to come up via `connect_or_spawn`, exercises an assertion,
//! and SIGTERMs the daemon by reading the PID file. Tempdir is
//! preserved as a `TempDir` so its `Drop` cleans up — but only after
//! the daemon has exited and released the files.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use artel_client::{Client, ClientError, SpawnError, SpawnOptions};
use artel_protocol::{Request, Response};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::sleep;

/// Path to the `artel-daemon` binary built by Cargo for these tests.
fn daemon_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_artel-daemon"))
}

struct AutoSpawned {
    _tempdir: TempDir,
    pid_path: PathBuf,
}

impl AutoSpawned {
    /// SIGTERM the spawned daemon (looked up via the PID file) and
    /// wait for it to exit.
    async fn shutdown(self) {
        let Self {
            _tempdir, pid_path, ..
        } = self;
        let _ = sigterm_pidfile(&pid_path).await;
    }
}

fn fresh_paths() -> (TempDir, PathBuf, PathBuf) {
    let tempdir = TempDir::new().unwrap();
    let socket = tempdir.path().join("daemon.sock");
    let pid = tempdir.path().join("daemon.pid");
    (tempdir, socket, pid)
}

async fn sigterm_pidfile(pid_path: &Path) -> std::io::Result<()> {
    let raw = std::fs::read_to_string(pid_path)?;
    let pid: i32 = raw.trim().parse().map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, format!("bad pid: {e}"))
    })?;
    let _ = kill(Pid::from_raw(pid), Signal::SIGTERM);
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if !pid_path.exists() {
            return Ok(());
        }
        sleep(Duration::from_millis(25)).await;
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "daemon did not exit within 5s",
    ))
}

#[tokio::test]
async fn happy_path_cold_dir_spawns_daemon() {
    let (tempdir, socket, pid_path) = fresh_paths();
    let opts = SpawnOptions::new(&socket, &pid_path, daemon_binary());
    let client = Client::connect_or_spawn(opts).await.unwrap();
    // Daemon answered Hello.
    assert!(client.daemon_version().get() > 0);
    // PID file now points at a real process.
    assert!(pid_path.exists(), "PID file should exist after spawn");
    drop(client);
    AutoSpawned {
        _tempdir: tempdir,
        pid_path,
    }
    .shutdown()
    .await;
}

#[tokio::test]
async fn second_call_reuses_existing_daemon() {
    let (tempdir, socket, pid_path) = fresh_paths();
    let first = Client::connect_or_spawn(SpawnOptions::new(&socket, &pid_path, daemon_binary()))
        .await
        .unwrap();
    let pid_after_first = std::fs::read_to_string(&pid_path).unwrap();
    let second = Client::connect_or_spawn(SpawnOptions::new(&socket, &pid_path, daemon_binary()))
        .await
        .unwrap();
    let pid_after_second = std::fs::read_to_string(&pid_path).unwrap();
    assert_eq!(
        pid_after_first.trim(),
        pid_after_second.trim(),
        "second connect_or_spawn must not have spawned a new daemon",
    );
    drop(first);
    drop(second);
    AutoSpawned {
        _tempdir: tempdir,
        pid_path,
    }
    .shutdown()
    .await;
}

#[tokio::test]
async fn stale_pid_file_is_recovered() {
    // Simulate a previous daemon that crashed without releasing its
    // PID file. The PID points at a reaped process, so it's stale and
    // a fresh daemon should be spawned.
    let (tempdir, socket, pid_path) = fresh_paths();
    let mut throwaway = Command::new("true").spawn().unwrap();
    let dead_pid = throwaway.id();
    throwaway.wait().unwrap();
    std::fs::write(&pid_path, format!("{dead_pid}\n")).unwrap();

    let client = Client::connect_or_spawn(SpawnOptions::new(&socket, &pid_path, daemon_binary()))
        .await
        .unwrap();
    let new_pid = std::fs::read_to_string(&pid_path).unwrap();
    assert_ne!(
        new_pid.trim(),
        dead_pid.to_string(),
        "PID file should now name the new daemon",
    );
    drop(client);
    AutoSpawned {
        _tempdir: tempdir,
        pid_path,
    }
    .shutdown()
    .await;
}

#[tokio::test]
async fn stale_socket_file_is_recovered() {
    // The daemon side handles this: after winning the PID lock, it
    // unlinks any leftover socket file before binding. Verify the
    // client path doesn't choke on the leftover.
    let (tempdir, socket, pid_path) = fresh_paths();
    std::fs::create_dir_all(socket.parent().unwrap()).unwrap();
    std::fs::write(&socket, b"junk").unwrap();
    let client = Client::connect_or_spawn(SpawnOptions::new(&socket, &pid_path, daemon_binary()))
        .await
        .unwrap();
    drop(client);
    AutoSpawned {
        _tempdir: tempdir,
        pid_path,
    }
    .shutdown()
    .await;
}

#[tokio::test]
async fn parallel_calls_settle_on_one_daemon() {
    // Two parallel cold starts: both spawn a daemon, but PID-file
    // contention means only one survives. Both clients connect to the
    // survivor and see the same peer id.
    let (tempdir, socket, pid_path) = fresh_paths();
    let opts_a = SpawnOptions::new(&socket, &pid_path, daemon_binary());
    let opts_b = SpawnOptions::new(&socket, &pid_path, daemon_binary());

    let (a, b) = tokio::join!(
        Client::connect_or_spawn(opts_a),
        Client::connect_or_spawn(opts_b),
    );
    let a = a.expect("client A");
    let b = b.expect("client B");
    assert_eq!(
        a.daemon_peer_id(),
        b.daemon_peer_id(),
        "both clients should be talking to the same daemon",
    );

    // Smoke: both can issue requests through the survivor.
    let resp = a.request(Request::ListSessions).await.unwrap();
    assert!(matches!(resp, Response::ListSessions { .. }));
    let resp = b.request(Request::ListSessions).await.unwrap();
    assert!(matches!(resp, Response::ListSessions { .. }));

    drop(a);
    drop(b);
    AutoSpawned {
        _tempdir: tempdir,
        pid_path,
    }
    .shutdown()
    .await;
}

#[tokio::test]
async fn missing_daemon_binary_yields_launch_error() {
    let (tempdir, socket, pid_path) = fresh_paths();
    let bogus = tempdir.path().join("does-not-exist");
    let err = Client::connect_or_spawn(SpawnOptions::new(&socket, &pid_path, &bogus))
        .await
        .unwrap_err();
    match err {
        ClientError::Spawn(SpawnError::Launch { path, .. }) => {
            assert_eq!(path, bogus);
        }
        other => panic!("expected Spawn::Launch, got {other:?}"),
    }
    // Nothing should have been written.
    assert!(!socket.exists());
    assert!(!pid_path.exists());
}

#[tokio::test]
async fn live_pid_no_socket_waits_for_socket_then_times_out() {
    // Synthesise the "daemon is mid-boot" state: PID file names a
    // long-running process (this test process), but the socket never
    // materialises. connect_or_spawn should NOT spawn a new daemon
    // (because the PID is alive), and should fail with Timeout.
    let (_tempdir, socket, pid_path) = fresh_paths();
    std::fs::write(&pid_path, format!("{}\n", std::process::id())).unwrap();

    let opts = SpawnOptions::new(&socket, &pid_path, daemon_binary())
        .with_spawn_timeout(Duration::from_millis(200));
    let err = Client::connect_or_spawn(opts).await.unwrap_err();
    match err {
        ClientError::Spawn(SpawnError::Timeout { socket: s, timeout }) => {
            assert_eq!(s, socket);
            assert_eq!(timeout, Duration::from_millis(200));
        }
        other => panic!("expected Spawn::Timeout, got {other:?}"),
    }
}
