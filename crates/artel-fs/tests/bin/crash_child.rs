//! Helper binary for `tests/crash_recovery.rs`.
//!
//! Spawns as a child of the integration test, hosts an artel
//! session and a workspace, then sits idle until the test SIGKILLs
//! it. The parent uses the child's stdout to drive timing
//! (instead of sleeping) so the test stays deterministic.
//!
//! Stdout protocol — one status line at a time, in this order:
//!
//! ```text
//! HOSTED <session-uuid>
//! TICKET <artel-join-ticket>
//! WORKSPACE_UP
//! READY
//! WROTE <relative-path>          # mid_write mode only, repeated
//! ```
//!
//! Modes:
//!
//! - `steady` — host the workspace, publish whatever's already in
//!   `--root`, emit `READY`, then idle.
//! - `mid_scan` — same, but the parent SIGKILLs us between
//!   `WORKSPACE_UP` and `READY` (i.e. mid scan-and-publish). Child
//!   behaves identically; the parent's choice of when to kill
//!   differentiates.
//! - `mid_write` — after `READY`, write a new file every 50 ms,
//!   emitting `WROTE <name>` per file. Parent SIGKILLs after the
//!   first WROTE arrives.
//!
//! Args:
//!
//! ```text
//! crash_child --socket <PATH> --root <PATH> --state-dir <PATH>
//!             --peer <NAME> --mode {steady|mid_scan|mid_write}
//! ```

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use artel_client::Client;
use artel_fs::{AttachPolicy, Workspace, WorkspaceConfig};

#[derive(Debug)]
struct Args {
    socket: PathBuf,
    root: PathBuf,
    state_dir: PathBuf,
    peer_name: String,
    mode: Mode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Steady,
    MidScan,
    MidWrite,
}

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let mut socket: Option<PathBuf> = None;
    let mut root: Option<PathBuf> = None;
    let mut state_dir: Option<PathBuf> = None;
    let mut peer_name: Option<String> = None;
    let mut mode: Option<Mode> = None;

    while let Some(a) = args.next() {
        let v = args
            .next()
            .ok_or_else(|| format!("missing value for {a}"))?;
        match a.as_str() {
            "--socket" => socket = Some(PathBuf::from(v)),
            "--root" => root = Some(PathBuf::from(v)),
            "--state-dir" => state_dir = Some(PathBuf::from(v)),
            "--peer" => peer_name = Some(v),
            "--mode" => {
                mode = Some(match v.as_str() {
                    "steady" => Mode::Steady,
                    "mid_scan" => Mode::MidScan,
                    "mid_write" => Mode::MidWrite,
                    other => return Err(format!("unknown mode {other}")),
                });
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }

    Ok(Args {
        socket: socket.ok_or("--socket required")?,
        root: root.ok_or("--root required")?,
        state_dir: state_dir.ok_or("--state-dir required")?,
        peer_name: peer_name.ok_or("--peer required")?,
        mode: mode.ok_or("--mode required")?,
    })
}

fn emit(line: &str) {
    // The parent reads stdout line-by-line via tokio's BufReader.
    // `println!` plus an explicit flush is enough — we don't need
    // the framing the daemon uses.
    println!("{line}");
    let _ = std::io::stdout().flush();
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(err) => {
            eprintln!("crash_child: {err}");
            return ExitCode::from(2);
        }
    };

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("crash_child: tokio runtime: {err}");
            return ExitCode::from(2);
        }
    };

    if let Err(err) = rt.block_on(run(args)) {
        eprintln!("crash_child: {err}");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

async fn run(args: Args) -> Result<(), String> {
    // Auth L1 fix #3 (PROTOCOL_VERSION 5): the daemon stamps its
    // own authenticated PeerId, so the child only needs to supply
    // the display name. Stability across child restarts is
    // guaranteed by the persisted iroh.key under the daemon's
    // state dir, not by the child's process identity.
    let client = Client::connect(&args.socket)
        .await
        .map_err(|e| format!("connect daemon: {e}"))?;

    let cfg = WorkspaceConfig::default().with_state_dir(args.state_dir.clone());
    let (ws, mut events) = Workspace::host_with(
        &client,
        args.peer_name.clone(),
        args.root.clone(),
        AttachPolicy::AllowExisting,
        cfg,
    )
    .await
    .map_err(|e| format!("Workspace::host_with: {e}"))?;
    let session = ws.session_id();
    let ticket = ws
        .join_ticket()
        .ok_or_else(|| "host workspace missing join_ticket".to_string())?;
    emit(&format!("HOSTED {session}"));
    emit(&format!("TICKET {}", ticket.as_str()));
    let ws = Arc::new(ws);
    let _handle = Arc::clone(&ws).run().await;
    emit("WORKSPACE_UP");

    // Drain workspace events to keep the channel from filling up.
    // We don't act on them — the parent observes its own peer's
    // events, not ours.
    tokio::spawn(async move { while events.recv().await.is_some() {} });

    emit("READY");

    match args.mode {
        Mode::Steady | Mode::MidScan => {
            // Idle forever — the parent SIGKILLs when ready.
            futures_util::future::pending::<()>().await;
        }
        Mode::MidWrite => {
            // Drop a fresh file every 50 ms with a sequential name.
            // The parent kills us after seeing the first WROTE.
            let mut seq: u64 = 0;
            loop {
                let name = format!("live-{seq:04}.txt");
                let path = args.root.join(&name);
                let payload = format!("crash-child-{seq}").into_bytes();
                if let Err(err) = tokio::fs::write(&path, &payload).await {
                    return Err(format!("write {}: {err}", path.display()));
                }
                emit(&format!("WROTE {name}"));
                seq += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    Ok(())
}
