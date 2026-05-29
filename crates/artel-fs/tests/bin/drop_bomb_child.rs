//! Helper binary for `tests/drop_bomb.rs`.
//!
//! Connects to the parent's already-running daemon, calls
//! `Workspace::host_with`, then either drops the workspace
//! ungracefully (`--mode ungraceful`) or calls
//! `Workspace::shutdown().await` first (`--mode graceful`), then
//! exits. The parent reads the child's stderr to assert whether the
//! Drop bomb fired.
//!
//! Args:
//!
//! ```text
//! drop_bomb_child --socket <PATH> --root <PATH> --state-dir <PATH>
//!                 --mode {ungraceful|graceful}
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use artel_client::Client;
use artel_fs::{AttachPolicy, Workspace, WorkspaceConfig};
use artel_protocol::{PeerId, PeerInfo};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Ungraceful,
    Graceful,
}

#[derive(Debug)]
struct Args {
    socket: PathBuf,
    root: PathBuf,
    state_dir: PathBuf,
    mode: Mode,
}

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let mut socket: Option<PathBuf> = None;
    let mut root: Option<PathBuf> = None;
    let mut state_dir: Option<PathBuf> = None;
    let mut mode: Option<Mode> = None;
    while let Some(a) = args.next() {
        let v = args
            .next()
            .ok_or_else(|| format!("missing value for {a}"))?;
        match a.as_str() {
            "--socket" => socket = Some(PathBuf::from(v)),
            "--root" => root = Some(PathBuf::from(v)),
            "--state-dir" => state_dir = Some(PathBuf::from(v)),
            "--mode" => {
                mode = Some(match v.as_str() {
                    "ungraceful" => Mode::Ungraceful,
                    "graceful" => Mode::Graceful,
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
        mode: mode.ok_or("--mode required")?,
    })
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(err) => {
            eprintln!("drop_bomb_child: {err}");
            return ExitCode::from(2);
        }
    };

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("drop_bomb_child runtime: {err}");
            return ExitCode::from(2);
        }
    };

    rt.block_on(async move { run(args).await });
    ExitCode::SUCCESS
}

async fn run(args: Args) {
    let alice = Client::connect(&args.socket).await.expect("client connect");
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");

    let cfg = WorkspaceConfig::default().with_state_dir(args.state_dir.clone());
    let (workspace, _events) = Workspace::host_with(
        &alice,
        alice_peer,
        args.root.clone(),
        AttachPolicy::AllowExisting,
        cfg,
    )
    .await
    .expect("host_with");

    match args.mode {
        Mode::Ungraceful => {
            // Drop without shutdown → bomb should fire.
            drop(workspace);
        }
        Mode::Graceful => {
            workspace.shutdown().await.expect("shutdown");
            // Subsequent drop must NOT fire the bomb.
        }
    }

    // Drop the client so it doesn't keep the daemon connection
    // alive past process exit.
    drop(alice);
}
