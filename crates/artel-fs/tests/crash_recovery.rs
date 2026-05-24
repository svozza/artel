//! Process-level crash recovery.
//!
//! `iroh-docs` (redb) and `iroh-blobs` (append-only blobs + redb
//! metadata) are designed to survive `SIGKILL`, but our code
//! layered on top is not automatically covered. These tests run
//! the host's `Workspace::run` in a child process, kill -9 it,
//! restart the same workspace from the same `state_dir`, and
//! assert disk + doc state agree.
//!
//! Three crash points are exercised:
//!
//! 1. **Steady-state crash** — full publish completed, peer in
//!    sync, then SIGKILL. Restart should reproduce identical disk
//!    state on both sides.
//! 2. **Mid scan-and-publish** — workspace starts with N seeded
//!    files; SIGKILL between `WORKSPACE_UP` and `READY`, before
//!    the initial scan finishes. Reconcile + rescan must finish
//!    the job on next start.
//! 3. **Mid live write** — after `READY`, the host writes new
//!    files in a loop; SIGKILL during the watcher's debounce
//!    window. The rescan on restart must include any file that's
//!    on disk but missing from the doc.
//!
//! The child is built as a separate `[[bin]]` (`crash_child`); the
//! parent reads its stdout to drive timing instead of sleeping.

mod common;

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use artel_client::Client;
use artel_fs::{AttachPolicy, Workspace, WorkspaceConfig, WorkspaceEvent};
use artel_protocol::{JoinTicket, PeerId, PeerInfo, Request, Response};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

const POLL: Duration = Duration::from_millis(100);
const FILE_BUDGET: Duration = Duration::from_secs(30);
const STATUS_BUDGET: Duration = Duration::from_secs(20);

/// Status the parent reads off the child's stdout. The wire format
/// is `LABEL [ARG]` per line. Only variants we actually match on
/// are constructed; unparsed lines are dropped.
#[derive(Clone, Debug)]
enum Status {
    Hosted,
    Ticket(JoinTicket),
    WorkspaceUp,
    Ready,
    Wrote,
}

struct ChildHandle {
    proc: Child,
    rx: mpsc::Receiver<Status>,
    pid: u32,
}

impl ChildHandle {
    /// Wait until a status matching `pred` appears, returning it.
    /// Drops earlier statuses; panics on timeout.
    async fn wait_for<F>(&mut self, pred: F, label: &str) -> Status
    where
        F: Fn(&Status) -> bool + Send + Sync,
    {
        timeout(STATUS_BUDGET, async {
            while let Some(s) = self.rx.recv().await {
                if pred(&s) {
                    return s;
                }
            }
            panic!("child stdout closed before {label}");
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {label}"))
    }

    /// `kill -9` the child and reap. Idempotent — already-exited
    /// children return ESRCH which we ignore.
    async fn sigkill(mut self) {
        let _ = kill(Pid::from_raw(self.pid.cast_signed()), Signal::SIGKILL);
        let _ = timeout(Duration::from_secs(5), self.proc.wait()).await;
    }
}

/// Spawn the `crash_child` bin pointed at `socket` / `root` /
/// `state_dir`, drain its stdout into a channel.
fn spawn_child(
    socket: &Path,
    root: &Path,
    state_dir: &Path,
    peer_name: &str,
    mode: &str,
) -> ChildHandle {
    let exe = env!("CARGO_BIN_EXE_crash_child");
    let mut proc = Command::new(exe)
        .args([
            "--socket",
            &socket.display().to_string(),
            "--root",
            &root.display().to_string(),
            "--state-dir",
            &state_dir.display().to_string(),
            "--peer",
            peer_name,
            "--mode",
            mode,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn crash_child");
    let pid = proc.id().expect("child pid");

    let stdout = proc.stdout.take().expect("child stdout");
    let (tx, rx) = mpsc::channel::<Status>(64);
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(s) = parse_status(&line)
                && tx.send(s).await.is_err()
            {
                return;
            }
        }
    });

    ChildHandle { proc, rx, pid }
}

fn parse_status(line: &str) -> Option<Status> {
    let mut parts = line.splitn(2, ' ');
    let kind = parts.next()?;
    let rest = parts.next().unwrap_or("");
    Some(match kind {
        "HOSTED" => Status::Hosted,
        "TICKET" => Status::Ticket(JoinTicket::from(rest)),
        "WORKSPACE_UP" => Status::WorkspaceUp,
        "READY" => Status::Ready,
        "WROTE" => {
            let _ = rest;
            Status::Wrote
        }
        _ => return None,
    })
}

/// Drive the child up to and including `WORKSPACE_UP`, capturing
/// the artel `JoinTicket` in passing.
async fn run_until_workspace_up(child: &mut ChildHandle) -> JoinTicket {
    let _ = child
        .wait_for(|s| matches!(s, Status::Hosted), "HOSTED")
        .await;
    let Status::Ticket(ticket) = child
        .wait_for(|s| matches!(s, Status::Ticket(_)), "TICKET")
        .await
    else {
        unreachable!("wait_for matched Status::Ticket pattern")
    };
    let _ = child
        .wait_for(|s| matches!(s, Status::WorkspaceUp), "WORKSPACE_UP")
        .await;
    ticket
}

/// Bring up Bob's workspace as a joiner. The same `bob_root` and
/// `bob_wstate` are reused across child restarts so we test the
/// returning-joiner path too.
async fn join_as_bob(
    socket: &Path,
    bob_root: &Path,
    bob_wstate: &Path,
    artel_ticket: JoinTicket,
    peer_id: [u8; 32],
) -> (
    Client,
    Arc<Workspace>,
    mpsc::Receiver<WorkspaceEvent>,
    tokio::task::JoinHandle<()>,
) {
    let bob = Client::connect(socket).await.unwrap();
    let bob_peer = PeerInfo::new(PeerId::from_bytes(peer_id), "bob");
    let session = match bob
        .request(Request::JoinSession {
            peer: bob_peer,
            ticket: artel_ticket,
        })
        .await
        .unwrap()
    {
        Response::JoinSession { session, .. } => session,
        other => panic!("JoinSession: got {other:?}"),
    };
    let cfg = WorkspaceConfig::default().with_state_dir(bob_wstate.to_path_buf());
    let (ws, events) = Workspace::join_with(
        &bob,
        session,
        bob_root.to_path_buf(),
        AttachPolicy::AllowExisting,
        cfg,
    )
    .await
    .expect("Workspace::join_with");
    let ws = Arc::new(ws);
    let handle = Arc::clone(&ws).run().await;
    (bob, ws, events, handle)
}

async fn wait_for_file(path: &Path, expected: &[u8]) {
    let deadline = Instant::now() + FILE_BUDGET;
    loop {
        if let Ok(bytes) = tokio::fs::read(path).await
            && bytes == expected
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "never saw expected bytes at {}",
            path.display(),
        );
        sleep(POLL).await;
    }
}

/// Test 1 — steady-state SIGKILL. Workspace fully published, peer
/// in sync, then kill -9. Restart should reproduce identical disk
/// state and live sync should resume.
#[tokio::test(flavor = "multi_thread")]
async fn steady_state_sigkill_preserves_state() {
    let (daemon_a, daemon_b) = common::spawn_pair().await;

    let alice_root = tempfile::tempdir().unwrap();
    let alice_wstate = tempfile::tempdir().unwrap();
    let bob_root = tempfile::tempdir().unwrap();
    let bob_wstate = tempfile::tempdir().unwrap();

    tokio::fs::write(alice_root.path().join("seed.txt"), b"steady-seed")
        .await
        .unwrap();

    // Phase 1: child publishes, Bob joins, both observe the seed
    // file, then SIGKILL Alice.
    let mut child = spawn_child(
        &daemon_a.socket,
        alice_root.path(),
        alice_wstate.path(),
        "alice",
        "steady",
    );
    let ticket = run_until_workspace_up(&mut child).await;
    let _ = child
        .wait_for(|s| matches!(s, Status::Ready), "READY")
        .await;

    let (bob, bob_ws, _bob_events, bob_handle) = join_as_bob(
        &daemon_b.socket,
        bob_root.path(),
        bob_wstate.path(),
        ticket,
        [2; 32],
    )
    .await;
    wait_for_file(&bob_root.path().join("seed.txt"), b"steady-seed").await;

    child.sigkill().await;

    // Tear Bob down before phase 2: the artel session is gone with
    // Alice's daemon-state for that session, and the next host
    // restart starts a fresh artel session. Bob will rejoin via
    // the new ticket.
    bob_ws.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), bob_handle).await;
    drop(bob);

    // Phase 2: respawn Alice from the same state dir; respawn Bob
    // against the same wstate so we exercise the returning-joiner
    // path too.
    let mut child = spawn_child(
        &daemon_a.socket,
        alice_root.path(),
        alice_wstate.path(),
        "alice",
        "steady",
    );
    let ticket2 = run_until_workspace_up(&mut child).await;
    let _ = child
        .wait_for(|s| matches!(s, Status::Ready), "READY")
        .await;

    let (bob, bob_ws, _bob_events, bob_handle) = join_as_bob(
        &daemon_b.socket,
        bob_root.path(),
        bob_wstate.path(),
        ticket2,
        [3; 32],
    )
    .await;

    let seed_after = tokio::fs::read(bob_root.path().join("seed.txt"))
        .await
        .expect("seed.txt should still exist on bob");
    assert_eq!(seed_after, b"steady-seed");

    // Live sync resumes: write via Alice's filesystem, observe on
    // Bob's. No settling sleep needed — `Workspace::run().await`
    // resolves only once both Bob's watcher is attached and his
    // applier has subscribed to the doc, so a write here will reach
    // his applier.
    tokio::fs::write(alice_root.path().join("post-restart.txt"), b"live-again")
        .await
        .unwrap();
    wait_for_file(&bob_root.path().join("post-restart.txt"), b"live-again").await;

    child.sigkill().await;
    bob_ws.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), bob_handle).await;
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

/// Test 2 — kill mid scan-and-publish.
///
/// Pre-seed N files. Spawn the child in `mid_scan` mode. SIGKILL
/// as soon as `WORKSPACE_UP` arrives, before `READY` — the child
/// is mid-scan. The exact set of files it managed to publish is
/// non-deterministic. Restart it and assert that all N files
/// converge into the doc and onto Bob's disk.
#[tokio::test(flavor = "multi_thread")]
async fn mid_scan_sigkill_recovers_via_reconcile() {
    const N: usize = 16;

    let (daemon_a, daemon_b) = common::spawn_pair().await;

    let alice_root = tempfile::tempdir().unwrap();
    let alice_wstate = tempfile::tempdir().unwrap();
    let bob_root = tempfile::tempdir().unwrap();
    let bob_wstate = tempfile::tempdir().unwrap();

    for i in 0..N {
        let payload = format!("seed-{i:02}").into_bytes();
        tokio::fs::write(alice_root.path().join(format!("f{i:02}.txt")), payload)
            .await
            .unwrap();
    }

    // Phase 1: kill immediately after WORKSPACE_UP. We don't wait
    // for READY — the kill point is inherently racy with respect
    // to the scan; "did anything reach the doc" is the recovery
    // guarantee we're testing.
    let mut child = spawn_child(
        &daemon_a.socket,
        alice_root.path(),
        alice_wstate.path(),
        "alice",
        "mid_scan",
    );
    let _ = run_until_workspace_up(&mut child).await;
    child.sigkill().await;

    // Phase 2: full restart in steady mode, then join Bob and let
    // sync run. All N files should be present on Bob.
    let mut child = spawn_child(
        &daemon_a.socket,
        alice_root.path(),
        alice_wstate.path(),
        "alice",
        "steady",
    );
    let ticket2 = run_until_workspace_up(&mut child).await;
    let _ = child
        .wait_for(|s| matches!(s, Status::Ready), "READY")
        .await;

    let (bob, bob_ws, _bob_events, bob_handle) = join_as_bob(
        &daemon_b.socket,
        bob_root.path(),
        bob_wstate.path(),
        ticket2,
        [4; 32],
    )
    .await;

    for i in 0..N {
        let expected = format!("seed-{i:02}");
        wait_for_file(
            &bob_root.path().join(format!("f{i:02}.txt")),
            expected.as_bytes(),
        )
        .await;
    }

    child.sigkill().await;
    bob_ws.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), bob_handle).await;
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

/// Test 3 — kill mid live-write.
///
/// After `READY`, the child writes a fresh file every 50 ms.
/// SIGKILL after the first `WROTE` — the watcher's 300 ms
/// debouncer means at least the most recent write hasn't been
/// published to the doc yet. After restart, the rescan
/// (`scan_and_publish_existing`) must reconcile any on-disk file
/// the prior process didn't get to publish.
#[tokio::test(flavor = "multi_thread")]
async fn mid_write_sigkill_resyncs_on_restart() {
    let (daemon_a, daemon_b) = common::spawn_pair().await;

    let alice_root = tempfile::tempdir().unwrap();
    let alice_wstate = tempfile::tempdir().unwrap();
    let bob_root = tempfile::tempdir().unwrap();
    let bob_wstate = tempfile::tempdir().unwrap();

    let mut child = spawn_child(
        &daemon_a.socket,
        alice_root.path(),
        alice_wstate.path(),
        "alice",
        "mid_write",
    );
    let _ticket = run_until_workspace_up(&mut child).await;
    let _ = child
        .wait_for(|s| matches!(s, Status::Ready), "READY")
        .await;

    // Wait for the first WROTE — the file is on disk but almost
    // certainly not yet in the doc (300ms debounce hasn't fired).
    let _first = child
        .wait_for(|s| matches!(s, Status::Wrote), "first WROTE")
        .await;
    child.sigkill().await;

    // Capture which files actually made it to disk before the
    // kill. Anything in this set must end up on Bob.
    let mut on_disk = Vec::new();
    let mut entries = tokio::fs::read_dir(alice_root.path()).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("live-") {
            on_disk.push(name);
        }
    }
    on_disk.sort();
    assert!(
        !on_disk.is_empty(),
        "expected at least one live-*.txt on disk"
    );

    // Phase 2: respawn in steady mode, join Bob, verify all
    // pre-kill on-disk files end up on Bob via rescan.
    let mut child = spawn_child(
        &daemon_a.socket,
        alice_root.path(),
        alice_wstate.path(),
        "alice",
        "steady",
    );
    let ticket2 = run_until_workspace_up(&mut child).await;
    let _ = child
        .wait_for(|s| matches!(s, Status::Ready), "READY")
        .await;

    let (bob, bob_ws, _bob_events, bob_handle) = join_as_bob(
        &daemon_b.socket,
        bob_root.path(),
        bob_wstate.path(),
        ticket2,
        [5; 32],
    )
    .await;

    for name in &on_disk {
        let expected = tokio::fs::read(alice_root.path().join(name))
            .await
            .unwrap_or_else(|e| panic!("read alice's {name}: {e}"));
        wait_for_file(&bob_root.path().join(name), &expected).await;
    }

    child.sigkill().await;
    bob_ws.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), bob_handle).await;
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
