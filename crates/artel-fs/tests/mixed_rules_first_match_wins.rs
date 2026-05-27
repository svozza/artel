//! First-match-wins ordering, end-to-end on the wire.
//!
//! Rule unit tests in `rules.rs` already verify ordering at the
//! `mode_for` level. This integration test confirms the same
//! ordering carries through the watcher → doc → applier pipeline:
//! a `docs/secret/foo.txt` write under
//! `[{ "docs/**" -> ReadWrite }, { "docs/secret/**" -> ReadOnly }]`
//! propagates (first rule wins → `ReadWrite`), and stops propagating
//! when the rule order is reversed.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use artel_client::Client;
use artel_fs::{AttachPolicy, Mode, PathRule, PathRules, Workspace, WorkspaceConfig};
use artel_protocol::{PeerId, PeerInfo, Request, Response};
use tokio::time::sleep;

use common::wait_for_file;

#[tokio::test(flavor = "multi_thread")]
async fn first_match_wins_carries_through_wire() {
    // Phase 1: broad ReadWrite rule precedes narrow ReadOnly. The
    // narrow rule is unreachable; `docs/secret/foo.txt` propagates.
    // Drive timing positively — poll for the secret on Bob's side.
    run_with_rules(
        PathRules {
            default: Mode::ReadWrite,
            rules: vec![
                PathRule {
                    glob: "docs/**".into(),
                    mode: Mode::ReadWrite,
                },
                PathRule {
                    glob: "docs/secret/**".into(),
                    mode: Mode::ReadOnly,
                },
            ],
        },
        Expectation::Propagates,
    )
    .await;

    // Phase 2: reorder. Narrow ReadOnly precedes broad ReadWrite.
    // Now `docs/secret/foo.txt` is blocked. Drive timing via a
    // marker file written *after* the secret — once Bob has the
    // marker, the secret would have arrived too if the rule weren't
    // blocking it.
    run_with_rules(
        PathRules {
            default: Mode::ReadWrite,
            rules: vec![
                PathRule {
                    glob: "docs/secret/**".into(),
                    mode: Mode::ReadOnly,
                },
                PathRule {
                    glob: "docs/**".into(),
                    mode: Mode::ReadWrite,
                },
            ],
        },
        Expectation::Blocked,
    )
    .await;
}

/// What the test expects to happen to `docs/secret/foo.txt` on
/// Bob's side. Each variant uses the shape-appropriate signal:
/// `Propagates` polls for the file directly (positive); `Blocked`
/// waits for a sentinel marker that was written *after* the secret
/// and then asserts the secret is still absent.
#[derive(Clone, Copy, Debug)]
enum Expectation {
    Propagates,
    Blocked,
}

/// Budget for the positive path: how long to wait for the secret to
/// reach Bob before failing. Generous because we go through
/// debounce + doc → sync → applier.
const PROPAGATE_BUDGET: Duration = Duration::from_secs(15);
const POLL: Duration = Duration::from_millis(100);

async fn poll_for(path: &Path, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        sleep(POLL).await;
    }
    false
}

/// Stand a host/joiner pair up with `rules`, write
/// `docs/secret/foo.txt` (and a marker), then verify the
/// `expectation` against Bob's filesystem.
async fn run_with_rules(rules: PathRules, expectation: Expectation) {
    let common::Pair {
        daemon_a,
        daemon_b,
        workspace_lookup_a,
        workspace_lookup_b,
    } = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _) = Workspace::host_with(
        &alice,
        alice_peer,
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default()
            .with_address_lookup_override(workspace_lookup_a)
            .with_rules(rules),
    )
    .await
    .expect("Workspace::host_with");
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
    let resp = bob
        .request(Request::JoinSession {
            peer: bob_peer,
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = tempfile::tempdir().unwrap();
    let (bob_ws, _) = Workspace::join_with(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_address_lookup_override(workspace_lookup_b),
    )
    .await
    .expect("Workspace::join_with");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // Write the secret + marker.
    tokio::fs::create_dir_all(alice_dir.path().join("docs/secret"))
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("docs/secret/foo.txt"), b"data")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("marker.txt"), b"go")
        .await
        .unwrap();

    let bob_secret = bob_dir.path().join("docs/secret/foo.txt");
    match expectation {
        Expectation::Propagates => {
            // Positive case: poll the secret directly. The marker
            // signal is *not* a substitute here — top-level marker
            // and nested secret travel through independent
            // debounce/publish paths and the marker can land on
            // Bob first even though the secret was written first.
            assert!(
                poll_for(&bob_secret, PROPAGATE_BUDGET).await,
                "first-match-wins broken: ReadWrite-first should let \
                 docs/secret/foo.txt through within {PROPAGATE_BUDGET:?}",
            );
        }
        Expectation::Blocked => {
            // Negative case: wait for the marker (written *after*
            // the secret) to arrive on Bob. Once it has, the
            // pipeline has had at least one full round-trip — if
            // the secret were going to leak, it would have arrived
            // by now too. The check stays a single `.exists()` to
            // catch a near-simultaneous leak; if marker-arrival
            // turns out to be insufficient settling for the
            // negative path, this would be the next thing to
            // tighten (e.g. a brief spin after marker arrival).
            wait_for_file(&bob_dir.path().join("marker.txt"), b"go").await;
            assert!(
                !bob_secret.exists(),
                "first-match-wins broken: ReadOnly-first should block \
                 docs/secret/foo.txt; it leaked to {}",
                bob_secret.display(),
            );
        }
    }

    alice_ws.shutdown().await;
    bob_ws.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), alice_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
