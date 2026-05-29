//! Full round-trip test.
//!
//! Two daemons + two `Workspace`s on the same artel session
//! exercise the full watcher → doc → applier loop in both
//! directions:
//!
//! - Alice writes `a.txt` → assert Bob sees it.
//! - Bob writes `b.txt` → assert Alice sees it.
//! - Alice writes `target/junk` → assert Bob does NOT see it
//!   (hardcoded skip).
//! - Echo guard sanity: count Doc entries for the key Bob just
//!   applied — there should be exactly 1, not 2.
//!
//! Runs 3 times in a row to flush out gossip-on-gossip-on-fs
//! flakiness.

mod common;

use common::testing_setup;

use std::sync::Arc;
use std::time::{Duration, Instant};

use artel_client::Client;
use artel_fs::{AttachPolicy, Workspace, WorkspaceConfig, key_to_path, path_to_key};
use artel_protocol::{PeerId, PeerInfo, Request, Response};
use futures_util::StreamExt;
use iroh_docs::store::Query;
use tokio::time::sleep;

const WAIT_BUDGET: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[tokio::test(flavor = "multi_thread")]
async fn round_trip_3_in_a_row() {
    // Run the full scenario 3 consecutive times. The `_run` index
    // is purely informational; if any iteration fails, the test
    // panics with the iteration index in the message.
    for run in 0..3 {
        eprintln!("--- round_trip iteration {run} ---");
        round_trip_once(run).await;
    }
}

// Long, deliberately linear: this is a top-down e2e scenario, and
// extracting helpers per-step would obscure the order more than the
// length hurts.
#[allow(clippy::too_many_lines)]
async fn round_trip_once(run: usize) {
    let pair = common::spawn_pair().await;
    let common::Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = pair;

    // Alice on daemon A hosts the artel session + workspace.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let alice_dir = tempfile::tempdir().unwrap();
    let (alice_ws, _) = Workspace::host_with(
        &alice,
        alice_peer,
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::host");
    let session = alice_ws.session_id();
    let ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_handle = Arc::clone(&alice_ws).run().await;

    // Bob on daemon B joins, then mounts a workspace.
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
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join");
    let bob_ws = Arc::new(bob_ws);
    let bob_handle = Arc::clone(&bob_ws).run().await;

    // No settling delay needed — `Workspace::run().await` only
    // resolves once the OS-level filesystem watch is attached.

    // 1. Alice writes a.txt → Bob sees it.
    let alice_a = alice_dir.path().join("a.txt");
    tokio::fs::write(&alice_a, b"alpha")
        .await
        .expect("write a.txt on alice");
    wait_for_file(
        &bob_dir.path().join("a.txt"),
        b"alpha",
        "bob sees a.txt",
        run,
    )
    .await;

    // 2. Bob writes b.txt → Alice sees it.
    let bob_b = bob_dir.path().join("b.txt");
    tokio::fs::write(&bob_b, b"beta")
        .await
        .expect("write b.txt on bob");
    wait_for_file(
        &alice_dir.path().join("b.txt"),
        b"beta",
        "alice sees b.txt",
        run,
    )
    .await;

    // 3. Alice writes target/junk — hardcoded skip; Bob must NOT
    //    see it. Use a sentinel file (`sentinel.txt`) written
    //    *after* `target/junk` to drive timing end-to-end: once
    //    Bob has the sentinel, Alice's watcher pipeline has
    //    finished its debounce, published, and propagated. If
    //    `target/junk` were going to leak, it would have arrived by
    //    then too. This avoids picking a fixed settling delay.
    let alice_target = alice_dir.path().join("target");
    tokio::fs::create_dir_all(&alice_target).await.unwrap();
    tokio::fs::write(alice_target.join("junk"), b"build artifact")
        .await
        .unwrap();
    tokio::fs::write(alice_dir.path().join("sentinel.txt"), b"after-junk")
        .await
        .unwrap();
    wait_for_file(
        &bob_dir.path().join("sentinel.txt"),
        b"after-junk",
        "bob sees sentinel after target/junk",
        run,
    )
    .await;
    let bob_target_path = bob_dir.path().join("target/junk");
    let leaked = tokio::fs::try_exists(&bob_target_path)
        .await
        .unwrap_or(false);
    assert!(
        !leaked,
        "[run {run}] target/junk leaked to bob: {}",
        bob_target_path.display(),
    );
    // Defense in depth: Alice's filter should have blocked the
    // publish in the first place, not just relied on Bob's applier
    // filter to catch it. Check Alice's doc directly.
    let junk_key = path_to_key(alice_ws.root.as_path(), &alice_ws.root.join("target/junk"))
        .expect("path_to_key for target/junk");
    let stream = alice_ws
        .doc()
        .get_many(Query::key_exact(junk_key))
        .await
        .expect("get_many on alice's doc");
    tokio::pin!(stream);
    let alice_published_junk = stream.next().await.is_some();
    assert!(
        !alice_published_junk,
        "[run {run}] alice's filter regression: target/junk made it into the doc",
    );

    // 4. Echo-guard sanity: count Doc entries for `a.txt` on Bob's
    //    side. The applier wrote `a.txt` to disk on bob, then the
    //    watcher fired — but the echo guard should suppress
    //    re-publishing. Net effect: exactly one entry per author
    //    (Alice's), zero from Bob.
    //
    //    Note: Bob's workspace root is canonicalised (e.g. macOS
    //    rewrites `/var/...` → `/private/var/...`), so we use
    //    `bob_ws.root` for the path-to-key call.
    let bob_a_canonical = bob_ws.root.join("a.txt");
    let key = path_to_key(bob_ws.root.as_path(), &bob_a_canonical).expect("key path");
    let stream = bob_ws
        .doc()
        .get_many(Query::key_exact(key.clone()))
        .await
        .expect("get_many");
    tokio::pin!(stream);
    let mut count = 0usize;
    while let Some(res) = stream.next().await {
        let _ = res.expect("entry ok");
        count += 1;
    }
    assert_eq!(
        count, 1,
        "[run {run}] expected exactly 1 doc entry for a.txt on bob; found {count}. \
         Echo guard regression?",
    );

    // Also sanity-check the key round-trips back to the right path
    // (catches a regression where path_to_key / key_to_path drift
    // out of sync).
    let recovered = key_to_path(bob_ws.root.as_path(), &key).expect("key_to_path");
    assert_eq!(recovered, bob_a_canonical);

    alice_ws.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
    let _ = tokio::time::timeout(Duration::from_secs(5), alice_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), bob_handle).await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

/// Poll `path` until it contains `expected_payload` or the deadline
/// elapses. Panics with `who` on failure.
async fn wait_for_file(path: &std::path::Path, expected_payload: &[u8], who: &str, run: usize) {
    let deadline = Instant::now() + WAIT_BUDGET;
    loop {
        if let Ok(bytes) = tokio::fs::read(path).await
            && bytes == expected_payload
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "[run {run}] {who}: never saw expected bytes at {}",
            path.display(),
        );
        sleep(POLL_INTERVAL).await;
    }
}
