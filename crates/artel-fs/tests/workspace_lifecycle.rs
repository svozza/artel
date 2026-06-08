//! Workspace lifecycle: attach policy, ticket publish, `run()` readiness,
//! IPC attachment registration, and the `Workspace::shutdown` contract.
//!
//! Consolidated from seven per-file bins (`attach_policy_host`,
//! `attach_policy_join`, `attach_policy_state_dir_only`,
//! `host_publishes_ticket`, `run_readiness`, `workspace_attachment`,
//! `workspace_shutdown_contract`) per
//! `docs/plans/2026-05-29-faster-cargo-test-plan.md` slice 2a. Each
//! original file's docstring is retained in section banners below so
//! `git blame` from a failing test still finds the rationale.

mod common;

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use artel_client::Client;
use artel_fs::{
    AttachPolicy, KIND_V1, PolicyViolation, TICKET_ACTION, Workspace, WorkspaceAttachmentV1,
    WorkspaceConfig, WorkspaceError, WorkspaceRole, list_known_workspaces, path_to_key,
    ticket as fs_ticket,
};
use artel_protocol::{Attachment, Event, MessageKind, Request, Response, SessionId};
use futures_util::StreamExt;
use iroh::test_utils::DnsPkarrServer;
use iroh_docs::DocTicket;
use iroh_docs::store::Query;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

use common::{LocalDaemon, daemon_testing_setup, shared_dns_pkarr, testing_setup};

/// [`WorkspaceConfig::default`] with the `Testing` endpoint setup so
/// tests don't hit n0's production relay (which times out on
/// restricted networks).
async fn test_ws_config() -> WorkspaceConfig {
    let dns_pkarr = shared_dns_pkarr().await;
    WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr))
}

// =============================================================
// Local helpers
// =============================================================

/// Display name used for the host across these tests. Auth L1 fix
/// #3: the daemon stamps its own authenticated `PeerId` on
/// `Workspace::host`/`host_with` callsites, so the test fixture only
/// needs to supply a label.
const fn host_peer() -> &'static str {
    "host"
}

/// Canonicalise a test path the same best-effort way the workspace
/// constructors do (mirrors the `canonicalise` helper in
/// `crates/artel-fs/src/workspace.rs`). Tempfile paths on macOS
/// round-trip through `/private/var/...`, so any assertion comparing
/// a stored attachment path against a test-constructed path needs to
/// pass both through the same fn.
fn canon(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

/// Raw IPC list — used to verify that [`list_known_workspaces`]
/// matches a hand-rolled `ListAttachments` round-trip.
async fn raw_list(client: &Client, kind: Option<&str>) -> Vec<Attachment> {
    let resp = client
        .request(Request::ListAttachments {
            kind: kind.map(str::to_owned),
        })
        .await
        .expect("ListAttachments");
    match resp {
        Response::Attachments { entries } => entries,
        other => panic!("expected Attachments, got {other:?}"),
    }
}

// =============================================================
// `Workspace::host` honours its [`AttachPolicy`]. Three properties:
//
// 1. `RequireEmpty` against a non-empty workspace root rejects with
//    [`WorkspaceError::Policy`] **before** any iroh state lands on
//    disk. We assert by checking the would-be `state_dir` is absent
//    after the failed call — a regression that spawned the iroh node
//    before the policy check would leave `iroh.key` and `doc-id` behind.
// 2. `AllowExisting` against the same dir succeeds and adopts the
//    pre-existing files into the doc.
// 3. `RequireEmpty` against a freshly-empty dir succeeds — proves the
//    rejection is precisely scoped to non-emptiness.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn host_require_empty_rejects_non_empty_dir_without_creating_state() {
    let harness = LocalDaemon::spawn().await;
    let client = Client::connect(&harness.socket).await.unwrap();

    let ws_dir = TempDir::new().unwrap();
    tokio::fs::write(ws_dir.path().join("user-data.txt"), b"surprise!")
        .await
        .unwrap();

    let err = Workspace::host(
        &client,
        host_peer(),
        ws_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
    )
    .await
    .expect_err("RequireEmpty must reject a non-empty dir");

    match err {
        WorkspaceError::Policy(PolicyViolation::DirNotEmpty {
            offending_entries, ..
        }) => {
            assert!(
                offending_entries
                    .iter()
                    .any(|p| p.ends_with("user-data.txt")),
                "offending_entries should name user-data.txt: {offending_entries:?}",
            );
        }
        other => panic!("expected Policy(DirNotEmpty), got {other:?}"),
    }

    // Critical: the iroh state dir must not have been created. A
    // regression that spawned the iroh node before the policy check
    // would leave `iroh.key` / `doc-id` behind under `.artel-fs/`.
    let state_dir = ws_dir.path().join(".artel-fs");
    assert!(
        !state_dir.exists(),
        "policy rejection must leave no iroh state behind, but {} exists",
        state_dir.display(),
    );

    drop(client);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn host_allow_existing_publishes_pre_seeded_contents() {
    let harness = LocalDaemon::spawn().await;
    let client = Client::connect(&harness.socket).await.unwrap();
    let dns_pkarr = common::shared_dns_pkarr().await;

    let ws_dir = TempDir::new().unwrap();
    tokio::fs::write(ws_dir.path().join("README.md"), b"hello")
        .await
        .unwrap();

    let (ws, _events) = Workspace::host_with(
        &client,
        host_peer(),
        ws_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("AllowExisting should succeed against pre-seeded dir");

    // Sanity: the pre-existing file made it into the doc.
    let stream = ws
        .doc()
        .get_many(Query::single_latest_per_key())
        .await
        .expect("get_many");
    tokio::pin!(stream);
    let mut found = false;
    while let Some(res) = stream.next().await {
        let entry = res.expect("entry");
        if String::from_utf8_lossy(entry.key()).contains("README.md") {
            found = true;
            break;
        }
    }
    assert!(found, "README.md should be published into the doc");

    ws.shutdown().await.expect("shutdown");
    drop(client);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn host_require_empty_accepts_truly_empty_dir() {
    let harness = LocalDaemon::spawn().await;
    let client = Client::connect(&harness.socket).await.unwrap();
    let dns_pkarr = common::shared_dns_pkarr().await;

    let ws_dir = TempDir::new().unwrap();
    let (ws, _events) = Workspace::host_with(
        &client,
        host_peer(),
        ws_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("RequireEmpty should accept fresh empty dir");

    ws.shutdown().await.expect("shutdown");
    drop(client);
    harness.stop().await;
}

// =============================================================
// `Workspace::join` honours its [`AttachPolicy`]. Two properties:
//
// 1. `RequireEmpty` against a non-empty joiner root rejects with
//    [`WorkspaceError::Policy`] **before** any subscribe / iroh work
//    happens. We assert by checking the state dir is absent.
// 2. `InitFromExisting` on the joiner side is rejected with
//    [`PolicyViolation::InitFromExistingNotMeaningfulOnJoin`] —
//    joiners have no canonical tree to seed from, so the variant is
//    host-only by design.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn join_require_empty_rejects_non_empty_dir_without_creating_state() {
    let common::Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = common::spawn_pair().await;

    // Stand the host up so there's a real session + ticket to join
    // against. The joiner's policy rejection should fire *before* we
    // get anywhere near the host's iroh node.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_dir = TempDir::new().unwrap();
    let (alice_ws, _) = Workspace::host_with(
        &alice,
        "alice",
        alice_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::host");
    let session = alice_ws.session_id();
    let artel_ticket = alice_ws
        .join_ticket()
        .expect("host has join_ticket")
        .clone();
    let alice_ws = Arc::new(alice_ws);

    // Bob joins the artel session and tries to mount a workspace
    // into a non-empty dir.
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket: artel_ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = TempDir::new().unwrap();
    tokio::fs::write(bob_dir.path().join("local-edit.md"), b"don't clobber me")
        .await
        .unwrap();

    let err = Workspace::join(
        &bob,
        session,
        bob_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
    )
    .await
    .expect_err("RequireEmpty must reject a non-empty join target");

    match err {
        WorkspaceError::Policy(PolicyViolation::DirNotEmpty {
            offending_entries, ..
        }) => {
            assert!(
                offending_entries
                    .iter()
                    .any(|p| p.ends_with("local-edit.md")),
                "offending_entries should name local-edit.md: {offending_entries:?}",
            );
        }
        other => panic!("expected Policy(DirNotEmpty), got {other:?}"),
    }

    let state_dir = bob_dir.path().join(".artel-fs");
    assert!(
        !state_dir.exists(),
        "policy rejection must not create iroh state, but {} exists",
        state_dir.display(),
    );

    // Bob's pre-existing file must not have been touched.
    let preserved = tokio::fs::read(bob_dir.path().join("local-edit.md"))
        .await
        .expect("local-edit.md still readable");
    assert_eq!(preserved, b"don't clobber me");

    alice_ws.shutdown().await.expect("shutdown");
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn join_init_from_existing_is_rejected() {
    // No host needed — the policy check fires before the joiner
    // does any session work.
    let dns_pkarr = Arc::new(DnsPkarrServer::run().await.unwrap());
    let daemon =
        common::spawn_daemon_with_setup(common::fresh_state(), daemon_testing_setup(&dns_pkarr))
            .await;
    let client = Client::connect(&daemon.socket).await.unwrap();

    // We need *some* session id to pass to `join`, but the policy
    // check fires before any IPC, so the value is immaterial.
    let session = SessionId::new_random();

    let join_dir = TempDir::new().unwrap();
    let err = timeout(
        Duration::from_secs(5),
        Workspace::join(
            &client,
            session,
            join_dir.path().to_path_buf(),
            AttachPolicy::InitFromExisting,
        ),
    )
    .await
    .expect("InitFromExisting must reject *quickly* — the check fires before any IPC")
    .expect_err("InitFromExisting must reject on join");

    assert!(
        matches!(
            err,
            WorkspaceError::Policy(PolicyViolation::InitFromExistingNotMeaningfulOnJoin),
        ),
        "expected InitFromExistingNotMeaningfulOnJoin, got {err:?}",
    );

    drop(client);
    daemon.stop().await;
}

// =============================================================
// `RequireEmpty` accepts a workspace root whose only inhabitant is
// the workspace's own `.artel-fs/` state directory.
//
// This is the returning-host / returning-joiner case: state survives
// across restarts under `<root>/.artel-fs/`, and a strict
// `RequireEmpty` that didn't exempt the state dir would refuse to
// resume — defeating the point of persistence.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn require_empty_accepts_dir_with_only_artel_fs_state() {
    let harness = LocalDaemon::spawn().await;
    let client = Client::connect(&harness.socket).await.unwrap();

    // Pre-create the state dir with the layout a previous lifetime
    // would have left behind. Real `iroh.key` content isn't needed —
    // the workspace will recreate or load it as appropriate. The
    // test point is purely "the policy check exempts .artel-fs".
    let ws_dir = TempDir::new().unwrap();
    let state_dir = ws_dir.path().join(".artel-fs");
    tokio::fs::create_dir_all(&state_dir).await.unwrap();

    let (ws, _events) = Workspace::host_with(
        &client,
        host_peer(),
        ws_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        test_ws_config().await,
    )
    .await
    .expect("RequireEmpty should accept dir with only .artel-fs/");

    ws.shutdown().await.expect("shutdown");
    drop(client);
    harness.stop().await;
}

// =============================================================
// `Workspace::host` stands the workspace up and lands a
// `workspace.ticket` system message on the artel session.
//
// Doesn't run the watcher / applier — verifies only that:
// 1. `Workspace::host` returns successfully against an existing artel
//    session.
// 2. A second client subscribed to the same session observes a
//    `MessageKind::System` event with action [`TICKET_ACTION`] and a
//    non-empty payload.
// 3. The payload deserialises as a real [`DocTicket`].
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn host_lands_ticket_on_session() {
    let harness = LocalDaemon::spawn().await;

    let alice = Client::connect(&harness.socket).await.unwrap();

    // Stand the workspace up. The temp dir gets a single seed file
    // so we exercise the scan-and-publish path too. The workspace
    // itself owns the `HostSession` round-trip — it derives the
    // session id from the local NamespaceId and registers with the
    // daemon.
    let ws_dir = TempDir::new().unwrap();
    tokio::fs::write(ws_dir.path().join("README.md"), b"hello workspace")
        .await
        .unwrap();

    let (workspace, _ws_events) = Workspace::host_with(
        &alice,
        "alice",
        ws_dir.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        test_ws_config().await,
    )
    .await
    .expect("Workspace::host");
    let session = workspace.session_id();

    // Subscribe *after* `host` returns. The daemon replays the
    // session log on subscribe, so the `workspace.ticket` system
    // message published during `host` is still observed here.
    let _ = alice
        .request(Request::Subscribe {
            session,
            since: None,
        })
        .await
        .unwrap();
    let mut events = alice.take_events().await.expect("events");

    // Pull events until we see the ticket system message.
    let payload = timeout(Duration::from_secs(15), async {
        loop {
            let ev = events.recv().await.expect("event channel closed");
            if let Event::Message { message, .. } = ev
                && message.kind == MessageKind::System
                && message.action == TICKET_ACTION
            {
                return message.payload;
            }
            // Anything else (PeerJoined, prior Messages) is fine —
            // keep draining.
        }
    })
    .await
    .expect("workspace.ticket message never arrived");

    assert!(!payload.is_empty(), "ticket payload should be non-empty");
    // The payload is a postcard-encoded `WorkspaceTicketEnvelope`.
    // Decode it first, then assert its `doc_ticket` parses as a
    // real `DocTicket`.
    let envelope = fs_ticket::decode(&payload).expect("envelope decode");
    let _: DocTicket = DocTicket::from_str(&envelope.doc_ticket).expect("valid DocTicket");

    workspace.shutdown().await.expect("shutdown");
    drop(events);
    drop(alice);
    harness.stop().await;
}

// =============================================================
// `Workspace::run().await` is the workspace's readiness barrier.
//
// Before commit `69bb860` it was a synchronous `fn run` that
// returned a `JoinHandle` immediately, leaving callers to guess at
// the timing of two independent races:
//
// 1. The watcher's debouncer hadn't yet attached its OS-level
//    filesystem watch (`FSEvents` on macOS, inotify on Linux), so a
//    write that landed under [`Workspace::root`] right after `run()`
//    could silently miss the watcher.
// 2. The applier's `doc.subscribe()` hadn't yet returned, so a remote
//    `InsertRemote` / `ContentReady` fired in the same window was
//    lost — iroh-docs subscribers are push-to-vec, no replay.
//
// Now `Workspace::run` is `async`, awaits both halves, and only
// resolves once each is observably ready.
//
// Caveat on the applier test: under tokio's scheduler, even without
// the gate the test's next `.await` (calling `doc.status()`)
// typically yields long enough for the applier task to be polled and
// subscribe. So the test is **non-strict** against regressions — a
// regressed `run()` may still pass it on a fast scheduler. It pins
// the post-condition (the contract) but relies on the watcher test
// and the `round_trip` integration test to catch real timing
// regressions.
// =============================================================

const RUN_READINESS_POLL: Duration = Duration::from_millis(20);
const RUN_READINESS_BUDGET: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread")]
async fn watcher_attached_when_run_resolves() {
    let harness = LocalDaemon::spawn().await;
    let client = Client::connect(&harness.socket).await.unwrap();

    let ws_dir = TempDir::new().unwrap();
    let (ws, _ws_events) = Workspace::host_with(
        &client,
        "alice",
        ws_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        test_ws_config().await,
    )
    .await
    .expect("Workspace::host");
    let ws = Arc::new(ws);
    let handle = Arc::clone(&ws).run().await;

    // No settling sleep — `run().await` is the barrier. Write
    // immediately and expect the watcher to pick it up.
    let target = ws.root.join("hello.txt");
    let payload = b"watcher-ready";
    tokio::fs::write(&target, payload).await.unwrap();

    let key = path_to_key(ws.root.as_path(), &target).expect("path_to_key");
    let deadline = Instant::now() + RUN_READINESS_BUDGET;
    let mut found = false;
    while Instant::now() < deadline {
        let stream = ws
            .doc()
            .get_many(Query::key_exact(key.clone()))
            .await
            .expect("get_many");
        tokio::pin!(stream);
        if stream.next().await.is_some() {
            found = true;
            break;
        }
        sleep(RUN_READINESS_POLL).await;
    }
    assert!(
        found,
        "watcher must have published hello.txt to the doc within {RUN_READINESS_BUDGET:?} \
         — if it didn't, run().await returned before the watcher attached",
    );

    ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), handle).await;
    drop(client);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn applier_subscribed_when_run_resolves() {
    let harness = LocalDaemon::spawn().await;
    let client = Client::connect(&harness.socket).await.unwrap();
    let dns_pkarr = common::shared_dns_pkarr().await;

    let ws_dir = TempDir::new().unwrap();
    let (ws, _ws_events) = Workspace::host_with(
        &client,
        "alice",
        ws_dir.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::host");
    let ws = Arc::new(ws);

    // Snapshot the subscriber count before run — `Workspace::host`
    // does some internal subscribing of its own (e.g. iroh-docs's
    // start_sync path), so we can't assume zero. What we want to
    // assert is that the applier *adds one*.
    let pre = ws.doc().status().await.expect("status").subscribers;

    let handle = Arc::clone(&ws).run().await;

    // Immediately on return, the applier's subscriber must already
    // be registered. No polling, no sleep — this is the contract.
    let post = ws.doc().status().await.expect("status").subscribers;
    assert!(
        post > pre,
        "expected applier to add a subscriber by the time \
         run().await resolves: pre={pre}, post={post} \
         — if equal, run().await returned before the applier subscribed",
    );

    ws.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), handle).await;
    drop(client);
    harness.stop().await;
}

// =============================================================
// E2E coverage for the `artel-fs/workspace/v1` attachment slice.
//
// `Workspace::host_with` and `Workspace::join_with` register a typed
// [`WorkspaceAttachmentV1`] with the daemon as part of standing up.
// These tests pin the user-visible properties of that registration
// through the IPC boundary:
//
// - Host registers with `role: Host` after a successful attach.
// - Joiner registers with `role: Joiner` against its *own* daemon
//   (each daemon's attachment view is local).
// - The typed [`list_known_workspaces`] helper returns the same data
//   as a raw `Request::ListAttachments` round-trip.
// - The attachment survives a daemon restart at the same state dir —
//   combined with the stable-session-id slice this makes a
//   workspace's discovery entry durable across host crashes.
// - `Request::LeaveSession` cascades the attachment via the daemon's
//   2b cascade contract.
// =============================================================

#[tokio::test(flavor = "multi_thread")]
async fn host_workspace_registers_attachment_via_ipc() {
    let harness = LocalDaemon::spawn().await;
    let dns_pkarr = Arc::new(DnsPkarrServer::run().await.unwrap());

    let alice = Client::connect(&harness.socket).await.unwrap();

    let ws_root = TempDir::new().unwrap();
    let ws_state = TempDir::new().unwrap();
    // Plumb a localhost `DnsPkarrServer` so the workspace's iroh
    // node skips n0 discovery (`endpoint.online()` waits on n0
    // home-relay registration under presets::N0; that's externally
    // rate-limited and flakes in CI). No peer participates in this
    // test, so a per-test localhost server is enough to take the
    // testing preset path.
    let cfg = WorkspaceConfig::default()
        .with_state_dir(ws_state.path().to_path_buf())
        .with_endpoint_setup(testing_setup(&dns_pkarr));

    let (workspace, _events) = Workspace::host_with(
        &alice,
        "alice",
        ws_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        cfg,
    )
    .await
    .expect("Workspace::host_with");
    let session = workspace.session_id();

    // The host's `host_with` registered an attachment as part of
    // standing up. List via raw IPC and confirm exactly one entry
    // back.
    let entries = raw_list(&alice, Some(KIND_V1)).await;
    assert_eq!(entries.len(), 1, "host should register exactly one entry");
    assert_eq!(entries[0].session, session);
    assert_eq!(entries[0].kind, KIND_V1);

    let decoded = WorkspaceAttachmentV1::decode(&entries[0].payload).expect("decode payload");
    assert_eq!(decoded.role, WorkspaceRole::Host);
    // `host_with` canonicalises both `root` and `state_dir`;
    // canonicalising the test paths the same way is the only robust
    // comparison (tempfile paths on macOS round-trip through
    // `/private/var/...`).
    assert_eq!(decoded.local_path, canon(ws_root.path()));
    assert_eq!(decoded.state_dir, canon(ws_state.path()));

    workspace.shutdown().await.expect("shutdown");
    drop(alice);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn join_workspace_registers_attachment_via_ipc() {
    // Each daemon's attachment view is local — Alice sees only her
    // own host attachment, Bob sees only his own joiner attachment.
    let common::Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_root = TempDir::new().unwrap();
    let (alice_ws, _alice_events) = Workspace::host_with(
        &alice,
        "alice",
        alice_root.path().to_path_buf(),
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

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_root = TempDir::new().unwrap();
    let (bob_ws, _bob_events) = Workspace::join_with(
        &bob,
        session,
        bob_root.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join");

    // Alice's daemon: one attachment, role=Host.
    let alice_entries = raw_list(&alice, Some(KIND_V1)).await;
    assert_eq!(alice_entries.len(), 1, "alice's daemon: one attachment");
    let alice_decoded =
        WorkspaceAttachmentV1::decode(&alice_entries[0].payload).expect("alice decode");
    assert_eq!(alice_decoded.role, WorkspaceRole::Host);
    assert_eq!(alice_entries[0].session, session);

    // Bob's daemon: one attachment, role=Joiner.
    let bob_entries = raw_list(&bob, Some(KIND_V1)).await;
    assert_eq!(bob_entries.len(), 1, "bob's daemon: one attachment");
    let bob_decoded = WorkspaceAttachmentV1::decode(&bob_entries[0].payload).expect("bob decode");
    assert_eq!(bob_decoded.role, WorkspaceRole::Joiner);
    assert_eq!(bob_entries[0].session, session);

    alice_ws.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn list_known_workspaces_helper_returns_typed_view() {
    let harness = LocalDaemon::spawn().await;
    let dns_pkarr = Arc::new(DnsPkarrServer::run().await.unwrap());

    let alice = Client::connect(&harness.socket).await.unwrap();
    let ws_root = TempDir::new().unwrap();
    // Localhost `DnsPkarrServer` → workspace iroh node uses testing
    // preset, skips n0 discovery. See
    // `host_workspace_registers_attachment_via_ipc`.
    let cfg = WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr));
    let (workspace, _events) = Workspace::host_with(
        &alice,
        "alice",
        ws_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        cfg,
    )
    .await
    .expect("host_with");
    let session = workspace.session_id();

    let known = list_known_workspaces(&alice)
        .await
        .expect("list_known_workspaces");
    assert_eq!(known.len(), 1);
    assert_eq!(known[0].session, session);
    assert_eq!(known[0].attachment.role, WorkspaceRole::Host);
    assert_eq!(known[0].attachment.local_path, canon(ws_root.path()));

    workspace.shutdown().await.expect("shutdown");
    drop(alice);
    harness.stop().await;
}

// `used_underscore_binding`: this test rebuilds a fresh `DaemonState`
// from `RunningDaemon._state` to give the second daemon the same
// on-disk paths. Renaming the field would ripple through every
// fixture caller; matches `host_resume_session_id.rs`.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines, clippy::used_underscore_binding)]
async fn attachment_persists_across_daemon_restart() {
    // Stable session id (slice 1) + persistent attachment (slice 2)
    // combine to make a workspace's discovery entry durable across
    // its host daemon crashing. Re-host at the same state dir, then
    // assert `list_known_workspaces` reports the same workspace
    // entry — same session, same paths, same role.
    //
    // The localhost `DnsPkarrServer` is shared across both phases
    // (cloned into each daemon and into each workspace) so the
    // pkarr publish from phase 1 outlives the daemon restart and
    // phase 2 republishes against the same fixture.
    let dns_pkarr = Arc::new(DnsPkarrServer::run().await.unwrap());

    let alice_root = TempDir::new().unwrap();
    let alice_wstate = TempDir::new().unwrap();
    let alice_daemon_state = common::fresh_state();

    // Phase 1: host once, capture session id and attachment.
    let daemon_a1 =
        common::spawn_daemon_with_setup(alice_daemon_state, daemon_testing_setup(&dns_pkarr)).await;

    let alice_a1 = Client::connect(&daemon_a1.socket).await.unwrap();
    let cfg_1 = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_endpoint_setup(testing_setup(&dns_pkarr));
    let (alice_ws_1, _alice_events_1) = Workspace::host_with(
        &alice_a1,
        "alice",
        alice_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        cfg_1,
    )
    .await
    .expect("phase 1 host_with");
    let session_id_1 = alice_ws_1.session_id();

    let known_1 = list_known_workspaces(&alice_a1).await.expect("list 1");
    assert_eq!(known_1.len(), 1, "phase 1 should have one entry");
    let phase_1_entry = known_1.into_iter().next().unwrap();
    assert_eq!(phase_1_entry.session, session_id_1);
    assert_eq!(phase_1_entry.attachment.role, WorkspaceRole::Host);

    // Tear down: workspace then daemon. Recover the daemon's
    // on-disk paths so phase 2 can reattach to the same state.
    alice_ws_1.shutdown().await.expect("shutdown");
    drop(alice_a1);
    let alice_daemon_state_2 = common::DaemonState {
        root: daemon_a1._state.root,
        socket: daemon_a1._state.socket.clone(),
        pid: daemon_a1._state.pid.clone(),
        sessions: daemon_a1._state.sessions.clone(),
        iroh_key: daemon_a1._state.iroh_key.clone(),
    };
    daemon_a1.shutdown.trigger();
    timeout(Duration::from_secs(10), daemon_a1.join)
        .await
        .expect("daemon_a1 stop")
        .expect("daemon_a1 join")
        .expect("daemon_a1 io");

    // Phase 2: fresh daemon, same state dir, same workspace. Stable
    // session id means the attachment from phase 1 is still indexed
    // under the same `(session, kind)` key on disk.
    let daemon_a2 =
        common::spawn_daemon_with_setup(alice_daemon_state_2, daemon_testing_setup(&dns_pkarr))
            .await;

    let alice_a2 = Client::connect(&daemon_a2.socket).await.unwrap();

    // The attachment must already be visible *before* phase-2
    // host_with re-registers — that's the durability claim. Phase 2
    // re-register would overwrite (same `(session, kind)`), so reading
    // first proves the on-disk file from phase 1 is what we're
    // observing. All three fields are checked here so a daemon-side
    // regression that drops/corrupts role or state_dir on disk reload
    // is caught BEFORE phase-2 host_with rewrites the file with
    // fresh-correct bytes.
    let known_pre_register = list_known_workspaces(&alice_a2)
        .await
        .expect("list pre re-register");
    assert_eq!(
        known_pre_register.len(),
        1,
        "phase-1 attachment should survive daemon restart",
    );
    assert_eq!(known_pre_register[0].session, session_id_1);
    assert_eq!(
        known_pre_register[0].attachment, phase_1_entry.attachment,
        "all attachment fields (local_path + state_dir + role) must \
         survive daemon restart byte-for-byte",
    );

    // Re-host: confirm the constructor still succeeds against a
    // pre-existing on-disk attachment (idempotent re-register at the
    // same `(session, kind)` overwrites with identical bytes). The
    // post-register equality assertion would be tautological — phase
    // 2 just rewrote the entry — so we only check the session id is
    // stable, which is the property the constructor uniquely provides.
    let cfg_2 = WorkspaceConfig::default()
        .with_state_dir(alice_wstate.path().to_path_buf())
        .with_endpoint_setup(testing_setup(&dns_pkarr));
    let (alice_ws_2, _alice_events_2) = Workspace::host_with(
        &alice_a2,
        "alice",
        alice_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        cfg_2,
    )
    .await
    .expect("phase 2 host_with");
    assert_eq!(
        alice_ws_2.session_id(),
        session_id_1,
        "stable session id across restart",
    );

    let known_2 = list_known_workspaces(&alice_a2).await.expect("list 2");
    assert_eq!(known_2.len(), 1, "phase 2 still one entry");
    assert_eq!(known_2[0].session, session_id_1);

    alice_ws_2.shutdown().await.expect("shutdown");
    drop(alice_a2);
    daemon_a2.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn attachment_removed_on_host_leave_session() {
    let harness = LocalDaemon::spawn().await;
    let dns_pkarr = Arc::new(DnsPkarrServer::run().await.unwrap());

    let alice = Client::connect(&harness.socket).await.unwrap();
    let ws_root = TempDir::new().unwrap();
    // Localhost `DnsPkarrServer` → workspace iroh node uses testing
    // preset, skips n0 discovery. See
    // `host_workspace_registers_attachment_via_ipc`.
    let cfg = WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr));
    let (workspace, _events) = Workspace::host_with(
        &alice,
        "alice",
        ws_root.path().to_path_buf(),
        AttachPolicy::AllowExisting,
        cfg,
    )
    .await
    .expect("host_with");
    let session = workspace.session_id();

    assert_eq!(raw_list(&alice, Some(KIND_V1)).await.len(), 1);

    // Cascade: leaving the session removes the attachment via the
    // 2b `delete(session)` cascade.
    alice
        .request(Request::LeaveSession { session })
        .await
        .expect("LeaveSession");

    assert!(
        raw_list(&alice, Some(KIND_V1)).await.is_empty(),
        "LeaveSession should cascade-delete the attachment",
    );

    workspace.shutdown().await.expect("shutdown");
    drop(alice);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn attachment_removed_on_joiner_leave_session() {
    // Companion to `attachment_removed_on_host_leave_session`. When
    // the joiner leaves its remote-mirror session, the daemon drops
    // the mirror entirely (the only local consumer is gone, so
    // there's nothing left to mirror) and the 2b cascade clears the
    // joiner's attachment via `store.delete(session)`. Symmetric in
    // shape with `host_closed_session` — same teardown, just
    // triggered by a local IPC leave instead of a gossip
    // `SessionClosed` from the host.
    //
    // The host's own daemon is NOT affected: sessions are per-daemon,
    // and the joiner's local-mirror leave is not visible upstream.
    let common::Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_root = TempDir::new().unwrap();
    let (alice_ws, _alice_events) = Workspace::host_with(
        &alice,
        "alice",
        alice_root.path().to_path_buf(),
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

    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let resp = bob
        .request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_root = TempDir::new().unwrap();
    let (bob_ws, _bob_events) = Workspace::join_with(
        &bob,
        session,
        bob_root.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
    )
    .await
    .expect("Workspace::join");

    // Sanity: each daemon has exactly its own attachment.
    assert_eq!(raw_list(&alice, Some(KIND_V1)).await.len(), 1);
    assert_eq!(raw_list(&bob, Some(KIND_V1)).await.len(), 1);

    bob.request(Request::LeaveSession { session })
        .await
        .expect("bob LeaveSession");

    assert!(
        raw_list(&bob, Some(KIND_V1)).await.is_empty(),
        "bob's joiner attachment must cascade-delete on his LeaveSession",
    );

    // Alice's host attachment is on a different daemon — fully
    // untouched by bob's leave.
    let alice_entries = raw_list(&alice, Some(KIND_V1)).await;
    assert_eq!(
        alice_entries.len(),
        1,
        "alice's host attachment must NOT be affected by bob leaving",
    );
    assert_eq!(alice_entries[0].session, session);

    alice_ws.shutdown().await.expect("shutdown");
    bob_ws.shutdown().await.expect("shutdown");
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

// =============================================================
// `Workspace::shutdown` contract pinned by the Tier-2 review trio
// (originals #2 + #3 + #4 in `docs/handoff-code-review-fixes.md`).
//
// Three properties:
//
// - **Idempotent on the empty slot.** A second `shutdown()` after a
//   successful first call returns `Ok(())` without trying to tear the
//   (now-absent) iroh node down a second time. Pre-fix this path also
//   unconditionally armed the Drop-bomb sentinel regardless of whether
//   a node was actually consumed; a partially-constructed Workspace
//   whose rollback already took the node could convince the bomb to
//   stay quiet on Drop.
// - **Failure surfaces, sentinel stays unarmed.** With the
//   `test-utils` fault knob armed, the inner `WorkspaceNode::shutdown`
//   returns an error; the outer `Workspace::shutdown` propagates it
//   instead of silently logging and arming the sentinel.
// - **Concurrent shutdowns serialise.** Two `shutdown()` futures
//   awaited via `tokio::join!` both return `Ok(())`. The inner
//   `node.shutdown().await` must NOT race: the lock is held across the
//   await, so the second caller observes an empty slot only after the
//   first caller finished tearing the node down.
// =============================================================

/// Generous-enough deadline for any single `shutdown()` to finish
/// against the localhost fixture. A regression that drops the
/// hold-the-lock-across-await invariant would manifest as either a
/// hang here or a very-fast-return.
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread")]
async fn second_shutdown_call_is_a_noop_and_returns_ok() {
    let common::Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_root = TempDir::new().unwrap();
    let cfg = WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr));
    let (ws, _events) = Workspace::host_with(
        &alice,
        "alice",
        alice_root.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        cfg,
    )
    .await
    .expect("Workspace::host_with");

    // First call: takes and tears down the node, arms the sentinel.
    timeout(SHUTDOWN_BUDGET, ws.shutdown())
        .await
        .expect("first shutdown finished within budget")
        .expect("first shutdown returned Ok");

    // Second call: slot already empty. Must still return Ok and must
    // not hang — pre-fix this path was an `if let / else` whose else
    // arm fell through to the unconditional sentinel store; same
    // observable result for `Ok` callers, but the consumer who calls
    // shutdown twice deserves a stable "yes, it's down" answer
    // either way.
    timeout(SHUTDOWN_BUDGET, ws.shutdown())
        .await
        .expect("second shutdown finished within budget")
        .expect("second shutdown returned Ok");

    drop(ws);
    drop(alice);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_propagates_router_failure_and_keeps_node_consumable() {
    let common::Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_root = TempDir::new().unwrap();
    let cfg = WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr));
    let (ws, _events) = Workspace::host_with(
        &alice,
        "alice",
        alice_root.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        cfg,
    )
    .await
    .expect("Workspace::host_with");

    // Arm THIS workspace's inner-shutdown to return an error. The
    // fault injection still drains the real router (best-effort) so
    // we don't leak the endpoint into the next test, but the outer
    // `Workspace::shutdown` sees an `Err` and must NOT arm
    // `did_shutdown`. Per-instance, so a parallel test in this
    // binary running its own shutdown won't trip our fault.
    ws.test_arm_shutdown_failure()
        .await
        .expect("workspace node still in slot when arming fault");
    let err = timeout(SHUTDOWN_BUDGET, ws.shutdown())
        .await
        .expect("forced-fail shutdown finished within budget")
        .expect_err("forced-fail shutdown returned Err");
    match err {
        WorkspaceError::Iroh(msg) => {
            assert!(
                msg.contains("test-utils fault injection"),
                "expected fault-injection error, got: {msg}",
            );
        }
        other => panic!("expected WorkspaceError::Iroh from fault injection, got: {other:?}"),
    }

    // The fault was single-shot and the inner shutdown takes
    // `self`, so the slot was emptied even on the failure path. A
    // second `shutdown()` therefore observes an empty slot and
    // returns Ok. The thing we're really pinning is that
    // `did_shutdown` was NOT armed by the failed first call;
    // surfacing the `Err` to the caller is the contract this
    // assertion locks in directly. (`tests/drop_bomb.rs` pins the
    // Drop-bomb-on-unset-flag side; a stderr child-process variant
    // for "bomb fires after Err shutdown" is finding #10's
    // territory and out of scope here.)
    timeout(SHUTDOWN_BUDGET, ws.shutdown())
        .await
        .expect("recovery shutdown finished within budget")
        .expect("recovery shutdown returned Ok");

    // Expected stderr noise: dropping `ws` here fires the Drop
    // bomb, because the *first* shutdown call returned Err and the
    // sentinel was correctly left unarmed. That's the property — a
    // caller who logged-and-ignored a failed shutdown still gets
    // the loud Drop message. The "[artel-fs] Workspace dropped
    // without calling shutdown()" line in test output is intentional.
    drop(ws);
    drop(alice);
    daemon_a.stop().await;
    daemon_b.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_shutdowns_both_return_ok_within_budget() {
    let common::Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_root = TempDir::new().unwrap();
    let cfg = WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr));
    let (ws, _events) = Workspace::host_with(
        &alice,
        "alice",
        alice_root.path().to_path_buf(),
        AttachPolicy::RequireEmpty,
        cfg,
    )
    .await
    .expect("Workspace::host_with");
    let ws = Arc::new(ws);

    // `tokio::join!` polls both arms cooperatively. The mutex is
    // held across the inner await, so caller B blocks until A
    // releases the guard; B then sees the empty slot and returns
    // `Ok(())`. The pre-fix code released the lock after
    // `slot.take()`, letting B return *while A was still in*
    // `router.shutdown`. Both arms reporting Ok within the budget
    // is the post-fix contract.
    let ws_a = Arc::clone(&ws);
    let ws_b = Arc::clone(&ws);
    let (a, b) = timeout(SHUTDOWN_BUDGET, async move {
        tokio::join!(ws_a.shutdown(), ws_b.shutdown())
    })
    .await
    .expect("both concurrent shutdowns finished within budget");
    a.expect("concurrent shutdown A returned Ok");
    b.expect("concurrent shutdown B returned Ok");

    // Belt-and-braces: a third sequential shutdown still returns Ok.
    // Without the lock-across-await invariant it could panic on a
    // double-take.
    timeout(SHUTDOWN_BUDGET, ws.shutdown())
        .await
        .expect("third shutdown finished within budget")
        .expect("third shutdown returned Ok");

    drop(ws);
    drop(alice);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
