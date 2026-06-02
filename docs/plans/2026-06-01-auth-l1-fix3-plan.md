# Auth L1 Fix #3 — strip `peer.id` from `JoinSession` / `HostSession` — plan

Source brainstorm: `docs/brainstorms/2026-06-01-auth-l1-fix3-shape.md`.
Picks **Option C**: drop the `peer: PeerInfo` field from
`Request::JoinSession` and `Request::HostSession`, replace with
`display_name: String`, bump `PROTOCOL_VERSION` 4 → 5. The daemon
stamps its own authenticated id internally. No compat shim. This
plan is *how*, not *what*.

Closes the IPC-side complement of auth L1: A1 enforces
`body.peer.id == delivered_from` on the gossip arms; this fix
removes the IPC field whose only correct value is the daemon's
authenticated id. Together they make spoofed `peer.id` structurally
unrepresentable.

## Sub-slice ordering

One landing — wire change forces every consumer to recompile
together; there's no in-between state. Inside the slice, three
intrinsic sub-tasks each end with `make test` + `make clippy`
clean both feature modes (default and `--all-features`). Each
sub-task is a separate commit; a fresh agent picking up between
sub-tasks finds the workspace green at the previous commit.

- **B1 — Wire surface change.** `artel-protocol` shape change +
  `PROTOCOL_VERSION` 4 → 5 + daemon dispatch + `Registry::join`
  idempotent self-rejoin + `AlreadyJoined` removal.
- **B2 — Consumer migrations.** `artel-fs::Workspace::host` /
  `host_with` narrow from `peer: PeerInfo` to `display_name`;
  same for `join` / `join_with` — but those don't currently take
  a `peer` (joiner-side membership is a precondition; the
  caller's IPC client issued `JoinSession` already), so this
  half is *only* deletion of the trailing references in doc
  comments.
- **B3 — Test rewrites.** Boundary-crossing tests
  (`tests/sessions.rs::two_clients_chat_end_to_end`,
  `client.rs::events_stream_delivers_message_events`,
  3 in-module tests in `session.rs::tests` keyed off the same
  fiction) become 2-daemon `Pair` tests where they cross the
  IPC boundary; in-module Registry unit tests keep their
  existing shape (Registry's pure-Rust API still takes
  `PeerInfo`).

The `PROTOCOL_VERSION` bump is bookkeeping. Pre-1.0 we have no
on-the-wire compatibility surface to defend; old and new daemons
or clients are not expected to interoperate. Same posture as the
parent A1 plan.

---

## Sub-slice B1 — Wire surface change

**Goal:** Remove `peer: PeerInfo` from `Request::JoinSession`
and `Request::HostSession`. Add `display_name: String` to each.
Bump `PROTOCOL_VERSION` to 5. Daemon's `dispatch` constructs
`PeerInfo` internally from `(bridge.authenticated_peer_id(),
display_name)` before calling into `Registry`. `Registry::join`
becomes idempotent on self-rejoin (returns existing-record
response, emits no second `PeerJoined`); `Registry::host`
self-resume already short-circuits and stays as-is. Remove the
now-unreachable `SessionError::AlreadyJoined` /
`ProtocolError::AlreadyJoined` variants.

### Files touched

- `crates/artel-protocol/src/rpc.rs`:
  - `Request::HostSession`: drop `peer: PeerInfo`; add
    `display_name: String`. Update doc comment to name the
    invariant: "the host's display name only — the daemon
    stamps its own authenticated `PeerId` (= iroh
    `EndpointId`); the IPC caller cannot influence the
    on-the-wire id." Cross-link the auth-L1 brainstorm + this
    plan.
  - `Request::JoinSession`: same treatment; doc comment names
    the same invariant.
  - `Response::JoinSession` is unchanged. The daemon already
    returns the assigned `SessionId` and head; callers that
    need their own `peer.id` already read it from
    `Response::Hello { daemon_peer_id }`. No new field.
  - Update the `proptest` `arb_request` arms (rpc.rs:851,
    rpc.rs:856) to construct the new shape with `arb_display_name`
    (just `"[\\PC]{0,128}"` reuse).
  - Update the existing serde / postcard unit tests at
    rpc.rs:477, rpc.rs:488, rpc.rs:504 to construct the new
    shape.

- `crates/artel-protocol/src/version.rs`:
  - Bump `PROTOCOL_VERSION` from `ProtocolVersion::new(4)` to
    `ProtocolVersion::new(5)`.
  - Rename `current_protocol_version_is_four` test to
    `current_protocol_version_is_five`; update its assertions
    (4 → 5).

- `crates/artel-protocol/src/error.rs`:
  - Remove `ProtocolError::AlreadyJoined(SessionId)`. Its sole
    legitimate trigger was "joiner-on-same-daemon as host
    rejoins"; under C, the daemon's authenticated id IS the
    host's id when the host issued `HostSession` from the
    same daemon, so the join short-circuits idempotently in
    `Registry::join` (see below) and never surfaces
    `AlreadyJoined`. The cross-daemon case "remote joiner
    rejoins" is genuinely different — the joiner's
    authenticated id is added the first time and the second
    `JoinSession` from the same authenticated id is also a
    self-rejoin, idempotent.
  - Remove the slug, the proptest arm, the round-trip test
    case, and the `AlreadyJoined` arms in any conversion
    code (server.rs:947, gossip_bridge.rs:873, session.rs:52,
    session.rs:105).

- `crates/artel-daemon/src/server.rs`:
  - `dispatch` (line ~543): the two arms change shape.
    ```rust
    Request::HostSession { display_name, session } => {
        let peer = PeerInfo {
            id: bridge.authenticated_peer_id(),
            display_name,
        };
        match registry.host(peer.clone(), session).await {
            Ok((session, ticket)) => {
                memberships.insert(session, peer);
                Response::HostSession { session, ticket }
            }
            Err(err) => Response::Error {
                error: session_error_to_protocol(&err),
            },
        }
    }
    Request::JoinSession { display_name, ticket } => {
        let peer = PeerInfo {
            id: bridge.authenticated_peer_id(),
            display_name,
        };
        match registry.join(&ticket, peer.clone()).await {
            Ok((session, head)) => {
                memberships.insert(session, peer);
                Response::JoinSession { session, head }
            }
            Err(err) => Response::Error {
                error: session_error_to_protocol(&err),
            },
        }
    }
    ```
  - `dispatch`'s signature gains `bridge: &Arc<GossipBridge>`
    so it can read the authenticated id. Today the bridge
    lives on the `Daemon` struct (server.rs:201 area); the
    caller into `dispatch` is `handle_connection` (further up
    in `server.rs`) which already has access to the
    `Daemon`. Pass the bridge through.
  - Under `cfg(not(feature = "iroh"))` the bridge doesn't
    exist; the dispatch path uses `SYNTHETIC_LOCAL_PEER_ID`
    (already exported from server.rs per the A2 plan) as
    the authenticated id stand-in. Add a thin
    `authenticated_peer_id()` helper at module scope that
    returns the bridge's id under iroh and the constant
    otherwise. (Two tiny `cfg`s rather than threading a
    different type.)
  - Remove the `SessionError::AlreadyJoined` arm at
    server.rs:947.

- `crates/artel-daemon/src/session.rs`:
  - `Registry::join` (line ~389): replace the
    `AlreadyJoined` early-return with an idempotent
    short-circuit. Shape:
    ```rust
    let mut s = session.lock().await;
    if s.members.contains(&peer.id) {
        // Self-rejoin: caller's authenticated id is already
        // a member. Daemon-side membership is per-
        // authenticated-identity (persistent across
        // consumer remounts); a re-host or re-join from the
        // same daemon is a no-op. No second PeerJoined.
        let head = if s.head == Seq::ZERO { None } else { Some(s.head) };
        return Ok((session_id, head));
    }
    self.store
        .add_member(session_id, &peer)
        .await
        .map_err(SessionError::Storage)?;
    s.members.insert(peer.id);
    let _ = s.events_tx.send(Event::PeerJoined { session: session_id, peer });
    ```
    Net effect: the early-return on `members.contains` no
    longer errors — it returns `Ok` with the existing head.
    The store write and `PeerJoined` emission happen only on
    a genuine first-time join.
  - `Registry::host`'s resume path at session.rs:306-340 is
    already idempotent for `(host_peer.id == s.host && kind
    == Local)` — no change needed. The
    `SessionError::SessionConflict` path at session.rs:313
    stays (it covers "different host claims the same id" and
    "remote-mirror id collides with a local request" — both
    legitimate rejection paths under C).
  - Remove `SessionError::AlreadyJoined` (line 52, line 105,
    line 429, and the conversion at the top of the daemon).
  - Update the `tests::join_twice_errors` unit test
    (session.rs:1346-1352): rename to
    `join_twice_is_idempotent` and assert the second join
    returns `Ok` with the same head as the first; assert
    the session's `members` set still contains exactly one
    entry for that peer.

- `crates/artel-daemon/src/gossip_bridge.rs`:
  - Remove the `SessionError::AlreadyJoined` arm at
    gossip_bridge.rs:873. (The conversion site only needs an
    arm for variants that still exist.)

- `crates/artel-protocol/src/transport/codec.rs`,
  `transport/framed.rs`, `transport/client.rs`:
  - Search for `Request::HostSession {` and `Request::JoinSession {`
    constructors; update each to the new shape. These are unit-
    test scaffolds; mechanical edits.

### Tests added (B1)

Unit tests in `crates/artel-protocol/src/rpc.rs::tests`:
- `host_session_request_postcard_round_trip_uses_new_shape` —
  construct `Request::HostSession { display_name: "alice".into(),
  session: None }`, postcard round-trip, assert equality. (Soft
  but pinned by type.)
- `join_session_request_postcard_round_trip_uses_new_shape` —
  same for join.
- `host_session_proptest_round_trip` / `join_session_proptest_round_trip`
  — proptest arms already exist; just confirm they exercise the
  new shape.

Unit tests in `crates/artel-daemon/src/session.rs::tests`:
- `join_twice_is_idempotent` (renamed from `join_twice_errors`)
  — host once, join twice with same `peer`, assert second
  return is `Ok((id, head))` with `head == None` and
  `members.len() == 2` (host + joiner; joiner not duplicated).
- `host_then_self_join_via_same_id_is_idempotent` — host,
  then the same `peer` joins via the host's own ticket.
  Asserts `Ok` with the existing head; no `AlreadyJoined`
  variant exists to error with.

E2E test in `crates/artel-daemon/tests/auth_l1_spoofing.rs`:
- `joiner_local_membership_uses_authenticated_id` (the test
  named in the handoff at lines 196-198 — it was sound in the
  failed attempt and survives unchanged here). Spin a `Pair`,
  Bob joins from his daemon. Hand-rolling Alice's
  `Subscribe { session, since: None }` reads
  `Event::PeerJoined { peer }`; assert `peer.id ==
  bob_daemon.peer_id()` regardless of what `display_name`
  Bob's IPC client passed.

### Definition of done (B1)

1. `Request::HostSession` and `Request::JoinSession` carry
   `display_name: String`, no `peer` field.
2. `PROTOCOL_VERSION == 5`.
3. Daemon's `dispatch` constructs `PeerInfo` from the bridge's
   authenticated id; IPC callers cannot influence the on-wire
   id.
4. `Registry::join` is idempotent on self-rejoin; the second
   call returns `Ok` and emits no second `PeerJoined`.
5. `SessionError::AlreadyJoined` and
   `ProtocolError::AlreadyJoined` are removed.
6. New unit + e2e tests pass; existing tests in both feature
   modes still compile (B2 / B3 may be needed before they pass
   — see the inter-commit gating note below).
7. fmt + clippy clean both feature modes.

**Commit subject:** `daemon: drop peer.id from Join/HostSession; daemon stamps authenticated id; idempotent self-rejoin (auth L1 fix #3, PROTOCOL_VERSION 5)`

**Inter-commit gating note:** B1 alone breaks `artel-fs`'s
two callsites at workspace.rs:1253 (`Request::HostSession {
peer, session: ... }`) and any test that builds the old shape.
Land B1 + B2 in one continuous session — don't push between
them. CI for B1 alone will not be green; that's expected. See
`docs/handoff-auth-l1-review-fixes.md` lines 41-58 for the
make-targets to run; running them between sub-tasks is fine
to confirm progress, but the green gate is at the end of B3.

---

## Sub-slice B2 — Consumer migrations

**Goal:** Update `artel-fs::Workspace::host` / `host_with`
signatures to take `display_name: impl Into<String>` instead
of `peer: PeerInfo`. Update doc comments. Update internal
call sites (`register_host`, etc.) to pass through. Drop the
`peer` parameter from joiner-side internals (it was already
unused after A1's bridge stamping but the signature still
mentioned it).

### Why narrow `Workspace::host_with`

The brainstorm's open question recommended (a) — narrow the
`Workspace` API to mirror the IPC narrowing. Same logic: a
field whose only correct value is the one the daemon already
knows should not be on the consumer-facing surface either.
Otherwise embedders can pass a hand-rolled `PeerInfo` whose
`id` differs from `client.daemon_peer_id()` and the resulting
mismatch is silent at the workspace layer (it's just dropped
on the floor before the IPC).

### Files touched

- `crates/artel-fs/src/workspace.rs`:
  - `Workspace::host` (line ~401):
    ```rust
    pub async fn host(
        client: &Client,
        display_name: impl Into<String>,
        root: PathBuf,
        policy: AttachPolicy,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError> {
        Self::host_with(client, display_name, root, policy, WorkspaceConfig::default()).await
    }
    ```
  - `Workspace::host_with` (line ~428): same narrowing.
    Internally collect into `let display_name =
    display_name.into();` once near the top.
  - `Workspace::host_with_inner` (line ~494): take
    `display_name: String`.
  - `register_host` (line ~1247): take `display_name: String`,
    issue `Request::HostSession { display_name, session: Some(session_id) }`.
  - `Workspace::join` and `join_with`: signatures don't take
    `peer` today (the joiner's IPC client already issued
    `JoinSession` before constructing the Workspace; see the
    `Joiner-side note` doc-comment at workspace.rs:1176-1181).
    Only change is doc-comment refresh: update references to
    `Request::JoinSession`'s `peer` field to refer to
    `display_name` instead.
  - Search `peer: PeerInfo,` in workspace.rs (one stray param
    reference at line 1249); migrate.
  - Doc comments at workspace.rs:363 (`On the joiner: whatever
    id the daemon's [Request::JoinSession]...`) and
    workspace.rs:388 (`Request::HostSession { peer, session }`)
    update to name `display_name`.

- `examples/chat-harness/src/main.rs` (line 211, local-only,
  uncommitted per `.gitignore`):
  - Today: `let alice_peer = PeerInfo::new(client.daemon_peer_id(), &cli.name);`
    then later passes that to `Workspace::host` /
    `Request::HostSession`.
  - Migrate: drop the `PeerInfo::new` line, pass `cli.name`
    (or `&cli.name`) directly to `Workspace::host`. The
    daemon stamps the same `daemon_peer_id` server-side.
  - This file is a local artifact — fix locally, never
    `git add`. Worth doing because keeping it compileable
    in-tree is part of the "the workspace as a whole
    builds" definition-of-done.

- Consumer-facing doc-comment refresh in
  `crates/artel-fs/src/workspace.rs:636` and
  workspace.rs:857. Mechanical.

### Tests added (B2)

Unit test in `crates/artel-fs/src/workspace.rs::tests`
(or whichever module currently smoke-tests `host_with`):
- `host_with_accepts_display_name_directly` — call with a
  plain `&str`, assert the resulting `WorkspaceEvent` /
  ticket is well-formed. (Mostly an API-shape pin: catches
  a future refactor that re-broadens the signature.)

No new e2e tests in B2 — the surface change is exercised by
every existing `host`/`join` test once they're migrated in
B3.

### Definition of done (B2)

1. `Workspace::host`, `host_with`, `host_with_inner`,
   `register_host` take `display_name: String` (or
   `impl Into<String>` at the public surface).
2. `Workspace::join` / `join_with` doc comments are refreshed
   (no signature change).
3. `examples/chat-harness/src/main.rs` builds locally
   (uncommitted).
4. `cargo build --workspace` and `cargo build --workspace
   --all-features` are clean.

**Commit subject:** `artel-fs: narrow Workspace::host to display_name (auth L1 fix #3)`

---

## Sub-slice B3 — Test rewrites

**Goal:** Migrate every test that constructs `Request::HostSession {
peer, ... }` or `Request::JoinSession { peer, ... }`. Two patterns:

1. **Tests that crossed the IPC boundary with two PeerInfos
   on one daemon (the production fiction).** These are 5
   tests named in the handoff. Rewrite as 2-daemon `Pair`
   tests so each "client" has its own authenticated id.
2. **Tests that just need a daemon to host + a daemon to
   join, single-PeerInfo-per-daemon.** Mechanical edit:
   replace `peer: PeerInfo::new(..., name)` with
   `display_name: name.into()`.

The pure-Rust `session.rs::tests` module (Registry-against-
MemoryStore tests, lines ~1100-1900) keeps its existing
shape — those tests don't cross the IPC boundary; they test
`Registry::join` / `Registry::host` with arbitrary
`PeerInfo` directly, which is a perfectly faithful unit
test of the registry surface (which still takes `PeerInfo`).
The single semantic change there is renaming
`join_twice_errors` to `join_twice_is_idempotent` (covered
in B1).

### Files touched

The full grep list (rg `Request::JoinSession` /
`Request::HostSession`) covers ~70 callsites. Most are
mechanical renames. Group by file:

- **B3.a Pair-rewrite (5 tests, IPC-boundary crossings).**
  - `crates/artel-daemon/src/session.rs::tests`:
    `join_artel_ticket_succeeds_and_emits_peer_joined`,
    `joiner_leave_local_session_keeps_session_alive`,
    `member_leave_emits_peer_left_and_keeps_session`. These
    are *in-module* registry tests, NOT IPC tests — the
    framing in the handoff was misleading. They construct
    `Registry::join(&ticket, peer)` directly and DON'T cross
    the IPC boundary. They stay as registry-level tests with
    arbitrary `PeerInfo` (registry's signature is unchanged).
    The "fiction" they simulate is fine *at the registry
    layer*: registry has no way to know two `PeerInfo`s are
    "different IPC clients of one daemon" because that
    information lives only at the IPC dispatch layer. So
    they don't need rewrites — only the
    `join_twice_errors` → `join_twice_is_idempotent` rename
    in B1 affects them.

    **This means the brainstorm's "5 tests rewritten as
    Pair" estimate was too aggressive.** The actual count is
    **2** — the IPC-crossing pair:
  - `crates/artel-daemon/tests/sessions.rs::two_clients_chat_end_to_end`
    (line 324). Today: one daemon, two `Client::connect`s,
    Alice IPC client + Bob IPC client share the daemon's id.
    Rewrite: spin a `Pair` (`common::spawn_pair`), Alice
    hosts on daemon_a, Bob joins from daemon_b. Read each
    side's authenticated id via `daemon.peer_id()`;
    construct expectations against those. The rest of the
    test (Bob sends; Alice observes; Bob leaves; Alice
    observes `PeerLeft`; Alice leaves; `SessionClosed`) is
    structurally unchanged.
  - `crates/artel-client/tests/client.rs::events_stream_delivers_message_events`
    (line ~213). Today: hand-rolled `alice()` and `bob()`
    PeerInfo helpers (lines 89-95) wired into a single
    daemon's two clients. Rewrite: `spawn_pair`-based, two
    daemons, each gets its own `Client::connect`. Drop the
    `alice()` / `bob()` helpers — they were the embodiment
    of the fiction.

- **B3.b Mechanical rename (everything else).**
  ~65 callsites. Per file:
  - `crates/artel-daemon/tests/identity.rs` (3 sites,
    98/191/213/358/378). Each test already operates with one
    daemon-per-actor; just swap `peer: PeerInfo::new(...)`
    for `display_name: name.into()`. Drop the now-unused
    `PeerInfo::new` constants (lines 99, 189, 208, 355, 375).
  - `crates/artel-daemon/tests/gossip.rs` (~14 sites). Same
    treatment. Each test already pairs daemons; the
    `let alice = PeerInfo::new(...);` lines become
    `let alice_name = "alice";` (or just inline the literal).
  - `crates/artel-daemon/tests/attachments.rs` (1 site).
    Trivial.
  - `crates/artel-daemon/tests/auth_l1_spoofing.rs`
    (4 sites). Already uses `daemon.peer_id()`; just rename
    the IPC field. Existing tests pin the wire-stamping
    invariant — those assertions stay.
  - `crates/artel-fs/tests/workspace_lifecycle.rs`
    (~16 sites). Same treatment. The `host_peer()` and
    `alice_peer` / `bob_peer` helpers become
    `host_name() -> &'static str` / inline string literals.
  - `crates/artel-fs/tests/workspace_filter.rs` (~13 sites).
    Same.
  - `crates/artel-fs/tests/workspace_restart.rs` (~7 sites).
    Same.
  - `crates/artel-fs/tests/workspace_sync.rs` (~5 sites).
    Same.
  - `crates/artel-fs/tests/crash_recovery.rs` (1 site at
    line 191). The handoff specifically called this test
    out as the failure-mode for the previous attempt. Under
    the new idempotent-self-rejoin behaviour (B1), Bob's
    phase-2 rejoin against daemon_b's stale-from-phase-1
    member entry is a no-op (`Ok` with existing head). The
    test should pass once renamed. Add an assertion: the
    second join returns `head: Some(_)` (because phase 1
    sent at least one message before the crash) to pin the
    "you get the head, not None" property.
  - `crates/artel-daemon/tests/sessions.rs` (~10 sites
    excluding `two_clients_chat_end_to_end`). Each test that
    builds a single `PeerInfo::new` per daemon: rename. Tests
    that build two `PeerInfo`s for a single daemon (a few
    more besides the named one): same `Pair` rewrite as B3.a.
    Audit list: lines 433-471 (`two_subscribers_share_session`,
    based on the visible context — confirm during edit), lines
    513-624 (multiple Send tests), 657-700 (host-leave
    mechanics), 798-820 (other multi-actor scenarios). For
    each: if the test's pre-condition is two distinct
    authenticated ids, rewrite as `Pair`; if the test only
    needs one daemon plus a separate side-channel actor (like
    a hand-crafted gossip frame), it can stay single-daemon.
  - `crates/artel-fs/tests/iroh_internals.rs` (1 site at
    493). Single-daemon mechanical rename.
  - `crates/artel-daemon/src/store/fs.rs::tests` (lines 852,
    903). These are Store-layer tests; `PeerInfo` is
    constructed for a hand-built `Session::new` — they
    don't touch the IPC at all. They're unaffected by the
    IPC rename. Leave them.

### New regression test (B3.c)

In `crates/artel-daemon/tests/sessions.rs`, add:

```rust
#[tokio::test]
async fn repeated_join_against_same_daemon_is_idempotent() {
    let (daemon_a, daemon_b, _dns) = common::spawn_pair().await;

    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let bob = Client::connect(&daemon_b.socket).await.unwrap();

    // Alice hosts; Bob joins; assertions on PeerJoined fire once.
    let (session, ticket, mut alice_events) = host_and_watch(&alice, "alice").await;
    bob.request(Request::JoinSession {
        display_name: "bob".into(),
        ticket: ticket.clone(),
    }).await.unwrap();
    let first = next_event(&mut alice_events).await;
    assert!(matches!(first, Event::PeerJoined { peer, .. } if peer.id == daemon_b.peer_id()));

    // Bob's second join — same authenticated id, same daemon.
    let resp = bob.request(Request::JoinSession {
        display_name: "bob".into(),
        ticket,
    }).await.unwrap();
    match resp {
        Response::JoinSession { session: got, .. } => assert_eq!(got, session),
        other => panic!("expected JoinSession, got {other:?}"),
    }

    // No second PeerJoined fires within a 500ms ceiling.
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(
            deadline.saturating_duration_since(std::time::Instant::now()),
            alice_events.recv(),
        ).await {
            Ok(Some(Event::PeerJoined { .. })) => {
                panic!("self-rejoin emitted a second PeerJoined")
            }
            Ok(Some(_)) | Ok(None) => break,
            Err(_) => {}
        }
    }

    daemon_a.stop().await;
    daemon_b.stop().await;
}
```

Pins the load-bearing semantic of B1's idempotent-self-rejoin
change. Lives next to the migrated
`two_clients_chat_end_to_end` so a future reader sees both
patterns side-by-side.

### Definition of done (B3)

1. Every `Request::HostSession {` and `Request::JoinSession {`
   constructor in the workspace uses `display_name`, not
   `peer`.
2. The 2 IPC-boundary-crossing tests
   (`two_clients_chat_end_to_end`,
   `events_stream_delivers_message_events`) are rewritten
   as `Pair` tests using `daemon.peer_id()` for assertions.
3. `repeated_join_against_same_daemon_is_idempotent` is
   added to `tests/sessions.rs`.
4. `make ci-local` is green: fmt + clippy both feature
   modes + nextest workspace + Tier C real-n0 + doctests.
5. `crates/artel-fs/tests/crash_recovery.rs::steady_state_sigkill_preserves_state`
   is green without modification — the new idempotent-
   self-rejoin closes the failure mode named in the handoff.

**Commit subject:** `tests: migrate to display_name; add idempotent-self-rejoin regression (auth L1 fix #3)`

---

## Cross-cutting concerns

### Things this plan explicitly does not do

- **No compat shim for `Request` shape.** Pre-1.0; no v4
  fallback parser; no `#[serde(default)]`; no env-var
  override. Same posture as the parent A1 plan.
- **No new `ProtocolError` variant.** Option B (typed
  `IdMismatch`) was rejected; no error type to police a
  field that no longer exists.
- **No `Workspace::shutdown` change.** The brainstorm
  picked "idempotent self-rejoin" over "Workspace emits
  LeaveSession on shutdown". `Workspace::shutdown` keeps
  its current shape (cancels token, tears down iroh node,
  no IPC LeaveSession). Daemon-side membership is per-
  authenticated-identity, persistent across consumer
  remounts.
- **No `Registry` API change.** Registry still takes
  `PeerInfo` (the IPC layer is what loses the field).
  Existing in-module unit tests against arbitrary
  `PeerInfo` are still meaningful — they exercise
  registry behaviour with synthesised inputs.
- **No new daemon-CLI flag, env var, or runtime config.**
- **No L2 / L3 work.** Slices B and C of the v1 auth
  story (per-message signing, capability events) remain
  out of scope per the parent brainstorm.
- **No `Workspace::host` / `host_with` parameter
  reordering.** The narrowing replaces `peer: PeerInfo`
  in-place with `display_name: impl Into<String>`. Other
  parameters keep their positions.
- **No deletion of `PeerInfo`.** The type still ships in
  `Event::PeerJoined`, `SessionMessage`, and gossip frame
  bodies (all daemon-emitted, not IPC-supplied). It just
  stops appearing in `Request` variants.

### Risks

1. **The handoff's "5 tests rewrite" count overshoots.**
   The plan above re-audited and found only 2 IPC-crossing
   tests; the in-module `session.rs::tests` are registry-
   level and unaffected by the IPC rename. If during
   implementation a test surfaces that I missed, drop it
   into B3.a, not B3.b. Trip wire: `rg "let .*_client = Client::connect"`
   inside `crates/artel-daemon/tests/` and
   `crates/artel-client/tests/` should return only the
   2 named tests with two `Client::connect` calls against
   one daemon. If more pop up, audit them individually.

2. **`crash_recovery::steady_state_sigkill_preserves_state`
   semantics.** The handoff called this out as the failure
   mode of the previous attempt. Under B1's
   idempotent-self-rejoin: phase 2's Bob rejoins; daemon_b's
   in-memory mirror still has Bob's authenticated id as a
   member from phase 1; the second join returns `Ok` with
   the existing head; the test continues. **Verify by
   running this test under B1 alone** (before B2/B3) — the
   test as currently written constructs the old
   `Request::JoinSession { peer, ... }` shape so it won't
   compile, but a one-line edit (rename `peer:` to
   `display_name:`) is enough to confirm the semantic. If
   semantic still fails, the brainstorm's
   `Workspace::shutdown emits LeaveSession` option needs a
   second look — but the brainstorm's analysis said
   idempotent self-rejoin is sufficient.

3. **Iroh-feature-off path.** Under
   `cfg(not(feature = "iroh"))` there's no bridge; the
   dispatch path uses `SYNTHETIC_LOCAL_PEER_ID` per A2.
   This means a no-iroh daemon stamps `[0u8; 32]` on every
   `JoinSession` / `HostSession`. That's documented as
   non-routable; the only consumer of an iroh-feature-off
   daemon is in-process unit tests. No new risk over A2.

4. **`AlreadyJoined` removal blast radius.** The error
   variant flows through `ProtocolError`,
   `SessionError`, slug strings, proptest arms, and three
   conversion sites. Mechanical; clippy will catch any
   missed arm. Trip wire: `rg "AlreadyJoined"` should
   return zero matches after B1.

5. **`Workspace::host` signature change is a semver-major
   for downstream.** The `artel-fs` crate is pre-1.0 and
   currently has only one downstream that we know about
   (`emdash`); per memory the codebase is alpha and
   breaking changes are explicit policy. No compat shim.

### Documentation hooks

A separate B4 documentation pass mirroring A3 is **not**
necessary here — this fix is the IPC-side complement of
the auth-L1 slice that already documented its work. A
single-paragraph addendum in
`docs/adr/001-collab-substrate-platform.md` § "Updates"
suffices, in B1's commit:

> 2026-06-01: L1 IPC-side closure (PROTOCOL_VERSION 5).
> `Request::HostSession` and `Request::JoinSession` no
> longer carry a `peer.id`; the daemon stamps its own
> authenticated id internally. `Registry::join` is
> idempotent on self-rejoin. See
> `docs/brainstorms/2026-06-01-auth-l1-fix3-shape.md` and
> `docs/plans/2026-06-01-auth-l1-fix3-plan.md`.

The roadmap doesn't need updating — auth-L1 is already
struck through; this fix tightens the same line.

---

## Critical files for implementation

- `crates/artel-protocol/src/rpc.rs` (B1 — `Request` shape)
- `crates/artel-protocol/src/version.rs` (B1 — bump)
- `crates/artel-protocol/src/error.rs` (B1 — drop
  `AlreadyJoined`)
- `crates/artel-daemon/src/server.rs` (B1 — dispatch)
- `crates/artel-daemon/src/session.rs` (B1 — `Registry::join`
  idempotent + drop `SessionError::AlreadyJoined`)
- `crates/artel-daemon/src/gossip_bridge.rs` (B1 — drop
  `AlreadyJoined` arm)
- `crates/artel-fs/src/workspace.rs` (B2 — narrow
  `host`/`host_with`)
- `crates/artel-daemon/tests/sessions.rs` (B3 — rewrite
  `two_clients_chat_end_to_end` + add
  `repeated_join_against_same_daemon_is_idempotent`)
- `crates/artel-client/tests/client.rs` (B3 — rewrite
  `events_stream_delivers_message_events`)
- `crates/artel-fs/tests/crash_recovery.rs` (B3 — verify
  failure mode closes; mechanical rename only)
- `crates/artel-daemon/tests/auth_l1_spoofing.rs` (B3 — extend)
- All other test files listed under B3.b (mechanical renames)
- `docs/adr/001-collab-substrate-platform.md` (B1 — Updates
  trailer addendum)
- `examples/chat-harness/src/main.rs` (B2 — local-only,
  uncommitted)
