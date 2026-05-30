# Handoff: code-review fixes (post-`bb8892f`)

Written 2026-05-28 after a high-effort code review of the diff
between `3d9b118` and `bb8892f` (substrate-instrumentation +
Drop-bomb + MemoryLookup→DnsPkarrServer migration). The review
ran 7 finder angles in parallel, deduped, then 7 verifiers, then
a Phase-3 gap-sweep and 3 more verifiers. Initial list: 15
ranked findings; this doc carries the fix plan for them.

Findings discovered *while fixing* the originals get appended
under "Findings discovered AFTER the original review" at the
bottom, with a date and a back-reference to the diagnostic log
that surfaced them.

The next agent should read this doc top-to-bottom before
starting any work — the findings are interleaved (some share a
file, some share a commit-shape, some are inverses of each
other).

**Delete this doc once every finding (originals AND
appended) has landed.** If the appended list grows faster than
the originals are landing, that's a signal to stop appending
here and start a fresh handoff doc — this one is supposed to
shrink toward zero, not grow indefinitely.

---

## Status as of 2026-05-29

Read this section FIRST. The detailed finding write-ups below
are still ground truth for the *open* items, but several have
already landed — don't re-attempt them.

### ALWAYS redirect long test output to a file and grep it

Tail-truncated terminal output silently hides failures buried
above the cutoff. The 2026-05-29 session lost meaningful tokens
re-running tests because a single-`cargo test` failure in
`workspace_shutdown_contract` was hidden in the middle of the
captured stream and only the trailing successes showed in the
tail. Two rules:

1. **Always redirect.** `cargo test ... > /tmp/X.log 2>&1`,
   then `grep -E "FAILED|^test result" /tmp/X.log` to summarise.
   Never tail-eyeball.
2. **Never claim a clean run from a truncated tail.** If the
   command's exit code is `0` and `grep -c FAILED /tmp/X.log` is
   `0`, only then is it green.

Same rule for clippy (`grep -E "^(error|warning:)"`) and any
build that emits more than a few hundred lines.

### Tests must NOT depend on inter-test ordering

Any process-wide static (atomic flag, env var, lock file) shared
between `#[tokio::test]` fns in the same integration binary is a
ticking time bomb: cargo runs tests on a thread pool by default,
so two tests can interleave and trip each other. If you need
fault injection, wire it per-instance (Arc<AtomicBool> on the
struct, exposed via a `test_*` method on the public type) — not
via a `static` in a `test_hooks` module. The 2026-05-29 session
fixed the `force_shutdown_failure` static-atomic bug exactly
this way; if you find another, fix it the same way.

### Required reading before touching tests

**`docs/diagnosing-flaky-tests.md`** — the diagnostic recipe
this repo uses for any cross-peer / real-n0 test that fails
intermittently. Per-phase `tokio::time::timeout` markers,
`tracing-subscriber` with wide RUST_LOG defaults, run-until-fail
to capture a full failing log. **"Flaky" is never an acceptable
label without forensic diagnosis** — it just means "real bug we
haven't found yet." This rule has been violated twice in the
sessions feeding this handoff (most recently 2026-05-29 on
`host_restart_live_writes_n0`); apply the recipe BEFORE
labelling anything an n0 infra flake.

This doc is referenced again from:
- The `feedback_no_handwaving_flaky_tests.md` memory entry
  (loads automatically — see `MEMORY.md`).
- `docs/plans/2026-05-29-faster-cargo-test-plan.md` (test-infra
  landed at commit `6d22e61` — cargo-nextest + by-subsystem
  consolidation + tiered pyramid). The diagnostic-legibility
  framing for serialising real-n0 tests is now wired in via the
  `n0` profile in `.config/nextest.toml`, not in the older "n0
  rate-limit flakiness" attribution that
  `docs/diagnosing-flaky-tests.md` § "What NOT to do" explicitly
  debunks.
- The "Conventions a fresh agent should keep" subsection at
  the bottom of this status block.

If you find yourself about to `#[ignore]` a test or write off a
failure as "flake," stop and read `docs/diagnosing-flaky-tests.md`
first.

### TDD-first is mandatory for every finding here

See the "Methodology: TDD-first, fix-after" section further
down this doc (right after this status block). The workflow for
EVERY finding is:

1. **Write a failing test first.** State the property in code.
2. **Run the test, confirm it fails.** Load-bearing step.
3. **Write the fix.** Smallest change that turns the test green.
4. **Run the test, confirm it passes.**
5. **Run the entire affected crate's test suite.**

The guardrail: **if the test passes before the fix, slow down.**
That's a signal of one of three things — test doesn't exercise
the bug / bug is misdiagnosed / bug was already fixed in a
parallel commit. The methodology section explains how to
distinguish them. Don't paper over it by tightening assertions
until red.

Reinforced by [[feedback-extensive-unit-tests]] in memory:
every artel crate change must ship with tests; treat
"no tests" as "not done." This is hard-coded into the
finding-by-finding workflow below.

### Landed (skip these)

| Finding | Commit | Notes |
|---|---|---|
| #1 — tombstone bypasses filter | `372d136` | filter check moved above tombstone branch in `applier.rs` + `workspace.rs::bulk_export`; new tests in `tombstone_filter_check.rs` |
| #5 + #16 — `Registry::join` addr-hint + `SessionError::InvalidAddr` reachability | `bac631f` | daemon installs `MemoryLookup` in iroh address-lookup chain; bridge seeds it from wire `host_addr` before subscribing; real-n0 test in `iroh_join_addr_hint_n0.rs` (kept `#[ignore]`d for CI, run manually 20× before changes touching session-join paths) |
| #12 — `iroh_docs_smoke` doc-comment | `cad4de7` | comment now matches actual sleep-and-poll loop (no fake "calls start_sync again" claim) |
| #13 — `on_removed` event-stream asymmetry | `5fe0812` | both modify and remove paths emit `WorkspaceEvent::Error` via shared `path_to_key_or_emit` helper; unit tests pin the property |
| Bug A from `host_restart_live_writes_n0` (NOT a numbered original — surfaced 2026-05-29) | `64aeeb1` | `Doc::share` was called with `AddrInfoOptions::default()` (= `Id`-only) despite a comment claiming "full addressing info." Switched to `AddrInfoOptions::RelayAndAddresses`. Pinned by per-phase tracing in the test. Failure rate 1/6 → 1/9 — bug B (#5c below) is the remaining failure |
| Roadmap drift fix — `2acaf9f` | `2acaf9f` | removed "occasionally flaky; the price of testing" handwaving from `docs/roadmap.md` and linked to `docs/diagnosing-flaky-tests.md` as the required diagnostic recipe |
| #2 + #3 + #4 — Workspace shutdown contract trio | `d9d9e0e` | `Workspace::shutdown` now returns `Result<(), WorkspaceError>`; lock held across the inner `node.shutdown().await` so concurrent callers serialise; sentinel only armed on Ok; rollback site logs the inner error. `WorkspaceNode::shutdown` returns `Result` and gains a `test-utils` fault-injection knob so tests can coerce the otherwise best-effort router failure path. New integration suite at `tests/workspace_shutdown_contract.rs` pins all three properties on the localhost `DnsPkarrServer` fixture. Public-API change: every shutdown call site (28 tests + chat-harness + drop_bomb_child) updated to `.expect("shutdown")`. `host_restart_live_writes_n0` flipped to `#[ignore]` with a `5c` reference — known-broken until that finding lands, kept on the codebase as a regression trap. **TDD methodology lapse, declared:** the API change (Result return) was made before the test was written; retrospective verification by stashing the production diffs and observing that `tests/workspace_shutdown_contract.rs` doesn't compile against the pre-fix `()` shape — the API change *is* the contract. A behavioural-only pre-fix test would have required real-n0 (relay-rejection race after concurrent shutdown), which is finding-#5c-territory. Doing it test-first in the strict sense was not feasible here; the lapse is logged so future agents don't repeat the shortcut without a real reason. **Follow-on (uncommitted, see #6 + #7 row below):** the original fault-injection knob was a process-wide `static AtomicBool` in `node::test_hooks::force_shutdown_failure`; that proved order-dependent under parallel test execution and was replaced with a per-`Workspace` `test_arm_shutdown_failure(&self)` method (per-instance `Arc<AtomicBool>` on the `WorkspaceNode`). The `test_hooks` re-export at `artel_fs::test_hooks::*` is gone; tests call `ws.test_arm_shutdown_failure().await` instead. |
| #6 + #7 — daemon `endpoint.online()` asymmetry + timeout | (uncommitted in working tree as of this write) | `tokio::time::timeout(30s, endpoint.online())` at both call sites (`crates/artel-fs/src/node.rs:~125` and `crates/artel-daemon/src/server.rs::resolve_iroh_runtime`); typed `WorkspaceError::RelayUnreachable(Duration)` and `StartError::RelayUnreachable(Duration)` surfaced on timeout. Daemon's `EndpointSetup` gains `awaits_relay()` mirroring `artel-fs`. Test scaffolding: new `EndpointSetup::TestingUnreachableRelay` variant (test-utils only, in BOTH crates) using RFC 5737 TEST-NET-1 (`192.0.2.1`) so the relay handshake provably never completes — no external network access required. New integration tests at `crates/artel-fs/tests/relay_unreachable.rs` and `crates/artel-daemon/tests/relay_unreachable.rs`. **Bonus fix surfaced during this work:** the static-atomic fault-injection knob from the trio commit (#2/#3/#4) was order-dependent under parallel test execution; replaced with a per-instance `Workspace::test_arm_shutdown_failure(&self)` method as documented in the trio's row above. **TDD followed properly:** premise probe (direct iroh `online()` against TEST-NET-1) confirmed the bug shape; scaffolding + E2E tests written before the fix; pre-fix the workspace test hung past the harness budget (40s) and the daemon test returned `Ok` (because daemon never awaited `online()`); both green post-fix. |

### Open (work from here)

In suggested order, with the recommendation at the top.

**Start here:** the next finding is **#5c** (host-restart loses
addr info for known sync peers). It NEEDS A BRAINSTORM BEFORE
WRITING CODE — three fix options are written up in the "Findings
discovered AFTER the original review" section at the bottom of
this doc (workspace-side persistence / daemon→workspace
addr-cache / wait for upstream). Don't pick blindly. The
layer-boundary call (option b) requires checking
[[feedback-no-speculative-abstractions]] rule 2.

**Before you start:** check `git log --oneline` to see whether
the #6 + #7 work landed in this session has been committed. If
it shows up modified-but-uncommitted in `git status`
(`crates/artel-{fs,daemon}/{src,tests}/...` plus the doc
changes), it's the bundled commit grouping #4 from below — read
its row in the Landed table above for the full surface, commit
it, then move on to #5c. Don't be confused by the
`relay_unreachable.rs` test files that look "untracked" — they
landed alongside the fix.

**Tier 3 — production correctness**
- **#5c — host-restart loses addr info for known sync peers.**
  See the "Start here" note above.

**Tier 4 — test-flake hardening**
- **#8 — `Workspace::endpoint_id` accessor + cross-peer test gate.**
  Adds a public method on `Workspace`; updates `spawn_pair` /
  `wait_for_workspace` to gate cross-peer tests on workspace
  endpoints' pkarr publish. Hardens `live_edit`,
  `delete_propagates`, `round_trip`, the read_only_*,
  `host_restart_live_writes`, etc. Test-infra (cargo-nextest +
  by-subsystem consolidation) landed at commit `6d22e61` per
  `docs/plans/2026-05-29-faster-cargo-test-plan.md`, so the
  `tests/common/mod.rs` shape is now stable; this finding's
  cross-peer-gate rewrite lands on top.
- **#9 — `drop_bomb_child` Testing-fixture.** Adds
  `EndpointSetup::TestingExternal { nameserver, pkarr_url }`
  variant + child-process env plumbing.

**Tier 5 — diagnostic / cleanup**
- **#10 + #17 — Drop-bomb diagnostic hardening.** Test-mode
  panic-on-drop + TUI-aware eprintln.
- **#11 + #15 — `EndpointSetup` deduplication.** 3 copies today
  across `artel-fs`, `artel-daemon`, and the
  `iroh_docs_smoke_pkarr.rs::Node` chain. Pairs with #15's
  unconditional `dns_resolver` override.
- **#14 — tracing logs sit before the awaited operation;
  `let _ = ...` swallows failures.** Move logs after await,
  match the Result, log Err separately.

### Conventions a fresh agent should keep

- **Don't re-attempt the "Landed" rows above** — verify with
  BOTH `git log --oneline` AND `git status` before writing
  tests. Some "Landed" rows are committed (commit hash in the
  Commit column); others may be marked "uncommitted in working
  tree" — those changes still exist on disk, just not in git
  history yet. If the test you were about to write already
  exists (committed or in `git status`), the finding is done.
- **Real-n0 tests stay `#[ignore]`d for CI**, but run them
  manually 20× per the recipe in `docs/diagnosing-flaky-tests.md`
  before claiming a fix is "real-n0 verified." `cargo test
  --test foo -- --ignored --nocapture --test-threads=1` is the
  command shape.
- **Append new findings only if they're orthogonal to the
  open list above.** If the next investigation surfaces a
  duplicate or sharper restatement of an existing finding,
  edit that finding in place rather than appending a new one.

---

## Methodology: TDD-first, fix-after

For every finding below, the workflow is:

1. **Write a failing test first.** State the property in code,
   commit the test alone (or stage it locally before the fix).
2. **Run the test, confirm it fails.** This is the load-bearing
   step — see the guardrail below.
3. **Write the fix.** Smallest change that turns the test green.
4. **Run the test, confirm it passes.** Plus the rest of the
   relevant suite — no regressions in adjacent properties.
5. **Run the entire affected crate's test suite.** Catch
   accidental cross-cutting breakage.

### Guardrail: if the test passes before the fix, slow down

A test that passes pre-fix means one of:
- **The test doesn't actually exercise the bug.** Re-read the
  failure scenario in this doc; trace the inputs and state the
  test sets up; confirm the trigger condition the verifier
  identified actually fires. Often the issue is a missing
  precondition (e.g. the bug needs a *peer-published* tombstone,
  not a local one; or it needs *concurrent* shutdowns, not
  sequential).
- **The bug is misdiagnosed.** Re-read the verifier's evidence
  quoted in this doc. If the failure scenario doesn't match the
  code shape, the finding is wrong — drop it from the plan
  rather than papering over.
- **The bug was already fixed in a parallel commit.** Less
  likely, but check git log for the file in case.

Don't paper over a failing-test-that-isn't-failing by tightening
assertions until red. That's how false-positive findings get
"fixed" with code changes that lock in the wrong behaviour.

---

## Verified findings: severity-ordered

Two findings from the review were REFUTED and are NOT on this
list (so the next agent doesn't waste time on them):

- **`presets::Empty` crypto-provider regression**: REFUTED.
  `presets::N0::apply` chains `Minimal.apply(builder)` at
  `iroh-0.98.2/src/endpoint/presets.rs:119`, so
  `Endpoint::builder(Empty) + N0.apply(builder)` is identical to
  `Endpoint::builder(N0)`.
- **`smoke_pkarr.rs` `.preset()` vs substrate `Preset::apply`
  divergence**: REFUTED. `Builder::preset` at
  `iroh-0.98.2/src/endpoint.rs:166-169` is exactly
  `preset.apply(self)`.

### Tier 1: silent correctness / data-loss bugs

These can corrupt user state in production. Fix first.

#### 1. Tombstone bypasses filter (BOTH applier and bulk_export)

- **Files**: `crates/artel-fs/src/applier.rs:~159-167` and
  `crates/artel-fs/src/workspace.rs::bulk_export ~1422-1440`.
- **Property violated**: a peer's tombstone for a path the
  *local* filter would skip should NOT call `tokio::fs::remove_file`.
- **Bug shape**: the in-applier `handle_entry` and the joiner-side
  `bulk_export` both check rules in this order: ReadOnly → tombstone
  short-circuit (`content_len == 0`) → filter. The tombstone branch
  short-circuits before the filter check. So an incoming tombstone
  for any path passes the filter check.
- **Failure scenario**: peer publishes a tombstone whose key
  resolves to a path the local filter rejects (e.g. asymmetric
  ignore globs across peers, version drift in filter rules, or
  an attacker-crafted key targeting a hardcoded-skip path like
  `.git/HEAD`). Local file is deleted regardless.
- **Test (write first)**:
  - Set up two peers via `spawn_pair` with one peer's filter
    excluding `excluded.txt` (or use a hardcoded skip path).
    Inject a tombstone via `doc.del(...)` directly (bypass the
    watcher) for the excluded key. Assert the local file
    survives.
  - Mirror in `bulk_export`: stand up a returning joiner whose
    state had `excluded.txt`, host doc has the same key as a
    tombstone; `Workspace::join_with` runs `bulk_export`; assert
    the file survives.
- **Fix**: move the `filter.check(&path)` block ABOVE the
  `if entry.content_len() == 0` branch in both files. (`ReadOnly`
  rules already sit at the top — keep that ordering.)
- **Care**: `bulk_export` and `applier::handle_entry` have
  identical ordering today; fix both in the same commit so they
  don't drift. The intent comment at workspace.rs:1406-1410 says
  it mirrors the applier; the new comment should say "filter +
  rules sit ABOVE the tombstone branch on both sides."

### Tier 2: silent contract failures (Drop bomb defeated)

These are correctness bugs at the substrate-contract level —
the Drop bomb fails to fire when it should. Fix together; the
three issues all touch `Workspace::shutdown`.

#### 2. `did_shutdown` armed when no node was taken

- **File**: `crates/artel-fs/src/workspace.rs:967-979`.
- **Bug shape**: the `let taken = { slot.take() }` block
  releases the lock; the `if let Some(node) = taken` arm awaits
  `node.shutdown()`; the `else { debug!(...) }` arm runs on the
  empty-slot path. Then unconditionally `did_shutdown.store(true,
  Release)` runs OUTSIDE the if/else.
- **Failure scenario**: a second concurrent `shutdown()` call,
  or a partially-initialised Workspace whose constructor's
  rollback left no node, takes `None`, skips the await, and
  flips the sentinel anyway. The bomb sees `did_shutdown=true`
  and stays silent on Drop.
- **Test (write first)**: build a Workspace, call `shutdown`
  once successfully, then call `shutdown` again. The second call
  observes empty slot. Drop the workspace. Assert the bomb did
  NOT fire (it shouldn't on a graceful path) — currently passes.
  Now write the failing test: deliberately race two concurrent
  `shutdown` futures via `tokio::join!`; the second arm doesn't
  await teardown; capture the first `did_shutdown=true` store;
  ensure that even before the FIRST caller's `node.shutdown()`
  resolves, the second caller has already set the flag. Assert
  this ordering doesn't happen (will fail under current code).
- **Fix**: move `self.did_shutdown.store(true, Ordering::Release);`
  INSIDE the `if let Some(node) = taken { ... }` arm so only
  the caller that actually consumed and tore down the node arms
  the OK signal.

#### 3. `did_shutdown` armed even when teardown failed

- **File**: `crates/artel-fs/src/workspace.rs:979`.
- **Bug shape**: `WorkspaceNode::shutdown` (`crates/artel-fs/src/node.rs:135-140`)
  calls `router.shutdown().await` and swallows the `Err` with
  `tracing::warn!`. `Workspace::shutdown` then unconditionally
  arms `did_shutdown=true` even though router teardown
  failed. The bomb is silent on Drop, but the relay session
  didn't actually close cleanly — exactly the case the bomb
  documents.
- **Test (write first)**: harder than #2 because we need to
  coerce `router.shutdown()` to fail. Options:
  - Inject a fault: drop the underlying iroh-runtime task before
    calling `shutdown`, so `Router::shutdown` returns Err.
  - Mock `WorkspaceNode::shutdown` via a test-only feature gate.
  - Don't write a runtime test for it — pin the property as a
    code-shape test using `// invariant:` comments and review.
  Pragmatic choice: add a `#[cfg(feature = "test-utils")]`
  fault-injection knob on `WorkspaceNode` that forces
  `shutdown` to return `Err` (via a once_cell flag). Test
  asserts `did_shutdown` stays `false` (or that `shutdown`
  returns a typed Err the caller can react to) when the fault
  fires.
- **Fix**: change `WorkspaceNode::shutdown(self) -> ()` to
  return `Result<(), WorkspaceError>`; `Workspace::shutdown`
  returns the same Result; only arm `did_shutdown` on Ok.
  Callers in chat-harness and tests need updating to handle
  the Result (most can `.expect("shutdown")` since they're
  exiting anyway).
- **Care**: this is a public API change. Coordinate with
  finding #2 — both touch the same `store(true, Release)` line.
  Land them in one commit.

#### 4. Concurrent `shutdown` doesn't synchronise teardown

- **File**: `crates/artel-fs/src/workspace.rs:967-980`.
- **Bug shape**: `let taken = { slot.take() };` only holds the
  lock for the take, NOT for the await. Caller A awaits
  `node.shutdown()` (slow); caller B takes the now-empty slot
  and returns immediately, claiming completion.
- **Failure scenario**: B's caller spawns a new Workspace at
  the same state dir before A's router is done — same
  `EndpointId` collision the bomb claims to prevent.
- **Test (write first)**: kick off two concurrent
  `workspace.shutdown()` calls via `tokio::join!`; have the
  second caller proceed to call `Workspace::host_with(...)` at
  the same state dir as soon as `shutdown` returns; assert that
  the new Workspace's `Endpoint::online` doesn't time out (i.e.
  the first caller's teardown actually completed before the
  second `shutdown` returned). This will fail under current
  code with a timeout.
- **Fix options** (pick one):
  - **(a) Hold the lock across the await**: acquire the Mutex,
    take the node, await `node.shutdown()`, drop the guard.
    Simple but blocks all callers (fine — they're all waiting
    for the same teardown).
  - **(b) `OnceCell<JoinHandle<()>>`-style shared future**:
    first caller spawns the teardown; subsequent callers await
    the same handle. More code, more correct semantics.
  - Recommend (a) for simplicity; the contention window is
    bounded by iroh's router shutdown.
- **Care**: this fix interacts with #2 and #3 — the post-fix
  `if let Some(node) = taken { node.shutdown().await?; ... }`
  block needs to live INSIDE the lock guard.

### Tier 3: production correctness gaps

These are real bugs that production users would hit. Fix after
the contract-failure trio above.

#### 5. `Registry::join` discards ticket `host_addr`

- **File**: `crates/artel-daemon/src/session.rs:574-585`.
- **Bug shape**: `let _ = host_addr;` discards the wire-form
  relay_url + direct_addrs the ticket carried. The new code
  relies entirely on iroh's pkarr/DNS to resolve the host's
  EndpointId — a ~500ms propagation race in production.
- **Failure scenario**: alice publishes ticket → bob joins
  immediately → bob's daemon dials by EndpointId → DNS empty →
  iroh-gossip times out at `JOIN_READY_TIMEOUT` (15s). Pre-
  migration the seed had been synchronous via `MemoryLookup`.
- **Test (write first)**: this is a real-n0 test by necessity
  (the bug is a propagation race in production). Add an
  `#[ignore]`d real-n0 test that:
  - Stands up two daemons via real n0 with a deterministic
    short delay between alice's publish and bob's dial.
  - Asserts bob's `JoinSession` succeeds within a budget that's
    tighter than `JOIN_READY_TIMEOUT` but covers normal
    discovery time.
  Run the test 20+ times un-ignored to confirm it consistently
  fails before the fix; un-`#[ignore]` once the fix lands.
- **Fix**: restore the synchronous addr seed. Two options:
  - **(a) Reintroduce `wire_addr_to_iroh`** and call
    `endpoint.add_node_addr(...)` (or the equivalent in iroh
    0.98) before subscribing to the gossip topic.
  - **(b) Pass the wire-form addr through to `bridge.join_session`**
    and have the bridge call iroh's add-known-addr API. The
    pre-migration path used `MemoryLookup::add_endpoint_info`;
    iroh's address-lookup chain has a different add-direct-addr
    API that the bridge can call.
  Recommend (b) — keeps the addr-seeding logic on one side.
- **Care**: `SessionError::InvalidAddr` (currently dead — see
  finding #14) becomes reachable again. Re-validate the wire
  format before passing to iroh. Update the comment at
  session.rs:574-580 to say "wire `host_addr` is used as a
  synchronous addr hint to sidestep pkarr propagation."

#### 6. Daemon-side `endpoint.online()` asymmetry

> **DONE (uncommitted in working tree as of this write).** See
> the Landed table at the top of this doc for the full surface.
> Write-up retained for archaeology only — do not re-attempt.

- **File**: `crates/artel-daemon/src/server.rs::resolve_iroh_runtime`
  (~654-690) and `crates/artel-daemon/src/endpoint_setup.rs`.
- **Bug shape**: `WorkspaceNode::spawn` gates `endpoint.online()`
  on `setup.awaits_relay()` (Production only). The daemon's
  `EndpointSetup` defines no `awaits_relay()` and
  `resolve_iroh_runtime` never calls `online()` even in
  Production. Pre-existing pre-migration; the new abstraction
  pins the asymmetry structurally.
- **Failure scenario**: a fresh daemon accepts IPC and broadcasts
  on gossip before the home-relay handshake completes; the first
  cross-daemon dial races the publish.
- **Test (write first)**: a real-n0 test that stands up alice's
  daemon and IMMEDIATELY (no settling delay) issues a
  `HostSession` + ticket-publish; bob joins; assert bob receives
  alice's first message within a tight budget. Run before fix to
  confirm the race; if it doesn't reproduce in 50 iterations,
  the bug is plausible-but-rare and probably doesn't warrant a
  fix on its own.
- **Fix**: add `awaits_relay()` to the daemon's `EndpointSetup`
  (mirror the workspace shape); call `endpoint.online().await`
  in `resolve_iroh_runtime` when `setup.awaits_relay()`.
- **Care**: this is the right time to address the
  duplicated-enum smell (finding #11) — if you move
  `EndpointSetup` to a shared location, fix both `awaits_relay`
  asymmetry and the duplication in one commit.

#### 7. `endpoint.online().await` has no timeout

> **DONE (uncommitted in working tree as of this write).** See
> the Landed table at the top of this doc for the full surface.
> Write-up retained for archaeology only — do not re-attempt.

- **File**: `crates/artel-fs/src/node.rs:142`.
- **Bug shape**: `if setup.awaits_relay() { endpoint.online().await; }`
  has no timeout. If n0's relay is unreachable, `online()` never
  resolves; `Workspace::host_with`/`join_with` hangs forever.
- **Failure scenario**: user offline (flight, captive portal) or
  during an n0 outage — `Workspace::host_with` hangs with no
  signal.
- **Test (write first)**: simulate unreachable relay. Iroh's
  test_utils may expose a "fake unreachable relay" config; if
  not, point a daemon at a deliberately-bad relay URL via a
  custom `EndpointSetup` variant. Assert the call returns Err
  within a budget rather than hanging.
- **Fix**: wrap in `tokio::time::timeout(BUDGET,
  endpoint.online())` and surface a typed
  `WorkspaceError::RelayUnreachable`. Pick a budget that's
  tight enough to fail fast but loose enough to cover normal
  startup (start with 30s).
- **Care**: same fix shape applies to finding #6's daemon-side
  `endpoint.online()` once that's added. Land #6 and #7
  together so the timeout is consistent across both call sites.

### Tier 4: test-flake hazards

These don't break production but make CI unreliable. Fix after
the production bugs.

#### 8. Workspace endpoints not gated by `wait_for_endpoint`

- **File**: `crates/artel-fs/tests/common/mod.rs:300` (the
  fixture's loop only gates daemon endpoints) +
  `crates/artel-fs/src/workspace.rs` (no public `EndpointId`
  accessor).
- **Bug shape**: `spawn_pair` waits for daemon endpoints'
  pkarr publish, but workspace endpoints (built later in tests
  via `Workspace::host_with` etc.) are never gated. The
  `wait_for_endpoint` helper's docstring documents this should
  happen but the helper is unusable without an accessor.
- **Failure scenario**: slow CI host → workspace publish lags
  → joiner's iroh-docs `import` dials by EndpointId before the
  host's pkarr record is queryable → 'Failed to establish
  connection' → test times out.
- **Test (write first)**: harder than usual because the bug is
  a race that current CI doesn't trip. Possible approaches:
  - Add deterministic delay to the workspace endpoint's
    publisher (test-only knob) and confirm the test fails.
  - Skip the test; instead, ASSERT the API gap directly:
    `Workspace::endpoint_id() -> Option<EndpointId>` exists and
    returns `Some` after `host_with`. Tests that rely on it
    pass once the accessor lands.
- **Fix**: add `pub fn endpoint_id(&self) -> EndpointId` (or
  `Option<EndpointId>` if pre-shutdown the slot may be empty)
  on `Workspace`. Update `spawn_pair` (or add a helper
  `wait_for_workspace`) so cross-peer tests gate on workspace
  endpoints too. Apply to the cross-peer suite.
- **Care**: this is the place to harden every cross-peer test
  in one go — `live_edit`, `delete_propagates`, `round_trip`,
  the read_only_*, host_restart_live_writes, etc. all benefit.

#### 9. `drop_bomb_child` uses real n0

- **File**: `crates/artel-fs/tests/bin/drop_bomb_child.rs:~98`.
- **Bug shape**: child binary uses `WorkspaceConfig::default()`
  → `EndpointSetup::Production` → real n0. The test has nothing
  to do with relay readiness.
- **Failure scenario**: n0 outage or rate limit causes the
  child to hang in `endpoint.online()`; parent test fails on
  daemon stop timeout; failure looks like a Drop-bomb regression
  but is actually n0 reachability.
- **Test (write first)**: this finding IS a test-quality issue,
  not a runtime bug. The TDD shape is: write a regression test
  that fails when the child uses real n0 and passes when it
  uses Testing. (Or skip the test and just verify by
  inspection.)
- **Fix**: parent passes `dns_pkarr.nameserver` and
  `dns_pkarr.pkarr_url` to the child via env vars; child
  reconstructs `EndpointSetup::Testing` pointing at those
  localhost servers. (Same approach applicable to `crash_recovery`
  — see roadmap "Future" section.)
- **Care**: requires adding a way to construct
  `EndpointSetup::Testing` from URL + socket-addr (currently it
  takes `Arc<DnsPkarrServer>`). Either add a new variant
  `TestingExternal { nameserver, pkarr_url }` or add a builder
  function on `DnsPkarrServer` that returns a borrow-compatible
  handle. Recommend the new variant — it's explicit.

### Tier 5: diagnostic / cleanup

Real but low-impact. Fix after the bug-fixes are in.

#### 10. Drop bomb captured in test stderr

- **File**: `crates/artel-fs/src/workspace.rs:1003`.
- **Bug shape**: cargo test captures stderr; passing in-process
  tests that violate the contract leave no visible signal.
- **Test (write first)**: an in-process test that drops a
  Workspace without `shutdown()`; assert the test SUITE fails
  even though that individual test passed. (Hard to write —
  may not be testable from inside cargo test.)
- **Fix options**:
  - **(a)** A process-wide atomic that increments on each bomb
    fire; a `#[dtor]` or test-suite-final-stage hook prints
    "WARNING: N Workspace drops without shutdown observed" so
    investigators see it post-suite.
  - **(b)** A panic on Drop in `cfg(test)` builds — converts
    silent contract violations into hard test failures.
    Aggressive but unambiguous.
  - **(c)** Document the limitation; rely on `drop_bomb.rs`'s
    child-process pattern. Lowest cost.
  Recommend (b) for `#[cfg(test)]` and keep the
  log-and-eprintln for production.
- **Care**: a panic-in-Drop for `#[cfg(test)]` only fires
  through the test harness, never in production binaries. The
  existing `drop_bomb.rs` tests would need a feature gate so
  the existing assertions still work.

#### 11. `EndpointSetup` duplicated across two crates

- **Files**: `crates/artel-fs/src/endpoint_setup.rs` +
  `crates/artel-daemon/src/endpoint_setup.rs` + the chain in
  `crates/artel-fs/tests/iroh_docs_smoke_pkarr.rs::Node::spawn`.
- **Bug shape**: three copies of the same enum / preset chain.
  The `awaits_relay()` asymmetry (finding #6) is one
  consequence.
- **Failure scenario**: maintenance cost — when iroh changes
  the preset chain or the AddrFilter API, three files need
  parallel updates. Already drifted (daemon doesn't have
  `awaits_relay`).
- **Test (write first)**: doesn't apply — this is a structural
  smell, not a behavioural bug. Skip the TDD step.
- **Fix**: extract `EndpointSetup` to a shared crate. Options:
  - **(a)** Add to `artel-protocol` (already a peer dep of both
    crates).
  - **(b)** Create a new `artel-iroh-runtime` crate.
  - **(c)** Have `artel-fs` depend on `artel-daemon` (or vice
    versa) and import. Cyclic-dep risk; probably not.
  Recommend (a) if the enum has no protocol-versioning
  implications, otherwise (b).
- **Care**: combine with finding #6 (daemon-side `awaits_relay`)
  — if you're moving the enum, make the workspace and daemon
  copies converge in the same commit.

#### 12. `iroh_docs_smoke.rs` doc-comment lies about `start_sync` retry

- **File**: `crates/artel-fs/tests/iroh_docs_smoke.rs:33`.
- **Bug shape**: the module doc-comment claims the test calls
  `start_sync` again on dial failure; the body has no such
  retry, just a 50ms sleep loop.
- **Fix**: either implement the retry the comment describes
  (`Doc::start_sync(...)` from the imported addrs whenever the
  poll loop detects no progress for N seconds), or rewrite the
  comment to match the actual sleep-and-poll loop.
- **Test**: doesn't apply; doc-comment fix.
- **Care**: if you implement the retry, the test becomes a
  valuable real-n0 canary for the iroh-docs no-internal-retry
  property. If you just rewrite the comment, the test still
  flakes on slow propagation but the comment is honest about
  it.

#### 13. `on_removed` event-stream asymmetry

- **File**: `crates/artel-fs/src/watcher.rs:~304` (and adjacent
  `on_modified`).
- **Bug shape**: `on_modified`'s errors emit `WorkspaceEvent::Error`;
  `on_removed`'s errors only `tracing::warn!`. Same observable
  consequence (peer never sees the change), divergent surfaces.
- **Test (write first)**: induce a `path_to_key` failure (e.g.
  invalid UTF-8 in a path) on both code paths; assert
  `WorkspaceEvent::Error` fires for both modify and remove.
- **Fix**: upgrade `on_removed` to also emit
  `WorkspaceEvent::Error`. (Don't downgrade `on_modified` to
  warn-only — observability is the right direction.)

#### 14. Tracing logs lie on failure paths

- **File**: `crates/artel-fs/src/applier.rs:88,159` and
  `crates/artel-fs/src/watcher.rs:~262`.
- **Bug shape**: `debug!` logs sit BEFORE the awaited operation;
  the operation's `Result` is then ignored with `let _ = ...`.
  On failure the trace asserts work that didn't happen.
- **Fix**: move logs AFTER the await, stamping the operation's
  outcome (or at least its Ok/Err). For `let _ = ...` paths,
  match the Result and log Err separately at warn level.
- **Test**: doesn't apply directly; spot-check the diff during
  review.

#### 15. `EndpointSetup::Testing` overrides `dns_resolver`
unconditionally

- **Files**: `crates/artel-fs/src/endpoint_setup.rs:93` and
  `crates/artel-daemon/src/endpoint_setup.rs:65` (duplicate).
- **Bug shape**: `.dns_resolver(dns_pkarr.dns_resolver())` runs
  AFTER `Minimal.apply(builder)`, unconditionally overriding any
  resolver Minimal might add in the future. Production keeps
  N0's resolver — silent test-vs-production divergence with no
  test coverage.
- **Test**: not directly testable today (Minimal doesn't set a
  resolver). Pin via a code-shape comment or move to a future
  iroh upgrade's checklist.
- **Fix**: leave as-is for now; add a code comment explaining
  the override is intentional and document what to check on
  iroh upgrades. (Or condition the override on whether
  `dns_pkarr.dns_resolver()` returns a different resolver than
  whatever's already on the builder — overengineering for
  today.)
- **Care**: if finding #11 lands first (single shared
  `EndpointSetup`), fix here is one site, not two.

#### 16. (out of order — this is just below #15 in severity)
`SessionError::InvalidAddr` is now unreachable

- **File**: `crates/artel-daemon/src/session.rs:64`.
- **Bug shape**: the variant remains but the only construction
  site (`wire_addr_to_iroh`) was deleted.
- **Fix**: either remove the variant (clean-up) OR — if finding
  #5 is fixed by reintroducing the addr parse — make the
  variant reachable again from the new validation site. Pair
  the decision with finding #5.

#### 17. Drop bomb's `eprintln!` corrupts TUI

- **File**: `crates/artel-fs/src/workspace.rs:1003-1005`.
- **Bug shape**: 5-line message writes raw to stderr regardless
  of TUI mode.
- **Fix**: gate `eprintln!` on `!std::io::stderr().is_terminal()`
  OR on `cfg!(debug_assertions)`. Keep `tracing::error!`
  unconditional. Matches the `headless-first-class` policy.
- **Test**: snapshot-style — capture stderr from a TUI-like
  test that drops Workspace; assert the captured bytes don't
  contain ANSI control corruption signatures. Probably not
  worth the test cost; spot-check by running chat-harness with
  the fix.

---

## Suggested fix-and-commit grouping

Each commit should be self-contained and pass the full test
suite. Suggested groups:

1. **Tombstone-bypasses-filter fix** (#1): single commit
   touching `applier.rs` + `workspace.rs::bulk_export`. New
   tests in a `tombstone_filter_check.rs` integration test.
2. **Workspace shutdown contract trio** (#2 + #3 + #4): single
   commit. Touches `workspace.rs::shutdown` and
   `node.rs::shutdown`. New tests for concurrent-shutdown,
   teardown-failure, and the unconditional-flag race. Public
   API change: `Workspace::shutdown` returns Result.
3. **`Registry::join` addr-hint restoration** (#5 + #16): the
   addr discard fix; revives `wire_addr_to_iroh` (or its
   replacement) and re-uses `SessionError::InvalidAddr`. New
   real-n0 test for the propagation-race fast-join.
4. **Daemon `endpoint.online` + timeout** (#6 + #7): DONE in
   working tree; see the Landed table at the top of this doc.
5. **EndpointSetup deduplication** (#11 + #15): move enum to a
   shared crate; consolidate the dns_resolver override.
6. **Workspace endpoint accessor + cross-peer gate** (#8): add
   `Workspace::endpoint_id`; harden `spawn_pair` callers.
7. **`drop_bomb_child` Testing-fixture** (#9): adds
   `EndpointSetup::TestingExternal` variant; child-process
   plumbing.
8. **Drop-bomb diagnostic hardening** (#10 + #17): test-mode
   panic-on-drop + TUI-aware eprintln.
9. **Doc + tracing cleanup** (#12 + #13 + #14): comment fixes,
   on_removed event symmetry, log-after-await reorderings.

Each group's tests should:
- Land FIRST as a failing test (TDD).
- Use the methodology in `docs/diagnosing-flaky-tests.md` —
  per-phase timeouts, tracing-subscriber, run-until-failure
  loop for any cross-peer test.
- Mirror the existing test-pyramid: every cross-peer property
  pinned by a `DnsPkarrServer` test (deterministic) PLUS a
  real-n0 sibling where the bug is production-discovery-
  specific (#5, #6 most clearly).

---

## Working tree state at handoff

Current branch `emdash/stable-id-jx4uy`. Last commit `bb8892f`
(the migration). Working tree should be clean except for
`docs/handoff-post-workspace-registry.md` which is unrelated and
was untracked from earlier sessions.

Untracked-but-load-bearing: `examples/chat-harness/` is gitignored
throwaway test infrastructure. The `Workspace::shutdown` fix in
its `main.rs` was made during the migration session; the harness
itself is local-only and shouldn't influence fix sequencing.

The roadmap (`docs/roadmap.md`) was updated 2026-05-28 with the
DnsPkarrServer state and the per-path-rules vs ticket-capability
distinction; no further roadmap changes required for these fixes.

---

## What "good" looks like for the next session

- Read this doc + `docs/diagnosing-flaky-tests.md` in full.
- Pick a tier; pick a finding within the tier; write the test
  first; confirm it fails; fix; confirm it passes; commit.
- Don't batch findings across tiers — each tier represents a
  different blast-radius story for the changelog.
- For findings #5 and #6, stand up a real-n0 test loop
  alongside the `DnsPkarrServer` deterministic test. Both
  passing is the "fixed" signal.
- Delete this doc once all findings have landed (or document
  any deferred findings here as their own fixed-or-deferred
  list at the end).

---

## Findings discovered AFTER the original review (added 2026-05-29)

### Tier 3: production correctness gaps

#### 5c. Host restart loses addr info for known sync peers

- **Files**: `crates/artel-fs/src/workspace.rs::host_with` /
  `host_with_inner` (the post-restart respawn path) — and
  upstream of that, iroh-docs's
  `engine::live::start_sync` →
  `LiveActor::get_sync_peers` (returns id-only `EndpointAddr`s
  from the persistent doc store). Test reproducer:
  `crates/artel-fs/tests/host_restart_live_writes_n0.rs::alice_post_restart_writes_reach_bob_real_n0`.
- **Bug shape**: when a host restarts and re-`open`s the doc,
  iroh-docs reads the stored "useful peers" list from its
  persistent doc store. The store keeps **peer-ids only** — no
  relay URL, no direct addrs. iroh-docs builds
  `EndpointAddr::new(public_key)` for each (live.rs:426),
  passes them to `join_peers`, which **skips** the `MemoryLookup`
  seeding at line 472 (`if !peer.is_empty()`) for id-only addrs.
  The first dial then races pkarr/DNS to find the peer; if DNS
  hasn't propagated the peer's latest publish, the dial fails.
  iroh-docs does not retry. Same race shape as findings #5/#16
  but on the host-restart path, not the joiner-ticket path.
- **Failure scenario**: alice hosts → bob joins → both exchange
  files → alice's daemon stops → alice's daemon respawns →
  alice tries to write a new file → applier publishes it to her
  doc → iroh-docs's live engine wakes up, calls `start_sync`
  with bob's id (from persistent store) → DNS lookup race
  fails → "Failed to establish connection" → bob never sees
  alice's post-restart writes. Reproduces ~1/9 with
  `--test-threads=1`; higher under suite contention.
- **Diagnosis evidence**: `iter_9.log` from the run-until-fail
  loop on 2026-05-29:
  ```
  >>> phase begin: wait for post_restart_bob.txt to reach alice
  ...DEBUG endpoint{id=4ad28af5fe (alice post-restart)}:
    RemoteStateActor{remote=5a65ba70f6 (bob)}:
    DnsAddressLookup{lookup_id=5a65ba70f6 origin_domain=dns.iroh.link.}:
    DNS lookup failed: no calls succeeded
  WARN gossip{me=4ad28af5fe}:
    dial failed: No addressing information available peer=5a65ba70f6
  ```
  Only `DnsAddressLookup` is consulted — no MemoryLookup hit, because
  `join_peers` saw `peer.is_empty() == true` and skipped seeding.
- **Fix shape (for next session — needs design discussion)**:
  - **Option (a) — workspace-side persistent peer-addrs**:
    track peer addrs we learn over the lifetime of a
    Workspace, persist to disk, re-seed iroh-docs's memory_lookup
    on restart. Same in-memory layer as iroh-docs's mechanism but
    survives process restart. Higher complexity; needs a
    storage format and pruning policy.
  - **Option (b) — daemon-side cross-pollination**: the
    daemon's gossip-bridge already learns each session-mate's
    iroh address-info on first contact (gossip neighbour
    metadata). Have the daemon push known-peer addrs to the
    workspace's MemoryLookup (would require adding one).
    Cleaner architecturally — daemon owns peer discovery, the
    workspace just consumes — but adds a new daemon→workspace
    plumbing channel.
  - **Option (c) — wait for upstream**: iroh-docs could
    persist peer addrs alongside ids in its own store. Worth
    raising upstream; not actionable in our window.
  Recommend a brainstorm before picking. Layer-boundary
  question: does (b) violate "daemon doesn't know about
  workspace internals" (memory rule
  [[feedback-no-speculative-abstractions]] rule 2)? If the
  daemon exposes a *generic* address-cache that any consumer
  can register a hint-sink with, no — that's clean layering.
- **Care**: this finding overlaps with Tier 5 #15 (Production
  daemon stress) — both are about long-running addr-book
  staleness. Land #5c before designing #15's stress harness so
  the harness exercises the right invariants.

### Failures resolved on 2026-05-29 (logged so the next agent
doesn't re-investigate)

- **Original `host_restart_live_writes_n0` failure mode**: was
  attributed in this session's first sweep to "n0 flake / Drop
  bomb noise." Diagnosed via the recipe in
  `docs/diagnosing-flaky-tests.md`. Root cause was twofold:
  (1) `Doc::share(.., AddrInfoOptions::default())` produced
  id-only tickets — fixed in `64aeeb1`; (2) post-restart
  peer-addr-loss — finding #5c above, deferred. The Drop-bomb
  fires observed in the failing log were *consequence* of the
  test panicking (workspaces dropped without `shutdown()` due
  to the panic), not the cause.
