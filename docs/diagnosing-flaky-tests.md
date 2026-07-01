# Diagnosing flaky tests

This is the methodology this repo uses for tests that fail
intermittently. Treat "flaky" as a label for "real bug we haven't
diagnosed yet" — never as a reason to ignore, retry, or just bump
timeouts. Concrete examples in this doc come from the
`iroh_docs_smoke` and `host_restart_*` investigations on
`emdash/stable-id-jx4uy`; the recipe generalises. (Those test files
were since consolidated into multi-section bins — see "Examples from
this codebase" at the bottom for where each lives now.)

The `flake-detective` agent (`.claude/agents/flake-detective.md`)
automates this recipe; point it at a flaky test rather than
re-deriving the process by hand.

## The recipe

### 1. Per-phase `tokio::time::timeout`

Wrap each meaningful step in a phase helper that prints begin/end
markers and panics with the **phase name** on timeout. A monolithic
30 s timeout tells you "the test hung." Per-phase timeouts tell you
*which* of N steps hung.

```rust
const PHASE_BUDGET: Duration = Duration::from_secs(20);

async fn phase<F, T>(name: &'static str, fut: F) -> T
where F: std::future::Future<Output = T>,
{
    eprintln!(">>> phase begin: {name}");
    let res = timeout(PHASE_BUDGET, fut)
        .await
        .unwrap_or_else(|_| panic!("phase hung past {PHASE_BUDGET:?}: {name}"));
    eprintln!("<<< phase end:   {name}");
    res
}
```

Use a `phase_budgeted("name", BIG_BUDGET, fut)` variant when one
step legitimately needs more time (real-network discovery,
multi-second sync, etc.). Different budgets per phase mean a slow
network step doesn't force the budget for everything else upward.

### 2. tracing-subscriber with wide `RUST_LOG` defaults

The substrate already calls `tracing::debug!` / `warn!` at every
decision point in the watcher, applier, and workspace lifecycle
(see `crates/artel-fs/src/{watcher,applier,workspace}.rs`). Tests
need to install a subscriber for those to be visible. Default the
filter wide enough that a captured failing log shows every layer
that could plausibly be the cause:

```rust
fn init_tracing() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| {
            concat!(
                "info,",
                "iroh=debug,iroh::discovery=trace,",
                "iroh_docs=debug,iroh_gossip=debug,iroh_blobs=debug,",
                "artel_fs=debug,artel_daemon=debug",
            ).to_string()
        });
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .with_test_writer()
            .with_target(true)
            .try_init();
    });
}
```

Honour `RUST_LOG` so investigators can narrow when chasing a
specific subsystem.

### 3. Run-until-failure to capture a real failing log

The next step after instrumenting is to actually look at a failing
run. A bash loop that captures full output and stops on the first
failure makes this trivial:

```bash
rm -f /tmp/iter_*.log
for i in $(seq 1 50); do
  cargo nextest run -p $crate --test $bin -E "test($name)" --no-capture \
    > /tmp/iter_$i.log 2>&1
  if [ $? -ne 0 ]; then
    echo "FAIL iter=$i log=/tmp/iter_$i.log"
    break
  else
    rm -f /tmp/iter_$i.log
  fi
done
```

For Tier C (`*_n0`) tests, add `--profile n0` — the default profile
filters them out — and space the iterations: n0 rate-limits under
back-to-back load, so a tight loop manufactures failures that look
like the bug but are just throttling.

Then read the failing log end-to-end. The truth is in there. The
`>>> phase begin:` and `<<< phase end:` markers tell you the
boundary; the iroh / substrate debug lines between them tell you
what was actually happening.

### 4. Match against the producer's own tests

If you're using a third-party crate (iroh, iroh-docs, etc.), check
their test suite at `~/.cargo/registry/src/.../<crate>-*/tests/`.
Their tests show how they intended the API to be used. iroh-docs's
own tests use a local `DnsPkarrServer` fixture and `presets::Minimal`
— *they* avoid n0's production discovery in their own tests because
they know it has propagation races. That is a strong signal.

Don't reinvent the address-lookup / discovery setup if upstream
provides a test fixture. `iroh::test_utils::DnsPkarrServer` exists
specifically for this; use it.

### 5. Two-tier test pyramid: `DnsPkarrServer` + real-n0

For cross-peer tests, run **both** of these and keep them
healthy:

- **`DnsPkarrServer` tests** (the default — `EndpointSetup::Testing`
  + `iroh::test_utils::DnsPkarrServer`): exercise the same
  pkarr-publish + DNS-resolve code path as production, but
  pointed at a localhost pkarr+DNS pair. Deterministic
  (`on_endpoint` gate eliminates the propagation race),
  localhost-fast, no n0 rate-limit exposure. Most cross-peer
  tests in this repo run on this fixture.
- **Real-n0 tests** (the production canary —
  `EndpointSetup::Production` or `ProductionCustomRelay`, test
  **fn names** suffixed `_n0` so the default nextest profile
  filters them and the `n0` profile selects them — see
  `.config/nextest.toml`): exercise n0's real pkarr/DNS
  infrastructure end-to-end. Catch infrastructure-layer bugs the
  localhost fixture can't see (relay session takeover for stable
  `EndpointId`s, propagation windows under load, n0 rate limits).
  Slower and occasionally flakier than the DnsPkarrServer tier;
  that's by design. Run via `make test-n0`; they need real
  pkarr+DNS reachability and are useless on restricted networks
  (plane/cafe wifi) even when the relay is localhost.

The pair is the diagnostic signal: when both fail, the bug is in
our substrate or in iroh. When only the n0 sibling fails, it's
n0 infrastructure flake (rate limit, propagation window) and the
substrate is fine. The pkarr sibling alone won't catch
production-only bugs (e.g. the relay-session-takeover bug
`host_restart_live_writes_n0` documents); the n0 sibling alone
won't reliably distinguish those from infra flakes. Keep both.

`MemoryLookup` is no longer in the pyramid. The substrate used to
expose an `address_lookup_override: Option<MemoryLookup>` knob
that took an in-memory address book; tests that used it short-
circuited too much of the production discovery path to catch real
bugs (`host_restart_ungraceful_n0`'s relay-rejection bug
reproduced only under real n0; the MemoryLookup sibling passed
silently). The migration to `DnsPkarrServer` happened in 2026-05.
The `EndpointSetup` enum has since grown beyond
`Production`/`Testing` (e.g. `TestingUnreachableRelay` for the
typed relay-error contract, `ProductionCustomRelay` for real-n0
discovery against a localhost relay) — every variant still runs
the real discovery code path; none reintroduces an in-memory
short-circuit. Don't reintroduce MemoryLookup. **Don't conflate
"passes under a hermetic fixture" with "works in production"** —
that's the insight DnsPkarrServer preserves and MemoryLookup
didn't.

When investigating *any* cross-process or cross-peer regression,
run the real-n0 variant before declaring a fix complete.

## What "good" diagnosis looks like

For a test failure to be considered diagnosed, the writeup must
answer all of:

1. **Which phase hung / failed?** Phase name from the panic.
2. **What's the last successful log line before failure?** Pinpoints
   which subsystem started failing.
3. **What's the actual error, and at what layer?** Not "DNS
   timeout" — the actual `LeafHashMismatch`, `LastOpenPath`,
   `Failed to resolve TXT record × 7`.
4. **Is the failure mode also reproducible deterministically?**
   Either by timing manipulation (e.g. fast Ctrl-C), by feature
   flag, or by `#[ignore]`d real-network test.
5. **Where is the bug, layer-wise?** Test-side, our substrate,
   third-party crate, or fundamental network?
6. **What's the fix, and at what layer?** "Bump the timeout"
   isn't a fix. Real fixes look like: gate on the right event;
   call shutdown before drop; emulate retry that real consumers
   need; switch to upstream's test fixture.

## What NOT to do

- **Don't `#[ignore]` a flaky test without a writeup of why it
  fails and what it proves about the system.** Ignoring is fine
  for tests that deliberately reproduce a known production bug
  (regression trap), or for tests gated behind opt-in slow paths.
  It is *not* fine as a way to make a failing test go away.
- **Don't bump timeouts as the only "fix."** If the underlying
  race exists, longer timeouts just shift the rate of
  reproduction.
- **Don't trust the existing handoff/docstring's diagnosis.**
  Re-derive it. The `iroh_docs_smoke` flake was attributed to
  "n0 rate limits" for an entire slice; the actual cause was two
  unrelated bugs (one ours, one upstream's missing retry).

## Case study: the auto-spawn timeout flakes (2026-06-10)

The `sessions.rs` auto-spawn tests (`happy_path_cold_dir_spawns_daemon`
and siblings) intermittently failed at the 5s
`DEFAULT_SPAWN_TIMEOUT` under full-suite runs, while passing 17/17 in
isolation. The diagnosis chain, recorded here because the verdict is
**not** airtight and may need reopening:

- **Root cause found (and fixed, `9a1a773`):** `PidFile::acquire` was
  check-then-write; two daemons racing a cold start could both "win",
  and the pidfile could end up naming the dead loser. The orphaned
  winner was then unkillable via pidfile-based teardown.
  `parallel_calls_settle_on_one_daemon` leaked exactly one daemon per
  full-suite run (3/3 reproductions, stderr captured). ~120 orphaned
  daemons had accumulated on the dev machine, each spinning on
  relay-reconnects forever (there is no post-startup relay-death exit
  path) and pkarr-publishing to real n0 DNS.
- **The herd was the suspected load source for the timeouts.** Direct
  CPU contention was ruled out by measurement: idle spawn→connectable
  is ~56ms, and even 12 saturated cores + 6 concurrent daemon spawns
  only reached ~77ms — nowhere near 5s.
- **Honest caveat: the original 4-test failure never reproduced in a
  clean environment** (6+ full-suite runs green, including relink-first
  and synthetic-herd variants). Herd-as-cause is strong circumstantial
  evidence — mechanism + correlation — not a smoking gun. **If those
  tests ever flake again with zero orphans present
  (`pgrep -fl artel-daemon`), the suspect list reopens.** Next suspect:
  macOS first-exec assessment storms — first exec of a freshly-linked
  binary inode costs ~800ms, and concurrent first-execs serialize
  globally (measured escalating 0.7s → 6.7s across 12 fresh inodes,
  with a daemon spawn racing the storm taking 7.3s). A `make test`
  right after a default↔all-features relink recreates exactly that
  shape.
- **Census the environment before blaming load.** Orphans whose
  `--state-dir` no longer exists (deleted tempdir) are provably leaked.
  A herd of them is invisible background load that surfaces as
  unrelated-looking timeout flakes days later.

## Case study: the relay-vs-direct handshake poisoning (2026-06-11)

`alice_post_restart_writes_reach_bob_real_n0` went from "intermittent"
(two captured failures on the orphan-polluted machine, 2026-06-09/10)
to **0/12 hard-fail on a clean machine** — zero orphans, pkarr publish
and DNS resolution verified working in-log. The deterministic sibling
stayed green throughout. The chain that found it, and the verdict:

- **The handoff doc's hypothesis was wrong, in a useful way.** It
  blamed "production DNS/relay timing variance." A `noq_proto=trace`
  run showed discovery and transport both *succeeding* — the failure
  was downstream of everything the hypothesis pointed at. Lesson
  re-learned from §"What NOT to do": re-derive, don't inherit.
- **Mechanism (upstream, noq-proto 0.17.0).** The joiner seeds the
  host's ticket addrs (relay URL + direct addrs) and dials. Same
  machine, so the Initial packet arrives at the host **twice**: via
  the relay transport (synthetic remote `fd15…:12345`, local IP never
  set) and via direct UDP (real remote, real local IP). The relay
  copy creates the server-side path; the direct copy is discarded as
  off-path — but not before `update_network_path_or_discard`
  (noq-proto 0.17.0 `connection/mod.rs:2309`) **learns its local IP
  onto the relay path**. The path is now a hybrid four-tuple
  `(local: real-interface-IP, remote: fd15…)` that no future packet
  can equal. The handshake-phase check (`mod.rs:4117`) compares
  four-tuples strictly, so every further relay-delivered handshake
  packet logs `discarding packet with unexpected remote during
  handshake` (35–99 per failing run) and the server-side handshake
  never completes. The dialer side completes ITS handshake, opens
  streams, and waits forever — which is why the failure surfaces as
  whatever timeout guards that phase.
- **One mechanism, many costumes.** Captured failure phases: gossip
  `JOIN_READY_TIMEOUT` (the original "Failure A"), iroh-docs
  `initial sync did not complete in 30s`, `grant_rw_and_wait` upgrade
  probe never reaching the host, daemon shutdown hang. Which costume
  you get depends on which connection (gossip / sync / upgrade) loses
  the relay-vs-direct race first. The original "Failure B" (9
  NeighborUps then post-restart stall) was this too — that run's
  gossip conns anchored direct-first and survived; a later conn
  didn't.
- **Why it changed character on 2026-06-10:** commit `44a4f8b` moved
  these tests from n0's public relay to an in-process localhost
  relay. Against the public relay, the same-machine direct packet
  always beat the cross-internet relay packet → poisoning was rare →
  "flaky". Against a localhost relay the relay copy reliably wins →
  deterministic failure. The test got *more* honest, not more broken.
- **Confirmation experiment (the causal flip):** temporarily seeding
  the dial with relay-URL-only (no direct addrs) in
  `wire_addr_to_iroh` flipped the gossip-join phase from 0/12 to
  passing — and the failure *moved downstream* to iroh-docs sync,
  whose connections iroh dials from its own addr book (relay + direct
  learned via discovery), outside our control. 2/9 full passes, 7/9
  failed at later phases with the same discard signature on the
  host's sync/gossip accepts. That ruled out the workaround AND
  triple-confirmed the mechanism: any iroh-internal dial that knows
  both transports can poison its accept path. Experiment reverted.
- **Layer verdict: upstream, fixed in iroh 1.0.0-rc.** noq-proto
  1.0.0-rc.1 changes the handshake comparison to
  `is_probably_same_path` (a path with no local IP matches any local
  IP) and gates local-IP learning on `is_handshake_confirmed()` —
  both verified present in the released rc crate source. See iroh
  PRs #4273/#4281 (four-tuple rework, called out in the rc1 blog
  post). No artel-side fix is correct: the substrate never gets to
  run, and per the gossip-only memory we don't want to re-plumb
  dialing.
- **Blast radius: the whole localhost-relay slice of the n0 tier.**
  A full-tier `--no-fail-fast` run confirmed 4 failures, all
  two-peer dials through the in-process relay
  (`alice_post_restart_writes_reach_bob_real_n0`,
  `capability_survives_host_restart_n0`,
  `legit_host_frames_accepted_n0`,
  `join_succeeds_within_tight_budget_real_n0` — the last one's
  "waiting on pkarr/DNS propagation" panic message was a
  mis-attribution; the addr hint worked, the acceptor's handshake
  was wedged). Every public-relay test (`crash_recovery_*_n0`,
  `doc_ticket_*_n0`) passed: against a cross-internet relay the
  same-machine direct packet always wins the race, keeping the bug
  dormant. `forged_session_closed_dropped_n0` passed once by winning
  that race — expect it intermittent, not safe.
- **Interim resolution (until iroh 1.0 stable + companion crates;
  iroh-docs/gossip/blobs currently pin `=1.0.0-rc.1`):** the `_n0`
  two-peer tests dial through n0's **public** relay
  (`EndpointSetup::Production`) again, restoring their coverage.
  This re-adds a Tier C dependency on n0's relay, but Tier C already
  depends on n0's DNS/pkarr by definition; Tier A+B stay fully
  hermetic on the localhost relay. Each touched test carries an
  INTERIM comment; when the iroh 1.0 upgrade lands, grep for
  `INTERIM (iroh 0.98.2)` and revert them to
  `common::shared_relay_url()` + `ProductionCustomRelay`, then
  re-run the tier 10×. If a reverted test still fails, that's a real
  regression: investigate, don't ignore.
- **Portable lessons:** (1) a flake that becomes deterministic after
  an environment change is a *gift* — diff the environments before
  diffing the code; (2) when a passing side thinks a connection is
  open and the accepting side never fires its callback, trace the
  **accepting side's transport layer** before suspecting the
  protocol library above it; (3) `RUST_LOG=noq_proto=trace` is the
  tool of last resort that actually ends arguments — packet-level
  discard logs named the exact line.

## Case study: debouncer Create+Remove annihilation (2026-07-01)

`recreating_identical_bytes_after_delete_propagates` (new in the
echo-guard PR, #6) deadlocked **deterministically on ubuntu CI on its
first-ever Linux run** while passing 15/15 on the macOS dev machine.
Not a flake — an OS-coverage gap. The chain and the verdict:

- **Failing log first, mechanism second.** The captured CI log showed
  the test's act-2 `remove_file` on alice's side produced *no watcher
  event at all*: no `on_removed`, no `doc.del`, no tombstone. Bob's
  copy never disappeared and `wait_for_missing` timed out.
- **Mechanism (by design, upstream).** Act 1 ended with alice's
  *applier* re-creating the file; act 2 deleted it ~300 ms later —
  inside the watcher's debounce window. notify-debouncer-full's model
  is "created then removed within one window = never existed": a
  Remove arriving while a Create is still queued annihilates both.
  Verified in the crate's own test suite
  (`notify-debouncer-full-0.6.0/test_cases/`
  `add_remove_event_after_create_event.hjson`: Remove after queued
  Create ⇒ `expected: {}`). §4's "match against the producer's own
  tests" applies to *behaviour* questions, not just API-usage ones.
- **Why macOS never saw it:** FSEvents reports post-unlink
  `Modify(Metadata)`/`Modify(Data)` events that reach `on_modified`'s
  NotFound→tombstone fallback regardless of the debouncer queue.
  Linux's clean `Remove` has no such second chance. A test that only
  ever ran on one OS has only ever tested one delete path.
- **Fix: an ordering guarantee, not a sleep.** The first instinct —
  sleep past the debounce window — is exactly what this doc says not
  to do. The shipped fix reworks act 2 so the *other* peer (bob)
  deletes: act 1 already ends with `wait_for_file(alice_path)`, which
  can only pass if bob's watcher flushed its debounce window and
  published the file. Bob's delete is therefore from steady state by
  construction. When a test needs "the watcher has settled", find the
  event that *proves* it settled; every phase transition should ride
  an observable effect, not wall-clock.
- **Known substrate edge left behind (recorded, not fixed):** a
  genuine local delete landing within the debounce window of a
  peer-driven create of the same path is silently swallowed on Linux
  — no tombstone, disk↔doc diverge until a workspace restart's
  `reconcile_doc_against_disk`. Real fix belongs at the watcher layer
  (e.g. don't annihilate a Remove whose path was previously synced,
  or a periodic reconcile). If a "deleted file reappears / peers
  never see my delete" report comes in on Linux, start here.

## Examples from this codebase

(Paths updated after the 2026-05-29 test-consolidation plan merged
the old per-file bins into multi-section bins.)

- `crates/artel-fs/tests/iroh_internals.rs` —
  `doc_ticket_round_trips_without_manual_address_seeding_n0`:
  production discovery + retry loop on dial failure; demonstrates
  the pkarr-propagation race and how to handle it as a real
  consumer would. Its deterministic sibling
  `doc_ticket_round_trips_via_localhost_pkarr_dns` runs the same
  property against `iroh::test_utils::DnsPkarrServer` — the
  two-tier pyramid of §5 inside one bin. (Formerly
  `iroh_docs_smoke.rs` / `iroh_docs_smoke_pkarr.rs`.)
- `crates/artel-fs/tests/workspace_restart.rs` —
  `alice_post_restart_writes_reach_bob_real_n0` and its Tier-B
  sibling `alice_post_restart_writes_reach_bob`: host-restart
  live-writes property with `Workspace::shutdown` properly
  called. (Formerly `host_restart_live_writes_n0.rs`.)
- `crates/artel-fs/tests/drop_bomb.rs` — pins the `Workspace::Drop`
  contract using a child process to capture stderr
  deterministically. Replaces the older `_ungraceful_n0` test
  (deleted 2026-05); the contract is "the substrate makes
  ungraceful drops loud," and a child-process stderr capture
  asserts that without the n0 round-trip.
- `crates/artel-daemon/src/pidfile.rs` —
  `concurrent_acquires_settle_on_exactly_one_winner`: regression
  trap for the 2026-06-10 orphan-leak case study above; an
  8-thread barrier race that the pre-flock protocol fails.
