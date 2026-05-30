# Faster `cargo test --workspace` — implementation plan

**Landed in full at commit `6d22e61` (slice 5).** Slice 6 (this
roadmap-and-handoff doc update) is the closing step.

## Status

All slices LANDED. Slices 2f, 2g, 3c were no-ops (already standalone).
Suite at 444 passed + 5 `*_n0` skipped under default nextest profile,
5 passed serially under the `n0` profile, fmt + clippy clean both
feature modes.

| Slice | Commit | What landed |
|---|---|---|
| pre-1 | `439b83a` | clear pre-existing fmt + clippy violations on the branch |
| 1 | `83fce54` | `.config/nextest.toml` (default/ci/n0 profiles + tier-c-serial test-group); rename `iroh_docs_smoke` + 2 in `iroh_identity` to `*_n0` |
| 2a | `d338ced` | `workspace_lifecycle.rs` ← 7 files, 18 tests |
| 2b | `a09af97` | `workspace_filter.rs` ← 10 files, 11 tests |
| 2c | `a89ab14` | `workspace_sync.rs` ← 6 files, 7 tests |
| 2d | `36b47c2` | `workspace_restart.rs` ← 5 files, 5 tests (first n0 in shared bin) |
| 2e | `a3ad106` | `iroh_internals.rs` ← 3 files, 3 tests |
| 2f, 2g | (no commit) | `drop_bomb` + `crash_recovery` confirmed standalone (child-process bins) |
| 3a | `53f4d51` | `sessions.rs` ← 4 files, 14 tests (Tier A) |
| 3b | `fee5f7d` | `gossip.rs` ← 8 files, 8 tests (Tier B) |
| 3c | (no commit) | `attachments.rs` confirmed standalone |
| 3d | `d2825c0` | `identity.rs` ← 4 files, 7 tests (mixed-tier — first daemon-side bin where the `*_n0` filter matters) |

Cumulative result: ~50 → 13 integration bins, **−1539 lines** net,
test count unchanged.

**Open:** slices 4 (drop `#[ignore]` from the 2 remaining Tier C
tests), 5 (CI / Make / contributor docs), 6 (roadmap → DONE).

---

Source brainstorm: `docs/brainstorms/2026-05-29-faster-cargo-test-brainstorm.md`. Roadmap entry: `docs/roadmap.md` § "Future" → "Faster `cargo test --workspace`" (the bullet under that heading). The brainstorm locks every design decision: adopt `cargo-nextest` with a tiered pyramid (Tier A → B → C with fail-fast across boundaries), consolidate the ~50 one-test-per-file integration binaries into ~10–12 by-subsystem files, defer `OnceCell<Pair>` fixture sharing. This plan is *how*, not *what*.

The migration must end with all of:

- `.config/nextest.toml` with `default` / `ci` / `n0` profiles
- ~11 bin-per-subsystem test files (down from ~50)
- Tier C tests live INSIDE their subsystem file, named with a `*_n0` test-fn suffix; `#[ignore]` removed
- Tier A = no iroh `Endpoint` bound; Tier B = iroh hermetic (`DnsPkarrServer` + `TestingUnreachableRelay`); Tier C = real n0
- `cargo test --workspace` still works as a fallback (doctests stay on `cargo test --doc` because nextest doesn't run them)
- `Makefile` + `.github/workflows/ci.yml` + a contributor-doc note all updated
- `docs/roadmap.md` § "Future" → "Faster `cargo test --workspace`" replaced with a "DONE" pointer at this plan + commit hash
- `docs/handoff-code-review-fixes.md` § "Open" → "Start here" stays at finding #8 but the cross-reference text gets updated to "test-infra has landed" instead of "pairs naturally with"

---

## Pre-flight: validated test inventory

Walked every `tests/*.rs` in `crates/artel-fs/`, `crates/artel-daemon/`, and `crates/artel-client/`. Tier classification rule from the brainstorm: **A = no iroh `Endpoint` bound; B = iroh hermetic; C = real n0.** "Hermetic" includes `DnsPkarrServer`-based fixtures *and* `TestingUnreachableRelay` (RFC 5737 TEST-NET-1 — provably unreachable, no external dependency).

`crates/artel-fs/tests/` (33 files):

| File | Endpoint setup today | Tier | Subsystem |
|---|---|---|---|
| `attach_policy_host.rs` | daemon `Production` + `iroh_key_path: None` (no iroh actually bound on daemon side) | B (Workspace::host binds an iroh node) | `workspace_lifecycle` |
| `attach_policy_join.rs` | `DnsPkarrServer` | B | `workspace_lifecycle` |
| `attach_policy_state_dir_only.rs` | daemon `Production` + `iroh_key_path: None` | B | `workspace_lifecycle` |
| `crash_recovery.rs` | parent `DnsPkarrServer`; child `crash_child` real n0 | C (child binds n0) | `crash_recovery` (own bin — child-process pattern) |
| `default_read_write_unchanged_behaviour.rs` | `DnsPkarrServer` (`spawn_pair`) | B | `workspace_filter` |
| `delete_propagates.rs` | `DnsPkarrServer` | B | `workspace_sync` |
| `disk_resume.rs` | `DnsPkarrServer` | B | `workspace_restart` |
| `drop_bomb.rs` | parent `Production`+`None`; child `drop_bomb_child` real n0 | C (child binds n0; #9 will flip to B but #9 lands AFTER this migration) | `drop_bomb` (own bin — child-process pattern) |
| `empty_file_no_error.rs` | `DnsPkarrServer` | B | `workspace_sync` |
| `host_publishes_ticket.rs` | daemon `Production`+`None`; Workspace::host binds iroh | B | `workspace_lifecycle` |
| `host_restart_live_writes.rs` | `DnsPkarrServer` | B | `workspace_restart` |
| `host_restart_live_writes_n0.rs` | real n0 (`#[ignore]`) | C | `workspace_restart` (test fn renamed `*_n0`) |
| `host_restart_ticket_stable.rs` | daemon `Production`+`None`; Workspace::host binds iroh | B | `workspace_restart` |
| `host_resume_session_id.rs` | `DnsPkarrServer` | B | `workspace_restart` |
| `iroh_docs_smoke.rs` | real n0 (no `#[ignore]`, currently runs on every `cargo test`) | C | `iroh_internals` (test fn renamed `*_n0`) |
| `iroh_docs_smoke_pkarr.rs` | `DnsPkarrServer` | B | `iroh_internals` |
| `join_bulk_export.rs` | `DnsPkarrServer` | B | `workspace_sync` |
| `join_ticket_timeout.rs` | `DnsPkarrServer` | B | `workspace_sync` |
| `live_edit.rs` | `DnsPkarrServer` | B | `workspace_sync` |
| `mixed_rules_first_match_wins.rs` | `DnsPkarrServer` | B | `workspace_filter` |
| `read_only_incoming_blocks_apply.rs` | `DnsPkarrServer` | B | `workspace_filter` |
| `read_only_outgoing_blocks_publish.rs` | `DnsPkarrServer` | B | `workspace_filter` |
| `read_only_outgoing_blocks_scan.rs` | `DnsPkarrServer` | B | `workspace_filter` |
| `read_only_post_join_live_blocks.rs` | `DnsPkarrServer` | B | `workspace_filter` |
| `read_only_post_join_live_delete_blocks.rs` | `DnsPkarrServer` | B | `workspace_filter` |
| `relay_unreachable.rs` | `TestingUnreachableRelay` | B | `iroh_internals` |
| `round_trip.rs` | `DnsPkarrServer` | B | `workspace_sync` |
| `run_readiness.rs` | daemon `Production`+`None`; Workspace::host binds iroh | B | `workspace_lifecycle` |
| `ticket_envelope_rejects_old_shape.rs` | `DnsPkarrServer` | B | `workspace_filter` |
| `ticket_envelope_round_trip.rs` | `DnsPkarrServer` | B | `workspace_filter` |
| `tombstone_filter_check.rs` | `DnsPkarrServer` | B | `workspace_filter` |
| `workspace_attachment.rs` | `DnsPkarrServer` | B | `workspace_lifecycle` |
| `workspace_shutdown_contract.rs` | `DnsPkarrServer` | B | `workspace_lifecycle` |

`crates/artel-daemon/tests/` (17 files):

| File | Tier | Subsystem |
|---|---|---|
| `attachments.rs` | A (`Production`+`None`, no iroh bound) | `attachments` |
| `auto_spawn.rs` | A (single-daemon spawn lifecycle, no iroh bound) | `sessions` |
| `end_to_end.rs` | A (`Production`+`None`, no iroh bound) | `sessions` |
| `host_resume.rs` | A (`Production`+`None`, no iroh bound) | `sessions` |
| `iroh_gossip_fanout.rs` | B | `gossip` |
| `iroh_gossip_smoke.rs` | B (`DnsPkarrServer`) | `gossip` |
| `iroh_identity.rs` | C (some test variants with `iroh_key_path: Some` + `Production` bind real n0) | `identity` (test fn renamed `*_n0` for the n0-binding subset) |
| `iroh_join_addr_hint_n0.rs` | C (`#[ignore]`) | `identity` (test fn renamed `*_n0`) |
| `iroh_join_announcement.rs` | B | `gossip` |
| `iroh_joiner_send_fanout.rs` | B | `gossip` |
| `iroh_joiner_send_rejected.rs` | B | `gossip` |
| `iroh_remote_mirror_persists_log.rs` | B (`DnsPkarrServer`) | `gossip` |
| `iroh_session_closed.rs` | B | `gossip` |
| `iroh_subscribe_replay.rs` | B | `gossip` |
| `peer_addr_cache_pkarr.rs` | B (`DnsPkarrServer`) | `identity` |
| `persistence.rs` | A (`Production`+`None`, no iroh bound) | `sessions` |
| `relay_unreachable.rs` | B (`TestingUnreachableRelay`) | `identity` |

`crates/artel-client/tests/` (1 file):

| File | Tier | Subsystem |
|---|---|---|
| `client.rs` | A (`Production`+`None`, no iroh bound) | already a single-bin file — leave as-is |

### Tests that don't fit cleanly

- **`iroh_identity.rs` is mixed-tier today.** `endpoint_id_is_stable_across_daemon_restarts` and `host_ticket_carries_a_real_endpoint_addr` use `iroh_key_path: Some(...)` + `Production` (bind real n0); `iroh_key_file_is_chmod_0600` and `no_iroh_key_path_keeps_synthetic_peer_id` are file-only (Tier A). After consolidation into `identity.rs`, the n0-binding test fns get the `*_n0` suffix; the file-only ones stay tier-A-flavoured (the test name suffix is what nextest's filter expression matches, not file location). Verified by reading the file at consolidation time.
- **`iroh_docs_smoke.rs` is currently real-n0 with NO `#[ignore]`** — it runs on every `cargo test --workspace` today. After consolidation into `iroh_internals.rs` the test fn gets the `*_n0` suffix and stops running on `cargo nextest run` (default profile filters out `_n0`). The pkarr sibling (`iroh_docs_smoke_pkarr.rs`) takes over the on-every-PR canary role; the `_n0` sibling moves to the `n0`/`ci` profile. This is a behavioural shift CI-side: the n0 smoke test stops running on every dev-machine `cargo nextest run` invocation. Documented loudly in slice 5's contributor-doc update.
- **`drop_bomb.rs` and `crash_recovery.rs` both spawn child binaries.** They stay as their own consolidated bins (one test fn each — no consolidation gain) because the child-binary plumbing is bin-specific (`CARGO_BIN_EXE_drop_bomb_child` / `CARGO_BIN_EXE_crash_child`). Brainstorm flagged drop_bomb explicitly; crash_recovery has the same shape so I'm grouping it identically.
- **`auto_spawn.rs` is technically about session bootstrap, not "session lifecycle"** in the same sense as `end_to_end` / `host_resume` / `persistence`. Could be a fifth `sessions/` bin (`bootstrap.rs`) or folded into `sessions.rs`. Recommendation: fold; ~7 tests in one bin is fine.

### Drop-bomb child + finding #9

Confirmed `tests/bin/drop_bomb_child.rs` uses `WorkspaceConfig::default()` → `EndpointSetup::Production` → real n0 today (line 98). Finding #9 in `docs/handoff-code-review-fixes.md` flips the child to `EndpointSetup::TestingExternal { nameserver, pkarr_url }` (a new variant pointing at the parent's `DnsPkarrServer` over env-var-passed sockets). Per the prompt's "Things to NOT include" list, **finding #9 lands AFTER this migration**. So during this plan:

- `drop_bomb.rs` stays Tier C (test fn renamed `*_n0` if we're consistent — but the bomb test isn't a real-n0 *property* test, just happens to bind real n0 because the child does). **Decision:** keep the test fn names unsuffixed, leaving `drop_bomb.rs` in the default profile. Rationale: the `_n0` suffix marks "this test is the production canary for an n0-touching property" (per `docs/diagnosing-flaky-tests.md` § 5 two-tier pyramid). Drop-bomb is *not* an n0-property test — it's a local Drop-contract test that happens to bind n0 because the child fixture hasn't migrated yet. Marking it `_n0` would mis-signal its purpose and route it to the wrong profile.
- The `drop_bomb` consolidated bin runs in the default profile, paying real n0 cost on every dev `cargo nextest run`, until #9 lands and converts the child to `TestingExternal`. Acceptable because there's only one test per side (`bomb_fires` + `bomb_quiet`) and they're <5s each.
- Same applies to `crash_recovery.rs` (3 tests). The roadmap's "Future" section already calls out converting `crash_child` to a Testing fixture as future work, not part of this migration.

### Test-runtime sampling for budget calibration

Cargo build of all test bins green (`cargo build --workspace --tests --all-features`). Per-test runtimes are only measurable from a real run; sampling each tier without running the suite is impractical here. **Recommendation: pick budgets from the brainstorm's draft and let slice 1 confirm/adjust them.**

- Tier A: 10 s slow-timeout. Unit tests + IPC-only integration tests. Rationale: any "tier A" test taking >10s is misclassified — surface it in slice 1 as a re-classification finding.
- Tier B: 60 s slow-timeout. Cross-peer integration over `DnsPkarrServer`. Rationale: `round_trip.rs` runs three full sequential round-trips today (its `round_trip_3_in_a_row` outer fn). 60 s covers worst-case CI host load with ~3× wall-clock for sync + applier + watcher debounce; tighter would re-introduce the kind of race the suite has been fighting.
- Tier C: 300 s slow-timeout. Real n0. Rationale: `host_restart_live_writes_n0.rs` already uses per-phase timeouts of 20 s with multiple phases. 300 s envelope budget covers the full sequence + relay handshake + propagation lag.

Slice 1 includes a one-shot calibration: run the existing `cargo test --workspace --all-targets` once with `cargo nextest run --workspace`, capture per-test wall-clock from nextest's report, and adjust budgets in `nextest.toml` if the data shows them too generous. **Mandatory:** redirect the run output to `/tmp/nextest_calibration.log` per the [[feedback-redirect-test-output]] rule; never tail-eyeball.

### Existing make/CI/pre-commit wiring

- **No Makefile.** None at repo root, none in any crate. Slice 5 adds one.
- **No pre-commit hooks.** No `.pre-commit-config.yaml`, no `.husky/`, no `pre-commit`-named files. Slice 5 mentions the option in contributor docs but does not add one (out of scope for this migration).
- **`.github/workflows/ci.yml`** has three jobs: `fmt`, `clippy`, `test`. The `test` job runs:
  ```
  cargo build --workspace --all-targets --all-features
  cargo test --workspace --all-targets
  cargo test --workspace --all-targets --all-features
  cargo test --workspace --doc --all-features
  ```
  Slice 5 replaces the two `cargo test --workspace --all-targets` lines with `cargo nextest run` invocations + an n0-tier step; keeps `cargo test --workspace --doc` (nextest doesn't run doctests).
- **No CONTRIBUTING.md** at repo root. Slice 5 adds a minimal one or appends to README — choice deferred to the slice-5 implementor.

---

## Slice ordering — validated

Brainstorm proposed five steps:
1. nextest config first
2. subsystem consolidation
3. drop `#[ignore]`s
4. update make/CI/docs
5. roadmap → DONE

**Validated; refining as 6 slices** to make the per-crate consolidation and the docs-update explicit:

1. **Slice 1 — nextest dev-dep + `nextest.toml` profile config** (no test changes)
2. **Slice 2 — `artel-fs` consolidation** (sub-slices 2a–2g, one PR per consolidated subsystem)
3. **Slice 3 — `artel-daemon` consolidation** (sub-slices 3a–3d)
4. **Slice 4 — drop `#[ignore]` attributes from Tier C tests**
5. **Slice 5 — wire CI / Make / contributor docs**
6. **Slice 6 — roadmap → DONE; update handoff-code-review-fixes pointer**

**Why this order, not the brainstorm's strict ordering:**

- Slice 1 first: validates nextest works against the current pile of files. If nextest can't run the existing suite cleanly, find that out in slice 1, not after consolidating 30+ files.
- `artel-fs` (slice 2) before `artel-daemon` (slice 3): bigger crate (33 → 7 bins vs 17 → 4), higher consolidation value, harder shape (mixes `DnsPkarrServer` + `TestingUnreachableRelay` + child-process bins). Doing `artel-fs` first surfaces shared-helper patterns that `artel-daemon` then picks up cleanly. Either order works; this is the recommendation.
- Slice 4 (`#[ignore]` removal) after consolidation: Tier C tests need to live in their subsystem file with `*_n0` suffix first; removing `#[ignore]` against a not-yet-renamed test fn would land them in the wrong nextest profile.
- Slice 5 (CI/Make wiring) before slice 6 (roadmap → DONE): roadmap can only mark DONE once CI is on nextest — that's the migration's binding contract.

Each slice ends with green tests (`cargo nextest run --workspace` + `cargo test --workspace --doc`) + clean fmt/clippy in both feature modes. Each slice is independently mergeable. The per-subsystem sub-slices in slice 2 and slice 3 are independently mergeable too — each is one consolidated bin, ~5–10 files merged into 1, ~50 lines of `tests/common/mod.rs` change at most.

---

## Slice 1 — nextest dev-dep + `nextest.toml`

**Goal:** Stand up `cargo-nextest` against the current 50-file suite. Validate it runs green; calibrate per-tier timeouts. Zero test-file changes.

### Files touched

- **`.config/nextest.toml`** — new file. Three profiles (`default`, `ci`, `n0`) + one test-group (`tier-c-serial`). The brainstorm's `tool.nextest.toml` vs `.config/nextest.toml` open question resolves to **`.config/nextest.toml`** — that's the standard repo-level path nextest looks for; `tool.nextest.toml` is an obsolete pre-0.9 location. (Confirm against nextest's `--help-config` docs at slice-1 implementation time.)
- **`Cargo.toml`** (workspace root) — no change. Nextest is invoked as `cargo nextest run` and doesn't need to be in `[dev-dependencies]` (it's a separate binary). Contributor-doc note in slice 5 says how to install (`cargo install cargo-nextest --locked`).
- No source changes, no test-file changes, no `Cargo.lock` change.

### `nextest.toml` shape

```toml
# .config/nextest.toml
#
# Tiered test pyramid for the artel workspace.
#
#   Tier A — no iroh `Endpoint` bound.        Default profile.  Fast.
#   Tier B — iroh hermetic (DnsPkarrServer +
#            TestingUnreachableRelay).         Default profile.  Medium.
#   Tier C — real n0.                          Only the `n0` /
#            Test fn names suffix `*_n0`.      `ci` profiles.    Slow.
#
# Conventions:
#   - Every test FUNCTION whose body binds real n0 (presets::N0,
#     EndpointSetup::Production with iroh actually started) has its
#     name suffixed `_n0`. nextest groups tests by name pattern,
#     not file path, so this works inside a consolidated bin too.
#   - The `tier-c-serial` test-group ensures Tier C runs serially
#     so the captured tracing log of any failing iteration is a
#     single coherent timeline (per docs/diagnosing-flaky-tests.md).
#   - cargo test still works as a fallback: `cargo test --workspace
#     --all-targets` (slow but unchanged) and `cargo test --workspace
#     --doc` (nextest doesn't run doctests, so doctests stay here
#     in either runner).

[profile.default]
# Developer ergonomics: `cargo nextest run` runs Tier A + B with
# fail-fast. Tier C tests are filtered out (developers run them
# manually with `--profile n0`).
fail-fast = true
slow-timeout = { period = "60s", terminate-after = 2 }
default-filter = "not test(/_n0$/)"

[profile.ci]
# CI runs Tier A + B + C. fail-fast across the boundary so a
# failing A or B test doesn't burn the slow C tier. Tier C is
# launched as a separate `cargo nextest run --profile ci-n0`
# invocation by the CI script; this profile covers A + B.
fail-fast = true
slow-timeout = { period = "60s", terminate-after = 2 }
default-filter = "not test(/_n0$/)"

[profile.n0]
# Tier C only. Serial within the tier (test-threads=1 + test-group
# below). 300s slow-timeout because real-n0 needs propagation
# windows. Used for:
#   - the manual run-until-fail loop documented in
#     docs/diagnosing-flaky-tests.md
#   - the CI's tier-C invocation (after the default profile
#     passes), via `cargo nextest run --profile n0`
slow-timeout = { period = "300s", terminate-after = 1 }
test-threads = 1
default-filter = "test(/_n0$/)"

[[profile.n0.overrides]]
filter = "test(/_n0$/)"
test-group = "tier-c-serial"

[test-groups.tier-c-serial]
max-threads = 1
```

**Notes:**
- The exact filter expression syntax (`test(/_n0$/)`) — nextest's filter DSL takes a regex inside `/.../`. Double-check against nextest's `--help-config` output during slice 1; if the syntax differs, fix it. This is the kind of thing that fails fast in slice 1.
- `default-filter` at profile level (not as a CLI arg) means `cargo nextest run` without any `-E` argument respects the profile's default filter. Confirmed available in nextest 0.9.x.
- `test-threads = 1` on the profile sets global concurrency; the `tier-c-serial` test-group is belt-and-braces (so even if a tier-C test escapes the profile by being run with a different invocation, it's still serial).

### Definition of done

1. `.config/nextest.toml` exists with the three profiles + one test-group as above.
2. `cargo install cargo-nextest --locked` succeeds in a clean environment (verify in CI's first run — slice 5 adds this step, but check ad-hoc here).
3. `cargo nextest run --workspace` (default profile) runs the current 50-file suite green. Tier C tests (currently `host_restart_live_writes_n0.rs` test fns + `iroh_join_addr_hint_n0.rs` + `iroh_docs_smoke.rs` + the n0-binding subset of `iroh_identity.rs`) are filtered out — `default-filter = "not test(/_n0$/)"` matches by test-fn name. **Important:** today, `host_restart_live_writes_n0.rs` test fns are named `alice_post_restart_writes_reach_bob_real_n0` and `iroh_join_addr_hint_n0.rs` is `join_succeeds_within_tight_budget_real_n0` — both already match `_n0$`. `iroh_docs_smoke.rs::doc_ticket_round_trips_without_manual_address_seeding` does NOT match — and it's the test that currently runs on every CI invocation. **Slice 1 must rename this test fn to `*_n0`** (single line change) so the default profile filters it out; otherwise CI's first nextest run will hit n0 and the migration's main premise (default = no n0) is broken. The same rule applies to the n0-binding test fns in `iroh_identity.rs` — the file isn't consolidated yet, but the test-fn rename can happen in slice 1 as the smallest-possible change to align the default profile with the design.
4. Per-test wall-clock times captured into `/tmp/nextest_calibration.log` via `cargo nextest run --workspace > /tmp/nextest_calibration.log 2>&1` (no `--nocapture`; nextest emits per-test summary lines). Read the log; if any "tier B" test takes >60s under nominal load, bump that profile's `slow-timeout` and document why; if any "tier A" test takes >10s, surface it as a re-classification finding.
5. `cargo nextest run --workspace --profile n0` runs the four currently-known Tier C tests serially. Run-until-fail loop per `docs/diagnosing-flaky-tests.md` § 3 still works.
6. `cargo test --workspace --all-targets` still runs green (fallback unchanged).
7. fmt + clippy clean both feature modes.

### Out of scope for slice 1

- Consolidation (slices 2 + 3).
- `#[ignore]` removal (slice 4) — Tier C tests stay `#[ignore]`'d for now; the `cargo nextest run --profile n0 -- --include-ignored` invocation runs them. **Note:** `--include-ignored` is a `cargo test` flag; nextest's equivalent is `--run-ignored=all`. Confirm syntax at slice-1 time.
- Make/CI/pre-commit (slice 5).
- Roadmap update (slice 6).

---

## Slice 2 — `artel-fs` consolidation

**Goal:** ~33 test files → 7 bins. Each sub-slice is one consolidated bin, mergeable independently.

### Sub-slice ordering

Within slice 2, sub-slices land in this order:

1. **2a — `workspace_lifecycle.rs`** (5 files → 1; 7 tests). Smallest semantic-coherence story; easiest to validate the consolidation pattern works end-to-end before scaling up.
2. **2b — `workspace_filter.rs`** (10 files → 1; 12 tests). The biggest single consolidation gain; once 2a's pattern is proven, this is mostly mechanical merge.
3. **2c — `workspace_sync.rs`** (6 files → 1; 6 tests).
4. **2d — `workspace_restart.rs`** (5 files → 1; 6 tests; includes the renamed `*_n0` test fns).
5. **2e — `iroh_internals.rs`** (3 files → 1; 3 tests; one is `*_n0`).
6. **2f — `drop_bomb.rs`** (no consolidation — already a single bin; no rename). Listed as a sub-slice only because the brainstorm calls it out as "special: spawns child binary, keep separate". Confirms the bin's name and Cargo.toml entry stay.
7. **2g — `crash_recovery.rs`** (no consolidation — same as 2f).

**Why 2a → 2b → 2c → 2d → 2e:** lifecycle is the smallest and proves the pattern; filter is the biggest and gets the lion's share of consolidation value once the pattern is proven; sync is straightforward cross-peer; restart pulls the first Tier C tests into a consolidated bin (touches the `_n0` rename + ignored-attr handling); iroh_internals is the cleanest "bin per concept" with the `iroh_docs_smoke_pkarr` + `iroh_docs_smoke` (`_n0` rename + `#[ignore]` removal still in slice 4) pair already structured the way they want to be consolidated. 2f and 2g are no-ops; listing them prevents future confusion about whether they were "missed."

### Sub-slice 2a — `workspace_lifecycle.rs`

#### Files touched

- **New: `crates/artel-fs/tests/workspace_lifecycle.rs`** — combines all test fns from the five files below.
- **Delete:**
  - `crates/artel-fs/tests/attach_policy_host.rs` (3 tests)
  - `crates/artel-fs/tests/attach_policy_join.rs` (2 tests)
  - `crates/artel-fs/tests/attach_policy_state_dir_only.rs` (1 test)
  - `crates/artel-fs/tests/host_publishes_ticket.rs` (1 test)
  - `crates/artel-fs/tests/run_readiness.rs` (2 tests)
  - `crates/artel-fs/tests/workspace_attachment.rs` (6 tests) **[arguably its own bin — see decision below]**
  - `crates/artel-fs/tests/workspace_shutdown_contract.rs` (3 tests)
- **Edit:** `crates/artel-fs/tests/common/mod.rs` if helpers diverge (probably not — the existing `spawn_pair` / `spawn_local_daemon` / `spawn_daemon_with_setup` triple covers everything). Re-read after consolidation; if any test-private helper got copy-pasted across files, hoist to `common/mod.rs`. The fixture rewrite #8 in `docs/handoff-code-review-fixes.md` is **not** in scope here — it lands after this migration per the handoff doc's "Open" → "Start here" pointer.

**Decision: split `workspace_attachment.rs` out into its own bin or fold it into `workspace_lifecycle`?** Today it's 6 test fns covering the daemon-IPC-side workspace attachment flow (`host_workspace_registers_attachment_via_ipc`, `attachment_persists_across_daemon_restart`, etc). Semantically these are about the IPC contract that `Workspace::host_with` / `join_with` hits when registering a `WorkspaceAttachmentV1`. Two reasonable groupings:

- **(a) Fold into `workspace_lifecycle.rs`** (recommendation). 13 tests in one bin is fine; reduces bin count by 1.
- **(b) Keep as `workspace_attachment.rs`** standalone. Bin count goes to 8 instead of 7.

Recommend (a) — workspace_attachment is a lifecycle property (registers on host_with, cascades on leave). The brainstorm's grouping put `attachments` under daemon-side, which is the IPC contract; the artel-fs side is "what does host_with/join_with do at the workspace lifecycle boundary". Folding is the cleaner story.

#### Public-API additions

**None.** Pure test consolidation.

#### Definition of done

1. `cargo nextest run --workspace -E 'binary(workspace_lifecycle)'` runs all 13 (or 7 if option (b)) tests green.
2. `cargo nextest run --workspace` (default profile) green; total test count unchanged from slice 1.
3. fmt + clippy clean both feature modes.
4. `git diff --stat` shows ~6 deleted test files, 1 new file. Net line count drops (shared imports + harness boilerplate consolidated).

### Sub-slice 2b — `workspace_filter.rs`

#### Files touched

- **New: `crates/artel-fs/tests/workspace_filter.rs`** — combines:
  - `default_read_write_unchanged_behaviour.rs` (1 test)
  - `mixed_rules_first_match_wins.rs` (1 test)
  - `read_only_incoming_blocks_apply.rs` (1)
  - `read_only_outgoing_blocks_publish.rs` (1)
  - `read_only_outgoing_blocks_scan.rs` (1)
  - `read_only_post_join_live_blocks.rs` (1)
  - `read_only_post_join_live_delete_blocks.rs` (1)
  - `ticket_envelope_rejects_old_shape.rs` (1)
  - `ticket_envelope_round_trip.rs` (1)
  - `tombstone_filter_check.rs` (2)

  → 10 files → 1; 11 test fns.

- **Delete** all 10 source files.
- **Edit `tests/common/mod.rs` only if needed** — most of these tests already use `spawn_pair`. No new helpers anticipated.

**Test-fn naming:** prefix each consolidated test with its origin theme so the consolidated file's test names stay greppable. Examples: `read_only_outgoing_blocks_publish` (no rename), `tombstone_filter_applier_check_gates_hardcoded_skip` (was `applier_filter_check_gates_tombstone_for_hardcoded_skip` — minor reorder for grep ergonomics; not required, judgment call at consolidation time).

#### Definition of done

Same shape as 2a. 10 deleted files, 1 new file.

### Sub-slice 2c — `workspace_sync.rs`

#### Files touched

- **New: `crates/artel-fs/tests/workspace_sync.rs`** — combines:
  - `delete_propagates.rs` (1)
  - `empty_file_no_error.rs` (1)
  - `join_bulk_export.rs` (1)
  - `join_ticket_timeout.rs` (2)
  - `live_edit.rs` (1)
  - `round_trip.rs` (1, the wrapping `round_trip_3_in_a_row`)

  → 6 files → 1; 7 test fns.

- **Delete** all 6 source files.
- `round_trip.rs`'s test does three sequential round-trips today — keep as one test fn, rename to `round_trip_3_in_a_row` if not already (it is).

#### Definition of done

Same shape as 2a.

### Sub-slice 2d — `workspace_restart.rs` + first `*_n0` rename

This is the first sub-slice that pulls Tier C tests into a consolidated bin. Two test-fn renames happen here.

#### Files touched

- **New: `crates/artel-fs/tests/workspace_restart.rs`** — combines:
  - `disk_resume.rs` (1 test: `workspace_state_survives_graceful_restart`)
  - `host_restart_live_writes.rs` (1 test: `alice_post_restart_writes_reach_bob`)
  - `host_restart_live_writes_n0.rs` (1 test: `alice_post_restart_writes_reach_bob_real_n0` — already `_n0`-suffixed; keep `#[ignore]` for now, slice 4 removes it)
  - `host_restart_ticket_stable.rs` (1 test: `re_hosting_same_dir_yields_structurally_identical_ticket`)
  - `host_resume_session_id.rs` (1 test: `re_hosting_recovers_session_id_and_resumes_message_flow`)

  → 5 files → 1; 5 test fns.

- **Delete** all 5 source files.

#### Test-fn renaming

- `alice_post_restart_writes_reach_bob_real_n0` already matches `*_n0$`. No rename needed.
- All other test fns are Tier B (`DnsPkarrServer`-backed or local-only). They must NOT match `*_n0` so they run on the default profile.

The single-file `host_restart_live_writes_n0.rs` keeps its `#[ignore]` attribute through this slice; slice 4 removes it.

#### Definition of done

1. `cargo nextest run --workspace -E 'binary(workspace_restart)'` runs 4 tests green (the `_n0` test stays filtered by default profile + `#[ignore]`).
2. `cargo nextest run --workspace --profile n0 -- --run-ignored=all` includes the `_n0` test (still serial). Optional: run-until-fail loop per `docs/diagnosing-flaky-tests.md`.
3. fmt + clippy clean.

### Sub-slice 2e — `iroh_internals.rs` + `iroh_docs_smoke` rename

#### Files touched

- **New: `crates/artel-fs/tests/iroh_internals.rs`** — combines:
  - `iroh_docs_smoke.rs` (1 test, **rename to `*_n0`** as flagged in slice 1's DoD #3)
  - `iroh_docs_smoke_pkarr.rs` (1 test)
  - `relay_unreachable.rs` (1 test)

  → 3 files → 1; 3 test fns.

- **Delete** all 3 source files.

#### Test-fn renaming

- `iroh_docs_smoke.rs::doc_ticket_round_trips_without_manual_address_seeding` → `doc_ticket_round_trips_without_manual_address_seeding_n0`. Single string-literal change. **This rename was ideally done in slice 1 (per DoD #3); if it wasn't, do it here.**
- `iroh_docs_smoke_pkarr.rs::doc_ticket_round_trips_via_localhost_pkarr_dns` — Tier B. No rename.
- `relay_unreachable.rs::host_with_unreachable_relay_returns_typed_error` — Tier B (`TestingUnreachableRelay`). No rename.

#### Definition of done

Same shape as 2d.

### Sub-slice 2f — `drop_bomb.rs` (no consolidation)

#### Files touched

**None.** Listed for explicit confirmation. The bin stays at `tests/drop_bomb.rs` with `tests/bin/drop_bomb_child.rs` as its child binary. `Cargo.toml` `[[bin]]` entry for `drop_bomb_child` stays unchanged.

#### Definition of done

`grep -l drop_bomb crates/artel-fs/tests/` shows exactly `tests/drop_bomb.rs` + `tests/bin/drop_bomb_child.rs`. No rename.

### Sub-slice 2g — `crash_recovery.rs` (no consolidation)

#### Files touched

**None.** Same as 2f. The bin stays at `tests/crash_recovery.rs` with `tests/bin/crash_child.rs` as its child binary.

#### Definition of done

Same shape as 2f.

---

## Slice 3 — `artel-daemon` consolidation

~17 test files → 4 bins.

### Sub-slice ordering

1. **3a — `sessions.rs`** (4 files → 1; ~12 tests). Pure Tier A; cleanest start.
2. **3b — `gossip.rs`** (8 files → 1; ~8 tests). The biggest daemon-side consolidation.
3. **3c — `attachments.rs`** (no consolidation — already a single file). Listed for confirmation.
4. **3d — `identity.rs`** (4 files → 1; ~6–8 tests; mixed-tier with `_n0` renames).

`identity.rs` last because it's the trickiest (mixed-tier, requires per-test-fn `_n0` decisions on the existing `iroh_identity.rs` content).

### Sub-slice 3a — `sessions.rs`

#### Files touched

- **New: `crates/artel-daemon/tests/sessions.rs`** — combines:
  - `auto_spawn.rs` (~7 tests: `happy_path_cold_dir_spawns_daemon`, `second_call_reuses_existing_daemon`, `stale_pid_file_is_recovered`, `stale_socket_file_is_recovered`, `parallel_calls_settle_on_one_daemon`, etc.)
  - `end_to_end.rs` (2 tests: `two_clients_chat_end_to_end`, `subscribe_replays_history`)
  - `host_resume.rs` (3 tests)
  - `persistence.rs` (2 tests)

  → 4 files → 1; ~14 test fns.

- **Delete** all 4 source files.

All Tier A. No `_n0` suffixes.

### Sub-slice 3b — `gossip.rs`

#### Files touched

- **New: `crates/artel-daemon/tests/gossip.rs`** — combines:
  - `iroh_gossip_smoke.rs` (1 test)
  - `iroh_gossip_fanout.rs` (1)
  - `iroh_join_announcement.rs` (1)
  - `iroh_joiner_send_fanout.rs` (1)
  - `iroh_joiner_send_rejected.rs` (1)
  - `iroh_session_closed.rs` (1)
  - `iroh_subscribe_replay.rs` (1)
  - `iroh_remote_mirror_persists_log.rs` (1)

  → 8 files → 1; 8 test fns.

- **Delete** all 8 source files.

All Tier B (`DnsPkarrServer`). No `_n0` suffixes.

### Sub-slice 3c — `attachments.rs` (no consolidation)

`crates/artel-daemon/tests/attachments.rs` already exists with 6 tests. No rename, no merge. Listed for explicit confirmation.

### Sub-slice 3d — `identity.rs` + per-test-fn `_n0` decisions

#### Files touched

- **New: `crates/artel-daemon/tests/identity.rs`** — combines:
  - `iroh_identity.rs` (4 tests, mixed-tier — see decisions below)
  - `peer_addr_cache_pkarr.rs` (1 test)
  - `iroh_join_addr_hint_n0.rs` (1 test, `_n0`-suffixed already, `#[ignore]`'d — slice 4 removes the ignore)
  - `relay_unreachable.rs` (1 test)

  → 4 files → 1; 7 test fns.

- **Delete** all 4 source files.

#### Per-test-fn tier classification within `iroh_identity.rs`

Today's `iroh_identity.rs` has four tests. Reading the file:

- `endpoint_id_is_stable_across_daemon_restarts` — uses `iroh_key_path: Some(...)` + `EndpointSetup::Production`. Binds real n0. **Rename to `endpoint_id_is_stable_across_daemon_restarts_n0`.**
- `host_ticket_carries_a_real_endpoint_addr` — same. **Rename to `host_ticket_carries_a_real_endpoint_addr_n0`.**
- `iroh_key_file_is_chmod_0600` — file-only check, doesn't bring an iroh endpoint up at all. Tier A. **No rename.**
- `no_iroh_key_path_keeps_synthetic_peer_id` — `iroh_key_path: None`. No iroh endpoint. Tier A. **No rename.**

Two test fns get the `_n0` suffix; the consolidated bin has 5 Tier-A/B tests + 2 Tier-C tests.

**Risk surfaced here:** the n0-binding identity tests have NO `#[ignore]` attribute today, so they currently run on every CI invocation. After the `_n0` rename, the default nextest profile filters them out — they STOP running on every CI invocation (a CI-side behavioural shift). This is the same shift `iroh_docs_smoke.rs` undergoes in 2e/slice 1. Both shifts need the slice 5 contributor-doc to call them out: "Tier C tests run only via `--profile n0` or `--profile ci`; default nextest skips them."

#### Definition of done

Same shape as 2d. Plus: `cargo nextest run --workspace --profile n0` includes the two newly-renamed `_n0` identity tests AND the now-`*_n0` `iroh_join_addr_hint_n0` test (`#[ignore]` still set; slice 4 removes).

---

## Slice 4 — drop `#[ignore]` attributes

**Goal:** Remove every `#[ignore]` attribute from Tier C tests. CI starts running tier C as part of the `n0` profile.

Brainstorm: "Drop existing `#[ignore]`s as part of the migration. The ignores were a workaround for the previous test-mixing shape; proper tiering + serial-within-tier-C removes the underlying reason. Real flakes get test-by-test ignores with writeups per `docs/diagnosing-flaky-tests.md` § 'What NOT to do' — never tier-wide ignore."

### Files touched

- `crates/artel-fs/tests/workspace_restart.rs` — remove `#[ignore = "real-n0; ..."]` on `alice_post_restart_writes_reach_bob_real_n0`.
- `crates/artel-daemon/tests/identity.rs` — remove `#[ignore = "real-n0; ..."]` on `join_succeeds_within_tight_budget_real_n0`.

That's the entire current `#[ignore]` surface (verified by `grep -l "#\[ignore" crates/artel-fs/tests/*.rs crates/artel-daemon/tests/*.rs`).

### Definition of done

1. `grep -l '#\[ignore' crates/artel-fs/tests/*.rs crates/artel-daemon/tests/*.rs` returns no matches (or only matches in test fns that still need `#[ignore]` for non-tier-C reasons — none expected).
2. `cargo nextest run --workspace --profile n0` runs all `_n0`-suffixed tests, including the two previously-ignored ones, **in serial. Once. Both pass.** That's the gate.

   The original draft of this slice required a 20-iteration run-until-fail loop here. Removed: it would burn 40–60min of local wall-clock for diagnostic data CI's per-push n0 step will accumulate over the first week post-slice-5 cheaper. The `peer_addr_cache_pkarr` deterministic Tier B already covers finding #5c (the bug `host_restart_live_writes_n0` was originally pinning); the n0 sibling is a production canary, not the substrate test. If a single-iteration run flakes here, then yes — diagnose per `docs/diagnosing-flaky-tests.md`, and per [[feedback-no-handwaving-flaky-tests]] do NOT re-add `#[ignore]` as a "fix".
3. `cargo nextest run --workspace` (default profile) still runs green; default profile filter still excludes `_n0` tests.
4. fmt + clippy clean.

### Risk: a previously-ignored test fails when un-ignored

Possible — single-iteration. Brainstorm framing: "We don't yet have data showing tier C is too flaky for CI — the migration plugs CI into the existing diagnostic infrastructure." If the single-iteration run fails, the slice's outcome is "run the diagnostic recipe, diagnose the underlying bug, fix it, then drop the ignore." If diagnosis is non-trivial, slice 4 *can* land with a per-test `#[ignore = "<finding number> — diagnosis pending"]` annotation, with a corresponding entry added to `docs/handoff-code-review-fixes.md` (open findings list). This is the "real flakes get test-by-test ignores with writeups" exception the brainstorm carves out.

If post-slice-5 CI shows the n0 step flaking >5% over a week, that's the data-gathering signal. Diagnose per recipe, fix the underlying bug. Don't gate-out tier C — the migration's whole framing is "stop using `#[ignore]` as the answer."

---

## Slice 5 — wire CI / Make / contributor docs

**Goal:** all developer + CI entrypoints use nextest. `cargo test --workspace` still works as a fallback (untested in CI; we just claim it works).

### Files touched

- **New: `Makefile`** at repo root.
- **Edit: `.github/workflows/ci.yml`** — replace the two `cargo test --workspace --all-targets` lines with nextest invocations. Add a tier-C step. Keep `cargo test --workspace --doc --all-features`.
- **Edit: `README.md`** § "Status" or new "Development" section — install instruction for nextest + the test commands.
- **No new pre-commit hook.** Out of scope for this migration; flagged for future work.

### `Makefile`

```makefile
# artel — top-level developer commands.

.PHONY: test test-n0 test-fallback fmt clippy ci-local

# Default test target: Tier A + B (no real n0). Fast.
# Equivalent to `cargo nextest run --workspace`.
test:
	cargo nextest run --workspace
	cargo test --workspace --doc --all-features

# Real-n0 tests only. Slow, serial. Used for the run-until-fail
# loop documented in docs/diagnosing-flaky-tests.md, and as the
# CI's tier-C step.
test-n0:
	cargo nextest run --workspace --profile n0

# Fallback target: cargo test instead of nextest. Slower (no
# inter-binary parallelism), but works without nextest installed.
# Doctests run via cargo test in either runner.
test-fallback:
	cargo test --workspace --all-targets
	cargo test --workspace --all-targets --all-features
	cargo test --workspace --doc --all-features

fmt:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings
	cargo clippy --workspace --all-targets --all-features -- -D warnings

# What CI runs locally — full pyramid.
ci-local: fmt clippy test test-n0
```

### `.github/workflows/ci.yml`

Replace the `test` job's body with:

```yaml
  test:
    name: test (${{ matrix.os }} / ${{ matrix.rust }})
    runs-on: ${{ matrix.os }}
    strategy:
      fail-fast: false
      matrix:
        os: [ubuntu-latest, macos-latest]
        rust: [stable]
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@master
        with:
          toolchain: ${{ matrix.rust }}
      - uses: Swatinem/rust-cache@v2
      - uses: taiki-e/install-action@v2
        with:
          tool: nextest
      - run: cargo build --workspace --all-targets --all-features
      - run: cargo nextest run --workspace --profile ci
      - run: cargo nextest run --workspace --profile ci --all-features
      - run: cargo nextest run --workspace --profile n0
      - run: cargo test --workspace --doc --all-features
```

**Notes:**

- `taiki-e/install-action` is the standard nextest installer for GitHub Actions (faster than `cargo install`).
- The `n0` step runs after the `ci` steps. fail-fast is `false` at the matrix level, but each step's exit code is honoured — a failing `ci` step terminates that matrix-cell's job, the `n0` step doesn't run for that cell. That's the brainstorm's "fail-fast across boundaries" intent.
- `cargo test --workspace --doc --all-features` stays — nextest doesn't run doctests.
- The two `--all-features` and non-`--all-features` ci runs preserve the matrix's "with and without `iroh` feature" coverage from the current CI.

### `cargo test` fallback in CI: tested or untested?

**Decision: untested in CI; documented as "should work."** Rationale:

- Running `cargo test --workspace --all-targets` in CI duplicates the test cost (already paid once via nextest); the migration's whole point is to spend less wall-clock on tests.
- Running it as `cargo test --workspace --no-run` (compile-only) costs less but only proves it builds — doesn't catch test-discovery bugs.
- Pre-1.0 alpha; first-run experience for someone without nextest is "install nextest" or "run `make test-fallback` once and live with the slow runtime." That's an acceptable contract.

If a future contributor reports `cargo test` doesn't work, that's a one-off bug to fix; not a regression we need CI to guard against.

### Contributor-doc text (for README.md or new CONTRIBUTING.md)

```markdown
## Development

### Tests

`artel` uses [`cargo-nextest`](https://nexte.st) for the integration
test pyramid:

- **Tier A + B** (unit + cross-peer over a localhost
  `DnsPkarrServer`): `make test` or `cargo nextest run --workspace`.
  Fast, deterministic, runs on every PR.
- **Tier C** (real n0 — pkarr.iroh.computer + production relay):
  `make test-n0` or `cargo nextest run --workspace --profile n0`.
  Slower, serial within the tier (so a failing iteration's tracing
  log is a single coherent timeline). Test fn names suffixed `_n0`.

Install nextest with:

```
cargo install cargo-nextest --locked
```

If you don't want to install nextest, `make test-fallback` runs
`cargo test --workspace --all-targets` instead. Slower; no
inter-binary parallelism.

Doctests run under `cargo test` in either runner (nextest doesn't
support doctests).

For diagnosing flaky tests, see
[`docs/diagnosing-flaky-tests.md`](docs/diagnosing-flaky-tests.md).
```

### Definition of done

1. `Makefile` exists with the targets above; `make test` and `make test-n0` work locally.
2. CI's `test` job uses nextest; both `cargo nextest run --workspace --profile ci` and `cargo nextest run --workspace --profile n0` run as separate steps. CI green on a clean push.
3. README.md (or CONTRIBUTING.md) documents nextest install + test commands.
4. `cargo test --workspace --all-targets` still runs green locally (manual verification, not CI).
5. fmt + clippy clean.

---

## Slice 6 — roadmap → DONE; update handoff pointer

**Goal:** close the loop. Roadmap entry replaced with a "DONE" pointer; handoff-code-review-fixes "Open" → "Start here" cross-ref text updated to reflect that test-infra has landed.

### Files touched

- `docs/roadmap.md` — replace the "Future" → "Faster `cargo test --workspace`" bullet with a DONE marker pointing at this plan + the slice-2-through-5 commits.
- `docs/handoff-code-review-fixes.md` — update the cross-reference text in two places (the "Status as of 2026-05-29" → "Open (work from here)" subsection, and the finding #8 writeup itself) to say "test-infra has landed at `<commit-hash>`" instead of "pairs naturally with the test-tiers/nextest work in `docs/roadmap.md` ... (commit `d9d9e0e`)". The "Start here" pointer at finding #8 stays — that's the next handoff item.
- This plan doc itself: append a one-line "Landed" status at the top with the final consolidating commit hash.

### Specific text changes in `docs/handoff-code-review-fixes.md`

Two locations:

1. The "Open (work from here)" intro (~ line 128):
   > **Start here:** the next finding is **#8** —
   > `Workspace::endpoint_id` accessor + cross-peer test gate. Tier 4.
   > Pairs naturally with the test-tiers/nextest work in
   > `docs/roadmap.md` § "Future" → "Faster `cargo test --workspace`"
   > (commit `d9d9e0e`); both touch `crates/artel-fs/tests/common/mod.rs`
   > and could share a single rewrite if landed close together.

   →

   > **Start here:** the next finding is **#8** —
   > `Workspace::endpoint_id` accessor + cross-peer test gate. Tier 4.
   > Test-infra (cargo-nextest + by-subsystem consolidation) landed at
   > `<commit-hash>` per `docs/plans/2026-05-29-faster-cargo-test-plan.md`,
   > so the `tests/common/mod.rs` shape is now stable; this finding's
   > rewrite of the cross-peer test gate lands on top.

2. The finding #8 writeup itself (~ line 137-148):
   Same update — strip the "pairs naturally with" paragraph; replace with a one-line note that test-infra has landed.

### `docs/roadmap.md` change

Replace the "Future" entry block (currently ~ lines 642-687, beginning with `- **Faster cargo test --workspace.**` and ending at `- **Symmetric P2P.**`) with:

```markdown
- **Faster cargo test --workspace.** DONE. cargo-nextest + by-subsystem
  consolidation per `docs/plans/2026-05-29-faster-cargo-test-plan.md`
  (commit `<final-hash>`). ~50 one-test-per-file integration bins
  collapsed to ~11 by-subsystem files; tiered pyramid (Tier A unit +
  hermetic Tier B `DnsPkarrServer`/`TestingUnreachableRelay` + serial
  Tier C real-n0) wired through `.config/nextest.toml` + `Makefile` +
  CI. `cargo test --workspace` still works as a fallback. n0-touching
  tests are now suffixed `*_n0` and run under `--profile n0`.
```

### Definition of done

1. `docs/roadmap.md` § "Future" → "Faster `cargo test --workspace`" replaced with the DONE marker.
2. `docs/handoff-code-review-fixes.md` § "Open" → "Start here" cross-ref updated; finding #8 writeup's cross-ref updated.
3. This plan doc appends a "Landed" line at the top.
4. fmt + clippy still clean.
5. CI green.

---

## Cross-cutting concerns

### Two-impls-or-none

No new traits. nextest config is data, not abstraction. Test consolidation merges files; doesn't introduce abstractions. `Makefile` targets are concrete commands. Honoured.

### Persistence-first constraint

N/A — this migration touches tests + tooling only, no production code, no persistence surface.

### Postcard wire enums must be externally-tagged

N/A — no wire shapes touched.

### Headless / Unix-only

`Makefile` is Unix-only (artel is Unix-only per [[project-unix-only-for-now]]). No regression.

### Extensive unit tests

Per [[feedback-extensive-unit-tests]]: this migration adds zero new tests. The brainstorm explicitly frames the work as "consolidation gets the link-cost win"; the existing test suite is what validates the migration. Each sub-slice's DoD includes "`cargo nextest run --workspace` runs green" — the existing suite is the test.

The exception: if slice 1's calibration step surfaces a misclassified test (tier-A taking >10s, tier-B taking >60s), that becomes a re-classification finding with its own writeup; not a new test.

### No speculative abstractions

[[feedback-no-speculative-abstractions]] rule 2 (layer boundaries count): the artel-fs vs artel-daemon test split *is* a real layer boundary (substrate vs consumer). The migration preserves it — `artel-fs/tests/` and `artel-daemon/tests/` stay as separate test trees with their own `common/mod.rs`. No proposal to merge them.

### Don't handwave away flaky tests

[[feedback-no-handwaving-flaky-tests]]: slice 1's calibration step + CI's standing per-push n0 step (post-slice-5) are the load-bearing diagnostic steps. If anything fails non-deterministically during the migration, apply the recipe in `docs/diagnosing-flaky-tests.md` BEFORE labelling. Specifically: do NOT bump timeouts as a "fix" (slice 1 calibrates from real data, not by guessing high); do NOT re-add `#[ignore]` to a previously-passing test without a writeup of the underlying bug.

### Redirect long test output

[[feedback-redirect-test-output]]: every cargo / nextest invocation in this plan that's expected to produce >100 lines of output redirects to `/tmp/X.log` and greps. Slice 1's calibration step explicitly does this. CI captures via standard log artifacts.

### Handoff doc uncommitted

[[feedback-handoff-doc-uncommitted]]: this plan does NOT modify `docs/handoff-post-workspace-registry.md` (which is uncommitted) or `docs/handoff-code-review-fixes.md`'s working-tree state. Slice 6 commits the cross-ref text update to the latter; that's the slice's artifact.

---

## Risks and unknowns

1. **Nextest CLI / config syntax drift.** This plan picks specific syntax for `default-filter`, `test-groups`, `--run-ignored=all`. nextest 0.9.x is the latest stable; syntax has been stable since ~0.9.50. Slice 1's DoD includes "validate against `cargo nextest --help-config`" — if anything's wrong, fix in slice 1 before propagating. Low-risk.

2. **`iroh_docs_smoke.rs` rename CI behavioural shift.** Currently runs on every CI invocation; after the rename it doesn't. Slice 5's contributor-doc note documents this. Low-risk because the pkarr sibling `iroh_docs_smoke_pkarr.rs` is already the deterministic on-every-PR canary per `docs/diagnosing-flaky-tests.md` § 5.

3. **`iroh_identity.rs` n0-binding tests stop running on every CI.** Same shape as risk 2. Two test fns (the `EndpointId` stable + ticket carries addr) move from tier-A-by-default-because-no-ignore to tier-C-by-rename. CI's `--profile n0` step covers them; the default profile doesn't. Low-risk.

4. **Slice 4's `#[ignore]` removal exposes a real n0 flake.** Possible. Mitigation: slice 4 runs the un-`#[ignore]`'d tests once locally before merging; if the single iteration fails, diagnose per recipe before merging. CI's per-push n0 step (slice 5) is the standing diagnostic — over the first week post-merge it'll surface any latent flake far more cheaply than burning local wall-clock on a pre-merge multi-iteration loop. If diagnosis is hard, slice 4 lands with a per-test `#[ignore = "<finding>"]` and a follow-on entry in `docs/handoff-code-review-fixes.md`.

5. **`workspace_attachment.rs` consolidation decision.** Plan recommends folding into `workspace_lifecycle.rs` (option (a)). If the slice-2a implementor disagrees and prefers (b) standalone, fine — final bin count is 7 vs 8, both within "10–12" target. Document the choice in the slice-2a commit message.

6. **Drop-bomb cost in default profile.** `drop_bomb.rs` runs in default profile and pays real n0 cost (~5s × 2 tests). Acceptable temporarily. Once finding #9 lands (post-migration), drop_bomb moves fully Tier B. Track via the cross-ref in `docs/handoff-code-review-fixes.md` § "Open" → finding #9.

7. **CI cache hit rate.** `cargo nextest run` shares the same target/ as `cargo test`, so existing `Swatinem/rust-cache@v2` keys still work. No cache shape change. Low-risk.

8. **`cargo nextest run --profile n0` running real n0 in CI.** Brainstorm's stance: yes, on every PR. We don't yet have data showing it's too flaky to gate on. CI's per-push n0 step **is** the data-gathering step (post-slice-5). If post-merge CI shows >5% n0-step failure rate over a week, the *follow-up* is to diagnose (per `docs/diagnosing-flaky-tests.md`) and fix the underlying bug — NOT to gate-out tier C. The migration's whole framing is "stop using `#[ignore]` as the answer."

---

## Critical files for implementation

- `.config/nextest.toml` (new in slice 1)
- `Makefile` (new in slice 5)
- `.github/workflows/ci.yml` (edit in slice 5)
- `README.md` or `CONTRIBUTING.md` (edit in slice 5)
- `crates/artel-fs/tests/*.rs` (consolidated in slice 2; new bins listed in each sub-slice)
- `crates/artel-daemon/tests/*.rs` (consolidated in slice 3)
- `crates/artel-fs/tests/common/mod.rs` (read-only in this migration; rewrite is finding #8 territory, post-migration)
- `crates/artel-daemon/tests/common/mod.rs` (same)
- `docs/roadmap.md` (edit in slice 6)
- `docs/handoff-code-review-fixes.md` (edit in slice 6)
