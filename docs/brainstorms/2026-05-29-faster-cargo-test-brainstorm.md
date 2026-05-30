---
date: 2026-05-29
topic: faster-cargo-test
---

# Faster `cargo test --workspace` — test-infra migration

## What we're building

Migrate the artel test suite from `cargo test` to `cargo nextest` with
a tiered test pyramid and consolidate the ~50 one-test-per-file
integration binaries down to ~10–12 by-subsystem binaries. Every
`tests/*.rs` becomes its own crate with full link cost today; the
suite is dominated by per-binary serial cost rather than test
runtime. Both proposals already exist in `docs/roadmap.md` § "Future"
→ "Faster `cargo test --workspace`"; this brainstorm validates the
unspecified design choices and produces a planning input.

## Why this approach

- **Consolidation alone gets the biggest win.** Each `tests/*.rs` is
  a separate binary, cargo links each with its full dep graph (iroh,
  tokio, etc.). One-test-per-file is a JS/Python pattern bleeding
  into Rust; the Rust ecosystem norm (tokio, hyper, serde_json,
  reqwest, sqlx) is few large by-subsystem files. Aligning with
  ecosystem practice removes the cost we're paying for the wrong
  shape.
- **Nextest gives parallelism across binaries** — closest analog to
  `vitest --pool=forks` running test files concurrently. `cargo test`
  runs binaries sequentially even though tests within a binary are
  multi-threaded. Drop-in switch; 3–5× wall-clock speedup on
  integration-heavy suites.
- **Tiered pyramid lets fast tests fail fast** without the slow
  cross-peer or real-n0 tests holding feedback hostage. Tier A → B →
  C with fail-fast across boundaries.
- **Drop existing `#[ignore]`s** as part of the migration. The
  ignores were a workaround for the previous test-mixing shape;
  proper tiering + serial-within-tier-C removes the underlying
  reason. Real flakes get test-by-test ignores with writeups per
  `docs/diagnosing-flaky-tests.md` § "What NOT to do" — never
  tier-wide ignore.

## Key decisions

- **Sequencing: test-infra first, then finding #8** ([handoff
  doc](../handoff-code-review-fixes.md)).
  *Why:* both rewrite cross-peer test surface (`tests/common/mod.rs`).
  Doing #8 first means writing helpers for the old per-file layout,
  then re-touching them during consolidation. Doing test-infra
  first means #8's `wait_for_workspace` lands once into the
  consolidated shape.
  *How to apply:* this plan owns the `tests/common/mod.rs` rewrite;
  #8's plan picks up the result.

- **Tier A/B boundary = "is iroh `Endpoint` bound?"** Not "uses
  pkarr or not."
  *Why:* `relay_unreachable.rs`, `iroh_identity.rs`,
  `drop_bomb.rs` (post-#9) all bind iroh endpoints without using
  `DnsPkarrServer`. Endpoint-binding is what costs ~50–200ms per
  test; that's the honest cost-vs-feedback boundary.
  *How to apply:* Tier A = no iroh `Endpoint`. Tier B = iroh runs
  but no traffic leaves localhost (`DnsPkarrServer`,
  `TestingUnreachableRelay`, `relay_unreachable`, `iroh_identity`).
  Tier C = real n0 (every `*_n0.rs` test name).

- **Default `cargo nextest run` runs A→B with fail-fast; tier C
  runs on every PR via a CI profile.** No tier-wide `--ignored`
  gate.
  *Why:* matches `cargo test` ergonomics today (one command does
  everything), no untested-on-push surprises. The previous
  `#[ignore]` wall on tier C was a workaround for test interleaving
  killing diagnostic legibility; serial-within-tier-C removes that
  reason. We don't yet have data showing tier C is too flaky for CI
  — the migration plugs CI into the existing diagnostic
  infrastructure (per-phase markers, run-until-fail recipe) and
  lets us learn from real signal.
  *How to apply:* nextest profiles — `default` runs A→B (matches
  `cargo nextest run`), `ci` runs A→B→C with serial tier C, `n0`
  runs only tier C for the manual run-until-fail loop.

- **Bin-per-subsystem layout, ~10–12 binaries total.** Files
  organised by what's tested, mirroring `src/`.
  *Why:* matches Rust ecosystem practice (tokio/hyper/serde_json
  pattern), predictable file location from "what am I testing,"
  smallest binary count. Alpha project so churn cost is acceptable.
  *How to apply:* concrete grouping (subject to refinement during
  planning):
  - `crates/artel-fs/tests/`: `workspace_lifecycle.rs`,
    `workspace_filter.rs`, `sync.rs`, `iroh_internals.rs`,
    `drop_bomb.rs` (special: spawns child binary, keep separate)
  - `crates/artel-daemon/tests/`: `sessions.rs`, `gossip.rs`,
    `identity.rs`, `attachments.rs`
  - Tier C tests live INSIDE the subsystem files with `*_n0`
    name suffix (nextest test-groups match by name pattern, not
    file). Drops the convention of separate `_n0.rs` files.

- **Defer `OnceCell<Pair>` fixture sharing.** Not in this
  migration.
  *Why:* consolidation alone gets the ~5× link-cost win.
  Fixture-sharing introduces order-dependent test bugs we
  specifically just hit (`force_shutdown_failure` static-atomic in
  the trio commit, fixed via per-instance state — see
  [[feedback-no-handwaving-flaky-tests]]). Optimisation, not
  prerequisite.
  *How to apply:* if specific binaries prove slow after
  consolidation lands, add `OnceCell<Pair>`-backed fixture sharing
  per-binary later, only for tests provably independent.

## Open questions (for the planning phase)

- **Exact subsystem grouping.** The bullets above are an opening
  offer. Plan should validate by walking each existing
  `tests/*.rs` and assigning, surfacing any test that doesn't fit
  cleanly.
- **Nextest config shape.** `tool.nextest.toml` vs
  `.config/nextest.toml`; how to express tier groups
  (`test-groups` + `slow-timeout` + `fail-fast`).
- **Make/CI wiring.** Today's `cargo test --workspace` is
  referenced from `make test`, pre-commit hooks, and CI. All three
  need updating, plus the install instruction for nextest in
  contributor docs (matters for contributor onboarding even in
  alpha).
- **`cargo test` fallback.** The roadmap says keep it working as a
  fallback so first-run experience isn't gated on installing
  nextest. Plan should confirm: do we keep it AS a tested fallback
  (CI runs both?) or just "it should work, untested"?
- **Test-name suffix convention enforcement.** If tier C tests
  must be named `*_n0` for the nextest group pattern to find them,
  is there a way to enforce this at lint/CI level (e.g. clippy
  custom lint, or a small build-script check)?
- **Drop-bomb handling.** `drop_bomb.rs` spawns a child binary
  (`tests/bin/drop_bomb_child.rs`); finding #9 wants the child to
  use `Testing` fixture instead of `Production`. If #9 lands
  during consolidation, the child becomes tier B; if before/after,
  tier assignment changes.
- **Per-tier timeout budget.** nextest supports `slow-timeout`
  per group. Tier A should be aggressive (~10s); tier B medium
  (~60s); tier C generous (~300s, since real-n0 needs propagation
  windows). Validate against actual current timings.

## Next steps

→ Plan agent. The plan should produce a step-by-step migration
that lands in this order:

1. Add nextest as a dev-dep + `nextest.toml` profile config
   (no test changes yet) — validates nextest works with current
   suite.
2. Subsystem-by-subsystem consolidation, one PR per subsystem
   so each is reviewable. Each PR: merge files, update
   `tests/common/mod.rs` if helpers change, run nextest to
   confirm green. Tier C tests get `*_n0` suffix during this
   step.
3. Drop `#[ignore]` attributes from tier C tests after all
   consolidation is done. CI starts running tier C.
4. Update `make test`, CI config, contributor docs.
5. Roadmap entry → "Future" → "Faster cargo test --workspace"
   gets a "DONE" pointer to this plan + commit hash.

After this lands, finding #8 (`Workspace::endpoint_id` accessor +
cross-peer test gate) becomes the next handoff item.
