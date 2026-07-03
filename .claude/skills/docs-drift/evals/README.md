# docs-drift evals

Regression suite for the `docs-drift` skill: three fixture scenarios with
planted (or deliberately absent) docs drift, run headlessly and graded
against assertions.

## Layout

- `evals.json` — the eval definitions: per eval a user-style prompt, the
  expected outcome, the fixture it runs against, and the assertions a run
  is graded on.
- `fixtures/*.patch` — the planted changes, as patches against `origin/main`
  (moving main by design: the skill's subject is drift against the current
  repo, so the evals track it).
- `setup-fixtures.sh` — rebuilds the three fixture worktrees under
  `/tmp/artel-eval/`. Fixtures 0–1 commit their change on an `eval/` branch;
  fixture 2 stays uncommitted on purpose (it proves working-tree-only
  changes count as "in flight").
- `run-evals.sh` — the whole loop: setup → `claude -p` headless run per
  fixture (report-only, no edits allowed) → `claude -p` judge call grading
  the report against the assertions → summary, non-zero exit on failure.
  Results land in `results/<timestamp>/` (gitignored).

## Running

```bash
.claude/skills/docs-drift/evals/run-evals.sh            # default model
.claude/skills/docs-drift/evals/run-evals.sh --model claude-haiku-4-5-20251001
```

Each run costs a handful of agent invocations (3 audits + 3 gradings), takes
a few minutes, and needs `claude` CLI auth. Not part of `make test` /
`make ci-local` — run it when the SKILL.md changes, or when checking a
smaller model can still drive the skill.

## The scenarios

| # | fixture | plants | proves |
|---|---------|--------|--------|
| 0 | `drift-variant` | new public `WorkspaceEvent::ScanCompleted` variant, consumer-guide enumeration not updated | detection of the classic stale-enumeration miss (with two-sided file:line evidence) |
| 1 | `clean-tests` | two new unit tests, nothing else | clean verdict without invented findings, with a "checked and current" account |
| 2 | `rustdoc-stale` | `MAX_FILE_SIZE` 1 MiB → 4 MiB, **uncommitted**; module rustdoc + roadmap.md still say 1 MiB | working tree counts as in-flight; stale-number detection in rustdoc and guides |

If a patch stops applying (a refactor touched its context), `git apply`
fails loudly in `setup-fixtures.sh` — regenerate the patch against the new
code (plant the same logical change) and commit the refreshed patch.

## History

First run (2026-07-03, iteration 1, Fable 5): 5/5 assertions on all three
evals — but the no-skill baseline also scored 5/5. The skill's measured
value on a frontier model is consistency of output shape, scope discipline
(no wandering into implementation review), and the encoded repo policy
(what counts as a living doc vs. point-in-time artifact) — not raw
detection. The step-by-step procedure in SKILL.md is kept deliberately for
smaller models, where the scaffolding is expected to matter more. See
`docs/brainstorms/2026-07-03-docs-gate-skill-brainstorm.md` for the design
rationale.
