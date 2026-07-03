---
name: docs-drift
description: Audit whether the docs and the code in flight on this branch have diverged — stale claims in docs/ guides, rustdoc on changed public APIs that no longer matches behavior, outdated CLAUDE.md guidance. Use before creating or marking a PR ready, when the user asks "are the docs still accurate?", "did anything go stale?", or wants a docs check on current changes — even if they don't say "drift". Reports divergences with evidence; applies fixes only on request.
---

# docs-drift — audit docs against in-flight code

Answer one question with evidence: **have the docs and the code currently in
flight diverged?** Report what you find; do not edit anything until the user
picks which findings to fix.

Why this exists: `make doc` and the pre-push hook already catch the mechanical
class (broken links, rustdoc `-D warnings`). What nothing catches is the
*content* class — a guide that enumerates enum variants that have since grown,
rustdoc that describes behavior a commit changed, a CLAUDE.md convention the
branch made obsolete. `docs/consumer-guide.md` went stale exactly this way
once (its `WorkspaceEvent` enumeration). That class needs judgement, which is
why this is a skill and not a hook.

## 1. Establish what's in flight

"In flight" = everything this branch will land, including uncommitted work:

```bash
git fetch origin main --quiet 2>/dev/null || true   # best-effort; offline is fine
BASE=$(git merge-base origin/main HEAD)
git diff --stat "$BASE"        # committed + working tree, one picture
git diff "$BASE" -- crates/    # the source changes to audit (read in full)
```

If the branch *is* main or the merge-base equals HEAD with a clean tree,
say there is nothing in flight and stop.

## 2. Inventory the change surface

From the diff, build a short list of what changed in kind, not just in path:

- **Public API surface**: added/removed/renamed `pub` items, changed
  signatures, new/removed enum variants, changed defaults. These are the
  items whose rustdoc and guide coverage must be re-checked.
- **Behavior**: changed semantics behind an unchanged signature (ordering,
  timing, error cases, event emission). Diffs of `///` lines in the branch
  are a signal the author already updated some docs — verify they match the
  final code, not an intermediate draft.
- **Conventions / workflow**: changes to Makefile targets, test tiers,
  directory layout, or process that CLAUDE.md or guides describe.

Test-only and private-refactor changes usually produce no findings — say so
plainly rather than inventing weak ones.

## 3. Audit each doc surface

Check three surfaces. For each, read the parts that intersect the change
surface and verify claims against the code as it now stands.

| Surface | What to check |
|---|---|
| `docs/` living guides — `consumer-guide.md`, `roadmap.md`, `diagnosing-flaky-tests.md`, `docs/roadmap/` | Enumerations (request verbs, event variants, options) still complete; described behavior still true; code snippets still compile against the new API shape. |
| Rustdoc on changed public items | For every public item the diff touches, read its `///` in the post-change code: does the prose still describe what the item does? Unchanged rustdoc on changed behavior is the classic miss. |
| `CLAUDE.md` (root, and any crate-level ones) | Commands, test tiers, file conventions, and architecture notes still accurate after this branch. |

**Out of scope** — point-in-time artifacts, never flag them as stale:
`docs/brainstorms/`, `docs/plans/`, `docs/handoff-*.md`,
`docs/architecture.html`, and `docs/adr/` (ADRs record decisions as made; if
the branch *contradicts* an accepted ADR, flag the contradiction as a finding,
but do not propose editing the ADR).

Grep is the tool here: for each changed public name or renamed concept, search
the in-scope docs for mentions and read each hit in context. A doc that never
mentions the changed area needs no finding.

## 4. Report

Lead with the verdict, then one entry per divergence:

```markdown
## Docs-drift: <clean | N divergences> (branch <name>, base <short-sha>)

### 1. <doc file> — <one-line summary>
- **Doc says** (docs/consumer-guide.md:41): "…quoted claim…"
- **Code now** (crates/artel-fs/src/events.rs:88): …what actually holds…
- **Fix**: …the minimal edit…

### Checked and current
- <doc surface>: <what was verified> ✓
```

Include the "checked and current" section — a clean verdict is only
trustworthy if it shows what was actually checked. Cite file:line on both
sides of every divergence; a finding without a quoted doc claim is a hunch,
not a finding.

Then stop and let the user choose what to fix. When asked to apply fixes,
make the minimal edit that restores accuracy — do not pad docs with coverage
nobody asked for, and do not reformat surrounding prose.
