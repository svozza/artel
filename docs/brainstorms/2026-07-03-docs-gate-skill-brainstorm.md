---
date: 2026-07-03
topic: docs-gate-skill
---

# Docs-gate: replace the broken push hook with a docs-drift skill

## What We're Building

A project skill (`.claude/skills/` in this repo, committed) that audits whether
docs and in-flight code have diverged. It compares everything since
`merge-base(origin/main, HEAD)` — including uncommitted working-tree changes —
against three doc surfaces:

1. `docs/` guides (e.g. `consumer-guide.md`'s `WorkspaceEvent` enumeration,
   which went stale once already),
2. rustdoc `///` on public APIs the diff touches (content claims, not broken
   links — `make doc` already gates the mechanical class),
3. `CLAUDE.md` agent guidance (conventions/architecture notes).

It **reports** specific divergences (stale claim, missing coverage, unchanged
rustdoc on changed behavior) and offers fixes; the user decides what to apply.
Mirrors the `/code-review` interaction model.

The broken prompt-type `git push` hook in `.claude/settings.local.json` is
deleted. The existing `gh pr create` ask-hook stays and gains a line in its
`permissionDecisionReason`: reject unless the docs-check skill was run (or
docs were explicitly addressed) for this PR.

## Why This Approach

The hook-based gate failed structurally: prompt hooks are transcript-blind, so
the verdict hinged on the Bash `description` field — written by the model being
gated (observed self-attestation bypass), and non-deterministic besides. A
skill dissolves the flaw rather than patching it: it runs in the main
conversation with full tool access, so it can inspect the real diff and read
the real docs — evidence, not attestation. It also decouples the check from
push timing: WIP pushes flow freely (the "too noisy" objection to `ask`
escalation), and the check runs when work is declared done.

Alternatives considered and dropped:
- **Mechanical command hook** (diff touches `crates/*/src/` without `docs/**`):
  deterministic but crude — can't judge *content* staleness, which is the whole
  remit. The mechanical class is already covered by `.githooks/pre-push`.
- **Command hook parsing `transcript_path`**: "evidence of docs review" is too
  fuzzy to parse from a transcript, unlike the PR-text case.
- **Pure ask-escalation on push**: too noisy for a frequent pusher.

## Key Decisions

- **Skill, not hook**: the checker needs tools (git diff, file reads) and
  judgement; hooks offer neither. Human invokes it at meaningful moments.
- **Report-then-offer-fixes**: doc updates need user judgement on framing;
  auto-edits risk padding docs with noise.
- **Scope = docs/ + rustdoc-content + CLAUDE.md**: `docs/architecture.html`
  excluded for now (point-in-time artifact status unclear).
- **Diff base = merge-base with origin/main + working tree**: matches "what
  this PR will land", catches divergence pre-commit.
- **Project skill, committed**: the conventions it encodes are artel-specific
  and should version with the repo.
- **Hook layer**: broken `git push` prompt hook deleted; PR-create ask dialog
  reason extended to remind the human to reject if the docs check wasn't run.
  Human remains the gate at the natural "work is done" moment.

## Open Questions

- Exact skill name (`docs-drift`? `docs-check`?) and whether it takes an
  optional base-ref argument later (deferred — YAGNI for now).
- Whether the PR-create reason line should mention the skill by name so the
  habit is discoverable in the dialog itself.

## Next Steps

→ plan/implement: write the skill (SKILL.md), delete the prompt hook, extend
the PR-create hook reason, pipe-test the edited hook JSON before wiring in.
