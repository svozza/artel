---
date: 2026-05-26
topic: host-restart-ticket-stable
---

# Host-restart ticket stability test

## What we're building

A dedicated integration test, `tests/host_restart_ticket_stable.rs`,
that pins the resume-ticket-stability property: re-hosting the same
workspace dir produces the same `NamespaceId` and the same host
`NodeId(s)`, so existing joiners' tickets remain valid across host
restart.

This is the "concrete first deliverable" called out in
`docs/roadmap.md` § "Multi-session resume across daemon restarts"
(lines 446–505) — pinned without committing to a registry or a
session-id design.

## Why this approach

`disk_resume.rs` already asserts the same structural-identity
properties (lines 225–235) inside a much broader two-daemon
end-to-end scenario: Alice hosts, Bob joins, both shut down, daemons
restart, Alice re-hosts, Bob rejoins, live sync continues, deletes
propagate. Total runtime ~15s.

A dedicated test scoped to just the host side — no joiner, no daemon
swap, no live sync — runs in ~2s and gives an unambiguous failure
mode. If the new test fails, the structural-identity property
broke. If `disk_resume.rs` fails, it could be any of a dozen
things. Faster feedback during development; sharper regression
surface.

We deliberately match `disk_resume.rs`'s decision to **NOT** assert
byte-identity of the whole ticket. That comment (line 222) explicitly
notes "address-discovery info inside a ticket can drift legitimately
(e.g. relay URL list ordering)". The roadmap's "byte-identical"
wording predates that nuance.

## Key decisions

- **Match `disk_resume.rs`'s assertion shape**: `NamespaceId` stable
  + host `NodeId(s)` stable. NOT byte-identity of the whole ticket.
  Rationale: address-discovery info can drift legitimately.

- **Single iroh-disabled daemon** (like `host_publishes_ticket.rs`,
  not the `Pair` fixture). The property is about per-workspace iroh
  state (`iroh.key`, `doc-id`), not the daemon's iroh layer. No
  joiner needed. Fastest fixture available.

- **One resume cycle**. host → drop → re-host. Mirrors the roadmap's
  deliverable spec exactly. If multi-cycle drift becomes a concern
  later, extend to N cycles then.

- **Optional**: assert `PathRules` round-trip across restart too
  (sub-slice 2 already proves rules ride the wire; this is just
  defence-in-depth for the resume case). Default-permissive on both
  sides, deep-equal.

- **No new daemon protocol surface.** This test is purely a
  consumer of `Workspace::host_with`'s existing resume behaviour
  (3b-1). It does not depend on session-id stability or a workspace
  registry — those are separate slices.

## Test shape

```text
1. Spawn iroh-disabled daemon
2. HostSession on a fresh client; capture artel ticket (unused after this)
3. Subscribe; drain until workspace.ticket lands; decode envelope; capture phase 1
4. Drop the workspace (host_with completes; shutdown)
5. Re-host the SAME dir (same state_dir, same workspace root, same daemon, same client)
6. Subscribe again; drain; decode envelope; capture phase 2
7. Assert: phase1.doc_ticket.capability.id() == phase2's
8. Assert: phase1.doc_ticket.nodes' NodeIds == phase2's
9. (Optional) Assert: phase1.rules == phase2.rules  (default-permissive on both)
```

~80 lines including imports + setup. No `Pair`. No `MemoryLookup`.
No second client.

## Open questions

- Whether to keep the structural-identity assertions inside
  `disk_resume.rs` as well (duplication) or prune them out (single
  source of truth). Defer until the new test lands and the question
  is concrete.

## Next steps

→ Implement directly (one ~80-line test file, no new modules,
no API changes). Plan-doc would be longer than the test.
