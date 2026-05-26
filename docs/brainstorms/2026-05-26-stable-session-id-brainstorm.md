---
date: 2026-05-26
topic: stable-session-id
---

# Stable session id across host restarts

## What we're building

`Request::HostSession` gains an optional `session: Option<SessionId>`
field. `None` preserves today's behaviour (mint a fresh random id);
`Some(id)` resumes that session if a matching local-host record
exists, or creates a new one with the supplied id if not.

`artel-fs::Workspace::host_with` derives a deterministic session id
from the workspace's `NamespaceId` and passes it on every host call.
A re-hosted workspace therefore always lands on the same session id,
which keeps an existing joiner's mirror reachable across host
restarts (the gossip topic is `session_id[..16]`, so stable session
id → stable topic for free).

This is item 1 in `docs/roadmap.md` § "Multi-session resume across
daemon restarts".

## Why this approach

Two shapes were on the table:

1. **Workspace derives the id from `NamespaceId`** (chosen).
2. **Explicit `Request::ResumeHost` verb** + persisted
   `.artel-fs/session-id` file.

Option 2 was rejected as speculative generality. It buys flexibility
for a hypothetical second consumer that doesn't have a stable
`NamespaceId`-equivalent, at the cost of: an extra round-trip on
first host, a load-bearing on-disk file (lose it and resume breaks
even though `NamespaceId` is intact), and a wider RPC surface. Per
`feedback_no_speculative_abstractions`, we ship the simpler shape
now; if a non-fs consumer ever needs explicit resume, `ResumeHost`
can be added additively without conflict.

Option 1 keeps the `NamespaceId` as the single source of truth: the
session id is *recoverable* by re-deriving, and the daemon never
imports `iroh-docs` or learns what a `NamespaceId` is. Coupling is
contained to artel-fs.

## Key decisions

- **Derivation lives in artel-fs only.** Daemon receives an opaque
  `SessionId`. Protocol crate stays iroh-free. `artel-fs` owns the
  `session_id_for(NamespaceId) -> SessionId` function.

- **Domain-separated, versioned hash.**
  `blake3::keyed_hash(b"artel-fs/session-id/v1", ns.as_bytes())[..16]`
  with UUID v8 variant bits set. The `v1` tag is the upgrade path;
  changing the derivation in the future is a new tag (and a
  breaking change for existing on-disk workspaces, which we accept
  the same way we accepted `NamespaceId` stability).

- **Always re-derive, never cache.** No `.artel-fs/session-id`
  file. The id is a pure function of `NamespaceId`, which is
  already persisted under `state_dir/doc-id`. Fewer things to go
  wrong; no drift between cache and source.

- **Conflict policy: reject loudly.** When `HostSession { session:
  Some(id), peer }` arrives and a record at `id` exists but
  `host != peer.id` or `kind == Remote`, the daemon returns a new
  `ProtocolError::SessionConflict` (or reuses `AlreadyJoined` /
  `NotHost` if either is a clean fit — TBD in planning). No silent
  overwrite, no `force` flag.

- **Resume preserves the full log.** When `id` matches and
  `host`+`kind` line up, the daemon reuses the record verbatim
  (same members, same head, same log), re-stamps the ticket with
  the current `daemon_addr`, and re-opens the gossip topic. This
  is the point of the persistence work that already landed (3b-1,
  3b-3).

- **Additive protocol change.** `Option<SessionId>` field, default
  `None`. Existing callers (CLI, tests) keep working without
  source changes once the field gets `#[serde(default)]`. Bump
  `PROTOCOL_VERSION` per the substrate's existing version-mismatch
  rules; older clients/daemons get the standard
  "restart required" error.

## Open questions

- **Error variant naming.** New `ProtocolError::SessionConflict` vs
  reusing `AlreadyJoined` / `NotHost`. Pick during planning; the
  on-wire shape is the only consequential decision.
- **Should the new field be a struct rather than an extra arg?**
  i.e. `HostSession { peer, options: HostOptions }` vs
  `HostSession { peer, session: Option<SessionId> }`. The roadmap
  hints there's more to come (workspace registry, list-hostable);
  if more knobs land, a struct is tidier. Defer until we see the
  second knob.
- **Workspace registry (#2 in the same roadmap section) and
  `Workspace::resume` ergonomics (#3) are separate slices.** This
  brainstorm only resolves #1.

## Next steps

→ `/workflows:plan` for implementation. The plan should cover:
- `artel-protocol`: `Option<SessionId>` field, version bump,
  new error variant if any, postcard round-trip + proptests.
- `artel-daemon`: `Registry::host` branches on the optional id,
  the new conflict / resume paths each get a unit test plus an
  e2e test through the daemon.
- `artel-fs`: `session_id_for` (with domain-tag round-trip
  proptest), `Workspace::host_with` plumbs it through, plus an
  e2e test that re-hosts the same dir and asserts the session
  id, gossip topic, and existing-joiner-still-reachable
  property.
- Per memory `feedback_extensive_unit_tests`: every crate
  change ships with tests.
