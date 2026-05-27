---
date: 2026-05-27
topic: workspace-registry
---

# Workspace registry — daemon-side per-session attachments

Roadmap item 2 of `docs/roadmap.md` § "Multi-session resume across daemon restarts". Item 1 (stable session id across host restarts) landed as commits `f9d6c0a` … `b0dc2f5`. Item 3 (`Workspace::resume`) is already covered by `Workspace::host_with` once item 2 lands — no new constructor needed.

## What we're building

A daemon-level *per-session attachments* primitive: any client can stash an opaque payload, tagged with a string `kind`, against a `SessionId`. Other clients (and the same client after a daemon restart) can list and read those attachments back. The daemon never parses the payload.

`artel-fs` is the first consumer: `Workspace::host_with` / `join_with` register an `artel-fs/workspace/v1` attachment carrying `{ local_path, state_dir, role }` so a CLI / GUI / future tool can enumerate the workspaces a daemon knows about and reattach without re-deriving from filesystem state.

## Why this approach

Three approaches were considered:

- **Workspace-shaped fields on the daemon.** Daemon stores typed `local_path: PathBuf, role: enum, state_dir: PathBuf, …`. Rejected: violates ADR-001 § "Daemon scope: medium". Daemon would now know what a workspace is.
- **Per-session named attachments (key/value, multi-slot per session).** Daemon stores `(session_id, name) → Vec<u8>`. Strictly more general; rejected as scope-creep — single attachment per `(session, kind)` covers the actual use case.
- **Per-session opaque metadata, single attachment per `(session, kind)`. ← chosen.** Daemon stores `(session_id, kind: String, payload: Vec<u8>)`. `kind` is an app-chosen tag (e.g. `"artel-fs/workspace/v1"`); `payload` is opaque bytes. Daemon never inspects payload. Aligned with ADR-001's existing "opaque payload" pattern for `SessionMessage`.

The core principle: substrate (daemon + protocol) is foundational; `artel-fs` is a *consumer*, not a privileged citizen. Even though it's likely the most popular consumer, the daemon's vocabulary stays generic. If the single-slot constraint turns out wrong, ripping it out and adding a `name` slot is acceptable — alpha, breaking changes are fine.

## Key decisions

- **Wire shape — three new RPC verbs added to `Request`/`Response`:**
  ```rust
  Request::RegisterAttachment { session: SessionId, kind: String, payload: Vec<u8> }
  Request::ListAttachments    { kind: Option<String> }      // None = all kinds
  Request::ForgetAttachment   { session: SessionId, kind: String }
  Response::Attachments       { entries: Vec<Attachment> }

  struct Attachment { session: SessionId, kind: String, payload: Vec<u8> }
  ```
  Externally-tagged via the existing `#[serde(rename_all = "snake_case")]` (no `tag/content` per `feedback_postcard_externally_tagged_enums`). `PROTOCOL_VERSION` ticks 2 → 3.

- **Idempotent overwrite.** `RegisterAttachment` against an existing `(session, kind)` overwrites. This matches the workspace-restart flow: `Workspace::host_with` registers on every startup, regardless of whether an entry exists. Reject-on-duplicate or version-stacking would force the consumer to inspect-then-write, which is racy and unhelpful.

- **`kind` filter on `ListAttachments` is optional exact-match.** `None` returns everything; `Some("artel-fs/workspace/v1")` returns just that kind. Prefix-match (`"artel-fs/"` → all versions) was considered and rejected as premature; can be added later without breaking the wire (additive Option field on a future request, or a sibling verb).

- **Lifecycle: cascade.** Removing a session (e.g. via `LeaveSession` on a host, daemon-side eviction) removes its attachments. Invariant: a registered attachment refers to a session that exists. `ForgetAttachment` is the "session still alive, I want my entry gone" escape hatch — rare, but cheap to ship and the IPC is awkward without it (you'd otherwise have to leave the session entirely just to drop your entry).

- **Disk-backed, persisted synchronously.** Attachments must survive daemon restart — that's the whole point. Persistence happens within the `RegisterAttachment` RPC, not lazily, matching the persistence-first principle the rest of `Registry` already follows. Layout extends `FsLogStore` (or a sibling); exact shape deferred to the plan — both "new file per attachment under each session's persistence dir" and "single `attachments.toml` per session" are reasonable; `feedback_extensive_unit_tests` will guide the choice once we see what tests need.

- **Reject-on-unknown-session at write.** `RegisterAttachment` for a `SessionId` the daemon doesn't know about returns `ProtocolError::UnknownSession` (existing variant — no new error type needed). Otherwise the cascade invariant is violable from the start.

- **First consumer (artel-fs) payload v1:**
  ```rust
  // In artel-fs, postcard-encoded into the opaque payload.
  struct WorkspaceAttachmentV1 {
      local_path: PathBuf,
      state_dir:  PathBuf,
      role:       WorkspaceRole,  // enum { Host, Joiner }
  }
  ```
  Kind tag: `"artel-fs/workspace/v1"`. `Workspace::host_with` / `join_with` issue `RegisterAttachment` after a successful attach. **Fast-follow:** add `last_seen: Option<i64>` (Unix epoch seconds) — additive, `Option<>`-typed so postcard `#[serde(default)]` covers it without a kind bump. Useful for "this workspace hasn't been touched in 6 months, prune it?" workflows. If we later want it *required*, kind bumps to `v2`.

- **`ForgetAttachment` is entry-only.** It does NOT delete the on-disk `state_dir` or any consumer-side state. Destructive deletion is never implicit. A future "purge workspace" CLI verb could combine `ForgetAttachment` + `rm -rf state_dir` explicitly.

## Open questions

These need to be resolved during planning, not now:

- **Exact persistence layout.** New file per attachment vs. single per-session attachments file vs. extending an existing table. Driven by what produces the cleanest `MemoryStore` test fixture pattern — pick whichever lets us write the unit tests `feedback_extensive_unit_tests` requires.
- **Cascade implementation point.** Hook into wherever `Session` is removed from `Registry::sessions` (today: `LeaveSession` host path, plus any future eviction). One place, but worth pinning down in the plan.
- **`ListAttachments` ordering.** Unspecified for v1 — return-order-not-guaranteed in the doc-comment. Consumers that care can sort client-side. If a use case for stable ordering shows up, add it then.
- **CLI verb.** Out of scope for the registry slice; ships separately as `artel workspace list`. The IPC surface this slice ships is what the CLI will be built on.

## Next steps

→ `/workflows:plan` for sub-slicing (likely 2a protocol, 2b daemon, 2c artel-fs consumer, 2d docs — mirroring the 1a/1b/1c/1d shape that worked for stable-session-id).
