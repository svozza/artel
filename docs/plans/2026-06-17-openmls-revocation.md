---
date: 2026-06-17
topic: openmls-revocation (Tier-1 robust write-revocation)
status: PLAN — ready to implement
adrs: docs/adr/002-no-mls-for-tier1-write-revocation.md, docs/adr/003-daemon-stays-namespace-agnostic.md
context: CONTEXT.md (Write authority / Capability & revocation / Tiers / Layer boundary)
---

# Tier-1 robust write-revocation — namespace rotation (no MLS)

This plan was settled in a grill-with-docs session (2026-06-17). No
standalone brainstorm — the contestable calls are recorded as ADRs and the
vocabulary in `CONTEXT.md`. The "Roads not taken" section below carries the
two reversals that would otherwise be lost.

## The problem (one line)

Write capability *is* possession of the iroh-docs `NamespaceSecret`; `Revoke`
suspends *delivery* but not *writing*, so a revoked peer keeps signing valid
entries that flush wholesale on any later sync. The fix is to rotate the
namespace so the revoked peer's retained secret is worthless.

## The decided design

- **No MLS.** Rotation mints a fresh random `NamespaceSecret` and ships it to
  survivors over the **existing** `DeliveryFrame::Secret` unicast. The OpenMLS
  spike confirmed the mechanics work but the exporter is only ever a *seed*
  above the namespace ed25519 key; MLS removes a non-risk (key-gen) and leaves
  the real risk (quiescence) untouched. Deferred to a content-encryption axis
  or Tier-2. See **ADR-002**.
- **Two verbs, two threat models.**
  - **Demote** (cooperative): `Grant{peer, Read}` + a **downgrade
    notification** the demoted daemon honours by halting its own watcher. No
    rotation. Trust-based. The notification is *load-bearing*, not UI sugar.
  - **Evict** (adversarial): `Revoke{peer}` + `PeerFilter` block + namespace
    rotation = the only true cryptographic write cut-off.
- **Author binding via same-seed.** Drop iroh-docs `author_default()`; seed the
  doc `Author` from the **same bytes** as the workspace endpoint key, so
  `AuthorId == endpoint_id` and the existing `peer_map` resolves
  `entry.author → daemon PeerId` for free — no announcement, unforgeable. Safe:
  a TLS `CertificateVerify` payload (64×`0x20` + context string) can never
  collide with an `entry.to_vec()` (32-byte namespace-pubkey prefix), so reuse
  of one ed25519 secret across transport + entry signing is sound.
- **Rotation = host-reauthored snapshot under a freeze-drain barrier.** Entries
  cannot be copied across namespaces (the author signature covers the
  `NamespaceId`), so carry-forward must *re-author*, and only a key's holder can
  sign. Therefore the **host** re-publishes the quiesced old doc's
  latest-per-key snapshot **under its own author** into the new namespace,
  filtering revoked authors at snapshot via the binding. Blobs never move
  (content-addressed; survivors re-download nothing).
- **Identity decoupling preserves the derivation verbatim.** `doc-id` becomes
  the write-once **genesis namespace**; `SessionId = session_id_for(genesis_ns)`
  is unchanged. The **current namespace** becomes a mutable `state_dir`
  attribute. `SessionId`/topic/tickets never change on rotation.
- **Layer boundary held (ADR-003).** All namespace state lives in `artel-fs`.
  The daemon stays an opaque `[u8;32]` courier; `namespace_epoch` rides *inside*
  the already-opaque `WorkspaceTicketEnvelope`. No `iroh-docs` dep below the fs
  line.

## The freeze-drain barrier (the correctness core)

1. Host computes the **survivor set** = current RW caps **minus** the evicted
   peer (already in `PeerMap`; the evicted peer is `PeerFilter`-blocked and
   never in the wait-set, so it cannot stall the freeze).
2. Host broadcasts a **freeze** (host-sequenced gossip message).
3. Survivors **pause their watcher**, flush pending sync, **ack** through the
   sequencer.
4. Host **drains** (bounded wait `T`), then snapshots a *provably quiescent*
   old doc. **A non-acking survivor is stranded** on the old doc (no new
   secret) and recovers on reconnect via `namespace_epoch` — same class as a
   peer that was offline during rotation. Rotation never blocks on liveness.
5. Host mints the new `NamespaceSecret`, re-publishes the filtered latest-per-key
   snapshot under host author, bumps `namespace_epoch`, ships secret+epoch to
   survivors only.
6. Survivors **import the new namespace, then resume** — new writes are newest
   by construction (import-before-republish invariant), no timestamp
   reconciliation needed.

## Joiner re-import (the live-doc swap)

`Workspace.doc` is immutable and the watcher/applier/cap_listener are spawned
under one `shutdown_token`. Naively calling `shutdown()` on an epoch bump would
**kill the cap_listener that carries the bump signal**. Fix: **split the
cancellation scope** into two sibling children of `shutdown_token`:

- `cap_token` → cap_listener (durable; survives re-import)
- `doc_token` → watcher / applier / `WorkspaceNode`

Re-import = pause watcher (the freeze step) → cancel+recreate `doc_token` →
tear down doc/node/tasks → re-establish doc against the new namespace (reuse the
tested `join_with` import path) → respawn watcher/applier under a fresh
`doc_token`. `cap_listener` untouched. **Consumer contract unchanged** —
`host/join/run/shutdown/events` are byte-for-byte the same; chat-harness (the
only external consumer) never touches doc/author/cap_listener/namespace.

## Slices

Slicing discipline mirrors B.5/C: protocol types first, then store/state, then
wiring/enforcement, then integration. Each sub-slice ends green on `make test`
and commits on its own (no Co-Authored-By, never push). `make ci-local` before
the final commit of each slice. **Tests-first: confirm red before
implementing.** Alpha — bump `PROTOCOL_VERSION` freely, hard-reject old shapes,
no migration shims.

### Slice 0 — Demote: downgrade notification + watcher-halt (standalone)

The roadmap's "ship this first regardless." Independently shippable; no rotation
dependency; makes the cooperative downgrade honest.

- Host→peer **downgrade unicast** mirroring `UPGRADE_ACTION` (new
  `DeliveryFrame` variant or `DowngradePayload` on the existing channel).
- Joiner: on receipt, **halt its own watcher** (voluntary write-stop) and
  surface a `WorkspaceEvent` so the consumer can react.
- Tests: demoted joiner stops publishing; survivors stop seeing its writes;
  notification idempotent on resume/re-deliver.
- **No namespace change.** Trust-based; documented as cooperative-only.

### Slice 1 — Author binding (same-seed)

- Replace `author_default()` with `author_import(Author::from_bytes(<endpoint
  key bytes>))` + `author_set_default`, on both host and joiner construction.
- Assert `AuthorId == endpoint_id`; `peer_map` resolves `entry.author`.
- Tests: every authored entry's author resolves to the daemon `PeerId`;
  cross-protocol-reuse safety pinned by a doc-comment + a test that the two
  signed byte-strings never share a prefix.
- Prereq for Slice 3's snapshot filter. No wire change to the daemon.

### Slice 2 — Identity decoupling (genesis vs current namespace)

- `doc-id` redefined as **write-once genesis**; add a `current-namespace`
  persisted attribute in `state_dir` (separate file, 0600 not needed — ids
  aren't secret).
- `SessionId = session_id_for(read(doc-id))` unchanged; host resume reads the
  current namespace from the new attribute, not `doc-id`.
- Add `namespace_epoch` to the `WorkspaceTicketEnvelope` (opaque to daemon).
- Tests: rotation-free round-trip leaves `SessionId`/topic/tickets stable;
  current-namespace attribute persists across restart; `session_id.rs` tests
  untouched and green.

### Slice 3 — Evict: freeze-drain rotation

The hard slice. Depends on 1 + 2.

- **3a — token split**: two sibling child tokens; re-import recreates
  `doc_token` only. Test: cap_listener survives a doc-token teardown.
- **3b — freeze/ack protocol**: host-sequenced freeze message + survivor ack +
  bounded-wait drain. Test: ack collection over post-revoke survivor set;
  non-acking survivor stranded + recovers on `namespace_epoch` bump.
- **3c — host snapshot + re-author**: filter latest-per-key by `author→PeerId ∈
  survivors`, re-publish under host author into a freshly minted namespace
  (`set_hash`, blobs unchanged), bump epoch, ship secret. Test: revoked
  author's entries absent from new doc; survivor content intact; blob store
  unchanged (no re-download).
- **3d — joiner re-import**: epoch-bump → quiesce → doc-token teardown →
  `join_with`-style import of new namespace → resume. Test: survivor converges
  on new namespace; evicted peer (with retained old secret) cannot reach the
  new doc and `PeerFilter` blocks its connection.
- **3e — end-to-end**: evict an adversarial RW peer; assert it can no longer
  produce state survivors accept, across a reconnect.

## Roads not taken (the two reversals — preserved here, not ADR-worthy)

1. **Carry-forward reconcile → freeze-drain barrier.** First adopted a windowed
   model where survivors carried their own straggler writes forward. Reversed:
   its convergence is *conditional* (a survivor re-publishing a stale value
   "now" can resurrect it past a newer one) because iroh-docs' public API stamps
   `Record::new_current` on every insert — no timestamped-insert exists, so the
   needed original-timestamp comparison would be hand-rolled across N nodes. The
   freeze-drain barrier eliminates stragglers entirely (one re-publisher, no
   comparison) at the cost of a bounded freeze + a dead-survivor strand.
2. **"Demote = read-only" is not a write cut-off.** A `Grant{Read}` peer keeps
   the live `NamespaceSecret`, has no joiner-side write check, and its watcher
   keeps publishing — so cooperative demote is enforcement *only* via the
   load-bearing notification + voluntary watcher-halt. The real cryptographic
   cut-off is Evict + rotation. This is why Demote and Evict are distinct verbs.

## Deferred / out of scope

- Tier-2 P2P revocation (project-at-merge, authority model, convergence under
  partition) — the symmetric-peer end-state; MLS or BeeKEM revisited there.
- Per-author **ingest** enforcement (reject non-RW authors' entries at every
  node) — the binding makes it possible, but it's the Tier-2 check; host-only
  sequencing covers v1.
- `namespace_epoch` re-import for a peer that was *offline across multiple*
  rotations — falls out of the genesis-anchored re-import but pin a test when
  Slice 3 lands.
