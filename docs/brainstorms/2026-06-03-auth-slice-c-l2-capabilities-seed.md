---
date: 2026-06-03
topic: auth-slice-c-l2-capabilities
status: SEED — consolidation for a fresh agent; needs /brainstorming → plan
parent: docs/brainstorms/2026-05-30-auth-story-brainstorm.md
predecessors:
  - docs/plans/2026-06-02-auth-slice-b-l3-signing-plan.md (Slice B / L3, SHIPPED)
  - docs/plans/2026-06-03-auth-slice-b5-control-frame-auth-plan.md (Slice B.5, SHIPPED)
---

# Auth Slice C — L2 capabilities (event-sourced grants)

## Purpose of this doc

Slice C (L2 capabilities) is the **last open layer of the v1 auth
story**. L1 (peer-id collapse) and L3/B + B.5 (per-message + control-
frame signing) have shipped. The L2 design already exists — in the
parent brainstorm `docs/brainstorms/2026-05-30-auth-story-brainstorm.md`
(§ "L2 — Capability model as event-sourced grants" and the Slice C
paragraph of § Slicing Strategy). **That design is sound and still the
plan of record.**

This seed exists because the parent predates Slice B and B.5, so its
version numbers and a few assumptions have drifted. A fresh agent
should read the parent for the *model*, then this doc for the *deltas
since*, then run `/brainstorming` (to settle the open questions below)
and `/workflows:plan`. Do **not** treat this as a plan — it makes no
new design decisions; it records current state.

## The L2 model in one paragraph (from the parent — unchanged)

Capability state is **not** host-side mutable state. `Grant { peer,
cap }` and `Revoke { peer }` are signed messages in the session log
under a new `MessageKind::Capability`. Every peer derives the current
cap set by replaying the log; enforcement is a **projection at-seq**
(the cap set *as of* a message's seq, not as-of-now). Two tiers for
v1: `Read` (subscribe + consume) and `ReadWrite` (today's behaviour).
Grant/revoke rides on `ReadWrite` ("any write-capable peer can grant").
Auto-grant on join: the originator (or any current grant-holder) emits
`Grant(joiner, ReadWrite)` when the joiner's first `JoinAnnouncement`
lands, preserving today's "ticket = full access" UX as a real signed
event. Cap-violation on inbound is **drop+log**, not fatal-to-session
(v1 simplification). Originator-as-root-of-trust is by *log content*,
not protocol privilege: the originator's first `Grant(self, ReadWrite)`
is the cap log's root, and other peers verify subsequent grants are
signed by current grant-holders — which is what keeps the model
symmetric-P2P-compatible. Full threat table is in the parent § Threat
Model.

## Deltas since the parent was written (what a fresh agent must NOT trust verbatim)

### 1. Version numbers — the parent is stale; here is current state

| Constant | Parent assumed | **Current (post-B.5)** | Slice C action |
|---|---|---|---|
| `PROTOCOL_VERSION` | "Slice A → 4" | **6** (`crates/artel-protocol/src/version.rs`) | bump 6 → 7 |
| `MESSAGE_FORMAT` | "Slice B bumps it" | **3** (`crates/artel-protocol/src/message.rs`) | only bump if the `SessionMessage` shape changes; adding a `MessageKind::Capability` variant does **not** change the struct, so probably no bump (confirm in plan) |
| `Meta::CURRENT_VERSION` | n/a | **3** (`crates/artel-daemon/src/store/fs.rs`) | only bump if the on-disk record gains a field |
| `TICKET_VERSION` | "2 → 3" | still **2** (`crates/artel-protocol/src/ticket.rs`) | **2 → 3 still correct** — this is the bump Slice C owns |

### 2. The `originator_pubkey` ⟷ `host_peer_id` overlap — DECISION DEFERRED TO C

This is the single most important carry-over. B.5's plan
(`docs/plans/2026-06-03-...-b5-...-plan.md` § "Risks / sequencing vs
Slice C") explicitly flags it:

> C plans an `originator_pubkey` ticket field + `MessageKind::Capability`.
> B.5's "host pubkey from the ticket" primitive (`session.host` =
> `host_peer_id`) **overlaps** `originator_pubkey`. B.5 reuses the
> existing `host_peer_id` — it does NOT add a second ticket field. When
> C lands, decide whether the two are the same field; B.5 leaves
> `host_peer_id` as the authority and notes the overlap so C subsumes
> rather than duplicates it.

The `SessionTicket` already carries `host_peer_id` (it's what the joiner
persists as `session.host` and verifies B.5's seq/ack/ctrl sigs
against). The parent wanted a *new* `originator_pubkey` field "for
verifying the originator's first cap grant before the rest of the log
has replayed." **These are very likely the same key** in the current
star-topology world (the host *is* the originator). The brainstorm
must decide:
- **Option A (likely):** reuse `host_peer_id` as the originator pubkey;
  C adds only `ticket_id`, not a second pubkey field. Simpler ticket.
- **Option B:** keep them distinct now to pre-stage symmetric P2P,
  where originator ≠ current sequencer. Costs a field today for a
  future that ADR-001 explicitly defers.
Default toward A unless the brainstorm surfaces a concrete need for B.

### 3. B.5 already built infrastructure C should reuse

- **`sign_ctrl` / `verify_ctrl`** (`crates/artel-protocol/src/signing.rs`,
  `"artel/ctrl-v1"`): host-origin auth over a session-scoped payload.
  C's grant/revoke frames want host-origin auth too — but note grants
  are **log-resident `SessionMessage`s**, so they're signed by the
  *author* via the existing `sign_body` / `verify_message`
  (`"artel/sig-v1"`) path, plus the host's `host_sig` seq-sig (B.5).
  C does **not** need a new domain tag for grants that ride in the log;
  it reuses the per-message signing B + B.5 already established. Reach
  for `sign_ctrl`/`verify_ctrl` only if C adds a *non-log* control
  frame (it probably doesn't — grants live in the log).
- **Reserved kind-tag byte.** `crates/artel-protocol/src/signing.rs`
  already reserves canonical-bytes `kind_tag` byte **`3`** for
  `MessageKind::Capability` (see the `kind_tag` match + the module
  doc-comment "capability=3 reserved"). Slice C lands the enum variant
  and fills that arm — existing v3 signatures stay valid by
  construction (the byte was pre-allocated for exactly this).
- **`host_epoch` / `EpochBeacon`** (B.5): orthogonal to caps. No
  interaction expected; confirm the cap log replays correctly across a
  host resume (the parent's open question on `Workspace::host_with` —
  it's part of the session log, so it should "just replay," but pin it).

### 4. `MessageKind` is still 3 variants

`crates/artel-protocol/src/message.rs`: `Chat`, `Tool`, `System`. Slice
C adds `Capability`. The `kind_tag` reservation (above) means the
signed-bytes layout is already forward-compatible. `Send` IPC payloads
(`SendPayload`) carry no cap field and shouldn't gain one — caps live in
the log, not the send command.

### 5. Auto-grant hook location

`Registry::ensure_member` (`crates/artel-daemon/src/session.rs:853`) is
where the host admits a joiner on `JoinAnnouncement`. That's the
natural site to emit the auto-`Grant(joiner, ReadWrite)`. Confirm
during planning that emitting a log message from there (vs the bridge's
`JoinAnnouncement` arm) is the right layering.

## Slice C shape (from the parent § Slicing Strategy — still accurate)

- New `MessageKind::Capability` with an `Action` = `Grant { peer, cap }`
  / `Revoke { peer }`. New `Capability` enum (`Read`, `ReadWrite`).
- Host's `Registry::send` projects cap-at-seq and rejects unauthorized
  writes; joiner-side replay (`apply_inbound_mirror_message` in
  `session.rs`, the B.5-extracted pipeline) does the same projection so
  every peer enforces locally — that's what gives data-plane caps their
  teeth.
- New ticket field(s) — at minimum `ticket_id`; `originator_pubkey`
  only if delta #2 lands on Option B. `TICKET_VERSION` 2 → 3.
- Auto-grant-on-join wired into `ensure_member`.
- Tests per `feedback_extensive_unit_tests`: store-unit +
  Registry-via-MemoryStore unit + e2e via real `Client` (+ at least one
  `_n0` per the two-tier pyramid). The mandated adversarial set from the
  parent threat table: read-only joiner's write rejected (host AND every
  joiner's local replay), revoked-peer write rejected at-seq, forged
  self-grant rejected, cross-session `Grant` replay rejected
  (`session_id` already in the signed scope — B shipped this).

## Open questions to settle in /brainstorming (from the parent + B.5)

1. **`originator_pubkey` vs `host_peer_id`** — delta #2 above. The
   load-bearing decision. Resolve first.
2. **Originator-grant-holder semantics.** v1 says "any current
   grant-holder can issue further grants." Too permissive? Alternative:
   only the originator issues *initial* grants; any grant-holder can
   revoke. (Parent § Open Questions.)
3. **Cap projection performance.** Replaying the whole log per inbound
   message is O(n²) worst case. v1 mitigation: cache cap-set-at-head,
   re-project only on `Capability` events. Probably premature — profile
   first. (Parent § Open Questions.)
4. **Cap log across host resume.** 3b-1 + B.5 make sessions resumable;
   the cap log is part of the session log, so it should replay from disk
   with no new work. Confirm in a test rather than assuming. (Parent §
   Open Questions.)
5. **Cap-violation telemetry.** drop+log is the v1 default. Settle the
   log-line content: enough to diagnose ("peer X tried to Send at seq Y,
   cap was Read") without leaking ticket internals. (Parent § Open
   Questions.)
6. **`MESSAGE_FORMAT` bump?** Adding `MessageKind::Capability` doesn't
   change the `SessionMessage` struct, so likely no bump — but the
   reserved `kind_tag` byte means even if a pre-C daemon somehow saw a
   `Capability` frame it couldn't decode the variant. Decide whether
   that warrants a format bump or is covered by the `PROTOCOL_VERSION`
   6 → 7 gossip-wire cutover. (New, B.5-era.)

## Cross-references

- Parent (the real L2 design): `docs/brainstorms/2026-05-30-auth-story-brainstorm.md`
- Slice B (L3 signing, SHIPPED): `docs/plans/2026-06-02-auth-slice-b-l3-signing-plan.md`
- Slice B.5 (control-frame auth, SHIPPED): `docs/plans/2026-06-03-auth-slice-b5-control-frame-auth-plan.md`
  — read its § "Risks / sequencing vs Slice C" for the overlap flag.
- Reusable signing infra: `crates/artel-protocol/src/signing.rs`
  (`sign_body`/`verify_message`, `sign_seq`/`verify_seq`,
  `sign_ctrl`/`verify_ctrl`, reserved `kind_tag` byte 3).
- Auto-grant hook: `Registry::ensure_member`, `crates/artel-daemon/src/session.rs`.
- Joiner-side enforcement seam: `apply_inbound_mirror_message`, same file.

## Next steps

→ Fresh agent: read the parent for the model, this doc for the deltas,
then `/brainstorming` to settle Q1–Q6 (Q1 first — it shapes the ticket),
then `/workflows:plan` Slice C. Each landing ships with the three-tier
test set per workspace rule.
