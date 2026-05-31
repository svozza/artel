---
date: 2026-05-30
topic: auth-story
---

# v1 Auth Story

## What We're Building

A v1 authentication and authorization model for `artel`, designed
before the pre-1.0 wire-protocol cutover so we don't have to bolt
security on later. Three layers land together:

- **L1 — In-session peer identity.** Collapse `PeerInfo.id` into
  the iroh `EndpointId`. Every host-side and joiner-side acceptance
  path checks that the body's `peer.id` matches the gossip-
  authenticated `delivered_from`. Eliminates the spoofed-authorship
  / ghost-membership bug class structurally.
- **L2 — Capability model as event-sourced grants.** Capability
  state is *not* host-side mutable state. `Grant { peer, cap }` and
  `Revoke { peer }` are signed messages in the session log
  (`MessageKind::Capability`). Every peer derives the current cap
  set by replaying the log; enforcement is a projection at-seq.
- **L3 — Per-message signing.** Every `SessionMessage` (Send,
  Capability, System) carries an ed25519 signature over the body
  including `session_id`. Sender's signing key is derivable from
  `peer.id` because L1 collapsed the namespaces.

L4 (per-consumer IPC trust) and L5 (cross-device user identity)
are named fast-follows, not v1.

## Why This Approach

### Event sourcing as the unifying model

The v1 design is event sourcing applied to authorization:

| Event sourcing concept | Our session log |
|---|---|
| Event log = source of truth | Signed `SessionMessage` log |
| Aggregate / state | Current cap set: `HashMap<EndpointId, Capability>` |
| Projection | Cap set at any seq, replayed from log |
| Commands | `Send`, `Grant`, `Revoke` (validated, then appended) |
| Snapshots | Persisted log on disk (3b-1, shipped) |
| Idempotent replay | Every peer arrives at the same cap state from the same log |

The two load-bearing properties are **deterministic replay** and
**append-only signed events**. They give us:

- Forward-compatibility with symmetric P2P. The cap layer doesn't
  depend on a designated host; only the *sequencer* role does, and
  ADR-001 already explicitly defers replacing the sequencer.
- Free audit trail. "Who granted bob at seq 47?" reads from the log.
- No separate revocation list to persist or replicate — revocation
  is just another event.

### Why collapse, not bind, for L1

`docs/roadmap/peer-identity-authentication.md` raised the fork.
Picked **collapse** (`PeerId == EndpointId.as_bytes()`) over **bind**
(signed binding from app `PeerId` to `EndpointId`):

- The indirection has no current consumer asking for it.
- L5 (cross-device user identity) introduces a *user* layer
  regardless of which option we pick — `EndpointId` is per-device
  either way. So bind doesn't help L5; it just adds a second per-
  device namespace.
- Collapse eliminates a whole bug class structurally rather than
  relying on a verification step we'd have to remember on every
  new frame body.

### Why data-plane caps, not control-plane

Initial sketch had host-side mutable cap tables. The user pushed
back ("doesn't feel very p2p") and the right answer is event
sourcing: cap grants live in the log, every peer projects.
Concrete differences:

| | Control-plane caps | Data-plane caps |
|---|---|---|
| Where is cap state? | Host's in-memory `HashMap` | Replayed from signed log |
| Who enforces `Send`? | Host only | Every peer |
| Symmetric-P2P upgrade cost | Rewrite | Minimal — only the sequencer changes |
| Revocation | Separate persisted list + admin RPC | Just another signed message |
| Audit trail | Build separately | Free |
| Tampered-replay detection | None (host trusted) | Sigs fail; drop tampered batch |

### Why every-peer enforcement (Strategy Y)

Given L3 ships anyway, the verification machinery exists. The
marginal cost is "call `verify()` and project cap-at-seq in the
joiner-side replay path." Buys the data-plane model its actual
teeth: if every peer doesn't verify, the cap log is decorative.

v1 simplification: cap-violation on inbound is **drop+log**, not
fatal-to-the-session. Hardening to fatal happens once we have a
story for "host published a malformed log, what now?"

## Key Decisions

- **L1 = collapse.** `artel-protocol::PeerId` becomes a newtype
  around 32 bytes documented as an `iroh::EndpointId`. `artel-
  protocol` stays iroh-free; the invariant lives in docs and is
  enforced at construction sites in `artel-daemon`.
- **L1 enforcement.** Host's `handle_inbound_frame` arms (every
  one that uses `peer.id`) check `body.peer.id == delivered_from`.
  Joiner-side bridge stamps `endpoint.id().as_bytes()` into
  outbound `PeerInfo` so the joiner upholds the invariant the
  host enforces. Mismatch → drop+log.
- **L2 capability tiers, v1.** Two: `Read` (can subscribe and
  consume) and `ReadWrite` (today's behaviour). No `Admin` tier;
  grant/revoke capability rides on top of `ReadWrite` for v1
  ("any write-capable peer can grant"). Tighten in a follow-up
  if a consumer needs separation.
- **L2 join → grant flow.** Auto-grant on ticket use. Originator
  (or any current grant-holder, post-v1) issues a default
  `Grant(joiner, ReadWrite)` when the joiner's first
  `JoinAnnouncement` lands. Preserves today's "ticket = full
  access" UX while putting the grant on the wire as a real
  signed event. Differentiated tickets (read vs read-write) is
  a v2 capability — orthogonal to whether the cap layer exists.
- **L2 revocation in v1.** `MessageKind::Capability` with
  `Action::Revoke { peer }`. Just another signed event. Pinned
  by an integration test where Bob writes, gets revoked, retries
  → host rejects, every joiner's local replay also rejects.
- **L2 ticket changes.** Bump `TICKET_VERSION` 2 → 3. New
  fields: `ticket_id` (so future revocation can name them) and
  `originator_pubkey` (for verifying the originator's first cap
  grant before the rest of the log has replayed). No `cap` field
  in the ticket — caps live in the log.
- **L3 signing scope.** Sign all log-resident `MessageKind`
  variants: `Chat`, `Tool`, `System`, `Capability`. Non-log
  gossip frames (acks, raw routing) stay unsigned because
  iroh-gossip's `delivered_from` already authenticates them at
  the network layer.
- **L3 signature scope (S1).** `sig = sign(version || session_id
  || timestamp || peer || kind || action || payload)`.
  Critically includes `session_id` — without it, `Grant`s are
  cross-session-replayable. Excludes `seq` (host-assigned). The
  host could reorder/duplicate signed messages, but the host is
  already the sequencer; this is an existing trust assumption,
  not a new one. Revisited when symmetric P2P lands and `seq`
  goes away.
- **Originator-as-root-of-trust by social convention.** The
  originator's first `Grant(self, ReadWrite)` is the cap log's
  root. Other peers verify subsequent grants are signed by
  current grant-holders. The protocol does not privilege the
  originator; the *log content* does. This is what makes the
  model symmetric-P2P-compatible.
- **Crypto.** ed25519 throughout, matching iroh's existing
  primitive. No new dep.
- **Session-key reuse.** Sender's signing key is the iroh
  endpoint secret key (already persisted at `~/.artel/iroh.key`
  mode 0600 by 2a). Verifier derives `VerifyingKey` from
  `peer.id` (=`EndpointId`). One key per daemon, not per
  session. Per-session signing keys are a v2 capability if
  ever needed.

## Threat Model

### In scope for v1 (prevented by L1+L2+L3)

| Attack | How v1 prevents it |
|---|---|
| Spoofed authorship — a joiner forges another peer's `PeerId` on `Send` | L1: `peer.id` must equal `delivered_from`. L3: signature must verify against `peer.id` as a public key |
| Ghost membership — a joiner injects a fake `JoinAnnouncement` for a peer that doesn't exist | L1: same check |
| Unauthorized writes — a read-only joiner sends a chat message | L2: cap projection at-seq rejects on host; every peer enforces locally |
| Cross-session replay — a `Grant` signed in session A is replayed in session B | `session_id` is in the signed scope |
| Tampered replay history — host (or attacker holding the log) modifies past messages on `Subscribe { since }` | L3: signatures fail to verify; receiver drops the tampered batch |
| Revoked peer reuses old credentials — Bob is revoked, then attempts to send | Cap projection is at-seq, not at-now; new messages from Bob fail at their own seq |
| Forged grants — Bob issues `Grant(bob, ReadWrite)` self-signed | L3: only sigs from current grant-holders project into the cap set |

### Out of scope for v1 (named follow-ups)

| Attack | Why deferred | Where it lands |
|---|---|---|
| Host rewrites seq order to censor / reorder | Host-as-sequencer trust; documented in ADR-001 § Future evolution | Symmetric P2P (long-term) |
| Local consumer impersonates another consumer over IPC | No multi-consumer story today | L4 fast-follow |
| Cross-device "same user" identity | No real cross-device story today | L5 fast-follow |
| DoS by flooding signed messages from a write-capable peer | Cap revocation handles malicious peers reactively; rate-limiting is a separate axis | "Production hardening" |
| Originator key loss = session unrecoverable | Acceptable v1 trade-off; key recovery has its own design pass | Out of scope, named here |
| Side-channel attacks on the iroh secret key file | OS-level concern; 0600 mode is the v1 baseline | Out of scope |
| Daemon-as-attacker against its own consumers | The daemon is in the consumer's TCB by construction | Out of scope |

## Slicing Strategy

Three landings, each independently shippable, each bumps wire
versions cleanly. Order matters because L2 and L3 both depend on
L1, and L2 (cap events) depends on L3 (signing infra).

- **Slice A — L1 collapse.** Smallest. Mostly a typing change in
  `artel-protocol` plus enforcement points in `artel-daemon`'s
  `handle_inbound_frame` arms and `Registry` mutators. No new
  message kinds. `PROTOCOL_VERSION` 3 → 4 because `PeerInfo`'s
  `id` invariants tighten. Joiner-side stamping of
  `endpoint.id().as_bytes()` is the bulk of the diff. Pinned by
  spoofing-attempt regression tests.
- **Slice B — L3 signing.** `SessionMessage` gains a `signature:
  [u8; 64]` field. Sender (every site that calls `Registry::send`
  or constructs a `SessionMessage` for the log) signs. Host's
  `Registry::send` and joiner-side `apply_inbound` both verify;
  reject on failure. Bumps `SCHEMA_VERSION` on persisted log
  format — log-replay path needs a migration story (probably
  "fresh sessions only post-cutover; pre-cutover sessions
  rejected with a clear error" given the alpha).
- **Slice C — L2 caps.** New `MessageKind::Capability` with
  `Action` = `Grant` / `Revoke`. New `Capability` enum (`Read`,
  `ReadWrite`). Host's `Registry::send` projects cap-at-seq and
  rejects unauthorized; joiner-side replay does the same. New
  ticket fields (`ticket_id`, `originator_pubkey`) — bumps
  `TICKET_VERSION` 2 → 3. Auto-grant-on-join wired into
  `ensure_member`.

Each slice ships with: store unit tests + Registry-via-MemoryStore
unit tests + e2e via real Client (per `feedback_extensive_unit_tests`).

## Open Questions

- **Cap projection performance.** Replaying the entire log on
  every inbound message is O(n²) in worst case. v1 mitigation:
  cache the cap-set-at-head between messages, only re-project
  on `Capability` events. Snapshot on disk if the cache miss
  becomes real. Probably premature; profile first.
- **What does `Workspace::host_with`'s session resume do with
  the cap log?** 3b-1 makes sessions resumable across host
  restart. The cap log replays from disk on resume (it's part
  of the session log). No new work — confirm in test.
- **Originator-grant-holder semantics under L2.** v1 says "any
  current grant-holder can issue further grants." Is that too
  permissive? Alternative: only the originator can issue
  initial grants, and any grant-holder can revoke. Worth
  thinking about during /workflows:plan but not blocking.
- **Slice B's log migration.** SCHEMA_VERSION bumps the on-disk
  format. We're alpha so "reject pre-cutover sessions" is fine,
  but call out clearly in release notes.
- **Cap-violation telemetry.** Drop+log is the v1 default. What
  goes in the log line? Enough to diagnose ("peer X tried to
  Send at seq Y, current cap was Read") but not enough to leak
  ticket internals. Settle during plan.

## Cross-references

- ADR-001 § "Auth and capability model" (line 203) — the parent
  deferral this doc cashes in.
- `docs/roadmap/peer-identity-authentication.md` — the L1
  pre-existing scoping doc. This brainstorm supersedes its
  open-design-questions section by picking collapse.
- `docs/roadmap.md` § "Future" → "Ticket-level capabilities &
  auth" — the L2 placeholder. Update on landing.
- `feedback_postcard_externally_tagged_enums` — the new
  `MessageKind::Capability` and `Capability` enums must be
  externally tagged.
- `feedback_extensive_unit_tests` — three slices × store-unit
  + Registry-unit + e2e is the test budget.

## Next Steps

→ `/workflows:plan` Slice A (L1 collapse) first. It's the
smallest, unblocks the other two, and validates the threat
model end-to-end on a single bug class.
