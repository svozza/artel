---
date: 2026-06-11
topic: ticket-revocation
status: BRAINSTORM — key decisions user-confirmed 2026-06-11, ready for /workflows:plan
parent: docs/brainstorms/2026-05-30-auth-story-brainstorm.md
predecessors:
  - docs/brainstorms/2026-06-03-auth-slice-c-l2-capabilities-seed.md (Slice C, SHIPPED)
  - roadmap.md § Future, tiered-tickets entry (names revocation as the residual)
---

# Ticket revocation — invalidate a minted-but-unused join ticket

## What we're building

A host can invalidate a join ticket it previously minted. Today the
only kill switches for a leaked ticket are its `expiry_ms` (fixed at
mint) and closing the whole session. This slice adds a host-side
**issued-ticket ledger** (persisted per session), a `RevokeTicket`
RPC, a `ListTickets` RPC, and a revocation check at the existing
admission seam. Post-admission peer revocation
(`CapabilityAction::Revoke` + `PeerFilter`) already exists and is
untouched; this slice is pre-admission only.

No wire-format change to the ticket itself: `SessionTicket` has
carried `ticket_id: TicketId` since `TICKET_VERSION` 3 precisely so
this layer wouldn't need a bump (`crates/artel-protocol/src/ids.rs:73`
— "carried but never enforced" in v1; this slice is the enforcement).

## Current state (verified against the tree, 2026-06-11)

| Claim | Verified at |
|---|---|
| `SessionTicket.ticket_id` on the wire, unenforced | `crates/artel-protocol/src/ticket.rs:58`, `ids.rs:73` |
| Admission verifies CapClaim (expiry → host sig) before any state mutation | `crates/artel-daemon/src/session.rs:1161-1184` (`ensure_member`) |
| CapClaim originates from the joiner's `JoinAnnouncement`; `SendRequest` path deliberately does **not** admit | `gossip_bridge.rs:846-871`, `gossip_bridge.rs:1091-1097` |
| Host records **nothing** at mint — `mint_ticket` generates a random id and returns the encoded ticket | `session.rs:769-794` |
| `IssueTicket` response returns only the encoded `JoinTicket`, not its id | `rpc.rs:407-411` |
| `issue_ticket` authority = caller subscribed + `SessionKind::Local` | `server.rs:680-696`, `session.rs:747-767` |
| Cap grant/revoke authority is **host-only** in code (tightened from "any RW holder") | `session.rs:385-393` |
| Session close cascades by deleting the whole session dir | `store/fs.rs` `delete()`, `session.rs:1086` |
| `SessionError::InvalidTicket` doc already says "malformed or revoked" | `session.rs:65`, `error.rs:32` |
| `PROTOCOL_VERSION` 7, `TICKET_VERSION` 4, `Meta` schema 3 | `version.rs:17`, `lib.rs:52`, `store/fs.rs:443-448` |

**Stale-memory note:** the roadmap NOTE (memory
`reopen-grant-authority-on-readonly-tickets`) still phrases L2 as "any
RW holder grants". The code tightened that to host-only during Slice C
(`session.rs:385`). That dissolves the "can any RW holder revoke a
ticket?" question — see Decision 3.

## Approaches considered

### Approach A: bare revoked-id set

Persist a `HashSet<TicketId>` per session (new field in `meta.json` or
a sidecar file). `RevokeTicket` inserts; admission checks membership.

**Pros:** minimal surface; one new store concept.
**Cons:** can't validate that a revoked id was ever issued (typo'd id
silently "succeeds"); `ListTickets` impossible without a second
structure; no way to tell the operator "that ticket was already used
by peer X". The host forgetting what it minted is the actual gap —
this papers over it.

### Approach B: issued-ticket ledger (RECOMMENDED)

Record every mint in a per-session ledger:
`{ ticket_id, granted_cap, expiry_ms, issued_at_ms, status: Active |
Revoked, used_by: Vec<PeerId> }`. `RevokeTicket` flips status (and can
reject unknown ids); admission checks `status == Revoked`; admission
success appends the peer to `used_by`. `ListTickets` reads it straight
off.

**Pros:** revocation validates against real issuance; `ListTickets`
falls out for free; `used_by` gives the operator the "already joined —
revoke the peer instead" hint; closes the leave-then-rejoin hole with
full observability.
**Cons:** slightly more state; a store-trait extension on both
backends (fs + memory).

### Approach C: revocation as a signed session-log event

Model `TicketRevoked { ticket_id }` as a `MessageKind::Capability`-like
host-signed log event, projected like the cap set.

**Pros:** symmetric-P2P-ready; one event-sourcing idiom for all auth
state.
**Cons:** YAGNI today — enforcement is host-only at admission (host is
sole sequencer), so no other peer ever needs to evaluate the revoked
set; broadcasting ticket ids to the topic leaks issuance metadata to
all members for zero enforcement benefit; heavier than the problem.
Revisit only with the symmetric-P2P rethink (roadmap § Future).

**Choice: B.** A is too little (the missing ledger *is* the feature's
substrate), C is too much (no consumer for the propagation).

## Key decisions

The four contestable calls (ledger vs bare set; used-ticket semantics;
slice surface; unknown-id behaviour) were put to the user on 2026-06-11
and confirmed as below. The rest follow from code constraints.

1. **Persistence: `tickets.json` sidecar in the session dir** (next to
   `meta.json` / the log), written with the existing tmp+rename
   discipline; `0600` like other session files. Survives host restart
   via `load_all` → `SessionRecord` (new field) → in-memory `Session`.
   Cascade on session close is free — `delete()` removes the dir.
   A separate file avoids a `Meta` schema bump; an absent file
   deserialises to an empty ledger (correct for pre-slice dirs, and we
   don't care about back-compat anyway — nothing is pushed).
   `SessionStore` grows ledger ops (exact shape is plan detail);
   memory backend mirrors fs.

2. **Enforcement point: host-only, at the existing CapClaim seam —
   and issued-only (fail closed; user-confirmed 2026-06-11).** In
   `ensure_member`, order becomes **expiry → cap-sig → ledger**, and
   the ledger check requires the claim's `ticket_id` to be **present
   and `Active`** — absence rejects, not just explicit revocation.
   Rationale: a deny-list fails *open* (a backup-rollback of
   `tickets.json` silently un-revokes; a ticket forged with a stolen
   signing key admits and — per Decision 4 — cannot even be revoked,
   since revoking an unknown id errors). Issued-only makes the ledger
   the single source of truth, survives disk loss fail-closed (next
   re-host re-mints), and means a stolen signing key alone no longer
   mints admissible tickets. Cost — pre-slice outstanding tickets stop
   admitting — is a back-compat cost, which is waived (precedent:
   B.5's MESSAGE_FORMAT cutover). Sig still runs before the ledger
   lookup so an unauthenticated forger can't oracle ledger contents.
   Check sits before the early-return-if-already-member, same as
   today's claim checks.
   Nothing propagates to joiners: joiners never admit anyone (the
   `SendRequest` backstop deliberately doesn't admit,
   `gossip_bridge.rs:1091`), so the host's set is the only one that
   matters. Confirmed: no gossip frame, no `GOSSIP_WIRE_VERSION`
   change.

3. **Authority: same as `IssueTicket`** — caller subscribed to the
   session + `SessionKind::Local` (i.e. the hosting daemon; any local
   IPC client of it). This matches both the mint path and the
   host-only cap grant/revoke rule the code already has. The "any RW
   holder" question from the memory is moot — code is host-only since
   Slice C. Remote members cannot revoke tickets in v1 (they also
   can't mint them).

4. **RPC shape (IPC only, postcard externally-tagged — append
   variants):**
   - `Request::RevokeTicket { session, ticket_id }` →
     `Response::TicketRevoked` (idempotent on already-revoked;
     **error on never-issued** — the ledger makes the distinction
     possible and a typo'd id should not report success).
   - `Request::ListTickets { session }` → `Response::Tickets { entries }`
     with `{ ticket_id, granted_cap, expiry_ms, issued_at_ms, status,
     used_by }`. The ledger stores metadata only — never the encoded
     bearer ticket.
   - `Response::IssuedTicket` gains `ticket_id: TicketId` (and
     `Response::HostSession` likewise, since `host()` mints the
     initial ticket — it must enter the ledger too). Without this the
     minter can't name the ticket to revoke it short of decoding the
     ticket string client-side.
   - New `ProtocolError` variant for never-issued
     (`UnknownTicket(TicketId)` or similar) rather than overloading
     `InvalidTicket`, which is the *joiner-facing* "won't tell you
     why" error. Plan decides the exact name/slug.

5. **Revoking an already-used ticket: revoke the ticket, don't touch
   the peer.** The two layers stay orthogonal: ticket revocation gates
   *future admissions*; `CapabilityAction::Revoke` + `PeerFilter`
   handle an admitted peer. The response/CLI can surface `used_by` so
   the operator knows a peer-revoke is also needed. Revoking a used
   ticket is *not* a no-op even with the bearer inside: it blocks
   (a) other holders of the same bearer token — tickets are not
   single-use — and (b) the leave-then-rejoin path (today `leave()`
   removes membership but the old ticket re-admits; revocation closes
   that).

6. **Version bumps: `PROTOCOL_VERSION` 7 → 8.** New request/response
   variants change the IPC surface; precedent is every RPC addition
   bumps it. Everything else stays: `TICKET_VERSION` 4 (wire form
   untouched — that's the whole point of the carried id),
   `MESSAGE_FORMAT` 3, `GOSSIP_WIRE_VERSION` 1, `Meta` schema 3
   (ledger is a sidecar file).

## P2P-lens check (does this foreclose symmetric P2P?)

Standing question for every slice (ADR-001 § Future evolution). Answer
here: **no new lock-in; one deliberate avoidance.**

- **What carries forward unchanged:** `TicketId` on the wire; the
  ledger concept (it is *issuer-local* state — "what did I mint" —
  which generalises to any-peer-mints); the RPC verbs (issuer-scoped,
  meaningful per-peer); the principle that ticket revocation gates
  admission and never touches an admitted peer. "Issuer revokes what
  they issued" is also causally sound in a no-host world.
- **What gets ripped, and why that's fine:** the single enforcement
  point (host's `ensure_member`). But that is not a *new* coupling —
  the entire ticket layer is already host-shaped (tickets are signed
  by the host key under `artel/ticket-cap-v1` and verified against
  `host_peer_id`). ADR-001 names "discovery and join" as one of the
  four open design problems of symmetric P2P; tickets, admission, and
  therefore revocation all get redesigned together in that rethink.
  This slice rides a layer already scheduled for that rebuild and
  adds zero host-shaped **wire** surface: no gossip frames, no
  propagation protocol, no signed revocation format. All
  host-coupling introduced here is daemon-local code — the cheap kind
  to rip.
- **The deliberate avoidance:** rejecting Approach C (revocation as a
  signed log event) is what keeps this true. C *looked* P2P-friendly
  but would have frozen a broadcast revocation wire format designed
  around today's sole-authority model — a premature answer to the
  P2P propagation question (which is the same quorum/causal-authority
  question already marked at `session.rs:387` for cap grants, and
  which ADR-001 says needs its own analysis). Deferring it means the
  P2P revocation design gets made when the P2P trust model exists.

## Residual gaps (documented, deliberately out of scope)

- **Gossip-lurker parity.** A revoked-ticket bearer (like an
  expired-ticket bearer today) still knows the session/topic id and
  may join the gossip mesh and read broadcasts; revocation gates
  *admission* (membership, auto-grant, upgrade delivery), not topic
  subscription. Exactly the same exposure class as expiry — this
  slice achieves parity, not more. A transport-layer fix is the
  PeerFilter-at-daemon / topic-key-rotation conversation, not this
  slice.
- **No rejection feedback to the joiner.** A rejected
  `JoinAnnouncement` is logged host-side only (`warn!` at
  `gossip_bridge.rs:870`); the joiner's mirror hangs until its own
  timeout. Same UX as expired tickets today. A NAK would be a gossip
  wire change — explicitly deferred.
- **Local join path** (`Registry::join`) skips expiry/cap-sig — but
  traced 2026-06-11, this is **not an enforcement bypass**, and the
  revoked check does not belong there. Membership is per-daemon (L1:
  every IPC caller is stamped `daemon_peer_id`, `server.rs:753-756`),
  so the existing-session arm only ever sees an id that is already a
  member — host id is seeded at `Session::new` (`session.rs:255`) and
  a second same-daemon joiner hits the self-rejoin early return
  (`session.rs:859`) *before any ticket claim is consulted*. A check
  there is dead code. The fresh-remote arm materialises a mirror and
  announces; the **host's `ensure_member` is the sole authoritative
  gate** — exactly where this slice adds the revoked check. Two
  things could still be done joiner-side, both non-security:
  (a) fail-fast on an *expired* ticket before standing up a
  mirror/gossip subscription that will only time out — pure UX,
  ~small; (b) nothing for *revocation* — the revoked set lives on the
  host and is invisible to the joiner daemon by design. Deferring (a)
  costs nothing structurally (purely local additive check, no wire or
  version impact); it is out of this slice.
- **Ledger GC.** Revoked/expired entries live until session close.
  Bounded by session lifetime; fine for v1.

## Open questions for the plan

- Exact `SessionStore` trait shape: full-ledger rewrite per mutation
  (matches `meta.json` style, simplest) vs append-style. Ledgers are
  small; rewrite is probably right.
- Where `used_by` is appended: inside `ensure_member` after successful
  admission — confirm lock ordering against the existing
  drop-before-send discipline (`session.rs:1209-1214`).
- Does `artel-fs::Workspace` surface anything (probably not — tickets
  are a daemon/CLI concern), and does the `artel` CLI grow
  `ticket list` / `ticket revoke` subcommands in this slice or the
  next? CLI today only has Status/Stop/List (`bin/artel.rs:36`).
- Error-ordering test: revoked + expired ticket — which error wins
  (suggest expiry first, matching check order).

## Test obligations (sketch — plan expands)

- Protocol: round-trip new request/response variants (postcard + JSON,
  matching existing rpc.rs test style); `IssuedTicket.ticket_id`
  matches the decoded ticket.
- Daemon unit: revoke → `ensure_member` rejects that claim; other
  tickets for the same session unaffected; idempotent re-revoke;
  never-issued id errors; revoke on remote-mirror session → `NotHost`.
- Persistence: revoke → restart (`load_all`) → still rejected;
  pre-slice session dir without `tickets.json` loads as empty ledger;
  session close removes the ledger with the dir.
- Rejoin hole: admit → leave → revoke ticket → rejoin attempt
  rejected. Note `leave()` on a Remote mirror deletes the joiner's
  whole local session (`session.rs:1080`), so the rejoin exercises
  the full materialise → JoinAnnouncement → `ensure_member` path —
  the revoked check is genuinely hit.
- Used-ticket seam: admit via ticket → revoke same ticket → admitted
  peer still functions (sends OK) but a second bearer is rejected;
  `used_by` populated.
- Tier B integration: two-daemon JoinAnnouncement with a revoked
  ticket → never admitted, no auto-grant, no upgrade delivery.

## Next steps

→ `/workflows:plan` for implementation slicing (one slice: ledger +
store + RPC + enforcement + tests; commit at the end per repo
convention, never push).
