---
date: 2026-06-11
topic: ticket-revocation
status: PLAN — ready to implement
brainstorm: docs/brainstorms/2026-06-11-ticket-revocation-brainstorm.md
---

# Ticket revocation — issued-ticket ledger + RevokeTicket/ListTickets

Source brainstorm (DECIDED, user-confirmed 2026-06-11):
`docs/brainstorms/2026-06-11-ticket-revocation-brainstorm.md`. The four
contestable calls are final: issued-ticket ledger (not a bare revoked
set); ticket-only revocation (admitted peers untouched); slice surface =
`RevokeTicket` + `ListTickets` + `ticket_id` in mint responses (CLI
deferred); error on never-issued revoke. A fifth call was settled after
plan review (2026-06-11): admission is **issued-only / fail-closed** —
ledger absence rejects, superseding the first draft's deny-list. P2P-lens check and residual gaps
are in the brainstorm — none block this plan.

Slicing discipline mirrors B.5/C: protocol-crate types first, then
store, then registry wiring/enforcement, then server dispatch +
integration. Each sub-slice ends green on `make test` and commits on
its own (no Co-Authored-By, never push). `make ci-local` before the
final commit.

## The decided line

The host records every ticket it mints in a per-session **ledger**
(`tickets.json` sidecar in the session dir). `RevokeTicket { session,
ticket_id }` flips an entry to `Revoked`; `ensure_member` — the sole
authoritative admission gate — requires a CapClaim's `ticket_id` to be
**present in the ledger and `Active`** (issued-only, fail closed;
user-confirmed 2026-06-11 superseding an earlier deny-list draft).
Ledger absence rejects exactly like revocation. The ledger check runs
**after** expiry and cap-sig so an unauthenticated forger can't oracle
ledger contents. Successful admission appends the
joiner to the entry's `used_by`. `ListTickets` returns ledger metadata
(never the encoded bearer string). No ticket/gossip wire change;
`PROTOCOL_VERSION` 7→8 for the new IPC verbs.

## Mint-site inventory (traced; the plan's one non-obvious finding)

`mint_ticket` (`session.rs:769`) is called from **three** sites, and
all three must write a ledger entry:

1. `Registry::host` create path (`session.rs:715`) — the initial
   session ticket.
2. `Registry::host` **resume** path (`session.rs:678`) — re-host of an
   existing local session re-mints a *fresh random* `TicketId` every
   time. So every daemon-restart re-host (and every `Workspace::
   host_with` remount) appends a ledger entry. Accepted: entries are
   ~100 bytes, bounded by session lifetime, and listing them is
   honest — those bearer strings genuinely exist and genuinely admit.
   The plan does NOT dedup or reuse ids on resume.
3. `Registry::issue_ticket` (`session.rs:747`) — explicit extra mints.

Shape: change `mint_ticket` → `fn mint_ticket(..) -> (JoinTicket,
TicketId)` and make each caller record the entry + persist. Recording
inside `mint_ticket` itself is wrong — it's sync and storeless; keep
it pure.

## Slice 1 — protocol crate: types, verbs, version

`crates/artel-protocol`:

- `ticket.rs` (or a new small `ledger` section in it): public
  `TicketStatus { Active, Revoked }` and `TicketEntry { ticket_id:
  TicketId, granted_cap: Capability, expiry_ms: u64, issued_at_ms:
  u64, status: TicketStatus, used_by: Vec<PeerId> }`. Both
  `Serialize`/`Deserialize` (postcard external tagging — new types, no
  compat concern). These live in the protocol crate because
  `Response::Tickets` carries them; the daemon persists the same type.
- `rpc.rs` — **append** variants (externally-tagged postcard: order is
  load-bearing, append only):
  - `Request::RevokeTicket { session: SessionId, ticket_id: TicketId }`
  - `Request::ListTickets { session: SessionId }`
  - `Response::TicketRevoked`
  - `Response::Tickets { entries: Vec<TicketEntry> }`
  - `Response::IssuedTicket` gains `ticket_id: TicketId`.
  - `Response::HostSession` gains `ticket_id: TicketId`.
- `error.rs` — `ProtocolError::UnknownTicket(TicketId)`, slug
  `"unknown_ticket"`. Doc: never returned to joiners; the joiner-facing
  rejection stays the deliberately-opaque `InvalidTicket` (whose
  docstring already says "or has been revoked").
- `version.rs` — `PROTOCOL_VERSION` 7→8 (+ its pinned test).
- `ids.rs:73` doc comment: `TicketId` is no longer "carried but never
  enforced" — update to point at this slice.

Tests (match existing rpc.rs style): postcard + JSON round-trips for
every new/changed variant; `TicketEntry` round-trip; slug test for
`UnknownTicket`. Mutating `IssuedTicket`/`HostSession` breaks existing
round-trip tests — update in the same commit. Doctests via the
Makefile pairing.

**Commit 1**, green on `make test`.

## Slice 2 — store: persistence of the ledger

`crates/artel-daemon/src/store`:

- `record.rs` — `SessionRecord.tickets: Vec<TicketEntry>` with
  `#[serde(default)]` semantics at the load site (absent sidecar ⇒
  empty vec).
- Trait (`mod.rs`) — one new method:
  `async fn put_tickets(&self, session: SessionId, tickets:
  &[TicketEntry]) -> io::Result<()>` — full-ledger rewrite per
  mutation, covering mint, revoke, and `used_by` appends with one op
  (ledgers are small; matches the `meta.json` rewrite idiom; decided
  in brainstorm open-questions).
- `fs.rs` — `TICKETS_FILE = "tickets.json"` in the session dir, JSON
  via the existing tmp+rename helper, `FILE_MODE` 0600. `put_tickets`
  errors `NotFound` if the session dir is missing (same contract as
  `bump_host_epoch`). `load_one` reads the sidecar; **absent file ⇒
  empty ledger** (pre-slice dirs load clean; we don't care about
  back-compat but this is also just the correct default). No `Meta`
  schema bump — that's the point of the sidecar.
- `memory.rs` — mirror: store the vec on the in-memory record.

Tests: put → load_all round-trip; absent `tickets.json` ⇒ empty;
`delete()` cascades (already free — assert the file is gone with the
dir); rewrite-in-place updates status/used_by; tmp-file sweep covers
the new file pattern if `sweep_tmp_files` is glob-based (verify).

**Commit 2**, green.

## Slice 3 — registry: record at mint, revoke, enforce, used_by

`crates/artel-daemon/src/session.rs`:

- `Session.tickets: Vec<TicketEntry>` (persisted via
  `record()`/`from_record` pass-through, unlike derived `caps`).
- `mint_ticket` returns `(JoinTicket, TicketId)`. All three call sites
  append `TicketEntry { status: Active, issued_at_ms: now_ms(),
  used_by: vec![] }` under the session lock and persist with
  `put_tickets`, **store-before-memory** per house discipline. The
  create path can fold the initial entry into `create(&record)`
  instead of a separate `put_tickets` (record already carries it).
  `Registry::host` / `issue_ticket` signatures grow the `TicketId`
  return for the server layer.
- New `Registry::revoke_ticket(session, ticket_id)`: authority = same
  shape as `issue_ticket` (lookup → `SessionKind::Local` else
  `NotHost`). Unknown id → new `SessionError::UnknownTicket(TicketId)`.
  Already-revoked → idempotent `Ok`. Else flip status, `put_tickets`,
  then memory.
- New `Registry::list_tickets(session)`: `Local` only (`NotHost` on
  mirrors), returns `Vec<TicketEntry>` clone.
- **Enforcement** in `ensure_member` — issued-only, fail closed: the
  existing claim checks (expiry → sig) run before the session lookup
  and stay put; the ledger check needs the session, so it lands just
  after the session arc lookup, under the lock, **before** the
  already-member early return. Look up the claim's `ticket_id` in the
  ledger: absent ⇒ `Err(SessionError::TicketRevoked)` (joiner-opaque,
  same as revoked — don't leak issued-vs-revoked to bearers); status
  `Revoked` ⇒ same error; `Active` ⇒ proceed. While the entry is in
  hand, cross-check the claim's `granted_cap` and `expiry_ms` against
  the ledger entry and reject mismatches — nearly free, and catches
  ledger/signature disagreement. Pre-slice outstanding tickets stop
  admitting: deliberate, back-compat waived (B.5 cutover precedent);
  a resumed session recovers by re-host (which re-mints with a fresh
  ledger entry).
- `used_by`: on successful admission (the newly-added-member path,
  after the store `add_member` succeeds), append `peer.id` to the
  claim's entry if absent + `put_tickets`. Do this while still holding
  the session lock, **before** the existing `drop(s)`/`events_tx.send`
  (the drop-before-send discipline at `session.rs:1209-1214` is about
  the send + auto-grant re-entry, not store writes — store writes
  under the lock are the existing idiom). Failure to persist used_by
  must NOT fail admission — log at warn; it's advisory metadata.
- `SessionError`: add `TicketRevoked` and `UnknownTicket(TicketId)` +
  the `PartialEq` arm-matching impl. `server.rs::
  session_error_to_protocol`: `TicketRevoked` → `ProtocolError::
  InvalidTicket` (joiner-opaque, same as expiry); `UnknownTicket(t)` →
  `ProtocolError::UnknownTicket(t)` (host-operator-facing).

Tests (unit, in-crate, memory store unless noted):

- revoke → `ensure_member` with that claim rejects; a *different*
  active ticket for the same session still admits.
- idempotent re-revoke `Ok`; unknown id → `UnknownTicket`; revoke on
  a Remote mirror → `NotHost`; revoke on unknown session →
  `UnknownSession`.
- expired **and** revoked claim → expiry wins (check-order pin).
- admit → `used_by` contains the peer; second admit same ticket
  different peer → both listed.
- admit → revoke same ticket → admitted peer still sends OK
  (orthogonality); a *new* bearer of the same ticket is rejected.
- admit → leave → revoke → re-join attempt rejected (the rejoin hole;
  exercise via `ensure_member` with the same claim — `leave()` on a
  mirror deletes the joiner's local session so the real path re-runs
  announcement → `ensure_member`).
- fs-store: revoke → rebuild registry from `load_all` → still
  rejected (persistence-first rule).
- all three mint sites write ledger entries; host-resume mints a
  *new* entry (count grows by one per resume).
- issued-only: claim with a ledger-absent id is rejected even when
  expiry+sig pass (the stolen-signing-key / rollback case); the
  rejection is indistinguishable from revoked at the protocol layer.
- cap/expiry cross-check: claim whose `granted_cap` or `expiry_ms`
  disagrees with the ledger entry is rejected.
- mint → admit round-trip: a ticket minted through each of the three
  sites admits (guards against a mint site forgetting its ledger
  write, which under issued-only would brick that ticket).

**Commit 3**, green.

## Slice 4 — server dispatch, integration, docs

- `server.rs`: `RevokeTicket` / `ListTickets` arms — membership gate
  (`NotSubscribed` if the caller isn't subscribed to the session, same
  as `IssueTicket` at `server.rs:685`), then registry call.
  `dispatch_host` / `IssueTicket` arm thread the new `ticket_id`
  through to the responses.
- Tier B integration (`crates/artel-daemon/tests/tiered_tickets.rs` is
  the natural home): two-daemon flow — host mints two tickets, revokes
  one; joiner A with the live ticket admits and syncs; joiner B with
  the revoked ticket is never admitted (no `PeerJoined` on the host,
  no auto-grant, no upgrade delivery; B's join times out
  joiner-side — same UX as expiry, per brainstorm residual). Plus:
  `ListTickets` over IPC reflects status + `used_by`; revoke survives
  host daemon restart (restart harness already exists in that file's
  tier).
- `artel-fs`: no surface change; run its suite to confirm (the
  `HostSession` response shape change may touch its client-side
  matches — fix up as found, e.g. `workspace.rs:774`).
- Docs: roadmap § Future tiered-tickets entry — replace "Ticket
  *revocation* … remains future work" with a DONE-style note
  (versions, ledger, verbs, the gossip-lurker + no-NAK residuals
  carry over from the brainstorm). `error.rs` / `session.rs:65` doc
  comments already say "or revoked" — now true; no change needed.
- `make ci-local` (fmt, clippy both feature modes, doc, tests, n0).

**Commit 4**, green on `make ci-local`.

## Explicitly out (per brainstorm — do not let scope creep back in)

- CLI `ticket list`/`ticket revoke` subcommands.
- Joiner-side expiry fail-fast in `materialise_remote_session` (UX
  only, traced non-security).
- Any gossip frame / NAK / revocation propagation (P2P-lens: don't
  freeze host-shaped wire formats).
- Ledger GC; kicking `used_by` peers on revoke.

## Risks / open-at-implementation

- `Response::HostSession`/`IssuedTicket` field additions ripple
  through every consumer match — mechanical but scattered
  (artel-client passthrough is untyped `request()`, so artel-fs and
  daemon tests are the match sites; let the compiler enumerate).
- Lock-order check at the `used_by` write: it sits inside the
  existing `ensure_member` critical section before `drop(s)` — no new
  lock acquisition, no await on another session lock; keep it that
  way.
- If `sweep_tmp_files` patterns are per-file rather than glob, add
  the tickets tmp pattern (verify in Slice 2).
- Issued-only makes `tickets.json` load-bearing for admission
  availability. Match the `Meta` posture: a *corrupt* sidecar fails
  the session load loudly (don't silently treat as empty — that would
  brick all outstanding tickets with no diagnostic); only a genuinely
  *absent* file loads as the empty ledger (fresh dir / pre-slice dir,
  where no-admissions-until-re-host is the intended fail-closed
  behaviour).
- Test churn: existing tiered-ticket tests that synthesize `CapClaim`s
  with arbitrary ids must mint through the registry (or seed the
  ledger) first. Mechanical; the compiler/test failures enumerate
  them.
