---
date: 2026-06-01
topic: auth-l1-fix3-shape
---

# Auth L1 — Fix #3: stamp peer.id on Registry::join (and friends)

## What We're Building

Close the L1 gap that survived the auth-L1 slice: `Registry::join`
(plus `host` and the lookup paths) currently honours the
IPC-supplied `peer.id` verbatim. The bridge stamps the daemon's
authenticated id on the wire, but the local in-memory
`Session::members` still records whatever the IPC client claimed.

A lying IPC client makes the local daemon disagree with the remote
on the joiner's id; downstream `LeaveSession` / `Subscribe` /
`Send` propagate or look up the wrong id. The handoff
(`docs/handoff-auth-l1-review-fixes.md` lines 108-204) walked this
trail and reverted the first attempt.

This brainstorm picks the trust model so the next slice can land
without reverting again.

## Why This Approach

Three options were sketched. Reframed honestly:

- **A. Silent stamp at IPC dispatch** — daemon overwrites
  `peer.id` from `PeerInfo`. Wire-compatible. Lying clients fail
  silently and confusingly far from the lie.
- **B. Typed `IdMismatch` rejection** — daemon rejects mismatched
  `peer.id`. Wire-compatible. Forces clients to read their id
  from `Hello` and pass it through. Loud failure, narrower test
  blast radius than A.
- **C. Strip `peer.id` from `JoinSession` / `HostSession`** —
  PROTOCOL_VERSION 4 → 5. The field stops existing. Daemon's
  authenticated id is the only id. Cleanest design; breaking
  change.

A and B both exist primarily to dodge breaking changes. The user
has explicitly removed that constraint ("alpha project, correct
over everything, breaking changes are fine"). With that constraint
gone, A/B's case collapses: they're keeping a field whose only
correct value is the one the daemon already knows.

**Pick C.** It eliminates the bug class structurally — same logic
that picked "collapse" over "bind" in the parent
`docs/brainstorms/2026-05-30-auth-story-brainstorm.md` § "Why
collapse, not bind". A field that can only correctly hold one
value should not be on the wire.

### Why not A or B even given the breaking-change-friendly steer

A is silently-correct-by-rewriting-the-input. The only failure
ergonomics it has is "your `peer.id` was different from what
arrived on the wire" deep in a downstream test. That's the
phenomenon that burned the first attempt — `dispatch` cached the
IPC-supplied `PeerInfo` into `memberships: HashMap<SessionId,
PeerInfo>` *before* the stamp ever ran, so the stamp had to move
to the IPC boundary, at which point all the in-process tests
broke anyway. A's "wire-compatible" is doing no work.

B is "A but loud". It pushes every IPC client (artel-fs, emdash,
chat-harness, future tools) to plumb a daemon-issued id through
their callsites. That's the same client-side migration cost as C,
*plus* the daemon now ships a typed error variant whose only
purpose is to police a field that didn't need to exist. C drops
the field instead.

## Key Decisions

- **Strip `peer.id` from the IPC.** `Request::JoinSession` and
  `Request::HostSession` lose `peer: PeerInfo` and gain
  `display_name: String`. `Send` is already host-stamped (no
  `peer` field on the request side); `Subscribe` and
  `LeaveSession` already key on `SessionId` alone. Server-pushed
  events (`PeerJoined { peer: PeerInfo }`, `PeerLeft { peer:
  PeerId }`) stay — they're emitted by the daemon, which has the
  authenticated id. **Why:** the only correct value of `peer.id`
  on a request is the daemon's authenticated id; the daemon
  already has it; the field is dead weight at best and a spoofing
  surface at worst. **How to apply:** every IPC callsite that
  built `PeerInfo::new(name, some_id)` becomes `display_name:
  name`; the daemon constructs the `PeerInfo` internally from
  `(authenticated_peer_id, display_name)`.

- **PROTOCOL_VERSION 4 → 5.** No compat shim, no
  `#[serde(default)]`, no v4 fallback parser. Pre-1.0 we don't
  defend a wire surface. Every consumer recompiles together.
  **Why:** mirrors the auth-L1 plan's "no compat code"
  decision; same alpha posture. **How to apply:** bump
  `crates/artel-protocol/src/version.rs`; rename the
  `current_protocol_version_is_four` test to
  `current_protocol_version_is_five`.

- **`Registry::join` becomes idempotent on self-rejoin.**
  `Workspace::shutdown` does NOT issue `LeaveSession`. Rejoining
  a session whose member set already contains the daemon's
  authenticated id is a no-op (returns `Ok(JoinResponse {
  session, head })` against the existing record; emits no
  `PeerJoined` event). **Why:** the daemon's session membership
  is the *authenticated identity's* membership — persistent
  across consumer (artel-fs / emdash) restarts. That's the
  point of the daemon model. A consumer drop is "this consumer
  view is gone", not "leave the session". **How to apply:**
  `Registry::join` short-circuits if `session.members.contains(
  authenticated_peer_id)`; `Registry::host` does the same on
  resume. The existing `AlreadyJoined` error becomes unreachable
  for self-rejoin and can be removed (or kept as
  defence-in-depth for an attacker injecting a forged
  `JoinAnnouncement` claiming our own id — that path drops at
  the bridge per A1's enforcement, so practically: remove).

- **Multi-IPC-clients-on-one-daemon tests are rewritten as
  2-daemon Pair tests.** Five tests named in the handoff
  (`session.rs::tests::join_artel_ticket_succeeds_and_emits_peer_joined`,
  `joiner_leave_local_session_keeps_session_alive`,
  `member_leave_emits_peer_left_and_keeps_session`,
  `client.rs::events_stream_delivers_message_events`,
  `tests/sessions.rs::two_clients_chat_end_to_end`) currently
  simulate Alice and Bob as different IPC clients of one
  daemon. Under C that scenario is unrepresentable — every
  IPC client of one daemon shares its authenticated id. **Why:**
  the unrepresentable scenario was production fiction anyway.
  **How to apply:** boundary-crossing tests
  (`tests/sessions.rs`, `client.rs`) become 2-daemon `Pair`
  tests (faithful to production). The `session.rs::tests`
  Registry unit tests are testing the Registry contract
  directly with arbitrary `PeerInfo` (no IPC boundary), so
  their existing shape stands — `Registry::join`'s pure-Rust
  signature still takes a `PeerInfo` (the IPC layer is what
  loses the field).

- **`Registry::join` / `host` signature stays `peer: PeerInfo`.**
  The IPC layer is what loses the `peer.id` field; inside the
  daemon, `Registry` is still constructed by the dispatch layer
  with a fully-formed `PeerInfo` (`authenticated_peer_id +
  IPC-supplied display_name`). **Why:** Registry is a
  pure-Rust API; its callers in tests want to pass arbitrary
  ids for unit-level scenarios. Keeping the type honest about
  what it accepts is more useful than narrowing to a
  reconstructed-internally id. **How to apply:** `dispatch` in
  `server.rs` constructs `PeerInfo { id:
  bridge.authenticated_peer_id(), display_name }` and passes
  that to `Registry::join` / `host`. The IPC field rename is
  what changes; the registry surface is unchanged.

- **Tests added to pin the new contract.**
  - `tests/auth_l1_spoofing.rs` (existing file from A1) gains
    `joiner_local_membership_uses_authenticated_id` — the test
    that already existed in the failed attempt; it's sound and
    survives unchanged.
  - `tests/sessions.rs` gains
    `repeated_join_against_same_daemon_is_idempotent` — daemon
    A hosts; daemon A's *same consumer* issues `JoinSession`
    twice (the second after a simulated workspace remount).
    Asserts: second response is `Ok`, no second `PeerJoined`
    fires on subscribers. Shores up the
    `crash_recovery::steady_state_sigkill_preserves_state`
    failure mode named in the handoff.
  - `crates/artel-protocol/src/rpc.rs::tests` gains
    `join_session_request_has_no_peer_id` — postcard
    round-trips a `JoinSession` and asserts the wire form
    carries no PeerId bytes. (Soft enforcement; the real
    enforcement is the type system once the field is gone.)

## Threat Model Coverage

The L1 brainstorm's spoofed-authorship / ghost-membership /
tampered-replay attacks are already structurally prevented by A1's
host-side `peer.id == delivered_from` check on the gossip arms.
This fix closes the *complementary* gap: the IPC-side trust model.

| Attack (IPC-side) | Prevention under C |
|---|---|
| Lying IPC client claims another peer's id on `JoinSession` | Field doesn't exist; daemon stamps its own |
| Lying IPC client claims another peer's id on `HostSession` | Field doesn't exist; daemon stamps its own |
| IPC client and remote daemon disagree on joiner's id | Both derive from the same iroh `EndpointId`; they cannot disagree |
| Future IPC consumer reintroduces the bug | Type system makes the bad call unrepresentable |

## Slicing Strategy

Single slice. The protocol bump, daemon-side dispatch change,
artel-fs / artel-client / chat-harness migrations, and test
rewrites all land together because the wire change forces them.
Splitting wouldn't help — there's no in-between state where the
crate compiles with `peer.id` on `Request::JoinSession` removed
but consumers still pass it.

Ordering inside the slice:

1. `crates/artel-protocol/src/rpc.rs` — drop `peer: PeerInfo` from
   `Request::JoinSession` and `Request::HostSession`; add
   `display_name: String` to each. Update the `proptest`
   `arb_request` arms.
2. `crates/artel-protocol/src/version.rs` — bump to 5; rename test.
3. `crates/artel-daemon/src/server.rs::dispatch` — read
   `display_name` from the request, construct the `PeerInfo`
   from `(bridge.authenticated_peer_id(), display_name)` before
   calling into `Registry`.
4. `crates/artel-daemon/src/session.rs::Registry::join` /
   `Registry::host` — make idempotent on self-rejoin.
   `Registry::host` resume path checks
   `session.host == peer.id` and short-circuits if equal; new
   `peer.id` against existing host record stays
   `SessionConflict` (current behaviour; correct).
5. Consumer migrations: `artel-fs` (Workspace), `artel-client`
   (Client::join_session API), `chat-harness` (local-only,
   uncommitted but should still compile in-tree), tests under
   `crates/artel-daemon/tests/`, `crates/artel-fs/tests/`,
   `crates/artel-client/tests/`.
6. Test rewrites per the "multi-client" decision.

## Open Questions

- **`Workspace::host_with` / `join_with` API shape.** The
  consumer-facing API takes a `PeerInfo` today. Two options:
  (a) narrow it to `display_name: impl Into<String>` to mirror
  the IPC narrowing, (b) keep the `PeerInfo` shape and ignore
  `peer.id` consumer-side too. Recommendation: (a) — same
  reasoning as the IPC narrowing; if the field has only one
  correct value, don't accept it. Settle during plan.

- **`Client::join_session` vs `Client::host_session` doc updates.**
  Both currently accept a `PeerInfo`. When narrowed, the
  doc-comment should explain *why* — point at this brainstorm.

- **`PeerInfo` itself.** Today `PeerInfo { id: PeerId,
  display_name: String }`. Question for v2: does `PeerInfo`
  exist outside server-pushed events at all? It still ships in
  `Event::PeerJoined` (correct — daemon-emitted), in
  `SessionMessage` (host-stamped), and inside gossip frame
  bodies (also host-stamped). So the type stays; it just stops
  appearing in `Request` variants. No type deletion.

- **Migration cost for `artel-fs::Workspace`.** Today
  `host_with(peer: PeerInfo, ...)`. Every consumer call site in
  `artel-fs` and tests passes a hand-rolled `PeerInfo`. The
  migration is mechanical (drop the `id` arg, pass
  `display_name` only); estimate is one commit's worth of
  in-place edits. Plan should enumerate them.

## Cross-references

- `docs/handoff-auth-l1-review-fixes.md` lines 108-204 — the
  failed-attempt trail. This brainstorm's recommendation
  supersedes that doc's "three options" sketch by picking C.
- `docs/brainstorms/2026-05-30-auth-story-brainstorm.md` —
  parent auth story; "Why collapse, not bind" is the same logic
  applied at the type-level (one namespace) that this fix
  applies at the wire-level (one source of truth).
- `docs/plans/2026-05-30-auth-l1-peer-id-collapse-plan.md` —
  the A1/A2/A3 plan. This fix is the auth-L1 IPC-side
  complement to A1's gossip-side enforcement.
- `docs/adr/001-collab-substrate-platform.md` § "Auth and
  capability model" — the ADR cross-link should pick up
  another "(L1 IPC closed YYYY-MM-DD)" annotation when this
  lands.
- `feedback_postcard_externally_tagged_enums` — the IPC
  request/response enums already use the externally-tagged
  shape; this fix adds no new variants but stays consistent.
- `feedback_extensive_unit_tests` — the test budget covers
  Registry-unit + IPC-postcard + e2e Pair scenarios.

## Next Steps

→ `/workflows:plan` for a single slice covering items 1–6 in
"Slicing Strategy". Land as one commit (or, if the test
rewrites get unwieldy, a wire-change commit + a test-migration
commit; both must land before merging since the workspace
won't compile in between).
