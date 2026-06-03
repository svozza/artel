---
date: 2026-06-02
topic: control-frame-auth-slice-b5
status: DECIDED — brainstorm output, ready for /workflows:plan
supersedes-seed: docs/brainstorms/2026-06-02-control-frame-auth-and-replay-brainstorm.md
---

# Slice B.5 — control-frame & sequence authentication

## What We're Building

Authenticate the three host-originated gossip frames a joiner acts on
but does not currently verify by origin: `GossipBody::Message`,
`GossipBody::SessionClosed`, and `GossipBody::SendAck`. Each gains a
**host signature** the joiner verifies against the host's public key —
the same 32 bytes it already persists as `session.host` (=
`host_peer_id` from the ticket). This is origin-authentication by
signature, not by who relayed the bytes, so it is
**topology-independent**: it survives the move from today's star
topology to multi-hop mesh / symmetric P2P, where the
`delivered_from`-based checks silently break.

The slice closes all three open findings from the seed (#1 replay, #2
forged `SessionClosed`, #3 forged `SendAck`) with **one mechanism**:
*the host signs everything it originates or sequences; joiners verify
host-origin against the host pubkey from the ticket.*

## Why This Approach

### The three findings share one root (sharper than the seed)

The seed framed #1 as "host-trust today, attacker-only post-symmetry."
**That's wrong, and it matters.** Tracing the live code:

- The joiner `Message` arm (`gossip_bridge.rs:800`) is
  `GossipBody::Message(m) => on_message(m)` with **no `delivered_from`
  check at all**.
- `on_message` (`session.rs:649`) does: dedup-by-seq → `verify_message`
  (author sig) → append.

So any ticket-holder on the topic can capture a genuine host-broadcast
`Message`, re-broadcast it under a **fresh seq** (e.g. a gap, or
`u64::MAX`), and the victim joiner appends it: no origin check, new seq
passes dedup, the genuine author sig verifies. That is a **live
unauthenticated-peer attack today** — same class as #2/#3, not a
host-trust artifact and not contingent on symmetric P2P. The seed's
"only the host re-broadcasts Message frames" is a fact about what the
*host sends*; it says nothing about what the joiner arm *accepts*.

Conclusion: **#1/#2/#3 are the same bug** — joiner-role arms accept
host-originated frame types without authenticating host-origin. One
fix, not three point-patches.

### Why signed frames, not the star-topology guard

Rejected outright (not "back pocket"). A `delivered_from == host`
joiner-arm guard is sound only under the star topology, bakes that
assumption deeper, and becomes a **false-drop bomb** the moment the
mesh densifies (it would drop a valid host frame relayed
joiner→joiner). The host signature *is* the topology-independent check;
layering a topology-dependent one on top adds only future wrong-drops.
Do not add it, even as defense-in-depth.

### Backwards compat is explicitly waived

Alpha-stage, both daemons rebuild together, no on-the-wire
compatibility surface to defend. "Reject pre-cutover sessions with a
clear error" (the Slice B precedent) is the migration story. This frees
the design to pick *correct* over *cheap* at every fork below.

## Key Decisions

### D1 — Host seq-sig lives **inside `SessionMessage`** (persisted)

`SessionMessage` gains a `host_sig: SigBytes` field alongside the
existing author `signature`. `MESSAGE_FORMAT` 2 → 3; on-disk log
migration = reject pre-cutover.

- **Rationale:** with migration cost waived, the transient-envelope
  option's only advantage evaporates. Persisting the seq-sig makes the
  log **self-authenticating per-entry**: each entry carries its
  sequencer's signature, so it survives a backfill served by a
  *non-sequencer* peer — exactly the symmetric-P2P future ADR-001
  targets. The transient choice loses the seq binding on disk and
  re-opens as a gap precisely when symmetric P2P lands.
- **Scope of host_sig:** domain-tagged canonical bytes
  `"artel/seq-v1" || session_id || seq || author_sig`. Binds *this seq*
  to *this body* under the host key. Replay-under-new-seq fails the
  host's seq-sig (the captured host_sig is bound to the original seq).
- **Verify path (one place):** thread `host_sig` + host pubkey into the
  existing `on_message` closure so all checks happen together *after*
  dedup: `dedup-by-seq → verify author sig → verify host_sig`. Do NOT
  verify eagerly in the bridge — that would re-pay crypto on the
  routine duplicate deliveries that review-fix #5 deliberately skips.

### D2 — `SendAck` is host-signed over `(session_id, req_id, result)`

Domain tag `"artel/ack-v1"`. Authenticating `req_id` unforgeably (the
seed's Q3 alternative) is a dead end: `req_id` is *broadcast in the
`SendRequest`* (`gossip.rs:75`), public the instant the request goes
out — unguessability buys nothing against a topic sniffer. Sign it.

- **Replay of a genuine old ack is already self-limiting:** `req_id` is
  a fresh v4 UUID; `pending_sends.remove(req_id)` returns `None` for a
  non-pending id, so a replayed ack resolves nothing. No nonce needed.
- `result` must be bound into the sig so an attacker can't flip a
  signed `Ok` into `Err` or vice-versa. Sign over a digest of the
  encoded `result`.

### D3 — `SessionClosed` is host-signed over `(session_id, host_epoch)`

Domain tag `"artel/ctrl-v1"`. **This is the only frame that needs an
epoch** — see "Freshness audit" below. `host_epoch` is a monotonic
per-host-incarnation counter persisted with the session, so a close
captured from incarnation N is rejected against a same-id resume at
N+1. (Sessions can be re-hosted on the same id per 3b-1, landing on the
same topic, so `session_id` alone is replayable across resume.)

- **Epoch source — open for the plan:** simplest is a counter persisted
  in the session record, bumped on each `host_session` for that id. The
  joiner learns the current epoch from... (see Open Questions — this is
  the one unresolved mechanism).

### D4 — Fold in the gossip-frame version byte

Re-introduce the deferred leading version byte on gossip frames as part
of this cutover (`[version: u8][postcard(GossipBody)]`; decode rejects
unknown versions explicitly). We're breaking the gossip wire anyway;
"must be right" + a real cutover moment is the right time. ~30 lines +
tests.

### Freshness audit — why epoch is scoped to `SessionClosed` only

The host key is the iroh endpoint secret, **stable across restart**, so
a signature alone never distinguishes incarnation N from N+1. Each
frame needs its own freshness token; only one lacks a natural one:

| Frame | Forgery closed by | Replay closed by |
|---|---|---|
| `Message` (#1) | host seq-sig | `seq` + dedup-before-verify (`session.rs:667`) — a replayed old Message lands on its original seq and is dropped |
| `SendAck` (#3) | host ack-sig over `result` | `req_id` v4 freshness — replayed ack finds no pending entry |
| `SessionClosed` (#2) | host ctrl-sig | **nothing natural** → needs `host_epoch` |

So the epoch mechanism stays tightly scoped to one frame.

### Resolved seed questions

- **Q1** → D1 (persisted).  **Q2** → fix #1 now; it's a live attack, not
  deferred host-trust.  **Q3** → sign the ack (D2).  **Q4** → host
  seq-sig (D1), not timestamp-window or replay-cache.  **Q5** → no
  interim star guard, ever.  **Q6** → fold in version byte (D4).
  **Q7** → moot: no timestamp-freshness check anywhere, so no
  clock-skew budget and no `Replay`-backfill exemption hole.

## Slice Shape

- **Name:** Slice B.5 — control-frame & sequence authentication.
- **Protocol crate:** `host_sig` field on `SessionMessage`
  (MESSAGE_FORMAT 2→3); gossip version byte; new domain tags
  `"artel/seq-v1"`, `"artel/ack-v1"`, `"artel/ctrl-v1"`; reshape
  `SessionClosed` and `SendAck` to carry sigs; signing-module helpers
  for the three new canonical-byte layouts (same hand-rolled,
  length-prefixed, domain-separated discipline as `canonical_bytes`).
- **Daemon:** host signs at `publish_message` / `publish_send_ack` /
  `publish_session_closed`; thread host pubkey (+ epoch) into
  `SessionRole::Joiner` and the `on_message` closure; verify in the
  joiner arms *after* dedup. `host_epoch` plumbed through
  `host_session` and the session record.
- **Versions:** `PROTOCOL_VERSION` 5→6; `MESSAGE_FORMAT` 2→3.
- **Tests (per `feedback_extensive_unit_tests`):** forged
  `SessionClosed` dropped; replayed `SessionClosed` across epoch bump
  dropped; forged `SendAck` (Ok and Err) dropped; replayed `Message`
  under a new seq dropped; legit host frames still accepted; round-trip
  encode/decode of each reshaped frame; the persisted-log migration
  rejection.

## Open Questions (for the plan)

- **D3 epoch distribution.** Where does the joiner learn the current
  `host_epoch` to validate a `SessionClosed` against? Options to weigh
  in the plan: (a) carry it in every `Message`/`SendAck` so the joiner
  tracks the latest seen; (b) put it in the ticket and bump → re-issue
  on resume; (c) derive from the highest seq seen (close is only valid
  at epoch ≥ last-seen). This is the one mechanism the brainstorm
  didn't fully close.
- **Migration messaging.** Exact error surface for a pre-cutover
  persisted log (`MESSAGE_FORMAT` 2 on disk) — reuse Slice B's path.
- **`host_sig` for the host's own log entries.** The host authors
  messages too (not just joiner `SendRequest`s); confirm the host
  self-signs `host_sig` on its own `Registry::send` path, not only on
  re-broadcast of joiner sends.

## Cross-references

- Seed: `docs/brainstorms/2026-06-02-control-frame-auth-and-replay-brainstorm.md`
- Parent story: `docs/brainstorms/2026-05-30-auth-story-brainstorm.md`
- Slice B (what shipped): `docs/plans/2026-06-02-auth-slice-b-l3-signing-plan.md`
- Reuse the `signing` module discipline: `crates/artel-protocol/src/signing.rs`
- `feedback_postcard_externally_tagged_enums` — reshaped `GossipBody`
  variants stay externally tagged.

## Next Steps

→ `/workflows:plan` Slice B.5. Resolve the D3 epoch-distribution
mechanism first — it's the only load-bearing detail left.
