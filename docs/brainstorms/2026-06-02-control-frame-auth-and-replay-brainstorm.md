---
date: 2026-06-02
topic: control-frame-auth-and-replay
status: SEED — input for a brainstorming session, not a committed plan
---

# Control-frame authentication & message-replay hardening

## Provenance

This doc is the residue of a code review of the auth Slice B (L3
per-message signing) landing — commits `21fac44..98d6b3d` on branch
`emdash/stable-id-jx4uy`. The review fixed seven issues in-place
(see "What already shipped" below). Three findings were **not**
fixable without a gossip-wire-format change, so they were carved out
here as a follow-up slice. This is a seed for that design pass, not a
finished plan — it deliberately over-documents the attack surface,
the transport constraints, and the option space.

The companion artifacts:
- `docs/brainstorms/2026-05-30-auth-story-brainstorm.md` — the v1 auth
  story (L1 / L2 / L3). This doc is a **sub-slice between B and C**, or
  a fast-follow on B.
- `docs/plans/2026-06-02-auth-slice-b-l3-signing-plan.md` — what B
  actually built.

## What already shipped (the review fixes — context, not scope here)

These are DONE; listed so the brainstorm doesn't re-litigate them:

1. **`verify_strict`** — `signing::verify_message` now uses ed25519
   strict verification, closing signature malleability (a captured sig
   can no longer be reshaped into a second valid 64-byte blob).
2. **Version floor** — `verify_message` rejects `version < MESSAGE_FORMAT`
   (`VerifyError::VersionTooOld`). The "downgrade floor" the doc claimed
   is now an actual check, not just a byte folded into canonical bytes.
3. **No-iroh log loss** — `read_log` now verifies signatures only when
   the build signs on write (`cfg!(feature = "iroh")`); a no-iroh daemon
   no longer drops its own `SIGNATURE_UNSIGNED` log on restart.
4. **Single authoritative verify** — removed the redundant bridge-side
   `verify_message` in `run_host_send`; `Registry::send`'s `Remote` arm
   is the one verify. Bridge still logs the rejection with `delivered_from`
   context.
5. **Dedup-before-verify** — the joiner mirror `on_message` callback
   now does the seq dedup check before the ed25519 verify, so duplicate
   gossip deliveries / replay re-broadcasts don't pay verification cost.
6. **`signing_key` Option doc** — corrected the field doc to describe
   the real sentinel-as-lit-fuse behaviour (it does not panic; it
   stamps `SIGNATURE_UNSIGNED`, which every verifier rejects loudly).
7. Shared `signing::verify_reason()` helper de-dups the
   `VerifyError → &str` mapping across the two daemon rejection sites.

**Still open (this doc's scope):** #1 replay, #2 forged `SessionClosed`,
#3 forged `SendAck`.

---

## The load-bearing transport fact

iroh-gossip's `delivered_from` is **NOT the origin of a frame** — it is
"the peer before us in the gossiping path" (iroh-gossip 0.99
`proto/plumtree.rs`, `api.rs::Message::delivered_from`: *"The endpoint
that delivered the message. This is not the same as the original
author."*).

Consequences that shape every option below:

- The host-side L1 check `drop_if_spoofed` (`peer.id == delivered_from`,
  in `gossip_bridge.rs`) is **only sound under a star topology** — where
  every joiner is a direct gossip neighbour of the host, so the relay
  hop and the origin coincide. We run a star today (`join_session`
  bootstraps joiners *only* off `host_endpoint_id`; the host subscribes
  with an empty bootstrap and waits). The check is correct now and
  silently becomes unsound the moment the mesh becomes multi-hop
  (more joiners, relay fan-out, or symmetric P2P).
- Therefore the "cheap" symmetric fix for the joiner side
  (`delivered_from == host_endpoint_id` on joiner-role arms) is **also
  only star-sound**. It works today, but it is the same latent
  assumption, and it would wrongly drop a host frame relayed
  joiner→joiner in a dense mesh.
- The topology-independent answer is **cryptographic**: authenticate
  the *origin* of a frame by a signature the relay can't forge, not by
  who handed us the bytes.

This is why these three items need a wire change and the other seven
didn't.

---

## The three open findings

### #1 — Message replay (seq is outside the signed scope)

**Mechanism.** `signing::canonical_bytes` deliberately excludes `seq`
(the host assigns it; the joiner signs before the host stamps it — see
the signing.rs module doc). The joiner mirror dedups inbound `Message`
frames *by seq* (`session.rs` `on_message`: `partition_point` on
`m.seq`). So a party who can re-publish a captured, validly-signed
`SessionMessage` under a *fresh* seq produces a duplicate that:
- passes `verify_message` (the signature covers everything except seq,
  and is genuine), and
- is NOT caught by the seq-dedup (the seq is new),

so it appends a second time, cryptographically attributable to the
original author. `timestamp_ms` IS in the signed scope but is **never
checked for freshness** anywhere (confirmed: `now_ms()` is only read at
sign time in `session.rs` / `gossip_bridge.rs`, never compared at
verify).

**Who can actually do it today.** Only the **host** re-broadcasts
`Message` frames (host `Registry::send` → `publish_message`; the host
also re-emits the whole log on `Replay`). Under the star topology a
joiner only ever receives `Message` frames *from the host*. So the
practical replay actor today is the host itself — which is **already a
trusted sequencer** in the v1 model:
- `2026-05-30-auth-story-brainstorm.md` § Out of scope: *"Host rewrites
  seq order to censor / reorder → Symmetric P2P (long-term)."*
- So **#1 reduces to an already-accepted v1 trust assumption** in the
  current topology. It becomes a novel, untrusted-party attack under
  multi-hop relay / symmetric P2P, or if any non-host code path is ever
  allowed to inject `Message` frames into a joiner mirror.

**Severity:** Low *today* (folds into accepted host-trust), Medium-High
*post-symmetry*. Worth fixing proactively because the fix is cheap and
the wire cutover window is now (pre-1.0).

**Fix options (cheapest → most thorough):**
- **(1a) Timestamp freshness window.** On the joiner-mirror receive
  path, reject `Message` whose signed `timestamp_ms` is outside
  `[now - skew, now + skew]`. Cheap, no wire change (timestamp already
  signed). *Limits* replay to a sliding window rather than eliminating
  it; needs a clock-skew budget; interacts badly with `Replay`
  backfill of legitimately-old messages (would have to exempt the
  replay path, which re-opens the hole). **Probably not sufficient
  alone.**
- **(1b) Bring `seq` into the signed scope.** Kills replay-under-new-seq
  outright: a different seq ⇒ different canonical bytes ⇒ BadSig. But
  it breaks the *reason* seq was excluded — the joiner signs before the
  host assigns seq. Would require either (i) the joiner pre-declares a
  client-local monotonic counter that the host preserves (two counters:
  author-local + host-global), or (ii) the host signs a counter-signature
  over `(body_sig, seq)` so the seq binding comes from the host's key.
  Option (ii) is essentially the same machinery as #2/#3 (host-signed
  envelopes) — **strong synergy, see "Unifying idea" below.**
- **(1c) Receiver-side replay cache.** Track seen `(peer.id,
  author_nonce)` and reject repeats. Requires adding an author nonce to
  the signed scope (wire change) and bounded-memory cache eviction.
  Heavier; only justified if 1a/1b don't fit.

### #2 — Forged `GossipBody::SessionClosed` (intra-session DoS)

**Mechanism.** The joiner-role `SessionClosed` arm in
`handle_inbound_frame` is **fully unauthenticated**: no `peer` field, no
signature, not subjected to `drop_if_spoofed`. It calls
`registry.host_closed_session(session)`, which deletes the persisted
mirror, **cascades attachment deletes**, emits `Event::SessionClosed`,
and tears down bridge state. `host_closed_session` only checks
`kind == Remote` and is idempotent — it never confirms the sender is
the host.

**Impact.** Any party on the session's gossip topic (any ticket-holder
who joined) can broadcast one forged `SessionClosed` and evict **every
other joiner's** mirror, with on-disk side effects (lost mirror log +
attachments), until the victim re-joins. The real host stays up,
unaware. This is **not** covered by the v1 trust model — the attacker
is a non-host topic member, a class the model is supposed to defend
(L1's whole point is "a joiner can't act as another identity").

**Severity:** Medium-High. One frame, no key material needed, durable
side effects, affects all other joiners.

### #3 — Forged `GossipBody::SendAck` (send-result spoofing)

**Mechanism.** The joiner-role `SendAck` arm carries `{ req_id, result }`
with no `peer`/signature and no `delivered_from` check. The `Ok(_)`
body's `SessionMessage` is never run through `verify_message`. It
resolves the joiner's pending `oneshot` keyed by `req_id`.

**Impact.** A topic member who races the host (or just guesses/sniffs
`req_id` — it's a v4 UUID broadcast in the `SendRequest`) can:
- forge `Ok(bogus_message)` → joiner's IPC client gets a fake success +
  bogus `seq` for a send the host may have rejected or never committed;
- forge `Err(..)` → joiner's IPC client sees a legitimately-committed
  send as failed → user/consumer retries → duplicate message.

It does **not** corrupt the joiner's log (that's populated by the
verified `Message` re-broadcast path, not the ack), so this is a
confused-client / result-integrity bug, not log corruption. Still: the
`SendAck` is the IPC client's source of truth for "did my send land."

**Severity:** Medium. Needs a race (or req_id capture), bounded blast
radius (the one in-flight send), but undermines the send-result
contract.

---

## Unifying idea: host-signed control + ack frames

#2 and #3 (and #1 option 1b-ii) all want the same primitive: **a frame
whose host-origin is provable by signature, independent of who relayed
it.** Proposal sketch:

- Add a host signature to the control/ack frames the joiner acts on:
  - `SessionClosed { session_id, timestamp_ms, sig }` — `sig` over a
    domain-separated canonical encoding (reuse the `signing` module's
    pattern: `"artel/ctrl-v1" || session_id || frame_tag || timestamp`).
  - `SendAck { req_id, result, sig }` — `sig` over
    `"artel/ack-v1" || session_id || req_id || result_digest`.
- Joiner-role arms verify `sig` against the **host's `peer.id`** (known
  from the join ticket — `host_peer_id`, already carried and validated
  in `materialise_remote_session`). This is origin-authentication that
  is **topology-independent**: it does not care about `delivered_from`,
  so it survives the move to a multi-hop mesh / symmetric P2P.
- For #1: have the host counter-sign the sequenced `Message`
  (`"artel/seq-v1" || session_id || seq || body_sig`). The joiner then
  verifies *both* the author sig (authorship) and the host seq-sig
  (this seq, from this host) — replay under a new seq fails the
  host's seq-sig. This is the clean version of 1b-ii and it reuses the
  same host-signing path as #2/#3.

If we do the host-signs-`Message` counter-signature, #1/#2/#3 collapse
into **one mechanism**: "the host signs everything it originates or
sequences; joiners verify host-origin by the host's pubkey from the
ticket." That's the altitude-correct framing — a single new capability
rather than three point-patches.

### Why not the cheap star-topology guard?

Considered and rejected as the *primary* fix: add
`if delivered_from != host_endpoint_id { drop }` to the joiner arms.
- **Pro:** no wire change, closes #2/#3 in practice *today*, ~10 lines.
- **Con:** sound **only** while the star topology holds (see "load-bearing
  transport fact"). It bakes the star assumption deeper into the code
  and would silently fail-open... actually fail-*closed* (drop valid
  frames) when the mesh densifies — a latent correctness bomb. If we
  ever want it as an interim mitigation, it MUST be gated behind an
  explicit asserted invariant ("joiners bootstrap only off the host")
  and a roadmap note, so the move to multi-hop trips a test rather than
  mysteriously dropping frames.
- **Decision for the seed:** prefer the signed-frame approach as the
  real fix; keep the star guard in the back pocket as a labelled interim
  only if shipping the wire change has to wait.

---

## Wire-format / migration considerations

- **`PROTOCOL_VERSION`** is at `5` (post auth-L1-fix3). Control-frame
  signing bumps the **gossip body** shape. Note the gossip frames
  currently have **no version envelope** (roadmap § "Wire versioning
  for gossip frames" — deliberately removed pre-1.0; an unrecognised
  body surfaces as `GossipFrameError::Malformed`). Adding signed
  control frames is a natural moment to re-introduce a leading version
  byte (roadmap calls it "~30 lines plus tests, right time is the v1
  cutover").
- **`MESSAGE_FORMAT`** (currently `2`) only needs a bump if the
  host-counter-signs-`Message` option (#1 via 1b-ii) changes the
  on-the-wire `SessionMessage` shape (e.g. a second `host_sig` field).
  If the host seq-sig rides on the gossip `Message` envelope rather than
  inside `SessionMessage`, the persisted log format can stay at `2`.
  **Open question — see below.**
- **Persisted log migration.** If `SessionMessage` gains a field, the
  on-disk log frames change shape. Slice B already established the
  precedent ("fresh sessions only post-cutover; pre-cutover sessions
  rejected with a clear error" given the alpha). Same story applies.
- **Sequencing vs Slice C.** Slice C (L2 caps) plans new ticket fields
  (`ticket_id`, `originator_pubkey`) and `MessageKind::Capability`. The
  host-pubkey-from-ticket primitive this doc needs **overlaps** with
  C's `originator_pubkey`. Decide whether control-frame signing lands
  as **Slice B.5** (before C, since C's grant/revoke frames will also
  want host-origin authentication) or is folded into C's ticket rework.
  Leaning B.5 — it's a security fix for a shipped slice, C is a feature.

---

## Open questions for the brainstorm

1. **Does host-counter-signing `Message` belong inside `SessionMessage`
   (persisted, MESSAGE_FORMAT bump, replay-safe across restart) or on
   the gossip `Message` envelope only (transient, no log migration, but
   the seq-binding is lost on disk and must be re-derived on replay)?**
   The persisted choice is cleaner for #1 but heavier.
2. **Is #1 worth fixing now at all, or do we explicitly accept it under
   host-trust until symmetric P2P and only fix #2/#3?** The threat model
   already defers host-reorder; #1 is arguably the same class. Counter:
   the fix is nearly free if we're already adding host-signed frames.
3. **`SendAck` integrity vs. just authenticating it.** Do we need the
   host to sign the ack, or is it enough to bind `req_id` unforgeably
   (e.g. derive `req_id` from a joiner secret + nonce so a racing peer
   can't resolve someone else's pending send)? Signing is simpler and
   reuses the host-pubkey primitive.
4. **Replay cache vs seq-in-scope vs timestamp window for #1** — pick one
   (see 1a/1b/1c). 1b-ii (host seq-sig) is the recommended seed.
5. **Interim star guard: ship it or not?** If the signed-frame slice
   can't land immediately, do we want the `delivered_from == host`
   joiner-arm guard as a labelled, test-asserted interim mitigation for
   #2/#3? Or accept the exposure until the real fix?
6. **Gossip frame version envelope** — fold the roadmap's deferred
   "leading version byte for gossip frames" into this slice, since we're
   touching the gossip wire anyway?
7. **Clock-skew budget** if any timestamp-freshness check is used
   anywhere — what's the tolerance, and where does wall-clock come from
   on headless daemons?

## Suggested slice shape (straw man — to be torn apart)

- **Name:** Slice B.5 — control-frame & sequence authentication.
- **Wire:** re-introduce a gossip frame version byte; add `sig` to
  `SessionClosed` and `SendAck`; host counter-signs sequenced `Message`
  (decision Q1 picks where the seq-sig lives).
- **Verify:** joiner-role arms verify host-origin sigs against
  `host_peer_id` from the ticket (topology-independent — kills the
  star-topology dependence of `drop_if_spoofed` on the joiner side).
- **Domain tags:** `"artel/ctrl-v1"`, `"artel/ack-v1"`, `"artel/seq-v1"`
  — same hand-rolled, length-prefixed, domain-separated discipline as
  `signing::canonical_bytes`.
- **Tests:** forged `SessionClosed` dropped; forged `SendAck` (Ok and
  Err) dropped; replayed `Message` under a new seq dropped; legitimate
  host frames still accepted; (if star guard interim) a test asserting
  the star-bootstrap invariant.
- **Versions:** `PROTOCOL_VERSION` 5→6; `MESSAGE_FORMAT` 2→3 *iff* Q1
  picks the persisted-counter-sig option.
- **Docs:** ADR-001 addendum; roadmap entry flip; this brainstorm →
  a `docs/plans/` plan.
