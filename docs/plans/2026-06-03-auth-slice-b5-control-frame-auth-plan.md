---
date: 2026-06-03
topic: auth-slice-b5-control-frame-auth
status: PLAN — ready to implement
brainstorm: docs/brainstorms/2026-06-02-control-frame-auth-slice-b5-brainstorm.md
---

# Auth Slice B.5 — control-frame & sequence authentication

Source brainstorm (DECIDED): `docs/brainstorms/2026-06-02-control-frame-auth-slice-b5-brainstorm.md`.
Seed: `docs/brainstorms/2026-06-02-control-frame-auth-and-replay-brainstorm.md`.
Decisions D1–D4 are final; this plan turns them into sliced, testable work
and resolves the open D3 epoch-distribution mechanism against the live code.

Mirrors Slice B's slicing discipline
(`docs/plans/2026-06-02-auth-slice-b-l3-signing-plan.md`): protocol-crate
types first, then daemon signing/verification wiring, then enforcement +
migration. Each sub-slice ends green on `make test` (cargo nextest) and
`make ci-local`, and commits on its own.

## The decided line

The host signs every frame it originates or sequences (`Message`,
`SendAck`, `SessionClosed`); joiners verify host-origin against the host
pubkey they already persist as `session.host` (= `host_peer_id` from the
ticket, set in `materialise_remote_session` at `session.rs:624`).
Topology-independent. Closes findings #1 (replay), #2 (forged
`SessionClosed`), #3 (forged `SendAck`) with one mechanism.

Locked decisions:
- **D1:** host seq-sig is a new persisted `host_sig: SigBytes` field INSIDE
  `SessionMessage`. `MESSAGE_FORMAT` 2→3, pre-cutover logs rejected.
  Canonical bytes `"artel/seq-v1" || session_id || seq || author_sig`.
  Verify in the joiner path AFTER dedup (dedup → author sig → host_sig).
- **D2:** `SendAck` host-signed over `"artel/ack-v1" || session_id ||
  req_id || result_digest`; `result` bound so Ok↔Err can't be flipped.
- **D3:** `SessionClosed` host-signed over `"artel/ctrl-v1" || session_id
  || host_epoch`; `host_epoch` is the freshness element defeating replay
  across same-id host resume. Distributed via a dedicated **signed**
  `EpochBeacon` frame (not an unsigned per-frame field — see resolution
  below). `Message`/`SendAck` carry no epoch.
- **D4:** leading gossip-frame version byte `[version: u8][postcard(body)]`;
  decode rejects unknown versions explicitly.
- `PROTOCOL_VERSION` 5→6. Backwards compat WAIVED (alpha).

---

## D3 resolution (the load-bearing open question)

### What the live code does on host restart / same-id resume (3b-1)

Traced end to end:

- Topic id derives purely from the session UUID (`gossip_bridge.rs::topic_for`).
  A re-host on the same id lands on the **same topic** by construction.
- The host iroh endpoint secret is stable across restart (`~/.artel/iroh.key`),
  so `session.host` is **identical** across incarnations N and N+1. A host
  signature alone cannot distinguish them — exactly the gap D3 names.
- Host resume: `Registry::host(peer, Some(id))` (`session.rs:412-454`)
  re-stamps the ticket and calls `bridge.host_session(id)`. After a process
  restart the bridge `sessions` map is empty, so `subscribe_inner` genuinely
  re-subscribes the same topic.
- **Joiner side — decisive:** an already-joined joiner does **NOT**
  re-`join_session` and does **NOT** re-read the ticket when the host bounces.
  Its `SessionState` (sender + forwarder) and persisted `Remote` mirror
  survive. The mesh observes NeighborDown→NeighborUp (host's new process,
  same EndpointId) and **both are explicitly ignored** by the forwarder
  (`gossip_bridge.rs:522` `Ok(_) => {}`). No resume handshake reaches the
  joiner.

This **kills option (b)** (ticket carries epoch, bump+reissue on resume): a
reissued ticket never reaches the already-joined victim population the
forged-close attack targets.

### Decision: a signed `EpochBeacon`, NOT a per-frame advisory epoch

> **Revised after plan review.** An earlier draft distributed `host_epoch`
> as an *advisory (unsigned-scope) field* on `Message`/`SendAck`. That is a
> correctness bug: `verify_seq` signs `tag || session_id || seq ||
> author_sig` — `host_epoch` is **not** in the signed scope. An attacker
> holds genuine `(seq, author_sig, host_sig)` tuples (every Message is
> broadcast to all topic members), so they can replay a genuine Message on a
> seq the victim hasn't appended yet (wide open during join `Replay` backfill)
> with the advisory epoch tampered to a huge value. It passes `verify_seq`
> (sig genuine, bound to that seq) and dedup (seq unseen), so the closure
> "vouches" for it and the watermark jumps. The real host's later
> `SessionClosed` then fails `host_epoch >= watermark` and is **dropped
> forever** — the defense against forged closes becomes a DoS that suppresses
> *real* closes. Rejected.

Sign `SessionClosed` over `"artel/ctrl-v1" || session_id || host_epoch`.
Distribute the epoch via a **dedicated signed frame**:

- `host_epoch: u64` is sourced from a new `SessionRecord.host_epoch`
  (default 0), bumped by one on each host **re-subscribe of an existing
  local-host record** — in `Registry::host`'s resume branch
  (`session.rs:420-454`), persisted before the ticket is returned and before
  `bridge.host_session`. A fresh create starts at epoch 0. This bump-point
  is precisely the incarnation boundary.
- New frame `GossipBody::EpochBeacon { host_epoch, host_sig }`, where
  `host_sig` is `sign_ctrl(key, session_id, host_epoch)` — **the same
  canonical bytes as `SessionClosed`**, so one verifier serves both. The
  host broadcasts a beacon on every `host_session` resume (best-effort, like
  the join announcement). This is the only frame that moves the watermark.
- The joiner persists a `host_epoch` watermark on its `Remote` mirror
  record (default 0), seeded at `join_session`. It advances the watermark to
  `max(watermark, host_epoch)` **only** on a beacon whose `verify_ctrl`
  passes. A `SessionClosed` is accepted iff `verify_ctrl` passes AND
  `host_epoch >= watermark`.

Why the beacon beats the advisory field on every axis:
- **No DoS surface.** The watermark only ever advances from a host-*signed*
  value; an attacker can't forge a high epoch (no host key), and a replayed
  *old* beacon can't lower a monotonic watermark.
- **Closes most of the residual.** The joiner learns N+1 immediately on
  resume, independent of session activity — it doesn't wait for new traffic.
- **Simpler wire.** `Message` and `SendAck` carry **no** epoch field at all
  (the brainstorm's freshness audit stays true: those frames need no epoch
  for their own replay safety). `Message`'s `GossipBody` shape is unchanged
  (the `host_sig` rides *inside* `SessionMessage`).

### Accepted residual (documented, not coded away)

One narrow window remains: the resume beacon broadcast is **lost** AND an
attacker replays a captured epoch-N `SessionClosed` before any later beacon
or activity reaches the joiner. The only durable effect of a believed close
is a mirror+attachment delete a re-join reconstructs. Tightenable later by
beacon retry or by piggybacking `host_epoch` onto the next signed `Message`
(in the signed scope this time — `"artel/seq-v1"` would grow an epoch
field). **Accept and document** (ADR addendum + threat model). "Must be
right" = honest threat model, not machinery for every sliver.

---

## Version bumps summary

- `PROTOCOL_VERSION` 5 → 6 (`crates/artel-protocol/src/version.rs`). Hard
  inter-daemon gossip-wire cutover (version byte + reshaped frames).
- `MESSAGE_FORMAT` 2 → 3 (`crates/artel-protocol/src/message.rs`) —
  `SessionMessage` gains persisted `host_sig`.
- `Meta::CURRENT_VERSION` 2 → 3 (`crates/artel-daemon/src/store/fs.rs:427`) —
  on-disk record gains `host_sig` per entry + `host_epoch` on the record.
  Reuses Slice B's `version != CURRENT_VERSION` rejection at `load_one`
  (`fs.rs:449-458`) and B3's skip-and-log on `load_all`. No new migration
  code, only the constant bump + a regression test.

---

## Sub-slice B5.1 — protocol crate: types, canonical bytes, version byte

**Goal:** all wire/type changes land in `artel-protocol`, round-trip-tested,
no daemon behavior yet.

### Files & functions

- `crates/artel-protocol/src/message.rs`
  - Add `host_sig: SigBytes` to `SessionMessage` (after `signature`, reuse
    `signature_serde`). Doc: host's sequencing signature over
    `"artel/seq-v1" || session_id || seq || author_sig`, distinct from the
    author `signature`.
  - `SessionMessage::new` gains a trailing `host_sig: SigBytes` param.
    **Blast radius — mechanical sweep, budget for it:** every construction
    site breaks, not just "fixtures." Known sites: `signing.rs::body_matching`,
    `gossip.rs::sample_msg`, and the `session.rs` / `store` test fixtures
    (`session.rs:1448` and the `Registry`/store unit tests). Sweep them in
    this commit; most pass `SIGNATURE_UNSIGNED` for `host_sig`.
  - Bump `MESSAGE_FORMAT` 2 → 3; flip `message_format_constant_is_two`.
  - Raise the postcard compactness ceiling for the extra 64-byte run.
- `crates/artel-protocol/src/signing.rs` — three new domain tags + helpers,
  mirroring `canonical_bytes`/`sign_body`/`verify_message` (hand-rolled,
  big-endian length-prefixed, domain-separated):
  - `SEQ_DOMAIN_TAG = b"artel/seq-v1"`: `seq_canonical_bytes(session_id, seq,
    author_sig)` = tag || session_id(16) || seq.to_be_bytes()(8) ||
    author_sig(64). `sign_seq(key, …)`, `verify_seq(host_pubkey: &PeerId, …,
    host_sig)`.
  - `ACK_DOMAIN_TAG = b"artel/ack-v1"`: tag || session_id(16) ||
    req_id(16) || (1-byte Ok/Err discriminant || postcard(result)). Sign the
    raw concatenation directly (ed25519 hashes internally — no separate
    digest dep). `sign_ack` / `verify_ack`.
  - `CTRL_DOMAIN_TAG = b"artel/ctrl-v1"`: tag || session_id(16) ||
    host_epoch.to_be_bytes()(8). `sign_ctrl` / `verify_ctrl`.
  - These verifiers reuse the existing `VerifyError` (`BadKey`/`BadSig`
    cover host-pubkey verification); only extend `verify_reason` if a new
    variant is genuinely needed.
- `crates/artel-protocol/src/gossip.rs`
  - `SessionClosed` (unit) → `SessionClosed { host_epoch: u64, host_sig:
    SigBytes }`.
  - `SendAck` → `SendAck { req_id, result, host_sig: SigBytes }` (**no**
    epoch field).
  - `Message(SessionMessage)` — **unchanged shape**; the `host_sig` rides
    inside `SessionMessage`. No envelope epoch.
  - New variant `EpochBeacon { host_epoch: u64, host_sig: SigBytes }` —
    host-published on resume; the only frame that advances the joiner's
    watermark. `host_sig` reuses the `"artel/ctrl-v1"` canonical bytes, so
    `verify_ctrl` serves both `EpochBeacon` and `SessionClosed`.
  - All stay externally tagged (`feedback_postcard_externally_tagged_enums`).
  - D4: `encode`/`decode` become `[version: u8][postcard(body)]`; new
    `GOSSIP_WIRE_VERSION: u8 = 1`; `decode` rejects unknown leading byte with
    a new `GossipFrameError::UnsupportedVersion { found, expected }`.

### Tests (B5.1) — `cargo nextest`

- `seq_canonical_bytes_field_offsets` / `ack_*` / `ctrl_*` — pin field order
  via differ-at-known-offset (like `canonical_bytes_includes_session_id_at_a_known_offset`).
- `sign_then_verify_{seq,ack,ctrl}_round_trip`.
- `verify_seq_rejects_wrong_host_key`, `verify_seq_rejects_seq_change`
  (the property finding #1 needs), `verify_ack_rejects_result_flip` (sign
  `Ok(msg)`, verify against `Err`-shaped bytes → BadSig, and reverse),
  `verify_ctrl_rejects_epoch_change`, `verify_ctrl_shared_by_beacon_and_close`
  (a `host_sig` produced for an `EpochBeacon` verifies a `SessionClosed` at
  the same epoch and vice-versa — pins the shared canonical bytes).
- `gossip_frame_has_version_byte`, `decode_rejects_unknown_version`,
  `decode_rejects_empty`.
- Round-trip per reshaped frame (`session_closed`, `send_ack_ok`,
  `send_ack_err`, `epoch_beacon`); `message_frame_round_trips` (shape
  unchanged but now carries `host_sig` inside);
  `session_message_host_sig_round_trips_postcard` + `..._json_as_hex`
  (mirror the existing `signature` field tests).

**Commit:** `protocol: host-sig fields on SessionMessage/SendAck/SessionClosed
+ seq/ack/ctrl canonical bytes + gossip version byte (auth Slice B.5.1,
MESSAGE_FORMAT 3, PROTOCOL_VERSION 6)`

---

## Sub-slice B5.2 — daemon: host signs; epoch sourced & persisted

**Goal:** the host stamps all three host signatures and the epoch; nothing
verifies yet. Ends green because no receiver rejects.

### Files & functions

- `crates/artel-daemon/src/store/record.rs` — `SessionRecord` gains
  `#[serde(default)] host_epoch: u64`; threaded through
  `Session::from_record` / `record` / `new` (default 0); new `host_epoch`
  field on `Session`. **One field, kind-dependent meaning:** on a `Local`
  session it is "this host's incarnation counter" (bumped on resume); on a
  `Remote` mirror it is "highest host epoch verified via beacon" (the
  watermark). Not two fields — the same slot, read/written per `SessionKind`.
- `crates/artel-daemon/src/store/fs.rs` — `Meta` gains `#[serde(default)]
  host_epoch: u64`; bump `Meta::CURRENT_VERSION` 2 → 3. **Fixture audit:**
  any `meta_version_is_two`-style assertion and hardcoded-v2 fixtures
  (`fs.rs:1288`) move in this same commit or B5.2 isn't green.
- `crates/artel-daemon/src/session.rs`
  - **Logged seq-assigning site — exactly one:** `Registry::send`
    (`session.rs:1097`, the `SessionMessage::new` at the prospective seq).
    Reached by both `Authoring::Local` (host's own IPC `Send`) **and**
    `Authoring::Remote` (joiner `SendRequest` re-sequenced by host). Stamp
    `host_sig = sign_seq(self.signing_key, session, prospective, &signature)`
    here for **both** arms — this is where the host self-signs its own
    authored messages too, not only joiner re-broadcasts. Under
    `cfg(not(feature="iroh"))` / no key, stamp `SIGNATURE_UNSIGNED`
    (lit-fuse posture, matching the author path).
    - Note: the `SessionMessage::new` at `session.rs:1229` (`author_remote`)
      is a throwaway `candidate` built only to reuse `verify_message` (seq
      excluded from canonical bytes); it is discarded, so it passes
      `SIGNATURE_UNSIGNED` for `host_sig` and needs no stamping.
  - `Registry::host` resume branch (`session.rs:420-454`): bump
    `s.host_epoch += 1` and persist before returning the ticket / calling
    `bridge.host_session`. Fresh create leaves epoch 0. Add a targeted
    `store.bump_host_epoch(session, epoch)` to avoid rewriting the full
    record. After `bridge.host_session` succeeds, broadcast the beacon (see
    bridge below) so already-joined joiners learn the new epoch immediately.
  - Host close path is `Registry::leave` case 1 (`session.rs:826-842`),
    which calls `bridge.publish_session_closed` — thread the session's
    current `host_epoch` in.
  - `log_since` / replay: persisted messages already carry their `host_sig`
    (D1), so no re-signing on replay. Confirm `run_host_replay` passes them
    through `publish_message` unchanged (now wrapped with current epoch).
- `crates/artel-daemon/src/gossip_bridge.rs`
  - `publish_message` (`:543`): unchanged `GossipBody::Message(message)`
    shape — the `host_sig` is already inside `message`. No epoch param.
  - `publish_send_ack` (`:656`): `host_sig = sign_ack(self.signing_key,
    session, req_id, &result)`; emit `SendAck { req_id, result, host_sig }`.
    No epoch.
  - `publish_session_closed` (`:595`): gains `host_epoch` param; `host_sig =
    sign_ctrl(self.signing_key, session, host_epoch)`; emit `SessionClosed
    { host_epoch, host_sig }`.
  - **New** `publish_epoch_beacon(session, host_epoch)`: `host_sig =
    sign_ctrl(…)`; emit `EpochBeacon { host_epoch, host_sig }`. Best-effort
    (warn on broadcast failure), modeled on `publish_join_announcement`.
    Called from `Registry::host`'s resume branch.
  - Bridge already holds `signing_key: Arc<iroh::SecretKey>` with
    `as_signing_key()` in use (`:405`) — no new plumbing.

### Tests (B5.2)

- Store-unit: `session_record_host_epoch_round_trips`; `meta_version_is_three`;
  `fs_persists_and_reloads_host_epoch`.
- Registry-via-MemoryStore unit (`#[cfg(feature="iroh")]`):
  - `host_send_local_stamps_verifiable_host_sig` — `Local` send produces a
    `host_sig` `verify_seq` accepts under the daemon's own pubkey.
  - `host_send_remote_stamps_host_sig_over_joiner_author_sig`.
  - `resume_bumps_host_epoch_and_persists` (restart via `Registry::load`,
    `host(peer, Some(id))`, assert persisted epoch 1; second resume → 2).
  - `fresh_host_starts_at_epoch_zero`.
  - `resume_broadcasts_epoch_beacon` — assert `publish_epoch_beacon` is
    invoked on resume with the bumped epoch and a `verify_ctrl`-valid sig.

No e2e in B5.2 (no verification fires yet) — matches Slice B's B1 posture.

**Commit:** `daemon: host signs seq/ack/ctrl frames + epoch beacon on resume;
host_epoch sourced, bumped, persisted (auth Slice B.5.2, Meta v3)`

---

## Sub-slice B5.3 — daemon: joiner verifies (after dedup) + enforcement + migration

**Goal:** turn verification on; reject forged/replayed control frames; reject
pre-cutover logs.

### Files & functions

- `crates/artel-daemon/src/gossip_bridge.rs`
  - `SessionRole::Joiner` (`:183`) carries only `on_message` today. Extend to
    `Joiner { on_message, host_pubkey: PeerId, host_epoch_watermark:
    Arc<AtomicU64> }`. `host_pubkey` is the ticket's `host_peer_id`, already
    available at the `join_session` call. The watermark is shared (the
    `EpochBeacon` arm writes it; the `SessionClosed` arm reads it; the
    `on_message` closure persists it to the mirror record).
  - `MessageHandler` stays `Arc<dyn Fn(SessionMessage)>` — **unchanged**.
    With the beacon design, `Message` carries no epoch, so the closure
    doesn't need one. (This reverses the earlier draft's invasive signature
    change; the watermark is moved only by the beacon arm.)
  - `join_session` (`:262`): seed the watermark `AtomicU64` from the
    persisted mirror's `host_epoch`.
  - `handle_inbound_frame` joiner arms:
    - `Message(message)`: `on_message(message)`; `host_sig` verification
      happens inside the closure after dedup (see session.rs). No watermark
      interaction.
    - `EpochBeacon { host_epoch, host_sig }` (**new arm**): `verify_ctrl(
      host_pubkey, session, host_epoch)`; on success advance the watermark to
      `max(watermark, host_epoch)` and persist it to the mirror record; on
      failure drop+warn. This is the **only** site that moves the watermark,
      and it moves only on a host-signed value — no poisoning surface.
    - `SendAck { req_id, result, host_sig }`: `verify_ack(host_pubkey,
      session, req_id, &result)` BEFORE resolving the oneshot; on failure
      drop+warn and do **not** resolve (joiner `send_remote` times out — far
      better than a forged result); on success `pending_sends.remove(req_id)`
      + resolve. (req_id-v4 freshness self-limits replayed genuine acks.)
    - `SessionClosed { host_epoch, host_sig }`: require `verify_ctrl(
      host_pubkey, session, host_epoch)` AND `host_epoch >= watermark`; else
      drop+warn (forged → bad sig; replayed-across-bump → epoch below
      watermark). On success call `host_closed_session`.
  - Host self-receive arms (`:787-796`): update patterns for the reshaped
    variants + the new `EpochBeacon` (host ignores its own beacon round-trip);
    behavior unchanged otherwise.
- `crates/artel-daemon/src/session.rs`
  - `materialise_remote_session::on_message` closure (`:649`): dedup lives at
    `:667`. Per D1, order is **dedup → author sig (`verify_message`, `:681`)
    → host seq-sig**. After the author check passes, add
    `signing::verify_seq(&s.host, session_for_log, msg.seq, &msg.signature,
    &msg.host_sig)`; on failure warn+drop (never touch disk/log). Only after
    both pass: persist/append/emit. **No watermark interaction here** — the
    watermark moves only in the bridge's `EpochBeacon` arm. Host pubkey is
    `session.host` (the `Remote` mirror's `host` = `host_peer_id`).
  - Thread `host_epoch_watermark` + `host_pubkey` from
    `materialise_remote_session` into `join_session`.
- `crates/artel-daemon/src/store/fs.rs` — migration: `Meta::CURRENT_VERSION
  == 3` (from B5.2) already rejects v2 dirs at `load_one` (`:449`). Reuse
  B3's `load_all` skip-and-log so a pre-cutover session is skipped with an
  operator line, not a crash. No new code beyond confirming the path + test.

### Tests (B5.3) — the mandated adversarial set

Store-unit:
- `pre_cutover_v2_meta_skipped_on_load` — plant a v2 meta+log dir; `load_all`
  skips it (others still load), one error log. (Reuses B3's
  `incompatible_schema_skips_session_dir` shape.)

Registry-via-MemoryStore unit (`#[cfg(feature="iroh")]`):
- `mirror_drops_message_with_bad_host_sig`.
- `mirror_accepts_message_with_valid_host_sig`.
- `mirror_drops_replayed_message_under_new_seq` — capture valid `(message,
  host_sig)`, re-feed with bumped `seq` (host_sig now mismatches) → dropped
  by `verify_seq`. This is finding #1, the live attack.
- `beacon_advances_watermark_only_when_host_signed` — a `verify_ctrl`-valid
  beacon advances the watermark; a wrong-key beacon does not.
- `replayed_message_cannot_poison_watermark` — the regression for the review
  blocker: feed a genuine `(message, host_sig)` on an unseen seq; assert the
  watermark is **unchanged** (only beacons move it), so a later legitimate
  `SessionClosed` at the real epoch is still accepted.

E2E (`crates/artel-daemon/tests/auth_b5_control_frames.rs`, new file mirroring
`auth_l3_signing.rs` / `auth_l1_spoofing.rs`, using the `Pair` harness):
- `forged_session_closed_dropped` — non-host topic member broadcasts
  `SessionClosed` with a wrong-key `host_sig`; victim joiner's mirror
  survives.
- `replayed_session_closed_across_epoch_bump_dropped` — host closes at epoch
  N; host resumes (N+1) and broadcasts its `EpochBeacon`; attacker replays
  the captured epoch-N close → dropped (epoch below the beacon-advanced
  watermark). Drives the D3 mechanism end to end.
- `forged_send_ack_ok_dropped` / `forged_send_ack_err_dropped` — racing peer
  publishes `SendAck` (Ok with bogus message; Err) with a bad `host_sig`;
  joiner `send_remote` does not resolve on the forged ack; IPC client never
  sees the spoofed result.
- `replayed_message_under_new_seq_dropped` — over real transport, capture a
  host `Message`, re-broadcast under a fresh seq; joiner appends exactly one.
- `legit_host_frames_accepted` — happy path: host send → mirror appends; host
  ack resolves the joiner send; host close tears the mirror down. All valid.

Tier C (`make test-n0`): at least `legit_host_frames_accepted_n0` and
`forged_session_closed_dropped_n0` so host-pubkey-from-ticket verification
holds over real n0 transport, not just `MemoryLookup`.

**Commit:** `daemon: joiner verifies host seq/ack/ctrl sigs after dedup;
reject forged+replayed control frames; skip pre-cutover logs (auth Slice
B.5.3)`

---

## Docs to update

- `docs/adr/001-*.md` — Updates entry (2026-06-03): Slice B.5; version stamps
  (PROTOCOL_VERSION 5→6, MESSAGE_FORMAT 2→3, Meta 2→3); new domain tags;
  host-pubkey-from-ticket origin auth (topology-independent); the **accepted
  D3 residual** stated plainly with the epoch-beacon named as future
  tightening; link both brainstorms.
- `docs/roadmap.md` — flip the "Control-frame & sequence authentication
  (auth Slice B.5)" bullet to DONE with version stamps + link to this plan;
  mark the deferred "Wire versioning for gossip frames" item DONE (folded in
  via D4).
- `docs/brainstorms/2026-06-02-control-frame-auth-slice-b5-brainstorm.md` —
  status footer linking this plan; note D3 resolved to a signed
  `EpochBeacon` + documented residual.

---

## Risks / sequencing vs Slice C

- **D3 residual** (above) is the one knowingly-open threat-model item: the
  resume beacon is lost AND an attacker replays an old close before any later
  beacon/activity reaches the joiner. Bounded (effect = a re-joinable mirror
  delete); tightenable later via beacon retry or epoch-in-`seq`-scope.
- **Slice C overlap — flag, don't design.** C plans an `originator_pubkey`
  ticket field + `MessageKind::Capability`. B.5's "host pubkey from the
  ticket" primitive (`session.host` = `host_peer_id`) **overlaps**
  `originator_pubkey`. B.5 reuses the existing `host_peer_id` — it does NOT
  add a second ticket field. When C lands, decide whether the two are the
  same field; B.5 leaves `host_peer_id` as the authority and notes the
  overlap so C subsumes rather than duplicates it. C's grant/revoke frames
  will want host-origin auth too — `sign_ctrl`/`verify_ctrl` is directly
  reusable, reinforcing B.5-before-C.
- **Hard cutover.** B.5 bumps `PROTOCOL_VERSION` to 6 and breaks the gossip
  wire. Compat waived; both daemons rebuild together. A mixed-version mesh
  fails cleanly at the version byte (`UnsupportedVersion`) instead of
  mis-decoding.
- **Ack signs over a message carrying its own host_sig.** The `SessionMessage`
  inside `Ok` now contains `host_sig` + `seq`, so the ack sig transitively
  binds them — sound but slightly circular; pin with `verify_ack_rejects_result_flip`.
- **Watermark moves only via the signed `EpochBeacon` arm.** This is the
  load-bearing invariant after the review (an earlier draft moved it from an
  unsigned advisory epoch on `Message`/`SendAck`, which let a replayed
  genuine Message poison the watermark and permanently suppress real closes).
  Keep the watermark write confined to the `EpochBeacon` arm; the
  `replayed_message_cannot_poison_watermark` test guards the regression.

## Next Steps

→ Implement B5.1 → B5.2 → B5.3 in order; `make test` green per sub-slice,
`make ci-local` + `make test-n0` before the final commit. Update docs with
B5.3.
