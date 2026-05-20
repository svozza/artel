# Handoff: post-2c-2c

Written 2026-05-19 right after `8145724` (joiner→host send over gossip).
Updated 2026-05-19 after 2c-2d, 2c-2e, and follow-ups (a) + (c) landed.
This is a temp doc to brief a fresh agent on what's left in artel.
Delete it once the open follow-ups feel sufficiently small to track
in the roadmap directly. The roadmap (`docs/roadmap.md`) remains the
long-form source of truth; this doc supplements it with the
**specific gaps** still open from Phase 2's iroh integration.

## Where we are

Phase 2 is the slice that turns artel from a fancy local IPC bus into
the P2P substrate ADR-001 promises. **Phases 2a–2c-2e plus
follow-ups (a) and (c) are all DONE — Phase 3 is unblocked.** The
remaining open follow-ups (b)/(d)/(e)/(f) are gravy: nice to have,
none blocking artel-fs.

End-to-end working today: two daemons on the same machine cross-seed
addresses, host emits a ticket, joiner imports it, both directions of
`Send` work over iroh-gossip, the joiner's mesh-up fires a
`JoinAnnouncement` so the host's IPC subscribers see `PeerJoined`
proactively, errors round-trip with the host's verdict, remote-mirror
sessions persist their `SessionKind` so they rehydrate as `Remote`
across daemon restarts, `Registry::leave` tears down the bridge's
per-session topic state on host close, the host broadcasts
`SessionClosed` on the way out so joiners drop their mirror and
emit `Event::SessionClosed` proactively, and a fresh joiner asks
the host to `Replay` the existing log so messages sent before the
joiner existed land in the joiner's mirror as Message events.
Clippy + fmt clean both feature modes (`--all-features` and
`--no-default-features`).

Note: the gossip wire no longer carries a version envelope. Pre-1.0
both daemons rebuild together, so an unrecognised body just surfaces
as `GossipFrameError::Malformed` at the bridge. Re-introducing a
versioning story is recorded as a Phase 4 item in the roadmap.

## Engineering principles (always applicable)

These are non-negotiable. Memory has the long form (`MEMORY.md`).
Brief version:

- **Two impls from day one or none.** No single-impl traits.
- **Persistence-first.** Every mutation hits the store before
  in-memory state changes, before fan-out. Store failure ⇒ registry
  stays consistent, client gets a `Storage` error.
- **Tests for every mutation.** Store unit tests + registry-via-
  `MemoryStore` unit tests + e2e via real `Client`. No code lands
  without all three.
- **Postcard wire enums must be externally tagged.** No
  `#[serde(tag, content)]` on wire types — postcard returns
  `WontImplement`.
- **Unix-only.** No Windows-aware code; socket layer is
  `#[cfg(unix)]` and `compile_error!` elsewhere.
- **Headless first-class.** The daemon runs under
  systemd/launchd/nohup. CLI is structured and `--json`-aware where
  meaningful.
- **No speculative abstractions.** Three similar lines is fine.
- **Gossip-only between daemons.** All inter-daemon traffic rides
  iroh-gossip topics. Don't add a direct-QUIC sidechannel even when
  ergonomically simpler — keeps symmetric P2P open as a future
  protocol change, not a transport rewrite.

---

# Slice 2c-2d — `JoinAnnouncement` gossip frame — DONE

Joiner broadcasts a `GossipBody::JoinAnnouncement { peer,
timestamp_ms }` once `subscribe_inner`'s `joined()` resolves. The
host's forwarder routes it to `Registry::ensure_member`, which
admits the peer + persists + emits `PeerJoined`. The lazy-admission
path inside `run_host_send` stays as an idempotent backstop in case
the announcement is lost or arrives out of order.

Tests: `tests/iroh_join_announcement.rs` (Bob joins without
sending; Alice observes `PeerJoined` and her `ListSessions` reports
`peer_count = 2`); wire round-trip unit test.

While we were here: dropped the `GossipFrame { version, body }`
envelope. Pre-1.0 there's nothing to defend against; capability
negotiation will be a real story later (recorded in roadmap §
"Phase 4 and beyond").

---

# Slice 2c-2e — Persist `SessionKind` + bridge teardown on close — DONE

**Persist `SessionKind`.** `SessionRecord` gains a `kind: SessionKind`
field (`Local` / `Remote`); `Session::from_record` reads it (no more
hardcoded `Local`); `Session::record` snapshots it. The `SessionKind`
type moved to `crate::store` so the record and the registry share one
definition. `FsLogStore`'s `meta.json` carries the new field too;
legacy meta files (no `kind`) deserialise to `Local` via
`#[serde(default)]`, which is correct retroactively (pre-2c-2e there
was no way for a remote mirror to reach disk).

**Bridge teardown on close.** `GossipBridge::forget_session(session)`
removes the session's `SessionState` from the bridge's map, aborts
the forwarder task (which drops the `GossipReceiver`), and drops the
`GossipSender`. Per the iroh-gossip 0.98 docs, dropping both halves
is the leave-topic signal. `Registry::leave` calls it on the
host-closes path. Joiner-side mirror cleanup is intentionally not
plumbed yet; it ties together with the `SessionClosed` frame work
(open follow-up (a)) — wiring it now without a way for joiners to
*learn* the host closed would just create a new orphan-mirror
problem.

**Behaviour change in `iroh_joiner_send_rejected.rs`.** With teardown
in place, a joiner's `SendRequest` after the host closes hits
`SendTimeout` (no one is on the topic to ack) instead of the host
serving `UnknownSession` from a still-live bridge. Test updated to
match. Restoring the specific error shape is the job of follow-up (a).

Tests added: store unit (memory + fs round-trip kind), legacy-meta
migration test in fs, registry-level rehydrate-as-Remote unit test
that asserts `Send` takes the remote-routing path.

---

# Open follow-ups (not yet sliced, in rough priority)

These are real gaps but each needs a design conversation before
slicing. Listed in the order I'd tackle them.

## (a) `GossipBody::SessionClosed` frame — DONE

`GossipBody::SessionClosed` (no payload) is broadcast from the
host's `Registry::leave` just before `forget_session` tears the
topic down. Joiner-side, the bridge forwarder calls
`Registry::host_closed_session`, which deletes the persisted
record, drops the in-memory mirror, emits `Event::SessionClosed`
to local IPC subscribers, and tears down the joiner's own bridge
entry. Idempotent (defends against duplicate broadcasts) and
guards against the wrong session kind (a SessionClosed for a
Local session is logged + ignored).

Tests: `tests/iroh_session_closed.rs` (Alice closes; Bob's events
stream surfaces `Event::SessionClosed`; `ListSessions` no longer
reports the session). `tests/iroh_joiner_send_rejected.rs`
restored to assert the specific `UnknownSession` shape, gated on
the joiner first observing the close event so the test isn't
racy.

## (b) Reconnect after host restart / network blip

Today: if the host's daemon goes down and comes back up (same
peer-id, persistent iroh secret key), can a joiner that had already
dialled them recover? The gossip mesh probably re-forms because the
addr book on both sides still has the right entries, but the
joiner's `pending_sends` for any in-flight request will time out
during the gap.

Need a deliberate test:

- `iroh_host_restart.rs`: daemon A hosts, B joins, B sends and gets
  ack; **stop daemon A, restart with same state dir**; B sends a
  second message; B should observe it after a re-mesh.

If this works out of the box, document it as supported. If it
doesn't, slice the fix.

## (c) `Subscribe { since: Some(N) }` over gossip — DONE

`GossipBody::Replay { since: Seq }` (joiner → host) plus the
existing `Message` frame (host → joiner) — no separate
`ReplayChunk`, since the host just re-broadcasts existing
messages and the joiner's mirror dedups by seq. Wire shape ended
up simpler than the original sketch: no req_id correlation needed
(every Message carries its own seq), no new chunked-transfer
machinery.

`bridge.join_session` publishes `Replay { since: ZERO }` right
after the JoinAnnouncement, so a fresh mirror is backfilled
immediately. Host-side `run_host_replay` calls
`Registry::log_since` and pipes each entry through the existing
`publish_message` path. Joiner-side `on_message` switched from
"strictly-monotonic head + drop" to "insert by seq, skip
duplicates" so live and replay traffic can interleave safely.

Tests: `tests/iroh_subscribe_replay.rs` (Alice sends 3 messages
*before* Bob joins, plus a 4th live; Bob's events stream surfaces
all 4). Wire round-trip + `log_since` registry units.

## (d) NAT-traversal failure surface

Roadmap § "Big unknowns" calls this out: today, a joiner whose
ticket points at an unreachable host hangs in `subscribe_inner` for
15s before surfacing as `BridgeError::Iroh("timed out waiting for
gossip neighbor")`. That's adequate but generic. iroh has structured
errors for "no relay reachable" vs "direct addrs failed"; surface
those.

Defer until someone hits it in practice.

## (e) Heartbeat / liveness

Long-running joiners need to know if the host has gone away
quietly. iroh-gossip's NeighborDown is the signal; today we ignore
it. Wire it through to a `Registry::host_unreachable(session_id)`
hook that emits `Event::HostUnreachable` to local subscribers.

## (f) Auth & capabilities

ADR-001 § "Auth and capability model" — explicitly deferred. The
v1 trust model is "anyone with the ticket bytes is the joiner they
claim to be." Phase 4. Don't pre-empt.

---

# Phase 3: artel-fs (medium-large)

Already documented in the roadmap, and the Phase-2 prerequisites
all landed (subscribe replay was the last). The roadmap text at
`docs/roadmap.md` § "Phase 3" is the spec; nothing to add here.

---

# Phase 4 and beyond

Roadmap is the authoritative listing. None of this is scoped yet.

---

# How to start

1. Read `docs/roadmap.md` § "Phase 2: iroh integration (multi-slice)"
   from the most-recent DONE slice backwards for context.
2. Read this doc for the open follow-ups.
3. Phase 3 (artel-fs) is unblocked. Start there if you're picking
   up new work — see `docs/roadmap.md` § "Phase 3".
4. (b)/(d)/(e) are open follow-ups, none blocking. Pick them up
   opportunistically or when a real user hits the rough edge.

When in doubt: small slices, tests at every layer.
