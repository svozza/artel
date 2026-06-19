# Seed: re-deliver the RW secret to a member returning AFTER a rotation

date: 2026-06-18
status: RESOLVED 2026-06-19 — designed via grill-with-docs and SHIPPED. See
         `docs/plans/2026-06-18-rw-redelivery.md` for the final design and
         `crates/artel-fs/tests/rw_redelivery.rs` for the real-n0 regressions.
         This seed is kept for the reasoning trail (and the corrected
         understanding that invalidated the first `PeerReannounced` attempt).
         NOTE: the plan extended this seed in one material way the seed missed —
         the rotated *read/write ticket* has the same reload re-delivery gap as
         the secret, so the fix re-delivers BOTH on the returning peer's
         `NODE_ID` announce, not just the secret. See the plan's
         "Implementation discovery" block.
relates-to:
  - ADR-003 (daemon stays namespace-agnostic)
  - CONTEXT.md "namespace_epoch", "Namespace rotation"
  - C1 (refresh durable distribution state on rotation) — committed on
    `fix/tier1-revocation-blockers`
  - `emit_upgrade` INVARIANT comment in `crates/artel-daemon/src/session.rs`
  - docs/handoff-m3-subscriber-lag-recovery.md (Gap/EOF recovery)

## Why this doc exists

A chat-harness reconnect smoke test surfaced "a returning RW peer comes back
read-only." We grilled the design, started building a fix (`PeerReannounced`),
and the grilling **invalidated two premises**. We reverted the substrate work
(it was never committed) and wrote this so the next agent starts from the
*corrected* understanding rather than re-walking the dead ends.

## What is NOT broken (verified empirically — don't re-investigate)

1. **Plain reconnect, no rotation: fully works.** iroh-docs capability `merge`
   is **monotonic** (`iroh_docs::sync::Capability::merge`, sync.rs:241 — only
   ever upgrades Read→Write, never downgrades). A joiner's workspace replica
   (`<workspace>/.artel-fs`) persists across a daemon restart, so a returning
   RW peer reloads its Write secret from disk; re-importing the Read ticket is
   a no-op merge. **Write capability survives a plain restart.**

2. **Restarted-joiner live sync resyncs both ways.** Verified with a throwaway
   probe test (`probe_joiner_daemon_restart_resyncs_both_ways`, since removed):
   bob RW → daemon restart at same paths → rejoin → alice→bob AND bob→alice
   both resume within seconds. So joiner-daemon-restart is NOT broken. (There
   was no prior test for this — the one "joiner restart" test,
   `alice_post_restart_writes_reach_bob`, actually restarts the *host* while
   bob stays up. This gap in coverage is worth a permanent test regardless.)

3. **The harness symptom you actually saw is a HARNESS-UI bug**, not substrate:
   the chat-harness sets `can_write = is_host` on startup and gates *sends* on
   that flag, so a returning joiner renders read-only and refuses to send even
   though the substrate would accept its write. Re-granting "fixed" it only
   because the grant emitted a fresh `UPGRADE_ACTION` that flipped the flag.
   **This is fixed separately, harness-only** (see "Harness fix" below).

## What IS broken (the real, narrow gap)

A member that is **offline across a namespace rotation** loses write and does
not recover it on rejoin:

  - bob holds RW on the genesis namespace; his replica has that secret on disk.
  - while bob is offline, the host evicts some *other* peer → **rotation** →
    new namespace, new secret (C1 re-mints + re-publishes the read envelope and
    the upgrade-secret cell to *currently-present* survivors).
  - bob rejoins. He gets the new *read* envelope (can read/sync the new
    namespace) but his persisted secret is for the **abandoned** namespace —
    worthless. Nothing re-delivers the *new* secret to him. He is read-only at
    the substrate until a manual re-grant.

This is the "RW peer offline across multiple/any rotations" item the original
revocation plan explicitly deferred (see the plan's "Deferred" section).

## The abandoned first attempt (why `PeerReannounced` did NOT work)

Design tried: new live-only `Event::PeerReannounced`, emitted by the daemon in
`ensure_member`'s already-member branch, consumed by the host fs cap-listener
to re-deliver the current secret (gated on `has_rw`, reading the live
`upgrade_secret` cell so post-rotation it's the current secret).

**It failed in test, for a structural reason:** a restarted joiner's daemon
**reloads** the remote-mirror session from disk (`Registry::load` rehydrates
records; `load_rehydrates_remote_session_with_remote_kind` test). So on rejoin,
`Registry::join` finds the session already present and takes the **self-rejoin
early-return** (session.rs ~1189-1196) — it never calls
`materialise_remote_session`, which is the *only* thing that broadcasts a fresh
gossip `JoinAnnouncement`. No announcement → the host's `ensure_member` never
runs → `PeerReannounced` never fires. (Yet sync still resumes — see "open
question" — so the reconnect re-subscribe happens on some *other* path that the
trigger must hook instead.)

Net: the trigger was hung off the wrong event. `ensure_member` fires on a
*fresh* gossip announce, not on a reloaded-session rejoin.

## Open questions for the deep-dive

1. **What path actually re-establishes a reloaded joiner's gossip + live sync?**
   The probe proves it happens, but `Registry::join` early-returns before any
   `bridge.join_session`. Find the real re-subscribe trigger (candidates: the
   `Workspace` cap-listener's `Subscribe`; some `host_session`-equivalent on the
   joiner side; an iroh-gossip `NeighborUp` re-mesh). **That trigger is where a
   secret re-delivery signal belongs**, not `ensure_member`.
   - Note `Registry::host` (Local resume) DOES re-subscribe via
     `bridge.host_session(id)` (session.rs ~930) + an `EpochBeacon`. The joiner
     (Remote) side appears to have no symmetric re-subscribe in `join` — confirm
     whether that's a latent joiner-reconnect gap or whether resync rides a
     different mechanism entirely.
2. **Where should secret re-delivery be driven from, given ADR-003?** The daemon
   can't hold the secret. Candidates (from the first attempt, still valid):
   - host-side idempotent re-delivery sweep over `rw_peers_except_host` (timer
     or on the host's own cap-listener reconnect);
   - joiner-side pull: a returning peer that detects epoch advanced (its
     persisted secret is stale) asks the host to re-deliver — but the joiner
     can't project caps in v1 (host-private), so it can't know it "should" be
     RW. Tier-2 P2P cap projection may make this natural.
   - whatever it is, it must read the live `upgrade_secret` cell
     (`RotationDistributeCtx.upgrade_secret` / `HostUpgradeCtx.namespace_secret`,
     the `Arc<Mutex<[u8;32]>>` from C1), never a captured copy.
3. **Detection: how does a returning peer (or the host) know re-delivery is
   needed?** The `namespace_epoch` in the ticket envelope is the obvious signal
   — a returning peer on a stale epoch. But the secret is delivered out-of-band
   from the envelope; reconciling the two is the design crux.
4. **Is this even worth solving before Tier-2?** A returning-across-rotation RW
   peer is recoverable today with a manual re-grant. If that's acceptable
   operationally until Tier-2 lands joiner-side cap projection, this may just be
   a documented limitation, not a v1 fix.

## Test shape for the eventual fix

- Honest regression: bob RW → offline → host evicts a *different* peer
  (rotation) while bob is down → bob's daemon restarts (persistent iroh.key) +
  rejoins → assert bob's post-rejoin write reaches the host on the rotated
  namespace, no re-grant. (The first attempt's
  `returning_rw_member_offline_across_rotation_regains_write` test was correct
  in *shape* and correctly FAILED — keep that structure; it's the real guard.)
- Also worth landing regardless: a permanent
  `joiner_daemon_restart_resyncs_both_ways` test (no rotation) — currently
  uncovered, and it's the thing that proves reconnect works at all.

## Harness fix (separate, in-progress — NOT the substrate gap)

The chat-harness (`examples/chat-harness`, gitignored) renders a returning
member read-only because `can_write` resets to `is_host` on startup. With the
substrate as-is (no rotation), the member CAN actually write — the flag is just
wrong. Options: have the harness probe its real write capability on startup, or
treat the absence of an `UPGRADE_ACTION` as unknown rather than read-only. This
does not require any substrate change and is tracked with the other harness
smoke-test polish.
