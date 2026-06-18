# Plan: re-deliver the RW secret to a member returning after a rotation

date: 2026-06-18
status: PLAN — designed via grill-with-docs; ready to implement.
supersedes: docs/brainstorms/2026-06-18-rw-redelivery-robust-guarantee-seed.md
relates-to:
  - ADR-003 (daemon stays namespace-agnostic)
  - CONTEXT.md "namespace_epoch", "Namespace rotation"
  - C1 (refresh durable distribution state on rotation) — `fix/tier1-revocation-blockers`
  - `emit_upgrade` INVARIANT comment (`crates/artel-daemon/src/session.rs`)

## Problem (corrected diagnosis)

A member that is **offline across a namespace rotation** loses write and does
not recover it on rejoin. The seed doc framed this as "secret re-delivery hung
off the wrong event (`PeerReannounced`)." Grilling the code corrected that into
**two independent defects on two sync planes**, where the secret gap is
*downstream* of a presence gap:

- **Plane (b), iroh-docs file sync — already self-heals.** On reload,
  `Workspace::join_with` re-`Subscribe`s; the daemon replays the *current*
  persisted `workspace_ticket` (`session.rs:2433`); the joiner
  `import_and_subscribe`s the **rotated read namespace** (`workspace.rs:1128`).
  Reads recover with no change. (This is what the seed doc's "resyncs both ways"
  probe was observing — hence the confusion.)

- **Plane (a), daemon gossip + host→peer unicast — does NOT self-heal.** A
  reloaded `Remote` mirror re-subscribes **nothing**: `Registry::load`
  rehydrates records but never re-drives `join_session`, and the Remote side has
  no symmetric resume (contrast `Registry::host`, which re-subscribes on resume
  at `session.rs:932`). So the joiner's `send_remote` fails with
  `BridgeError::UnknownSession` (`session.rs:2059`). And because the RW secret
  is delivered as a **live-only, non-replayable** broadcast (`emit_upgrade`
  INVARIANT, `session.rs:2539`), nothing re-delivers it after the miss.

Net: the returning peer gets the new *read* envelope (can read/sync the rotated
namespace) but its persisted write secret is for the abandoned namespace, and
the only re-delivery trigger (`Event::PeerJoined` on a *fresh* gossip announce)
never fires for a reloaded mirror.

## Why the first attempt (`PeerReannounced`) failed

It emitted a new event from `ensure_member`'s already-member branch, consumed by
the host cap-listener. But `ensure_member` only runs on a fresh gossip
`JoinAnnouncement`, which a reloaded joiner never publishes (`Registry::join`
takes the self-rejoin early-return, `session.rs:1189`; and the consumer calls
only `Subscribe`, never `JoinSession`, on reload). The event was correct in
spirit (re-deliver on the host side, reading the live C1 secret); the trigger
upstream of it structurally never fired. **`PeerReannounced` stays retired** —
the fix below uses an existing signal instead.

## Design decisions (resolved during grill)

- **Initiator = host-push (B), not joiner-pull.** Detection of "needs
  re-delivery" belongs where the *authority* lives — host-side `peer_map.has_rw`
  is the cap projection. This also subsumes **offline read→write promotion** (a
  peer promoted while offline holds no prior secret, so a joiner-side
  "is my secret stale?" check has nothing to compare) — exactly the shoe the
  `emit_upgrade` INVARIANT predicts. One mechanism covers both.
- **No persisted claim material (b).** The signed cap-claim
  (`ticket_id/granted_cap/expiry_ms/cap_sig`) is NOT persisted in
  `SessionRecord` and we keep it that way — no credential sitting in a sidecar
  that could drive a re-announce after revocation.
- **Announce-less re-subscribe (6b).** Restoring gossip presence does NOT
  re-run admission: the returning joiner is already a durable member host-side
  (`members` is in `SessionRecord`, rehydrated by `load`), and the host send
  path gates on that durable set (`run_host_send` → `Registry::send`'s
  `NotMember` check at `session.rs:2022`). So a re-subscribe can skip
  `publish_join_announcement` and the joiner's sends still pass.
- **Lazy re-subscribe (7c), not an eager startup sweep.** Secret recovery is
  consumer-gated on `join_with`/`NODE_ID` regardless, so bind presence-restore
  to the exact moment of need: the `send_remote` `UnknownSession` site. No
  startup sweep, no subscriptions for sessions never reattached. Eventual
  consistency is acceptable (user-confirmed).

## The fix — two parts

### Part 1 (daemon): lazy gossip re-subscribe for a reloaded Remote mirror

- New bridge primitive `GossipBridge::resubscribe_session(session, host_peer)`:
  `subscribe_inner(Joiner role)` + `publish_replay(Seq::ZERO)`, **without**
  `publish_join_announcement`. Bootstrap from the persisted `host` EndpointId
  only (no `host_addr` is persisted — pkarr/DNS resolves the host's real addr;
  slower than the ticket-hint path but correct, and the host is long-lived).
  - The Joiner role needs `on_message` (mirror-apply callback) and
    `host_epoch_watermark`, reconstructed the same way
    `materialise_remote_session` does (`session.rs:1297-1311`,
    `:1282`). Factor that closure construction so both call sites share it.
- Wire it into the send path: when `send_remote` returns
  `BridgeError::UnknownSession` for a `Remote` mirror (`session.rs:2059`),
  re-subscribe via `resubscribe_session`, then retry the send once. On still-
  `UnknownSession`, surface the original error.
- Idempotency (verified): `subscribe_inner` no-ops a live slot
  (`gossip_bridge.rs:692`); mirror apply dedups by seq (`session.rs:2914`), so
  the `publish_replay(ZERO)` re-pull on a populated mirror is safe.

This also independently fixes **live chat after a joiner daemon restart**, which
is currently silently broken and untested.

### Part 2 (artel-fs host cap-listener): re-deliver the secret on `NODE_ID_ACTION`

- At the host's `NODE_ID_ACTION` handler (`workspace.rs:3551`), after
  `peer_map.register(workspace_id, message.peer.id)`, add: if
  `host_ctx.is_some()` and `peer_map.has_rw(message.peer.id)`, spawn
  `publish_upgrade(client, session, peer, current_secret)` reading the **live
  C1 cell** (`ctx.namespace_secret`, refreshed on rotation). Reuse the existing
  delivery body verbatim (mirror of `workspace.rs:3617`/`3459`).
- **Ordering-safe by construction:** the joiner emits `NODE_ID` only *after* its
  cap-listener is live (`workspace.rs:1186` precedes the `NODE_ID` send at
  `:1199`), so the host's secret round-trip cannot outrun the joiner's receiver.
  This is the property the live-only `emit_upgrade` broadcast requires.
- **Fires always** (user-confirmed): every reattach re-pushes; idempotent on the
  joiner (`import_namespace(Write)` is a monotonic Read→Write merge on the
  already-imported rotated namespace). Reduce later only if chatter is a problem.

`host_ctx` already carries the C1 cell as `namespace_secret:
Arc<Mutex<[u8;32]>>` (`workspace.rs:3306`). No new wiring — the
`NODE_ID_ACTION` arm currently only has `joiner_ctx` in scope on the host path
via `host_ctx`; confirm `host_ctx` is threaded into `handle_*` for that arm
(the `PeerJoined` arm at `:3621` already uses it).

## Causal chain (seed-doc target: joiner daemon restarts, then app reattaches)

1. `join_with` → `Subscribe` → reads recover (plane b) + daemon replays current
   read envelope.
2. cap-listener spawned (`workspace.rs:1186`).
3. `join_with` sends `NODE_ID` (`workspace.rs:1199`).
4. `send_remote` hits `UnknownSession` → **Part 1** re-subscribes gossip, retries.
5. `SendRequest` reaches host, passes durable `NotMember` gate.
6. host cap-listener sees `NODE_ID` → **Part 2** `has_rw`-gated
   `publish_upgrade(current secret)`.
7. joiner receives live `UPGRADE_ACTION`, `import_namespace(Write)` merges onto
   the rotated namespace → **write recovered, no re-grant.**

## What dissolved

- Seed open-Q#1 (where does a reloaded joiner re-subscribe?): *nowhere today* —
  Part 1 is that path, triggered lazily.
- Seed open-Q#3 (epoch detection): unneeded — the C1 secret and the current read
  envelope are both current by construction; no reconciliation.
- Seed open-Q#2 (ADR-003): honored — the secret lives in artel-fs; the daemon
  only re-subscribes gossip and couriers the opaque frame.

## Tests (tests-first)

1. **Honest regression (real-n0, `_n0`):** bob RW → bob daemon down → host evicts
   a *different* peer (rotation) while bob is down → bob daemon restarts
   (persistent iroh.key) + app reattaches → assert bob's post-rejoin write
   reaches the host on the rotated namespace, **no re-grant**.
2. **Offline read→write promotion (real-n0, `_n0`):** peer Read, offline,
   promoted to RW while down (the case host-side detection is meant to subsume),
   rejoins → gains write.
3. **Permanent `joiner_daemon_restart_resyncs_both_ways` (no rotation):**
   currently uncovered — proves Part 1 fixes live gossip presence at all.
4. **Daemon unit tests:** `resubscribe_session` idempotency on a live slot;
   `send_remote` retry-after-resubscribe; announce-less re-subscribe still
   passes the host `NotMember` gate.

## Out of scope / deferred

- Joiner-side cap projection / joiner-pull re-delivery (Tier-2 P2P).
- The chat-harness `can_write = is_host` UI wart (harness-only, gitignored).
