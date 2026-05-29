---
date: 2026-05-29
topic: host-restart-peer-addr-cache
---

# Daemon-side peer-addr cache for host-restart sync

## What we're building

A persistent, daemon-internal cache of peer `NodeAddr`s, snapshotted
from iroh's `Endpoint` on graceful daemon shutdown and re-seeded into
the daemon's existing `MemoryLookup` (`IrohRuntime.addr_hint`,
installed at `crates/artel-daemon/src/server.rs:705`) on startup.

Fixes finding **#5c** in `docs/handoff-code-review-fixes.md`: when an
artel-fs host restarts, iroh-docs reads id-only `EndpointAddr`s from
its persistent doc store, skips its internal `memory_lookup` seeding
at `live.rs:472`, and races pkarr/DNS to find the peer. The race
loses, the dial fails, and post-restart writes never reach the peer.
Reproduced by `crates/artel-fs/tests/host_restart_live_writes_n0.rs`
(currently `#[ignore]`d as a regression trap pointing at #5c).

## Why this approach

The handoff doc framed three options assuming the workspace has a
`MemoryLookup` we'd push into. **It doesn't.** Surface map (done
2026-05-29):

- `MemoryLookup` lives only on the daemon — `IrohRuntime.addr_hint`,
  installed in `resolve_iroh_runtime` at `server.rs:705`.
- iroh-docs's `LiveActor` has its own *internal* `memory_lookup` we
  cannot inject into; it's populated only when `join_peers` receives
  non-empty `EndpointAddr`s (`engine/live.rs:472`).
- No daemon→workspace push channel exists. The 2026-05-27 attachment
  registry is unidirectional workspace→daemon RPC.
- Daemon currently ignores `NeighborUp`/`NeighborDown` at
  `gossip_bridge.rs:388`.

That eliminates option (a) (workspace-side persistence — would need a
new push surface and would pierce iroh-docs internals) and reshapes
(b) into a pure daemon-internal change: peer discovery is already a
daemon-layer concern, the daemon already owns `MemoryLookup`, and
iroh-docs's resolver chain transparently consults the daemon's
endpoint resolvers because they share the same iroh `Endpoint`.

Under [[feedback-no-speculative-abstractions]] **rule 2** this is
clean: no layer boundary is crossed (workspace stays unaware), no
runtime trait is introduced, and the change earns its keep against
a concrete failing test.

## Key decisions

- **Source of truth: snapshot iroh `Endpoint` state**, not `NeighborUp`
  events. iroh already aggregates gossip + dial + relay + pkarr
  signals into per-peer `remote_info`; sourcing one signal
  (`NeighborUp`) is strictly worse than reading the aggregate.
  *Why:* event-only misses peers we never gossip-neighboured (mesh
  topology, race with forwarder startup), and `NeighborUp` may carry
  PublicKey only — would have to snapshot at the event anyway.

  **Deviation found during planning (2026-05-29):** iroh 0.98.2 does
  not expose `remote_info_iter()` — only single-id
  `endpoint.remote_info(id) -> Option<RemoteInfo>`. The daemon must
  therefore maintain its own enumerable set of "ids worth
  snapshotting." Decision: derive that set on shutdown from the
  union of (a) the gossip-bridge's session map (host + member peer
  ids across all live sessions) and (b) ids loaded from the
  previous cache at startup (so cached-but-not-yet-redialled peers
  survive one round-trip restart). This is a concrete need of one
  concrete cache, not a speculative abstraction —
  [[feedback-no-speculative-abstractions]] rule 1 still holds.

- **Snapshot timing: graceful shutdown only.** No periodic writer.
  *Why:* the cache is a freshness gamble regardless of frequency —
  peer addrs can change between any snapshot and a crash, so periodic
  writes don't reduce staleness, they just slightly improve "first
  uptime" coverage. MemoryLookup is one resolver in a chain
  (pkarr/DNS run alongside), so a stale hit is at worst one extra
  failed dial before fresh discovery — never permanent breakage.
  Stale cache ≥ no cache; that's the bar.
  *How to apply:* one new write call site in the daemon's existing
  graceful-shutdown path; load on startup before
  `add(addr_hint.clone())` at `server.rs:705`.

- **Storage: single per-daemon file in the daemon state dir**
  (e.g. `peer_addrs.postcard` alongside `iroh.key`). MemoryLookup is
  endpoint-wide, so per-session partitioning would be artificial.
  *Why:* matches how the daemon already manages its own keypair;
  one resolver, one cache, one file.

- **Pruning: size-cap at write.** Snapshot persists at most N
  (e.g. 256) most-recently-seen peers, ranked by last-activity
  timestamp from iroh. Bounds disk and memory deterministically; old
  peers age out as new ones replace them.
  *Why:* simpler than a time cutoff (no "what's the right TTL"
  bikeshed) and bounds disk regardless of how long the daemon runs.

- **Wire format: postcard, externally-tagged enums** per
  `feedback-postcard-externally-tagged-enums`. Persisted file is a
  daemon-internal shape, not protocol-level — but versioning the
  envelope from day one is cheap. Suggested shape:
  `PeerAddrCacheV1 { entries: Vec<(EndpointId, NodeAddr,
  last_seen_unix_secs)> }`.

- **Failure modes are non-fatal.** Read errors at startup (missing
  file, decode error, schema drift) log and proceed with empty
  cache. Write errors at shutdown log and proceed — never block
  shutdown. Per [[project-headless-first-class]], the cache is a
  performance/freshness optimisation, not a correctness primitive.

## Open questions (for the planning phase)

- ~~Exact iroh API for snapshotting endpoint state.~~ Resolved
  during planning: 0.98.2 has no `remote_info_iter`; daemon
  enumerates via bridge session map ∪ previously-loaded ids. See
  Key decisions § "Source of truth" above.
- **Verify before coding (load-bearing):** does iroh-docs's dial
  path (called from `LiveActor` after `join_peers` skips memory_lookup
  seeding for id-only peers, `engine/live.rs:472`) traverse the same
  `endpoint.address_lookup()` chain that the daemon's `MemoryLookup`
  is registered on? The whole approach assumes yes (resolver is
  endpoint-wide; both share the iroh `Endpoint`). If iroh-docs
  short-circuits with its own resolver, this fix doesn't reach the
  failing dial path and the approach changes. Spot-check before
  writing any production code.
- Does `MemoryLookup::add_endpoint_info` accept a stale addr without
  side-effects, or does it merge/replace? (It just adds, per the
  bridge's existing usage at `gossip_bridge.rs:226` — but reconfirm
  before relying on "stale entry is harmless.")
- Test shape — likely a new `DnsPkarrServer` deterministic test
  pinning the load + seed path PLUS un-`#[ignore]`-ing
  `host_restart_live_writes_n0` as the production canary
  (per the two-tier pyramid in `docs/diagnosing-flaky-tests.md`).
  Pre-fix: deterministic test fails because cache file doesn't exist
  / isn't loaded; n0 sibling fails per the existing reproducer.
  Post-fix: both green. **TDD-first per the handoff's methodology
  section** — write the deterministic test before any production
  code.
- Whether to surface a `--clear-peer-cache` daemon CLI flag for
  diagnostics. Probably YAGNI; defer until a real operator need
  shows up.

## Next steps

→ `/workflows:plan` for implementation details, or proceed directly
to TDD: failing deterministic test first
(`tests/peer_addr_cache_pkarr.rs`), then production code, then
un-`#[ignore]` `host_restart_live_writes_n0` as the n0 sibling.
