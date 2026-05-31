# Peer-identity authentication

**Status update (2026-05-30):** L1 (peer-id collapse) shipped as
auth Slice A — `PROTOCOL_VERSION` 4. `artel-protocol::PeerId` is
now defined as the iroh `EndpointId` bytes; host-side
`SendRequest` / `JoinAnnouncement` handlers reject frames whose
body `peer.id` doesn't match the gossip-authenticated
`delivered_from`; joiner-side outbound paths stamp the daemon's
authenticated id; the synthetic-id construction surface
(`--peer-id` flag, `derive_default_peer_id`, `FALLBACK_PEER`) is
gone. The open-design-questions section below — picking between
**collapse**, **bind**, and **trusted-IPC-only** — is superseded
by the v1 auth-story brainstorm at
`docs/brainstorms/2026-05-30-auth-story-brainstorm.md` (which
picked **collapse**) and the implementation plan at
`docs/plans/2026-05-30-auth-l1-peer-id-collapse-plan.md`. The
failure-mode catalog and threat-model rationale below remain the
load-bearing motivation for L1 and the starting-point reference
for L2 (capability events) and L3 (per-message signing).

**Status:** future work, not scoped. Belongs under ADR-001 § "Auth
and capability model" (line 203) which already defers
read-only/write-restricted tickets, signed messages, and ticket
revocation. This doc is the more specific peer-identity strand of
that deferral.

## Summary

The application-level `PeerInfo.id` (a 32-byte `PeerId`) is **not
authenticated**. Any IPC client picks whatever 32 bytes it wants,
and the daemon ships those bytes inside `JoinAnnouncement`,
`SendRequest`, and `Message` gossip frames without verifying that
the bytes correspond to the iroh `EndpointId` that actually
delivered the frame. A malicious joiner can forge authorship and
membership claims on any session they have legitimate access to.

Network-layer identity (`iroh::EndpointId`) is fine — iroh-gossip
authenticates `Message.delivered_from` via the underlying QUIC
transport, and that field is signed by the delivering peer. The
gap is at the application layer: we trust frame *body* contents to
identify peers when only the *envelope* identity is authenticated.

## Failure modes (concrete)

All require a peer who has legitimately joined a session — they
can't be triggered without the ticket. None of them touch the
peer-addr cache (#5c, landed); that path was always sourced from
authenticated `delivered_from`.

1. **Spoofed authorship.** Bob legitimately joins alice's session,
   then issues `SendRequest { peer: carol_peer_info, ... }`. Alice's
   `Registry::send` (called from `run_host_send` in
   `crates/artel-daemon/src/gossip_bridge.rs`) stamps the resulting
   `SessionMessage` as authored by carol and broadcasts it. There
   is no check that `peer.id` matches the gossip-authenticated
   sender.

2. **Spoofed membership.** Bob sends a `JoinAnnouncement` claiming
   any peer id; alice's `Registry::ensure_member` admits them and
   the ghost membership is persisted to the session log.

3. **Spoofed identity in arbitrary RPC paths.** Any other
   gossip-frame body that carries a `PeerInfo`/`PeerId` and is
   trusted by the host has the same shape. (Tombstones,
   leave-session signals, replay requests, etc. — audit on
   implementation.)

4. **EndpointId-keyed seed bait.** A pre-existing risk, but
   relevant context: had the peer-addr cache been keyed on
   body-content `peer_id` (it isn't — see
   `docs/brainstorms/2026-05-29-host-restart-peer-addr-cache-brainstorm.md`),
   a malicious joiner could have planted attacker-controlled addrs
   under arbitrary `EndpointId`s in alice's `MemoryLookup`. The
   current implementation routes through `delivered_from`, which
   blocks this. Future code that touches `addr_hint` must hold the
   same line: never seed from unauthenticated body content.

## Why it wasn't fixed in #5c

Finding #5c (host-restart peer-addr cache) is a network-layer
concern: persist iroh's per-peer addr state across daemon restart.
That fix uses authenticated network identity end-to-end (cache
key = `EndpointId`, sourced from `delivered_from` and
`endpoint.remote_info(id)`). The application-layer
`PeerInfo`-spoofing problem pre-exists #5c and would persist if
#5c were rolled back; it is orthogonal.

The right fix is a deliberate protocol change, not a one-line
patch:

- Bind application `PeerId` to iroh `EndpointId` at session-join
  time.
- Verify the binding on every inbound frame's body before trusting
  the embedded peer info.
- Either (a) require `peer_info.id == delivered_from.as_bytes()`
  (collapses the two namespaces into one — simplest), or (b) ship
  per-session signed envelopes so a peer can authorise a separate
  application identity (more flexible — needed if we ever want
  rotating display identities or proxy/agent peers acting on
  behalf of others).

## Open design questions

- **Collapse or bind?** Option (a) above (single namespace —
  application `PeerId` IS the iroh `EndpointId` bytes) eliminates
  the bug class entirely but loses the indirection that lets
  embedders use any 32-byte id. ADR-001 doesn't require the split;
  worth re-examining whether it's load-bearing for any planned
  consumer.
- **Backward-compat on the wire.** Existing `JoinAnnouncement`/
  `SendRequest`/`Message` frames embed a `PeerInfo` whose
  `peer_id` is unauthenticated. A hard cutover is fine pre-1.0;
  past that we'd need a new versioned envelope. See roadmap
  "Future" → "Wire versioning for gossip frames."
- **Display name vs identity.** `PeerInfo` carries
  `display_name`; that's clearly metadata, not identity, and stays
  unauthenticated. This work scopes only the id.
- **Multi-device / agent identity.** If we ever want one human
  identity to operate from multiple iroh endpoints, the simple
  collapse breaks. Defer until there's a concrete user story.

## What this fix would touch

- `artel-protocol`: `PeerInfo` shape, gossip-body schemas,
  envelope versioning if we keep two namespaces.
- `artel-daemon`: `Registry::ensure_member`, `Registry::send`
  (host-side), every `handle_inbound_frame` arm that uses
  `peer.id`.
- Joiner-side bridge: stamp `delivered_from` (or
  `endpoint.id().as_bytes()`) into outbound `PeerInfo` so the
  invariant the host enforces is one the joiner upholds locally
  too.
- New regression tests: a malicious-joiner integration test
  pinning each spoofing class refuses to authorise the spoofed
  identity.

## Cross-references

- ADR-001 § "Auth and capability model" (line 203) — the parent
  deferral.
- Roadmap "Future" → "Ticket-level capabilities & auth" — the
  ticket-side counterpart (read-only/write-restricted tickets).
  Identity authentication is a prerequisite for meaningful
  capability enforcement.
- `docs/brainstorms/2026-05-29-host-restart-peer-addr-cache-brainstorm.md`
  — captures the design context for #5c, including why
  `delivered_from` (not body `peer_id`) is the load-bearing
  identity for the cache path.
