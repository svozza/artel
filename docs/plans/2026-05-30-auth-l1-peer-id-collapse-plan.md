# Auth Slice A — L1 peer-id collapse — implementation plan

Source brainstorm: `docs/brainstorms/2026-05-30-auth-story-brainstorm.md`. The
brainstorm picks **Option A (collapse)**: `artel-protocol::PeerId` becomes
the iroh `EndpointId` bytes, and every host-side acceptance path verifies
that frame-body `peer.id` matches the gossip-authenticated `delivered_from`.
This plan is *how*, not *what* — every design decision is the brainstorm's.

This is **Slice A** of the v1 auth story. Slices B (per-message signing) and
C (capability events) are **out of scope** and depend on this slice landing
first.

## Sub-slice ordering

Sub-slices are intrinsic and each independently mergeable. Each ends green
on `cargo test --workspace`, fmt + clippy clean both feature modes
(`--all-features` and default).

- **A1 — Daemon enforcement + doc updates.** Host-side
  `delivered_from` ↔ body-`peer.id` check on the two host-role gossip
  arms (`SendRequest`, `JoinAnnouncement`). Joiner-side stamps the
  daemon's authenticated id into outbound `PeerInfo`. New regression
  tests. Doc comments on `PeerId` / `PeerInfo` / `GossipBody` updated
  in the same commit so the documented invariant matches the
  enforced one. Bumps `PROTOCOL_VERSION` 3 → 4.
- **A2 — Synthetic-peer-id retirement.** `--peer-id` flag and
  `derive_default_peer_id()` removed. The iroh-feature-off path still
  exists for `cfg(not(feature = "iroh"))` embeds but uses a constant
  zeroed id documented as non-routable.
- **A3 — Documentation.** Brainstorm cross-link, ADR-001 update,
  `docs/roadmap/peer-identity-authentication.md` superseded note,
  roadmap "Future" → strike "Peer-identity authentication" once this
  ships, point at the brainstorm for the L2/L3 follow-ups.

The `PROTOCOL_VERSION` bump is bookkeeping. Pre-1.0 we have no
on-the-wire compatibility surface to defend; old and new
daemons/clients are not expected to interoperate. The constant lives
next to the code that establishes the new wire meaning so a
workspace-wide `cargo build` recompiles both daemon and client
together. Zero compatibility code, no migration shims.

---

## Sub-slice A1 — Daemon enforcement + doc updates

**Goal:** Establish the `PeerId == EndpointId` invariant *and* enforce
it. Host-side gossip arms that consume a body-carried `peer` field
reject frames whose `peer.id` doesn't match the gossip-authenticated
`delivered_from`. Joiner-side outbound paths stamp the daemon's own
authenticated id into `PeerInfo` before publishing. Doc comments on
`PeerId`, `PeerInfo`, and the relevant `GossipBody` arms are updated
in the same commit so the documented invariant matches the enforced
one.

### Scope of host-side enforcement

Two arms only — both `SessionRole::Host`:

- `GossipBody::SendRequest { peer, .. }` — host-issued `Send` proxy.
- `GossipBody::JoinAnnouncement { peer, .. }` — joiner-arrival admit.

Joiner-role arms (`Message`, `SendAck::Ok`) carry `SessionMessage`
bodies whose `peer.id` is the *original sender's* id, not the host
that re-published the frame. `delivered_from` on those arms is the
host. Verifying `body.peer.id == delivered_from` would be wrong; the
correct invariant is "the host vouches for the message", which needs
end-to-end signatures to enforce. **Joiner-side enforcement of
`Message` / `SendAck::Ok` is therefore deferred to Slice B**, where
L3 signatures will give each `SessionMessage` an independently
verifiable identity. Until B lands, joiners trust their host (the
existing model). The doc comment on the helper function names this
gap explicitly.

### Files touched

- `crates/artel-protocol/src/ids.rs` — update the `PeerId` doc
  comment. Replace the existing first sentence ("Sized to fit an
  iroh node id but the protocol crate stays free of any iroh
  dependency.") with: "32 bytes that ARE an `iroh::EndpointId` (an
  Ed25519 public key). The protocol crate stays free of any iroh
  dependency, but daemon-side enforcement requires this invariant —
  the host drops gossip frames whose body `peer.id` doesn't match
  the gossip-authenticated sender. `PeerId::from_bytes` accepts any
  32 bytes for use in unit-test fixtures that don't cross a real
  gossip mesh; once the bytes hit the network they're checked
  against `delivered_from`." The remainder of the existing comment
  ("Equality and ordering are byte-wise. Display is lowercase hex.")
  stays.
- `crates/artel-protocol/src/message.rs`:
  - `PeerInfo::id` doc: "Cryptographic identity. MUST equal the iroh
    `EndpointId` that delivered the carrying gossip frame; the host
    drops mismatched frames. See ADR-001 § Auth and capability model
    and `docs/brainstorms/2026-05-30-auth-story-brainstorm.md`."
  - `PeerInfo::display_name` doc: replace "Display names are
    advisory; trust comes from the peer id" with "Display names are
    advisory and never authenticated. Trust comes from the peer id,
    which is itself authenticated by the gossip transport."
- `crates/artel-protocol/src/gossip.rs`:
  - `GossipBody::SendRequest::peer` doc: "The originating peer (the
    joiner client's identity). The daemon enforces that `peer.id`
    matches the gossip-authenticated `delivered_from` of the
    carrying frame; mismatched frames are dropped at the bridge."
  - `GossipBody::JoinAnnouncement::peer` doc: same treatment.
- `crates/artel-protocol/src/version.rs` — bump `PROTOCOL_VERSION`
  from `ProtocolVersion::new(3)` to `ProtocolVersion::new(4)`. Rename
  the `current_protocol_version_is_three` test →
  `current_protocol_version_is_four` and update its assertions.
- `crates/artel-daemon/src/gossip_bridge.rs`:
  - Add a private helper at module scope:
    ```rust
    /// Verify that the application-level `peer.id` carried inside
    /// a gossip-frame body matches the gossip-authenticated
    /// `delivered_from` for that frame. The body field is shipped
    /// by the sender; `delivered_from` is signed by the iroh
    /// transport and trustworthy. A mismatch is the L1 spoofed-
    /// authorship / ghost-membership attack class; we drop the
    /// frame at the bridge with a warn log.
    fn peer_id_matches_delivered_from(
        body_peer_id: PeerId,
        delivered_from: iroh::EndpointId,
    ) -> bool {
        body_peer_id.as_bytes() == delivered_from.as_bytes()
    }
    ```
    Pure function for testability. Both `iroh::EndpointId::as_bytes`
    and `PeerId::as_bytes` return `&[u8; 32]`; byte-wise equality is
    the entire check.
  - The forwarder loop in `subscribe_inner` (`gossip_bridge.rs:405`)
    already destructures `IrohGossipEvent::Received(msg)` and reads
    `msg.delivered_from` for the `tracked_peer_ids` insert. Plumb
    that `delivered_from` into `handle_inbound_frame` so the verify
    can run there next to the dispatch:
    ```rust
    handle_inbound_frame(&bridge, session_for_log, &role, body, msg.delivered_from).await;
    ```
  - `handle_inbound_frame` signature gains a fifth arg
    `delivered_from: iroh::EndpointId`. Two host-role arms gain a
    precondition guard, before any other work in the arm body:
    - `(SessionRole::Host, GossipBody::SendRequest { peer, .. })`
    - `(SessionRole::Host, GossipBody::JoinAnnouncement { peer, .. })`

    Guard shape (identical for both):
    ```rust
    if !peer_id_matches_delivered_from(peer.id, delivered_from) {
        warn!(
            ?session,
            body_peer = %peer.id,
            authenticated = %PeerId::from_bytes(*delivered_from.as_bytes()),
            "dropping gossip frame: body peer.id does not match delivered_from",
        );
        return; // drop the frame; no ack, no admit, no fan-out
    }
    ```
    Joiner-role arms (`Message`, `SendAck`) get NO guard in this
    slice — see "Scope of host-side enforcement" above for the
    rationale and the deferral to Slice B. Add a comment in
    `handle_inbound_frame` next to the joiner-role arms naming
    the gap and pointing at the brainstorm.
  - **Joiner-side outbound stamping.** Both `send_remote` (line ~317)
    and `publish_join_announcement` (line ~529) build a `PeerInfo`
    from an IPC-caller-supplied value and then publish. The IPC
    caller's claimed `peer.id` cannot be trusted (this is the
    invariant the joiner side has to uphold so the host's check
    above is meaningful). Override `peer.id` with the daemon's
    authenticated id before encoding:
    ```rust
    let peer = PeerInfo { id: self.authenticated_peer_id(), ..peer };
    ```
    The override preserves `peer.display_name` so callers that set
    a custom display name still see it on the wire.
  - Add field `authenticated_peer_id: PeerId` to `GossipBridge`,
    populated at construction from `endpoint.id().as_bytes()`. Add
    a `pub(crate) fn authenticated_peer_id(&self) -> PeerId`
    accessor so the override sites read cleanly. Choosing `PeerId`
    (the protocol newtype) rather than `iroh::EndpointId` here
    keeps every callsite in this module typed in protocol
    vocabulary, since `PeerInfo.id` is `PeerId`.
  - `GossipBridge::new` signature gains a fourth argument:
    ```rust
    pub(crate) fn new(
        gossip: Gossip,
        addr_hint: MemoryLookup,
        tracked_peer_ids: Arc<std::sync::Mutex<std::collections::BTreeSet<iroh::EndpointId>>>,
        endpoint_id: iroh::EndpointId,
    ) -> Self
    ```
    The body computes
    `let authenticated_peer_id = PeerId::from_bytes(*endpoint_id.as_bytes());`
    and stores it.
- `crates/artel-daemon/src/server.rs` — at the existing
  `GossipBridge::new` call site (line ~255), pass
  `rt.endpoint.id()` as the new fourth argument. The runtime
  already has the value in scope.

### Tests added

Unit tests in `crates/artel-daemon/src/gossip_bridge.rs::tests`:
- `peer_id_matches_delivered_from_accepts_equal_bytes` — pair of
  all-`0xab` bytes; assert true.
- `peer_id_matches_delivered_from_rejects_mismatch` — flip one
  byte; assert false.
- `peer_id_matches_delivered_from_zero_bytes_match` — all zeros on
  both sides; confirms we don't special-case anything.
- `bridge_new_stores_authenticated_peer_id` — construct a bridge
  with a known `EndpointId`, assert
  `bridge.authenticated_peer_id().as_bytes() == endpoint_id.as_bytes()`.
  Under `#[cfg(feature = "iroh")]`.

E2E tests in `crates/artel-daemon/tests/auth_l1_spoofing.rs` (new
file, mirrors the shape of `crates/artel-daemon/tests/gossip.rs`).
Each test asserts only observable behaviour — no log scraping.

- `host_drops_send_request_with_spoofed_peer_id`. Spin a `Pair`
  (existing fixture in `crates/artel-daemon/tests/common/mod.rs`).
  Daemon A hosts; daemon B joins legitimately. Then via B's
  gossip handle (exposed by `Daemon::iroh()`) hand-craft and
  broadcast `GossipBody::SendRequest { req_id, peer: alice_peer_info, payload }` —
  Alice's id stamped onto a frame published from B's endpoint.
  Assertions:
  - No `SendAck` arrives within a 2 s ceiling (drop is silent).
  - Alice's IPC subscriber never observes a `Message` event for
    the spoofed payload.
- `host_drops_join_announcement_with_spoofed_peer_id`. Same
  fixture. From B's gossip handle, broadcast
  `GossipBody::JoinAnnouncement { peer: ghost_peer_info, .. }`
  where `ghost_peer_info.id` is some never-seen 32-byte value.
  Assert Alice's `Subscribe`'d event stream never sees a
  `PeerJoined` for that id. Membership snapshot via
  `Request::ListSessions` shows the host alone.
- `host_accepts_send_request_with_matching_peer_id`. Regression
  guard: a legitimate joiner-side `Send` produces a `Message`
  on Alice's stream within the usual ceiling. Mostly redundant
  with `tests/gossip.rs::iroh_joiner_send_fanout`; kept short to
  pin the don't-over-block-legitimate-traffic property next to
  the spoofing tests.
- `joiner_outbound_stamps_authenticated_peer_id`. Bob's IPC
  client passes a `PeerInfo` with a deliberately-wrong id
  (e.g. `PeerId::from_bytes([0xee; 32])`) on `Subscribe` /
  `Send`. Assert the `SessionMessage` Alice observes has
  `peer.id == bob_endpoint_id.as_bytes()`, NOT the IPC caller's
  claim. Pins the outbound-stamp invariant from the joiner side.

### Definition of done

1. `peer_id_matches_delivered_from` helper exists and is unit-tested.
2. Host-side `handle_inbound_frame` arms for `SendRequest` and
   `JoinAnnouncement` reject body/`delivered_from` mismatches with
   a warn log. Joiner-side `Message` / `SendAck::Ok` enforcement is
   explicitly NOT in scope — a comment in `handle_inbound_frame`
   names the gap and points at the brainstorm.
3. Joiner-side outbound `JoinAnnouncement` and `SendRequest` frames
   carry `PeerInfo.id == endpoint.id().as_bytes()` regardless of
   the IPC caller's claim. Pinned by
   `joiner_outbound_stamps_authenticated_peer_id`.
4. `PROTOCOL_VERSION == 4`.
5. Doc comments on `PeerId`, `PeerInfo`, and the relevant
   `GossipBody` arms name the enforced invariant.
6. Every unit + e2e test added passes; all existing tests in both
   feature modes still pass. fmt + clippy clean both feature
   modes; `cargo doc --workspace` builds.

**Commit subject:** `daemon: enforce peer.id == delivered_from on host gossip arms; stamp authenticated id on joiner outbound (auth L1, PROTOCOL_VERSION 4)`

---

## Sub-slice A2 — Retire the synthetic peer-id path

**Goal:** Remove `--peer-id` from `artel-daemon` CLI args and
`derive_default_peer_id`. `PeerId` always sources from the iroh
endpoint when the feature is on; without the feature, a constant
zeroed id is used and documented as non-routable. Keeps the
crate compileable in `cfg(not(feature = "iroh"))` mode (CI tests
both modes) without leaving a footgun where a user could pass an
arbitrary id and have the daemon ship it on the network.

### Why this is its own sub-slice

A1 is the load-bearing change. A2 removes a construction footgun —
code that lets an operator stamp any 32 bytes as the daemon's
identity — and is mostly mechanical. Splitting it out keeps A1's
diff focused on the enforcement and stamping contract.

### Files touched

- `crates/artel-daemon/src/main.rs`:
  - Remove the `--peer-id <hex>` arg from the clap definition.
  - Remove `parse_peer_id_hex`, `decode_nibble`,
    `derive_default_peer_id`. The latter is currently called only
    in this file.
  - The branch at line ~113 collapses to an unconditional
    `let iroh_key_path = state_dir.join("iroh.key");`. The
    `daemon_peer_id` field on `DaemonConfig` becomes redundant —
    see next bullet.
- `crates/artel-daemon/src/server.rs`:
  - `DaemonConfig::daemon_peer_id` — drop the field (under
    `cfg(feature = "iroh")` it was only used as the
    `resolve_iroh_runtime` fallback when no key path was
    supplied; under `cfg(not(feature = "iroh"))` it was the
    only source). New shape:
    ```rust
    #[cfg(not(feature = "iroh"))]
    /// Non-routable, non-authenticated id used when the iroh
    /// feature is off. Equal to `[0u8; 32]`. Outbound gossip
    /// is impossible in this mode, so the bytes serve only as
    /// a placeholder in the local IPC handshake's
    /// `Response::Hello { daemon_peer_id }`. A future
    /// embedder use case (e.g. unit tests that talk only to a
    /// local registry) sees a stable, obviously-synthetic
    /// value rather than per-process drift.
    pub const SYNTHETIC_LOCAL_PEER_ID: PeerId = PeerId::from_bytes([0; 32]);
    ```
    Replace `config.daemon_peer_id` reads at server.rs lines 229
    and 234 with this constant under
    `cfg(not(feature = "iroh"))`. Under
    `cfg(feature = "iroh")`, `resolve_iroh_runtime` always
    returns the endpoint's id — no fallback path.
  - `resolve_iroh_runtime` signature: drop the
    `requested_peer_id: PeerId` parameter. The function now
    *always* loads/creates an iroh key and uses the resulting
    `EndpointId`. Update its single call site (server.rs:227)
    accordingly.
- `crates/artel-daemon/tests/common/mod.rs`:
  - Drop `FALLBACK_PEER`. Its only legitimate use was as
    `daemon_peer_id` in `DaemonConfig`, which no longer takes
    that field. Test fixtures that need a peer id for
    `PeerInfo::new("…", id)` should sample from the daemon's
    actual `Response::Hello { daemon_peer_id }` after the
    `Hello` round-trip.
  - Same treatment in `crates/artel-fs/tests/common/mod.rs`.
- Anywhere else `FALLBACK_PEER`, `parse_peer_id_hex`, or
  `derive_default_peer_id` are referenced — find via
  `rg "FALLBACK_PEER|parse_peer_id_hex|derive_default_peer_id"`
  and migrate. Per `feedback_extensive_unit_tests`, every test
  that referenced the old constant should still pass post-
  migration without weakening its assertion; if the only
  reasonable migration is "use the daemon-supplied id", do
  that and add a one-line comment naming the change.

### Tests added

Unit tests in `crates/artel-daemon/src/server.rs::tests`:
- `daemon_peer_id_in_hello_response_matches_iroh_endpoint_id`
  (under `#[cfg(feature = "iroh")]`). Pin the load-bearing
  property: a fresh daemon's `Hello` response carries the iroh
  endpoint's id, byte-identical. Without iroh, the same property
  with `SYNTHETIC_LOCAL_PEER_ID`.
- `synthetic_local_peer_id_is_zero` — assert the constant.

No new e2e tests — A1's tests already cover the meaningful
runtime behaviour.

### Definition of done

1. `--peer-id` and `derive_default_peer_id` are removed; the
   only path to a daemon's `PeerId` runs through the iroh
   endpoint (or `SYNTHETIC_LOCAL_PEER_ID` without the feature).
2. `DaemonConfig::daemon_peer_id` is removed.
3. `FALLBACK_PEER` constants in test fixtures are removed; tests
   sample the live daemon's id via the `Hello` round-trip.
4. fmt + clippy clean both feature modes; `cargo test
   --workspace` green; `cargo doc --workspace` builds.

**Commit subject:** `daemon: drop synthetic peer-id path; PeerId always sources from iroh EndpointId (auth L1, A2)`

---

## Sub-slice A3 — Documentation

**Goal:** Mark roadmap-level deferrals resolved by Slice A; cross-
link the brainstorm and plan; update ADR-001's "Updates" trailer
with a one-paragraph addendum.

### Files touched

- `docs/roadmap.md` § "Future" — strike through "Peer-identity
  authentication" and add a "DONE" line referencing the brainstorm
  and this plan. The Auth L2/L3 follow-ups stay open and get a
  pointer to the brainstorm. Note the `PROTOCOL_VERSION` 3 → 4
  bump in the table at the top.
- `docs/roadmap/peer-identity-authentication.md` — prepend a
  "Status update (2026-05-30)" header noting that Slice A landed
  and pointing at the brainstorm + plan. Don't delete the file:
  the failure-mode catalog and design-space discussion remain
  useful as the load-bearing rationale for L1, and L2/L3 will
  grow their own roadmap docs that cite back here.
- `docs/adr/001-collab-substrate-platform.md` — append an
  "Updates" entry: "2026-05-30: L1 peer-id authentication
  (PROTOCOL_VERSION 4). `PeerId` and iroh `EndpointId` are now
  one namespace. Host-side gossip-frame handlers reject frames
  whose body `peer.id` doesn't match the gossip-authenticated
  `delivered_from`. Synthetic / `--peer-id`-supplied identities
  are removed. See `docs/brainstorms/2026-05-30-auth-story-
  brainstorm.md` and `docs/plans/2026-05-30-auth-l1-peer-id-
  collapse-plan.md`. § Open questions § Auth and capability
  model is now L2 + L3 territory; L1 is closed."
- ADR-001's § "Auth and capability model" line stays in the
  open-questions section because L2 and L3 are unresolved; add
  a parenthetical "(L1 resolved 2026-05-30)" so a reader scanning
  the open questions sees what's left.

### Tests added

None — documentation only.

### Definition of done

1. Roadmap "Peer-identity authentication" item marked done with
   cross-links.
2. `peer-identity-authentication.md` carries a status header.
3. ADR-001 trailer updated; the open-questions section names L1
   as resolved.
4. `cargo doc --workspace` builds clean (no broken intra-doc
   links).

**Commit subject:** `docs: mark auth L1 roadmap item done; ADR-001 update for PROTOCOL_VERSION 4`

---

## Cross-cutting concerns

### Things this plan explicitly does not do

- **No per-message signing.** That is Slice B. Mentioned at the
  helper doc-comment in A1 to scope the deferred joiner-side
  `Message` / `SendAck::Ok` enforcement.
- **No capability events.** That is Slice C; depends on Slice B.
- **No `Workspace` shape change.** `artel-fs::Workspace::host_with`
  / `join_with` already accept a caller-supplied `PeerInfo`; the
  IPC caller's claimed id is overridden at the bridge boundary in
  A1. Embedders that want display-name personalisation still set
  `peer.display_name` freely.
- **No L4 (per-consumer IPC trust).** Named fast-follow in the
  brainstorm; the changes here don't foreclose it (the IPC handshake
  is unchanged in shape; consumer-id can ride alongside `Hello` in
  a future bump).
- **No L5 (cross-device user identity).** Same — named fast-follow.
  The collapse decision rests on the observation that `EndpointId`
  is per-device regardless of which option we picked, so L5 will
  introduce a *user* layer above peer identity in either world.
- **No Windows.** Per `project_unix_only_for_now.md`. New code is
  platform-neutral; tested only on the macOS + Linux CI matrix.
- **No `--peer-id` shim.** No backwards-compat flag, no migration
  path, no env-var override. The synthetic-id path is gone, full
  stop.
- **No code that accommodates the version bump.** Pre-1.0 we have
  no on-the-wire compatibility surface to defend; old and new
  daemons/clients are not expected to interoperate. The
  `PROTOCOL_VERSION` constant ticks for clarity, not for
  conditional behaviour. No `#[serde(default)]`, no
  field-presence-detection, no v3-fallback parsers anywhere.

### Risks

1. **Test fixtures that depend on `FALLBACK_PEER` or hand-rolled
   `PeerId::from_bytes(...)` for in-process tests.** A2 retires the
   `FALLBACK_PEER` constant but tests that build a `PeerInfo` for a
   *client* (not a daemon) still need *some* id. The right pattern
   is to round-trip `Hello` against a live daemon and use whatever
   `daemon_peer_id` it returns. For pure-unit tests that don't
   spin a daemon (the registry-via-MemoryStore tests inside
   `session.rs::tests`), arbitrary 32-byte ids are still fine
   because no gossip frames cross — host-side enforcement only
   triggers on real iroh `delivered_from` values. Note this in
   `crates/artel-daemon/src/session.rs::tests`'s comment block
   when migrating test fixtures.

2. **Joiner-side `Message` / `SendAck::Ok` deferred.** A malicious
   *host* could rebroadcast a `SessionMessage` with arbitrary
   `peer.id` and joiners would accept it. This is "Tampered replay
   history" in the brainstorm's threat model — explicitly Slice B
   territory because L3 signatures are the right tool. Until B
   lands, joiners trust their host (the existing model). The
   comment next to the joiner-role arms in `handle_inbound_frame`
   names the gap explicitly so a future code reader sees the
   scope cleanly.

3. **`peer_id_matches_delivered_from` is byte equality, not iroh
   public-key validation.** We rely on the invariant
   `EndpointId == 32 bytes that ARE an Ed25519 public key`, so a
   byte equality of two `EndpointId.as_bytes()` results IS a
   public-key match. If iroh ever changes `EndpointId` to a
   non-Ed25519 shape (Curve25519, larger keys, etc.), this check
   needs revisiting. The doc-comment on the helper names the
   assumption.

4. **`authenticated_peer_id` storage on the bridge.** Stored as a
   `PeerId`, computed once at bridge construction from
   `endpoint.id().as_bytes()`. The bridge is constructed once per
   daemon — no churn. If the daemon ever rotates its iroh key
   while running (it doesn't today; `iroh_key.rs` provides no
   mechanism), the cached value goes stale. Adding a key-rotate
   surface is outside the scope of this slice; if it lands, this
   field becomes a closure capturing the live endpoint or a
   `Mutex<PeerId>`.

---

## Critical files for implementation

- `crates/artel-protocol/src/ids.rs` (A1 — doc)
- `crates/artel-protocol/src/message.rs` (A1 — doc)
- `crates/artel-protocol/src/gossip.rs` (A1 — doc)
- `crates/artel-protocol/src/version.rs` (A1 — `PROTOCOL_VERSION` bump)
- `crates/artel-daemon/src/gossip_bridge.rs` (A1 — most of the work)
- `crates/artel-daemon/src/server.rs` (A1 + A2 — `GossipBridge::new`
  call site, `DaemonConfig` field removal, `resolve_iroh_runtime`
  signature)
- `crates/artel-daemon/tests/auth_l1_spoofing.rs` (A1 — new e2e file)
- `crates/artel-daemon/src/main.rs` (A2 — CLI flag and helper removal)
- `crates/artel-daemon/tests/common/mod.rs` (A2 — fixture migration)
- `crates/artel-fs/tests/common/mod.rs` (A2 — fixture migration)
- `docs/roadmap.md` (A3)
- `docs/roadmap/peer-identity-authentication.md` (A3)
- `docs/adr/001-collab-substrate-platform.md` (A3)
