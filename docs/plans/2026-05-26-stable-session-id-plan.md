# Stable session id across host restarts — implementation plan

Source brainstorm: `docs/brainstorms/2026-05-26-stable-session-id-brainstorm.md`. The brainstorm picks Option 1 (artel-fs derives the id from `NamespaceId`), `Option<SessionId>` field on `Request::HostSession`, no on-disk cache, hard-reject conflicts. This plan is *how*, not *what* — every design decision is the brainstorm's.

This is item 1 of `docs/roadmap.md` § "Multi-session resume across daemon restarts". Items 2 (workspace registry) and 3 (`Workspace::resume`) are **out of scope**.

## Branching prerequisite

The current branch `emdash/stable-id-jx4uy` is at the initial scaffolding commit `92cac08` and contains no source — every file this plan touches lives on `main`. Before sub-slice 1a, **rebase onto `main`** (or branch fresh from `main`):

```
git fetch origin main
git rebase origin/main
```

Sub-slice ordering is intrinsic: 1a (protocol) → 1b (daemon) → 1c (artel-fs) → 1d (docs). Each is independently mergeable; each ends with green tests, fmt + clippy clean both feature modes (`--all-features` and default).

---

## Sub-slice 1a — Protocol: `Option<SessionId>` field on `Request::HostSession`

**Goal:** Wire-shape change only. `Request::HostSession` learns to carry an optional caller-supplied session id; `ProtocolError` learns to express the resume-conflict outcome; `PROTOCOL_VERSION` ticks 1 → 2.

### Files touched

- `crates/artel-protocol/src/rpc.rs` — add `session: Option<SessionId>` to `Request::HostSession`. Field gets `#[serde(default)]` so existing postcard-encoded payloads (no field at the `HostSession` slot) round-trip cleanly into `None`. The variant stays under the existing `#[serde(rename_all = "snake_case")]` (externally tagged) — adding a field is fine; do **not** reach for `#[serde(tag = ..., content = ...)]` per `feedback_postcard_externally_tagged_enums.md`. Update the variant doc-comment to name the new resume semantics ("`None` mints a fresh id; `Some(id)` resumes that session if one already exists, or creates it with the supplied id.").
- `crates/artel-protocol/src/error.rs` — add a new `ProtocolError::SessionConflict(SessionId)` variant. Justified over reusing `AlreadyJoined` / `NotHost`:
  - `AlreadyJoined` means "this peer is already a member" — semantically wrong for "this id exists with a different owner."
  - `NotHost` is for joiner-side `Send` against a remote-mirror session — also semantically wrong here.
  - The brainstorm leaves the choice to planning; introducing `SessionConflict` keeps each variant a clean fit. Add `slug() = "session_conflict"`, an `#[error("session id {0} already exists with a different host or kind")]` annotation, and entries in the `slug_is_stable_per_variant` and `display_messages_are_human_readable` tests (and in `arb_error()` for the proptest battery).
- `crates/artel-protocol/src/version.rs` — bump `PROTOCOL_VERSION` from `ProtocolVersion::new(1)` to `ProtocolVersion::new(2)`. Update the `current_protocol_version_is_one` test → `current_protocol_version_is_two`. The version-mismatch path doesn't change shape: an old client (v1) talking to a new daemon (v2) gets the existing `ProtocolError::VersionMismatch` (verbatim wire shape) and the daemon's existing "restart required" message stands.

### Public API additions

```rust
// artel-protocol::rpc
pub enum Request {
    // ...
    HostSession {
        peer: PeerInfo,
        /// If `Some`, the daemon resumes the session at this id when
        /// a matching local-host record exists, or creates one with
        /// this id if not. If `None` (the default), a fresh random
        /// id is minted.
        ///
        /// Returns `ProtocolError::SessionConflict` when an existing
        /// record at this id has a different host or is a remote
        /// mirror.
        #[serde(default)]
        session: Option<SessionId>,
    },
    // ...
}

// artel-protocol::error
pub enum ProtocolError {
    // ...
    /// `HostSession { session: Some(id) }` was issued for an `id`
    /// that exists locally but with a different host or as a
    /// remote-mirror session.
    #[error("session id {0} already exists with a different host or kind")]
    SessionConflict(SessionId),
}
```

No `HostOptions` struct — per `feedback_no_speculative_abstractions.md` and the brainstorm's "ship `Option<SessionId>` exactly as the brainstorm specifies."

### Tests added

In `crates/artel-protocol/src/rpc.rs::tests`:
- `host_session_request_round_trip_with_session_id` — `Some(SessionId::from_bytes([7;16]))`, postcard + JSON round-trip.
- `host_session_request_round_trip_with_no_session_id` — explicit `None`, round-trip.
- `host_session_request_decodes_legacy_payload_as_none` — encode `{ peer }` against the *old* shape (use a hand-crafted postcard byte sequence that omits the new field, or a parallel local struct with one fewer field) and assert it deserialises into `Request::HostSession { peer, session: None }`. This documents the `#[serde(default)]` backwards-compat property loud and clear; without it future drift could silently break the v1→v2 boundary.
- Update `arb_request()` in the proptest block: the `HostSession` arm gains `proptest::option::of(any::<[u8; 16]>())` and produces both `Some` and `None`. The existing `request_round_trip` and `wire_message_round_trip` proptests pick this up automatically.

In `crates/artel-protocol/src/error.rs::tests`:
- Add `SessionConflict` arms to `slug_is_stable_per_variant` (`"session_conflict"`), `display_messages_are_human_readable`, and `arb_error()` so postcard + JSON round-trip props cover it.

In `crates/artel-protocol/src/version.rs::tests`:
- Rename `current_protocol_version_is_one` → `current_protocol_version_is_two` and update its assertion.

### Backwards-compat note

Per the brainstorm: "older clients/daemons get the standard `restart required` error." That path is `ProtocolError::VersionMismatch` and is unchanged at the wire level. A v1 client connecting to a v2 daemon hits the daemon's `Hello` handler, gets `VersionMismatch { client: v1, daemon: v2 }` back, surfaces the existing display string, and exits. A v2 client talking to a v1 daemon takes the symmetric path. No N-1 fallback work in scope.

### Definition of done

1. `Request::HostSession` carries `session: Option<SessionId>` with `#[serde(default)]`; postcard + JSON + proptest round-trips green.
2. `ProtocolError::SessionConflict(SessionId)` exists with stable slug, display, and round-trip coverage.
3. `PROTOCOL_VERSION == 2`. Version-mismatch path still produces `VersionMismatch` at the existing wire shape.
4. Legacy-payload-decodes-to-`None` test passes.
5. fmt + clippy clean both feature modes; `cargo doc` builds.

**Commit subject:** `protocol: add Option<SessionId> to HostSession + SessionConflict error (PROTOCOL_VERSION 2)`

---

## Sub-slice 1b — Daemon: `Registry::host` resume + conflict paths

**Goal:** `Registry::host` branches on the new `Option<SessionId>` arg. `None` keeps today's "mint a fresh random id" path. `Some(id)` either reuses the existing local-host record verbatim (preserving log + members + head, re-stamping the ticket with the current `daemon_addr`, re-opening the gossip topic) or returns `ProtocolError::SessionConflict`.

### Files touched

- `crates/artel-daemon/src/session.rs`:
  - `Registry::host` signature changes from `(&self, host_peer: PeerInfo)` to `(&self, host_peer: PeerInfo, session: Option<SessionId>)`. Body branches:
    1. `None` → today's path: `SessionId::new_random()`, build `Session::new(...)`, `store.create(&record)`, insert into `sessions`, open gossip topic. Unchanged.
    2. `Some(id)` and no existing local entry → mint at the supplied id (replace the `new_random()` line with the supplied id). Persist + insert + open topic identically to the `None` path.
    3. `Some(id)` and existing local entry → load it under the `sessions` read lock; verify `s.host == host_peer.id` and `s.kind == SessionKind::Local`. If either check fails → return `SessionError::SessionConflict(id)`. If both pass → re-stamp a fresh ticket from the *current* `self.daemon_addr` (the existing record's prior addr is stale across daemon restarts) and re-open the gossip topic (`bridge.host_session(id)` — idempotent on the bridge side; if not, plan adds idempotency in this slice).
  - Add `SessionError::SessionConflict(SessionId)`, the in-crate twin of the protocol-level variant. Wire it into the existing `PartialEq` impl and into `session_error_to_protocol` (in `server.rs`) → `ProtocolError::SessionConflict(id)`.
  - The verbatim-resume case touches no `members`, no `log`, no `head` — it's a pure reattach. The `Session` struct's broadcast channel is also reused (the `events_tx` was kept across daemon restarts as a fresh channel inside `from_record`; resuming after a same-process resume keeps the same one).
- `crates/artel-daemon/src/server.rs`:
  - `Request::HostSession { peer, session }` arm: forward `session` into `registry.host(peer, session).await`. Single-line change beyond the destructure.
  - `session_error_to_protocol`: add the `SessionError::SessionConflict(id) → ProtocolError::SessionConflict(id)` arm.
- `crates/artel-daemon/src/gossip_bridge.rs`:
  - `host_session(session_id)`: confirm idempotency (resume calls it a second time on a daemon that's already host). If the existing impl panics or errors on an already-known session, swap the panic/`Err` for an `Ok(())` early-return. Add a unit test for double-call idempotency.

### Tests added

Unit tests in `crates/artel-daemon/src/session.rs::tests` (using the existing `MemoryStore` fixture pattern at line ~896):
- `host_with_some_id_creates_session_at_that_id` — first-time host with a supplied `SessionId`. Assert the returned id matches and the store has a record at that id.
- `host_with_some_id_resumes_existing_local_session` — pre-seed a `SessionRecord { kind: Local, host: alice }` via `MemoryStore::create`, build `Registry::load`, then call `registry.host(alice, Some(id))`. Assert: the registry's in-memory `Session` for `id` has its `members`, `log`, and `head` byte-identical to the pre-seeded record (NOT a fresh empty one). Assert: the returned ticket parses cleanly and its `host_addr` matches `registry.daemon_addr` (re-stamped, possibly differing from any addr in the pre-seeded record).
- `host_with_some_id_rejects_when_host_differs` — pre-seed a record at `id` with `host: alice`. Call `registry.host(bob, Some(id))`. Assert `Err(SessionError::SessionConflict(id))`. Assert the in-memory session is unchanged (still `host: alice`).
- `host_with_some_id_rejects_when_kind_is_remote` — pre-seed a record at `id` with `kind: Remote`. Call `registry.host(alice, Some(id))`. Assert `Err(SessionError::SessionConflict(id))`. Same no-mutation guarantee.
- `host_with_none_still_mints_random_id` — regression guard for the existing path.
- Extend the existing PartialEq tests to cover `SessionConflict`.

Bridge unit test in `crates/artel-daemon/src/gossip_bridge.rs::tests`:
- `host_session_is_idempotent_on_double_call` — call `bridge.host_session(id)` twice; assert second call returns `Ok(())`.

E2E test in `crates/artel-daemon/tests/host_resume.rs` (new file — mirrors the shape of `crates/artel-daemon/tests/persistence.rs`):
- Spin two `Daemon` instances back-to-back against the same `state_dir`, both with `iroh` feature on and `MemoryLookup` cross-seeding.
- Daemon A hosts (`Client::request(Request::HostSession { peer: alice, session: None })`), gets `id`. Send a couple of messages so the log is non-empty. Take note of `id`, the ticket, and the head seq.
- Shut Daemon A down. Spin Daemon B against the same `state_dir`. Bob (a separate client+daemon) joins via the *original* ticket — confirms Daemon B rehydrated the session from disk.
- Daemon A's "host process" reconnects to Daemon B (they share a state dir; in this test that's a single client connecting back to Daemon B) and issues `HostSession { peer: alice, session: Some(id) }`. Assert: `Response::HostSession { session, .. }` with `session == id`, and the ticket re-stamped with Daemon B's current `daemon_addr`. Issue a `Subscribe { since: None }` and assert the full pre-restart log replays — proof that resume preserved the log.
- Conflict variant: separately, daemon C issues `HostSession { peer: bob, session: Some(id) }` against the same daemon (where alice's record is loaded). Assert `Response::Error { error: ProtocolError::SessionConflict(id) }`.

### Definition of done

1. `Registry::host(peer, Some(id))` resumes a verbatim local-host record (members, log, head, broadcast channel preserved) when `host` and `kind` match.
2. Same call returns `SessionError::SessionConflict` when `host` differs or `kind == Remote`. Mapped to `ProtocolError::SessionConflict` at the wire boundary.
3. `Registry::host(peer, None)` is byte-equivalent to today's behaviour.
4. The ticket returned by a resume call is re-stamped with the daemon's *current* `daemon_addr` so a joiner who imports a re-stamped ticket reaches the correct address even after the daemon's iroh endpoint rotates.
5. Gossip topic is re-opened on resume (`bridge.host_session(id)` is idempotent).
6. Unit tests + 1 e2e test pass; fmt + clippy clean both feature modes.

**Commit subject:** `daemon: resume Registry::host on Option<SessionId>; reject conflicts`

---

## Sub-slice 1c — artel-fs: derive a stable session id from `NamespaceId`

**Goal:** `Workspace::host_with` derives a deterministic `SessionId` from the workspace's `NamespaceId` and passes it through `Client::host_session` (or directly via `Client::request(Request::HostSession { ..., session: Some(id) })` — see below). A re-host of the same dir lands on the same session id, gives the same gossip topic (topic = `session_id[..16]`, unchanged), and lets an existing joiner resume.

### Files touched

- `crates/artel-fs/src/lib.rs` — declare a new `pub mod session_id;` (or inline into `keystore.rs`; new file is cleaner). Re-export `pub use session_id::session_id_for;`.
- `crates/artel-fs/src/session_id.rs` — **new module.**
  ```rust
  use artel_protocol::SessionId;
  use iroh_docs::NamespaceId;

  /// Domain-tag for the v1 derivation. Bumping this is a breaking
  /// change for existing on-disk workspaces; same upgrade contract
  /// as `NamespaceId` stability.
  const DOMAIN_TAG: &[u8; 32] = b"artel-fs/session-id/v1\0\0\0\0\0\0\0\0\0\0";

  /// Derive a stable, version-tagged `SessionId` from a workspace's
  /// `NamespaceId`. Pure function — no I/O, no caching, never fails.
  pub fn session_id_for(ns: NamespaceId) -> SessionId {
      let hash = blake3::keyed_hash(DOMAIN_TAG, ns.as_bytes());
      let mut bytes: [u8; 16] = hash.as_bytes()[..16].try_into().unwrap();
      // UUID v8 variant bits per RFC 9562 §5.8: high nibble of byte 6
      // is 0b1000 (version 8), high two bits of byte 8 are 0b10
      // (RFC variant). Brainstorm explicitly chose v8.
      bytes[6] = (bytes[6] & 0x0F) | 0x80;
      bytes[8] = (bytes[8] & 0x3F) | 0x80;
      SessionId::from_bytes(bytes)
  }
  ```
  `blake3::keyed_hash` requires a `&[u8; 32]` key — pad the tag to 32 bytes. The padded tag is itself part of the v1 contract; future versions (`v2` etc.) get a new padded constant.
- `crates/artel-fs/src/workspace.rs`:
  - `host_with` derivation point: after `open_or_create_doc` succeeds (we have `doc.id() -> NamespaceId` at that moment, regardless of returning vs. fresh), compute `let derived_id = session_id_for(doc.id());`. Pass `Some(derived_id)` through to the daemon.
  - The daemon call site for hosting today is buried inside the artel-client API (`Client::request(Request::HostSession { peer })` is what the consumer of `Workspace::host_with` does *before* calling `Workspace::host_with` — `host_with` itself takes a `session: SessionId` arg already). Re-read of `host_with` confirms: the workspace consumer picks the session id by issuing `HostSession` themselves, then passes the resulting `SessionId` to `Workspace::host_with`. **The plumbing that needs to change is at the level of whatever helper the consumer uses.**

    Two shapes the consumer can take:
    1. The consumer calls `client.request(Request::HostSession { peer, session: None })` themselves and threads the returned id into `Workspace::host_with`. This is the existing pattern.
    2. We add a `Workspace`-side helper that does the host-then-attach atomically. This *is* a useful helper, but it's the ergonomics of roadmap item 3 (`Workspace::resume`), which is out of scope.

    Per the brainstorm: "`artel-fs::Workspace::host_with` derives a deterministic session id ... and passes it on every host call." The consumer-driven shape (1) means the brainstorm's promise has to be realised inside `host_with` — but `host_with` already takes a `session: SessionId` from outside. The resolution: `host_with` can't unilaterally re-host on the consumer's behalf without changing its arg list.

    **Decision:** make `host_with` *verify* that the supplied `session` matches the derived id when state is returning, and propagate the derived id outward via a new helper. Concretely:

    - Add `Workspace::derive_session_id(state_dir: &Path) -> Result<Option<SessionId>, WorkspaceError>` — a free helper (or static method) that reads the persisted `doc-id` file and returns `Some(session_id_for(ns))` if present, `None` if not. Unit test it. Consumers call this *before* `HostSession` and pass the result as the `session` field.
    - In `host_with`, after `open_or_create_doc` resolves the `NamespaceId`, **assert** (or rather: warn-log on mismatch) that the supplied `session: SessionId` arg equals `session_id_for(doc.id())`. A mismatch means the consumer didn't use `derive_session_id`, which is a contract violation: surface a `WorkspaceError::SessionIdMismatch { expected, got }` and bail. This makes the deterministic-id contract enforceable.

    This shape is consistent with the brainstorm's stated property ("re-host of the same dir always lands on the same session id") *and* keeps the consumer-driven host pattern. The alternative — making `host_with` issue `HostSession` itself — changes the public API and tangles with the no-`HostOptions`-struct constraint.
  - Add `WorkspaceError::SessionIdMismatch { expected: SessionId, got: SessionId }` to `crates/artel-fs/src/error.rs`.

### Public API additions

```rust
// artel-fs::session_id (re-exported from lib.rs)
pub fn session_id_for(ns: NamespaceId) -> SessionId;

// artel-fs::workspace
impl Workspace {
    /// Read the persisted `NamespaceId` at `state_dir/doc-id` and
    /// derive its stable [`SessionId`] for use with
    /// `Request::HostSession { session: Some(...) }`. Returns
    /// `Ok(None)` when the workspace has never been hosted (no
    /// persisted `doc-id`).
    pub fn derive_session_id(state_dir: &Path) -> Result<Option<SessionId>, WorkspaceError>;
}
```

### Tests added

Unit tests in `crates/artel-fs/src/session_id.rs`:
- `session_id_is_stable_for_a_given_namespace_id` — call `session_id_for` twice on the same `NamespaceId::from(&[7u8; 32])`, assert byte-identical.
- `session_id_differs_for_distinct_namespace_ids` — two arbitrary distinct `NamespaceId`s map to distinct session ids.
- `session_id_has_uuid_v8_variant_bits` — assert byte 6 high nibble is `0x8` and byte 8 high two bits are `0b10`.
- A small proptest generating random `[u8; 32]` inputs and verifying both stability and v8 bits.

Unit tests in `crates/artel-fs/src/workspace.rs::tests`:
- `derive_session_id_returns_none_for_fresh_state_dir` — tempdir, no `doc-id` file, returns `Ok(None)`.
- `derive_session_id_returns_consistent_id_for_persisted_doc_id` — write a synthetic `doc-id` file (32 bytes) and assert `derive_session_id` returns `Some(session_id_for(NamespaceId::from(...)))` matching a hand-computed reference.
- `host_with_rejects_session_id_not_derived_from_namespace` — pre-seed a `state_dir` with a known `doc-id` and call `host_with` with a fabricated `SessionId::new_random()`. Assert `Err(WorkspaceError::SessionIdMismatch { .. })`.

E2E test `crates/artel-fs/tests/host_resume_session_id.rs` (new file, mirrors `crates/artel-fs/tests/disk_resume.rs`'s `Pair`-fixture shape):
1. Spin a host-side `Pair` (Alice's daemon + Bob's daemon, cross-seeded `MemoryLookup`).
2. Alice: `derive_session_id(state_dir)` → `None`. Issue `HostSession { peer, session: None }` → get `id1`. Call `Workspace::host_with(client, id1, root, AllowExisting, default config)` — confirm it succeeds. **At first-time host, there's no `doc-id` yet** — so the helper returns `None`, the consumer passes `None` to `HostSession`, the daemon mints a random id, and `host_with` then sees the random id passed by the caller versus the derived id from the freshly-created `NamespaceId`.

   **Resolution:** the contract is "if `derive_session_id` returns `Some`, the host MUST use it; if it returns `None`, the host MUST issue `HostSession { session: None }` and trust the daemon to mint." `host_with`'s mismatch check needs a corresponding allowance: if `state_dir/doc-id` did *not* exist before `open_or_create_doc` (the `returning == false` branch), `host_with` *adopts* the random id as the workspace's id and skips the mismatch check. This is fine because the freshly-minted random id and the derived id will, on subsequent restarts, be the same (the `NamespaceId` is now persisted, so `derive_session_id` returns `Some(...)` next time).

   Concretely: `host_with` knows whether `open_or_create_doc` returned `(_, returning=true)` or `(_, returning=false)`. The mismatch check fires only on `returning=true`. The fresh-host path skips it.

3. Continue test: shut Alice's daemon down (taking the workspace with it). Spin a fresh daemon at the same state dir. Re-host: `let id2 = derive_session_id(state_dir)?.expect("doc-id is persisted");` — call `HostSession { session: Some(id2) }` → daemon resume path (1b) takes over and returns `id2`. Assert `id2 == id1`.
4. Assert the gossip topic byte-derived from `id2` equals the topic derived from `id1` (the `topic_for` function lives in `gossip_bridge.rs`; we don't import it but compute `id2.as_bytes()[..16]` directly to match the topic-derivation contract).
5. **The user-visible property test:** Bob joined Alice in step 2 with the *original* ticket. After Alice's daemon restart and re-host, Bob's existing connection is still subscribed to the same gossip topic. Assert that Alice can `Send` a message after re-host and Bob receives it. (This is the actual point of the work; without it the rest is theatre.)

### Definition of done

1. `session_id_for(NamespaceId) -> SessionId` exists; pure, deterministic, v8-tagged, no I/O.
2. `Workspace::derive_session_id(state_dir)` exposed; returns `None` for fresh dirs, `Some(stable_id)` for hosted dirs.
3. `host_with` enforces "supplied session id matches `session_id_for(doc.id())`" on the returning-host path; allows mismatch on first-host (where the daemon-minted id is *adopted* as the new namespace's id).
4. Re-hosting the same dir lands on the same session id and the same gossip topic across daemon restarts.
5. An existing joiner's mirror keeps receiving messages from the host across the host's daemon restart (e2e proven).
6. fmt + clippy clean both feature modes.

**Commit subject:** `artel-fs: derive stable SessionId from NamespaceId; resume on re-host`

---

## Sub-slice 1d — Documentation

**Goal:** Mark roadmap item 1 done; cross-link the brainstorm and plan; add a one-paragraph ADR-001 addendum noting the protocol change.

### Files touched

- `docs/roadmap.md` — under § "Multi-session resume across daemon restarts", strike through item 1 and add a "DONE" line referencing `docs/brainstorms/2026-05-26-stable-session-id-brainstorm.md` and `docs/plans/2026-05-26-stable-session-id-plan.md`. Items 2 and 3 stay open. Note the `PROTOCOL_VERSION` 1 → 2 bump in the table.
- `docs/adr/001-collab-substrate-platform.md` — append a one-paragraph addendum under § "Decisions intentionally deferred" (or a new "Updates" section at the bottom) noting that `Request::HostSession` now carries an optional caller-supplied `SessionId` and that the protocol version is 2. The ADR's RPC surface enumeration ("`host_session`, `join_session`, ...") doesn't need editing — the verb count is unchanged.

### Tests added

None — documentation only.

### Definition of done

1. Roadmap item 1 marked done with cross-links to brainstorm + plan.
2. Roadmap protocol-version table reflects v2.
3. ADR-001 addendum lands (or a justification for skipping it lands in the commit message).
4. `cargo doc --workspace` builds clean (no broken intra-doc links to renamed items).

**Commit subject:** `docs: mark stable-session-id roadmap item done; ADR-001 addendum for PROTOCOL_VERSION 2`

---

## Cross-cutting concerns

### Things this plan explicitly does not do

- **No workspace registry.** Roadmap item 2; out of scope.
- **No `Workspace::resume` ergonomics helper.** Roadmap item 3; out of scope.
- **No `.artel-fs/session-id` cache.** Brainstorm rejected this. The id is re-derived on every call.
- **No `HostOptions` struct.** Brainstorm rejected this. `Option<SessionId>` ships as a bare field.
- **No changes to `gossip_bridge.rs::topic_for`.** Topic = `session_id.as_bytes()[..16]` is unchanged; stable session id → stable topic falls out for free.
- **No new sidechannel.** All inter-daemon traffic stays on iroh-gossip per `feedback_gossip_only_inter_daemon.md`.
- **No Windows.** Per `project_unix_only_for_now.md`. The new code is platform-neutral but tested only on macOS + Linux CI.

### Risks

1. **`Workspace::host_with`'s first-host path.** The fresh-namespace case mints a random id daemon-side, while the derived id from the same namespace is what the workspace will need on next restart. The plan resolves this by skipping the mismatch check on the `returning=false` branch and trusting the daemon-minted id to be adopted as the namespace's id going forward. This relies on the daemon's `Some(id)` resume path actually re-using the same id next time, which 1b guarantees. Mitigation: the `host_resume_session_id.rs` e2e test exercises exactly this round-trip.

2. **Re-stamping the ticket on resume.** A joiner who saved the *old* ticket from before the host restart will now find the gossip topic still works (same session id), but the embedded `host_addr` is stale. This is fine because the re-stamped ticket is what new joiners will see; old joiners are already in the gossip mesh and don't need the addr again. Document in `Registry::host`'s resume branch as a load-bearing invariant.

3. **`PROTOCOL_VERSION` bump risk.** Strictly an additive change at the postcard level — `#[serde(default)]` covers the field — but the bump is conservative because we're adding a *new* `ProtocolError` variant that an old client wouldn't recognise (`SessionConflict`). An old client decoding `ProtocolError::SessionConflict(id)` from a new daemon would fail with a postcard variant-tag error. Bumping protects that surface. If a future slice wants additive `ProtocolError` variants without a bump, the brainstorm-listed deferral ("multi-version daemon coexistence") becomes load-bearing — out of scope today.

4. **`blake3::keyed_hash` key size.** The 32-byte key requirement means the version tag has to be padded. The padding bytes are part of the v1 contract — changing them is `v2`, full stop. A unit test asserting the padded constant's exact bytes catches accidental edits.

---

## Critical files for implementation

- `crates/artel-protocol/src/rpc.rs`
- `crates/artel-protocol/src/error.rs`
- `crates/artel-protocol/src/version.rs`
- `crates/artel-daemon/src/session.rs`
- `crates/artel-daemon/src/server.rs`
- `crates/artel-daemon/src/gossip_bridge.rs`
- `crates/artel-fs/src/lib.rs`
- `crates/artel-fs/src/session_id.rs` (new)
- `crates/artel-fs/src/workspace.rs`
- `crates/artel-fs/src/error.rs`

(All paths are relative to the workspace root. The current scaffolding worktree doesn't have these yet — the branching prerequisite at the top of this plan covers the rebase.)
