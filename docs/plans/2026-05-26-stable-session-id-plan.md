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

**Goal:** `Workspace::host_with` becomes the single entry point for hosting an artel-fs workspace: it opens (or creates) the local namespace, derives the session id from it, and issues `HostSession` *itself*. A re-host of the same dir lands on the same session id, gives the same gossip topic, and lets an existing joiner resume seamlessly across the host's daemon restart.

### Why this shape (approach A)

The earlier draft of this plan had `host_with` *verify* that the caller had supplied the right session id (via a separate `derive_session_id` helper). That shape is broken on first host: the caller has no `doc-id` yet, so `derive_session_id` returns `None`, the caller passes `None` to `HostSession`, the daemon mints a random id `id_random`, and the workspace's persisted record sits at `id_random`. On the next restart, `derive_session_id` returns `Some(id_derived)` (from the now-persisted `NamespaceId`). The caller passes `Some(id_derived)`, the daemon takes the *create* branch (no record at `id_derived`), and a fresh empty session is stamped — leaving the original log orphaned at `id_random`.

The fix is to make `host_with` own the HostSession call. It opens the namespace, derives the id, calls `HostSession { session: Some(derived) }` exactly once, and the same id is used on first host and every subsequent resume. No race between caller and workspace, no mismatch path to worry about.

This trades a wider `host_with` signature change (loses the `session: SessionId` arg, gains a `peer: PeerInfo` arg) against eliminating an entire bug class. Per `feedback_no_speculative_abstractions`, ship the simpler shape that's correct on day one. The signature change touches every artel-fs test (~16 call sites) plus the consumer's lone host call; mechanical update.

The naming (`host_with`, "host") is on borrowed time anyway — ADR-001 § "Future evolution" signals a symmetric P2P direction where "host" stops being privileged. Approach A's structure (`open_namespace → derive_id → register_with_daemon`) is exactly what the symmetric peer flow will look like; only the verb name changes when that lands.

### Files touched

- `crates/artel-fs/src/lib.rs` — declare `pub mod session_id;` and re-export `pub use session_id::session_id_for;`.
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
      // UUID v8 variant bits per RFC 9562 §5.8.
      bytes[6] = (bytes[6] & 0x0F) | 0x80;
      bytes[8] = (bytes[8] & 0x3F) | 0x80;
      SessionId::from_bytes(bytes)
  }
  ```
  `blake3::keyed_hash` requires a `&[u8; 32]` key — pad the tag to 32 bytes. The exact byte sequence of the padded tag is part of the v1 contract; a unit test pins it.
- `crates/artel-fs/src/workspace.rs`:
  - **Signature change.** `Workspace::host_with` drops its `session: SessionId` parameter and gains `peer: PeerInfo`. New shape:
    ```rust
    pub async fn host_with(
        client: &Client,
        peer: PeerInfo,
        root: PathBuf,
        policy: AttachPolicy,
        config: WorkspaceConfig,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError>
    ```
    Returns `(Workspace, events)` as today, *plus* the new `Workspace::session_id` accessor (see below) so callers that need the id post-construction can read it.
  - **Body restructure.** Order today is: enforce policy → ensure state dir → spawn node → `open_or_create_doc` → reconcile/scan → `share` → `publish_ticket(client, session, ticket, rules)`. New order:
    1. Enforce policy, ensure state dir, spawn node (unchanged).
    2. `open_or_create_doc` → `(doc, returning)`. We now have `doc.id()` (a `NamespaceId`) regardless of returning/fresh.
    3. `let session_id = session_id_for(doc.id());` — pure, no fallible step.
    4. `client.request(Request::HostSession { peer, session: Some(session_id) })` — single round trip. On `Ok`, the daemon returned the same id back (1b's resume-or-create-with-id path). On `Err(SessionConflict(_))`, propagate as `WorkspaceError::SessionConflict`.
    5. Continue with reconcile (if returning) → scan → share → `publish_ticket(client, session_id, ticket, rules)`.
  - **Store the id on `Workspace`.** Add `session_id: SessionId` field plus `pub fn session_id(&self) -> SessionId` accessor. Useful for tests and for any consumer that needs the id without re-deriving.
  - Add `WorkspaceError::SessionConflict(SessionId)` to `crates/artel-fs/src/error.rs`. Maps from `ClientError::Protocol(ProtocolError::SessionConflict(id))` at the point we issue HostSession; surface to callers so they can present the error meaningfully.
  - The existing `WorkspaceError::Client(ClientError)` arm absorbs all other `request` failures unchanged.

### Public API additions

```rust
// artel-fs::session_id (re-exported from lib.rs)
pub fn session_id_for(ns: NamespaceId) -> SessionId;

// artel-fs::workspace
impl Workspace {
    /// The artel session id this workspace is attached to. For
    /// hosts this is `session_id_for(self.doc.id())` — derived
    /// from the local NamespaceId so a re-host of the same dir
    /// lands on the same id. For joiners this is whatever the
    /// daemon said `JoinSession` returned.
    pub fn session_id(&self) -> SessionId;
}

// artel-fs::error
pub enum WorkspaceError {
    // ... existing variants
    /// The daemon rejected `HostSession { session: Some(id) }`
    /// because a different host or a remote-mirror session
    /// already owns this id. In practice this means the user is
    /// pointing two different daemons at the same artel state
    /// dir, which we don't support today.
    SessionConflict(SessionId),
}
```

### Migration of existing call sites

Every test that currently does:
```rust
let (session, ticket) = match client.request(Request::HostSession { peer, session: None }).await? { ... };
let (ws, events) = Workspace::host_with(&client, session, root, policy, cfg).await?;
```
becomes:
```rust
let (ws, events) = Workspace::host_with(&client, peer, root, policy, cfg).await?;
let session = ws.session_id();
```

Tests that need the join ticket separately (most do, to drive a Bob in the same test) still get it the same way they do today — by subscribing to the workspace events stream and waiting for the `workspace.ticket` system message. That part doesn't change.

Affected files (approximately, from a `Workspace::host_with` grep):
- `crates/artel-fs/tests/{disk_resume,host_restart_ticket_stable,ticket_envelope_round_trip,ticket_envelope_rejects_old_shape,read_only_*,attach_policy_host,attach_policy_join,join_bulk_export,live_edit,delete_propagates,round_trip,default_read_write_unchanged_behaviour,mixed_rules_first_match_wins,empty_file_no_error,host_publishes_ticket}.rs`
- `crates/artel-fs/tests/bin/crash_child.rs` (the one consumer that runs in a child process)
- `crates/artel-fs/tests/run_readiness.rs`, `attach_policy_state_dir_only.rs`

Mechanical change. Don't introduce semantic differences while migrating.

### Tests added

Unit tests in `crates/artel-fs/src/session_id.rs`:
- `session_id_is_stable_for_a_given_namespace_id` — call `session_id_for` twice on the same `NamespaceId::from([7u8; 32])`, assert byte-identical.
- `session_id_differs_for_distinct_namespace_ids` — two arbitrary distinct `NamespaceId`s map to distinct session ids.
- `session_id_has_uuid_v8_variant_bits` — assert byte 6 high nibble is `0x8` and byte 8 high two bits are `0b10`.
- `domain_tag_byte_sequence_is_pinned` — assert `DOMAIN_TAG` equals the exact 32-byte literal. Catches accidental edits that would silently change every workspace's session id.
- A proptest generating random `[u8; 32]` namespace bytes and verifying stability + v8 bits.

E2E test in `crates/artel-fs/tests/host_resume_session_id.rs` (new file). Cleanest fixture is a single `Pair` (Alice's daemon + Bob's daemon, cross-seeded MemoryLookup) plus an Alice daemon restart in the middle:

1. Spin `Pair`. Alice's `Workspace::host_with` runs against an empty workspace dir. The constructor derives `id1 = session_id_for(doc.id())` and registers it with daemon A. Capture `id1` via `alice_ws.session_id()`. Capture the published ticket via Alice's IPC subscribe loop (existing test pattern).
2. Bob joins via the captured ticket; assert his workspace mirrors Alice's (existing pattern, just enough to prove the live path works).
3. Alice's daemon shuts down. The workspace dir's state (`iroh.key`, `doc-id`, redb store) is untouched. Spawn a fresh Alice daemon at the same state dir.
4. Alice calls `Workspace::host_with` *again* on the same root. The constructor re-opens the persisted `NamespaceId`, computes `id2 = session_id_for(doc.id())`, calls `HostSession { session: Some(id2) }`. Daemon's resume path (1b) reuses the existing record. Assert `alice_ws.session_id() == id1`.
5. Assert that Bob's mirror (still alive in Daemon B, still subscribed to the gossip topic derived from `id1 == id2`) receives a fresh `Send` from Alice after the re-host. **This is the user-visible property; without it the rest is theatre.**
6. (Optional but cheap) assert that the gossip-topic bytes from `id2` equal those from `id1` — `topic_for(id) = id.as_bytes()[..16]` per `gossip_bridge.rs`, which we don't import; recompute inline.

### Definition of done

1. `session_id_for(NamespaceId) -> SessionId` exists; pure, deterministic, v8-tagged, no I/O. Domain tag bytes pinned by test.
2. `Workspace::host_with` signature is `(client, peer, root, policy, config)`. The function opens the namespace, derives the session id, registers with the daemon via `HostSession { session: Some(...) }`, and proceeds with the existing reconcile/scan/publish-ticket flow.
3. `Workspace::session_id() -> SessionId` accessor exposed.
4. `WorkspaceError::SessionConflict(SessionId)` added; mapped from the daemon's protocol-level variant at the request site.
5. Re-hosting the same dir under a fresh daemon lands on the same `SessionId` and same gossip topic; an existing joiner keeps receiving messages from the host across the host's daemon restart (e2e proven).
6. All existing tests migrated to the new `host_with` signature; suite stays green.
7. fmt + clippy clean both feature modes.

**Commit subject:** `artel-fs: derive stable SessionId from NamespaceId; host_with takes ownership of HostSession`

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
