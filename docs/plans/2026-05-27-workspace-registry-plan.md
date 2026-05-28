# Workspace registry — implementation plan

Source brainstorm: `docs/brainstorms/2026-05-27-workspace-registry-brainstorm.md`. This plan is *how*, not *what* — design decisions are the brainstorm's.

This is item 2 of `docs/roadmap.md` § "Multi-session resume across daemon restarts". Item 1 (stable session id across host restarts) landed as commits `f9d6c0a` (1a protocol), `79b3193` (1b daemon), `05b10b2` (1c artel-fs), `b0dc2f5` (1d docs). Item 3 (`Workspace::resume`) is already covered by `Workspace::host_with`'s reattach behaviour and does not need its own slice — it gets noted as DONE in the 2d roadmap pass at the end.

If you are picking this up cold, read in this order:
1. `docs/adr/001-collab-substrate-platform.md` — architectural contract. § "Daemon scope: medium" and § "Versioned message envelope, opaque payload" are load-bearing for the design choices here.
2. `docs/brainstorms/2026-05-27-workspace-registry-brainstorm.md` — what we're building and why.
3. This plan.
4. The shape of stable-session-id slice 1 (`docs/plans/2026-05-26-stable-session-id-plan.md`) — same 4-slice cadence, same code-touch shape; consult when in doubt.

## Engineering principles for this slice

The user has flagged that the project's "no speculative abstractions" memory note is too strong as a blanket rule, and that this slice is the right place to exercise the nuance. Captured here so a fresh agent picking this up has the lens already focused:

**The substrate (daemon + protocol) is foundational.** It is a platform on which other crates are built. Per ADR-001 § "Daemon scope: medium", the daemon owns sessions, peers, persistence, and a small RPC surface. It explicitly does *not* own workspace sync, app-specific message schemas, or AI/agent concerns — payloads are opaque bytes, the daemon never inspects them.

**`artel-fs` is a consumer.** Important and likely the most popular consumer, but a *consumer*. Not a privileged citizen. Future consumers (CRDT-doc apps, hypothetical GUIs, non-Rust clients per ADR-001 § "Non-Rust clients become possible") will sit alongside `artel-fs` at the same layer.

**The "no speculative abstractions" memory still applies inside a layer.** Don't introduce a `Workspace` trait with one impl. Don't ship a `RegistryBackend` trait when the daemon has one storage path. That guidance hasn't changed.

**What is NOT speculative: keeping layer boundaries clean.** Letting `artel-fs`-shaped fields leak into the daemon would be a layering violation, not foresight. The daemon stores `(session_id, kind: String, payload: Vec<u8>)` because that is the *generic primitive* the daemon's own vocabulary supports. The string `kind` is the consumer's namespace; opaque `payload` is the consumer's wire shape. This is the same pattern ADR-001 already established for `SessionMessage` payloads — application-defined `kind` + `action`, opaque `payload`, daemon never inspects.

**Concretely for this slice:** the daemon ships `RegisterAttachment` / `ListAttachments` / `ForgetAttachment`. The verbs do not say "Workspace". The fields do not contain `local_path`. `artel-fs` defines `WorkspaceAttachmentV1` *inside its own crate*, postcard-encodes it into the opaque payload, and tags it with `kind = "artel-fs/workspace/v1"`. A future CRDT-doc crate would tag with its own kind and ship its own payload schema. The daemon learns nothing about either.

**This is alpha — breaking changes are acceptable.** If the single-attachment-per-`(session, kind)` constraint turns out wrong, ripping it out and adding a `name` slot is fine. We don't ship hooks for "future flexibility" unless we know what they're for.

---

Sub-slice ordering is intrinsic: 2a (protocol) → 2b (daemon) → 2c (artel-fs) → 2d (docs). Each is independently mergeable; each ends with green tests, fmt + clippy clean both feature modes (`--all-features` and default), and `cargo doc --workspace` clean of any new warnings.

---

## Sub-slice 2a — Protocol: attachment RPC verbs + `PROTOCOL_VERSION` 2 → 3

**Goal:** Wire-shape change only. Three new `Request` variants, one new `Response` variant carrying a list of attachments, `PROTOCOL_VERSION` ticks 2 → 3. No daemon-side or artel-fs changes in this slice — the new verbs return `ProtocolError::Internal("not implemented")` from the daemon until 2b lands.

Actually, simpler: 2a does *not* land server-side handlers at all. The variants exist on the wire; the daemon's `handle_request` match arm is left as `ProtocolError::Internal("attachment RPCs require daemon support — see slice 2b")` so a v3 client connecting before 2b is deployed gets a clear error rather than a "no such variant" decode failure. This mirrors how `HostSession`'s `session: Option<SessionId>` field shipped in 1a before 1b's resume path landed — protocol changes lead, daemon implementation follows.

### Files touched

- `crates/artel-protocol/src/rpc.rs` — add three `Request` variants and one `Response` variant.

  ```rust
  pub enum Request {
      // ... existing variants ...

      /// Register an opaque attachment against a session.
      ///
      /// `kind` is a consumer-chosen tag (e.g. `"artel-fs/workspace/v1"`)
      /// the daemon uses only for indexing — it never parses
      /// `payload`. Within a `(session, kind)` pair, registering
      /// overwrites any existing entry; this is the idempotent
      /// re-register flow consumers use on restart.
      ///
      /// Returns [`ProtocolError::UnknownSession`] if the session is
      /// not known to the daemon. Attachments cascade-delete with
      /// their session.
      RegisterAttachment {
          /// Session this attachment is bound to.
          session: SessionId,
          /// Consumer-namespaced tag, e.g. `"artel-fs/workspace/v1"`.
          /// Treated as opaque by the daemon.
          kind: String,
          /// Consumer-defined bytes. Daemon never inspects.
          payload: Vec<u8>,
      },

      /// List attachments the daemon knows about.
      ///
      /// `kind` is an exact-match filter. `None` returns every
      /// attachment for every known session. `Some(k)` returns only
      /// attachments tagged with `k`. Order is not specified;
      /// callers that care should sort client-side.
      ListAttachments {
          /// Optional exact-match `kind` filter. `None` = all kinds.
          kind: Option<String>,
      },

      /// Remove an attachment without removing its session.
      ///
      /// Used by consumers that want their entry gone but the
      /// underlying session still alive. Idempotent: forgetting an
      /// attachment that does not exist is `Ok(())`. Forgetting an
      /// attachment whose session doesn't exist is also `Ok(())`
      /// (the cascade already cleared it).
      ForgetAttachment {
          /// Session the attachment is bound to.
          session: SessionId,
          /// Tag of the attachment to remove.
          kind: String,
      },
  }

  pub enum Response {
      // ... existing variants ...

      /// Reply to [`Request::RegisterAttachment`] (success).
      AttachmentRegistered,

      /// Reply to [`Request::ListAttachments`].
      Attachments {
          /// Matching attachments. Order unspecified.
          entries: Vec<Attachment>,
      },

      /// Reply to [`Request::ForgetAttachment`] (success).
      AttachmentForgotten,
  }

  /// One entry in [`Response::Attachments`]. Pure data, no daemon-
  /// side semantics attached.
  #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct Attachment {
      /// Session this attachment is bound to.
      pub session: SessionId,
      /// Consumer-namespaced tag.
      pub kind: String,
      /// Consumer-defined opaque bytes. Use `serde_bytes` mod (see
      /// `SendPayload::payload`) so postcard treats it as bytes
      /// rather than a `Vec<u8>` sequence — saves space and matches
      /// the prior precedent.
      #[serde(with = "send_payload_bytes")]
      pub payload: Vec<u8>,
  }
  ```

  All three new `Request` variants stay under the existing `#[serde(rename_all = "snake_case")]` (externally tagged). Do **not** reach for `#[serde(tag = ..., content = ...)]` per `feedback_postcard_externally_tagged_enums.md`. Same for `Response`. The `Attachment` struct uses the existing `send_payload_bytes` mod (defined later in the same file at ~line 190) for its `payload` field — postcard-bytes-aware in non-human-readable mode, plain `Vec<u8>` in JSON. Tests already cover this pattern; see `SendPayload`.

  Re-export `Attachment` from `crates/artel-protocol/src/lib.rs` alongside the existing `JoinTicket` / `SessionSummary` re-exports.

- `crates/artel-protocol/src/version.rs` — bump `PROTOCOL_VERSION` from `ProtocolVersion::new(2)` to `ProtocolVersion::new(3)`. Update `current_protocol_version_is_two` → `current_protocol_version_is_three`. The version-mismatch path doesn't change shape: an old client (v2) talking to a new daemon (v3) gets the existing `ProtocolError::VersionMismatch`. Same in reverse.

- `crates/artel-protocol/src/error.rs` — **no changes.** `RegisterAttachment` reuses the existing `ProtocolError::UnknownSession(SessionId)` for its rejection path; we don't need a new variant. The brainstorm explicitly chose cascade lifecycle, so "session doesn't exist" is the only natural error path.

### Public API additions

```rust
// artel-protocol::rpc

pub enum Request {
    // ...
    RegisterAttachment { session: SessionId, kind: String, payload: Vec<u8> },
    ListAttachments    { kind: Option<String> },
    ForgetAttachment   { session: SessionId, kind: String },
}

pub enum Response {
    // ...
    AttachmentRegistered,
    Attachments { entries: Vec<Attachment> },
    AttachmentForgotten,
}

pub struct Attachment {
    pub session: SessionId,
    pub kind: String,
    pub payload: Vec<u8>,
}
```

### Tests added

In `crates/artel-protocol/src/rpc.rs::tests`:

- `register_attachment_request_round_trip` — postcard + JSON round-trip with non-empty `kind` and `payload`.
- `list_attachments_request_round_trip_with_kind` — `kind: Some(...)` round-trips.
- `list_attachments_request_round_trip_without_kind` — `kind: None` round-trips. Specifically asserts the wire shape against today's filters with explicit `None` so a future drift is caught.
- `forget_attachment_request_round_trip` — round-trip.
- `attachments_response_round_trip_empty` — empty `entries: Vec<Attachment>`.
- `attachments_response_round_trip_multi_kind` — three `Attachment`s with distinct `kind` values, one with empty `payload` (boundary), one with a 64 KiB payload (size sanity, not pinning a max).
- `attachment_payload_round_trip_postcard_uses_bytes_encoding` — assert the postcard-encoded form of an `Attachment` with payload `vec![0xAB; 4]` is no larger than `4 + ~few-bytes-of-tag` (regression guard: `Vec<u8>` without `serde_bytes` would balloon to one byte per element).
- Update `arb_request()` and `arb_response()` proptest generators in the existing block to include the new variants (and `Attachment` for the response). Existing `request_round_trip` / `wire_message_round_trip` proptests pick this up automatically.

In `crates/artel-protocol/src/version.rs::tests`:

- Rename `current_protocol_version_is_two` → `current_protocol_version_is_three` and update its assertion.

### Backwards-compat note

Strictly an additive change at the postcard level for the `Request` and `Response` enums: new variants are unrecognised by older clients but the wire boundary surfaces as `VersionMismatch` at `Hello` time so old clients never reach the variant-decode path. The bump 2 → 3 is required because we're adding new `Response` variants that an old client wouldn't recognise (`AttachmentRegistered`, `Attachments`, `AttachmentForgotten`). If we ever want additive `Response` variants without a bump, ADR-001 § "Multi-version daemon coexistence" becomes load-bearing — explicitly out of scope.

### Definition of done

1. `Request::{RegisterAttachment, ListAttachments, ForgetAttachment}` exist with the exact shape above; postcard + JSON + proptest round-trips green.
2. `Response::{AttachmentRegistered, Attachments, AttachmentForgotten}` exist; round-trips green; `Attachment` struct round-trips via the bytes-aware `payload` encoding.
3. `PROTOCOL_VERSION == 3`. Version-mismatch path produces `VersionMismatch` at the existing wire shape.
4. `Attachment` re-exported from `artel-protocol` lib root.
5. fmt + clippy clean both feature modes; `cargo doc --workspace` builds clean.

**Commit subject:** `protocol: add attachment RPCs (RegisterAttachment / ListAttachments / ForgetAttachment, PROTOCOL_VERSION 3)`

---

## Sub-slice 2b — Daemon: registry storage + handler wiring + cascade

**Goal:** The daemon stores attachments on disk under each session's directory, indexes them for `ListAttachments`, hooks the cascade into `store.delete(session)`, and wires the three new request variants into `handle_request`.

### Storage layout

Add to `crates/artel-daemon/src/store/fs.rs`'s `FsLogStore`. Per-session disk layout becomes:

```text
sessions_dir/
  <session-uuid>/
    meta.json       — host, members, head (today; unchanged)
    log             — append-only postcard frames (today; unchanged)
    attachments/    — NEW. One file per kind.
      <kind-encoded>.bin
```

**Filename scheme.** `kind` is an arbitrary string the consumer chose; we cannot use it raw as a filename (`/`, control chars, casing collisions on macOS). Encode each filename as **lowercase hex of the kind's UTF-8 bytes**, suffix `.bin`. For the canonical `artel-fs/workspace/v1` kind, the filename is `61727465...`. Hex is chosen over base64 for case-insensitive-fs robustness; over base32 because we don't need brevity (these are not user-visible). Reverse-decoded on `load_all`.

**File format.** Each file contains the raw `payload: Vec<u8>` bytes only. The `kind` is recovered from the filename (decode hex), the `session` from the parent dir name. No header, no length prefix, no postcard envelope — the file *is* the payload. This matches the brainstorm's "daemon never parses payload" rule literally: the daemon reads the file as bytes and ships them over the wire.

**Atomicity.** Same `path.tmp` + fsync + rename pattern the existing `write_meta` already uses. Add a new private `fn write_attachment(path: &Path, payload: &[u8])` helper modeled on `write_meta`. Apply `chmod` to `0o600` post-rename.

**Cascade.** `FsLogStore::delete` already does `remove_dir_all(session_dir)`, which transparently removes the `attachments/` subdir too. No code change required for the cascade itself — it falls out of the existing recursive delete. Add an explicit test (see below) so a future refactor doesn't regress this.

### `SessionStore` trait extensions

In `crates/artel-daemon/src/store/mod.rs`, add three methods to the existing `SessionStore` trait:

```rust
#[async_trait::async_trait]
pub(crate) trait SessionStore: Send + Sync + std::fmt::Debug {
    // ... existing methods ...

    /// Persist (or overwrite) an attachment payload for `(session, kind)`.
    /// Returns `Ok(false)` if the session is not known to the store —
    /// the caller maps this to `ProtocolError::UnknownSession`. Returns
    /// `Ok(true)` on success.
    async fn put_attachment(
        &self,
        session: SessionId,
        kind: &str,
        payload: &[u8],
    ) -> io::Result<bool>;

    /// List every attachment matching `kind_filter`. `None` returns
    /// all kinds; `Some(k)` returns only attachments whose `kind == k`.
    /// Order unspecified. The returned `Vec<StoredAttachment>` is
    /// pure data; transport-side `Attachment` is built from it in the
    /// server.
    async fn list_attachments(
        &self,
        kind_filter: Option<&str>,
    ) -> io::Result<Vec<StoredAttachment>>;

    /// Remove the attachment at `(session, kind)`. Idempotent: missing
    /// session OR missing attachment file is `Ok(())`.
    async fn delete_attachment(&self, session: SessionId, kind: &str) -> io::Result<()>;
}

/// Pure-data record returned by `list_attachments`. Distinct from
/// `artel_protocol::Attachment` so the store stays free of protocol
/// types (matching how `SessionRecord` is independent of `Response`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredAttachment {
    pub(crate) session: SessionId,
    pub(crate) kind: String,
    pub(crate) payload: Vec<u8>,
}
```

Justification for the `bool` return on `put_attachment`: the daemon needs to distinguish "session doesn't exist" (→ `ProtocolError::UnknownSession`) from "I/O failed" (→ `ProtocolError::Internal`). Bubbling `io::Error` doesn't capture the "no such session" case cleanly. The alternative — returning `Result<(), AttachmentPutError>` — is more typing for one bit of information; sticking with `Result<bool, io::Error>` keeps the trait minimal.

### `MemoryStore` and `FsLogStore` implementations

**`MemoryStore`** (`crates/artel-daemon/src/store/memory.rs`):
- Add a field `attachments: tokio::sync::RwLock<HashMap<(SessionId, String), Vec<u8>>>`. Keep the field private and the type local — no new public surface.
- `put_attachment`: check `sessions.read().await.contains_key(&session)` → `Ok(false)` if absent. Otherwise `attachments.write().await.insert((session, kind.into()), payload.into())` → `Ok(true)`.
- `list_attachments`: snapshot the map; filter by `kind_filter`; convert each `((session, kind), payload)` into `StoredAttachment`.
- `delete_attachment`: `attachments.write().await.remove(&(session, kind.into()))`. Always `Ok(())`.
- Cascade: extend the existing `delete(session)` body to also do `attachments.write().await.retain(|(s, _), _| *s != session)`. Same atomicity guarantees the trait already promises (write happens under the same lock pattern as the rest of `MemoryStore`).

**`FsLogStore`** (`crates/artel-daemon/src/store/fs.rs`):
- `put_attachment`: blocking task. Check `session_dir(session).is_dir()` for the existence test (it's the same condition `load_all` uses). Create `session_dir/attachments/` if missing (mode `0o700`). Atomic write of `<kind-hex>.bin` (mode `0o600`). Return `Ok(true)` / `Ok(false)`.
- `list_attachments`: blocking task. Iterate `sessions_dir`, for each session subdir iterate `attachments/`, hex-decode the filename, filter by `kind_filter`, read bytes, build `StoredAttachment`. Skip files whose name doesn't hex-decode (warn + continue, same pattern as the existing `load_all` for unparseable session dirs). Skip files larger than `MAX_FRAME_SIZE` (16 MiB; same cap the log uses). The existence of the cap is documented in the trait doc-comment so consumers know they can't ship arbitrary blobs.
- `delete_attachment`: blocking task. `std::fs::remove_file(session_dir/attachments/<kind-hex>.bin)`. Map `NotFound` → `Ok(())`. Other errors propagate.
- Cascade: no change. `delete()` already does `remove_dir_all(session_dir)`, which sweeps `attachments/`.

### Server wiring

In `crates/artel-daemon/src/server.rs`, add three arms to `handle_request`. Skim the existing `Subscribe` and `LeaveSession` arms for the pattern (both go through `Registry`, both translate `SessionError` via `session_error_to_protocol`).

Decision point: do attachments live under `Registry` or directly on the server?

**Place attachments on `Registry`.** Reasons:
- The cascade is in `Registry`'s domain — `Registry::leave_session` (host) and `Registry::handle_session_closed` (remote mirror) are already where `store.delete(session)` is called. Cascade has to be a `Registry` concern; spreading it across `Registry` and `server` would be a layering smell.
- The existence check for `RegisterAttachment` (does the session exist?) is exactly `Registry::sessions.contains_key(session)` — already a `Registry` guard.
- Tests reuse the existing `Registry::load`-with-`MemoryStore` fixture pattern from `session.rs::tests` (line ~896). Fits naturally.

Add three `Registry` methods:

```rust
impl Registry {
    pub(crate) async fn register_attachment(
        &self,
        session: SessionId,
        kind: String,
        payload: Vec<u8>,
    ) -> Result<(), SessionError> {
        match self.store.put_attachment(session, &kind, &payload).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(SessionError::UnknownSession(session)),
            Err(err) => Err(SessionError::Storage(err)),
        }
    }

    pub(crate) async fn list_attachments(
        &self,
        kind_filter: Option<&str>,
    ) -> Result<Vec<StoredAttachment>, SessionError> {
        self.store
            .list_attachments(kind_filter)
            .await
            .map_err(SessionError::Storage)
    }

    pub(crate) async fn forget_attachment(
        &self,
        session: SessionId,
        kind: String,
    ) -> Result<(), SessionError> {
        self.store
            .delete_attachment(session, &kind)
            .await
            .map_err(SessionError::Storage)
    }
}
```

`SessionError` does not need a new variant — `UnknownSession` already exists and carries the `SessionId`. `session_error_to_protocol` already maps it to `ProtocolError::UnknownSession`.

In `server.rs::handle_request`, add the three arms. Each translates `Vec<StoredAttachment>` → `Vec<artel_protocol::Attachment>` (a one-line `.into_iter().map(...).collect()`); this is the only place protocol types meet store types, mirroring how `Response::Subscribe { messages }` converts.

### Cascade hook — sanity check

The cascade is "free" in the FS store via `remove_dir_all`, and explicitly coded in the memory store. But the reasoning needs to be visible at the `Registry` level so a future contributor changing `delete` semantics doesn't accidentally break it. Add a doc-comment on `SessionStore::delete`:

```rust
/// Forget the session entirely. Used when the host leaves.
///
/// **Cascade invariant:** any attachments associated with `session`
/// must be removed atomically with the session itself. The on-disk
/// implementation gets this for free via `remove_dir_all`; the
/// in-memory implementation must explicitly clear them.
async fn delete(&self, session: SessionId) -> io::Result<()>;
```

### Tests added

Store tests in `crates/artel-daemon/src/store/fs.rs::tests`:
- `put_then_list_attachment_round_trips` — create a session, `put_attachment` with a non-empty payload, `list_attachments(None)` returns it, payload bytes match.
- `put_attachment_overwrites_existing_at_same_kind` — put twice; list returns the second payload.
- `put_attachment_for_unknown_session_returns_false` — no `create()` first; `put_attachment` returns `Ok(false)`. No file created on disk.
- `list_attachments_filters_by_kind_exact_match` — three attachments with two kinds; filter on each kind returns the right subset.
- `list_attachments_returns_empty_when_no_attachments` — sessions exist but no attachments.
- `delete_attachment_is_idempotent` — call twice; second call is `Ok(())`.
- `delete_attachment_on_unknown_session_is_ok` — never called `create`; `delete_attachment` is `Ok(())`.
- `delete_session_cascades_attachments` — put two attachments, `delete(session)`, list returns empty, attachments dir gone from disk.
- `attachment_filename_is_hex_encoded` — assert the on-disk filename is `lowercase-hex(utf8 bytes of kind) + ".bin"`. Pins the contract.
- `non_hex_attachment_filenames_are_skipped_with_warning` — manually drop a `not-hex.bin` into `attachments/`; `list_attachments` doesn't include it and doesn't error.
- `oversized_attachment_payload_is_skipped` — write a >16 MiB file directly; `list_attachments` skips it.

Equivalent tests in `crates/artel-daemon/src/store/memory.rs::tests` covering the in-memory path. Same names where applicable.

Registry tests in `crates/artel-daemon/src/session.rs::tests` (using the existing `MemoryStore` fixture pattern):
- `register_attachment_persists_via_store` — happy path; assert `MemoryStore` shows the entry.
- `register_attachment_for_unknown_session_returns_unknown_session_error` — assert `SessionError::UnknownSession`.
- `list_attachments_returns_entries_across_multiple_sessions` — two sessions, one attachment each, list with `None`.
- `forget_attachment_removes_entry` — register, forget, list returns empty.
- `cascade_removes_attachments_when_host_leaves` — host registers an attachment, host calls `LeaveSession` (via `Registry::leave_session`), assert `list_attachments` is empty afterwards.
- `cascade_removes_attachments_when_remote_session_closes` — same shape as above but exercising the `SessionKind::Remote` `handle_session_closed` path.

Server-level integration tests in `crates/artel-daemon/tests/`. Make a new file `tests/attachments.rs`:
- `register_then_list_round_trips_via_ipc` — happy path through `Client::request`. One client, one session, one attachment.
- `list_attachments_filters_by_kind_via_ipc` — register two, filter on one.
- `register_attachment_unknown_session_surfaces_unknown_session_error` — `Client::request` returns `Err(ClientError::Protocol(ProtocolError::UnknownSession))`.
- `forget_attachment_is_idempotent_via_ipc` — call twice, second call is `Ok`.
- `attachments_persist_across_daemon_restart` — same shape as `crates/artel-daemon/tests/persistence.rs`. Daemon A starts, hosts a session, registers an attachment, daemon A stops. Daemon B starts against the same `state_dir`. Daemon B's `ListAttachments` returns the entry. **This is the load-bearing user-visible property** — a fresh agent should run this test first when picking up 2b.
- `attachments_cascade_when_host_leaves_via_ipc` — same as the unit cascade test but via the IPC boundary.

### Definition of done

1. `SessionStore` trait extended with `put_attachment` / `list_attachments` / `delete_attachment`. Both `MemoryStore` and `FsLogStore` implementations green.
2. `FsLogStore` persists attachments under `sessions_dir/<session>/attachments/<kind-hex>.bin` with the same `0o700` / `0o600` permission discipline the rest of the directory uses.
3. Cascade verified: `delete(session)` removes attachments via the `remove_dir_all` path on disk, and via the in-memory `retain` on the memory store.
4. `Registry::register_attachment` / `list_attachments` / `forget_attachment` exist and use `SessionError::{UnknownSession, Storage}` — no new error variant.
5. Server arms wired in `handle_request`; protocol-error mapping uses the existing `session_error_to_protocol`.
6. Unit tests (store, registry) + IPC integration tests pass. The cross-daemon-restart test specifically pins the persistence property.
7. fmt + clippy clean both feature modes.

**Commit subject:** `daemon: per-session attachments — store + registry + cascade + IPC handlers`

---

## Sub-slice 2c — artel-fs: register `WorkspaceAttachmentV1` on host/join

**Goal:** `Workspace::host_with` and `Workspace::join_with` register an `artel-fs/workspace/v1` attachment with the daemon after a successful attach. A `Workspace::list_known_workspaces(client)` helper exposes the typed view back to consumers without making them postcard-decode the opaque payload themselves.

### Why a typed helper, and where it lives

Per the layering principle at the top of this plan: the daemon ships `Vec<Attachment>`, opaque payloads. `artel-fs` consumers shouldn't have to learn about `kind` strings, `serde_bytes`, or postcard-decoding to enumerate workspaces. A small helper inside `artel-fs` is the right shape — it's a *consumer-side* convenience, not an abstraction.

This is the spot where the "no speculative abstractions" memory note reads as too strong without the layering nuance. We're not introducing a `Backend` trait or an `AttachmentCodec` interface. We're shipping a typed read function that decodes the v1 payload, in the crate that owns the v1 schema, with one impl. That's idiomatic, not speculative.

### Files touched

- `crates/artel-fs/src/lib.rs` — declare `pub mod attachment;` and re-export the public surface (`WorkspaceAttachmentV1`, `WorkspaceRole`, `KIND_V1`, `list_known_workspaces`).

- `crates/artel-fs/src/attachment.rs` — **new module.**

  ```rust
  //! `artel-fs` attaches a small typed record to its session via
  //! `Request::RegisterAttachment` so a CLI / GUI / future tool can
  //! enumerate the workspaces the daemon knows about without reading
  //! `~/.artel/` filesystem state directly.
  //!
  //! The daemon stores the payload opaquely; the schema lives here.

  use std::path::PathBuf;

  use artel_client::{Client, ClientError};
  use artel_protocol::{Request, Response};
  use serde::{Deserialize, Serialize};

  use crate::error::WorkspaceError;

  /// Kind tag for the v1 schema. Bumping this is a breaking change;
  /// add a parallel `KIND_V2` and migrate consumers explicitly.
  pub const KIND_V1: &str = "artel-fs/workspace/v1";

  /// Whether this side of the workspace was the host or a joiner.
  #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
  #[serde(rename_all = "snake_case")]
  pub enum WorkspaceRole {
      Host,
      Joiner,
  }

  /// Wire-stable v1 payload for the `artel-fs/workspace/v1`
  /// attachment kind. Postcard-encoded into the opaque
  /// `Attachment::payload`.
  ///
  /// Schema is frozen for `KIND_V1` — postcard rejects payloads
  /// whose field count doesn't match exactly (no `serde(default)`
  /// honoured for missing trailing fields). New fields require a
  /// parallel `KIND_V2` + struct + helper; consumers query both
  /// kinds and merge. See the brainstorm's `last_seen` fast-follow
  /// for the first such planned bump.
  #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct WorkspaceAttachmentV1 {
      /// Canonicalised workspace root (where the user's files live).
      pub local_path: PathBuf,
      /// Resolved state directory (default `<local_path>/.artel-fs/`,
      /// or whatever `WorkspaceConfig::state_dir` set).
      pub state_dir: PathBuf,
      /// Whether this side hosts or joined.
      pub role: WorkspaceRole,
  }

  impl WorkspaceAttachmentV1 {
      /// Postcard-encode for shipping inside `Attachment::payload`.
      pub(crate) fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
          postcard::to_allocvec(self)
              .map_err(|e| WorkspaceError::Iroh(format!("attachment encode: {e}")))
      }

      /// Postcard-decode from an `Attachment::payload`.
      pub fn decode(bytes: &[u8]) -> Result<Self, WorkspaceError> {
          postcard::from_bytes(bytes)
              .map_err(|e| WorkspaceError::Iroh(format!("attachment decode: {e}")))
      }
  }

  /// One workspace as the daemon knows it: the underlying session id
  /// plus the decoded v1 payload.
  #[derive(Clone, Debug, PartialEq, Eq)]
  pub struct KnownWorkspace {
      /// Session id this workspace is attached to.
      pub session: artel_protocol::SessionId,
      /// Decoded payload.
      pub attachment: WorkspaceAttachmentV1,
  }

  /// List every `artel-fs/workspace/v1` workspace the daemon knows.
  ///
  /// Skips entries whose payload fails to decode (logged via
  /// `tracing::warn!`); they could be stragglers from a future kind
  /// version that this build doesn't speak. A future `list_v2` would
  /// be a sibling helper, not a flag on this one.
  pub async fn list_known_workspaces(
      client: &Client,
  ) -> Result<Vec<KnownWorkspace>, WorkspaceError> {
      let resp = client
          .request(Request::ListAttachments {
              kind: Some(KIND_V1.to_string()),
          })
          .await
          .map_err(WorkspaceError::Client)?;
      let entries = match resp {
          Response::Attachments { entries } => entries,
          other => {
              return Err(WorkspaceError::Iroh(format!(
                  "unexpected response to ListAttachments: {other:?}",
              )));
          }
      };
      let mut out = Vec::with_capacity(entries.len());
      for entry in entries {
          // Defence-in-depth: filter actually applied server-side, but
          // confirm. Also tolerant of future `kind` siblings the
          // server returns for a None filter.
          if entry.kind != KIND_V1 {
              continue;
          }
          match WorkspaceAttachmentV1::decode(&entry.payload) {
              Ok(att) => out.push(KnownWorkspace {
                  session: entry.session,
                  attachment: att,
              }),
              Err(err) => {
                  tracing::warn!(
                      session = %entry.session,
                      error = %err,
                      "skipping undecodeable v1 workspace attachment",
                  );
              }
          }
      }
      Ok(out)
  }
  ```

  Note the encode/decode pair: `encode` is `pub(crate)` (only `host_with`/`join_with` should encode); `decode` is `pub` so test fixtures and consumers can decode arbitrary `Attachment::payload` bytes (e.g. for diagnostics). Same shape as `crate::ticket::decode`.

- `crates/artel-fs/src/workspace.rs`:
  - `Workspace::host_with`, between `register_host` returning successfully and the existing `if returning { reconcile_doc_against_disk }` block, register the attachment:
    ```rust
    let join_ticket = register_host(client, peer, session_id).await?;

    // Register a typed attachment so a CLI / GUI can enumerate
    // this workspace without reading `~/.artel/` directly. The
    // daemon stores it opaquely (see ADR-001 § "Daemon scope:
    // medium"); the schema lives in `crate::attachment`.
    register_workspace_attachment(
        client,
        session_id,
        &root,
        &state_dir,
        WorkspaceRole::Host,
    )
    .await?;
    ```
  - Same call shape inside `Workspace::join_with`, after the join succeeds and before the workspace handle is built. Use `WorkspaceRole::Joiner`.
  - Add the helper:
    ```rust
    async fn register_workspace_attachment(
        client: &Client,
        session: SessionId,
        local_path: &Path,
        state_dir: &Path,
        role: crate::attachment::WorkspaceRole,
    ) -> Result<(), WorkspaceError> {
        let payload = crate::attachment::WorkspaceAttachmentV1 {
            local_path: local_path.to_path_buf(),
            state_dir: state_dir.to_path_buf(),
            role,
        }
        .encode()?;
        let resp = client
            .request(Request::RegisterAttachment {
                session,
                kind: crate::attachment::KIND_V1.to_string(),
                payload,
            })
            .await
            .map_err(WorkspaceError::Client)?;
        match resp {
            Response::AttachmentRegistered => Ok(()),
            other => Err(WorkspaceError::Iroh(format!(
                "unexpected response to RegisterAttachment: {other:?}",
            ))),
        }
    }
    ```

  Failure mode: if `RegisterAttachment` fails, `host_with` / `join_with` propagates the error and the workspace doesn't come up. This matches the existing pattern (we already propagate `register_host` and `publish_ticket` failures). The brainstorm doesn't argue for graceful degradation — a workspace whose attachment never landed is invisible to discovery, which is a real bug we want to surface, not paper over. Document this in the function doc-comment.

- `crates/artel-fs/src/error.rs` — no new variant. `WorkspaceError::Client` and `WorkspaceError::Iroh` cover the cases.

### Public API additions

```rust
// artel-fs::attachment (re-exported from lib.rs)
pub const KIND_V1: &str;
pub enum WorkspaceRole { Host, Joiner }
pub struct WorkspaceAttachmentV1 { local_path: PathBuf, state_dir: PathBuf, role: WorkspaceRole }
impl WorkspaceAttachmentV1 {
    pub fn decode(bytes: &[u8]) -> Result<Self, WorkspaceError>;
}
pub struct KnownWorkspace { pub session: SessionId, pub attachment: WorkspaceAttachmentV1 }
pub async fn list_known_workspaces(client: &Client) -> Result<Vec<KnownWorkspace>, WorkspaceError>;
```

### Migration of existing call sites

Every test that calls `Workspace::host_with` / `join_with` continues to work unchanged — the new `RegisterAttachment` round trip is internal to those constructors, transparent to callers. **Two confirmation points** for a fresh agent:

1. `Workspace::host_with` issues *two* IPC round-trips (HostSession + RegisterAttachment) where today it does one (just HostSession). For tests that subscribe to event streams *between* these calls, this could in principle race. None of the existing tests do this — confirm by running `cargo test -p artel-fs --all-features` and watching for new flakes. The 2c-2c era of work taught us that adding an IPC step can shake out latent ordering assumptions; budget some debug time here.
2. The order matters: register the attachment *after* `register_host` (which produces the session id) and *before* the long-running scan + ticket publish (so a user-visible failure during scan doesn't leave a registered-but-never-actually-functional workspace dangling). The cascade in 2b removes the attachment if the host then `LeaveSession`s, but *not* if the host crashes mid-scan; that's acceptable for v1 — the `last_seen` fast-follow will let consumers prune stale entries.

### Tests added

Unit tests in `crates/artel-fs/src/attachment.rs::tests`:
- `workspace_attachment_v1_round_trips_postcard` — encode + decode, byte-equal output.
- `workspace_attachment_v1_decode_rejects_garbage_bytes` — `decode(b"not postcard")` returns `Err(...)`.
- `workspace_attachment_v1_decode_rejects_truncated_bytes` — encode, truncate by 3 bytes, decode errors. Pins the "no graceful partial decode" behaviour.
- `kind_v1_string_is_pinned` — `assert_eq!(KIND_V1, "artel-fs/workspace/v1")`. Catches accidental edits.

Workspace-level tests in `crates/artel-fs/src/workspace.rs::tests`:
- None (the constructor calls require a live daemon — covered by integration tests below).

E2E test in `crates/artel-fs/tests/workspace_attachment.rs` (new file, mirrors the shape of `tests/host_resume_session_id.rs`):
- `host_workspace_registers_attachment_via_ipc` — single iroh-disabled daemon harness (the attachment IPC needs no iroh), Alice hosts, fetch via `Request::ListAttachments { kind: Some(KIND_V1.into()) }`, assert one entry, assert `WorkspaceAttachmentV1::decode` round-trips with `role: Host` and matching `local_path` / `state_dir`.
- `join_workspace_registers_attachment_via_ipc` — `Pair` fixture (cross-seeded). Alice hosts, Bob joins, both clients independently `ListAttachments`. Each should see *its own daemon's* attachment for its own role. Assert Alice sees `Host`, Bob sees `Joiner`.
- `list_known_workspaces_helper_returns_typed_view` — same fixture as test 1, but go through the typed `list_known_workspaces` helper instead of raw IPC. Assert `KnownWorkspace { session, attachment }` shape.
- `attachment_persists_across_daemon_restart` — Alice's daemon hosts, registers an attachment, daemon shuts down, fresh daemon at the same `state_dir`. The session resumes (1c property), the attachment is still listable, and `list_known_workspaces` returns the same `local_path`. **This is the user-visible end-to-end property of slices 1+2 combined** and is the test a fresh agent should run last to confirm the slice landed.
- `attachment_removed_on_host_leave_session` — Alice hosts, registers, then issues `Request::LeaveSession`. Subsequent `ListAttachments` returns empty. Pins the cascade behaviour from the IPC consumer's view.

### Definition of done

1. `crate::attachment` module exists with `WorkspaceAttachmentV1`, `WorkspaceRole`, `KIND_V1`, `KnownWorkspace`, `list_known_workspaces`. Re-exported from lib.rs.
2. `Workspace::host_with` and `Workspace::join_with` register their attachment after a successful attach. Failure to register propagates as `WorkspaceError`.
3. The wire `Attachment::payload` is the postcard encoding of `WorkspaceAttachmentV1` (verified by the round-trip e2e test).
4. `list_known_workspaces` returns a typed `Vec<KnownWorkspace>` filtered to v1.
5. Cross-daemon-restart e2e test green — the load-bearing property.
6. All existing `artel-fs` tests still pass (the new IPC round-trip in `host_with`/`join_with` doesn't introduce flakes).
7. fmt + clippy clean both feature modes.

**Commit subject:** `artel-fs: register WorkspaceAttachmentV1 on host/join; list_known_workspaces helper`

---

## Sub-slice 2d — Documentation

**Goal:** Roadmap item 2 marked done; ADR-001 addendum noting the new RPC verbs and `PROTOCOL_VERSION` 3; roadmap item 3 (`Workspace::resume`) marked done with a pointer to the existing `host_with` reattach behaviour (no slice needed).

### Files touched

- `docs/roadmap.md` — under § "Multi-session resume across daemon restarts":
  - Item 2 (workspace registry): strike through, add a "DONE" line referencing the brainstorm + plan and naming the wire surface (`RegisterAttachment` / `ListAttachments` / `ForgetAttachment`, `PROTOCOL_VERSION` 2 → 3).
  - Item 3 (`Workspace::resume`): mark done in place — the existing item-3 prose already notes that `host_with` covers this. Add a one-liner pointer to the 2d commit and close the section.
  - "Stale-daemon detection and cleanup" remains open as before.

- `docs/adr/001-collab-substrate-platform.md`:
  - Header `**Updated**:` line: append `; 2026-MM-DD (workspace registry — see "Updates" below)` (use the slice's actual landing date).
  - § "Updates" (the section added in 1d): append a new dated subsection. One paragraph. Three sentences max:
    1. New RPC verbs and what they do (one-line summary).
    2. The opaque-`kind`/opaque-`payload` decision and why it's the same pattern as `SessionMessage`.
    3. The `PROTOCOL_VERSION` 2 → 3 bump and that the verb count in § "Daemon scope: medium" is unchanged because the verbs are *attachment-shaped*, not workspace-shaped.

  The ADR's RPC enumeration ("`host_session`, `join_session`, …") in § "Daemon scope: medium" is *not* edited inline. The "Updates" section is the only place changes accumulate post-acceptance. This is the discipline 1d established.

### Tests added

None — documentation only.

### Definition of done

1. Roadmap item 2 (workspace registry) marked done with cross-links.
2. Roadmap item 3 (`Workspace::resume`) marked done in place with a pointer to `host_with`'s existing reattach behaviour (no separate slice).
3. ADR-001 "Updates" section gains a new dated subsection.
4. `cargo doc --workspace` builds clean.

**Commit subject:** `docs: mark workspace-registry roadmap item done; ADR-001 addendum for PROTOCOL_VERSION 3`

---

## Cross-cutting concerns

### Things this plan explicitly does not do

- **No multi-attachment-per-(session, kind).** Single slot per `(session, kind)`. If a use case for multi-slot shows up, ripping out and adding a `name` field is acceptable; alpha.
- **No `last_seen: i64` field on `WorkspaceAttachmentV1`.** Fast-follow per the brainstorm. Adding it requires a parallel `KIND_V2` + `WorkspaceAttachmentV2` (postcard rejects field-count mismatches, so `serde(default)` won't paper over an additive field on `KIND_V1`).
- **No CLI `artel workspace list` verb.** Out of scope for the registry slice; ships separately on top of `list_known_workspaces`.
- **No GUI / non-Rust client work.** The IPC surface is what this slice ships; client work is a downstream concern.
- **No `RegistryBackend` trait, no `AttachmentCodec` trait, no plugin system.** Per the layering principle at the top: the daemon has one storage path (`SessionStore`), `artel-fs` has one schema (`WorkspaceAttachmentV1`). Future schemas land as parallel kinds, not via runtime dispatch.
- **No N-1 protocol-version compatibility.** A v2 client talking to a v3 daemon (or vice versa) gets `VersionMismatch`. ADR-001 § "Multi-version daemon coexistence" is explicitly future work.
- **No filesystem migration of pre-2b daemons.** A daemon upgraded across the 2b boundary simply has no attachments yet; the first `Workspace::host_with` after upgrade registers one. No on-disk format migration, no version field in the attachment file format.
- **No Windows.** Per `project_unix_only_for_now.md`. The new code is platform-neutral but tested only on macOS + Linux CI.

### Risks

1. **The IPC round-trip count in `host_with` / `join_with` increases.** Today: 1 (HostSession). After 2c: 2 (HostSession + RegisterAttachment). Joiners go from 2 (Subscribe + envelope-wait) to 3 (Subscribe + envelope-wait + RegisterAttachment). Latency should be sub-ms over the local Unix socket but if a test was relying on the precise event ordering between `host_with` returning and a follow-up `Subscribe`, it could go flaky. Mitigation: the 2c migration step explicitly lists this as a confirmation point; the existing 1c migration of 16 tests was a reference for how to spot that pattern.

2. **Hex-encoded filenames are not user-readable.** A developer poking around `~/.artel/sessions/<uuid>/attachments/` sees `61727465...bin` not `artel-fs/workspace/v1.bin`. Acceptable for v1 — the daemon's state dir is not a user-facing surface. If it bites, the next iteration can ship a `kind.txt` sidecar (encoded filename → original kind) at the cost of one extra file per attachment. Don't ship that today.

3. **Cascade-via-`remove_dir_all` is implicit.** A future contributor refactoring `FsLogStore::delete` could break the cascade silently. Mitigation: the `delete_session_cascades_attachments` test pins the property, and the doc-comment on `SessionStore::delete` calls out the invariant.

4. **`list_attachments` reads every attachment file from disk.** For workloads with thousands of sessions × many kinds, this is wasteful when the caller only wants one kind. v1 doesn't pre-index by `kind`; `list_attachments(Some(kind))` still walks all session dirs. Acceptable for now — the daemon doesn't scale to thousands of sessions in any other dimension either. Optimisation is a future concern.

5. **`ForgetAttachment` racing with cascade-via-`LeaveSession`.** Both can plausibly run concurrently. The current locking on `Registry::sessions` serialises `LeaveSession`'s `store.delete`; `forget_attachment` doesn't take the same lock. Worst case: forget runs against a session that's about to be cascade-deleted; the file may already be gone (handled — `delete_attachment` returns `Ok(())` on `NotFound`). Mitigation: tests cover the idempotent-delete path; no extra synchronisation needed.

6. **Postcard-decode failures in `list_known_workspaces` are silently skipped.** A future `KIND_V1` schema breakage (adding a required field) would surface as warn-and-skip, not an error. This is the conservative choice — a single bad payload shouldn't take down enumeration — but it does mean a bug that corrupts payloads goes unnoticed unless someone reads logs. Mitigation: `list_known_workspaces` uses `tracing::warn!` so a structured-logging consumer can alert. If we ever want strict mode, a sibling `list_known_workspaces_strict` is the additive way.

7. **Joiner-side `LeaveSession` does not cascade the joiner's attachment.** `Registry::leave` only invokes `store.delete(session)` when the leaver is the host (which closes the session). For a joiner, it calls `store.remove_member` and the session record (and therefore the attachment) lingers — `list_known_workspaces` keeps reporting the workspace even after the joiner has left. Pinned by `joiner_leave_session_does_not_cascade_attachment_today` as a fail-loud regression-trap: the day a fix lands the test will fail and force the brainstorm/plan/test to be re-aligned. Two reasonable closes (each a separate slice): (a) `Workspace::shutdown` issues an explicit `ForgetAttachment` before tearing down; (b) `Registry::leave` for a non-host on a remote-mirror with no other local consumers fully drops the mirror (which would also cascade the attachment via the existing 2b `remove_dir_all`).

---

## Critical files for implementation

Substrate side:
- `crates/artel-protocol/src/rpc.rs`
- `crates/artel-protocol/src/lib.rs`
- `crates/artel-protocol/src/version.rs`
- `crates/artel-daemon/src/store/mod.rs`
- `crates/artel-daemon/src/store/fs.rs`
- `crates/artel-daemon/src/store/memory.rs`
- `crates/artel-daemon/src/session.rs`
- `crates/artel-daemon/src/server.rs`
- `crates/artel-daemon/tests/attachments.rs` (new)

Consumer side:
- `crates/artel-fs/src/lib.rs`
- `crates/artel-fs/src/attachment.rs` (new)
- `crates/artel-fs/src/workspace.rs`
- `crates/artel-fs/tests/workspace_attachment.rs` (new)

Docs:
- `docs/roadmap.md`
- `docs/adr/001-collab-substrate-platform.md`

(All paths relative to the workspace root.)
