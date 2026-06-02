# Auth Slice B — L3 per-message signing — implementation plan

Source brainstorm: `docs/brainstorms/2026-05-30-auth-story-brainstorm.md`. The
brainstorm picks **per-message ed25519 signatures** scoped over `(version,
session_id, timestamp_ms, peer, kind, action, payload)` — `seq` is host-
assigned and explicitly excluded. The signing key is the iroh endpoint secret
(`~/.artel/iroh.key`); the verifying key is derivable from `peer.id` because
Slice A collapsed `PeerId` onto `EndpointId`. This plan is *how*, not *what*.

This is **Slice B** of the v1 auth story. **Slice A (L1 collapse) shipped on
2026-05-30** (`docs/plans/2026-05-30-auth-l1-peer-id-collapse-plan.md`) and
its IPC closure (auth L1 fix #3) shipped on 2026-06-01 (commits `21fac44`,
`a0ead33`, `b667668`, `PROTOCOL_VERSION 5`). **Slice C (L2 capability events)**
depends on this slice landing first because the cap events are themselves
signed messages.

## Where the architectural weight lives

The non-obvious detail: **the joiner-side daemon must sign — not the host.**
Today every `SessionMessage` is constructed exactly once, on the host, in
`Registry::send` at `crates/artel-daemon/src/session.rs:932`. That is fine
when the host is also the message *author* (host-originated `Send`), but for
joiner-originated sends the wire flow is:

1. Joiner's IPC client calls `Send` → joiner's daemon calls
   `GossipBridge::send_remote` (`gossip_bridge.rs:356`).
2. `send_remote` publishes a `GossipBody::SendRequest { req_id, peer,
   payload: SendPayload { kind, action, payload } }` and waits for an ack.
3. Host receives the frame in `handle_inbound_frame`'s `(Host, SendRequest)`
   arm (`gossip_bridge.rs:683`), calls `run_host_send`, which drives the
   host's `Registry::send` — that's where `SessionMessage::new` runs and
   `seq` + `timestamp_ms` get assigned.
4. Host broadcasts a `GossipBody::Message(m)` and a `GossipBody::SendAck { result }` carrying the same `m`.

The brainstorm's signature scope (S1) excludes `seq` precisely because the
host stamps it — but it *includes* `peer`, `kind`, `action`, `payload`, and
`timestamp_ms`. So either (a) the joiner picks `timestamp_ms` and signs at
step 1, with the host preserving it through the round-trip, or (b) the host
picks `timestamp_ms` and the host signs the joiner's message — which would
mean the joiner's authorship signature is impossible to forge for a malicious
host but the joiner has no on-wire proof of its own intent until B+something.

This plan goes with **(a)**: the joiner stamps `timestamp_ms`, signs the
candidate body, and ships `signature` alongside `payload` in `SendRequest`.
The host preserves both verbatim, assigns `seq`, builds the
`SessionMessage`, and re-broadcasts. Receivers verify against `peer.id` over
the unchanged scope (which excludes `seq`), so the signature stays valid
through the round-trip. The host **also verifies before appending** —
otherwise the threat-model row "Forged grants — Bob issues `Grant(bob,
ReadWrite)` self-signed" leaks a hole that won't be plugged until Slice C.

This shape has two consequences this plan threads through:

- `Registry::send` no longer always stamps `timestamp_ms`. The host arm
  (called from `run_host_send` for joiner-initiated sends) takes a pre-
  stamped `timestamp_ms` and a pre-built `signature`, and only assigns
  `seq`. The host-originated arm (host's IPC client calls `Send`) takes
  no timestamp from the caller, stamps it itself, signs, and assigns
  `seq` — same flow as today plus signing. The two arms differ in one
  bit ("did someone else already author this body?"), and the cleanest
  expression is a typed parameter — see Sub-slice B1 § Signature
  pre-assembly.
- `SendPayload` is no longer enough; the wire body shipped by joiners
  needs `(timestamp_ms, kind, action, payload, signature)`. New struct
  in `artel-protocol::rpc`: `SignedSendPayload`. `SendPayload` is *kept*
  because it is also the IPC-side `Request::Send` payload from
  `artel-client` to its local daemon — the IPC caller doesn't have a
  signing key, the daemon does. That IPC boundary stays unsigned; the
  daemon stamps `timestamp_ms` and signs the moment the request crosses
  into `Registry::send` (or, for remote sessions, into `send_remote`,
  which signs and encodes a `SignedSendPayload`).

## Sub-slice ordering

Three sub-slices, each independently mergeable, each ends green on `make
test` and `make ci-local`. Each commits on its own.

- **B1 — Signing infrastructure: protocol types + iroh-key plumbing.**
  Adds `signature: [u8; 64]` to `SessionMessage`. Adds `signing.rs` to
  `artel-protocol` with byte-canonical `SigBody`, `sign_body`,
  `verify_body`, behind a new `signing` feature on the protocol crate
  that pulls in `ed25519-dalek` only inside the daemon graph (the
  protocol crate stays no-dep by default for embeds that don't sign).
  Plumbs the daemon's `iroh::SecretKey` through to `Registry` so
  `Registry::send` can sign. Bumps `MESSAGE_FORMAT` 1 → 2 and
  `Meta::CURRENT_VERSION` 1 → 2.
- **B2 — Sign + verify on the wire and at rest.** `Registry::send`
  signs every message it appends. Joiner-side `send_remote` signs the
  body and ships a new `SignedSendPayload` in `SendRequest`. Host's
  `run_host_send` rebuilds the body, verifies the joiner's signature
  before driving `Registry::send`, and `Registry::send` for remote-
  authored messages threads through the joiner's signature (does NOT
  re-sign over the joiner's payload). Receiver-side
  `materialise_remote_session::on_message` verifies before appending
  to its mirror. Log-load (`store/fs::read_log`) verifies each frame
  before returning it to `Registry::host` / `from_record`.
- **B3 — Schema bump + fresh-sessions-only migration story.** No
  on-disk migration. `Meta::CURRENT_VERSION` mismatch on load returns
  a typed error (`SessionStoreError::IncompatibleSchema { found,
  expected }`); the daemon logs it once with the path and skips the
  session directory rather than crashing. A `make` target /
  `clean-sessions` flag is **not** in scope — operators delete
  `~/.artel/sessions/<id>` by hand. Documented in CHANGELOG-style
  commit message and in ADR-001 § Updates.

The split is not arbitrary. B1 is type and plumbing only — it compiles
and tests but doesn't yet sign anything live (existing flow paths use
a sentinel zero signature, which the verifier rejects in
`#[cfg(test)]` only behind a flag). B2 is the load-bearing change.
B3 is paperwork — but it has its own commit because the SCHEMA_VERSION
bump deserves to be visible in `git log` for the alpha-recovery
playbook.

There is **no `PROTOCOL_VERSION` bump** in this slice. The IPC wire
shape changes (`SendRequest` gains a signature inside the new
`SignedSendPayload`), but pre-1.0 we treat client + daemon as
co-rebuilt — same posture as Slice A. The `MESSAGE_FORMAT` bump is the
on-the-wire signal that this version of the bytes carries a signature.

---

## Sub-slice B1 — Signing infrastructure

**Goal:** Add the signature field to `SessionMessage`. Add a
canonical-bytes module to `artel-protocol`. Plumb the iroh secret key
from the daemon's `IrohRuntime` down to `Registry` so the next slice
can call `sign` from `Registry::send` and `send_remote`. End the
sub-slice green with the new field present and zero-valued on the
wire — verification doesn't yet fire.

### Why infrastructure is its own sub-slice

The diff for B2 ("verify on every receive path") touches eight call-
sites and a fixture rebuild. Pre-staging the type + canonical bytes
keeps B2's commit focused on the load-bearing security boundary
without burying the typing change underneath it.

### Files touched

- `crates/artel-protocol/Cargo.toml`:
  - Add an optional `ed25519-dalek` dep behind a new `signing`
    feature. The protocol crate stays no-dep by default (embedders
    who never sign — e.g. the docs site or a future read-only viewer
    — pay zero crypto-build cost). The feature is enabled by
    `artel-daemon` and `artel-client` in their `[dependencies]`
    tables. `ed25519-dalek = { version = "3.0.0-pre.6", default-
    features = false, features = ["std"], optional = true }`.
    Version pinned to match the workspace lockfile (already at
    `3.0.0-pre.6` via iroh's transitive). No new top-level dep
    crawl; cargo will dedupe.
- `crates/artel-protocol/src/lib.rs`:
  - Add `pub mod signing;` under `#[cfg(feature = "signing")]`. (See
    "Why feature-gate it" below for the rationale.)
- `crates/artel-protocol/src/signing.rs` (new file, ~150 lines):
  ```rust
  //! Per-message ed25519 signing for `SessionMessage`.
  //!
  //! See `docs/brainstorms/2026-05-30-auth-story-brainstorm.md` § L3.
  //! Signature scope is intentionally narrower than the full struct
  //! — `seq` is host-assigned and excluded so a joiner can sign
  //! before the host stamps the seq.
  //!
  //! ## Canonical bytes
  //!
  //! The signed bytes are NOT the postcard encoding of the whole
  //! message. They are a domain-separated, fixed-format byte string
  //! built from a stable subset:
  //!
  //! ```text
  //! "artel/sig-v1"  ||  session_id (16 bytes)
  //!                ||  message_format (1 byte)
  //!                ||  timestamp_ms_be (8 bytes)
  //!                ||  peer.id (32 bytes)
  //!                ||  kind_tag (1 byte: chat=0, tool=1, system=2,
  //!                                       capability=3 reserved)
  //!                ||  action_len_be (4 bytes) || action_utf8
  //!                ||  payload_len_be (4 bytes) || payload_bytes
  //! ```
  //!
  //! Domain prefix (`"artel/sig-v1"`) prevents cross-protocol
  //! reuse: a signed `SessionMessage` body cannot be replayed as,
  //! say, a signed iroh routing frame even if both happen to be
  //! ed25519-signed. `session_id` is included so `Grant` events
  //! can't be cross-session-replayed (see brainstorm § Threat
  //! Model). `message_format` rides inside the signed bytes so a
  //! downgrade attack (force `version` back to 1, the unsigned-era
  //! shape) flips the signature. The version-1 era never signs at
  //! all, so we never have to verify against `version=1` bytes;
  //! `version=2` is the floor.
  //!
  //! Capability kind tag is reserved at byte 3 even though the
  //! enum doesn't define it yet — Slice C lands the variant, and
  //! pre-allocating the byte means existing v2 signatures stay
  //! valid post-C.
  use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey, SignatureError};

  use crate::ids::{PeerId, SessionId};
  use crate::message::{MessageFormat, MessageKind, PeerInfo, SessionMessage};

  /// 64-byte ed25519 signature.
  pub type SigBytes = [u8; 64];

  /// All-zero signature used as the in-memory sentinel for
  /// "not yet signed". Postcard-encoded zero is shorter than a
  /// random sig so wire-size telemetry stays meaningful; the
  /// verifier rejects it (sentinel-rejection is a unit test).
  pub const SIGNATURE_UNSIGNED: SigBytes = [0u8; 64];

  /// Domain-prefix string baked into every canonical-bytes
  /// blob. Versioned so a future reshape (different field order,
  /// new field set, different domain) is unambiguous.
  pub const DOMAIN_TAG: &[u8] = b"artel/sig-v1";

  fn kind_tag(kind: MessageKind) -> u8 {
      match kind {
          MessageKind::Chat => 0,
          MessageKind::Tool => 1,
          MessageKind::System => 2,
          // MessageKind::Capability => 3,  // reserved for Slice C
      }
  }

  /// Build the canonical signed bytes for a message body.
  /// Used by both signers and verifiers — they MUST agree
  /// byte-for-byte. Length-prefix every variable field to
  /// foreclose extension/truncation collisions.
  #[must_use]
  pub fn canonical_bytes(
      session_id: SessionId,
      version: MessageFormat,
      timestamp_ms: u64,
      peer: &PeerInfo,
      kind: MessageKind,
      action: &str,
      payload: &[u8],
  ) -> Vec<u8> {
      let action = action.as_bytes();
      let mut out = Vec::with_capacity(
          DOMAIN_TAG.len() + 16 + 1 + 8 + 32 + 1 + 4 + action.len() + 4 + payload.len(),
      );
      out.extend_from_slice(DOMAIN_TAG);
      out.extend_from_slice(session_id.as_bytes()); // SessionId is 16 bytes (uuid)
      out.push(version.get());
      out.extend_from_slice(&timestamp_ms.to_be_bytes());
      out.extend_from_slice(peer.id.as_bytes());
      out.push(kind_tag(kind));
      out.extend_from_slice(&u32::try_from(action.len()).expect("action <= 4 GiB").to_be_bytes());
      out.extend_from_slice(action);
      out.extend_from_slice(&u32::try_from(payload.len()).expect("payload <= 4 GiB").to_be_bytes());
      out.extend_from_slice(payload);
      out
  }

  /// Sign a message body using the daemon's signing key.
  pub fn sign_body(
      key: &SigningKey,
      session_id: SessionId,
      version: MessageFormat,
      timestamp_ms: u64,
      peer: &PeerInfo,
      kind: MessageKind,
      action: &str,
      payload: &[u8],
  ) -> SigBytes {
      let bytes = canonical_bytes(session_id, version, timestamp_ms, peer, kind, action, payload);
      key.sign(&bytes).to_bytes()
  }

  /// Verify a `SessionMessage`'s signature against `peer.id` as
  /// a public key. Caller passes the `session_id` separately
  /// because it isn't on the message.
  ///
  /// Errors:
  /// - [`VerifyError::SentinelUnsigned`] if `signature` is
  ///   all-zero (a freshly-constructed v2 message that never
  ///   went through `sign_body`).
  /// - [`VerifyError::BadKey`] if `peer.id` is not a valid
  ///   ed25519 public key (i.e. doesn't decode to a curve
  ///   point). Drops; do not append.
  /// - [`VerifyError::BadSig`] if the signature does not
  ///   verify under `peer.id`.
  pub fn verify_message(
      session_id: SessionId,
      message: &SessionMessage,
      signature: &SigBytes,
  ) -> Result<(), VerifyError> {
      if signature == &SIGNATURE_UNSIGNED {
          return Err(VerifyError::SentinelUnsigned);
      }
      let verifying = VerifyingKey::from_bytes(message.peer.id.as_bytes())
          .map_err(|_| VerifyError::BadKey)?;
      let sig = Signature::from_bytes(signature);
      let bytes = canonical_bytes(
          session_id,
          message.version,
          message.timestamp_ms,
          &message.peer,
          message.kind,
          &message.action,
          &message.payload,
      );
      verifying.verify(&bytes, &sig).map_err(|_| VerifyError::BadSig)
  }

  #[derive(Debug, thiserror::Error, PartialEq, Eq)]
  pub enum VerifyError {
      #[error("signature is the zero sentinel; message was never signed")]
      SentinelUnsigned,
      #[error("peer id is not a valid ed25519 public key")]
      BadKey,
      #[error("signature does not verify against peer id")]
      BadSig,
  }
  ```
  Notes on the canonical bytes design:
  - Big-endian length-prefixes everywhere. Standard. No
    UTF-8 vs locale ordering concerns.
  - We do NOT sign over postcard's encoding of the whole
    message. Rationale: postcard's varint for `seq` would
    bleed seq into the signed bytes, and we explicitly
    *don't* sign seq. Hand-rolling the canonical layout
    avoids both that and any future postcard-encoding-tweak
    breaking signatures.
  - `SessionId::as_bytes()` exists today (Slice A used it
    indirectly via `as_slice` for the gossip topic salt; if
    only `as_slice` exists, add `as_bytes()` returning
    `&[u8; 16]` here — the underlying type is `Uuid`).
- `crates/artel-protocol/src/ids.rs`:
  - Confirm `SessionId::as_bytes(&self) -> &[u8; 16]` exists.
    If only `as_slice` exists, add a `pub fn as_bytes` accessor;
    no allocation, returns the underlying `Uuid::as_bytes`.
- `crates/artel-protocol/src/message.rs`:
  - Add `signature: SigBytes` field to `SessionMessage`. Field
    order: append at the end so postcard sees the version-1
    fields in the same positions on the wire — but with
    `MESSAGE_FORMAT` bumped to 2 the wire format is *not*
    forwards-compatible anyway, so this is purely a code-
    review nicety.
  - Bump `MESSAGE_FORMAT` from `MessageFormat::new(1)` to
    `MessageFormat::new(2)`. Update the
    `message_format_constant_is_one` test → `..._is_two`.
  - `SessionMessage::new` signature gains a `signature:
    SigBytes` parameter at the tail. Update the doc comment to
    cite the brainstorm and explain that callers who don't yet
    have a key (test fixtures, the registry-via-MemoryStore
    unit tests) can pass `signing::SIGNATURE_UNSIGNED`.
  - Update the existing `SessionMessage` field-order comment
    block to call out that `signature` follows `payload`,
    not part of the signed scope (it IS the signature).
  - Postcard size unit test: rewrite the `..._is_compact`
    upper bound to account for the 64-byte signature
    (today's bound is 80; new bound is `80 + 64 = ~144`,
    leave a little headroom and assert `< 160`).
- `crates/artel-protocol/src/rpc.rs`:
  - **New struct** `SignedSendPayload`: same fields as
    `SendPayload` plus `timestamp_ms: u64` and `signature:
    SigBytes`. This is the wire-side body for joiner→host
    `SendRequest`. Doc comment: "The IPC `Request::Send`
    body is `SendPayload` (no key on the IPC caller); the
    joiner's daemon stamps `timestamp_ms` and signs to
    produce `SignedSendPayload`, which rides on
    `GossipBody::SendRequest`. The host preserves
    `timestamp_ms` and `signature` verbatim into the broadcast
    `SessionMessage`; receivers verify against `peer.id`."
  - Keep `SendPayload` as-is — it's still the IPC-side type.
- `crates/artel-protocol/src/gossip.rs`:
  - `GossipBody::SendRequest` swaps `payload: SendPayload` for
    `payload: SignedSendPayload`. Doc-comment update to point
    at the new struct.
  - Update the `send_request_frame_round_trips` test to
    construct a `SignedSendPayload` with `SIGNATURE_UNSIGNED`
    + `timestamp_ms: 1_700_000_000_000`. Round-trip still
    works without verifying.
- `crates/artel-daemon/Cargo.toml`:
  - Enable the `signing` feature on `artel-protocol`:
    `artel-protocol = { workspace = true, features =
    ["signing"] }`.
- `crates/artel-client/Cargo.toml`:
  - Same — `signing` feature on `artel-protocol`. The
    client doesn't sign yet (the brainstorm leaves consumer-
    side signing to L4 fast-follow), but it imports
    `SessionMessage` and so transitively needs the field.
- `crates/artel-daemon/src/server.rs`:
  - `IrohRuntime` (line 150-ish) gains a `signing_key:
    Arc<iroh::SecretKey>`. Today the secret is moved into the
    Endpoint at line 750. iroh's `Endpoint::secret_key` takes
    `SecretKey` by value but `iroh::SecretKey` is `Clone` (it's
    a 32-byte struct), so we clone before passing to
    `EndpointBuilder`. Store the clone on `IrohRuntime` for
    later read.
  - Add `pub(crate) fn signing_key(&self) -> Arc<iroh::SecretKey>`
    accessor.
- `crates/artel-daemon/src/session.rs`:
  - `Registry` gains an `Option<Arc<iroh::SecretKey>>` (None
    in `cfg(not(feature = "iroh"))` builds; Some when iroh is
    on). Wired in via the existing `Registry::new` /
    `Registry::with_bridge` constructor — passed in from
    `server.rs` where `IrohRuntime` is in scope.
  - For B1 only: `Registry::send` does **not** yet call sign.
    It passes `SIGNATURE_UNSIGNED` to `SessionMessage::new` and
    the existing tests stay green because nobody verifies yet.
    A `// TODO(slice-b2): replace SIGNATURE_UNSIGNED with sign_body`
    comment makes the deferred change visible. This sentinel
    is the lit fuse — B2's sign-on-send change makes every old
    test path green again; if we forget to wire signing in,
    `verify_message` fails with `SentinelUnsigned` on every
    receive path the moment B2 turns verification on, and the
    test suite goes red catastrophically.
- `crates/artel-daemon/src/store/fs.rs`:
  - Bump `Meta::CURRENT_VERSION` from `1` to `2`.
  - Update `Meta::write_meta` / `Meta::read_meta` round-trip
    test; today the meta version on disk is independent of
    `MESSAGE_FORMAT`, but since the on-disk log frames now
    embed signatures, refusing to load v1 meta is the cleanest
    "this old session was written by a pre-signing daemon"
    detection.
  - In `read_log` (line 648), do **not** add verification yet
    — that's B2. The frames decode fine because postcard
    backwards-incompatibility is handled by the meta version
    bump rejecting old session directories before we reach
    `read_log`.

### Why feature-gate it

`artel-protocol` is a leaf crate consumed by both daemon and client.
The brainstorm argues "no new dep" — accurate at the workspace
level: `ed25519-dalek` is already in `Cargo.lock`. But adding it as
a non-optional dep on `artel-protocol` would compile-link it into
every `cargo check` of every consumer (including a future browser-
WASM viewer where ed25519 has no business). Optional + feature-
gated keeps the protocol crate honest about its dependencies.
Daemon + client opt in via their Cargo.toml; nothing else has to
care.

### Tests added (B1)

Unit tests in `crates/artel-protocol/src/signing.rs::tests` (under
`#[cfg(feature = "signing")]`). Each one is one assertion; cheap.

- `domain_tag_is_artel_sig_v1`. Pin the bytes; a fixture
  regression detector.
- `canonical_bytes_includes_session_id_at_a_known_offset`. Build
  two canonical-byte blobs that differ only in `session_id`,
  assert the diff lives at byte 12 (`DOMAIN_TAG.len()`). Pins the
  field order without snapshotting the whole blob.
- `canonical_bytes_excludes_seq`. Build the canonical bytes with
  every input fixed, then build a `SessionMessage` with
  different `seq` values and re-sign — signatures differ ONLY if
  the implementation accidentally wove `seq` in. The test asserts
  identical signature bytes for two messages whose only
  difference is `seq` (using a fixed `SigningKey` from a
  determinstic seed).
- `sign_then_verify_round_trip`. Generate a key, build a body,
  sign, verify against the corresponding `VerifyingKey`. Asserts
  `Ok(())`.
- `verify_rejects_sentinel_unsigned`. `SIGNATURE_UNSIGNED` →
  `VerifyError::SentinelUnsigned`.
- `verify_rejects_wrong_signer`. Sign with key A, verify against
  key B → `VerifyError::BadSig`.
- `verify_rejects_tampered_payload`. Sign body, flip one byte in
  the payload, verify → `VerifyError::BadSig`.
- `verify_rejects_tampered_session_id`. Same body, verify with a
  different `SessionId` → `VerifyError::BadSig`. Pins the
  cross-session-replay defense.
- `verify_rejects_tampered_kind`. Sign as `Chat`, flip to `Tool`,
  verify → `VerifyError::BadSig`. Pins the "an attacker can't
  reuse a Chat-signed body as a Tool-signed body" property —
  load-bearing for Slice C, where `Capability` will join the
  enum.
- `verify_rejects_tampered_timestamp`. Same scope check as above.
- `verify_rejects_invalid_peer_id_bytes`. `peer.id` set to a
  known non-curve-point (e.g. a peer id whose y-coord byte makes
  the point non-canonical); verify → `VerifyError::BadKey`. The
  `from_bytes` call inside `VerifyingKey` does the validation;
  this test pins the surface.
- `verify_rejects_seq_change`. Sign a message, mutate its `seq`
  field on the receive side, verify still succeeds. Pinned
  because the sig scope DOES exclude seq — this is the property
  that lets the host stamp seq after the joiner signs. (Without
  this test, a future "let's sign more!" refactor that included
  seq would silently break the wire flow.)

Unit tests in `crates/artel-protocol/src/message.rs::tests`:
- `message_format_constant_is_two` — replaces the existing
  `..._is_one` assertion.
- Update `session_message_postcard_is_compact`'s upper bound to
  `< 160` (was `< 80`) to account for the 64-byte signature.
- Update existing `session_message_postcard_round_trip` and
  proptest variants to construct messages with
  `SIGNATURE_UNSIGNED` (or any 64-byte value); no behavioural
  change beyond the new field.

Unit tests in `crates/artel-protocol/src/rpc.rs::tests`:
- `signed_send_payload_postcard_round_trip`. Field-by-field
  round trip.

Unit test in `crates/artel-daemon/src/server.rs::tests`:
- `iroh_runtime_signing_key_round_trips`. (under `#[cfg(feature
  = "iroh")]`.) Spin a Testing-mode iroh runtime, assert
  `runtime.signing_key().to_bytes()` equals the bytes loaded
  from `iroh.key`. Pins that the cloned key on `IrohRuntime` is
  the same one the endpoint is using.

No new e2e tests in B1 — verification doesn't fire yet.

### Definition of done (B1)

1. `artel-protocol::signing` module exists; all unit tests
   green.
2. `SessionMessage::signature` field exists and round-trips
   through postcard + JSON.
3. `MESSAGE_FORMAT == 2`; `Meta::CURRENT_VERSION == 2`.
4. `SignedSendPayload` exists; `GossipBody::SendRequest` carries
   it; round-trips.
5. `Registry` carries the daemon's signing key as
   `Option<Arc<SecretKey>>`; `IrohRuntime::signing_key`
   accessor exists; the wiring compiles.
6. `Registry::send` still passes `SIGNATURE_UNSIGNED`. The TODO
   comment naming Slice B2 is in place.
7. `make ci-local` green both feature modes.

**Commit subject:** `protocol: add canonical-bytes signing + signature field on SessionMessage; daemon wires SecretKey through Registry (auth Slice B1, MESSAGE_FORMAT 2)`

---

## Sub-slice B2 — Sign + verify on every send / receive / replay path

**Goal:** Turn signing on. Every freshly-authored `SessionMessage`
is signed before it's appended or broadcast; every received
`SessionMessage` (gossip Message, gossip SendAck::Ok, log replay) is
verified before its body is acted on.

### Files touched

- `crates/artel-daemon/src/session.rs`:
  - `Registry::send` is split into two arms by adding a typed
    parameter that says where the authorship is coming from:
    ```rust
    /// How `Registry::send` should treat the body it's given:
    /// did *this* daemon author it (and so should sign now), or
    /// did a remote joiner author it (so we already have their
    /// signature and timestamp, and we just need to assign seq +
    /// re-verify before append)?
    enum Authoring {
        Local,
        Remote { timestamp_ms: u64, signature: SigBytes },
    }
    ```
    Most callers (host's own IPC `Send`) pass `Local`.
    `run_host_send` (the joiner-`SendRequest`-arrival path) passes
    `Remote { ... }`. The function:
    - For `Local`: stamp `timestamp_ms = now_ms()`, sign with the
      registry's signing key (panic-loud if the key is `None` and
      iroh is on — that's a wiring bug, not a runtime case).
    - For `Remote { timestamp_ms, signature }`: trust the caller's
      timestamp, but **verify the signature before assigning seq +
      appending**. If verification fails, return a new
      `SessionError::SignatureRejected { peer_id, reason }` —
      `run_host_send` then translates that to `Err` in the
      `SendAck` so the joiner sees a clean rejection rather than
      a timeout.
    - Either way: `seq` is assigned, the message is built with
      `SessionMessage::new(seq, timestamp_ms, peer, kind, action,
      payload, signature)`, then appended.
    The signing-key-`None` arm is unreachable in production with
    iroh on. Under `cfg(not(feature = "iroh"))` we skip signing
    AND verification — there's no wire surface to defend, just
    in-process IPC, and L4 is the right layer to plug that hole.
    A `#[cfg(not(feature = "iroh"))]` arm uses `SIGNATURE_UNSIGNED`
    explicitly; the `Local` `Some(key)` arm is the only path that
    actually signs.
  - `materialise_remote_session::on_message` (line 530–575) —
    add `verify_message(session_id, &msg, &msg.signature)`
    before persistence + mutation. On failure, log at warn (no
    abort, no panic — drop the frame, mirror stays consistent),
    and bump a `tracing::warn` counter. Comment cites the
    threat-model row "Tampered replay history".

    Concretely:
    ```rust
    if let Err(err) = verify_message(session_for_log, &msg, &msg.signature) {
        warn!(
            session = ?session_for_log,
            seq = ?msg.seq,
            peer = %msg.peer.id,
            ?err,
            "dropping inbound Message: signature verify failed",
        );
        return;
    }
    ```
- `crates/artel-daemon/src/gossip_bridge.rs`:
  - `send_remote` (line 356) — before publishing, sign:
    ```rust
    let timestamp_ms = wallclock_now_ms();
    let signature = sign_body(
        &self.signing_key,
        session,
        MESSAGE_FORMAT,
        timestamp_ms,
        &peer, // already overridden to authenticated id above
        payload.kind,
        &payload.action,
        &payload.payload,
    );
    let signed = SignedSendPayload {
        timestamp_ms,
        kind: payload.kind,
        action: payload.action,
        payload: payload.payload,
        signature,
    };
    let body = GossipBody::SendRequest { req_id, peer, payload: signed };
    ```
    `GossipBridge` gains `signing_key: Arc<iroh::SecretKey>`;
    plumbed in via `GossipBridge::new` at construction (called
    from `server.rs` where `IrohRuntime::signing_key()` is in
    scope). The clone is cheap (32 bytes) and stored as `Arc`
    so all the `tokio::spawn`'d forwarders can see it without a
    refcount-per-forward.
  - `run_host_send` (line ~796) — receives the
    `SignedSendPayload` (gossip carries it now). Pre-verify
    once at the bridge boundary, then call `Registry::send`
    with `Authoring::Remote { timestamp_ms, signature }`. The
    pre-verify is "belt and suspenders": `Registry::send`
    will verify too, but doing it at the bridge means the
    bridge logs include the iroh `delivered_from` for the
    specific frame, which is useful triage when a bug
    produces sig failures. (Spoofing-style attacks are caught
    by the L1 `delivered_from` check, which still runs first
    via `drop_if_spoofed` at line 689 — so by the time we
    reach `run_host_send` we know `peer.id == delivered_from`,
    and the only remaining gap is "did `peer.id` actually
    sign?".)

    On verify failure at the bridge: synthesise a
    `GossipBody::SendAck { req_id, result: Err(ProtocolError::Signature(...)) }`
    so the joiner's `pending_sends` resolves cleanly instead
    of timing out. New variant on `ProtocolError`:
    `Signature(String)`.
- `crates/artel-protocol/src/error.rs`:
  - Add `ProtocolError::Signature(String)` variant. Postcard
    + JSON round-trip; `Display` says "signature rejected: …".
- `crates/artel-daemon/src/store/fs.rs`:
  - `read_log` (line 648) — gains a `session_id: SessionId`
    parameter so it can call `verify_message`. Each frame:
    decode → verify → push or drop. Dropped frames go to a
    `warn!` and are skipped, not appended to the in-memory
    `Vec`. Truncating-at-first-bad-frame would be wrong: a
    single tampered frame mid-log shouldn't sever everything
    after it; replay drops the bad frame and keeps the
    surrounding context.

    Caveat: dropping a bad frame leaves `seq` non-contiguous in
    the in-memory log. `Registry::head` is set from the
    last-good frame's seq (which is what we'd compute anyway).
    Document the gap in the in-line comment: "a tampered seq
    leaves a hole; that is correct — the receiver has no truth
    for the missing seq, and the host's authoritative log is
    where the truth lives. A future `Replay { since: head }`
    will refetch."
  - `load_all` (line 193) — wire the session id through to
    `read_log`; the meta file already carries it (it's the
    directory name → `SessionId` via `Uuid::parse_str`).
- `crates/artel-protocol/src/rpc.rs`:
  - `Request::Send` IPC body still uses `SendPayload` (no
    signature, no timestamp — the daemon stamps both). No change
    here; the IPC boundary stays unsigned in v1. L4 is where this
    becomes a problem worth solving.

### Tests added (B2)

Unit tests in `crates/artel-daemon/src/session.rs::tests`:
- `registry_send_local_signs_with_daemon_key`. Build a
  registry with a known SigningKey, call `send`, retrieve the
  resulting `SessionMessage`, verify against the corresponding
  `VerifyingKey`. (Pin the load-bearing signing path.)
- `registry_send_remote_authoring_verifies_signature_first`.
  Build a remote `Authoring` with a wrong signature, call
  `send`, assert `SessionError::SignatureRejected`.
- `registry_send_remote_authoring_preserves_timestamp_and_signature`.
  Sign a body off-registry, call `send` with `Authoring::Remote
  { timestamp_ms, signature }`, assert the appended message has
  exactly that timestamp and signature.

Unit tests in `crates/artel-daemon/src/store/fs.rs::tests`:
- `read_log_drops_tampered_frame_and_keeps_surrounding`. Write
  three frames; corrupt the middle frame's payload byte-by-byte
  (post-postcard, so structurally valid); read back; assert
  exactly 2 messages return, the dropped one is logged, and the
  surrounding seqs are unaffected.
- `read_log_drops_unsigned_sentinel`. Write a frame with
  `SIGNATURE_UNSIGNED`; read back; assert 0 messages, 1 warn.
- `read_log_with_v1_meta_returns_incompatible`. Pre-Slice-B
  meta on disk → `IncompatibleSchema { found: 1, expected: 2 }`.

E2E tests in `crates/artel-daemon/tests/auth_l3_signing.rs`
(new file, mirrors `auth_l1_spoofing.rs`):
- `joiner_send_arrives_signed_at_host`. Spin a `Pair`. Bob's
  IPC client `Send`s. Alice's daemon receives it, verifies,
  appends to its log. Assert the appended message verifies
  against Bob's `EndpointId` as a `VerifyingKey`.
- `host_drops_send_with_tampered_signature`. Use Bob's gossip
  handle to publish a hand-rolled `SendRequest` whose signature
  doesn't match the body. Assert no `Message` shows up on
  Alice's stream within the 2s ceiling, and the `SendAck` Bob
  sees carries a `ProtocolError::Signature(...)`.
- `joiner_drops_message_with_tampered_signature`. Alice
  legitimately broadcasts. Then a third party (well, Alice's
  own gossip handle in tests) re-broadcasts a `Message` with
  the body byte-flipped post-signing. Bob's mirror receives
  both; the second one is dropped on signature failure. Assert
  Bob's `Subscribe` event stream sees exactly one `Message`
  with the legitimate payload.
- `log_replay_drops_tampered_frames`. Write a session log to
  disk, corrupt one frame's payload, restart the daemon, assert
  the resumed log has the surrounding messages and not the
  tampered one. (Restarts go through `load_all` → `read_log`,
  where the verification fires.)
- `cross_session_grant_replay_is_rejected`. (Defensive — Slice
  C is where Grants live, but the property is testable now: a
  legitimate Send signed for session A, replayed on session
  B's topic, fails to verify because `session_id` is in the
  signed scope.) Two sessions, one signed body, replay → drop.

### Definition of done (B2)

1. `Registry::send` signs every locally-authored body with the
   daemon's iroh secret key.
2. Joiner-side `send_remote` signs the body before publishing
   the `SendRequest`.
3. Host-side `run_host_send` verifies the joiner's signature
   before invoking `Registry::send`; failures land as
   `ProtocolError::Signature` in the `SendAck`.
4. `Registry::send` re-verifies remote-authored signatures
   before appending (defense in depth at the registry boundary,
   not just the bridge).
5. Joiner-side `materialise_remote_session::on_message`
   verifies before persisting + emitting.
6. `read_log` verifies each frame, drops tampered frames with a
   warn, and preserves surrounding context.
7. `make ci-local` green; new e2e suite green.
8. `make test-n0` (Tier C) green — the sign + verify
   round-trip MUST hold over a real iroh transport, not just
   `MemoryLookup` fixtures.

**Commit subject:** `daemon: sign every session message; verify on send / receive / replay (auth Slice B2)`

---

## Sub-slice B3 — Schema-bump migration paperwork

**Goal:** Document the SCHEMA_VERSION bump. No code change beyond a
small one-shot guard that turns `Meta::CURRENT_VERSION` mismatch into
a clean operator-facing error message ("session was written by a
pre-2026-06-02 daemon; delete `~/.artel/sessions/<id>` to recover").

### Files touched

- `crates/artel-daemon/src/store/fs.rs`:
  - `Meta::read_meta` — on `version != Self::CURRENT_VERSION`,
    return a typed `SessionStoreError::IncompatibleSchema {
    found, expected, path }` instead of a generic deserialize
    error. The error's `Display` impl includes the path so
    operators see exactly which directory to remove.
  - `load_all` — on `IncompatibleSchema`, log at `error!` with
    the path and skip the directory rather than crash the
    daemon. Existing well-formed sessions still load.
  - Unit test: `incompatible_schema_skips_session_dir`. Plant a
    v1 meta + log on disk, run `load_all`, assert other
    sessions still load and the v1 dir does not appear.
- `docs/adr/001-collab-substrate-platform.md`:
  - Append "Updates" entry for 2026-06-02:
    > 2026-06-02: Auth Slice B (L3 per-message signing).
    > `MESSAGE_FORMAT` 1 → 2 and `Meta::CURRENT_VERSION` 1 → 2.
    > Pre-2026-06-02 session directories are skipped on load
    > (logged as IncompatibleSchema). No on-disk migration —
    > operators delete `~/.artel/sessions/<id>` to recover. See
    > `docs/brainstorms/2026-05-30-auth-story-brainstorm.md`
    > and `docs/plans/2026-06-02-auth-slice-b-l3-signing-plan.md`.
- `docs/roadmap.md`:
  - Mark "L3 per-message signing" as DONE (or strike from the
    "Future" section if it's listed there).
- `docs/brainstorms/2026-05-30-auth-story-brainstorm.md`:
  - Append a "Status (2026-06-02)" header line: "Slice B (L3
    signing) shipped 2026-06-02. Slice C (L2 cap events) is
    next."

### Tests added (B3)

- `incompatible_schema_skips_session_dir` (above).

No new e2e — B2's tests already exercise the load path.

### Definition of done (B3)

1. `IncompatibleSchema` typed error exists; `load_all` skips
   incompatible session dirs with a single error log.
2. ADR-001 trailer carries the 2026-06-02 update.
3. Roadmap marks L3 signing DONE.
4. Brainstorm has a status footer.

**Commit subject:** `daemon: skip incompatible-schema session dirs on load; doc auth Slice B (SCHEMA_VERSION 2)`

---

## Cross-cutting concerns

### Things this plan explicitly does not do

- **No L2 capability events.** That is Slice C; depends on B
  landing. The `kind_tag` byte for `MessageKind::Capability` is
  pre-allocated (byte 3) so v2 signatures stay valid post-C.
- **No L4 (per-consumer IPC trust).** The IPC `Request::Send`
  body stays unsigned (`SendPayload`, not `SignedSendPayload`).
  Consumers are still in the daemon's TCB by construction.
- **No L5 (cross-device user identity).** Per-message sigs are
  per-device because the iroh secret is per-device. L5 lifts
  identity above device.
- **No on-disk migration.** Pre-cutover sessions are skipped on
  load with an operator-facing log line. Pre-1.0 alpha posture;
  the brainstorm explicitly endorses this.
- **No `PROTOCOL_VERSION` bump.** Wire shape changes
  (`SendRequest` payload type + `SessionMessage` field) but pre-
  1.0 we co-rebuild client + daemon, so PROTOCOL_VERSION stays
  at 5. `MESSAGE_FORMAT` does the version-signal job.
- **No `Replay { since }` re-fetch on tampered-frame detection.**
  When `read_log` drops a frame, we leave a seq hole. A future
  `Replay` from the host can fill it; the joiner's mirror
  already dedups by seq. Wiring an automatic refetch into the
  drop path is a tightening, not a v1 must.
- **No host-side rebroadcast of the signature in `SendAck`.**
  The `SendAck` already carries the full `SessionMessage`,
  which now embeds the signature. Joiners verify it the same
  way they verify a `Message` frame — one verify path, one
  pile of bytes.
- **No signing-key rotation.** The iroh secret is read once at
  daemon start and held for the daemon's lifetime. If the key
  file is rotated under a running daemon, behaviour is
  undefined (the in-memory key keeps signing with the old
  bytes). Adding rotation is a separate design pass, named in
  Slice A's Risks § 4.
- **No signing of non-log gossip frames.** `SendAck`,
  `SessionClosed`, `Replay`, `JoinAnnouncement` are not signed.
  The brainstorm § L3 signing scope rules them out: iroh-
  gossip's `delivered_from` already authenticates them at the
  network layer. Only log-resident bodies need an
  independently-verifiable identity.

### Risks

1. **`MESSAGE_FORMAT` 2 doesn't gate against a misbehaving v2
   peer that ships `SIGNATURE_UNSIGNED`.** A buggy/malicious v2
   daemon could send a body with the zero sentinel. Verifier
   correctly rejects it (`VerifyError::SentinelUnsigned`), but
   the receiver can't tell "buggy peer" from "malicious peer"
   without context. The drop+log message names the source
   `peer.id` (= `delivered_from` because L1) so the operator
   can identify the misbehaving daemon. Acceptable for v1.

2. **Signature size doubles small messages.** A 16-byte chat
   payload was ~50 bytes on the wire post-postcard; +64 for
   the signature is a 128% overhead. Telemetry-only impact
   pre-1.0; not a hot path. If wire size becomes a real
   concern, post-quantum-friendly aggregation (Slice D-style)
   is where it goes — out of scope here.

3. **`canonical_bytes` allocates per call.** Every send + every
   verify allocates a fresh `Vec`. For a steady-state mesh
   with 10 peers each sending 100 msg/s, that's 2000
   allocations/s for verification on each daemon (received
   from 9 peers, signed locally, re-verified at registry
   boundary). The byte string is bounded by message size + a
   small overhead, so allocator pressure is low. If this
   shows up on a flame graph, switch to a stack-buffer +
   length-counted helper. Don't pre-optimise.

4. **`Clone` on `iroh::SecretKey`.** iroh's `SecretKey` is a
   thin wrapper around `[u8; 32]` and is `Clone`, but the
   semantic of cloning a *secret* key is worth flagging.
   `Arc<SecretKey>` is what we store on `IrohRuntime` /
   `Registry` / `GossipBridge`; the bytes themselves never
   leave RAM (no serialisation, no syscall beyond the
   ed25519-dalek `sign` call). Same threat model as Slice
   A's `Risks § 1` ("anyone with read access to the key file
   can impersonate the daemon").

5. **`Authoring::Remote { signature }` is opaque to the
   `Local` callers.** A future refactor that makes a host's
   IPC-side `Send` go through the same wire path as a
   joiner's would need to flip from `Local` to `Remote`. The
   typed enum makes that explicit (no risk of "oh I forgot
   we re-sign on this path"). If `Authoring` grows a
   third variant — say, `Replayed` for log-rehydration — the
   enum makes that addition checked.

6. **`read_log` drops a frame without `Replay`-ing.** A
   tampered persisted log leaves a seq gap. Subscribe `since`
   replay will deliver the surrounding messages but skip the
   gap, which is correct ("we have no truth for that seq").
   If a downstream consumer (e.g. `artel-fs` mirror) treats
   seq gaps as fatal, this surfaces as a runtime error. None
   of today's consumers do; document.

---

## Critical files for implementation

- `crates/artel-protocol/src/signing.rs` (B1 — new file, ~150 lines)
- `crates/artel-protocol/src/message.rs` (B1 — `SessionMessage::signature`, `MESSAGE_FORMAT 2`)
- `crates/artel-protocol/src/rpc.rs` (B1 — `SignedSendPayload`)
- `crates/artel-protocol/src/gossip.rs` (B1 — `SendRequest` payload type)
- `crates/artel-protocol/src/error.rs` (B2 — `ProtocolError::Signature`)
- `crates/artel-protocol/Cargo.toml` (B1 — `signing` feature + `ed25519-dalek` optional)
- `crates/artel-daemon/Cargo.toml`, `crates/artel-client/Cargo.toml` (B1 — feature opt-in)
- `crates/artel-daemon/src/server.rs` (B1 — `IrohRuntime::signing_key`)
- `crates/artel-daemon/src/session.rs` (B1 + B2 — `Registry` key field, `Authoring`, `Registry::send` arms)
- `crates/artel-daemon/src/gossip_bridge.rs` (B1 + B2 — `GossipBridge::signing_key`, `send_remote` signs, `run_host_send` verifies)
- `crates/artel-daemon/src/store/fs.rs` (B1 + B2 + B3 — `Meta::CURRENT_VERSION 2`, `read_log` verifies, `IncompatibleSchema`)
- `crates/artel-daemon/tests/auth_l3_signing.rs` (B2 — new e2e file)
- `docs/adr/001-collab-substrate-platform.md` (B3)
- `docs/roadmap.md` (B3)
- `docs/brainstorms/2026-05-30-auth-story-brainstorm.md` (B3 — status footer)
