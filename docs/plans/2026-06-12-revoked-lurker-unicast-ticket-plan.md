---
date: 2026-06-12
topic: revoked-lurker-unicast-ticket
status: PLAN — ready to implement
brainstorm: docs/brainstorms/2026-06-12-revoked-lurker-unicast-ticket-brainstorm.md
---

# Revoked-ticket lurker fix — unicast the workspace ticket + gate Replay

Source brainstorm (DECIDED, user-confirmed 2026-06-12):
`docs/brainstorms/2026-06-12-revoked-lurker-unicast-ticket-brainstorm.md`.
The five contestable calls are final and must not be reopened:

1. Joiner = persist-in-mirror + replay-on-Subscribe.
2. Host = daemon-owned distribution (publish-once IPC; deliver on
   publish + at admission).
3. Wire = externally-tagged `DeliveryFrame` enum, ALPN `/1`→`/2`, cap
   1 KiB→64 KiB, old shape deleted.
4. `run_host_replay` membership-gated **+** admission-triggered replay.
5. DocsGate/PeerFilter allow-list flip = follow-up, OUT of scope.

Alpha stance (memory `alpha-no-backwards-compat`): no interop, no
shims; mixed-version daemons fail the handshake / ALPN negotiation and
that is intended.

## The decided line

Today the host broadcasts a read-capability `WorkspaceTicketEnvelope`
over the session log (`publish_ticket`), and `run_host_replay`
re-serves that backlog to **any** gossip subscriber with no membership
check. A revoked/expired-ticket bearer subscribes to the topic,
receives the replayed envelope, imports the `DocTicket`, and syncs the
files. This plan removes the broadcast entirely: the host workspace
publishes the envelope **once** to its daemon over IPC; the host
daemon persists it and delivers it host→peer over the direct-QUIC
upgrade channel (the sanctioned gossip-only exception) — to all
current members on publish and to each peer at admission. The joiner
daemon persists the opaque envelope bytes in its mirror record and
surfaces them to the workspace as a synthetic `TICKET_ACTION` System
message, both live on receipt and replayed on every `Subscribe`. In
the same effort, `run_host_replay` becomes membership-gated and the
host backfills a peer the moment it admits them.

After this, nothing capability-bearing rides the gossip topic, and a
lurker's `Replay` is refused.

## Joiner-facing surface: reuse the synthetic System message (resolved)

The brainstorm left "new `Event` variant vs synthetic System-message
reuse" open. **Resolved: reuse a synthetic `System`/`TICKET_ACTION`
message** (the exact mechanism `emit_upgrade` already uses for
`UPGRADE_ACTION`, `session.rs:2165`). Consequences:

- No `Event` enum change. `wait_for_ticket` (`workspace.rs:1614`)
  keeps its match (`MessageKind::System && action == TICKET_ACTION`)
  verbatim — only the *source* of that message changes (unicast +
  persistence, not gossip broadcast).
- Unlike `UPGRADE_ACTION` (live-only, replay-excluded), the workspace
  ticket is **persisted and replayed**: the daemon injects it into the
  `Subscribe` replay set from the persisted copy, so late attach and
  joiner restart both work.
- `host_daemon_peer_id` (which `wait_for_ticket` reads from
  `message.peer.id`, `workspace.rs:1632`) is stamped as the session
  host on the synthetic message — same as the upgrade path stamps
  `PeerInfo::new(s.host, "host")`.

So the version bump is driven by the **new host IPC request** + the
**new direct-stream frame variant**, not an `Event` change.
`PROTOCOL_VERSION` 8→9.

## Slice 1 — protocol crate: delivery frame, IPC verb, ALPN, version

`crates/artel-protocol`:

- `upgrade.rs` — replace the bare `UpgradeFrame` struct with an
  externally-tagged enum (memory: never serde tag/content on wire
  types):
  ```rust
  pub enum DeliveryFrame {
      Secret(UpgradeFrame),                 // existing NamespaceSecret payload, unchanged
      WorkspaceTicket {
          session_id: SessionId,
          envelope_bytes: Vec<u8>,          // postcard WorkspaceTicketEnvelope, opaque here
      },
  }
  ```
  Keep `UpgradeFrame` as the inner `Secret` payload (its existing
  `session_id` + `namespace_secret` fields and round-trip tests stay).
  `UPGRADE_ALPN` → `b"artel/upgrade/2"`. `UPGRADE_ACK` unchanged.
  `envelope_bytes` uses `serde_bytes`. The 1 KiB read cap lives in the
  daemon handler (Slice 4), not here, but document the 64 KiB ceiling
  on the variant.
- `rpc.rs` — **append** variants (postcard order load-bearing):
  - `Request::PublishWorkspaceTicket { session: SessionId,
    envelope_bytes: Vec<u8> }` (`serde_bytes`). Replaces the host
    workspace's broadcast `Send` of `TICKET_ACTION`.
  - `Response::WorkspaceTicketPublished` — ack.
  - The IPC `Request::DeliverUpgrade` stays as-is (RW secret path); it
    is unrelated to the new publish verb.
- `version.rs` — `PROTOCOL_VERSION` 8→9 (+ pinned test).
- Confirm `Response::Error` stays at postcard index 12
  (`handshake_postcard_indices_are_pinned`) — appends go below.

Tests (match existing `upgrade.rs` / `rpc.rs` style):
- `DeliveryFrame` postcard round-trip for both variants; externally-
  tagged shape pinned (variant tag byte); unknown-variant byte
  rejected; `Secret` variant still decodes the old `UpgradeFrame`
  fields.
- `UPGRADE_ALPN` string pins to `artel/upgrade/2`.
- `PublishWorkspaceTicket` / `WorkspaceTicketPublished` postcard +
  JSON round-trips.
- Version pin updated to 9.

**Commit 1**, green on `make test`.

## Slice 2 — store + record: persist the envelope (host + mirror)

`crates/artel-daemon/src/store`:

- `record.rs` — `SessionRecord.workspace_ticket: Option<Vec<u8>>` with
  `#[serde(default)]`. One slot, kind-independent meaning:
  - `Local` (host): the envelope this host published, so it survives
    host-daemon restart and re-delivers without the workspace
    re-publishing.
  - `Remote` (mirror): the envelope the joiner received over unicast,
    replayed to the workspace on `Subscribe`.
- Trait (`mod.rs`) — one new method, mirroring `put_tickets`:
  `async fn put_workspace_ticket(&self, session: SessionId, envelope:
  &[u8]) -> io::Result<()>` (full rewrite of the one slot; errors
  `NotFound` on unknown session — the contract `put_tickets` /
  `bump_host_epoch` use).
- `fs.rs` — `WORKSPACE_TICKET_FILE = "workspace-ticket.bin"` sidecar in
  the session dir, raw bytes via the tmp+rename helper, `0600` (it is
  capability-bearing — same sensitivity as `tickets.json`). `load_one`
  reads it; **absent ⇒ `None`**. Add its tmp pattern to
  `sweep_tmp_files` if that sweep is pattern-based (verify, as Slice 2
  of the revocation plan did).
- `memory.rs` — store the `Option<Vec<u8>>` on the in-memory record.

Tests: put → load round-trip (both kinds); absent file ⇒ `None`;
rewrite replaces; `delete()` cascade removes the sidecar with the dir;
corrupt/oversized handling follows the `tickets.json` posture (a
present-but-unreadable sidecar fails the load loudly rather than
silently dropping a capability the joiner depends on — match
`Meta`/ledger posture).

**Commit 2**, green.

## Slice 3 — daemon: delivery plumbing, emit, persist, deliver-on-publish

`crates/artel-daemon`:

### 3a. Direct-stream send/receive for the new frame

- `server.rs::dispatch_deliver_upgrade` — generalise the dial/ACK
  plumbing into a shared `deliver_frame(endpoint, target, DeliveryFrame)`
  helper (connect on `/2` ALPN, length-prefixed postcard, read ACK).
  `DeliverUpgrade` builds `DeliveryFrame::Secret(..)` through it.
- `upgrade_protocol.rs::accept` — decode a `DeliveryFrame`; raise the
  read cap 1 KiB→64 KiB. Dispatch:
  - `Secret` → existing `registry.emit_upgrade(..)`.
  - `WorkspaceTicket { session_id, envelope_bytes }` →
    new `registry.emit_workspace_ticket(session_id, remote_peer,
    envelope_bytes)`.
- `session.rs::emit_workspace_ticket` — mirror `emit_upgrade`'s
  validation (session exists, is `Remote`, `sender_peer == s.host`),
  then:
  1. persist via `put_workspace_ticket` (store-before-memory);
  2. store the bytes on the in-memory `Session`;
  3. emit a **live** synthetic `Event::Message` with
     `MessageKind::System`, `action = workspace.ticket`, payload =
     `envelope_bytes`, `peer = PeerInfo::new(s.host, "host")`,
     `Seq::ZERO`, unsigned — same construction as `emit_upgrade`, but
     persisted (step 1) so it also replays.
  Idempotent on a re-delivery of identical bytes (compare-and-skip the
  persist + re-emit only if changed, to avoid log-spam on
  admission-redelivery).

### 3b. Replay injection of the persisted envelope

- `session.rs::subscribe` — when building the `replay` vec, if the
  session has a persisted `workspace_ticket`, **prepend** a synthetic
  `TICKET_ACTION` System message reconstructed from it (host-stamped,
  `Seq::ZERO`). This is what makes late attach / joiner restart work:
  `Workspace::join_with` issues `Subscribe { since: None }` and drains
  for `TICKET_ACTION` — it now finds the replayed copy even though the
  live unicast happened before the workspace attached.
  - Ordering: the ticket message must arrive before the workspace
    gives up; prepending to the replay set guarantees it's first.
  - `UPGRADE_ACTION`'s replay-exclusion (`session.rs:1562`) is
    unaffected — that filter is in `log_since` (gossip replay), not
    the IPC `subscribe` replay. The workspace ticket is injected into
    IPC `subscribe` only; it must NOT enter `log_since` (it's not a
    log entry and must never re-broadcast on the topic — that would
    reintroduce the leak). Add a guard/test pinning that
    `log_since` never emits a `TICKET_ACTION` frame.

### 3c. Deliver-on-publish + deliver-at-admission

- `server.rs` — new `Request::PublishWorkspaceTicket` arm: membership
  gate (`NotSubscribed` like `IssueTicket`), then
  `registry.publish_workspace_ticket(session, envelope_bytes)`.
- `session.rs::publish_workspace_ticket(session, envelope)`:
  `Local`-only (`NotHost` on mirror / `UnknownSession`). Persist the
  envelope on the host record (`put_workspace_ticket` + memory). Then
  deliver to every current member except self: for each, build
  `DeliveryFrame::WorkspaceTicket` and `deliver_frame` over QUIC.
  Best-effort per peer — warn on failure; admission-redelivery
  (below) and joiner re-announce cover the offline case. (Lean from
  brainstorm open-q: warn + redelivery-on-reannounce, no bounded
  retry.)
- `session.rs::ensure_member` — on **successful new admission** of a
  peer to a `Local` session, if the host has a persisted
  `workspace_ticket`, deliver it to that peer over QUIC. Place this
  alongside the existing auto-grant block (after `drop(s)`, same
  re-entry-safe region). This is the trigger that handles
  leave-then-rejoin and joiner-restart (both re-run the
  JoinAnnouncement → `ensure_member` path) and the "workspace wasn't
  up at admission, but the envelope is persisted" case.

### 3d. Replay gating + admission-triggered replay (Decision 4)

`gossip_bridge.rs`:

- `run_host_replay` — serve only if `delivered_from` is a current
  member of `session`; else drop with a warn. Needs a registry lookup
  (`registry.is_member(session, peer)` — add a thin accessor if not
  present). `delivered_from` is the trustworthy id (same L1 assumption
  as `drop_if_spoofed`).
- `handle_inbound_frame` JoinAnnouncement arm — after
  `registry.ensure_member(..)` returns `Ok`, immediately call
  `run_host_replay(bridge, session, Seq::ZERO)` for that peer's
  benefit. Re-announce of an existing member is idempotent on
  membership but still re-serves the backlog (the joiner dedups by
  seq — `apply_inbound_mirror_message`), which is harmless and covers
  a member that resubscribed.
  - Note the existing `Replay` arm (joiner's explicit request) stays,
    now membership-gated — covers an already-member resubscribe.

Tests (daemon unit + Tier B):
- `emit_workspace_ticket`: Remote-only; sender-must-be-host;
  persists + emits live; re-delivery of same bytes is idempotent.
- `subscribe` replays the persisted envelope as a `TICKET_ACTION`
  System message; absent ⇒ no such message.
- `log_since` never emits `TICKET_ACTION` (no topic re-broadcast).
- `publish_workspace_ticket`: Local-only; persists; delivers to all
  members (assert via a fake/echo endpoint or the Tier B two-daemon
  harness).
- `ensure_member` delivers the persisted envelope to a newly admitted
  peer; no delivery if none persisted.
- Replay gate: non-member `Replay` → no Message frames served;
  member `Replay` → served. Admission-triggered replay: a fresh
  joiner receives the backlog without its own pre-admission `Replay`
  being honoured (sequence: JoinAnnouncement admit → host replays).

**Commit 3**, green.

## Slice 4 — artel-fs: publish-via-IPC, delete broadcast, join path

`crates/artel-fs/src/workspace.rs`:

- `publish_ticket` — replace the broadcast `Send` of `TICKET_ACTION`
  with `Request::PublishWorkspaceTicket { session, envelope_bytes }`
  (encode the `WorkspaceTicketEnvelope` exactly as today via
  `ticket::encode`). The host-side cap-listener gains nothing new —
  the daemon owns delivery. Keep the byte-stable envelope construction
  (the restart contract depends on identical bytes).
- `host_with_inner` — still `share(Read)` to build the envelope; call
  the new publish path. The `share(Write)` / NamespaceSecret path is
  untouched.
- `join_with_inner` / `wait_for_ticket` — **no structural change**:
  still `Subscribe { since: None }`, still drain for the
  `TICKET_ACTION` System message, still honour `join_ticket_timeout`.
  The message now originates from the daemon's unicast-delivery +
  replay path. Confirm `host_daemon_peer_id` extraction
  (`message.peer.id`) still yields the host id (it does — the daemon
  stamps `s.host`).
- `crates/artel-fs/src/lib.rs` — `TICKET_ACTION` stays exported (still
  the action string on the synthetic message); `publish_ticket`'s
  internal change is invisible to consumers.

### Legacy broadcast hard-reject

- A `TICKET_ACTION` System message arriving via the **gossip log**
  (stale host, malicious peer broadcasting a forged envelope) must NOT
  drive a v2 joiner. Since the joiner now only ever sees
  `TICKET_ACTION` from the daemon's unicast+replay injection (the host
  no longer logs it), a broadcast `TICKET_ACTION` would only appear if
  someone synthesises one. Pin with a test: a workspace whose only
  `TICKET_ACTION` source is a peer `Send` (no unicast delivery) does
  **not** materialise — i.e. the daemon does not inject from log
  entries, only from the persisted unicast copy. (Rewrite of
  `workspace_filter.rs::joiner_rejects_old_shape_doc_ticket_payload`,
  which currently asserts broadcast-decode behaviour.)

**Commit 4**, green (`make test` — many fs integration tests join via
the old broadcast and now exercise the unicast path; they should pass
unchanged in behaviour, fail only if delivery regressed).

## Slice 5 — regression tests (written FIRST, see note), positive path, n0

Per the task: the lurker regression tests are written **first, against
current code, and must fail today**. Practically, author them at the
top of implementation (before Slice 1) on a scratch commit or just run
them red to confirm, then let Slices 1–4 turn them green. Recorded as
a distinct slice here for the commit ledger; the *writing* leads.

`crates/artel-fs/tests/` (new file, e.g. `revoked_lurker.rs`, Tier B):

- **`revoked_ticket_lurker_gets_no_replica`**: host mints a ticket and
  revokes it; a third daemon subscribes to the session topic with the
  revoked ticket and runs the full `Workspace::join_with` lurk flow
  with a bounded `join_ticket_timeout`. Assert: `join_with` does NOT
  produce a populated workspace — no file content on disk, and the
  doc has no replica (no namespace imported). Against current code
  this FAILS (the lurker gets both). Turns green because the envelope
  never reaches a non-member (no unicast to a non-admitted peer; no
  replayed envelope — Replay is gated).
- **`expired_ticket_lurker_gets_no_replica`**: identical with an
  expired ticket instead of revoked.

Positive-path / regression-of-regression:
- **Read-tier admitted joiner** receives the envelope via unicast and
  syncs files (covered by rewrites across `workspace_sync.rs`,
  `workspace_filter.rs`, `workspace_lifecycle.rs`). Audit each
  `TICKET_ACTION` / `wait_for_ticket` site (`grep` inventory:
  `workspace_filter.rs:1023`, `workspace_lifecycle.rs:443`,
  `workspace_restart.rs:52/597/697`) and adjust any that hand-craft a
  broadcast `TICKET_ACTION` to the new publish path.
- **RW joiner**: envelope + NamespaceSecret both arrive (two
  `DeliveryFrame` kinds on one channel) — extend
  `tiered_tickets.rs::direct_stream_upgrade_delivers_secret` or a
  sibling.
- **Late attach**: daemon `JoinSession` admitted, `Workspace::join_with`
  invoked afterwards → envelope arrives from the mirror's persisted
  copy via `Subscribe` replay.
- **Joiner daemon restart**: persisted envelope survives; second
  `join_with` succeeds with no host re-publish (extends the
  `gossip.rs::joiner_replays_system_message_after_daemon_restart`
  shape).
- **Host restart**: envelope reloads with the record; workspace
  re-publish is byte-stable — carry over
  `workspace_restart.rs::re_hosting_same_dir_yields_structurally_identical_ticket`
  and `alice_post_restart_writes_reach_bob`, asserting the delivered
  bytes are identical across restart.

Tier C (`_n0`), `crates/artel-fs/tests/workspace_restart.rs` or a new
`_n0` test: **`unicast_workspace_ticket_delivery_real_n0`** — host +
joiner on real n0 (`EndpointSetup::Production`), assert the joiner
receives the envelope and exports a file across real QUIC/relay.
Suffix `_n0`, serial per `.config/nextest.toml`, run via
`make test-n0`. Precedent:
`alice_post_restart_writes_reach_bob_real_n0`.

**Commit 5**, green on `make test`; `make test-n0` for the `_n0` test;
`make ci-local` before the final commit.

## Slice 6 — docs + memory (final commit, with Slice 5 or separate)

- `docs/roadmap.md` — update the `artel-fs` / auth status: the
  gossip-lurker capability leak is closed; note the two carried
  residuals (replay traffic is still topic-visible to lurkers though
  capability-free; DocsGate/PeerFilter allow-list flip remains a
  follow-up). Bump the `PROTOCOL_VERSION` 9 / `artel/upgrade/2`
  references and the test count.
- Memory updates on landing (per the recall set):
  - `revoked-ticket-lurker-reads-files` → mark FIXED, point at this
    plan + the regression tests.
  - Note the new sanctioned-unicast payload (workspace ticket) on
    `gossip-only-inter-daemon` so the exception's scope stays recorded.
- `make ci-local` clean.

**Commit 6** (or fold into Commit 5).

## After landing (NEVER committed — scripts/ is git-excluded)

- Update `scripts/wsdemo` if the API changed. The public `Workspace`
  API is unchanged (host/join signatures identical; `publish_ticket`
  is internal), so wsdemo likely needs **no** code change — but its
  module doc comment (`main.rs:40-48`) describes the lurker residual
  as a known hole; update that prose to say it's closed.
- Rebuild + clippy: `cargo build --manifest-path scripts/wsdemo/Cargo.toml`
  and `cargo clippy --manifest-path scripts/wsdemo/Cargo.toml` (own
  workspace — `make test` doesn't cover it).
- Re-run the 2026-06-12 revoked-lurker smoke test; confirm the replica
  no longer materialises. Check `pgrep -af 'target/.*artel'` for
  orphan daemons before blaming n0 (memory
  `orphan-daemons-were-flake-source`).
- Leave the main workspace git-clean; never `git add scripts/` or
  `docs/handoff-*`.

## Explicitly OUT (do not let scope creep back)

- DocsGate/PeerFilter allow-list flip + NODE_ID announce resequencing
  (Decision 7 → follow-up slice).
- Topic-key rotation / true chatter privacy (replay frames remain
  topic-visible; capability-free is the bar for this slice).
- Any change to the artel-session ticket wire (`TICKET_VERSION`) or
  the NamespaceSecret/RW path beyond sharing the delivery helper.
- A NAK to the rejected lurker (joiner-side timeout is the UX, same as
  expiry — carried residual).

## Risks / open-at-implementation

- **`Request::Send` of `TICKET_ACTION` removal**: confirm nothing else
  in `artel-fs` or tests *sends* a `TICKET_ACTION` message expecting
  broadcast semantics. The `grep` inventory above is the checklist;
  let the compiler + test failures enumerate the rest.
- **Replay-injection ordering**: the synthetic ticket message is
  prepended to the IPC `subscribe` replay, but the workspace's
  `wait_for_ticket` also tolerates it arriving live first. Ensure no
  double-injection (live emit + replay) confuses the workspace — it
  matches on action and returns on first hit, so a duplicate is
  harmless, but assert it in a test.
- **`is_member` accessor**: `run_host_replay`'s gate needs a member
  check without taking the per-session lock in a deadlock-prone way —
  add a read-only accessor mirroring `is_local_session`
  (`session.rs:712`).
- **Admission-triggered replay re-entry**: it runs inside the
  bridge's JoinAnnouncement arm after `ensure_member` (which already
  drops the session lock before returning) — confirm no lock is held
  across `run_host_replay`'s broadcast.
- **64 KiB cap**: a pathological `PathRules` with thousands of globs
  could exceed it. Acceptable — document the ceiling; a workspace with
  64 KiB of rules is misconfigured. The encode side (`ticket::encode`)
  has no cap today; not adding one (the gossip path had none either).
- **Mixed-version mesh**: a `/1` daemon and a `/2` daemon fail at ALPN
  negotiation on the upgrade channel; the gossip handshake
  (`PROTOCOL_VERSION`) also rejects. Both are intended (alpha).
```
