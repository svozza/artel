---
date: 2026-06-12
topic: revoked-lurker-unicast-ticket
status: BRAINSTORM — key decisions user-confirmed 2026-06-12, ready for plan
parent: docs/brainstorms/2026-06-11-ticket-revocation-brainstorm.md (§ Residual gaps, "Gossip-lurker parity")
demo: wsdemo smoke test 2026-06-12 (memory `revoked-ticket-lurker-reads-files`)
superseded-note: 2026-06-19 — the verified-claims table below is a snapshot as
  of 2026-06-12 (stale line numbers; ALPN was still `artel/upgrade/1`). Two
  rows it recorded as gaps have since been CLOSED: "`PeerJoined` does not
  re-fire for an existing member" and "restarted joiner daemon does NOT
  resubscribe gossip" — both are now addressed by the offline-rejoin
  re-delivery work (`docs/plans/2026-06-18-rw-redelivery.md`): a reloaded
  mirror lazily re-subscribes, and re-delivery is triggered off the joiner's
  `NODE_ID` re-announce rather than `PeerJoined`. Read the table as history.
---

# Revoked-ticket lurker reads files — unicast the workspace ticket

## What we're building

A bearer of a revoked (or expired) artel-session ticket is correctly
refused admission, but today still ends up with a **live read-only
replica of the workspace files**. Proven end-to-end 2026-06-12 via the
wsdemo smoke test. The leak chain (each link verified in code):

1. Gossip topic subscription is unauthenticated — the session id is in
   the ticket; joining the mesh requires no admission (`topic_for`,
   `gossip_bridge.rs:167`).
2. The host serves `GossipBody::Replay` to ANY subscriber — no
   membership check in `run_host_replay` (`gossip_bridge.rs:1153`),
   and the frame carries no signed identity usable for auth
   (`delivered_from` is the relay hop, per the B.5 topology analysis).
3. The replayed backlog includes the host's `workspace.ticket`
   broadcast (System/`TICKET_ACTION`, `publish_ticket` in
   `artel-fs/src/workspace.rs:1527`) — a **read-capability
   `DocTicket`** wrapped in `WorkspaceTicketEnvelope` with `PathRules`.
   That is the capability leak: the lurker's iroh-docs node syncs the
   doc and bulk-exports the files.
4. `DocsGate` / `PeerFilter` are deny-lists — they reject
   known-REVOKED workspace ids; an unknown lurker id passes.

The fix: **nothing capability-bearing rides the gossip topic.** The
read-capability envelope moves to host→peer unicast over the existing
direct-QUIC upgrade channel (the one sanctioned gossip-only exception,
which already carries the RW `NamespaceSecret`), delivered at
admission. Additionally (user-confirmed, see Decision 4): the host's
`Replay` path becomes membership-gated, so a lurker gets *nothing* —
not even session chatter.

Alpha stance applies throughout (memory `alpha-no-backwards-compat`):
no old-client interop, no migration shims; versions bump freely and
mixed-version meshes fail the existing handshake.

## Current state (verified against the tree, 2026-06-12)

| Claim | Verified at |
|---|---|
| `run_host_replay` serves any subscriber, no membership check | `gossip_bridge.rs:1153-1172`; `Replay` arm at `:932` admits-nothing but serves all |
| Host's read `DocTicket` broadcast on the session log | `workspace.rs:701-706` (`share(Read)` → `publish_ticket`) |
| RW secret already rides direct QUIC, not gossip | `upgrade.rs` (`UPGRADE_ALPN = b"artel/upgrade/1"`), `dispatch_deliver_upgrade` (`server.rs:803`), `UpgradeProtocol::accept` (`upgrade_protocol.rs:44`) |
| Upgrade receive path validates sender == session host, Remote-only | `Registry::emit_upgrade` (`session.rs:2113-2145`) |
| Upgrade events are live-only: `Seq::ZERO`, never persisted, excluded from replay | `session.rs:2153-2174`, replay skip at `:1562` |
| Host cap-listener re-delivers RW secret on `PeerJoined` | `workspace.rs:2177-2196` |
| `PeerJoined` does NOT re-fire for an existing member (`ensure_member` early-returns) | `session.rs:1443-1445` |
| Joiner blocks in `wait_for_ticket` draining replayed events for `TICKET_ACTION` | `workspace.rs:894`, `wait_for_ticket` at `:1614` |
| `join_ticket_timeout: None` = wait forever (late attach is a documented contract) | `WorkspaceConfig::join_ticket_timeout` doc, `workspace.rs:148-155` |
| Restarted joiner daemon does NOT resubscribe gossip: self-rejoin early-returns before `materialise_remote_session`; `Registry::load` never calls `bridge.join_session` | `session.rs:1047-1048` + `:1084-1091`, `load` at `:675-701` |
| Mirror messages persist via `apply_inbound_mirror_message`, so `Subscribe` replays survive joiner restart | `session.rs:2204+`, pinned by `gossip.rs::joiner_replays_system_message_after_daemon_restart` |
| Joiner's `NODE_ID` announce happens AFTER initial sync | `workspace.rs:992-1003` (end of `join_with_inner`) |
| Byte-stable host ticket across restarts is a pinned contract | `workspace_restart.rs::re_hosting_same_dir_yields_structurally_identical_ticket` |
| UpgradeFrame size cap is 1 KiB (envelope with globs can exceed it) | `upgrade_protocol.rs:63` |
| `PROTOCOL_VERSION` 8, `TICKET_VERSION` 4, `MESSAGE_FORMAT` 3, `GOSSIP_WIRE_VERSION` 1, envelope v1 | `version.rs`, `gossip.rs:59`, `artel-fs/src/ticket.rs:29` |

## Approaches considered (joiner receive path — the deciding question)

### Approach A: live-only delivery, mirror the RW-secret pattern

Synthetic event on receipt, never persisted, host re-delivers on
`PeerJoined`. Most consistent with the existing invariant — but a
workspace that attaches **after** the live delivery has no recovery
path: the consumer holds only the `SessionId` (not the join ticket),
so it cannot re-announce to trigger re-delivery, and `PeerJoined`
never re-fires for an existing member. Late attach (a documented
contract — `join_ticket_timeout: None`) would hang forever. Also
broken by joiner-daemon restart (no gossip resubscribe on the
self-rejoin path). Rejected.

### Approach B: joiner persists the envelope; Subscribe replays it (CHOSEN)

Host daemon delivers the envelope over direct QUIC; the joiner daemon
persists it (opaque bytes) in its mirror record and injects a
synthetic ticket event both live on receipt and from the persisted
copy on every `Subscribe`. `join_with` keeps its drain-events shape;
`join_ticket_timeout` semantics carry over unchanged. Survives joiner
restart and late attach by construction. Cost: the mirror record gains
a capability-bearing field — same sensitivity class as the doc replica
the joiner already stores in the same state dir, `0600` like the rest.

### Approach C: joiner pulls the ticket over direct QUIC on demand

Robust to all orderings with zero persistence, but inverts the
sanctioned unicast direction (peer→host dial), adds a
request/response protocol to a one-shot push channel, and widens the
gossip-only exception beyond "host→peer delivery of session-key
material". Rejected.

**Choice: B** (user-confirmed). A fails two documented contracts; C
bends a convention this slice is supposed to honour.

## Key decisions (user-confirmed 2026-06-12)

1. **Joiner side: persist-in-mirror + replay-on-Subscribe**
   (Approach B above). The envelope is opaque bytes to the daemon
   (artel-daemon does not depend on artel-fs; decode stays in
   `artel-fs`). Stored alongside the mirror's session record with the
   same tmp+rename + `0600` discipline as `tickets.json`; absent ⇒
   none yet. The joiner-side synthetic event replaces `TICKET_ACTION`
   message-watching in `wait_for_ticket`.

2. **Host side: daemon-owned distribution.** The host workspace
   publishes the envelope **once** to its daemon via a new IPC request
   (replacing today's broadcast `Send`); the host daemon persists it
   in the session record and owns delivery:
   - on publish → deliver to all current members (minus self);
   - at admission (`ensure_member` success, including the
     JoinAnnouncement re-announce path — so leave-then-rejoin and
     joiner-restart both force redelivery) → deliver to the new peer;
   - host-daemon restart → envelope reloads with the session record;
     re-publish on workspace resume is idempotent (byte-stable ticket
     contract means the same bytes land — `workspace_restart.rs` pins
     this; the test moves from "same broadcast" to "same persisted
     envelope / same delivered bytes").
   This directly answers "what if the host workspace isn't up when a
   peer is admitted": the daemon delivers the persisted envelope
   anyway. The cap-listener keeps only its existing RW-secret duties.
   Rationale for rejecting the cap-listener-owned alternative:
   `PeerJoined` doesn't re-fire for existing members, and the
   workspace being down at admission time would silently skip a peer.

3. **Wire shape: one delivery channel, two payload kinds, ALPN bump.**
   `UPGRADE_ALPN` → `b"artel/upgrade/2"`. The bare `UpgradeFrame`
   struct is replaced by an externally-tagged postcard enum (memory:
   never serde tag/content on wire types):
   - `Secret { … }` — the existing NamespaceSecret payload;
   - `WorkspaceTicket { session_id, envelope_bytes }` — the
     postcard-encoded `WorkspaceTicketEnvelope` (incl. `PathRules`),
     opaque at this layer.
   Size cap 1 KiB → 64 KiB (envelopes carry user globs). Old frame
   shape deleted outright; mixed-version daemons fail at ALPN
   negotiation — the cleanest possible failure, per the alpha stance.
   Receive-side validation mirrors `emit_upgrade`: session exists,
   is `Remote`, sender is the session host.

4. **Replay becomes membership-gated, with admission-triggered
   backfill** (user chose to gate now, not defer). Two halves:
   - `run_host_replay` serves only if `delivered_from` ∈ session
     members — the same L1 topology assumption (`delivered_from` is
     trustworthy; in today's star topology the host hears joiners
     directly) as the existing `drop_if_spoofed` checks. Non-member
     requests drop with a warn.
   - The race this creates (joiner's first `Replay` follows its
     `JoinAnnouncement`; admission is async, so a gated host could
     drop the joiner's own backfill): solved by **admission-triggered
     replay** — when the host's JoinAnnouncement arm admits a peer
     (`ensure_member` Ok), it immediately runs `run_host_replay
     (session, Seq::ZERO)` for them. The new member always needs the
     backlog; no request, no retry loops, no timing dependence
     (memory: don't handwave flaky shapes). The joiner's explicit
     `Replay` publish stays for the already-member resubscribe case.
   - Note: replayed frames are broadcast on the topic (iroh-gossip has
     no unicast), so a lurker still *sees* replay traffic addressed to
     legitimate joiners. Accepted residual for v1 — same exposure as
     live chatter before this slice; the capability material no longer
     rides the topic at all, which is the load-bearing change. True
     chatter privacy is the topic-key-rotation conversation
     (roadmap § Future).

5. **Legacy broadcast hard-reject.** `publish_ticket` (broadcast
   `Send` of `TICKET_ACTION`) is deleted. The joiner's new wait path
   only accepts the synthetic unicast-delivered event — a broadcast
   `TICKET_ACTION` System message from a stale/malicious peer is inert
   chatter by construction, and a test pins that it does NOT
   materialise a workspace (precedent:
   `workspace_filter.rs::joiner_rejects_old_shape_doc_ticket_payload`,
   which itself gets rewritten — it currently asserts the broadcast
   path's decode behaviour).

6. **Version bumps.** `PROTOCOL_VERSION` 8 → 9 (new IPC
   request/response + new `Event` variant). Upgrade ALPN `/1` → `/2`
   (Decision 3). Unchanged: `TICKET_VERSION` 4 (artel-session ticket
   untouched), `MESSAGE_FORMAT` 3 (no `SessionMessage` change),
   `GOSSIP_WIRE_VERSION` 1 (no frame shape changes — replay gating and
   admission-replay are behavioural), `WorkspaceTicketEnvelope` v1
   (same bytes, new transport). `Meta`/record schema: the two new
   persisted envelope slots follow the `tickets.json` sidecar
   precedent — absent file ⇒ none; no schema bump.

7. **DocsGate/PeerFilter allow-list flip: follow-up slice, not here**
   (user-confirmed). With the ticket unicast, a lurker never learns
   the doc namespace, so the docs-sync gate is defence-in-depth rather
   than the primary barrier. The flip requires resequencing the
   joiner's `NODE_ID` announce ahead of initial sync (today it's
   after — the allow-list would block the very sync `join_with` waits
   on), which is its own risk and deserves its own failing-test-first
   treatment. Roadmap note + memory update on landing.

## Scope of the gossip-only exception (convention check)

The exception stays exactly "host→peer unicast delivery of
session-key material": NamespaceSecret (existing) + workspace ticket
envelope (this slice). Both flow host→peer only, both are delivered
over the same ALPN, both are validated sender-is-host on receipt.
Symmetric-P2P lens: per ADR-001, ticket/admission/discovery all get
redesigned together in the P2P rethink; this slice adds daemon-local
delivery code on a layer already scheduled for that rebuild, and
removes a *broadcast* capability leak that would have been worse in a
mesh.

## Test obligations (sketch — plan expands; per memory: extensive or not done)

**Regression (write FIRST against current code; must fail today):**
- fs-tier: revoked-ticket bearer drives the full lurk flow (join mesh
  with revoked ticket → subscribe → wait out the join) and must end
  with **no file content on disk and no doc replica**. Today it gets
  both — that's the bug.
- Mirror coverage for an **expired** ticket (identical hole).

**Positive path:**
- Admitted Read-tier joiner receives the envelope via unicast; files
  sync (rewrites of existing broadcast-join tests across
  `workspace_sync.rs`, `workspace_filter.rs`, `workspace_lifecycle.rs`,
  `workspace_restart.rs`, `tiered_tickets.rs`).
- RW joiner: envelope + NamespaceSecret both arrive (two frame kinds,
  one channel).
- Late attach: daemon admitted, workspace attaches afterwards →
  envelope arrives from the mirror's persisted copy.
- Joiner daemon restart: persisted envelope survives; `join_with`
  succeeds without the host re-broadcasting.
- Host restart: envelope reloads + re-publish is byte-stable
  (`workspace_restart.rs` contract carried over).
- Admission-triggered replay: fresh joiner gets full backlog with no
  explicit Replay served pre-admission.
- Replay gate: non-member's `Replay` is refused (no Message frames
  triggered by it); member's served.
- Legacy broadcast inert: a `TICKET_ACTION` System broadcast does not
  materialise a workspace on a v2 joiner.
- Wire: `DeliveryFrame` postcard round-trip, externally-tagged shape
  pinned, size cap, unknown-variant reject; ALPN string pinned.
- Daemon unit: envelope persist/reload (host + mirror sides), deliver
  on admission, deliver-all on publish, `emit_*` validation (Remote
  only, sender-is-host).

**Tier C (`_n0`):** one sibling — unicast envelope delivery across
real QUIC/relay (the new path crosses real infrastructure; precedent:
`alice_post_restart_writes_reach_bob_real_n0`). Follow
`.config/nextest.toml` tiering; run via `make test-n0`.

**After landing (never committed):** update `scripts/wsdemo` to the
new join flow, rebuild it (own workspace — `make test` doesn't cover
it), re-run the 2026-06-12 revoked-lurker smoke test and confirm the
replica no longer materialises.

## Open questions for the plan

- Exact IPC shapes: `Request::PublishWorkspaceTicket { session,
  envelope_bytes }` (host) + how `Subscribe`/event-stream surfaces the
  joiner-side synthetic event (`Event` variant vs synthetic
  System-message reuse). Postcard indices: append-only on both enums.
- Where the host-side envelope persists: `SessionRecord` field with
  sidecar file (mirror the `tickets.json` idiom) — names and exact
  store-trait ops are plan detail.
- Delivery failure handling on publish-to-all (peer offline): warn +
  rely on admission-redelivery at next re-announce, or bounded retry?
  (Lean: warn + redelivery-on-reannounce; joiner restart re-announces
  via `JoinAnnouncement`, covering the common offline case.)
- Does `dispatch_deliver_upgrade`'s dial/ACK plumbing factor into a
  shared helper for both frame kinds? (Almost certainly yes.)
- Slice boundaries + commit order (failing tests first, per task).

## Next steps

→ plan at `docs/plans/2026-06-12-revoked-lurker-unicast-ticket-plan.md`,
then implement in slices with a commit per slice.
