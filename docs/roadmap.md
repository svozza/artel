# Roadmap

Forward-looking plan for `artel`, written 2026-05-18 after the persistence
slice landed (commit `c5fb93c`). This document is the source of truth for
"what's next" — a fresh agent should be able to pick it up and execute
without re-asking design questions.

ADR-001 (`docs/adr/001-collab-substrate-platform.md`) is the architectural
contract. This roadmap describes the order in which the remaining ADR
commitments get implemented, plus the unknowns that must be resolved
along the way.

## Status

(Last refreshed 2026-06-22.)

| Crate | State |
|---|---|
| `artel-protocol` | Wire types + Unix-socket transport. Done. `PROTOCOL_VERSION` `13` (`9` workspace-ticket unicast → `10` `Event::Gap` lag-recovery → `11` cooperative-demote downgrade → `12` host-authority `RemoveSessionMember` → `13` gossip payload-size cap / `PayloadTooLarge`), `MESSAGE_FORMAT` `3`, `TICKET_VERSION` `4` (tiered tickets), `GOSSIP_WIRE_VERSION` `3` (`2` ctrl-v2 signatures → `3` 1 MiB transport cap), upgrade ALPN `artel/upgrade/2`. |
| `artel-daemon` | Persistent daemon + binary + flock-based pidfile (orphan-leak fix `9a1a773`) + issued-ticket ledger with revocation + lazy gossip re-subscribe for reloaded joiner mirrors (re-delivery after restart) + host-authority member removal (`RemoveSessionMember`, drops an evicted peer from durable membership). Done. |
| `artel-client` | Stateless multiplexed client + `artel` CLI binary + `connect_or_spawn`. Done. |
| `artel-fs` | Phase 3a (MVP) + 3b-1 (persistence) + 3b-3 (crash recovery) + host/join safety + PeerFilter shipped. Watcher new-subtree rescan landed (`e8244fe`, closes the inotify backfill race). Tier-1 write-revocation: namespace-secret rotation on evict + offline-peer re-delivery on rejoin, hardened 2026-06-21 (host-restart re-seed, `NODE_ID` re-delivery de-storm, evict drops daemon membership). Configurable filter (3b-4) shipped as consumer-owned exclude list (#35) + streaming large-file sync with a configurable size cap (#33, `max_file_size` default 64 MiB, `None` = unlimited; publish/apply stream via iroh-blobs) + incoming transfer-progress events (#38, `Transferring`, advisory/throttled). Author identity (3b-2) remains. |

897 tests passing on Tier A+B (`make test`), 18 more on Tier C
(`make test-n0`, real n0). fmt + clippy clean in both feature modes.
CI runs ubuntu + macos on stable; workspace `rust-version` is 1.95.

The substrate is a real P2P system with a complete v1 auth story:
two daemons cross-seed addresses over iroh-gossip, host/joiner
messaging round-trips through ack-correlated signed gossip frames,
sessions persist across restarts, `artel-fs::Workspace` mirrors a
directory between peers, and hosts can mint capability-scoped
tickets (Read / ReadWrite, with expiry) whose grants are enforced
end-to-end, on iroh 1.0. What's left is observability and the
consumer-driven 3b leftovers.

## Phase 1: client auto-spawn — DONE

Shipped `Client::connect_or_spawn(SpawnOptions)` plus daemon-side
stale-socket cleanup and an opt-in `--auto-spawn` flag on the `artel`
CLI. The original design notes follow for reference; for the actual
behaviour, read `crates/artel-client/src/spawn.rs` and the
integration tests in `crates/artel-daemon/tests/auto_spawn.rs`.

### Original design (preserved for reference)

ADR-001 § "Auto-spawned daemon lifecycle" calls for "the first client
connect spawns the daemon if it is not running." Today,
`Client::connect` errors with `Transport(Io)` if the socket is missing.

### Scope

Add `Client::connect_or_spawn(socket_path, daemon_binary)` to
`artel-client`:

1. Try `connect(socket_path)`. If success, done.
2. On `NotFound` / `ConnectionRefused`:
   - Read the PID file (resolved from socket_path's parent).
   - If PID file exists and points at a live process → wait briefly
     and retry connect (race: daemon is still starting up).
   - If PID file is stale or missing → spawn `daemon_binary` as a
     detached child, wait for the socket to appear (with timeout),
     retry connect.
3. Stale-socket recovery: if the socket file exists but is unreachable
   AND the PID file is stale, delete both and spawn fresh.

### Open questions

- **How does the client find the daemon binary?** Three options, ranked:
  1. Caller passes the path explicitly. Cleanest. Recommended.
  2. Search `$PATH` for `artel-daemon`. Convenient; ambiguous if multiple.
  3. Hardcode the install path. Brittle.
- **Detach mechanism on Unix.** `std::process::Command` + double-fork +
  `setsid` is the textbook approach. tokio doesn't ship a "spawn
  detached" helper; do it manually.

### Tests

- Auto-spawn happy path: tempdir, no daemon → `connect_or_spawn`
  succeeds. Verify a daemon process exists with the expected PID.
- Stale-PID recovery: write a fake PID file pointing at PID 1 (or a
  recently-exited child) → succeeds.
- Race-on-startup: two `connect_or_spawn` calls in parallel against a
  cold dir → exactly one daemon ends up running.
- Daemon binary missing → clean error, no spawned process.

### Definition of done

- `Client::connect_or_spawn` works against a real `artel-daemon` binary
  in a tempdir.
- `artel` CLI uses it: `artel list` against a stopped daemon either
  spawns one transparently OR fails with a clear "no daemon, run
  `artel-daemon` first" error (decide during implementation).
- New unit tests + 1 integration test in `artel-client/tests/`.

## Phase 2: iroh integration (multi-slice)

This is the slice that turns artel from a fancy local IPC bus into the
P2P substrate ADR-001 promises. Sliced into 2a..2d to keep blast
radius small.

### 2c-2c — Joiner→host send over gossip — DONE

- `artel-protocol::gossip` v1 → v2: adds
  `GossipBody::SendRequest { req_id, peer, payload }` and
  `GossipBody::SendAck { req_id, result }`. Joiner publishes the
  request; host's bridge picks it up, drives `Registry::send`,
  publishes the ack with the assigned `SessionMessage` (or the
  host's `ProtocolError` on rejection). Joiner correlates via the
  `req_id` Uuid.
- All inter-daemon traffic stays on the gossip topic. No
  dedicated direct-QUIC sidechannel — preserves the option of
  symmetric P2P later (ADR-001 § "Future evolution") since the
  transport doesn't bake in the host-as-sequencer assumption.
- `GossipBridge` gains `pending_sends: HashMap<Uuid, oneshot::Sender>`
  and a `Weak<Registry>` injected at startup via
  `attach_registry`. Per-session `SessionRole { Host, Joiner }`
  drives the inbound forwarder's dispatch. `send_remote` allocates
  a req_id, registers the oneshot, broadcasts the request, awaits
  the ack with a 10s ceiling.
- `Registry::send` now returns the freshly-built `SessionMessage`
  (not just `Seq`) so the bridge can package it into `SendAck.Ok`.
  IPC dispatch reads `.seq` from the result.
- Lazy membership: a joiner's first `SendRequest` doubles as their
  arrival on the host. `Registry::ensure_member` admits + persists
  + emits `PeerJoined` before delegating to `send`. A future slice
  can replace this with an explicit `JoinAnnouncement` frame.
- `SessionError::HostRejected(ProtocolError)` carries the host's
  verdict back through the joiner's IPC response verbatim — a
  joiner that sends after the host closes the session sees
  `UnknownSession` rather than a generic `Internal`.
- 2 new e2e tests:
  - `tests/iroh_joiner_send_fanout.rs`: Bob joins Alice's session,
    Bob sends, Alice and Bob both observe the `Message` with the
    host-assigned seq. Bob's IPC reply carries the same
    `SessionMessage`.
  - `tests/iroh_joiner_send_rejected.rs` (rewritten): Alice
    closes the session, Bob sends → IPC error surfaces the host's
    `ProtocolError::UnknownSession` via the `SendAck.Err` path.
- 230 → 235 tests; clippy + fmt clean both feature modes.

### 2c-2b — Host→joiner one-way gossip fanout — DONE

- New `artel-protocol::gossip` module: `GossipFrame` + `GossipBody`
  envelope (postcard) carrying `SessionMessage` between daemons on
  a topic. v1 wire version; bumped on structural change.
- New `gossip_bridge.rs`: `GossipBridge` owns per-session
  `(GossipSender, forwarder JoinHandle)` pairs. Topic id is
  derived deterministically from session id (first 16 bytes), so
  no topic field needed in tickets.
- `Registry` gains an optional `bridge`. `Registry::host` opens a
  topic; `Registry::send` (host side) publishes each new
  `SessionMessage`; `Registry::join` for a remote ticket
  materialises a local mirror, seeds the host's addr into the
  endpoint's address book, and spawns a forwarder that decodes
  inbound frames into the mirror's log + broadcast.
- `Session` gains a `kind: Local | Remote` discriminator. `Send`
  on a remote session returns `ProtocolError::NotHost` (joiner
  send arrives in 2c-2c with request/reply correlation).
- Joiner-side `subscribe` waits on `GossipReceiver::joined` (15 s
  ceiling) before `JoinSession` returns, so the gossip mesh is
  formed by the time the host can publish. Without it a host that
  sent immediately after the joiner's IPC handshake landed
  silently lost the message.
- 2 e2e tests split across binaries to avoid in-process iroh
  contention: `tests/iroh_gossip_fanout.rs` (host→joiner round
  trip) and `tests/iroh_joiner_send_rejected.rs` (joiner `Send`
  surfaces `NotHost`). Each ~3.4 s.
- 223 → 230 tests; clippy clean both feature modes.

### 2c-2a — Tickets carry host NodeAddr — DONE

- Bumped `TICKET_VERSION` 1 → 2.
- New `WireEndpointAddr { peer_id, relay_url, direct_addrs }`
  in `artel-protocol::ticket`. Iroh-free mirror of
  `iroh::EndpointAddr`. Postcard-encodes inside the ticket body.
- `Registry` gains a `daemon_addr: WireEndpointAddr` field;
  `Daemon::start` snapshots `iroh::Endpoint::addr()` into it via a
  new `iroh_endpoint_to_wire` boundary. Falls back to id-only when
  the daemon is local-only.
- Ticket decode does a self-consistency check
  (`host_addr.peer_id == host_peer_id`) so tampered or
  cross-version tickets surface as `Malformed`.
- 220 → 223 tests (+2 ticket unit tests, +1 e2e identity test).
  No routing yet — that's 2c-2b.

### 2c-1 — iroh-gossip wiring + accept loop — DONE

- iroh-gossip 0.98 added behind the existing `iroh` feature.
- The daemon now stands up an `IrohRuntime` ({ Endpoint, Gossip,
  Router }) at start; `Router::shutdown` cleans up everything,
  including the underlying Endpoint, on the way out.
- `DaemonConfig` gains an `endpoint_setup: EndpointSetup` knob so
  integration tests can swap n0 production discovery for a
  localhost `iroh::test_utils::DnsPkarrServer` without touching
  any other daemon code. The field is unconditionally present;
  `EndpointSetup::default()` (Production / `presets::N0`) works
  in either feature config. (Originally an opaque
  `Option<AddressLookupOverride>` wrapping a `MemoryLookup`;
  migrated to the upstream `DnsPkarrServer` fixture in
  `bb8892f` after `MemoryLookup` proved too aggressive a
  short-circuit — see "n0 rate-limit flakiness" below.)
- `Daemon::iroh()` returns the runtime to embedders/tests. No
  `Registry` changes yet — that comes with 2c-2.
- 1 new smoke test (`tests/iroh_gossip_smoke.rs`): two in-process
  daemons cross-seed addresses, subscribe to a topic, exchange a
  payload. Real QUIC handshake, ~3 s.
- 219 -> 220 tests; both feature modes still clean.

### 2b — Real artel:-prefixed ticket format — DONE

- `artel-protocol::ticket`: postcard-encoded payload of
  `{version, session_id, host_peer_id}` wrapped in
  `artel:<base32-nopad-lowercase>`. ~85-char text form.
- `Registry::host` emits the new format; `Registry::join` decodes via
  the new module. Old `artel-local:<uuid>` strings now hit
  `MissingPrefix` → wire `InvalidTicket`.
- 12 ticket unit tests (1 proptest); 208 -> 219 total.
- Deferred to 2c: `NodeAddr` and topic id in the wire payload.
  Wire version slot is reserved so adding them is non-breaking for
  this build (it'll bump TICKET_VERSION).

### 2a — Endpoint + persisted secret key — DONE

- iroh 0.98 added as a default-on `iroh` cargo feature on
  `artel-daemon`. Without the feature the daemon is local-only with a
  synthetic peer id.
- New `iroh_key.rs`: `load_or_create(path)` that reads or generates an
  ed25519 secret with `OsRng`, persists 32 bytes atomically at mode
  0600 next to `daemon.pid` (`~/.artel/iroh.key` by default).
- `Daemon::start` builds an `Endpoint` from the loaded key when
  `DaemonConfig::iroh_key_path` is `Some` and uses the resulting
  `EndpointId` as the wire peer id. `Daemon::run` calls
  `endpoint.close()` on shutdown.
- 11 new tests (8 unit, 3 e2e); 197 -> 208 total.

Today's placeholders:

- `artel-protocol::PeerId` is an opaque `[u8; 32]` (sized for an iroh
  node id but with no iroh dep).
- `artel-protocol::JoinTicket` is `String`. Daemon-side it's
  `"artel-local:<uuid>"`.
- `artel-daemon::server::DaemonConfig::daemon_peer_id` is supplied by
  the caller; `main.rs` derives a synthetic from PID.

### Scope

1. **Add `iroh` as an optional dep on `artel-daemon`.** Behind a
   default-on `iroh` cargo feature so the daemon can still be built
   without it (useful for unit tests, niche embeds).
2. **Spawn an iroh `Endpoint` at daemon startup.** Persist the secret
   key under `~/.artel/iroh.key` (mode 0600). On restart, load the same
   key so the daemon's peer identity is stable. `daemon_peer_id` becomes
   the iroh `NodeId`.
3. **Real ticket format.** A ticket carries:
   - `SessionId` (so the daemon knows which session to route into)
   - Host's iroh `NodeAddr` (so the joiner can dial)
   - A nonce / topic identifier for iroh's gossip layer
   Encoded as base32 (per iroh convention) with a clear `artel:` prefix.
   Old `artel-local:` tickets are rejected with `InvalidTicket`.
4. **Inter-daemon transport.** When daemon A hosts and daemon B joins:
   - B's daemon parses the ticket, dials A's NodeAddr.
   - A QUIC connection is established between the two iroh endpoints.
   - Subsequent `Send` from B's client → B's daemon → iroh → A's daemon
     → A's session log → broadcast to A's subscribers (including any
     of B's clients subscribed via B's daemon).
5. **Daemon-as-host vs daemon-as-relay.** Today only the host daemon
   has the session log. Other peers' daemons hold mirror state per
   ADR-001 (sessions persist locally so reconnection is fast). Mirror
   state is replayable from the host on rejoin via `Subscribe { since }`.

### Big unknowns to resolve up front

- **iroh-gossip vs custom QUIC streams.** iroh has a gossip primitive
  that sounds like a fit but adds dependencies. Direct-streams via
  `Endpoint::accept` may be simpler. Investigate before committing.
- **Connection lifecycle.** When does daemon A drop its connection to
  daemon B? Idle timeout? Explicit Leave? Affects how `Subscribe`
  behaves across daemons.
- **Authoritative log ownership.** Currently the host's daemon assigns
  Seq. With iroh, network round-trip on every `Send` means clients see
  visible latency. Options:
  1. Host-assigns-seq, send is round-trip blocking. Simple, slow.
  2. Local daemon assigns provisional seq, host reconciles. Complex.
  3. CRDT-style log with vector clocks. ADR-001 § "Symmetric P2P"
     territory — explicitly future work.
  Pick (1) for v1; document the latency trade-off; revisit after real
  usage. ADR-001 already commits to the host-as-sequencer model.
- **NAT traversal failures.** iroh handles relay fallbacks but not
  every network is friendly. The transport layer needs to surface
  "couldn't reach the host" cleanly to the joiner.
- **Persistence & iroh state.** The session log already persists. iroh
  connection state (active sessions, peers seen, etc.) is in-process —
  on daemon restart we drop existing peer connections and they
  reconnect. This is fine for v1.

### Tests

- **Single-daemon iroh smoke test.** Daemon A hosts; client connects
  through the daemon; basic Hello/HostSession works with the iroh
  Endpoint up but no peer activity.
- **Two-daemon round-trip.** Spin two daemons in-process with separate
  state dirs and separate iroh endpoints. A hosts, B joins via the real
  ticket, B sends, A's subscriber observes the message.
- **Host disconnect / rejoin.** Daemon B disconnects from A, rejoins,
  Subscribe-since-N replays missed messages.
- **Invalid ticket variants.** Old `artel-local:` ticket → InvalidTicket.
  Truncated base32 → InvalidTicket. Valid format but unreachable
  NodeAddr → distinguishable error.
- **Persisted iroh secret key.** Restart the daemon, NodeId is stable.

### Definition of done

- Two daemons on the same machine (different state dirs) can host/join
  and exchange messages.
- A single daemon can also host and a client (via `artel-client`) can
  send/receive without any inter-daemon hop.
- Tickets are real iroh tickets, not strings. Old tickets rejected.
- Daemon NodeId is stable across restarts.
- `iroh` cargo feature is default-on; without the feature, the daemon
  is local-only and rejects join attempts with a clear error.

## Phase 3: artel-fs (medium-large)

ADR-001 § "Doc handles across IPC" picked the **ticket-handout** shape
for v1: each `Workspace` spawns its own iroh `Endpoint` + `Gossip` +
`Docs` + `Blobs`, distinct from the daemon's. The daemon stays
ignorant of doc semantics; tickets ride the artel session as a
`MessageKind::System` message with action `workspace.ticket`.

### Slice 3a — MVP — DONE

Sub-slices, in order:

- **3a-1** — iroh-docs / iroh-blobs version-compat smoke test. Confirmed
  iroh 0.98 + iroh-docs 0.98 + iroh-blobs 0.100 mate; verified the
  `DocTicket` carries enough `EndpointAddr` info on its own (no
  out-of-band `add_endpoint_info` needed).
- **3a-2** — Pure-logic modules: `keys` (path↔key, NFC, traversal
  guards), `filter` (hardcoded skips + symlink + `.gitignore` + 1 MiB
  cap), `echo_guard` (pending-set + last-published-hash). 26 unit
  tests.
- **3a-3** — `Workspace::host`. Spawns its own iroh node, creates the
  Doc, runs `scan_and_publish_existing`, broadcasts the `DocTicket`
  as a system message. Integration test: a second client subscribed
  to the session observes the ticket.
- **3a-4** — `Workspace::join`. Subscribes to the session, drains
  events until the ticket arrives (15 s ceiling), `import_and_subscribe`
  → wait for `SyncFinished` + `PendingContentReady` → `bulk_export`
  to disk under echo guard. Two-daemon test: Bob's empty dir mirrors
  Alice's two files after `join` returns.
- **3a-5** — Watcher + applier. `notify-debouncer-full` 300 ms
  debounce → `Doc::set_bytes` / `Doc::del`. Applier listens on
  `Doc::subscribe()`, handles `InsertRemote` and `ContentReady` with
  250 ms echo-guard release grace. `Workspace::run` spawns both as
  tokio tasks. Live-edit test: Alice writes → Bob's filesystem
  reflects within ~1 s.
- **3a-6** — End-to-end round-trip test (`tests/round_trip.rs`).
  Bidirectional file edits, gitignore exclusion, and echo-guard
  sanity (1 doc entry per applied key, not 2). Runs the full
  scenario 3 consecutive times to flush out gossip-on-gossip-on-fs
  flakiness.

Storage was memory-only this slice (`Docs::memory()` + blob
`MemStore`); on workspace restart, host re-scans the dir and
re-publishes. Disk-backed Docs/Blobs is a follow-up slice.

### Slice 3b — hardening

- **3b-1 — Disk-backed storage.** DONE.
  `iroh.key` (mode 0600) + `doc-id` + `Docs::persistent(...)/docs/`
  (redb + default-author) + `FsStore` blobs all live under a per-
  workspace `state_dir` (default `<root>/.artel-fs/`, configurable
  via `WorkspaceConfig::with_state_dir`). Host reuses the same
  `NamespaceId` across restarts so existing tickets stay valid; on
  reopen the host runs a reconcile pass that tombstones doc entries
  whose backing files vanished offline, then `scan_and_publish_existing`
  re-asserts the current disk state. Joiners persist their docs +
  blobs too, so an offline joiner keeps its synced files on disk.
  `bulk_export` queries `Query::single_latest_per_key().include_empty()`
  so a returning joiner picks up tombstones the host published while
  it was offline. See `docs/handoff-phase-3b.md` for the layout
  rationale and the residual sketches for 3b-2/3/4 below.
- **3b-2 — Persistent author identity.** Today we lean on
  `iroh-docs`'s built-in `default-author` file under `state_dir/docs/`,
  which is good enough until a real consumer wants per-author
  attribution surfaced in `WorkspaceEvent`. Sketch in handoff doc.
- **3b-3 — Crash recovery.** DONE.
  `tests/crash_recovery.rs` spawns Alice's host as a child process
  (`tests/bin/crash_child.rs`), SIGKILLs it at three different
  points (steady-state, mid scan-and-publish, mid live-write), and
  verifies the workspace recovers on restart with Bob's mirror
  intact. Surfaced and fixed `iroh-docs`'s 500 ms commit-batch
  window: a SIGKILL between `Docs::create` returning and the redb
  commit firing leaves a `doc-id` pointing at a non-existent
  namespace; `open_or_create_doc` self-heals by recreating the doc
  when `Docs::open` doesn't find the persisted namespace.
- **3b-4 — Configurable filter.** DONE, in a different shape than
  sketched. The original sketch (`WorkspaceConfig::filter:
  FilterRules` extending or overriding the hardcoded skip list) was
  rejected: the hardcoded skips (`.git`, `target`, `node_modules`,
  `.DS_Store`, `*.swp`, `*.tmp`, `.artel-fs`) are the substrate's
  *self*-protection and stay non-overridable by design. What shipped
  instead: `WorkspaceConfig::exclude` (consumer-owned glob list,
  dotfile default, replace-not-merge — #35, which also removed the
  `.gitignore` layer) and `WorkspaceConfig::max_file_size`
  (streaming large-file sync, cap as accident-guard — #33).

None block the next phase; pick them up when a real consumer needs
them.

## Phase 4: production hardening

Two concrete workstreams that close gaps blocking real consumers
from using `artel-fs` end-to-end. Both are now DONE; the original
design notes are preserved below.

### Workspace host/join safety — DONE

Shipped per `docs/plans/2026-05-22-workspace-host-join-safety-plan.md`:
clone-of-host's-tree semantics, and an `AttachPolicy { RequireEmpty,
AllowExisting }` parameter on `Workspace::host` / `Workspace::join`.
`RequireEmpty` (the safe default for fresh hosts and joiners) refuses
to attach to a non-empty root, where "empty" is computed at the top
level only — the state dir, hardcoded-skip paths (`.git/`, `target/`,
etc.) don't count, and top-level symlinks do. See the `AttachPolicy`
docs in `crates/artel-fs/src/workspace.rs`.

#### Original problem statement (preserved for reference)

Today `Workspace::host` runs `scan_and_publish_existing` on
whatever dir it's pointed at, and `Workspace::join` runs
`bulk_export` into whatever dir it's pointed at. Both behaviours
are unsafe in the wrong dir:

- Hosting on the wrong dir (e.g. `~`) publishes the entire tree
  into the doc and out to any joiner. Surfaced 2026-05-20 while
  smoke-testing two-process sync end-to-end; nearly published a
  whole home dir before someone noticed.
- Joining into a non-empty dir silently overwrites local files
  via `bulk_export`'s `tokio::fs::write(&path, ...)`. The joiner
  has no opportunity to inspect what's about to land.
- The current asymmetry — joiner's pre-existing files are *not*
  propagated outward — is itself a bug or a feature depending on
  which mental model we pick.

Open design questions before any code change:
1. **Mental model.** "Shared bucket" (any party can drop files
   in, all parties see all) vs "clone of host's tree" (host owns
   the canonical tree; joiners get a copy and can edit it but
   their local pre-existing files are irrelevant on join).
   Clone-semantics matches `git clone`, removes the
   joiner-publishes-outward risk entirely, and is probably the
   right v1 default.
2. **Default policy on non-empty target dir.** Refuse to host or
   join if the dir is non-empty, modulo `--allow-existing` /
   `--init-from-this-dir` opt-in flags? Empty `.artel-fs/` and
   filtered paths (`.git`, `target`, etc.) shouldn't count as
   "non-empty".
3. **Symmetric scan?** If we adopt shared-bucket semantics
   instead of clone, the joiner needs `scan_and_publish_existing`
   too, which multiplies the wrong-dir risk onto the joiner side.
   Only worth doing once #1 and #2 are settled.

Until this is designed, do not silently change scan/bulk_export
behaviour or add piecemeal guards. Existing testing should use
fresh empty dirs.

### Multi-session resume across daemon restarts — DONE

All three sub-items below landed (stable session id, attachment
registry, resume-in-place via `host_with`). Original notes preserved.

Surfaced 2026-05-20 while smoke-testing two-process sync: when
either side's CLI dies and restarts pointing at the same
workspace dir, the new instance can't reliably reattach to the
existing session — the joiner's daemon is still tracking an
old session id, the new host mints a fresh one, and the two
sides talk past each other. Workaround today is `rm -rf
.artel-fs/` between runs, which defeats the point of persistence.

The substrate is mostly there:

- Phase 1 auto-spawn lets the daemon outlive any single client
  invocation. A long-running daemon at the default state path
  (`artel-protocol::transport::path::default_dir`) is the basis
  for everything below.
- 3b-1 disk-backed persistence makes the host's namespace + iroh
  identity stable across restarts. A re-hosted workspace produces
  a byte-identical ticket, and existing joiners' tickets keep
  working without re-issuing.
- 3b-3 crash recovery proved both halves resume correctly under
  SIGKILL.

What's missing:

1. **~~Stable session id across host restarts.~~** DONE.
   `Request::HostSession` now carries an optional caller-supplied
   `Option<SessionId>` (`PROTOCOL_VERSION` bumped 1 → 2);
   `artel-fs::Workspace::host_with` derives the id deterministically
   from the local `NamespaceId` via `session_id_for` and registers
   with the daemon at that id. First host mints; every restart
   resumes the existing local-host record verbatim (members, log,
   head preserved). Re-stamps the ticket with the daemon's current
   `daemon_addr` so a joiner with the old ticket keeps working
   across the host's daemon restart. See
   `docs/brainstorms/2026-05-26-stable-session-id-brainstorm.md` and
   `docs/plans/2026-05-26-stable-session-id-plan.md`. Sub-slices 1a
   (protocol), 1b (daemon), 1c (artel-fs) all landed; 1d is this
   roadmap update.
2. **~~Workspace registry on the daemon side.~~** DONE. The wire
   shape is *attachment*-shaped, not workspace-shaped per ADR-001's
   layering — the daemon never inspects payloads. Three new RPCs:
   `Request::RegisterAttachment` / `ListAttachments` /
   `ForgetAttachment` (and a `Response::Attachments` carrying a
   `Vec<Attachment>`); `PROTOCOL_VERSION` bumped 2 → 3. Daemon
   stores per-session attachments under
   `<session>/attachments/<lowercase-hex(kind)>.bin`, cascading
   with the session via `remove_dir_all`. `artel-fs` defines
   `WorkspaceAttachmentV1` (postcard, schema frozen for
   `KIND_V1 = "artel-fs/workspace/v1"`) and registers on
   `Workspace::host_with` / `join_with`; consumers enumerate via
   the typed `list_known_workspaces` helper. Both leave paths
   cascade: host-leave closes the session and clears all
   attachments; joiner-leave on a `Remote` mirror drops the mirror
   entirely and clears the joiner's attachment. See
   `docs/brainstorms/2026-05-27-workspace-registry-brainstorm.md`
   and `docs/plans/2026-05-27-workspace-registry-plan.md`.
   Sub-slices 2a (protocol), 2b (daemon storage + cascade + IPC
   handlers), 2c (artel-fs registration + `list_known_workspaces`)
   all landed; 2d is this roadmap update + ADR-001 addendum.
3. **~~`Workspace::resume`?~~** DONE in place — `Workspace::host_with`
   already does the right thing if `.artel-fs/` exists: opens the
   existing namespace (via 1c's `session_id_for` derivation), runs
   the reconcile pass, re-broadcasts the ticket. No new constructor
   needed; consumers just call `host_with` again. The structural-
   identity property (same `NamespaceId`, same host `NodeId(s)`) is
   what existing joiners' tickets actually depend on; byte-identity
   of the whole ticket is too strong because address-discovery info
   inside a ticket can drift legitimately (e.g. relay URL list
   ordering — see `disk_resume.rs` line 222). Pinned by
   `tests/host_restart_ticket_stable.rs`.

#### Stale-daemon detection and cleanup

Surfaced 2026-05-23 while finishing the Workspace host/join
safety slice: rapid back-to-back `cargo test` runs against
`round_trip` / `live_edit` / `crash_recovery` failed ~30% with
"never saw expected bytes" panics that a 30-second cool-down
fully eliminated.

Investigated 2026-05-25 (see prior `docs/handoff-stale-daemon.md`,
deleted on resolution). Evidence: Matrix A (rapid back-to-back)
2/8 pass, Matrix D (rapid + `pkill` between runs) 3/6 pass —
identical failure rates ruled out *local* process state, leaving
n0 DNS publish/resolve as the accumulator. The per-workspace
iroh nodes used `iroh::endpoint::presets::N0`, so each test run
made four pkarr publish + DNS-lookup calls against
`dns.iroh.link`, hitting external rate limits the test harness
had no business paying.

**First fix (2026-05-25), since superseded**: `WorkspaceConfig`
gained an `address_lookup_override: Option<MemoryLookup>` knob;
test harness cross-seeded a shared `MemoryLookup` across both
daemons and both workspaces. 10/10 passed on the rapid-iteration
matrix. But the in-memory address book was too aggressive a
short-circuit — see the host-restart-ungraceful investigation
below — so the substrate moved on.

**Current shape (2026-05-28, `bb8892f`)**: substrate now exposes
`EndpointSetup::{Production, Testing}` on both `WorkspaceConfig`
and `DaemonConfig`. `Testing` wraps an
`iroh::test_utils::DnsPkarrServer` — the upstream-recommended
fixture iroh-docs uses in its own tests. Localhost pkarr-publish
HTTP server + localhost DNS server with shared state, run for
the test's lifetime. Same code path as production except for
the physical infrastructure (no n0 rate limits, no DNS
propagation race against `dns.iroh.link`). `tests/common/mod.rs`
hands one `Arc<DnsPkarrServer>` to both daemons and both
workspaces; the `on_endpoint(&id, timeout)` gate eliminates the
publish/dial race. Production callers stay on
`EndpointSetup::Production` (the default — `presets::N0` +
n0 discovery) unchanged.

`crash_recovery.rs` stays on real n0 by necessity — the child
binary runs in a separate process and can't share an in-process
fixture. It's slow, and any failure must be diagnosed via
`docs/diagnosing-flaky-tests.md` (per-phase timeouts +
tracing-subscriber + run-until-fail) before being labelled an
infra issue. "Flaky" is never an acceptable resting state.

The test pyramid has since grown to three tiers (see the
faster-cargo-test entry under Future: Tier A unit + Tier B hermetic
`DnsPkarrServer` / localhost relay + Tier C real-n0 `*_n0` under
`--profile n0`). The original two-tier example, kept for the
diagnostic principle:
- `iroh_internals.rs::doc_ticket_round_trips_via_localhost_pkarr_dns`
  (default, deterministic, fast) asserts the
  `DocTicket`-carries-enough-addressing contract over the localhost
  fixture.
- `..._without_manual_address_seeding_n0` (real n0) asserts the same
  property over n0's real infrastructure with an
  application-layer retry loop. Both passing → substrate fine.
  If the n0 sibling fails while the hermetic sibling passes,
  that's a hypothesis (production-discovery-only bug vs. infra
  flake vs. topology-triggered upstream bug — see the 2026-06-11
  case study) — not a conclusion. Apply the recipe in
  `docs/diagnosing-flaky-tests.md` and confirm before
  labelling.

The migration retired the `MemoryLookup`-based knob entirely;
the `host_restart_ungraceful_n0` regression-trap test was also
deleted because `tests/drop_bomb.rs` already pins the
contract via a child-process stderr capture without paying the
n0 round-trip. See `docs/diagnosing-flaky-tests.md` for the
diagnostic recipe and the corrected pyramid.

What's still on the table:

- **Production daemon stress**: a daemon that's been up for
  days through many connect/disconnect cycles should not
  silently degrade. Build a stress harness (spawn N fresh
  daemons in sequence against the same default state dir,
  measure connection-setup latency on the Nth) and verify it
  doesn't trend upward. The test-harness fix dodged the
  symptom in CI but the production-side fragility — n0
  registrations stale-pointing at dead peers, unreaped
  iroh-gossip topic memberships — is unproven and unmeasured.
  Lower priority than the resume work above.

## Near-term

- **~~iroh 1.0 upgrade.~~** DONE (2026-06-22). The whole family
  moved to stable together: `iroh` 0.98 → `1.0`, `iroh-relay` 0.98 →
  `1.0`, `iroh-gossip`/`iroh-docs` 0.98 → `0.101`, `iroh-blobs`
  0.100 → `0.103`; `n0-error` 0.1 → `1.0` to match iroh's. The
  upgrade pulls in the noq-proto 1.0.0 four-tuple rework
  (iroh#4273/#4281) that fixes the handshake path-poisoning bug —
  iroh 0.98.2's noq-proto 0.17.0 deterministically wedged
  acceptor-side handshakes when the dialer reached a localhost relay
  and same-machine direct addrs simultaneously (diagnosed
  2026-06-11; case study in `docs/diagnosing-flaky-tests.md`). All
  four `INTERIM (iroh 0.98.2)` sites — `workspace_restart.rs`,
  `revoked_lurker.rs`, `auth_b5_control_frames.rs`, `identity.rs` —
  were reverted from n0's public relay (`Production`) back to the
  localhost shared relay (`ProductionCustomRelay`), so Tier C no
  longer depends on n0's public infra. The rc1-era breaking changes
  the notes warned about (`IncomingLocalAddr` → `LocalTransportAddr`,
  `PathEvent` non-exhaustive, `CustomSender::poll_send` `src` arg)
  don't touch any of our call sites. What actually broke and was
  fixed: `iroh::tls::CaRootsConfig` → `CaTlsConfig` (the
  `ca_roots_config` builder method is deprecated, renamed
  `ca_tls_config`); `iroh::endpoint::ConnectionInfo` removed —
  `EndpointHooks::after_handshake` now takes `&Connection`;
  `DnsPkarrServer`'s `endpoint_origin` / `pkarr_url` fields are now
  private (only `pkarr_url()` has an accessor), so the test DNS
  origin is owned via `endpoint_setup::TEST_DNS_ORIGIN` and the
  fixture is constructed with `run_with_origin(TEST_DNS_ORIGIN)`
  rather than the argless `run()`.

## Future

Listed for completeness, no detailed plan yet:

- **~~Ticket-level capabilities & auth (tiered tickets).~~** DONE
  (2026-06-05..07; `PROTOCOL_VERSION` 6→7, `TICKET_VERSION` 3→4).
  Tickets now carry `granted_cap: Capability { Read, ReadWrite }`,
  `expiry_ms`, and a host signature over `(ticket_id, cap, expiry)`
  under the `"artel/ticket-cap-v1"` domain, verified at admission.
  `Request::IssueTicket { session, granted_cap, expiry_ms }` lets a
  host mint multiple tickets at different tiers for one session.
  Read-tier joiners receive the unicast-delivered workspace ticket
  but can't author; a later `CapabilityAction::Grant` to ReadWrite
  triggers **upgrade delivery** — the `NamespaceSecret` rides a
  direct QUIC stream (`artel/upgrade/2` ALPN since the lurker fix,
  `UpgradeProtocol` on the daemon Router, `DeliveryFrame` /
  `UpgradePayload` wire types in `artel-protocol::upgrade`)
  rather than the gossip topic, so the secret is never broadcast.
  This is the one sanctioned exception to gossip-only inter-daemon
  traffic — host→peer unicast of session-key material, not session
  traffic; since 2026-06-12 it carries two payload kinds: the RW
  `NamespaceSecret` and the read-capability workspace ticket
  envelope. **Ticket *revocation* DONE** (2026-06-11;
  `PROTOCOL_VERSION` 7→8, no ticket/gossip wire change). The host
  records every mint in a per-session issued-ticket **ledger**
  (`tickets.json` sidecar, full-rewrite idiom, 0600);
  `Request::RevokeTicket { session, ticket_id }` flips an entry,
  `Request::ListTickets` returns metadata + `used_by` (never the
  bearer string), and `IssuedTicket`/`HostSession` responses carry
  the `ticket_id`. Admission is **issued-only, fail closed**:
  `ensure_member` requires the claim's id to be present-and-Active
  in the ledger and cross-checks cap/expiry — absence, revocation,
  and mismatch all collapse to the joiner-opaque `InvalidTicket`
  (no ledger oracle; the check runs after expiry + cap-sig).
  Revocation is ticket-only: already-admitted peers keep membership
  (use a capability revoke; `used_by` names them). One residual
  carried from the brainstorm: a revoked-ticket joiner gets no NAK
  (same silent-timeout UX as expiry) — accepted for v1. CLI
  `ticket list`/`ticket revoke` subcommands deferred.
  **Gossip-lurker capability leak CLOSED** (2026-06-12;
  `PROTOCOL_VERSION` 8→9, upgrade ALPN `/1`→`/2`): the
  demo-proven hole where a revoked/expired-ticket bearer could
  subscribe to the (unauthenticated) session topic, have
  `run_host_replay` serve it the backlog, and import the broadcast
  read-capability `WorkspaceTicketEnvelope` into a live file
  replica. Fix: nothing capability-bearing rides the gossip topic
  any more. The host workspace publishes the envelope ONCE over IPC
  (`Request::PublishWorkspaceTicket`); the daemon persists it
  (`workspace-ticket.bin` sidecar, 0600) and delivers host→peer
  over the direct-stream channel (the sanctioned gossip-only
  exception; `DeliveryFrame` enum now carries both the RW
  `NamespaceSecret` and the workspace ticket, 64 KiB cap). The
  joiner daemon persists the envelope in its mirror and injects a
  synthetic `TICKET_ACTION` System message live + on every
  `Subscribe` (late attach / joiner restart work by construction).
  `run_host_replay` is membership-gated, with admission-triggered
  replay closing the gate-vs-admission race; log-borne
  `TICKET_ACTION` broadcasts are suppressed on every joiner-visible
  surface (legacy/forged broadcast = inert). Regression-pinned by
  `artel-fs/tests/revoked_lurker.rs` (revoked + expired lurkers end
  with no file content and no doc replica). Carried residuals:
  replay traffic is still topic-visible to lurkers (capability-free
  chatter; true privacy = topic-key rotation, § Future); the
  DocsGate/PeerFilter deny-list → allow-list flip remains a
  follow-up (requires resequencing the joiner's NODE_ID announce
  ahead of initial sync); and **the membership-gated `Replay` arm
  keys on `delivered_from`**, which iroh-gossip defines as the relay
  hop, not the frame origin — the same dependence auth Slice B.5
  deliberately avoided for signed frames (see that entry below). The
  gate is sound under v1's star topology (the host hears every
  joiner directly) and is defense-in-depth only — the capability
  leak it backstops is already closed by moving the envelope off
  gossip onto host→peer unicast. But `GossipBody::Replay` carries no
  signed identity, so in a multi-hop mesh a lurker's `Replay`
  relayed through a member would pass the gate and the host would
  re-broadcast the backlog (capability-free, but more than the
  "lurkers see only live chatter" guarantee intends). **MUST become
  a signed, B.5-style requester identity (or move the gate into
  `log_since`/`run_host_replay` keyed on a verified peer) as part of
  the P2P causal-DAG rethink** — see the L2 host-only enforcement
  note (`l2-host-only-enforcement-v1`); the same topology assumption
  is what that rethink retires.
  NOTE (memory `reopen-grant-authority-on-readonly-
  tickets`): now that sub-RW members exist, the L2 "any RW holder
  grants" rule is load-bearing — revisit grant authority if a tier
  between Read and ReadWrite is ever added.
  Distinct from per-path read-only **rules**, which shipped earlier
  as `PathRules { Mode::ReadOnly }` on `WorkspaceConfig` (host binds
  rules at originate-time; rules ride the ticket envelope; watcher /
  applier / scan / bulk-export all honour them).
- **~~Peer-identity authentication.~~** L1 DONE 2026-05-30
  (`PROTOCOL_VERSION` 4). `artel-protocol::PeerId` is now defined as
  the iroh `EndpointId` bytes; host-side `SendRequest` and
  `JoinAnnouncement` arms reject body / `delivered_from` mismatches,
  joiner-side outbound paths stamp the daemon's authenticated id,
  and the synthetic-id construction site (`--peer-id` flag,
  `derive_default_peer_id`, `FALLBACK_PEER`) is gone. See
  `docs/brainstorms/2026-05-30-auth-story-brainstorm.md` for the
  full v1 auth story (L1 collapse + L2 capability events + L3
  per-message signing); the brainstorm supersedes the
  open-design-questions section of
  [`docs/roadmap/peer-identity-authentication.md`](roadmap/peer-identity-authentication.md).
  **L3 (per-message signing) DONE 2026-06-02** (`PROTOCOL_VERSION` 5,
  `MESSAGE_FORMAT` 2): every `SessionMessage` carries an ed25519
  signature over domain-separated canonical bytes; host and joiner-
  mirror receive paths verify (`verify_strict`, version floor) and
  reject on failure. See
  `docs/plans/2026-06-02-auth-slice-b-l3-signing-plan.md`.
  **L2 (capability events, Slice C) DONE 2026-06-05**: C.1–C.3
  shipped capability grant/revoke as host-signed session events,
  host-only enforcement (host is sole sequencer; joiner-side
  enforcement deliberately deferred — see memory
  `l2-host-only-enforcement-v1`), and cap-replay on host restart.
  The docs-gate + transport-layer `PeerFilter` block revoked peers
  bidirectionally (`356e8c2`). With B.5 and tiered tickets below,
  this completed the v1 auth story.
  **Implication of host-only caps:** `Capability` grant/revoke is
  host-private (kept off the gossip wire *and* off replay), so a joiner
  is never told its capability changed — a grant is observable to it
  only incidentally (RW delivers the `NamespaceSecret` via
  `UPGRADE_ACTION`), and a **revoke is not observable at all**: the host
  simply stops accepting the peer's sync. Consequence for consumers: a
  revoked joiner gets **no error** — its local writes still succeed
  (file → `set_bytes` on its own replica both return `Ok`; there is no
  joiner-side cap check), and only cross-peer replication stops, rejected
  on the *host's* side by the docs-gate / `PeerFilter`. So revocation is
  effectively a **silent one-way partition**: the joiner keeps writing to
  itself, sees its own optimistic UI, and is never told its changes stop
  propagating. An app can react to promotion but not to its own demotion.

  Three *distinct* concerns hide behind "fix revoke", worth separating
  before anyone calls one of them "the fix":

  1. **Notification** — does the revoked peer find out? Today: no. Two
     fixes: a narrow host→peer **downgrade unicast** mirroring
     `UPGRADE_ACTION` (host-only enforcement unchanged; could land in v1),
     or the **P2P cap-propagation** rethink where joiners maintain their
     own cap-set projection and observe `Revoke` directly (the principled
     end-state; notification falls out for free, but it's the v2-scale
     symmetric-peer change — see
     `docs/brainstorms/2026-06-03-auth-slice-c-l2-capabilities-seed.md`).
  2. **Read cut-off** — stop them pulling new state. Today: works
     (`PeerFilter` rejects their connection).
  3. **~~Write cut-off~~ — stop them *producing* valid state. DONE
     2026-06-18 (Tier-1 host-centric rotation).** An `Evict` (`Revoke`)
     now rotates the namespace: the host mints a fresh `NamespaceSecret` +
     `NamespaceId`, carries the survivors' latest-per-key entries into the
     new doc, redistributes the rotated Write ticket to the remaining RW
     peers, and never gives it to the revoked peer. The revoked peer keeps
     the *old* secret, which is now worthless — its post-revoke writes land
     in the abandoned doc nobody feeds, so the "flushes wholesale on
     re-grant" bug is gone. Shipped as slices C1–C4 (durable distribution
     state, persisted `namespace_epoch`, replayed-revoke idempotency,
     never-drop rotation signal) and D1–D4 (host-first reimport,
     unresolvable-author surfacing, same-seed binding) — the C/D commit
     series on the `fix/tier1-revocation-blockers` effort.

     **RW peer offline across a rotation — DONE 2026-06-18.** A member
     that is offline when a rotation happens used to come back stuck: its
     persisted secret + replayed workspace ticket are both for the
     abandoned namespace, and the live-only re-delivery it would have
     received was lost. Now, on the returning peer's `NODE_ID` re-announce,
     the host re-delivers **both** the current secret *and* the current
     rotated ticket; a reloaded joiner daemon also lazily re-subscribes its
     gossip topic on its first post-restart send. This subsumes the
     `emit_upgrade` INVARIANT's "offline promotion" case (a peer promoted
     while offline). See `docs/plans/2026-06-18-rw-redelivery.md` and the
     real-n0 regressions in `crates/artel-fs/tests/rw_redelivery.rs`.

     What remains genuinely deferred here is **P2P** write-revocation (no
     host to drive a rotation) — that needs per-author authorization at
     project-at-merge (Tier 2), below.

  Whether to "fix" notification at all is partly threat-model dependent:
  for an **adversarial** kick, staying silent is arguably correct (don't
  tell an attacker they've been cut so they switch tactics); for a
  **cooperative** downgrade (RW → read-only collaborator), silence is just
  bad UX. v1 conflates both into one `Revoke`; distinguishing them is its
  own design question.

  **What namespace-secret rotation cost (the write cut-off fix — now
  shipped, retained as the rationale for how it was built).** Write
  capability *is* possession of the `NamespaceSecret` (the
  iroh-docs document write key, one symmetric secret shared by all RW
  peers). The only way to make a revoked peer's secret worthless is to
  rotate the namespace — mint a new secret, give it to survivors, never
  to the revoked peer. A new secret means a new `NamespaceId` means a
  *different iroh-docs document* — which sounds catastrophic for a
  long-lived session with large shared files, but **is not**, because
  storage is content-addressed:

  - **iroh-blobs (`FsStore`, one per node)** holds the file bytes, keyed
    by hash, *namespace-agnostic*. The **iroh-docs document** is only a
    `path → content-hash` mapping (metadata, not data). The secret/ID
    govern the *mapping*, not the blobs.
  - "Migrating" to the rotated namespace re-points keys at hashes that
    already exist locally — copying a list of `(path, 32-byte hash)`
    pairs (kilobytes for thousands of files), **not** file contents. A
    4 GB file is one 32-byte hash in both the old and new doc; its bytes
    never move or re-transfer.
  - Survivors already hold the blobs, so importing the rotated namespace
    reconciles only the *entry set* — for every unchanged file the hash
    is already present, nothing downloads. Cheap on the wire too.
  - The revoked peer's post-revoke writes land in the **old, abandoned**
    doc nobody feeds — so the "flushes wholesale on re-grant" bug above
    fixes itself once rotation exists.

  So rotation's real cost is **not** the data. It's:

  1. **Decouple `SessionId` from `NamespaceId`.** Today `SessionId =
     session_id_for(NamespaceId)` (load-bearing for re-host-across-restart
     resume). Rotation changes the namespace, so the session id must be
     minted+persisted once and the namespace become a *mutable attribute*
     of the session. Touches `session_id.rs`, host resume, daemon record.
  2. **A `namespace_epoch` in the ticket envelope** so joiners re-import
     on a bump.
  3. **The freeze-drain-snapshot barrier** — the genuinely hard slice:
     freeze writes, drain in-flight ones, snapshot the entry set
     atomically, or a survivor's concurrent write lands in the old doc
     and is lost. Correctness-critical; a concurrency problem, not a
     bytes problem.
  4. **Downgrade notification** (slice from the notification concern
     above) — independently shippable, cheap, and the thing that makes a
     revoked peer's app able to react.

  **Alpha note (no backward compat):** this lets us change the SessionId
  derivation, envelope shape, and on-disk `doc-id` layout outright (bump
  `PROTOCOL_VERSION`, hard-reject old shapes) — deletes the dual-format
  reads and persisted-session migration, ~⅓ of the fiddly work. It does
  **not** reduce the identity decoupling or the quiescence barrier; those
  are intrinsic.

  **Strategic fork — resolved: Tier 1 shipped (2026-06).** This was
  originally held pending a forcing function, with the caveat that
  host-centric rotation (Tier 1) is **partially throwaway** if the
  symmetric-P2P rethink lands: per-author authorization that rejects
  revoked authors at *project-at-merge* (Tier 2 — see
  `l2-host-only-enforcement-v1` and the delivery-rethink brainstorm)
  supersedes namespace rotation and is the only thing that works when
  there's no single gatekeeper to drive a rotation. We went ahead and
  built Tier 1 (slices 1–3 + the downgrade notification, slice 4): the
  `SessionId`/`NamespaceId` decoupling, `namespace_epoch` in the envelope,
  and the freeze-drain-snapshot barrier are all done, plus offline-peer
  re-delivery on rejoin. Tier 2 remains the eventual end-state for the
  P2P (no-host) case and still supersedes this when it lands; until then,
  Tier-1 rotation is the working write-revocation for the host-sequencer
  model.

  **What P2P revocation *additionally* pulls in (beyond project-at-merge).**
  The host model collapses three problems into "the host said so, in
  sequence order" — drop the host and all three come back:

  1. **An authority model — *who* may revoke.** Not automatically quorum;
     it's a design axis with at least three points: a **founder/owner
     key** (whoever created the session, or a designated admin key,
     signs revocations — simplest; a *logical* authority without a
     *runtime* one), **capability delegation** (admin is itself a
     grantable cap — any current admin revokes; cf.
     `reopen-grant-authority-on-readonly-tickets`), or **k-of-n quorum**
     (valid only with a threshold of admin signatures — robust against a
     single compromised admin, most complex). Which one is a
     *threat-model* choice, not a foregone "we need quorum." Crucially
     this is **per-session policy, chosen at creation**, not one global
     decision — a solo/small-team workspace can run owner-key while an
     adversarial multi-party one runs quorum, same substrate. The model
     becomes part of the session's genesis record so every peer projects
     against the same rule.
  2. **Monotonic project-at-merge.** The peer being revoked also *writes*
     — it can keep signing entries (including ones contradicting its own
     revocation) or withhold the revocation from some peers. So a
     revocation must be a **high-water mark in the causal DAG**: once any
     peer sees a valid revocation at causal point X, no later-merged
     entry from that author past X is ever accepted, regardless of
     arrival order. Not a mutable flag.
  3. **Convergence under partition.** Two peers healing a split must
     agree on the membership set, or it forks permanently. This is the
     step that turns "design a quorum" into "design a conflict-free
     *authorization* CRDT over the content CRDT" — the real meat of the
     symmetric-P2P rethink.

  The revocation authority (1) is the visible tip; the
  causal-consistency machinery (2)+(3) is the iceberg, and it's *why*
  host-centric rotation is the pragmatic near-term answer — the
  sequencer gives you robust revoke without solving any of (1)–(3). You
  take these on only at the moment you give up the host, which is the
  same moment you'd be building project-at-merge anyway.

  **Ecosystem leverage (verified 2026-06; re-check before relying).** No
  turnkey "P2P revocation" crate exists — the three rows want different
  tools and the seam between them is ours — but there's more off-the-shelf
  than expected, and one fact decides the host-vs-P2P split:

  | Need | Off-the-shelf? |
  |---|---|
  | iroh: transport / discovery / blobs / gossip / per-author *signed* entries / *content* convergence | ✅ already have it (`AuthorId` writes; iroh-docs is the content CRDT) |
  | (1) authority / delegation tokens | **Biscuit** (`biscuit-auth` 6.0, Eclipse/Clever Cloud, prod) — attenuable Datalog caps. **UCAN** Rust crate is stale (last release 2023). **Both punt revocation *convergence* to the app** — they solve "who may revoke," not the iceberg. |
  | (2) make the secret worthless (rotation crypto) | **OpenMLS** v0.8.1, **audited** (SRLabs, 2026-05) — member removal with post-compromise security. ⚠️ **RFC 9420 §14: stock MLS assumes a delivery service *serializes* commits — it does NOT merge concurrent ones.** |
  | (3) causal ordering + membership convergence | **No stable dep.** References only. |

  Two leverage facts that map cleanly onto the fork:

  - **OpenMLS needs a sequencer → it fits Tier 1.** The host *is* the
    delivery service MLS wants, so audited off-the-shelf member-removal
    crypto drops straight into host-centric rotation — strongly prefer it
    over hand-rolled secret rotation. In sequencerless P2P stock MLS
    forks; you'd need the research variants **DMLS** (Phoenix R&D — keeps
    MLS wire format, epoch space forks into a tree, puncturable-PRF for
    forward secrecy) or **DCGKA** (Weidner/Kleppmann, CCS'21 — *not* MLS;
    authenticated causal-order broadcast, concurrency-native, loses
    TreeKEM's O(log n)).
  - **Ink & Switch Keyhive is the closest Tier-2 reference** (`keyhive_core`
    0.4.1, 2026-06, Apache-2.0, **pre-alpha/unaudited** — evaluate, don't
    depend). Its architecture validates rows (1)–(3) almost verbatim:
    convergent capabilities + a hash-linked membership-op DAG (a CRDT,
    RIBLT-reconciled) + **BeeKEM**, a TreeKEM-derived group-key agreement
    that **needs only *causal* order, not total order** — i.e. exactly
    what an iroh-gossip/causal-DAG substrate can provide, unlike MLS.
  - **Matrix state-res v2** is the battle-tested prior art for (3) —
    *deterministic convergence + auth-filtering that drops unauthorized
    events* (NOT a CRDT; that filtering is what lets fork-and-merge
    re-enforce a ban). Reference impl `ruma-state-res`. Learn from it,
    don't depend; its buried body is **state resets** (recurring, and a
    CVE — CVE-2025-49090, only partially fixed in v2.1 / Room v12).

  **Frontier note:** *nobody* has shipped the malicious **mutual-revocation**
  case (two admins concurrently revoke each other) — Keyhive explicitly
  defers it, Matrix needed a CVE fix and got it "mostly." If we ever need
  that guarantee, we're at the research frontier, not integrating a
  library.
- **Control-frame & sequence authentication (auth Slice B.5).** DONE
  (2026-06-03; `PROTOCOL_VERSION` 5→6, `MESSAGE_FORMAT` 2→3, `Meta`
  2→3). A code review of the L3 landing surfaced three issues that
  needed a gossip-wire change and so were carved out of B: (#1) message
  **replay** — `seq` is outside the signed scope and the joiner dedups
  by seq, so a validly-signed body could be re-appended under a fresh
  seq; (#2) **forged `SessionClosed`** — the unauthenticated joiner arm
  let any topic member evict every other joiner's mirror (with on-disk
  attachment cascade); (#3) **forged `SendAck`** — the unauthenticated
  ack arm let a racing peer spoof a send result to the joiner's IPC
  client. Closed with one mechanism: **the host signs everything it
  originates or sequences; joiners verify host-origin against the host
  pubkey from the join ticket** (`session.host` = `host_peer_id`),
  topology-independent (does not depend on `delivered_from`, which
  iroh-gossip defines as the relay hop, not the origin). `host_sig` on
  `SessionMessage` (seq-sig, verified after dedup), on `SendAck`
  (result bound), and on `SessionClosed` (over a per-incarnation
  `host_epoch` distributed via a signed `EpochBeacon` — the only frame
  that advances the joiner's watermark). One documented residual: a
  lost resume beacon racing a replayed old close (effect = a re-joinable
  mirror delete). See
  `docs/plans/2026-06-03-auth-slice-b5-control-frame-auth-plan.md` and
  `docs/brainstorms/2026-06-02-control-frame-auth-slice-b5-brainstorm.md`.
  (L2 / Slice C subsequently landed — see the entry above.)
- **N-1 protocol-version compatibility.** Today version mismatch is
  fatal. Some scheme that lets a daemon serve clients one version
  behind would smooth upgrades.
- **Wire versioning for gossip frames.** DONE (2026-06-03, folded into
  auth Slice B.5's cutover). `encode`/`decode` now stamp a leading
  `GOSSIP_WIRE_VERSION: u8` byte (`[version][postcard(body)]`); `decode`
  rejects an unknown leading byte with
  `GossipFrameError::UnsupportedVersion { found, expected }` rather than
  mis-decoding postcard bytes into the wrong variant. A full
  capability-negotiation story is still future work, but a mixed-version
  mesh now fails cleanly instead of swallowing garbage.
- **Observability.** Structured metrics endpoint, `collab-daemon list`
  → `artel sessions inspect <id>` deeper view.
- **Faster `cargo test --workspace`.** DONE. cargo-nextest +
  by-subsystem consolidation per
  `docs/plans/2026-05-29-faster-cargo-test-plan.md` (commit
  `6d22e61`). ~50 one-test-per-file integration bins collapsed to
  ~13 by-subsystem files; tiered pyramid (Tier A unit + hermetic
  Tier B `DnsPkarrServer` / `TestingUnreachableRelay` + serial
  Tier C real-n0) wired through `.config/nextest.toml` + `Makefile`
  + CI. `cargo test --workspace` still works as a fallback.
  n0-touching tests are now suffixed `*_n0` and run under
  `--profile n0`.
- **Symmetric P2P.** ADR-001 § "Future evolution" — drop the
  host-as-sequencer model. Big rethink, not a v2 deliverable.
- **WASM / non-Rust clients.** ADR-001 § "Non-Rust clients become
  possible." Architectural door open; work not scoped.

## Engineering principles, distilled

For a fresh agent picking this up:

1. **Two impls from day one or none.** Don't introduce a trait with
   one implementation. See `feedback_no_speculative_abstractions` in
   memory.
2. **Ship persistence-first paths.** Every mutation persists to disk
   before in-memory state is updated, before fan-out. If the disk
   write fails, the registry stays consistent and the client gets a
   clear `Storage` error.
3. **Tests for every mutation.** Pattern is: store unit tests +
   registry-via-MemoryStore unit tests + e2e tests via real Client.
   No code lands without all three.
4. **Postcard wire enums must be externally-tagged.** Postcard rejects
   `#[serde(tag, content)]`. See memory.
5. **Unix-only for now.** Don't write Windows-aware code; the project
   gates the whole socket layer behind `#[cfg(unix)]` and emits a
   `compile_error!` elsewhere.
6. **Headless first-class.** The daemon must run cleanly under
   systemd/launchd/nohup. CLI output is structured and `--json`-aware
   where it makes sense.
7. **Don't add abstractions beyond what the task requires.** A bug fix
   doesn't need surrounding cleanup. A one-shot operation doesn't need
   a helper.
