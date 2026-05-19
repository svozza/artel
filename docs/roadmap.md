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

| Crate | State |
|---|---|
| `artel-protocol` | Wire types + Unix-socket transport. Done. |
| `artel-daemon` | Persistent in-memory daemon + `artel-daemon` binary. Done. |
| `artel-client` | Stateless multiplexed client + `artel` CLI binary + `connect_or_spawn`. Done. |
| `artel-fs` | Stub. |

197 tests passing. fmt + clippy clean in both feature modes (with and
without `--all-features`). CI runs ubuntu + macos on stable; workspace
`rust-version` is 1.95.

The substrate works end-to-end on a single machine with synthetic peer
ids — not yet a real P2P system. The remaining work makes it one.

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
- `DaemonConfig` gains an opaque `address_lookup:
  Option<AddressLookupOverride>` so integration tests can seed
  `MemoryLookup` for direct localhost dialing without touching the
  n0 relay infrastructure. The override is `pub`-but-uninhabited
  when the `iroh` feature is off so the struct literal stays
  feature-flag-free.
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

ADR-001 § "Doc handles across IPC" is the design discussion. Decision
deferred there: ticket-handout (clients spin up their own iroh node
just for docs and drive the doc directly) vs daemon proxies the doc
API. ADR-001 picked **ticket-handout** for v1.

### Scope

1. New crate methods/types in `artel-fs`:
   - `Workspace::open(client: &Client, session: SessionId, root: PathBuf)`
     attaches a filesystem-backed workspace to an existing session.
   - `Workspace::watch()` returns a `Stream<Item = WorkspaceEvent>`.
2. The workspace maintains an `iroh-docs` Doc per session. The daemon
   hands the workspace a Doc ticket via a new RPC (`HostWorkspace`
   returns a ticket; `JoinWorkspace` accepts one).
3. The workspace process spins up its **own** iroh node (small one,
   docs only). It imports the ticket, drives the doc directly. Two
   iroh nodes per app (daemon's main one + workspace's docs one).
4. File-watching: `notify` crate. Filesystem events → doc writes,
   doc events → filesystem writes, with echo guards so we don't loop.
5. Gitignore filtering by default; configurable.

### Tests

- Round-trip: write a file in client A, observe in client B.
- Echo guard: a doc-driven write doesn't trigger another doc write.
- Gitignore: `target/`, `.git/`, etc. are skipped.
- Persistence: closing and reopening a workspace recovers prior state.
- Crash recovery: kill the workspace mid-write, reopen, no corruption.

### Definition of done

- Two `artel-fs::Workspace` handles attached to the same session see
  each other's file edits.
- Gitignore-aware default. Configurable via `WorkspaceConfig`.
- Documented as the canonical example of "build a CRDT-app on artel."

## Phase 4 and beyond

Listed for completeness, no detailed plan yet:

- **Capabilities & auth.** Read-only tickets, signed messages,
  ticket revocation. ADR-001 § "Auth and capability model" — explicitly
  deferred.
- **N-1 protocol-version compatibility.** Today version mismatch is
  fatal. Some scheme that lets a daemon serve clients one version
  behind would smooth upgrades.
- **Observability.** Structured metrics endpoint, `collab-daemon list`
  → `artel sessions inspect <id>` deeper view.
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
