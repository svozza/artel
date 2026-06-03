# ADR-001: Collaborative Substrate as a Platform

**Status**: Accepted
**Date**: 2026-05-15
**Accepted**: 2026-05-17
**Updated**: 2026-05-17 (state dir renamed `~/.collab/` → `~/.artel/`; Windows support deferred to v2); 2026-05-27 (stable session id across host restarts — see "Updates" below); 2026-05-28 (workspace registry — opaque per-session attachments, see "Updates" below); 2026-05-30 (L1 peer-id authentication via collapse — see "Updates" below); 2026-06-01 (L1 IPC-side closure: peer.id stripped from Host/JoinSession — see "Updates" below); 2026-06-03 (control-frame & sequence authentication, auth Slice B.5 — see "Updates" below)

> Originally drafted as ADR-012 in the [`leandrodamascena/harness`](https://github.com/leandrodamascena/harness) repository ([PR #8](https://github.com/leandrodamascena/harness/pull/8)). Adopted as ADR-001 here as the founding design document for `artel`.

## Context

ADR-011 introduced collaborative sessions for harness: peers connect over iroh, share a chat, and (optionally) sync a workspace. The implementation lives inside the harness binary and is shaped to harness's needs — a chat log, a filesystem-backed workspace doc, a TUI to render both.

While building this, it became clear that the collaborative substrate is more general than harness. The combination of "discoverable, persistent, NAT-traversed peer messaging plus optional shared state" is a useful primitive in its own right. A shared-doc app, a multi-agent orchestrator, a remote pair-programming tool, and harness itself are all variations on the same theme. Today, none of them can be built without re-implementing what harness already has — or forking harness and ripping the AI parts out.

A second pressure: sessions today live and die with the host process. Closing the harness TUI ends the session, kills any in-flight agent work, and disconnects every peer. Long-running agents, async multi-agent pipelines, headless watcher agents, and reconnect-from-another-device workflows are all blocked by this. Persistence is a requirement, not a nice-to-have, for the workflows we want to enable next.

This ADR proposes extracting the substrate into a standalone platform — a daemon plus client crate — so that harness becomes one consumer of the platform among potentially many, and so that sessions can outlive any single app process.

## Challenges

- **Persistence model.** A library can persist *state* to disk, but cannot keep an iroh node *online* between app runs. Reconnect-from-anywhere agents and cloud subagents that outlive the requester both require something on the user's machine to hold the connection open while no app is attached.
- **Scope creep in the daemon.** A daemon that owns "everything collaborative" risks becoming a feature warehouse: chat logs, workspace sync, presence, capabilities, app-specific state. Each addition raises the IPC surface and locks in opinions that may not match a future app.
- **Trait design with one example.** Today's only workspace is filesystem sync. Defining a `Workspace` trait now would bake filesystem assumptions into a contract meant to be polymorphic. But shipping nothing reusable means each future app reinvents file watching, echo guards, gitignore filtering.
- **Adoption friction.** Asking new users to install a service before they can run `harness --host` is a real adoption tax. The substrate must feel as "single-binary just works" as today's harness, even though it gains a daemon.
- **Cross-platform IPC.** Unix domain sockets work on Mac and Linux; Windows needs named pipes. v1 ships Unix-only (Linux + macOS); Windows named-pipe support is deferred until a real user requests it (see § "Open questions").
- **Version skew.** Two installed apps may depend on different `collab-client` versions, and the daemon binary may be newer or older than either. Silent breakage is not acceptable.

## Decision

### Three-crate split

The substrate is delivered as three Rust crates:

1. **`collab-daemon`** — a long-running local process that owns iroh node(s) and persists session state.
2. **`collab-client`** — a Rust crate that apps depend on. Wraps the IPC, exposes an idiomatic async API. Apps do not speak the wire protocol directly.
3. **`fs-workspace`** — an opt-in crate providing today's filesystem sync. Apps that want filesystem mirroring depend on this; apps that don't, don't.

Harness becomes an app that depends on `collab-client` and `fs-workspace`, plus its existing AI/chat/tool/permission code (which is unrelated to the substrate). A future shared-doc app would depend on `collab-client` and ship its own CRDT logic — not `fs-workspace`.

### Repository layout

The substrate lives in its own repository as a Cargo workspace; each consumer (including harness) lives in a separate repository.

```
collab/                    ← substrate repo (workspace)
  Cargo.toml
  crates/
    collab-daemon/
    collab-client/
    fs-workspace/
    collab-protocol/       ← shared types / wire format

harness/                   ← consumer repo
  → depends on `collab-client` + `fs-workspace` from crates.io
```

**The substrate knows nothing about its consumers.** Harness is treated as a third-party consumer, identical to any other app. The substrate does not have harness-specific code paths, harness-shaped APIs, or harness-aware tests. This is what makes it a platform rather than a shared library extracted from harness.

Concretely:

- Substrate releases on its own cadence (slower, protocol-stability-driven). Consumers pull in versions when they choose to upgrade.
- Cross-repo coordination is the consumer's responsibility. If a substrate API changes, consumers pin to the older version until they're ready, then upgrade.
- The substrate's CI tests the substrate. Each consumer's CI tests that consumer against whatever substrate version it pins.
- Local development across the boundary uses Cargo's `[patch.crates-io]` to point at a local checkout — the consumer opts in temporarily, the substrate doesn't know it happened.

**First-party consumers don't get special treatment.** A future first-party app (e.g. a shared-doc app) lives in its own repository too, not bundled into the substrate or into a consumers monorepo. Independent release cadence, focused contributor base, and focused issue tracker matter more than the convenience of cross-repo refactors.

A monorepo for first-party consumers was considered and rejected: the convenience would come at the cost of blurring the platform/consumer boundary that this ADR is specifically establishing.

### Daemon scope: medium

The daemon owns:

- iroh node(s) and the network connections they represent
- per-session state: peers, sequence numbers, ordered message log
- on-disk persistence of the above so sessions survive daemon restarts
- a small RPC surface (~5–7 verbs): `host_session`, `join_session`, `list_sessions`, `subscribe(session_id)`, `send(session_id, msg)`, `leave_session`, plus management commands

The daemon does **not** own:

- workspace sync of any kind
- app-specific message schemas (payloads are opaque bytes)
- AI/agent/tool concerns

The unit of persistence is a session. Workspace sync is one possible thing to layer on top, not something every app needs.

### One app per session

Each session is owned by one app at a time. The platform is not a shared bus where many apps live in the same session. This matches today's substrate shape, keeps the IPC surface small, and avoids inventing a routing model before there is a real second-app requirement that needs it.

### Auto-spawned daemon lifecycle

The first client connect spawns the daemon if it is not running. The daemon writes its PID and binds a Unix socket under the user's home directory:

```
~/.artel/
  daemon.sock     ← Unix socket apps connect to
  daemon.pid      ← daemon's PID
  daemon.log      ← stdout/stderr
  sessions/       ← per-session state, message logs
```

Stale state recovery follows standard daemon conventions: if a connect fails and the PID file points at a dead process, the client deletes the stale socket and PID file and spawns a fresh daemon.

Explicit management commands (`collab-daemon status / stop / restart`) are available for users who want them, but no install step is required for the demo path. Running `harness --host` continues to "just work" with no prerequisites beyond the binary.

The daemon may also be installed as a launchd / systemd service by users who prefer that, but this is not required.

### Versioned message envelope, opaque payload

The daemon understands today's `SessionMessage` shape so it can offer useful tooling (`collab-daemon list`, `collab-daemon tail <session>`, deduplication on reconnect, "messages since seq N" queries):

```rust
struct SessionMessage {
    version: u8,        // protocol version
    seq: u64,           // assigned by host
    timestamp: u64,
    peer: PeerInfo,
    kind: MessageKind,  // Chat, Tool, System (and future categories)
    action: String,
    payload: Vec<u8>,   // opaque bytes; app chooses serialization
}
```

The `payload` field is opaque bytes rather than today's `serde_json::Value`. Apps choose their own serialization (JSON, postcard, protobuf, raw) and the daemon never inspects it. Tooling that wants to display payloads renders them as `<N bytes>` unless the app provides a separate description channel later.

This is a small narrowing of today's ADR-011 model and preserves the path to stricter typing the existing model already anticipates.

### Version mismatch is an explicit error

The client sends its protocol version on connect. The daemon either accepts or replies with `unsupported version: client=N, daemon=M, restart required`. The client surfaces this clearly to the user. No silent breakage; no falling back to a partial protocol.

For v1, "restart the daemon" is the resolution. Side-by-side daemon versions and N-1 backwards compatibility are deferred.

### No `Workspace` trait yet

`fs-workspace` ships as a concrete crate following a *convention* — take a session handle, return a ticket plus an event stream, run in the background, support graceful shutdown — but there is no `Workspace` trait. With only one workspace type, a trait would bake filesystem assumptions into a contract meant to be polymorphic. The convention establishes the shape; the trait can emerge once a second workspace type (e.g. a CRDT doc) exists and the common API is obvious from real usage.

This is a deliberate deferral, not an oversight.

### Doc handles across IPC

`fs-workspace` (and any future workspace crate) needs to drive `iroh-docs` directly. There are two viable shapes:

- **Daemon proxies the doc API.** Clients call `set_bytes`, `subscribe`, etc. through the IPC. Larger protocol surface, single source of truth for iroh state.
- **Daemon hands out doc tickets.** Clients spin up their own (small) iroh node *just for docs*, import the ticket, and drive the doc directly. Smaller protocol, but apps end up with two iroh nodes.

The ticket-handout path is leaner and keeps the daemon ignorant of doc semantics. The cost is that workspace-using apps still bind a node. We propose ticket-handout for v1 and revisit if it causes real problems.

### Use cases this unlocks

The persistence guarantee is what makes the daemon worth its cost. Examples enabled (and impossible today):

- **Long-running agents you reconnect to.** Kick off a 4-hour refactor before lunch, close the laptop, rejoin from a cafe.
- **Headless agents with no human attached.** Watcher agents that react to file changes or peer messages with no TUI.
- **Async multi-agent pipelines.** Agent A posts a plan; Agent B (perhaps on a beefier cloud machine) wakes up later, consumes it, posts results.
- **Cloud subagents that outlive the requester.** Spawn an EC2 peer, disconnect entirely, the peer keeps working and posts results when ready. (Strengthens `docs/ideas/remote-subagents.md`.)
- **Cross-device intervention.** Cancel from your phone what your laptop started.
- **Resumable workflows.** Network blip, app crash, machine reboot — the session and message log are on disk; whoever rejoins picks up from the last sequence number.

### Architecture

```
┌─────────────────────────────┐  ┌─────────────────────────────┐
│  harness (app)              │  │  shared-doc (hypothetical)  │
│  ┌─────────────────────┐    │  │  ┌─────────────────────┐    │
│  │ TUI, chat, tools,   │    │  │  │ TUI, doc CRDT, ...  │    │
│  │ permissions, hooks  │    │  │  └─────────────────────┘    │
│  └─────────────────────┘    │  │  ┌─────────────────────┐    │
│  ┌─────────────────────┐    │  │  │   collab-client     │    │
│  │ collab-client │ fs-ws│   │  │  └─────────────────────┘    │
│  └─────────────────────┘    │  │                              │
└──────────────┬──────────────┘  └──────────────┬──────────────┘
               │                                  │
               │            IPC (Unix socket)     │
               │                                  │
               └──────────────┬───────────────────┘
                              │
                ┌─────────────▼─────────────┐
                │      collab-daemon        │
                │  ┌─────────────────────┐  │
                │  │ session log on disk │  │
                │  │ peer state          │  │
                │  └─────────────────────┘  │
                │  ┌─────────────────────┐  │
                │  │ iroh node(s)        │  │
                │  └─────────────────────┘  │
                └───────────────────────────┘
                              │
                              │  iroh (NAT-traversed P2P)
                              │
                       ┌──────┴──────┐
                       │ remote peers │
                       └──────────────┘
```

The transport, auth, and session abstractions from ADR-011 carry forward — they are now the daemon's internals rather than harness's. The harness chat loop, tools, hooks, and permissions are unchanged in shape; they just hold a `collab-client` handle instead of a `SessionHandle`.

## Open questions

- **Doc handles across IPC** (proxy vs. ticket-handout). Proposal: ticket-handout for v1, revisit if the dual-node cost bites.
- **Auth and capability model** (L1 resolved 2026-05-30). Today anyone with a ticket can read and write. The daemon makes it easier to add capabilities (read-only tickets, signed messages) but ADR-011 deferred this. Should be revisited deliberately, not bolted on. L1 (peer-id authentication via collapse into `iroh::EndpointId`) shipped 2026-05-30; L2 (capability events) and L3 (per-message signing) remain open — see `docs/brainstorms/2026-05-30-auth-story-brainstorm.md` for the full design space.
- **Multi-version daemon coexistence.** "Restart the daemon" is fine for v1, but if multiple apps from multiple installers end up disagreeing on protocol versions, side-by-side daemons or N-1 compatibility may become necessary.
- **Relationship to ADR-011 open questions.** Execution model (local / host / sandbox), workspace write model (unrestricted vs. agent-only), host migration, and cancellation are all still open and orthogonal to this ADR. Some of them get easier with a persistent daemon (host migration in particular); none are resolved by it.
- **Telemetry and observability.** A daemon makes it natural to expose introspection (`collab-daemon list`, log files, metrics endpoint). What level of observability ships in v1 vs. later is undecided.

## Consequences

- **The harness binary gains a runtime dependency on the daemon.** First-run UX still feels like a single binary because the client auto-spawns the daemon, but advanced users (debugging, sandboxed CI environments) need to know the daemon exists.
- **Cross-platform work.** Unix socket on Mac/Linux. Windows named-pipe support is deferred (the wire types in `artel-protocol` are platform-agnostic; only the socket layer needs to grow). Crates like `interprocess` cover this if/when it becomes a v2 deliverable.
- **The substrate becomes versioned independently of harness.** `collab-client` releases, `collab-daemon` releases, and harness releases are now three coordinated streams. Version negotiation between them must be explicit.
- **Sessions can outlive apps.** This is the entire point but also a new failure mode: orphaned sessions that nobody is paying attention to. `collab-daemon list` and a TTL/cleanup story will be needed.
- **`fs-workspace` is reusable but not yet polymorphic.** Apps that want filesystem sync get it for free. Apps that want a different workspace type write their own crate. There is no trait to swap implementations through — this is a deliberate v1 simplification.
- **The opaque-payload move slightly narrows ADR-011's loose-message-model intent.** Apps still get full flexibility within their payload, but cross-app introspection of payloads requires per-app tooling. We accept this trade — apps don't share sessions today.
- **No backwards compatibility with today's in-process collab code.** The substrate is a clean break. The project is in alpha, so a major refactor of `SessionHandle`, `WorkspaceSync`, and the chat-loop integration is acceptable — and probably desirable — over staged migration. ADR-011 sessions started before the cutover cannot be resumed by daemon-era clients.
- **Several future improvements become straightforward.** Host migration (the daemon already holds the message log), cloud subagents (just another peer that talks to a daemon), cross-device sessions (any device with the ticket can join the daemon), and cancellation semantics (daemon mediates) all fit naturally.
- **Non-Rust clients become possible (but are not a v1 deliverable).** Because apps speak to the daemon over a local socket, the wire protocol is language-agnostic by definition. Two viable routes for getting clients into other languages:
  - *Per-language native clients.* Hand-write a client library in each target language (Python, TypeScript, Go, etc.) that opens the socket and speaks the protocol directly. Native types, native async, native packaging — at the cost of N libraries to keep in sync as the protocol evolves.
  - *WASM client.* Compile `collab-client` (or a stripped core) to WASM once, and have host languages call into it via a WASM runtime. One canonical client kept in lockstep with the daemon automatically. The streaming/subscription APIs are awkward across the WASM↔host boundary today, but the WIT / component-model direction (`wit-bindgen`) is specifically designed to make this idiomatic and could be revisited as the ecosystem matures.

  Either route requires committing to a stable, documented wire protocol, which is itself a non-trivial deliverable. v1 only ships `collab-client` for Rust. The architectural door is open; the work is not scoped.
- **Decisions intentionally deferred:** `Workspace` trait, doc-API proxying, auth/capabilities beyond tickets, multi-version daemon coexistence, observability surface. These are listed as open questions, not undefined behavior.

## Future evolution

The daemon is a prerequisite for several directions that are out of scope for this ADR but worth naming so the v1 design is understood as enabling, not foreclosing, them.

### Symmetric peer-to-peer (no designated host)

Today's session model has a host that assigns sequence numbers and a workspace doc the host originates. Once every peer runs a daemon, every peer is *always online* with a stable iroh identity and local storage for the session log — which is exactly what's needed to drop the host role entirely.

The hard parts that remain are not infrastructural but design:

- **Ordering without a sequencer.** CRDTs (good for state, awkward for human-readable chat timelines), Lamport / vector clocks with deterministic tiebreak (partial order with consistent display), or an event-DAG model (Matrix-style). Each has real UX implications.
- **Discovery and join.** Today the host generates the ticket. Symmetric P2P needs gossip, DHT, or invite-tree models for finding the session.
- **Trust and sandboxing.** A symmetric mesh where any peer's agent can trigger tool calls makes sandboxed execution (ADR-011's libkrun direction) load-bearing rather than optional.
- **Session lifecycle.** No host means no obvious "session ended" signal. TTLs or explicit closure replace host-disconnect.

### Capability-discoverable agent mesh

Symmetric P2P unlocks a different model: peers don't just exchange messages, they advertise *capabilities* ("this peer has a beefy machine and can run the test suite", "this peer has access to that codebase", "this peer is good at Rust refactors"). Other peers query the mesh — "I need an agent that can run cargo build, who's free?" — and the mesh routes the request.

Agents become a discoverable, swappable resource pool rather than per-task spawned subprocesses. This composes naturally with the cloud-subagent direction in `docs/ideas/remote-subagents.md`: a cloud peer is just a daemon advertising different capabilities than a laptop peer.

The v1 daemon does not implement any of this, but the architecture is compatible with it: capabilities would be a new message kind (or a small dedicated channel), discovery would be a routing layer above the existing transport, and the daemon's always-on identity gives the mesh the stable participants it needs.

### Why this is in "future evolution" and not the proposal

These directions all require a designed-for-symmetry session model, not just a daemon. Committing to them in ADR-012 would balloon the scope and prematurely freeze decisions (ordering model, discovery model, capability schema) that need their own analysis. The v1 daemon design deliberately keeps the host-as-sequencer model from ADR-011 because it works and because changing it is a separate decision.

What ADR-012 *does* commit to is not foreclosing these directions: per-user daemons, persisted message logs, and opaque payloads all carry forward unchanged into a symmetric P2P world.

## Updates

### 2026-05-27: Stable session id across host restarts (PROTOCOL_VERSION 2)

`Request::HostSession` now carries an optional caller-supplied `Option<SessionId>`. `None` preserves today's mint-a-fresh-id behaviour; `Some(id)` resumes the existing local-host record (members, log, head) when a matching entry exists, or creates one at the supplied id otherwise. A `Some(id)` against an existing record with a different host or `kind: Remote` is rejected with the new `ProtocolError::SessionConflict(SessionId)` variant. `PROTOCOL_VERSION` ticks 1 → 2; the v1↔v2 boundary still surfaces as the existing `VersionMismatch` error so old clients see "restart required" cleanly. `artel-fs::Workspace::host_with` is the first consumer: it derives the id deterministically from the local `iroh-docs` `NamespaceId`, so a re-host of the same workspace dir under a fresh daemon recovers the same session id (and gossip topic), keeping existing joiners alive across the host's daemon restart. The verb count in § "Daemon scope: medium" is unchanged. See `docs/brainstorms/2026-05-26-stable-session-id-brainstorm.md` and `docs/plans/2026-05-26-stable-session-id-plan.md`.

### 2026-05-28: Workspace registry — opaque per-session attachments (PROTOCOL_VERSION 3)

Three new RPC verbs let consumers attach a small typed record against a session and enumerate them later: `Request::RegisterAttachment { session, kind, payload }` / `Request::ListAttachments { kind: Option<String> }` / `Request::ForgetAttachment { session, kind }`, with a matching `Response::Attachments { entries: Vec<Attachment> }`. The daemon never inspects `payload` and uses `kind` only as an indexing tag — same opaque-byte / consumer-namespaced shape `SessionMessage`'s `kind` + `action` + `payload` already established for in-session messaging, applied here to per-session metadata that survives the session lifecycle. `artel-fs` is the first consumer: it defines `WorkspaceAttachmentV1` (postcard, schema frozen for `KIND_V1 = "artel-fs/workspace/v1"`), registers on `Workspace::host_with` / `join_with`, and surfaces `list_known_workspaces` as a typed read helper. `PROTOCOL_VERSION` ticks 2 → 3; the verb count in § "Daemon scope: medium" is unchanged because the verbs are *attachment*-shaped (a generic primitive of the daemon's vocabulary), not workspace-shaped (which would have leaked `artel-fs`'s schema into the substrate). See `docs/brainstorms/2026-05-27-workspace-registry-brainstorm.md` and `docs/plans/2026-05-27-workspace-registry-plan.md`.

### 2026-05-30: L1 peer-id authentication (PROTOCOL_VERSION 4)

`PeerId` and iroh `EndpointId` are now one namespace — `artel-protocol::PeerId` is documented as 32 bytes that ARE an iroh `EndpointId` (an Ed25519 public key). Host-side gossip-frame handlers (`SendRequest`, `JoinAnnouncement`) reject frames whose body `peer.id` doesn't match the gossip-authenticated `delivered_from`, eliminating the spoofed-authorship / ghost-membership bug class structurally. Joiner-side outbound paths stamp the daemon's authenticated id into outbound `PeerInfo` so the host's check is meaningful. Synthetic / `--peer-id`-supplied identities and the `derive_default_peer_id` PID-mixing helper are removed; without the iroh feature the daemon advertises a documented all-zero non-routable `SYNTHETIC_LOCAL_PEER_ID`. `PROTOCOL_VERSION` ticks 3 → 4. § Open questions § Auth and capability model is now L2 + L3 territory; L1 is closed. See `docs/brainstorms/2026-05-30-auth-story-brainstorm.md` and `docs/plans/2026-05-30-auth-l1-peer-id-collapse-plan.md`.

### 2026-06-01: L1 IPC-side closure (PROTOCOL_VERSION 5)

`Request::HostSession` and `Request::JoinSession` no longer carry a `peer: PeerInfo` field. The daemon stamps its own authenticated `PeerId` (= iroh `EndpointId`) server-side; the IPC caller passes only a `display_name: String` advisory label. Closes the IPC-side complement of the 2026-05-30 gossip-side enforcement: under the previous shape, a lying IPC client made the local daemon disagree with the remote on the joiner's id (the bridge stamped the authenticated id on the wire while `Registry::join` recorded the IPC-supplied one verbatim); under this shape the field doesn't exist, so the disagreement is structurally unrepresentable. `Registry::join` becomes idempotent on self-rejoin — a second `JoinSession` from the same daemon (same authenticated id) returns `Ok` with the existing head and emits no second `PeerJoined`, so consumer remounts (e.g. `Workspace::shutdown` followed by a re-host or re-join against the persisted member set) are no-ops rather than errors. `SessionError::AlreadyJoined` and `ProtocolError::AlreadyJoined` are removed. `Workspace::host`/`host_with` narrow from `peer: PeerInfo` to `display_name: impl Into<String>`. `PROTOCOL_VERSION` ticks 4 → 5. See `docs/brainstorms/2026-06-01-auth-l1-fix3-shape.md` and `docs/plans/2026-06-01-auth-l1-fix3-plan.md`.

### 2026-06-03: Control-frame & sequence authentication (auth Slice B.5; PROTOCOL_VERSION 6, MESSAGE_FORMAT 3, Meta 3)

The host now signs every frame it originates or sequences, and joiners verify host-origin against the host pubkey they already persist as `session.host` (= the ticket's `host_peer_id`). This is origin-authentication **by signature, not by relayer** — topology-independent, so it survives the move from today's star topology to multi-hop mesh / symmetric P2P where `delivered_from`-based checks silently break. One mechanism closes three findings:

- **Replay-under-new-seq (#1).** `SessionMessage` gains a persisted `host_sig: SigBytes` — the host's signature over `"artel/seq-v1" || session_id || seq || author_sig`. The joiner verifies it **after** dedup (dedup → author sig → host seq-sig), so a genuine frame replayed on a fresh seq fails (the captured `host_sig` is bound to the original seq). Persisting it makes each log entry self-authenticating per its sequencer — a backfill served by a non-sequencer peer still verifies. `MESSAGE_FORMAT` ticks 2 → 3.
- **Forged `SendAck` (#3).** `SendAck` gains `host_sig` over `"artel/ack-v1" || session_id || req_id || result` — `result` is in the signed scope so an `Ok`/`Err` flip fails. The joiner verifies before resolving its in-flight oneshot; a forged ack does not resolve (the send times out rather than surfacing a spoofed result). `req_id` v4 freshness self-limits replayed genuine acks.
- **Forged / replayed `SessionClosed` (#2).** `SessionClosed` gains `host_sig` over `"artel/ctrl-v1" || session_id || host_epoch`. `host_epoch` is a per-host-incarnation counter (new `SessionRecord.host_epoch`, `Meta` v2 → 3): 0 on a fresh create, bumped on each host resume. It is the freshness element — the iroh endpoint secret is stable across restart, so a host signature alone can't distinguish incarnation N from N+1. Distributed via a dedicated **signed** `EpochBeacon { host_epoch, host_sig }` (the same `"artel/ctrl-v1"` bytes, so one verifier serves both frames), broadcast best-effort on every host resume. **The beacon is the only frame that advances a joiner's `host_epoch` watermark, and only on a host-signed value** — so an attacker can't forge a high epoch and a replayed old beacon can't lower a monotonic watermark. A `SessionClosed` is accepted iff `verify_ctrl` passes AND `host_epoch >= watermark`. (An earlier draft distributed the epoch as an *unsigned advisory field* on `Message`/`SendAck`; that let a replayed genuine Message poison the watermark and permanently suppress real closes — rejected. The watermark write is confined to the `EpochBeacon` arm; `replayed_message_cannot_poison_watermark` guards the regression.)

Folded in: the gossip wire gains a leading version byte (`[GOSSIP_WIRE_VERSION: u8][postcard(body)]`); `decode` rejects an unknown byte with `GossipFrameError::UnsupportedVersion` rather than mis-decoding. `PROTOCOL_VERSION` ticks 5 → 6 — a hard inter-daemon cutover; backwards compat is **waived** (alpha), both daemons rebuild together, and a mixed-version mesh fails cleanly at the version byte. On-disk migration reuses Slice B's path: a pre-cutover (`Meta` v2) session directory is rejected at `load_one` and skipped-and-logged by `load_all`.

**Accepted residual (documented, not coded away):** one narrow window remains — the resume beacon broadcast is *lost* AND an attacker replays a captured epoch-N `SessionClosed` before any later beacon or activity reaches the joiner. The only durable effect of a believed close is a mirror+attachment delete a re-join reconstructs. Tightenable later by beacon retry or by piggybacking `host_epoch` into the signed `"artel/seq-v1"` scope on the next `Message`.

**Slice C overlap (flagged, not designed):** C plans an `originator_pubkey` ticket field; B.5's "host pubkey from the ticket" primitive (`session.host` = `host_peer_id`) overlaps it. B.5 reuses the existing `host_peer_id` and does not add a second ticket field; C decides whether the two are the same field and subsumes rather than duplicates. C's grant/revoke frames want host-origin auth too — `sign_ctrl`/`verify_ctrl` is directly reusable. See `docs/brainstorms/2026-06-02-control-frame-auth-slice-b5-brainstorm.md` and `docs/plans/2026-06-03-auth-slice-b5-control-frame-auth-plan.md`.
