# Building on artel — consumer guide

This is the guide for **app authors** who want to build on artel. It covers the mental model, the API surface, the patterns that work, and the sharp edges. For the one-paragraph pitch and dependency snippets, see the [README](../README.md). For the design rationale, see [`adr/001-collab-substrate-platform.md`](adr/001-collab-substrate-platform.md).

## Mental model: a daemon, sessions, and opaque payloads

artel gives your app three things:

1. **A local daemon** that owns the iroh node and the peer connections, and **persists** a per-session message log to disk so sessions outlive your app process. You never touch iroh directly for messaging — you talk to the daemon over a Unix socket via `artel-client`.
2. **Sessions**: a host creates one and gets a **ticket**; joiners present the ticket. The daemon sequences messages (assigns a `Seq`), fans them to subscribers, and replays the log on reconnect.
3. **Opaque payloads**: the daemon never inspects your message bytes. You choose the serialization. The daemon offers ordering, persistence, replay, and membership/capability enforcement — not schema.

If your app also wants files kept in sync across peers, add `artel-fs` (below). It is *optional* and runs in your process, not the daemon.

## The client API

```rust
Client::connect(path)                  // connect to an already-running daemon
Client::connect_or_spawn(SpawnOptions) // connect, auto-spawning the daemon if absent
client.request(Request) -> Response    // one request/response round-trip
client.take_events() -> EventStream    // take the connection's single event stream
```

`take_events()` returns the stream **once** — a connection has exactly one. This matters: if you both `Subscribe` for live events *and* need a second consumer (e.g. a control-event tap while `artel-fs` owns the main stream), open a **second `Client` connection**. See "Two connections" below.

### The request verbs

The consumer-relevant subset (the full enum also carries host↔peer delivery plumbing — `DeliverUpgrade`, `DeliverDowngrade`, `DeliverRotate`, `PublishWorkspaceTicket`, `RemoveSessionMember` — that `artel-fs` drives for you; you should never need to call those directly):

| Request | Purpose |
|---|---|
| `Hello { client_version }` | Version handshake. Sent automatically by the client on connect; mismatch is an explicit error. |
| `HostSession { display_name, session }` | Create (`session: None`) or resume (`Some(id)`) a hosted session. Returns id + ticket. |
| `JoinSession { display_name, ticket }` | Join via a ticket. |
| `ListSessions` | Sessions this daemon hosts or has joined. |
| `Subscribe { session, since }` | Subscribe to events; `since: Some(seq)` replays the log from there. |
| `Send { session, payload }` | Send an opaque-payload message; daemon assigns the `Seq`. |
| `LeaveSession { session }` | Host: closes for all. Joiner: disconnects this peer. |
| `IssueTicket { session, granted_cap, expiry_ms }` | Mint an additional ticket at a capability tier. Host-only. |
| `RevokeTicket` / `ListTickets` | Manage the issued-ticket ledger. |
| `RegisterAttachment` / `ListAttachments` / `ForgetAttachment` | Bind opaque consumer state to a session (e.g. `artel-fs` stores its workspace metadata here). Daemon indexes by `kind`, never parses. |

### The event stream

`Subscribe` drives `Event`s to your `EventStream`: `Message` (a sent payload, with its `Seq`), `PeerJoined` / `PeerLeft` (membership; `PeerJoined` carries `PeerInfo { id, display_name }`, `PeerLeft` only the bare `PeerId` — capture display names at join time if you need them later), `SessionClosed`, and `Gap`. **These are daemon-authenticated** — they are the trustworthy source for who is in the session and what their capability is. Do not reconstruct membership from app payloads.

**Handle `Event::Gap`.** If your consumer falls behind, the daemon drops events rather than blocking, and sends `Gap { session }` — the connection stays open. Recover by re-`Subscribe`ing with `since: Some(last_seen_seq)`, which replays every logged message past the gap. Live-only events (`PeerJoined` / `PeerLeft` / `SessionClosed`) that fell in the gap are **not** replayed; if you track membership, reconcile it separately after a gap rather than assuming your roster is still current.

## Filesystem sync with `artel-fs`

`artel-fs` mirrors a directory across peers. It runs in your process and spawns its own small iroh endpoint (the daemon stays file-sync-agnostic — this is the ticket-handout model from ADR-001).

```rust
Workspace::host_with(&client, name, root, AttachPolicy, WorkspaceConfig)
    -> (Workspace, mpsc::Receiver<WorkspaceEvent>)
Workspace::join_with(&client, name, root, ticket, rules, ...)
    -> (Workspace, mpsc::Receiver<WorkspaceEvent>)
```

`WorkspaceEvent` includes:

- `PeerWrote { path }` / `PeerDeleted { path }` — a peer's change landed on your disk.
- `SkippedTooLarge { path, size }` — a file exceeded the size cap (either direction).
- `SkippedReadOnly { path, direction }` — a path-event was skipped by your `PathRules`. One event per skipped path-event, no coalescing — a `target/**: ReadOnly` rule with chatty editor saves will be noisy; dedupe in your app if needed.
- `Demoted` — the host cooperatively downgraded this node RW → Read; the workspace has stopped publishing local changes. Reflect it in your UI (e.g. flip a send-gate).
- `PeerRevoked { peer }` — this workspace's cap-listener applied a host-authored revoke; from this moment the transport gates block that peer. Fires on every application, including session-log replay after a reconnect or restart — treat it as idempotent state ("peer is out"), not an edge.
- `RevokedPeerBlocked { peer, direction }` — a connection involving a revoked peer was blocked at the transport layer (`Incoming`: the peer dialed us and was rejected; `Outgoing`: our own engine's dial to the stale member was refused). Advisory — the block is already enforced when it fires; useful for surfacing "a revoked peer is still knocking" in an orchestrator or UI.
- `Error(String)` — non-fatal error in the live loop; the workspace keeps running.

**React to `PeerWrote` rather than polling** if your app wants to know when synced files change — but treat the stream as **advisory, not guaranteed**: the live loops drop events rather than block when your consumer is slow (a blocked consumer would freeze replication for the whole namespace). Sync itself is unaffected — the files on disk are always right. If you need a complete picture, rescan the workspace directory; use events as the trigger, not the record.

**Shut down explicitly.** Call and `await` `workspace.shutdown()` before dropping. Drop alone leaks the workspace's QUIC/relay session for minutes — and because the iroh key is persisted, the *next* host of the same state dir comes up with the same endpoint id, gets rejected by the relay as a duplicate, and hangs waiting to come online. The symptom is baffling at a distance (post-restart writes silently stop reaching peers); the cause is a missed `shutdown` one restart earlier. A drop without `shutdown` logs a loud error on purpose.

### `PathRules`: scope what syncs

`PathRules` decide, per relative path, whether it's `ReadOnly`, `ReadWrite`, or excluded. A common pattern: pin the workspace **root read-only** and make only one subtree writable, so you can point artel at a real project directory and sync just your app's scratch area without sharing or risking the rest:

```rust
PathRules { /* root ReadOnly, ".app/**" ReadWrite */ }
```

`PathRules::read_write()` is the permissive default. Built-in filtering also skips `.git`, `target`, `node_modules`, symlinks, and files over the size cap.

## Patterns that work

### "Chat as files" — content sync instead of message-passing

A powerful pattern: don't send chat (or other append-only app content) as `Send` messages at all. Instead, **each peer appends to its own file** under the synced workspace (`.chat/<peer-id>.jsonl`), and the file-sync layer replicates it. Every peer tails everyone else's files. Benefits: the content becomes persisted, replayable workspace state for free, and you have *one* sync mechanism instead of two.

Why **per-peer** files and not one shared file: artel-fs / iroh-docs sync is **last-writer-wins per file blob** — concurrent appends to a single shared file clobber each other. One file per writer means each blob has a single writer and never conflicts; the logical "shared log" is the merge of all per-writer files.

### Stable session ids across restarts

`HostSession { session: Some(id) }` resumes a known session. Derive `id` from stable local state (as `artel-fs` does from the workspace) so a re-host after a daemon restart lands on the same session id. A `Some(id)` whose record has a different host is rejected with `SessionConflict`.

### Two connections for a control-event tap

`Subscribe` consumes a connection's single event stream. If `artel-fs`'s `join_with` already took your main client's stream but you also need to watch membership/capability events (e.g. to resolve a peer's display name, or to detect a capability upgrade), open a **second `Client` connection** dedicated to that `Subscribe`, and keep it alive — dropping it kills the stream's reader.

## Sharp edges (read before shipping)

- **The read-only flush trap.** If your app writes content via file-sync (the "chat as files" pattern), a *read-only* peer's local writes **succeed on disk but cannot sync**, then flush **wholesale** the moment it's granted write — so "rejected" doesn't mean "gone". If you want intuitive "blocked means discarded" semantics, gate writes in your app on an observed write-capability flag and never write blocked content to the synced file (a `can_write` send-gate). Note this gate is cosmetic — the *real* read-only guarantee is enforced below your app in iroh-docs.
- **Demote and evict are different operations — know which one you're doing.** The host changes a peer's capability by `Send`ing a host-signed `SendPayload { kind: MessageKind::Capability, .. }` carrying a postcard-encoded `CapabilityAction`; `artel-fs`'s cap-listener reacts on both sides. There are two ways to take write access away, with different observability by design (see [ADR-002](adr/002-no-mls-for-tier1-write-revocation.md)):
  - **Cooperative demote** (`CapabilityAction::Grant { cap: Read }`): for a trusted collaborator. The host delivers a downgrade notification; the demoted node halts its own publishing and your event stream gets `WorkspaceEvent::Demoted` — react to it (flip your send-gate). The write-stop is *voluntary*: the peer keeps its key, so this is UX, not security.
  - **Evict** (`CapabilityAction::Revoke`): adversarial removal. The host rotates the namespace, so the evicted peer's retained key becomes worthless — the cryptographic write cut-off. The evicted peer is **deliberately not notified** (don't tell an attacker it's been cut); its local writes keep succeeding and land in the old, abandoned doc, never reaching survivors. Survivors follow the rotation automatically.
- **A demoted node stays halted for the life of the process.** Re-granting write to a peer you demoted re-delivers the key, but does not restart the halted watcher — the node keeps reading peer writes and doesn't resume publishing until the workspace is restarted. If your app supports demote-then-re-grant cycles, plan a workspace restart into the re-grant path.
- **Grants replay is live-only, but the host re-delivers.** Capability messages are excluded from log replay, so a joiner granted write and then restarted comes back *read-only at first* — the host automatically re-delivers the write key when the peer rejoins or re-announces. Tolerate a brief read-only window after a restart (don't treat it as a lost grant), and subscribe your control-event tap *before* `join_with` sends its announce so you observe the re-delivery.
- **Version negotiation is fail-loud.** A client whose protocol version the daemon doesn't support gets an explicit error, not a degraded protocol. For v1 the resolution is "restart the daemon." Surface it to the user clearly.
- **iroh version coupling.** If your app *also* depends on iroh directly (or on another iroh-based crate), it must agree with the iroh major artel pins, or you'll pull two incompatible iroh trees. Prefer letting artel own the iroh dependency and not depending on iroh directly.
- **Not on crates.io.** Versions are `0.0.0`; consume by git `rev` or local path. Inter-crate deps carry both `path` and `version`, so git dependencies resolve correctly.

## Where to look next

- [`README.md`](../README.md) — pitch, crate table, dependency snippets, minimal examples.
- [`docs/adr/`](adr/) — design decisions, especially 001 (platform), 002 (revocation), 003 (daemon namespace-agnosticism).
- [`docs/roadmap.md`](roadmap.md) — what's deferred; the main consumer-relevant item is the symmetric-P2P (no-host) rethink, which will eventually change how capabilities propagate.
