# artel

A peer-to-peer collaborative substrate for Rust applications.

`artel` is a long-running local daemon plus a Rust client crate that gives apps:

- discoverable, NAT-traversed peer-to-peer messaging (built on [iroh](https://iroh.computer))
- persistent session state that outlives any individual app process
- an opt-in filesystem-sync workspace
- a small RPC surface so apps don't have to know how any of the above works

The substrate is consumer-agnostic. Any app — an AI harness, a shared-doc editor, a multi-agent orchestrator — can build on it without the substrate knowing they exist.

## Status

Alpha. The platform decision lives in [`docs/adr/001-collab-substrate-platform.md`](docs/adr/001-collab-substrate-platform.md); the forward-looking plan is in [`docs/roadmap.md`](docs/roadmap.md).

Working today: IPC + persistence + daemon + client, iroh-gossip sessions over real n0 infrastructure, `artel-fs` filesystem sync, and the v1 authorization surface (tiered tickets, capability levels, host-enforced read-only / read-write, a revocation ledger, and `PeerFilter`). Not yet published to crates.io — consume via a git or path dependency (see [Using artel in your app](#using-artel-in-your-app)).

## Crates

| Crate | Purpose |
|---|---|
| `artel-protocol` | Wire protocol types shared by daemon and client. iroh-free, transport-free. |
| `artel-daemon` | Long-running local process that owns the iroh node(s) and persists session state. Namespace-agnostic — knows nothing about `iroh-docs`. |
| `artel-client` | The crate your app depends on. Wraps the Unix-socket IPC in an idiomatic async API. |
| `artel-fs` | Optional filesystem-sync workspace built on top of a session. Spawns its own iroh endpoint + docs; shares a ticket over the session. |

Other workspace types (CRDT docs, KV stores, etc.) can be implemented as sibling crates following the same convention.

## How it fits together

```
   your app
   ┌──────────────────────────────┐
   │  artel-client   artel-fs      │   ← you depend on these
   └──────────┬───────────┬────────┘
              │ Unix sock  │ iroh-docs ticket
   ┌──────────▼───────┐    │  (handed out over the session)
   │   artel-daemon   │    │
   │  session log     │    ▼
   │  iroh node(s) ───┼──► NAT-traversed P2P to remote peers
   └──────────────────┘
```

- Your app talks to a local **daemon** over a Unix socket (`artel-client`). The daemon owns the iroh node, the peer connections, and the persisted per-session message log.
- The daemon is **namespace-agnostic**: it sequences and persists opaque-payload messages but knows nothing about file sync. Filesystem sync (`artel-fs`) runs in your process, spawns its *own* small iroh endpoint, and the host hands joiners a doc ticket over the session. This "ticket-handout" split is [ADR-001 §"Doc handles across IPC"](docs/adr/001-collab-substrate-platform.md).
- The daemon **auto-spawns** on first client connect, so a single-binary app "just works" with no install step.

## Using artel in your app

artel is not on crates.io yet. Depend on it by git or local path.

**Git dependency** (reproducible; good for CI and co-authors):

```toml
[dependencies]
artel-client = { git = "https://github.com/svozza/artel.git", rev = "<commit-sha>" }
artel-fs     = { git = "https://github.com/svozza/artel.git", rev = "<commit-sha>" }  # only if you want file sync
```

**Local path via `[patch]`** (best for developing your app and artel together in one compile loop):

```toml
[dependencies]
artel-client = "0.0.0"
artel-fs     = "0.0.0"

[patch.crates-io]
artel-client = { path = "../artel/crates/artel-client" }
artel-fs     = { path = "../artel/crates/artel-fs" }
```

### Minimal session (no file sync)

```rust
use artel_client::{Client, SpawnOptions};
use artel_protocol::{Request, Response};

// Connect to the daemon, auto-spawning it if not already running.
let client = Client::connect_or_spawn(SpawnOptions::default()).await?;

// Host a session; get back a session id and a shareable ticket.
let resp = client.request(Request::HostSession {
    display_name: "alice".into(),
    session: None, // None = mint a fresh id; Some(id) = resume a known one
}).await?;
let (session, ticket) = match resp {
    Response::HostSession { session, ticket, .. } => (session, ticket),
    other => panic!("unexpected: {other:?}"),
};

// Subscribe to the session's event stream and send an opaque-payload message.
client.request(Request::Subscribe { session, since: None }).await?;
client.request(Request::Send { session, payload }).await?; // payload: app-chosen bytes
let mut events = client.take_events().await.expect("event stream");
while let Some(event) = events.recv().await { /* render */ }
```

A joiner calls `Request::JoinSession { display_name, ticket }` with the host's ticket.

### Filesystem sync (`artel-fs`)

```rust
use artel_fs::{Workspace, WorkspaceConfig, PathRules, AttachPolicy};

// Host a workspace rooted at `root`, syncing files to joiners.
let (workspace, mut events) = Workspace::host_with(
    &client,
    "alice",
    root,                       // PathBuf
    AttachPolicy::default(),
    WorkspaceConfig {
        // Pin the root read-only and only `.chat/**` writable, for example.
        rules: Some(PathRules { /* root ReadOnly, .chat/** ReadWrite */ ..Default::default() }),
        ..Default::default()
    },
).await?;

// Drain WorkspaceEvent::{PeerWrote, PeerDeleted, SkippedTooLarge, Error, ...}.
while let Some(ev) = events.recv().await { /* react */ }
```

A joiner calls `Workspace::join_with(&client, name, root, ticket, rules, ...)`.

For the full mental model, the API verbs, the patterns that work, and the sharp edges, read the **[consumer guide](docs/consumer-guide.md)**.

### The "chat as files" pattern

A common way to build on artel is to avoid a message-passing protocol entirely and let **content sync carry your app's append-only data**. For a chat, each peer appends to its *own* file in the synced workspace (`.chat/<peer-id>.jsonl`, one JSON line per message); the `artel-fs` doc-sync layer replicates those files like any other content, and every peer tails the others'. There is no chat wire protocol — the file *is* the transport, which means the history is persisted and replayable for free. Per-peer (not one shared) files matter: doc sync is last-writer-wins per blob, so one writer per file avoids clobbering. The consumer guide covers this and the gotcha it carries (the [read-only flush trap](docs/consumer-guide.md#sharp-edges-read-before-shipping)).

### Authorization model (v1)

- A **ticket** admits a peer to a session at a **capability**: read-only or read-write. Issue extra tickets with `Request::IssueTicket { granted_cap, expiry_ms }`.
- Capabilities are **host-enforced** in v1: the host is the sole sequencer and the only party that holds the cap-set. A read-only joiner physically cannot author synced writes (it holds no `NamespaceSecret`), so read-only is enforced at the iroh-docs layer, not merely in the UI.
- **Revocation** of a *ticket* blocks future admissions. Note the v1 limitation: grant is observable to a joiner (it requires handing over a key) but live *revoke-downgrade* is not propagated to joiners — see the L2 entry in [`docs/roadmap.md`](docs/roadmap.md). Plan around this if your app needs live revoke UX.

## Development

### Tests

`artel` uses [`cargo-nextest`](https://nexte.st) for the integration test pyramid:

- **Tier A + B** (unit + cross-peer over a localhost `DnsPkarrServer` / `TestingUnreachableRelay`): `make test` or `cargo nextest run --workspace`. Fast, deterministic, runs on every PR.
- **Tier C** (real n0 — `pkarr.iroh.computer` + production relay): `make test-n0` or `cargo nextest run --workspace --profile n0`. Slower, serial within the tier (so a failing iteration's tracing log is a single coherent timeline). Test fn names suffixed `_n0`; the default profile filters them out via `not test(/_n0$/)`.

Install nextest with:

```
cargo install cargo-nextest --locked
```

If you don't want to install nextest, `make test-fallback` runs `cargo test --workspace --all-targets` instead. Slower; no inter-binary parallelism. Doctests run under `cargo test` in either runner (nextest doesn't support doctests).

For diagnosing flaky tests, see [`docs/diagnosing-flaky-tests.md`](docs/diagnosing-flaky-tests.md).

## License

Dual-licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
