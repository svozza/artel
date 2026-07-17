# artel-fs

Filesystem workspace sync built on top of artel sessions.

Mirrors a directory across peers. Runs in your process and spawns its own small
iroh endpoint; the host hands joiners a doc ticket over the session (the
"ticket-handout" model — the daemon stays file-sync-agnostic). Provides the
watcher/applier, configurable exclude + size filtering (hidden files are
excluded by default, `WorkspaceConfig::exclude` overrides; files stream in
both directions, with `WorkspaceConfig::max_file_size` — default 64 MiB,
`None` = unlimited — as an accident-guard), and `PathRules` to scope which
paths sync and at what capability.

```rust
use artel_fs::{Workspace, WorkspaceConfig, AttachPolicy};

let (workspace, mut events) = Workspace::host_with(
    &client, "alice", root, AttachPolicy::default(), WorkspaceConfig::default(),
).await?;
```

See the [workspace README](../../README.md) and the
[consumer guide](../../docs/consumer-guide.md), especially the "chat as files"
pattern and the read-only flush trap.

<!-- TODO(pre-crates.io): expand into a standalone crate page before publishing.
     Needs to stand alone on crates.io: a full host/join example, the
     `PathRules` / `WorkspaceConfig` / `AttachPolicy` reference, the
     `WorkspaceEvent` variants, and the feature flags (e.g. test-utils). -->
