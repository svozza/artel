# artel-daemon

The long-running local process that owns the iroh node(s) and persists session
state.

Holds the peer connections and the per-session, on-disk message log so sessions
survive daemon restarts and outlive any app process. It is **namespace-agnostic**:
it sequences and persists opaque-payload messages but knows nothing about
`iroh-docs` or filesystem sync — that lives in consumer-side crates like
`artel-fs`. The first client connect auto-spawns it; explicit `status` / `stop` /
`restart` management commands exist for users who want them.

Apps do not depend on this crate — they talk to the running daemon via
`artel-client`. This crate is the binary plus its library internals.

See the [workspace README](../../README.md) and the
[consumer guide](../../docs/consumer-guide.md).

<!-- TODO(pre-crates.io): expand into a standalone crate page before publishing.
     Needs to stand alone on crates.io: the daemon lifecycle (auto-spawn, stale
     PID/socket recovery), the ~/.artel on-disk layout, the management
     subcommands, and headless/systemd/launchd operation notes. -->
