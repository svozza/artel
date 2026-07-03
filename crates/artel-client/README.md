# artel-client

Rust client for connecting to the `artel-daemon`.

A thin async wrapper over the daemon's Unix-socket IPC: connect (auto-spawning
the daemon if needed), send `Request`s, and consume the session `Event` stream.
Apps build on this rather than speaking the wire protocol directly.

```rust
use artel_client::{Client, SpawnOptions};
use artel_protocol::Request;

let client = Client::connect_or_spawn(
    SpawnOptions::new(socket_path, pid_path, daemon_binary),
).await?;
let resp = client.request(Request::HostSession {
    display_name: "alice".into(),
    session: None,
}).await?;
```

See the [workspace README](../../README.md) and the
[consumer guide](../../docs/consumer-guide.md) for the full picture.

<!-- TODO(pre-crates.io): expand this into a standalone crate page before
     publishing. crates.io renders this file as the crate's landing page, so
     it needs to stand on its own — a runnable end-to-end example (host +
     join), the `Client` method list, the `SpawnOptions` knobs, and a feature
     flag table — without relying on the workspace README being nearby. -->
