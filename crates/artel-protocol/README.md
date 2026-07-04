# artel-protocol

Wire protocol shared between `artel-daemon` and `artel-client`.

The versioned IPC contract: the `Request` / `Response` / `Event` types, the
`Hello` version handshake, and session/ticket/capability types. This crate is
free of `iroh`; with default features it is pure wire types, so both ends and
any future non-Rust client can depend on a single source of truth for the wire
format. The Unix-socket IPC transport and postcard-based framing are opt-in
behind the `tokio` feature.

Most apps depend on `artel-client` (which re-exports what they need) rather than
this crate directly.

See the [workspace README](../../README.md) and the
[consumer guide](../../docs/consumer-guide.md).

<!-- TODO(pre-crates.io): expand into a standalone crate page before publishing.
     Needs to stand alone on crates.io: the request-verb table, the wire
     versioning / compatibility policy, and a note on the postcard
     externally-tagged-enum constraint for wire types. -->
