# artel

A peer-to-peer collaborative substrate for Rust applications.

`artel` is a long-running local daemon plus a Rust client crate that gives apps:

- discoverable, NAT-traversed peer-to-peer messaging (built on [iroh](https://iroh.computer))
- persistent session state that outlives any individual app process
- an opt-in filesystem-sync workspace
- a small RPC surface so apps don't have to know how any of the above works

The substrate is consumer-agnostic. Any app — an AI harness, a shared-doc editor, a multi-agent orchestrator — can build on it without the substrate knowing they exist.

## Status

Pre-alpha. The design lives at [`docs/adr/001-collab-substrate-platform.md`](docs/adr/001-collab-substrate-platform.md). The forward-looking plan is in [`docs/roadmap.md`](docs/roadmap.md).

Local-only IPC, persistence, daemon, and client are working today. iroh integration and `artel-fs` are the next two phases.

## Crates

| Crate | Purpose |
|---|---|
| `artel-protocol` | Wire protocol types shared by daemon and client |
| `artel-daemon` | Long-running local process that owns iroh node(s) and persists session state |
| `artel-client` | Rust client apps depend on; wraps the IPC |
| `artel-fs` | Optional filesystem-sync workspace built on top of a session |

Other workspace types (CRDT docs, KV stores, etc.) can be implemented as sibling crates following the same convention.

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
