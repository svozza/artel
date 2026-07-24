---
name: iroh-connection-fixture
description: Use when writing a unit test for something that takes a real iroh::endpoint::Connection — an EndpointHooks impl (before_connect/after_handshake) or a ProtocolHandler impl (accept/shutdown) — and there's no live Connection to hand it. Triggers on "test after_handshake", "test accept()", "how do I get a Connection in a unit test", "PeerFilter test", "DocsGate test", or any hook/handler whose method signature takes Connection by value or reference. Points at the shared fixture instead of re-deriving the loopback-bind + handshake dance from iroh's source each time.
---

# iroh-connection-fixture — get a real `Connection` into a unit test

Don't read `iroh`'s crate-registry source to figure out how to fabricate a
`Connection`. This has already been solved and re-solved three times in this
codebase (`artel-daemon::upgrade_protocol`'s `bind_endpoint`,
`artel-fs/tests/iroh_internals.rs`'s node fixtures, and an ad hoc version
that used to live in `artel-fs::docs_gate`'s tests) before anyone noticed the
duplication. The `docs_gate` version is now the canonical one, extracted to
`artel_iroh_setup::test_fixtures`.

## Which fixture fits

Two different problems look similar but need different fixtures:

| Need | Fixture |
|---|---|
| A test that already knows the exact endpoint to dial and just wants a real `Connection` in hand — e.g. driving `EndpointHooks::after_handshake` or `ProtocolHandler::accept` directly | `artel_iroh_setup::test_fixtures::{bind_loopback, dial_in}` |
| Peers that must discover each other by `EndpointId` — cross-daemon protocol dials, workspace joins, anything using tickets | `EndpointSetup::Testing { dns_pkarr }` + `iroh::test_utils::DnsPkarrServer` (see `crates/artel-iroh-setup/src/lib.rs` module docs) |

If you're not sure which: if the test can hardcode `server.addr()` and dial
it directly, you want `dial_in`. If the test needs pkarr/DNS lookup to find
the peer, you want the `Testing` discovery fixture.

## Using `dial_in` / `bind_loopback`

Requires the `test-utils` feature (already forwarded by both
`artel-fs`/`artel-daemon`'s own `test-utils` feature).

```rust
use artel_iroh_setup::test_fixtures::{bind_loopback, dial_in};

#[tokio::test]
async fn my_hook_rejects_revoked_peer() {
    // alpns: every ALPN this endpoint must ACCEPT connections for.
    // Pure dialers (never call server.accept()) pass vec![].
    let server = bind_loopback(vec![MY_ALPN.to_vec()]).await;

    // Drives a real client dial + server-side handshake concurrently,
    // hands back the server-side Connection.
    let connection = dial_in(&server, MY_ALPN).await;

    // Now drive the thing under test directly:
    let outcome = my_hook.after_handshake(&connection).await;
    // ...assert on outcome / emitted events...

    server.close().await;
}
```

For a `ProtocolHandler` (needs `Docs`/`Blobs`/whatever backing state, not
just the hook trait), see `crates/artel-fs/src/docs_gate.rs`'s `tests` module
for the full pattern including constructing the wrapped protocol.

## Don't

- Don't call `iroh::Endpoint::builder(presets::N0)` or reach for a real relay
  for this — `presets::Minimal` + loopback is deterministic and fast.
- Don't `tokio::join!` only the client's `connect()` future and await the
  server accept separately — `server.accept()` resolving is not the same as
  the handshake completing; both sides must be driven concurrently or the
  test hangs (this exact mistake produced a 30s timeout during development).
- Don't add a fourth reimplementation. If `dial_in`/`bind_loopback` don't fit
  a new case, extend `artel_iroh_setup::test_fixtures` instead of copying.
