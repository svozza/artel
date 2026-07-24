//! Shared fixtures for unit tests that need a real `iroh::endpoint::Connection`.
//!
//! [`bind_loopback`] + [`dial_in`] drive a minimal two-endpoint handshake
//! on IPv4 loopback, no discovery layer involved — deliberately smaller
//! than [`crate::EndpointSetup::Testing`]'s `DnsPkarrServer` fixture, which
//! is for tests that need peers to discover each other by `EndpointId`
//! (cross-daemon protocol dials, workspace joins). Reach for these two
//! when the test already knows the exact endpoint to dial and just needs
//! a `Connection` in hand.
//!
//! Extracted from an ad hoc version that first appeared in
//! `artel-fs::docs_gate`'s tests, so the next hook/handler unit test
//! that needs a real `Connection` has a home for it instead of
//! re-deriving the loopback-bind + handshake dance from iroh's source.
//! Distinct from `artel-daemon::upgrade_protocol`'s `bind_endpoint` and
//! `artel-fs/tests/iroh_internals.rs`'s node fixtures, which dial by
//! `EndpointId` through the `DnsPkarrServer` discovery layer above —
//! a different problem this module doesn't try to solve.

use std::net::Ipv4Addr;

use iroh::Endpoint;
use iroh::endpoint::{Connection, presets};

/// Bind an endpoint on IPv4 loopback only, with no discovery layer.
///
/// `clear_ip_transports` + an explicit loopback `bind_addr` matches
/// iroh's own `direct_pair` test fixture: without it, multi-homed CI
/// hosts advertise addresses a loopback-only dial can't reach.
///
/// `alpns` should list every ALPN this endpoint must *accept*
/// connections for; pass `vec![]` for a pure dialer that only calls
/// [`Endpoint::connect`] and never needs to `accept()`.
pub async fn bind_loopback(alpns: Vec<Vec<u8>>) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .alpns(alpns)
        .clear_ip_transports()
        .bind_addr((Ipv4Addr::LOCALHOST, 0))
        .expect("valid loopback bind addr")
        .bind()
        .await
        .expect("bind endpoint")
}

/// Dial `server` and hand back the server-side [`Connection`] it produced.
///
/// Lets a test drive `ProtocolHandler::accept` or an `EndpointHooks`
/// method directly against a real handshake instead of a synthetic
/// value. `server` must already have `alpn` in its accepted ALPN list
/// (see [`bind_loopback`]).
///
/// `server.accept()` only resolves once the first handshake packet
/// arrives — completing the handshake needs a further `.accept()` +
/// await that must run *concurrently* with the client's `connect()`
/// (the client's future won't resolve until the server side responds).
/// Both sides are therefore driven inside one `tokio::join!`.
pub async fn dial_in(server: &Endpoint, alpn: &[u8]) -> Connection {
    let client = bind_loopback(vec![]).await;
    let addr = server.addr();
    let server_side = async {
        let incoming = server.accept().await.expect("server accept");
        incoming
            .accept()
            .expect("accept incoming")
            .await
            .expect("server handshake")
    };
    let (client_conn, server_conn) = tokio::join!(client.connect(addr, alpn), server_side);
    client_conn.expect("client connect");
    server_conn
}
