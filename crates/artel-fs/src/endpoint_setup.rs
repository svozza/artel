//! Substrate's endpoint-discovery hook.
//!
//! [`EndpointSetup`] picks how the per-`Workspace` iroh `Endpoint`
//! finds peers. Two variants:
//!
//! - [`EndpointSetup::Production`] — the [`presets::N0`] preset,
//!   which adds n0's pkarr publish + DNS resolve + relay default.
//!   Real deployments use this.
//! - [`EndpointSetup::Testing`] (only with `feature = "test-utils"`)
//!   — [`presets::Minimal`] + the caller's [`DnsPkarrServer`]. A
//!   localhost pkarr-publish HTTP server + a localhost DNS server
//!   with shared state, run for the test's lifetime. Deterministic
//!   (no propagation race against `dns.iroh.link`), localhost-fast,
//!   exercises the same code paths as production except the
//!   physical infrastructure. iroh-docs uses the same fixture in
//!   its own tests; this is the upstream-recommended pattern.
//!
//! [`presets::Minimal`] has no relay, so a `Testing` endpoint must
//! NOT call [`iroh::Endpoint::online`] (which awaits relay
//! readiness — Minimal would hang forever). [`Self::awaits_relay`]
//! is the gate.
//!
//! [`EndpointSetup::TestingUnreachableRelay`] (test-utils only) is
//! the inverse fixture: Minimal + a deliberately-unrouteable relay
//! URL ([RFC 5737] TEST-NET-1 `192.0.2.1`). `awaits_relay()`
//! returns `true`, so callers enter [`iroh::Endpoint::online`] and
//! the relay handshake never completes — exercising the timeout
//! wrapper that surfaces [`crate::WorkspaceError::RelayUnreachable`]
//! instead of hanging forever.
//!
//! [RFC 5737]: https://datatracker.ietf.org/doc/html/rfc5737

#![allow(clippy::redundant_pub_crate)]

/// How the per-`Workspace` iroh endpoint discovers peers.
///
/// See module docs for variant semantics.
#[derive(Clone, Default)]
pub enum EndpointSetup {
    /// Production: [`iroh::endpoint::presets::N0`] — pkarr publish
    /// plus DNS resolve via n0 infrastructure, with the n0 default
    /// relay map. The caller should `await endpoint.online()`
    /// after `bind()` so the home-relay handshake completes before
    /// the first dial.
    #[default]
    Production,
    /// Testing: [`iroh::endpoint::presets::Minimal`] +
    /// [`iroh::test_utils::DnsPkarrServer::preset`]. Runs against a
    /// localhost pkarr+DNS pair shared by every endpoint that holds
    /// a clone of the same `Arc<DnsPkarrServer>`. Skip
    /// `endpoint.online()` — Minimal has no relay.
    #[cfg(feature = "test-utils")]
    Testing {
        /// Shared DNS+pkarr fixture. Held by tests for the duration
        /// of the test; the inner `Drop` shuts the localhost
        /// servers down when the last clone goes out of scope.
        dns_pkarr: std::sync::Arc<iroh::test_utils::DnsPkarrServer>,
    },
    /// Testing-with-an-unreachable-relay: [`iroh::endpoint::presets::Minimal`]
    /// plus a custom [`iroh::RelayMode`] pointed at an RFC 5737
    /// TEST-NET-1 address. [`Self::awaits_relay`] returns `true` so
    /// the caller hits [`iroh::Endpoint::online`]; the relay
    /// handshake never completes. Sole consumer is the integration
    /// test that pins the timeout-and-typed-error contract for
    /// [`crate::WorkspaceError::RelayUnreachable`].
    #[cfg(feature = "test-utils")]
    TestingUnreachableRelay,
}

impl std::fmt::Debug for EndpointSetup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Production => f.write_str("EndpointSetup::Production"),
            #[cfg(feature = "test-utils")]
            Self::Testing { .. } => f.write_str("EndpointSetup::Testing { dns_pkarr: <..> }"),
            #[cfg(feature = "test-utils")]
            Self::TestingUnreachableRelay => f.write_str("EndpointSetup::TestingUnreachableRelay"),
        }
    }
}

impl EndpointSetup {
    /// Apply this setup's discovery layer to an endpoint builder.
    pub(crate) fn apply(&self, builder: iroh::endpoint::Builder) -> iroh::endpoint::Builder {
        use iroh::endpoint::presets::Preset;
        match self {
            Self::Production => iroh::endpoint::presets::N0.apply(builder),
            #[cfg(feature = "test-utils")]
            Self::Testing { dns_pkarr } => {
                let builder = iroh::endpoint::presets::Minimal.apply(builder);
                // `DnsPkarrServer::preset()` defaults the
                // PkarrPublisher's `AddrFilter` to `relay_only`
                // because the upstream fixture is paired with a
                // test relay. Our tests run direct UDP between
                // localhost peers (no relay), so the publisher
                // must use `ip_only` instead — otherwise it
                // publishes nothing (no relay url to publish)
                // and the joiner's DNS lookup returns empty.
                // The filter lives on the publisher builder, NOT
                // on the endpoint builder. iroh-docs's
                // `tests/util.rs` works around the same
                // constraint by spinning up a test relay; we
                // don't want a relay in the loop, so we publish
                // direct IPs instead.
                let dns_address_lookup = iroh::address_lookup::DnsAddressLookup::builder(
                    dns_pkarr.endpoint_origin.clone(),
                );
                let pkarr_publisher =
                    iroh::address_lookup::PkarrPublisher::builder(dns_pkarr.pkarr_url.clone())
                        .addr_filter(iroh::address_lookup::AddrFilter::ip_only());
                builder
                    .address_lookup(dns_address_lookup)
                    .address_lookup(pkarr_publisher)
                    .dns_resolver(dns_pkarr.dns_resolver())
            }
            #[cfg(feature = "test-utils")]
            Self::TestingUnreachableRelay => {
                let builder = iroh::endpoint::presets::Minimal.apply(builder);
                // RFC 5737 TEST-NET-1 — guaranteed unrouteable on
                // the public internet, so the home-relay handshake
                // that `endpoint.online()` waits on never completes.
                let url = "https://192.0.2.1/"
                    .parse::<iroh::RelayUrl>()
                    .expect("static RFC 5737 TEST-NET-1 url parses");
                builder.relay_mode(iroh::RelayMode::custom([url]))
            }
        }
    }

    /// Whether this setup connects to a relay. `Testing` (Minimal)
    /// has no relay so callers must skip [`iroh::Endpoint::online`];
    /// otherwise the future never completes.
    /// `TestingUnreachableRelay` returns `true` deliberately — the
    /// fixture's whole job is to drive the timeout wrapper.
    pub(crate) const fn awaits_relay(&self) -> bool {
        match self {
            Self::Production => true,
            #[cfg(feature = "test-utils")]
            Self::Testing { .. } => false,
            #[cfg(feature = "test-utils")]
            Self::TestingUnreachableRelay => true,
        }
    }
}
