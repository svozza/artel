//! Shared iroh endpoint-discovery setup.
//!
//! [`EndpointSetup`] picks how an artel iroh `Endpoint` (the daemon's
//! network identity, or a per-`Workspace` node) finds peers.
//! `artel-daemon` and `artel-fs` are peer crates — neither may depend
//! on the other (the daemon stays namespace-agnostic per ADR-003;
//! artel-fs talks to the daemon only over IPC) — so the setup they
//! share lives here, below both. Each re-exports the type, so
//! consumers keep writing `artel_daemon::EndpointSetup` /
//! `artel_fs::EndpointSetup`.
//!
//! One production variant plus four `test-utils`-gated fixtures:
//!
//! - [`EndpointSetup::Production`] — the [`presets::N0`] preset,
//!   which adds n0's pkarr publish + DNS resolve + relay default.
//!   Real deployments use this.
//! - `EndpointSetup::Testing` — [`presets::Minimal`] + the
//!   caller's [`DnsPkarrServer`]. A localhost pkarr-publish HTTP
//!   server + a localhost DNS server with shared state, run for the
//!   test's lifetime. Deterministic (no propagation race against
//!   `dns.iroh.link`), localhost-fast, exercises the same code paths
//!   as production except the physical infrastructure. iroh-docs
//!   uses the same fixture in its own tests; this is the
//!   upstream-recommended pattern.
//! - `EndpointSetup::TestingUnreachableRelay` — the inverse
//!   fixture: Minimal + a deliberately-unrouteable relay URL
//!   ([RFC 5737] TEST-NET-1 `192.0.2.1`). `awaits_relay()` returns
//!   `true`, so callers enter [`iroh::Endpoint::online`] and the
//!   relay handshake never completes — exercising the
//!   [`await_relay_ready`] timeout that each consumer surfaces as its
//!   own typed relay-unreachable error instead of hanging forever.
//! - `EndpointSetup::ProductionCustomRelay` — the N0 preset with
//!   the relay overridden to a caller-supplied URL (self-signed TLS
//!   accepted). Points real-network tests at a localhost
//!   `iroh-relay` instead of n0's public relay.
//! - `EndpointSetup::TestingWithRelay` — Minimal + a custom relay
//!   only (no pkarr/DNS), for cross-process tests that can share a
//!   localhost relay but not an in-process `DnsPkarrServer`.
//!
//! [`presets::Minimal`] has no relay, so a `Testing` endpoint must
//! NOT call [`iroh::Endpoint::online`] (which awaits relay
//! readiness — Minimal would hang forever).
//! [`EndpointSetup::awaits_relay`] is the gate, applied by
//! [`await_relay_ready`].
//!
//! [`presets::N0`]: iroh::endpoint::presets::N0
//! [`presets::Minimal`]: iroh::endpoint::presets::Minimal
//! [`DnsPkarrServer`]: https://docs.rs/iroh/latest/iroh/test_utils/struct.DnsPkarrServer.html
//! [RFC 5737]: https://datatracker.ietf.org/doc/html/rfc5737

use std::time::Duration;

/// How long [`await_relay_ready`] waits for the home-relay handshake.
///
/// Bounds the `endpoint.online()` wait: tight enough to fail fast
/// when the relay is unreachable (offline laptop, captive portal, n0
/// outage), loose enough to cover normal startup.
pub const HOME_RELAY_BUDGET: Duration = Duration::from_secs(30);

/// DNS origin domain for the `Testing` fixture's [`iroh::test_utils::DnsPkarrServer`].
///
/// Must match the origin the server is actually constructed with.
/// iroh 1.0 made `DnsPkarrServer`'s `endpoint_origin` field private
/// (no accessor) and its argless `run()` hardcodes `"dns.iroh.test"`
/// internally. To avoid silently coupling to that private default we
/// own the value here and construct the fixture via
/// `DnsPkarrServer::run_with_origin(TEST_DNS_ORIGIN)`, so the origin
/// the server serves on and the origin `EndpointSetup::apply`
/// resolves against are the same string.
#[cfg(feature = "test-utils")]
pub const TEST_DNS_ORIGIN: &str = "dns.iroh.test";

/// How an artel iroh endpoint discovers peers.
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
    /// TEST-NET-1 address. `Self::awaits_relay` returns `true` so
    /// the caller hits [`iroh::Endpoint::online`]; the relay
    /// handshake never completes. Sole consumers are the integration
    /// tests that pin each crate's timeout-and-typed-error contract
    /// (`RelayUnreachable`).
    #[cfg(feature = "test-utils")]
    TestingUnreachableRelay,
    /// Production with a custom relay URL: uses N0's DNS/pkarr infra
    /// but overrides the relay to a caller-supplied URL and skips TLS
    /// cert verification. Used by tests to point at a localhost relay
    /// with self-signed certs instead of n0's public relay.
    #[cfg(feature = "test-utils")]
    ProductionCustomRelay {
        /// The relay URL to use instead of n0's default.
        relay_url: iroh::RelayUrl,
    },
    /// Minimal endpoint with only a custom relay (no pkarr/DNS). Used
    /// by cross-process tests where both endpoints share a localhost
    /// relay but can't share a `DnsPkarrServer`. The relay handles
    /// routing by `EndpointId` alone.
    #[cfg(feature = "test-utils")]
    TestingWithRelay {
        /// The relay URL to connect to.
        relay_url: iroh::RelayUrl,
    },
}

impl std::fmt::Debug for EndpointSetup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Production => f.write_str("EndpointSetup::Production"),
            #[cfg(feature = "test-utils")]
            Self::Testing { .. } => f.write_str("EndpointSetup::Testing { dns_pkarr: <..> }"),
            #[cfg(feature = "test-utils")]
            Self::TestingUnreachableRelay => f.write_str("EndpointSetup::TestingUnreachableRelay"),
            #[cfg(feature = "test-utils")]
            Self::ProductionCustomRelay { relay_url } => {
                write!(f, "EndpointSetup::ProductionCustomRelay {{ {relay_url} }}")
            }
            #[cfg(feature = "test-utils")]
            Self::TestingWithRelay { relay_url } => {
                write!(f, "EndpointSetup::TestingWithRelay {{ {relay_url} }}")
            }
        }
    }
}

impl EndpointSetup {
    /// Apply this setup's discovery layer to an endpoint builder.
    #[must_use]
    pub fn apply(&self, builder: iroh::endpoint::Builder) -> iroh::endpoint::Builder {
        use iroh::endpoint::presets::Preset;
        match self {
            Self::Production => iroh::endpoint::presets::N0.apply(builder),
            #[cfg(feature = "test-utils")]
            Self::Testing { dns_pkarr } => {
                let builder = iroh::endpoint::presets::Minimal.apply(builder);
                // The PkarrPublisher builder's own default
                // `AddrFilter` is `relay_only`, because the upstream
                // fixture is normally paired with a test relay. Our
                // tests run direct UDP between localhost peers (no
                // relay), so the publisher must use `ip_only`
                // instead — otherwise it publishes nothing (no relay
                // url to publish) and the joiner's DNS lookup
                // returns empty. Hence the explicit `.addr_filter`
                // override on the publisher builder below.
                // iroh-docs's `tests/util.rs` works around the same
                // constraint by spinning up a test relay; we don't
                // want a relay in the loop, so we publish direct IPs
                // instead.
                // `DnsPkarrServer`'s `endpoint_origin` / `pkarr_url`
                // fields became private in iroh 1.0; only `pkarr_url()`
                // has a public accessor. We own the origin ourselves
                // via [`TEST_DNS_ORIGIN`] and construct the fixture with
                // `run_with_origin` (see the `Testing` construction
                // sites), so the lookup origin here and the origin the
                // server actually serves on share one source of truth
                // rather than silently depending on iroh's internal
                // `run()` default.
                let dns_address_lookup =
                    iroh::address_lookup::DnsAddressLookup::builder(TEST_DNS_ORIGIN.to_string());
                let pkarr_publisher =
                    iroh::address_lookup::PkarrPublisher::builder(dns_pkarr.pkarr_url().clone())
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
            #[cfg(feature = "test-utils")]
            Self::ProductionCustomRelay { relay_url } => {
                let builder = iroh::endpoint::presets::N0.apply(builder);
                builder
                    .relay_mode(iroh::RelayMode::custom([relay_url.clone()]))
                    .ca_tls_config(iroh::tls::CaTlsConfig::insecure_skip_verify())
            }
            #[cfg(feature = "test-utils")]
            Self::TestingWithRelay { relay_url } => {
                let builder = iroh::endpoint::presets::Minimal.apply(builder);
                builder
                    .relay_mode(iroh::RelayMode::custom([relay_url.clone()]))
                    .ca_tls_config(iroh::tls::CaTlsConfig::insecure_skip_verify())
            }
        }
    }

    /// Whether this setup connects to a relay. `Testing` (Minimal)
    /// has no relay so callers must skip [`iroh::Endpoint::online`];
    /// otherwise the future never completes.
    /// `TestingUnreachableRelay` returns `true` deliberately — the
    /// fixture's whole job is to drive the timeout wrapper.
    #[must_use]
    pub const fn awaits_relay(&self) -> bool {
        match self {
            Self::Production => true,
            #[cfg(feature = "test-utils")]
            Self::Testing { .. } => false,
            #[cfg(feature = "test-utils")]
            Self::TestingUnreachableRelay => true,
            #[cfg(feature = "test-utils")]
            Self::ProductionCustomRelay { .. } => true,
            #[cfg(feature = "test-utils")]
            Self::TestingWithRelay { .. } => true,
        }
    }
}

/// Block until the home-relay handshake (`endpoint.online()`)
/// completes, bounded by [`HOME_RELAY_BUDGET`] — or return
/// immediately when `setup` has no relay to wait for
/// ([`EndpointSetup::awaits_relay`]).
///
/// The bound exists to fail fast when the relay is unreachable
/// (offline laptop, captive portal, n0 outage, or the
/// `TestingUnreachableRelay` fixture) instead of hanging startup
/// forever.
///
/// # Errors
///
/// `Err(HOME_RELAY_BUDGET)` when the handshake didn't complete within
/// the budget. Each consumer maps this to its own typed error
/// (`artel_daemon`'s `StartError::RelayUnreachable`, `artel_fs`'s
/// `WorkspaceError::RelayUnreachable`) — the error types deliberately
/// stay per-crate.
pub async fn await_relay_ready(
    setup: &EndpointSetup,
    endpoint: &iroh::Endpoint,
) -> Result<(), Duration> {
    if setup.awaits_relay()
        && tokio::time::timeout(HOME_RELAY_BUDGET, endpoint.online())
            .await
            .is_err()
    {
        return Err(HOME_RELAY_BUDGET);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_awaits_relay() {
        assert!(EndpointSetup::Production.awaits_relay());
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn awaits_relay_truth_table() {
        // `Testing` is the one variant with no relay at all —
        // awaiting `online()` on it would hang forever, so it must
        // be the only `false`. Every relay-configured variant is
        // `true`, including `TestingUnreachableRelay`, whose whole
        // job is to drive the `await_relay_ready` timeout.
        let url: iroh::RelayUrl = "https://192.0.2.1/".parse().unwrap();
        assert!(EndpointSetup::TestingUnreachableRelay.awaits_relay());
        assert!(
            EndpointSetup::ProductionCustomRelay {
                relay_url: url.clone(),
            }
            .awaits_relay()
        );
        assert!(EndpointSetup::TestingWithRelay { relay_url: url }.awaits_relay());
        // `Testing` needs a live DnsPkarrServer to construct; its
        // `false` arm is pinned by every Tier-B integration test
        // that would hang in `await_relay_ready` otherwise.
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn debug_impl_names_variants_without_dumping_fixtures() {
        let url: iroh::RelayUrl = "https://192.0.2.1/".parse().unwrap();
        assert_eq!(
            format!("{:?}", EndpointSetup::Production),
            "EndpointSetup::Production"
        );
        assert_eq!(
            format!("{:?}", EndpointSetup::TestingUnreachableRelay),
            "EndpointSetup::TestingUnreachableRelay"
        );
        assert!(
            format!(
                "{:?}",
                EndpointSetup::ProductionCustomRelay { relay_url: url }
            )
            .starts_with("EndpointSetup::ProductionCustomRelay")
        );
    }
}
