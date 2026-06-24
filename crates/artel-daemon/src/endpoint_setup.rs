//! Daemon's endpoint-discovery hook.
//!
//! Mirror of `artel_fs::EndpointSetup`. Defined separately because
//! `artel-daemon` and `artel-fs` are peer crates (neither depends
//! on the other). The duplication is small; sharing it via a third
//! crate is more scaffolding than this enum is worth.
//!
//! Only available when the `iroh` feature is on — the type wraps
//! iroh-specific configuration. The no-iroh build keeps a
//! placeholder `()` field on `DaemonConfig` to keep the struct
//! shape feature-flag stable.
//!
//! See `artel-fs::endpoint_setup` for the full rationale on
//! `Production` vs `Testing` semantics, the relay-readiness gate,
//! and the upstream `DnsPkarrServer` fixture.

#![cfg(feature = "iroh")]
#![allow(clippy::redundant_pub_crate)]

/// DNS origin domain for the `Testing` fixture's [`iroh::test_utils::DnsPkarrServer`].
///
/// Mirror of `artel_fs::endpoint_setup::TEST_DNS_ORIGIN`. iroh 1.0
/// made `DnsPkarrServer`'s `endpoint_origin` field private (no
/// accessor) and its argless `run()` hardcodes `"dns.iroh.test"`
/// internally. We own the value here and construct the fixture via
/// `DnsPkarrServer::run_with_origin(TEST_DNS_ORIGIN)`, so the origin
/// the server serves on and the origin `EndpointSetup::apply`
/// resolves against are the same string rather than coupling to
/// iroh's private default.
#[cfg(feature = "test-utils")]
pub const TEST_DNS_ORIGIN: &str = "dns.iroh.test";

/// How the daemon's iroh endpoint discovers peers.
#[derive(Clone, Default)]
pub enum EndpointSetup {
    /// Production: [`iroh::endpoint::presets::N0`].
    #[default]
    Production,
    /// Testing: [`iroh::endpoint::presets::Minimal`] + a shared
    /// [`iroh::test_utils::DnsPkarrServer`]. See
    /// `artel-fs::EndpointSetup::Testing` for the longer rationale.
    #[cfg(feature = "test-utils")]
    Testing {
        /// Shared DNS+pkarr fixture.
        dns_pkarr: std::sync::Arc<iroh::test_utils::DnsPkarrServer>,
    },
    /// Testing-with-an-unreachable-relay: [`iroh::endpoint::presets::Minimal`]
    /// plus a custom [`iroh::RelayMode`] pointed at an RFC 5737
    /// TEST-NET-1 address. Drives [`iroh::Endpoint::online`] into
    /// the timeout path so [`crate::server::Daemon::start`] surfaces
    /// a typed [`crate::server::StartError::RelayUnreachable`]
    /// instead of hanging. Mirrors `artel_fs::EndpointSetup`.
    #[cfg(feature = "test-utils")]
    TestingUnreachableRelay,
    /// Production with a custom relay URL: uses N0's DNS/pkarr infra
    /// but overrides the relay to a caller-supplied URL and skips TLS
    /// cert verification. Used by binary-spawn tests to point the
    /// daemon subprocess at a localhost relay with self-signed certs.
    #[cfg(feature = "test-utils")]
    ProductionCustomRelay {
        /// The relay URL to use instead of n0's default.
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
                // See `artel_fs::endpoint_setup` for why the
                // `AddrFilter::ip_only` lives on the publisher
                // builder rather than on the endpoint builder.
                // `endpoint_origin` / `pkarr_url` became private in
                // iroh 1.0 (only `pkarr_url()` is public). Own the
                // origin via [`TEST_DNS_ORIGIN`] so it matches the
                // `run_with_origin` the fixture is built with, rather
                // than depending on iroh's internal `run()` default.
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
                // RFC 5737 TEST-NET-1 — guaranteed unrouteable so
                // the home-relay handshake never completes. See the
                // sibling variant in `artel_fs::endpoint_setup` for
                // the full rationale.
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
        }
    }

    /// Whether this setup connects to a relay. Mirrors
    /// `artel_fs::EndpointSetup::awaits_relay`. `Production` and
    /// `TestingUnreachableRelay` do; `Testing` (Minimal +
    /// `DnsPkarrServer`) does not — calling
    /// [`iroh::Endpoint::online`] on a `Testing` endpoint hangs
    /// forever because Minimal has no relay configured.
    pub(crate) const fn awaits_relay(&self) -> bool {
        match self {
            Self::Production => true,
            #[cfg(feature = "test-utils")]
            Self::Testing { .. } => false,
            #[cfg(feature = "test-utils")]
            Self::TestingUnreachableRelay => true,
            #[cfg(feature = "test-utils")]
            Self::ProductionCustomRelay { .. } => true,
        }
    }
}
