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
}

impl std::fmt::Debug for EndpointSetup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Production => f.write_str("EndpointSetup::Production"),
            #[cfg(feature = "test-utils")]
            Self::Testing { .. } => f.write_str("EndpointSetup::Testing { dns_pkarr: <..> }"),
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
        }
    }
}
