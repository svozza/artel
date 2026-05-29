//! Library crate for the artel daemon.
//!
//! The binary in `src/main.rs` is a thin wrapper around this library. The
//! library shape exists so end-to-end tests can spin the daemon up
//! in-process without going through `fork`/`exec`.

#[cfg(feature = "iroh")]
pub(crate) mod endpoint_setup;
#[cfg(feature = "iroh")]
pub(crate) mod gossip_bridge;
#[cfg(feature = "iroh")]
pub(crate) mod iroh_key;
#[cfg(feature = "iroh")]
pub(crate) mod peer_addr_cache;
pub mod pidfile;
pub mod server;
pub mod session;
pub mod shutdown;
pub(crate) mod store;

#[cfg(feature = "iroh")]
pub use endpoint_setup::EndpointSetup;
#[cfg(feature = "iroh")]
pub use server::IrohRuntime;
pub use server::{Daemon, DaemonConfig, StartError};
