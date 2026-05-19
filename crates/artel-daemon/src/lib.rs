//! Library crate for the artel daemon.
//!
//! The binary in `src/main.rs` is a thin wrapper around this library. The
//! library shape exists so end-to-end tests can spin the daemon up
//! in-process without going through `fork`/`exec`.

#[cfg(feature = "iroh")]
pub(crate) mod iroh_key;
pub mod pidfile;
pub mod server;
pub mod session;
pub mod shutdown;
pub(crate) mod store;

#[cfg(feature = "iroh")]
pub use server::IrohRuntime;
pub use server::{AddressLookupOverride, Daemon, DaemonConfig, StartError};
