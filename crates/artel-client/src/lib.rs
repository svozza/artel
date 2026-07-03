//! Rust client for connecting to the `artel-daemon`.
//!
//! Thin async wrapper over the IPC transport. Handles the version
//! handshake, demultiplexes responses against in-flight requests, and
//! exposes incoming events as an [`EventStream`] (an
//! `mpsc::Receiver<Event>`; drain it with `.recv().await`). Apps that
//! want a richer ergonomic layer (session handles, typed payload
//! helpers, etc.) build on top of this.

#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]

mod client;
mod error;
mod spawn;

pub use client::{Client, EventStream};
pub use error::ClientError;
pub use spawn::{SpawnError, SpawnOptions};
