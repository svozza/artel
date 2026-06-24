//! Wire protocol shared between `artel-daemon` and `artel-client`.
//!
//! This crate is dependency-free of `iroh` and of any IPC transport. It
//! defines the over-the-socket types and version constants the two sides
//! agree on. See `docs/adr/001-collab-substrate-platform.md` for the
//! motivating design.
//!
//! # Wire format
//!
//! - **Binary IPC frames** are serialized with `postcard` and prefixed with
//!   their length by the framing layer (defined separately in
//!   `artel-client` / `artel-daemon`). All wire-form enums in this crate
//!   use serde's default *external* tagging because postcard does not
//!   implement adjacently- or internally-tagged enums.
//! - **Human-readable rendering** (CLI output, logs, fixtures) uses
//!   `serde_json`. Round-trips through both formats are verified by tests.
//!
//! # Versioning
//!
//! Two version axes evolve independently:
//!
//! - [`ProtocolVersion`] / [`PROTOCOL_VERSION`] — the IPC handshake
//!   version, negotiated once per connection via [`Request::Hello`].
//! - [`MessageFormat`] / [`MESSAGE_FORMAT`] — the per-message envelope
//!   version stamped on every [`SessionMessage`].

#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]

pub mod capability;
pub mod error;
pub mod gossip;
pub mod ids;
pub mod message;
pub mod rpc;
#[cfg(feature = "signing")]
pub mod signing;
pub mod ticket;
#[cfg(feature = "tokio")]
pub mod transport;
pub mod upgrade;
pub mod version;

pub use capability::{ACTION_GRANT, ACTION_REVOKE, Capability, CapabilityAction};
pub use error::ProtocolError;
pub use ids::{PeerId, Seq, SessionId, TicketId};
pub use message::{
    DOWNGRADE_ACTION, MESSAGE_FORMAT, MessageFormat, MessageKind, PeerInfo, ROTATE_ACTION,
    SIGNATURE_UNSIGNED, SessionMessage, SigBytes, TICKET_ACTION, UPGRADE_ACTION,
};
pub use rpc::{
    Attachment, Event, JoinTicket, Request, RequestId, Response, SendPayload, SessionSummary,
    SignedSendPayload, WireMessage,
};
pub use ticket::{
    SessionTicket, TICKET_PREFIX, TICKET_VERSION, TicketEntry, TicketError, TicketStatus,
    WireEndpointAddr,
};
pub use upgrade::{
    DeliveryFrame, DowngradePayload, RotatePayload, UPGRADE_ACK, UPGRADE_ALPN, UpgradeFrame,
    UpgradePayload,
};
pub use version::{PROTOCOL_VERSION, ProtocolVersion, VersionMismatch};
