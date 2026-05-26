//! Request / response / event types for the daemon ↔ client RPC.
//!
//! See ADR-001 § "RPC surface". The IPC framing (length prefix, transport)
//! is intentionally not specified here — both `artel-daemon` and
//! `artel-client` agree on a separate framing convention. This module only
//! defines the *payload* of each frame.

use serde::{Deserialize, Serialize};

use crate::error::ProtocolError;
use crate::ids::{PeerId, Seq, SessionId};
use crate::message::{PeerInfo, SessionMessage};
use crate::version::ProtocolVersion;

/// Correlates a [`Request`] with its [`Response`].
///
/// Allocated by the client. Unique per connection. Reuse across connections
/// is fine; uniqueness within a connection is what matters.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct RequestId(u64);

impl RequestId {
    /// The first valid request id.
    pub const ZERO: Self = Self(0);

    /// Construct from a raw `u64`.
    #[must_use]
    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    /// The raw `u64` value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Allocate the next request id, panicking on overflow.
    ///
    /// At one request per nanosecond a `u64` lasts 584 years, so overflow
    /// in practice means a connection-id-recycling bug.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(n) => Some(Self(n)),
            None => None,
        }
    }
}

/// An opaque join ticket distributed out-of-band.
///
/// Today this is the iroh `NodeAddr` + topic encoded by the daemon. The
/// protocol crate treats it as an opaque string; the daemon parses and
/// validates it. Tickets are bearer credentials — anyone with one can join.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JoinTicket(pub String);

impl JoinTicket {
    /// Borrow the underlying string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for JoinTicket {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for JoinTicket {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// Brief, list-safe summary of a session.
///
/// Returned by [`Request::ListSessions`]. Does *not* include the full log;
/// use [`Request::Subscribe`] for that.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSummary {
    /// Session identifier.
    pub id: SessionId,
    /// Whether this daemon is the host of the session.
    pub is_host: bool,
    /// Number of currently-connected peers, including ourself.
    pub peer_count: u32,
    /// Highest sequence number seen so far.
    pub last_seq: Option<Seq>,
}

/// A request the client sends to the daemon.
///
/// Every variant has a corresponding successful [`Response`] variant; the
/// daemon may also respond with [`Response::Error`] on any request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Request {
    /// Negotiate protocol version. Must be the first request on a fresh
    /// connection. The daemon either accepts or returns
    /// [`ProtocolError::VersionMismatch`].
    Hello {
        /// Version the client speaks.
        client_version: ProtocolVersion,
    },

    /// Create or resume a hosted session. Returns the session's
    /// [`SessionId`] and a shareable [`JoinTicket`].
    ///
    /// `session` controls how the id is chosen:
    /// - `None` (the default): mint a fresh random id. This is
    ///   today's behaviour and the right choice for first-time
    ///   hosts that have nothing to resume.
    /// - `Some(id)`: resume the session at that id when a matching
    ///   local-host record exists; otherwise create a new session
    ///   with the supplied id. Used by callers (e.g. `artel-fs`)
    ///   that derive a stable id from local state so a re-host of
    ///   the same workspace lands on the same session id across
    ///   daemon restarts.
    ///
    /// A `Some(id)` request whose existing record has a different
    /// host or is a remote-mirror session is rejected with
    /// [`ProtocolError::SessionConflict`].
    HostSession {
        /// Display info for this peer (the host).
        peer: PeerInfo,
        /// Caller-supplied session id, or `None` to mint a fresh one.
        session: Option<SessionId>,
    },

    /// Join an existing session via its ticket.
    JoinSession {
        /// Display info for this peer (the joiner).
        peer: PeerInfo,
        /// Ticket obtained out-of-band from the host.
        ticket: JoinTicket,
    },

    /// List sessions the daemon is currently hosting or joined.
    ListSessions,

    /// Subscribe to event delivery for a session. Optionally replays the
    /// log from `since` onwards.
    Subscribe {
        /// Session to subscribe to.
        session: SessionId,
        /// If set, replay messages with `seq > since` before live delivery
        /// resumes. If `None`, the daemon delivers only future messages.
        since: Option<Seq>,
    },

    /// Send a message into a session. The daemon assigns the [`Seq`].
    Send {
        /// Session to send into.
        session: SessionId,
        /// Application payload. The daemon does not inspect it.
        payload: SendPayload,
    },

    /// Leave a session. For hosts this also closes the session for all
    /// peers; for joiners it disconnects only this peer.
    LeaveSession {
        /// Session to leave.
        session: SessionId,
    },
}

/// Fields of a [`Request::Send`] that the client supplies.
///
/// Distinct from [`SessionMessage`] because the host-assigned `seq`,
/// `version`, and `peer` are filled in by the daemon, not the client.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendPayload {
    /// Top-level category.
    pub kind: crate::message::MessageKind,
    /// Application-defined verb.
    pub action: String,
    /// Opaque application bytes.
    #[serde(with = "send_payload_bytes")]
    pub payload: Vec<u8>,
}

mod send_payload_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            s.collect_seq(bytes.iter().copied())
        } else {
            s.serialize_bytes(bytes)
        }
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        if d.is_human_readable() {
            Vec::<u8>::deserialize(d)
        } else {
            serde_bytes::ByteBuf::deserialize(d).map(serde_bytes::ByteBuf::into_vec)
        }
    }
}

/// The daemon's reply to a [`Request`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Response {
    /// Reply to [`Request::Hello`]. Contains the daemon's own version so
    /// the client can log it; mismatches arrive via [`Response::Error`]
    /// rather than this variant.
    Hello {
        /// Version the daemon speaks.
        daemon_version: ProtocolVersion,
        /// The daemon's own peer id, advertised so clients can show it
        /// without having to host or join a session first.
        daemon_peer_id: PeerId,
    },

    /// Reply to [`Request::HostSession`].
    HostSession {
        /// Identifier for the new session.
        session: SessionId,
        /// Ticket the host distributes out-of-band to invitees.
        ticket: JoinTicket,
    },

    /// Reply to [`Request::JoinSession`].
    JoinSession {
        /// Identifier for the joined session.
        session: SessionId,
        /// Highest seq the daemon has seen for the session at join time.
        /// Useful for clients deciding what to backfill.
        head: Option<Seq>,
    },

    /// Reply to [`Request::ListSessions`].
    ListSessions {
        /// One entry per active session.
        sessions: Vec<SessionSummary>,
    },

    /// Reply to [`Request::Subscribe`]. The client should expect [`Event`]
    /// frames after this.
    Subscribed {
        /// Session that was subscribed to.
        session: SessionId,
    },

    /// Reply to [`Request::Send`]. Acknowledges the message and reports
    /// the assigned sequence number.
    Sent {
        /// Session the message was added to.
        session: SessionId,
        /// Sequence number the daemon assigned.
        seq: Seq,
    },

    /// Reply to [`Request::LeaveSession`].
    Left {
        /// Session that was left.
        session: SessionId,
    },

    /// Any request may produce this in place of its expected variant.
    Error {
        /// Wire-representable error.
        error: ProtocolError,
    },
}

/// Asynchronous event the daemon pushes to a subscribed client.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Event {
    /// A new (or replayed) message in a subscribed session.
    Message {
        /// Session the message belongs to.
        session: SessionId,
        /// The message itself.
        message: SessionMessage,
    },

    /// A peer joined a subscribed session.
    PeerJoined {
        /// Session the peer joined.
        session: SessionId,
        /// The peer.
        peer: PeerInfo,
    },

    /// A peer left a subscribed session.
    PeerLeft {
        /// Session the peer left.
        session: SessionId,
        /// Peer id of the departed peer.
        peer: PeerId,
    },

    /// The session was closed by the host.
    SessionClosed {
        /// Session that closed.
        session: SessionId,
    },
}

/// One frame on the IPC wire.
///
/// Requests and responses are correlated by [`RequestId`]; events have no
/// id since they are server-pushed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireMessage {
    /// Client → daemon.
    Request {
        /// Identifier the client picked.
        id: RequestId,
        /// Request payload.
        request: Request,
    },
    /// Daemon → client, in reply to a request with this id.
    Response {
        /// Echoes the [`Request::Request`] id this is replying to.
        id: RequestId,
        /// Response payload.
        response: Response,
    },
    /// Daemon → client, unsolicited. Server-push of a subscription event.
    Event {
        /// Event payload.
        event: Event,
    },
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    use super::*;
    use crate::error::ProtocolError;
    use crate::message::{MessageKind, PeerInfo};
    use crate::version::{PROTOCOL_VERSION, VersionMismatch};

    fn sample_peer() -> PeerInfo {
        PeerInfo::new(PeerId::from_bytes([1; 32]), "alice")
    }

    fn sample_session_message() -> SessionMessage {
        SessionMessage::new(
            Seq::new(3),
            42,
            sample_peer(),
            MessageKind::Chat,
            "chat.message",
            b"hi".to_vec(),
        )
    }

    // ---- RequestId ----

    #[test]
    fn request_id_zero_then_one() {
        assert_eq!(RequestId::ZERO.next(), Some(RequestId::new(1)));
    }

    #[test]
    fn request_id_overflow_returns_none() {
        assert_eq!(RequestId::new(u64::MAX).next(), None);
    }

    // ---- JoinTicket ----

    #[test]
    fn join_ticket_from_str_and_string() {
        let from_str: JoinTicket = "abc".into();
        let from_string: JoinTicket = String::from("abc").into();
        assert_eq!(from_str, from_string);
        assert_eq!(from_str.as_str(), "abc");
    }

    // ---- Request round-trips ----

    #[test]
    fn hello_request_round_trip() {
        let req = Request::Hello {
            client_version: PROTOCOL_VERSION,
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn host_session_request_round_trip() {
        let req = Request::HostSession {
            peer: sample_peer(),
            session: None,
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn host_session_request_round_trip_with_session_id() {
        let req = Request::HostSession {
            peer: sample_peer(),
            session: Some(SessionId::from_bytes([7; 16])),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);

        // JSON round-trip too — externally-tagged enum, snake_case fields.
        let json = serde_json::to_string(&req).unwrap();
        let back: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn join_session_request_round_trip() {
        let req = Request::JoinSession {
            peer: sample_peer(),
            ticket: JoinTicket::from("ticket-blob"),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn list_sessions_request_is_unit_shaped() {
        let req = Request::ListSessions;
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);
        // JSON of a unit variant is a bare string.
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, "\"list_sessions\"");
    }

    #[test]
    fn subscribe_request_round_trip_with_and_without_since() {
        let session = SessionId::from_bytes([7; 16]);
        for since in [None, Some(Seq::new(99))] {
            let req = Request::Subscribe { session, since };
            let bytes = postcard::to_allocvec(&req).unwrap();
            let back: Request = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(req, back);
        }
    }

    #[test]
    fn send_request_round_trip() {
        let req = Request::Send {
            session: SessionId::from_bytes([2; 16]),
            payload: SendPayload {
                kind: MessageKind::Tool,
                action: "tool.exec".into(),
                payload: b"args".to_vec(),
            },
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let back: Request = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, back);
    }

    // ---- Response round-trips ----

    #[test]
    fn hello_response_round_trip() {
        let resp = Response::Hello {
            daemon_version: PROTOCOL_VERSION,
            daemon_peer_id: PeerId::from_bytes([9; 32]),
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let back: Response = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, back);
    }

    #[test]
    fn host_session_response_round_trip() {
        let resp = Response::HostSession {
            session: SessionId::from_bytes([3; 16]),
            ticket: JoinTicket::from("xyz"),
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let back: Response = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, back);
    }

    #[test]
    fn list_sessions_response_round_trip_empty_and_populated() {
        for sessions in [
            Vec::<SessionSummary>::new(),
            vec![SessionSummary {
                id: SessionId::from_bytes([4; 16]),
                is_host: true,
                peer_count: 3,
                last_seq: Some(Seq::new(42)),
            }],
        ] {
            let resp = Response::ListSessions {
                sessions: sessions.clone(),
            };
            let bytes = postcard::to_allocvec(&resp).unwrap();
            let back: Response = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(resp, back);
        }
    }

    #[test]
    fn error_response_carries_protocol_error() {
        let resp = Response::Error {
            error: ProtocolError::VersionMismatch(VersionMismatch {
                client: PROTOCOL_VERSION,
                daemon: ProtocolVersion::new(99),
            }),
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let back: Response = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, back);
    }

    // ---- Event round-trips ----

    #[test]
    fn event_message_round_trip() {
        let ev = Event::Message {
            session: SessionId::from_bytes([5; 16]),
            message: sample_session_message(),
        };
        let bytes = postcard::to_allocvec(&ev).unwrap();
        let back: Event = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn event_peer_joined_round_trip() {
        let ev = Event::PeerJoined {
            session: SessionId::from_bytes([6; 16]),
            peer: sample_peer(),
        };
        let bytes = postcard::to_allocvec(&ev).unwrap();
        let back: Event = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(ev, back);
    }

    // ---- WireMessage envelope ----

    #[test]
    fn wire_message_request_round_trip() {
        let frame = WireMessage::Request {
            id: RequestId::new(1),
            request: Request::Hello {
                client_version: PROTOCOL_VERSION,
            },
        };
        let bytes = postcard::to_allocvec(&frame).unwrap();
        let back: WireMessage = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(frame, back);
    }

    #[test]
    fn wire_message_response_correlates_id() {
        let id = RequestId::new(123);
        let frame = WireMessage::Response {
            id,
            response: Response::Sent {
                session: SessionId::from_bytes([0; 16]),
                seq: Seq::new(7),
            },
        };
        let bytes = postcard::to_allocvec(&frame).unwrap();
        let back: WireMessage = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(frame, back);
        if let WireMessage::Response { id: back_id, .. } = back {
            assert_eq!(back_id, id);
        } else {
            panic!("expected Response");
        }
    }

    #[test]
    fn wire_message_event_has_no_id() {
        let frame = WireMessage::Event {
            event: Event::SessionClosed {
                session: SessionId::from_bytes([0; 16]),
            },
        };
        let bytes = postcard::to_allocvec(&frame).unwrap();
        let back: WireMessage = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(frame, back);
    }

    #[test]
    fn wire_message_unknown_variant_rejected() {
        let bad = "{\"made_up\":{}}";
        let result: Result<WireMessage, _> = serde_json::from_str(bad);
        assert!(result.is_err());
    }

    // ---- Property-based ----

    fn arb_send_payload() -> impl Strategy<Value = SendPayload> {
        (
            prop_oneof![
                Just(MessageKind::Chat),
                Just(MessageKind::Tool),
                Just(MessageKind::System),
            ],
            "[\\PC]{0,32}",
            proptest::collection::vec(any::<u8>(), 0..256),
        )
            .prop_map(|(kind, action, payload)| SendPayload {
                kind,
                action,
                payload,
            })
    }

    fn arb_peer() -> impl Strategy<Value = PeerInfo> {
        (any::<[u8; 32]>(), "[\\PC]{0,32}")
            .prop_map(|(id, name)| PeerInfo::new(PeerId::from_bytes(id), name))
    }

    fn arb_request() -> impl Strategy<Value = Request> {
        prop_oneof![
            any::<u32>().prop_map(|v| Request::Hello {
                client_version: ProtocolVersion::new(v),
            }),
            (arb_peer(), proptest::option::of(any::<[u8; 16]>())).prop_map(|(peer, s)| {
                Request::HostSession {
                    peer,
                    session: s.map(SessionId::from_bytes),
                }
            }),
            (arb_peer(), "[\\PC]{0,128}").prop_map(|(peer, ticket)| Request::JoinSession {
                peer,
                ticket: JoinTicket::from(ticket),
            }),
            Just(Request::ListSessions),
            (any::<[u8; 16]>(), proptest::option::of(any::<u64>())).prop_map(|(s, since)| {
                Request::Subscribe {
                    session: SessionId::from_bytes(s),
                    since: since.map(Seq::new),
                }
            }),
            (any::<[u8; 16]>(), arb_send_payload()).prop_map(|(s, payload)| Request::Send {
                session: SessionId::from_bytes(s),
                payload,
            }),
            any::<[u8; 16]>().prop_map(|s| Request::LeaveSession {
                session: SessionId::from_bytes(s),
            }),
        ]
    }

    proptest! {
        #[test]
        fn request_round_trip(req in arb_request()) {
            let bytes = postcard::to_allocvec(&req).unwrap();
            let back: Request = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(req, back);
        }

        #[test]
        fn request_json_round_trip(req in arb_request()) {
            let json = serde_json::to_string(&req).unwrap();
            let back: Request = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(req, back);
        }

        #[test]
        fn request_id_postcard_round_trip(v in any::<u64>()) {
            let id = RequestId::new(v);
            let bytes = postcard::to_allocvec(&id).unwrap();
            let back: RequestId = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(id, back);
        }

        #[test]
        fn wire_message_round_trip(req in arb_request(), id in any::<u64>()) {
            let frame = WireMessage::Request {
                id: RequestId::new(id),
                request: req,
            };
            let bytes = postcard::to_allocvec(&frame).unwrap();
            let back: WireMessage = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(frame, back);
        }

        #[test]
        fn request_id_next_is_monotonic(v in 0u64..u64::MAX) {
            let id = RequestId::new(v);
            let next = id.next().unwrap();
            prop_assert!(next > id);
            prop_assert_eq!(next.get(), v + 1);
        }
    }
}
