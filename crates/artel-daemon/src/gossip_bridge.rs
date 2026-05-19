//! Bridge between [`crate::session::Registry`] and the iroh gossip
//! substrate.
//!
//! The bridge owns nothing the registry does — it doesn't store
//! sessions, doesn't sequence messages. Its only job is to plumb
//! bytes between two places:
//!
//! - **Host side.** When [`crate::session::Registry::send`] commits
//!   a [`SessionMessage`], the bridge wraps it in a
//!   [`GossipFrame::Message`](`artel_protocol::gossip::GossipBody::Message`)
//!   and broadcasts it on the session's gossip topic.
//! - **Joiner side.** When the registry detects a remote ticket
//!   (`host_peer_id` ≠ self), the bridge subscribes to the topic,
//!   spawns a forwarder task that decodes inbound frames and feeds
//!   them into the local `Session`'s `events_tx` and `log` so the
//!   joiner's IPC subscribers see the host's messages.
//!
//! Phase 2c-2b ships **host → joiner only**: a joiner's `Send`
//! returns [`SessionError::NotSupported`]. The reverse path needs
//! request/reply correlation across gossip and is deferred to 2c-2c.
//!
//! [`SessionError::NotSupported`]: crate::session::SessionError::NotHost

#![allow(clippy::redundant_pub_crate)]
//!
//! ## Topic id
//!
//! Derived deterministically from the session id — the first 16
//! bytes of the topic's 32-byte id are the session UUID, the last
//! 16 bytes are zeros. That keeps the ticket compact (no extra
//! topic field) and means a session that gets re-hosted from the
//! same id lands on the same topic by construction.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use artel_protocol::gossip::{self, GossipBody};
use artel_protocol::{PeerId, SessionId, SessionMessage};
use bytes::Bytes;
use futures_util::StreamExt;
use iroh::EndpointAddr;
use iroh::address_lookup::memory::MemoryLookup;
use iroh_gossip::api::Event as IrohGossipEvent;
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{debug, warn};

/// Bounded wait for the gossip mesh to form on the joiner side. If
/// no `NeighborUp` arrives in this window the join fails — a longer
/// fallback would just hide a real connectivity problem.
const JOIN_READY_TIMEOUT: Duration = Duration::from_secs(15);

/// Inter-daemon plumbing. One instance per daemon, shared across
/// every session it joins or hosts.
#[derive(Debug)]
pub(crate) struct GossipBridge {
    gossip: Gossip,
    /// Address book the daemon was built with, when present. Joiners
    /// inject the host's [`iroh::EndpointAddr`] here so the gossip
    /// dial finds it. `None` means we're relying on iroh's default
    /// discovery.
    address_lookup: Option<MemoryLookup>,
    /// Live topic handles, keyed by session id. We keep the sender
    /// alive for the lifetime of the session so broadcasts work; the
    /// receiver is owned by a forwarder task whose `JoinHandle` lives
    /// here too.
    sessions: Mutex<HashMap<SessionId, SessionState>>,
}

#[derive(Debug)]
#[allow(dead_code)] // forwarder JoinHandle is only consumed by Drop.
struct SessionState {
    sender: iroh_gossip::api::GossipSender,
    forwarder: tokio::task::JoinHandle<()>,
}

/// Map a [`SessionId`] to the gossip [`TopicId`] used for its traffic.
fn topic_for(session: SessionId) -> TopicId {
    let mut bytes = [0u8; 32];
    bytes[..16].copy_from_slice(session.as_bytes());
    TopicId::from_bytes(bytes)
}

impl GossipBridge {
    /// Construct a bridge wrapping `gossip`. `address_lookup` is the
    /// same handle the daemon's [`iroh::Endpoint`] was built with;
    /// joiners reach into it to seed the host's addr before
    /// subscribing.
    pub(crate) fn new(gossip: Gossip, address_lookup: Option<MemoryLookup>) -> Self {
        Self {
            gossip,
            address_lookup,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Subscribe to a session's gossip topic as **host**. No
    /// bootstrap peers — we wait for joiners to find us. Returns
    /// once the topic handle is registered; broadcasts via
    /// [`Self::publish_message`] start working immediately, although
    /// they reach no-one until at least one joiner connects.
    pub(crate) async fn host_session(&self, session: SessionId) -> Result<(), BridgeError> {
        self.subscribe_inner(session, vec![], None).await
    }

    /// Subscribe to a session's gossip topic as **joiner**. Seeds
    /// the daemon's address book with `host_addr` so the gossip
    /// dial succeeds, then subscribes with `host_peer` as the
    /// bootstrap. Spawns a forwarder task that decodes inbound
    /// gossip frames and pushes the resulting [`SessionMessage`]s
    /// into `on_message`.
    pub(crate) async fn join_session(
        &self,
        session: SessionId,
        host_peer: PeerId,
        host_addr: EndpointAddr,
        on_message: impl Fn(SessionMessage) + Send + Sync + 'static,
    ) -> Result<(), BridgeError> {
        if let Some(lookup) = &self.address_lookup {
            lookup.add_endpoint_info(host_addr);
        }
        let host_endpoint_id = iroh::EndpointId::from_bytes(host_peer.as_bytes())
            .map_err(|e| BridgeError::Iroh(format!("bad host peer id: {e}")))?;
        self.subscribe_inner(session, vec![host_endpoint_id], Some(Arc::new(on_message)))
            .await
    }

    /// Common subscribe path. When `on_message` is `Some`, the
    /// forwarder is wired up; when `None`, we just hold the topic
    /// open (host case — the host fans out via `publish_message`).
    ///
    /// Joiners (those with a non-empty bootstrap) additionally wait
    /// for [`GossipReceiver::joined`] before returning so the gossip
    /// mesh is actually wired up by the time `JoinSession` reports
    /// success. Without this, a host that calls `publish_message`
    /// immediately after the joiner's IPC handshake completes can
    /// broadcast into a topic that has no neighbors yet, and the
    /// joiner silently misses the message.
    ///
    /// [`GossipReceiver::joined`]: iroh_gossip::api::GossipReceiver::joined
    async fn subscribe_inner(
        &self,
        session: SessionId,
        bootstrap: Vec<iroh::EndpointId>,
        on_message: Option<MessageHandler>,
    ) -> Result<(), BridgeError> {
        let topic_id = topic_for(session);
        let wait_for_neighbor = !bootstrap.is_empty();
        let topic = self
            .gossip
            .subscribe(topic_id, bootstrap)
            .await
            .map_err(|e| BridgeError::Iroh(format!("subscribe: {e}")))?;
        let (sender, mut receiver) = topic.split();
        if wait_for_neighbor {
            timeout(JOIN_READY_TIMEOUT, receiver.joined())
                .await
                .map_err(|_| BridgeError::Iroh("timed out waiting for gossip neighbor".into()))?
                .map_err(|e| BridgeError::Iroh(format!("joined: {e}")))?;
        }
        let forwarder = tokio::spawn(async move {
            // The host case has on_message = None: drain the receiver
            // anyway so the channel doesn't fill up and back-pressure
            // gossip; we just discard the frames. The host's IPC
            // subscribers are served by Registry's local broadcast
            // and don't need re-decoding from the wire.
            while let Some(item) = receiver.next().await {
                match item {
                    Ok(IrohGossipEvent::Received(msg)) => {
                        if let Some(handler) = on_message.as_ref() {
                            match gossip::decode(&msg.content) {
                                Ok(frame) => match frame.body {
                                    GossipBody::Message(m) => handler(m),
                                },
                                Err(err) => {
                                    warn!(?err, "gossip frame decode failed; dropping");
                                }
                            }
                        }
                    }
                    Ok(_) => {} // NeighborUp/Down/Lagged: nothing to forward.
                    Err(err) => {
                        warn!(error = %err, "gossip receiver error; forwarder exiting");
                        break;
                    }
                }
            }
            debug!(?session, "gossip forwarder exited");
        });
        self.sessions
            .lock()
            .await
            .insert(session, SessionState { sender, forwarder });
        Ok(())
    }

    /// Broadcast `msg` on the gossip topic for `session`. Called by
    /// the host side of [`Registry::send`] after a successful
    /// store-write. No-op (with a debug log) if the bridge wasn't
    /// hosting this session — the local fan-out has already
    /// happened, this is just bonus reach.
    pub(crate) async fn publish_message(&self, session: SessionId, msg: SessionMessage) {
        let sender = self
            .sessions
            .lock()
            .await
            .get(&session)
            .map(|s| s.sender.clone());
        let Some(sender) = sender else {
            debug!(
                ?session,
                "publish_message called for session without gossip topic; dropping",
            );
            return;
        };
        let frame = gossip::message_frame(msg);
        let bytes = Bytes::from(gossip::encode(&frame));
        if let Err(err) = sender.broadcast(bytes).await {
            warn!(error = %err, "gossip broadcast failed");
        }
    }
}

type MessageHandler = Arc<dyn Fn(SessionMessage) + Send + Sync + 'static>;

/// Errors the bridge surfaces. Distinct from
/// [`crate::session::SessionError`] so the registry can decide how
/// to fold them in.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BridgeError {
    #[error("iroh: {0}")]
    Iroh(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use artel_protocol::SessionId;

    #[test]
    fn topic_id_is_derived_from_session_uuid() {
        let session = SessionId::from_bytes([0xab; 16]);
        let topic = topic_for(session);
        let bytes = topic.as_bytes();
        assert_eq!(&bytes[..16], session.as_bytes());
        assert!(
            bytes[16..].iter().all(|b| *b == 0),
            "tail must be zeros, got {:?}",
            &bytes[16..],
        );
    }

    #[test]
    fn distinct_sessions_get_distinct_topics() {
        let a = topic_for(SessionId::from_bytes([1; 16]));
        let b = topic_for(SessionId::from_bytes([2; 16]));
        assert_ne!(a.as_bytes(), b.as_bytes());
    }
}
