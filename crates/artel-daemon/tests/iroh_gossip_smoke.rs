//! Phase 2c-1 smoke test: two in-process daemons exchange a payload
//! over iroh-gossip on a shared topic.
//!
//! Goal: prove the gossip wiring inside [`artel_daemon::Daemon`] is
//! correctly stitched together — `Endpoint`, `Gossip`, and `Router`
//! share state, an inbound connection on the gossip ALPN reaches the
//! `Gossip` instance, and broadcasts on a topic round-trip between
//! two endpoints. **No `Registry` traffic** is involved here; that
//! plumbing arrives in 2c-2.
//!
//! Discovery runs against a localhost
//! [`iroh::test_utils::DnsPkarrServer`] shared by both daemons —
//! same code path as production, just pointing at a localhost
//! pkarr+DNS pair instead of n0's. Deterministic and fast.

#![cfg(feature = "iroh")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig, EndpointSetup};
use artel_protocol::PeerId;
use bytes::Bytes;
use futures_util::StreamExt;
use iroh::test_utils::DnsPkarrServer;
use iroh_gossip::api::Event;
use iroh_gossip::proto::TopicId;
use tempfile::TempDir;
use tokio::time::timeout;

const FALLBACK_PEER: PeerId = PeerId::from_bytes([0xee; 32]);

/// Small bag of paths a test daemon needs.
struct State {
    _root: TempDir,
    socket: PathBuf,
    pid: PathBuf,
    sessions: PathBuf,
    iroh_key: PathBuf,
}

fn fresh_state() -> State {
    let root = TempDir::new().unwrap();
    State {
        socket: root.path().join("daemon.sock"),
        pid: root.path().join("daemon.pid"),
        sessions: root.path().join("sessions"),
        iroh_key: root.path().join("iroh.key"),
        _root: root,
    }
}

async fn spawn_daemon(state: &State, dns_pkarr: &Arc<DnsPkarrServer>) -> RunningDaemon {
    let daemon = Daemon::start(DaemonConfig {
        socket_path: state.socket.clone(),
        pid_path: state.pid.clone(),
        sessions_dir: state.sessions.clone(),
        daemon_peer_id: FALLBACK_PEER,
        iroh_key_path: Some(state.iroh_key.clone()),
        endpoint_setup: EndpointSetup::Testing {
            dns_pkarr: Arc::clone(dns_pkarr),
        },
    })
    .await
    .expect("daemon start");

    let runtime = daemon.iroh().expect("iroh runtime").clone();
    let shutdown = daemon.shutdown_handle();
    let join = tokio::spawn(daemon.run());
    RunningDaemon {
        runtime,
        shutdown,
        join,
    }
}

struct RunningDaemon {
    runtime: artel_daemon::IrohRuntime,
    shutdown: Arc<Shutdown>,
    join: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl RunningDaemon {
    async fn stop(self) {
        self.shutdown.trigger();
        timeout(Duration::from_secs(10), self.join)
            .await
            .expect("daemon did not exit within 10s")
            .expect("daemon panicked")
            .expect("daemon io");
    }
}

#[tokio::test]
async fn two_daemons_exchange_a_payload_over_gossip() {
    let dns_pkarr = Arc::new(DnsPkarrServer::run().await.expect("dns_pkarr"));
    let state_a = fresh_state();
    let state_b = fresh_state();

    let daemon_a = spawn_daemon(&state_a, &dns_pkarr).await;
    let daemon_b = spawn_daemon(&state_b, &dns_pkarr).await;

    let addr_a = daemon_a.runtime.endpoint.addr();
    let addr_b = daemon_b.runtime.endpoint.addr();
    let id_a = addr_a.id;
    let id_b = addr_b.id;
    assert_ne!(id_a, id_b, "both daemons must have distinct identities");

    // Wait until both daemons' pkarr records are queryable on the
    // shared localhost server. Without this, the joiner's first
    // dial can race the publisher's announce loop. The timeout
    // budget is generous; a real failure here is a publish loop
    // bug, not a network race.
    dns_pkarr
        .on_endpoint(&id_a, Duration::from_secs(10))
        .await
        .expect("daemon A pkarr-published");
    dns_pkarr
        .on_endpoint(&id_b, Duration::from_secs(10))
        .await
        .expect("daemon B pkarr-published");

    // Both subscribe to the same topic. A is the "host" (empty
    // bootstrap); B bootstraps from A.
    let topic_id = TopicId::from_bytes([0x42; 32]);

    let topic_a = daemon_a
        .runtime
        .gossip
        .subscribe(topic_id, vec![])
        .await
        .expect("A subscribes");
    let topic_b = daemon_b
        .runtime
        .gossip
        .subscribe(topic_id, vec![id_a])
        .await
        .expect("B subscribes");

    let (sender_a, mut receiver_a) = topic_a.split();
    let (sender_b, mut receiver_b) = topic_b.split();
    drop(sender_a); // unused — A only receives in this test

    // Wait for the join to settle on B's side. `joined()` resolves
    // once at least one neighbor has sent a NeighborUp.
    timeout(Duration::from_secs(15), receiver_b.joined())
        .await
        .expect("B never joined the topic")
        .expect("B joined() errored");

    // B broadcasts; A should observe it as Event::Received.
    let payload = Bytes::from_static(b"hello from B");
    sender_b
        .broadcast(payload.clone())
        .await
        .expect("B broadcast");

    let observed = timeout(Duration::from_secs(15), async {
        loop {
            match receiver_a.next().await {
                Some(Ok(Event::Received(msg))) => return msg.content,
                Some(Ok(Event::NeighborUp(_) | Event::NeighborDown(_) | Event::Lagged)) => {}
                Some(Err(err)) => panic!("A receiver error: {err}"),
                None => panic!("A stream ended before payload"),
            }
        }
    })
    .await
    .expect("A never observed B's broadcast");

    assert_eq!(observed.as_ref(), payload.as_ref());

    // Drop the topic handles before shutting the daemons down so the
    // gossip task doesn't fight us during Router::shutdown.
    drop(receiver_a);
    drop(sender_b);
    drop(receiver_b);

    daemon_a.stop().await;
    daemon_b.stop().await;
}
