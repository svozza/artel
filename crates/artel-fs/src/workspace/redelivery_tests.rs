//! Re-delivery must distinguish a replayed announce from a fresh reattach.

use super::*;

use artel_daemon::shutdown::Shutdown;
use artel_daemon::{Daemon, DaemonConfig};
use artel_protocol::capability::{Capability, CapabilityAction};
use artel_protocol::upgrade::RotatePayload;
use iroh::test_utils::DnsPkarrServer;
use tempfile::TempDir;

const BUDGET: Duration = Duration::from_secs(10);

pub(super) async fn spawn_daemon(
    root: &Path,
    dns_pkarr: Arc<DnsPkarrServer>,
) -> (Arc<Shutdown>, tokio::task::JoinHandle<io::Result<()>>) {
    let daemon = timeout(
        BUDGET,
        Daemon::start(DaemonConfig {
            socket_path: root.join("daemon.sock"),
            pid_path: root.join("daemon.pid"),
            sessions_dir: root.join("sessions"),
            iroh_key_path: Some(root.join("iroh.key")),
            endpoint_setup: artel_daemon::EndpointSetup::Testing { dns_pkarr },
        }),
    )
    .await
    .expect("daemon startup timed out")
    .expect("daemon startup");
    (daemon.shutdown_handle(), tokio::spawn(daemon.run()))
}

fn signed_announce(
    session: SessionId,
    seq: u64,
    peer: PeerId,
    key: &iroh::SecretKey,
) -> SessionMessage {
    let workspace_id = key.public();
    let payload = NodeIdAnnouncePayload {
        workspace_id: *workspace_id.as_bytes(),
        signature: key
            .sign(&node_id_announce_signed_bytes(session, peer, workspace_id))
            .to_bytes(),
    };
    SessionMessage::new(
        Seq::new(seq),
        1,
        artel_protocol::PeerInfo::new(peer, "bob"),
        MessageKind::System,
        NODE_ID_ACTION,
        postcard::to_allocvec(&payload).expect("encode announce"),
        artel_protocol::message::SIGNATURE_UNSIGNED,
        artel_protocol::message::SIGNATURE_UNSIGNED,
    )
}

async fn receive_rotate(
    events: &mut artel_client::EventStream,
    session: SessionId,
    peer: PeerId,
) -> RotatePayload {
    loop {
        let event = events.recv().await.expect("observer stream closed");
        if let Event::Message {
            session: event_session,
            message,
        } = event
            && event_session == session
            && message.kind == MessageKind::System
            && message.action == artel_protocol::ROTATE_ACTION
        {
            let payload: RotatePayload =
                postcard::from_bytes(&message.payload).expect("decode rotate");
            assert_eq!(payload.target_peer, peer);
            assert_eq!(payload.namespace_epoch, 1);
            return payload;
        }
    }
}

/// A daemon ACKs a live-only delivery before any workspace consumes it.
/// A fresh announce at the same namespace epoch must therefore re-deliver,
/// even when a historical announce already claimed that epoch.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn fresh_announce_redelivers_after_replay_before_listener() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,artel_fs=debug,artel_daemon=debug,iroh=debug".into()),
        )
        .with_test_writer()
        .try_init();

    let dns_pkarr = Arc::new(
        DnsPkarrServer::run_with_origin(crate::TEST_DNS_ORIGIN.to_string())
            .await
            .expect("localhost discovery"),
    );
    let alice_root = TempDir::new().expect("alice state");
    let bob_root = TempDir::new().expect("bob state");
    let (alice_shutdown, alice_task) =
        spawn_daemon(alice_root.path(), Arc::clone(&dns_pkarr)).await;
    let (bob_shutdown, bob_task) = spawn_daemon(bob_root.path(), Arc::clone(&dns_pkarr)).await;

    // Keep the expected regression timeout outside assertions so even the
    // failing baseline shuts both real endpoints down before panicking.
    let result = timeout(Duration::from_secs(40), async {
        let alice = Arc::new(
            Client::connect(alice_root.path().join("daemon.sock"))
                .await
                .expect("alice client"),
        );
        let bob_socket = bob_root.path().join("daemon.sock");
        let bob = Client::connect(&bob_socket).await.expect("bob client");
        let host_peer = alice.daemon_peer_id();
        let bob_peer = bob.daemon_peer_id();
        for peer in [host_peer, bob_peer] {
            let endpoint_id = iroh::EndpointId::from_bytes(peer.as_bytes()).expect("endpoint id");
            dns_pkarr
                .on_endpoint(&endpoint_id, BUDGET)
                .await
                .expect("daemon published to localhost discovery");
        }

        let (session, ticket) = match alice
            .request(Request::HostSession {
                display_name: "alice".into(),
                session: None,
            })
            .await
            .expect("host session")
        {
            Response::HostSession {
                session, ticket, ..
            } => (session, ticket),
            other => panic!("unexpected host response: {other:?}"),
        };
        let joined = bob
            .request(Request::JoinSession {
                display_name: "bob".into(),
                ticket,
            })
            .await
            .expect("join session");
        assert!(matches!(joined, Response::JoinSession { .. }));

        let peer_map = Arc::new(PeerMap::new(host_peer));
        peer_map.apply_capability(
            host_peer,
            &CapabilityAction::Grant {
                peer: bob_peer,
                cap: Capability::ReadWrite,
            }
            .encode(),
        );
        let secret = [0x91; 32];
        let write_ticket = DocTicket::new(
            iroh_docs::Capability::Write(iroh_docs::NamespaceSecret::from_bytes(&secret)),
            vec![],
        )
        .to_string();
        let (rotation_tx, _rotation_rx) = mpsc::unbounded_channel();
        let ctx = HostUpgradeCtx {
            client: alice,
            session,
            namespace_secret: Arc::new(std::sync::Mutex::new(secret)),
            current_write_ticket: Arc::new(std::sync::Mutex::new((write_ticket.clone(), 1))),
            redelivered_announces: Arc::new(
                std::sync::Mutex::new(std::collections::HashMap::new()),
            ),
            rotation_tx,
        };
        let key = iroh::SecretKey::from_bytes(&[0x92; 32]);

        // This observer stands in for the daemon's live delivery while no
        // workspace cap-listener exists. It never imports either payload.
        let (observer, mut events) = cap_resubscribe(&bob_socket, session, None)
            .await
            .expect("first observer");
        handle_node_id_message(
            session,
            &signed_announce(session, 10, bob_peer, &key),
            &peer_map,
            Some(&ctx),
        );
        let first = timeout(BUDGET, receive_rotate(&mut events, session, bob_peer))
            .await
            .map_err(|_| "replayed announce never delivered the first rotate")?;
        assert_eq!(first.doc_ticket, write_ticket);

        // Seeing Rotate proves the first claim and both sequential RPCs
        // reached Bob before the replacement listener can subscribe.
        // Subscribe cannot replay this event: Rotate is live-only.
        drop(events);
        drop(observer);
        let (_listener, mut events) = cap_resubscribe(&bob_socket, session, None)
            .await
            .expect("replacement listener");
        handle_node_id_message(
            session,
            &signed_announce(session, 11, bob_peer, &key),
            &peer_map,
            Some(&ctx),
        );
        let second = timeout(BUDGET, receive_rotate(&mut events, session, bob_peer))
            .await
            .map_err(|_| "fresh announce was suppressed by the replay's namespace-epoch claim")?;
        assert_eq!(second, first);
        Ok::<(), &str>(())
    })
    .await;

    alice_shutdown.trigger();
    bob_shutdown.trigger();
    for task in [alice_task, bob_task] {
        timeout(BUDGET, task)
            .await
            .expect("daemon shutdown timed out")
            .expect("daemon task")
            .expect("daemon shutdown");
    }
    result
        .expect("redelivery scenario timed out")
        .expect("fresh announce must recover a delivery missed before listener attachment");
}
