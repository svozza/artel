//! Replay recovery must gate rotation, and cancelled startup must release its resources.

use super::*;

use artel_protocol::capability::{Capability, CapabilityAction};
use artel_protocol::transport::server::Listener;
use artel_protocol::{PROTOCOL_VERSION, PeerInfo, WireMessage};
use futures_util::SinkExt;
use iroh::test_utils::DnsPkarrServer;
use tempfile::TempDir;
use tokio_util::task::AbortOnDropHandle;

const BUDGET: Duration = Duration::from_secs(10);

fn init_tracing() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                    "info,artel_fs=debug,artel_daemon=debug,artel_client=debug,iroh_docs=debug"
                        .into()
                }),
            )
            .with_test_writer()
            .try_init();
    });
}

async fn discovery() -> Arc<DnsPkarrServer> {
    Arc::new(
        timeout(
            BUDGET,
            DnsPkarrServer::run_with_origin(crate::TEST_DNS_ORIGIN.to_string()),
        )
        .await
        .expect("discovery startup timed out")
        .expect("localhost discovery"),
    )
}

async fn wait_until(
    mut condition: impl FnMut() -> bool,
) -> Result<(), tokio::time::error::Elapsed> {
    timeout(BUDGET, async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
}

/// The first missed revoke must not rotate using the intermediate survivor
/// set: the second peer is also revoked before replay completes.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines, clippy::large_futures)]
async fn recovery_rotation_waits_for_final_replay_projection() {
    init_tracing();
    let dns = discovery().await;
    let daemon_root = TempDir::new().expect("daemon state");
    let workspace_root = TempDir::new().expect("workspace state");
    let (daemon_shutdown, daemon_task) =
        super::redelivery_tests::spawn_daemon(daemon_root.path(), Arc::clone(&dns)).await;
    let client = Client::connect(daemon_root.path().join("daemon.sock"))
        .await
        .expect("host client");
    let (workspace, _events) = timeout(
        BUDGET,
        Workspace::host_with(
            &client,
            "host",
            workspace_root.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            WorkspaceConfig::default()
                .with_endpoint_setup(EndpointSetup::Testing { dns_pkarr: dns }),
        ),
    )
    .await
    .expect("host startup timed out")
    .expect("host workspace");
    let workspace = Arc::new(workspace);
    let run = Arc::clone(&workspace).run().await;
    let (peer_map, docs) = workspace
        .node
        .lock()
        .await
        .as_ref()
        .map(|node| (Arc::clone(&node.peer_map), node.docs.clone()))
        .expect("live node");
    let host = client.daemon_peer_id();
    let first_peer = PeerId::from_bytes([0x71; 32]);
    let second_peer = PeerId::from_bytes([0x72; 32]);
    for peer in [first_peer, second_peer] {
        peer_map.apply_capability(
            host,
            &CapabilityAction::Grant {
                peer,
                cap: Capability::ReadWrite,
            }
            .encode(),
        );
    }
    let author = iroh_docs::Author::from_bytes(&[0x73; 32]);
    let author_id = author.id();
    docs.author_import(author)
        .await
        .expect("import second peer author");
    peer_map.register(
        iroh::EndpointId::from_bytes(author_id.as_bytes()).expect("author endpoint"),
        second_peer,
    );
    workspace
        .doc()
        .set_bytes(
            author_id,
            b"path/revoked.txt".to_vec(),
            Bytes::from_static(b"second peer"),
        )
        .await
        .expect("seed entry under second peer's author");

    peer_map.begin_replay();
    peer_map.apply_capability(
        host,
        &CapabilityAction::Revoke { peer: first_peer }.encode(),
    );
    let epoch_before = workspace.namespace_epoch.load(Ordering::Relaxed);
    let seq = Seq::new(workspace.rotated_revoke_seq.load(Ordering::Relaxed) + 1);
    let waiters_before = peer_map.readiness_waiter_count();
    let rotating_workspace = Arc::clone(&workspace);
    let mut rotation = AbortOnDropHandle::new(tokio::spawn(async move {
        rotating_workspace.handle_host_evict(first_peer, seq).await;
    }));
    // A waiter is positive evidence of reaching the authorization gate.
    // Merely polling Pending could instead stop inside document I/O.
    let reached_gate =
        wait_until(|| peer_map.readiness_waiter_count() > waiters_before || rotation.is_finished())
            .await;
    let held_at_gate = reached_gate.is_ok()
        && !rotation.is_finished()
        && peer_map.readiness_waiter_count() > waiters_before;
    let epoch_while_held = workspace.namespace_epoch.load(Ordering::Relaxed);

    peer_map.apply_capability(
        host,
        &CapabilityAction::Revoke { peer: second_peer }.encode(),
    );
    peer_map.finish_replay();
    let completed = timeout(BUDGET, &mut rotation).await;
    if completed.is_err() {
        rotation.abort();
        let _ = (&mut rotation).await;
    }
    let epoch_after = workspace.namespace_epoch.load(Ordering::Relaxed);
    let retained_revoked_entry = timeout(
        BUDGET,
        workspace
            .doc()
            .get_exact(workspace.author, b"path/revoked.txt", false),
    )
    .await;

    // Clean up before asserting the expected pre-fix failures.
    timeout(BUDGET, workspace.shutdown())
        .await
        .expect("workspace shutdown timed out")
        .expect("workspace shutdown");
    timeout(BUDGET, run)
        .await
        .expect("workspace tasks timed out")
        .expect("workspace tasks");
    drop(client);
    daemon_shutdown.trigger();
    timeout(BUDGET, daemon_task)
        .await
        .expect("daemon shutdown timed out")
        .expect("daemon task")
        .expect("daemon shutdown");

    assert!(held_at_gate, "rotation did not wait for replay readiness");
    assert_eq!(
        epoch_while_held, epoch_before,
        "rotation advanced during replay"
    );
    completed
        .expect("rotation timed out after replay")
        .expect("rotation task");
    assert_eq!(
        epoch_after,
        epoch_before + 1,
        "rotation must complete after replay"
    );
    assert!(
        retained_revoked_entry
            .expect("doc query timed out")
            .expect("doc query")
            .is_none(),
        "rotation carried the second peer's entry forward under the host author",
    );
}

/// A capability message proves the listener is consuming replay. The fake
/// server then withholds its marker forever and observes client cancellation.
async fn serve_held_replay(
    listener: Listener,
    session: SessionId,
    host: PeerId,
    peer: PeerId,
) -> bool {
    let mut stream = listener.accept().await.expect("accept listener client");
    let hello = stream
        .next()
        .await
        .expect("Hello frame")
        .expect("decode Hello");
    let WireMessage::Request {
        id,
        request: Request::Hello { .. },
    } = hello
    else {
        panic!("expected Hello, got {hello:?}");
    };
    stream
        .send(WireMessage::Response {
            id,
            response: Response::Hello {
                daemon_version: PROTOCOL_VERSION,
                daemon_peer_id: host,
            },
        })
        .await
        .expect("Hello response");
    let subscribe = stream
        .next()
        .await
        .expect("Subscribe frame")
        .expect("decode Subscribe");
    let WireMessage::Request {
        id,
        request:
            Request::Subscribe {
                session: requested,
                since: None,
            },
    } = subscribe
    else {
        panic!("expected initial Subscribe, got {subscribe:?}");
    };
    assert_eq!(requested, session);
    stream
        .send(WireMessage::Response {
            id,
            response: Response::Subscribed { session },
        })
        .await
        .expect("Subscribe response");
    let grant = CapabilityAction::Grant {
        peer,
        cap: Capability::ReadWrite,
    };
    stream
        .send(WireMessage::Event {
            event: Event::Message {
                session,
                message: SessionMessage::new(
                    Seq::new(1),
                    1,
                    PeerInfo::new(host, "host"),
                    MessageKind::Capability,
                    grant.action_str(),
                    grant.encode(),
                    artel_protocol::message::SIGNATURE_UNSIGNED,
                    artel_protocol::message::SIGNATURE_UNSIGNED,
                ),
            },
        })
        .await
        .expect("replay grant");
    // Deliberately send no ReplayComplete. EOF must come from cancellation.
    stream.next().await.is_none()
}

#[tokio::test]
async fn cancelling_initial_replay_drops_listener_connection() {
    init_tracing();
    let root = TempDir::new().expect("fake daemon state");
    let socket = root.path().join("daemon.sock");
    let listener = Listener::bind(&socket).await.expect("fake daemon socket");
    let session = SessionId::from_bytes([0x74; 16]);
    let host = PeerId::from_bytes([0x75; 32]);
    let peer = PeerId::from_bytes([0x76; 32]);
    let peer_map = Arc::new(PeerMap::replaying(host));
    let cancel = CancellationToken::new();
    let (events_tx, _events) = mpsc::channel(16);
    let mut server = AbortOnDropHandle::new(tokio::spawn(serve_held_replay(
        listener, session, host, peer,
    )));
    let starting_map = Arc::clone(&peer_map);
    let starting_cancel = cancel.clone();
    let mut startup = AbortOnDropHandle::new(tokio::spawn(async move {
        spawn_cap_listener_from_socket(
            Some(&socket),
            session,
            starting_map,
            starting_cancel,
            None,
            None,
            events_tx,
        )
        .await
    }));
    let applied =
        wait_until(|| peer_map.has_rw(peer) && peer_map.readiness_waiter_count() > 0).await;
    let pending_before_cancel = !startup.is_finished() && !peer_map.is_ready();
    startup.abort();
    let cancelled = timeout(BUDGET, &mut startup).await;
    let gate_closed = timeout(BUDGET, peer_map.wait_ready()).await;
    let disconnected = timeout(BUDGET, &mut server).await;

    // Explicitly stop the detached pre-fix listener before reporting failure.
    cancel.cancel();
    peer_map.close();
    if disconnected.is_err() {
        let _ = timeout(BUDGET, &mut server).await;
    }
    applied.expect("listener never applied the replay grant and reached its readiness wait");
    assert!(
        pending_before_cancel,
        "startup must remain pending without the replay marker"
    );
    assert!(
        cancelled
            .expect("startup cancellation timed out")
            .is_err_and(|error| error.is_cancelled()),
        "startup must be cancelled at its readiness wait",
    );
    assert!(
        !gate_closed.expect("cancelled startup left its projection gate pending"),
        "cancelled startup must close its projection gate",
    );
    assert!(
        disconnected
            .expect("cancelled startup retained the subscription connection")
            .expect("fake daemon task"),
        "expected transport EOF after cancelling startup",
    );
}

#[tokio::test]
async fn dropping_rollback_closes_gate_and_shuts_down_node() {
    init_tracing();
    let root = TempDir::new().expect("node state");
    let peer_map = Arc::new(PeerMap::replaying(PeerId::from_bytes([0x77; 32])));
    let setup = EndpointSetup::Testing {
        dns_pkarr: discovery().await,
    };
    let (events_tx, _events) = mpsc::channel(16);
    let node = timeout(
        BUDGET,
        WorkspaceNode::spawn(root.path(), &setup, Arc::clone(&peer_map), events_tx),
    )
    .await
    .expect("node startup timed out")
    .expect("node startup");
    let endpoint = node.test_endpoint();
    let rollback = WorkspaceRollback {
        listener_abort: None,
        leave_on_rollback: None,
        forget_attachment: None,
        node: Some(node),
    };
    let ready = peer_map.wait_ready();
    tokio::pin!(ready);
    assert!(futures_util::poll!(&mut ready).is_pending());
    drop(rollback);
    let gate_closed = timeout(BUDGET, &mut ready).await;
    // Retain an endpoint clone so ordinary reference dropping cannot close it.
    // Iroh's closure event proves rollback initiated explicit shutdown.
    let node_closed = timeout(BUDGET, endpoint.closed()).await;
    peer_map.close();
    assert!(!gate_closed.expect("rollback left readiness waiters parked"));
    node_closed.expect("rollback did not close the endpoint");
}
