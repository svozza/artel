//! Real-daemon restart regression with the cap-listener's IPC backfill gated.
//! The proxy forwards genuine protocol frames; it neither supplies capabilities
//! nor implements the readiness projection being tested.

mod common;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use artel_client::Client;
use artel_fs::{
    AttachPolicy, Direction, NODE_ID_ACTION, Workspace, WorkspaceConfig, WorkspaceEvent,
};
use artel_protocol::capability::{Capability, CapabilityAction};
use artel_protocol::transport::{self, server::Listener};
use artel_protocol::{
    Event, MessageKind, PeerId, ROTATE_ACTION, Request, Response, SessionId, UPGRADE_ACTION,
    WireMessage,
};
use futures_util::{SinkExt, StreamExt};
use iroh_docs::engine::LiveEvent;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const PHASE_BUDGET: Duration = Duration::from_secs(20);
// A negative observation, only while a semaphore holds a specific real frame.
// This never releases a gate or substitutes for a positive readiness signal.
const HELD_BUDGET: Duration = Duration::from_millis(300);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Messages,
    Revoke,
    Complete,
}

struct Gates {
    messages: Semaphore,
    revoke: Semaphore,
    complete: Semaphore,
    ready: AtomicBool,
    saw_mapping: AtomicBool,
    saw_grant: AtomicBool,
    reached: mpsc::UnboundedSender<Stage>,
}

impl Gates {
    async fn hold(&self, stage: Stage) {
        let _ = self.reached.send(stage);
        let gate = match stage {
            Stage::Messages => &self.messages,
            Stage::Revoke => &self.revoke,
            Stage::Complete => &self.complete,
        };
        if let Ok(permit) = gate.acquire().await {
            permit.forget();
        }
    }

    fn release_all(&self) {
        self.messages.close();
        self.revoke.close();
        self.complete.close();
    }
}

type Observations = Arc<Mutex<Vec<String>>>;

struct ReplayProxy {
    socket: PathBuf,
    gates: Arc<Gates>,
    reached: mpsc::UnboundedReceiver<Stage>,
    observations: Observations,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    _directory: tempfile::TempDir,
}

impl ReplayProxy {
    async fn start(upstream: PathBuf, session: SessionId, bob: PeerId) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("replay.sock");
        let listener = Listener::bind(socket.clone()).await.unwrap();
        let (reached_tx, reached) = mpsc::unbounded_channel();
        let gates = Arc::new(Gates {
            messages: Semaphore::new(0),
            revoke: Semaphore::new(0),
            complete: Semaphore::new(0),
            ready: AtomicBool::new(false),
            saw_mapping: AtomicBool::new(false),
            saw_grant: AtomicBool::new(false),
            reached: reached_tx,
        });
        let observations = Observations::default();
        let cancel = CancellationToken::new();
        let task = {
            let gates = gates.clone();
            let observations = observations.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move {
                let mut connections = JoinSet::new();
                loop {
                    tokio::select! {
                        () = cancel.cancelled() => break,
                        connection = listener.accept() => {
                            let downstream = connection.expect("proxy accept");
                            let upstream = transport::client::connect(&upstream)
                                .await.expect("proxy upstream");
                            let gates = gates.clone();
                            let observations = observations.clone();
                            connections.spawn(async move {
                                forward_connection(
                                    downstream, upstream, session, bob, gates, observations,
                                ).await;
                            });
                        }
                        result = connections.join_next(), if !connections.is_empty() => {
                            result.expect("connection task").expect("proxy connection panicked");
                        }
                    }
                }
                connections.abort_all();
                while connections.join_next().await.is_some() {}
            })
        };
        Self {
            socket,
            gates,
            reached,
            observations,
            cancel,
            task,
            _directory: directory,
        }
    }

    async fn stop(self) {
        self.gates.release_all();
        self.cancel.cancel();
        self.task.await.expect("proxy task");
    }
}

async fn forward_connection(
    downstream: transport::Framed<tokio::net::UnixStream>,
    upstream: transport::Framed<tokio::net::UnixStream>,
    session: SessionId,
    bob: PeerId,
    gates: Arc<Gates>,
    observations: Observations,
) {
    let (downstream_tx, mut downstream_rx) = downstream.split();
    let downstream_tx = tokio::sync::Mutex::new(downstream_tx);
    let (mut upstream_tx, mut upstream_rx) = upstream.split();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let requests = async {
        while let Some(frame) = downstream_rx.next().await {
            let frame = frame.expect("decode client frame");
            if let WireMessage::Request { request, .. } = &frame {
                let violation = match request {
                    Request::DeliverUpgrade { target_peer, .. } if *target_peer == bob => {
                        Some("DeliverUpgrade attempted for revoked Bob")
                    }
                    Request::DeliverRotate { target_peer, .. } if *target_peer == bob => {
                        Some("DeliverRotate attempted for revoked Bob")
                    }
                    Request::PublishWorkspaceTicket { .. }
                        if !gates.ready.load(Ordering::SeqCst) =>
                    {
                        Some("workspace ticket published before ReplayComplete")
                    }
                    _ => None,
                };
                if let Some(violation) = violation {
                    observations.lock().unwrap().push(violation.into());
                }
            }
            if upstream_tx.send(frame).await.is_err() {
                break;
            }
        }
    };
    // A Subscribe response may follow the first replay event on the wire.
    // Keep reading upstream while an event is held, or the proxy itself would
    // block Client::request and make the old constructor appear to await replay.
    let responses = async {
        while let Some(frame) = upstream_rx.next().await {
            let frame = frame.expect("decode daemon frame");
            if matches!(&frame, WireMessage::Event { .. }) {
                if event_tx.send(frame).is_err() {
                    break;
                }
            } else if downstream_tx.lock().await.send(frame).await.is_err() {
                break;
            }
        }
    };
    let events = async {
        let mut first_message = true;
        while let Some(frame) = event_rx.recv().await {
            match &frame {
                WireMessage::Event {
                    event:
                        Event::Message {
                            session: id,
                            message,
                        },
                } if *id == session => {
                    if first_message {
                        first_message = false;
                        gates.hold(Stage::Messages).await;
                    }
                    if message.peer.id == bob && message.action == NODE_ID_ACTION {
                        gates.saw_mapping.store(true, Ordering::SeqCst);
                    }
                    if message.kind == MessageKind::Capability {
                        match CapabilityAction::decode(&message.payload) {
                            Ok(CapabilityAction::Grant {
                                peer,
                                cap: Capability::ReadWrite,
                            }) if peer == bob => {
                                gates.saw_grant.store(true, Ordering::SeqCst);
                            }
                            Ok(CapabilityAction::Revoke { peer }) if peer == bob => {
                                gates.hold(Stage::Revoke).await;
                            }
                            _ => {}
                        }
                    }
                }
                WireMessage::Event {
                    event: Event::ReplayComplete { session: id },
                } if *id == session => {
                    gates.hold(Stage::Complete).await;
                    gates.ready.store(true, Ordering::SeqCst);
                }
                _ => {}
            }
            if downstream_tx.lock().await.send(frame).await.is_err() {
                break;
            }
        }
    };
    tokio::select! {
        () = requests => {}
        () = responses => {}
        () = events => {}
    }
}

async fn bounded<T>(name: &str, future: impl std::future::Future<Output = T>) -> Result<T, String> {
    eprintln!(">>> phase begin: {name}");
    let result = timeout(PHASE_BUDGET, future)
        .await
        .map_err(|_| format!("phase timed out: {name}"))?;
    eprintln!("<<< phase end: {name}");
    Ok(result)
}

/// Both the historical RW grant and the final revoke come from Alice's
/// genuine daemon log. Expected red-test failures are accumulated so the
/// gates can be released and every workspace/daemon shut down before panicking.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines, clippy::large_futures)]
async fn host_restart_waits_for_replay_and_never_redelivers_to_revoked_peer() {
    common::init_tracing();
    let common::Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = common::spawn_pair().await;
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let bob_peer = bob.daemon_peer_id();
    let alice_dir = tempfile::tempdir().unwrap();
    let alice_state = tempfile::tempdir().unwrap();
    let bob_dir = tempfile::tempdir().unwrap();
    let config = WorkspaceConfig::default()
        .with_state_dir(alice_state.path().to_path_buf())
        .with_endpoint_setup(common::testing_setup(&dns_pkarr))
        .with_daemon_socket(daemon_a.socket.clone());
    let (alice_ws, mut alice_events) = bounded(
        "initial host",
        Workspace::host_with(
            &alice,
            "alice",
            alice_dir.path().to_path_buf(),
            AttachPolicy::AllowExisting,
            config,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let session = alice_ws.session_id();
    let alice_id =
        iroh::EndpointId::from_bytes(&alice_ws.test_endpoint_id_bytes().await.unwrap()).unwrap();
    let ticket = alice_ws.join_ticket().unwrap().clone();
    let alice_ws = Arc::new(alice_ws);
    let alice_run = alice_ws.clone().run().await;
    assert!(matches!(
        bob.request(Request::JoinSession {
            display_name: "bob".into(),
            ticket,
        })
        .await
        .unwrap(),
        Response::JoinSession { .. }
    ));
    let (bob_ws, bob_events) = bounded(
        "Bob workspace",
        Workspace::join_with(
            &bob,
            session,
            bob_dir.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            WorkspaceConfig::default()
                .with_endpoint_setup(common::testing_setup(&dns_pkarr))
                .with_daemon_socket(daemon_b.socket.clone()),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    common::drain_ws_events(bob_events);
    let bob_ws = Arc::new(bob_ws);
    let bob_run = bob_ws.clone().run().await;
    let bob_id =
        iroh::EndpointId::from_bytes(&bob_ws.test_endpoint_id_bytes().await.unwrap()).unwrap();
    common::wait_for_endpoint(&dns_pkarr, &bob_id).await;
    common::grant_rw_and_wait(&alice, session, bob_peer, bob_dir.path(), alice_dir.path()).await;
    common::revoke(&alice, session, bob_peer).await;
    common::wait_for_event(
        &mut alice_events,
        PHASE_BUDGET,
        "initial PeerRevoked",
        |event| matches!(event, WorkspaceEvent::PeerRevoked { peer } if *peer == bob_peer),
    )
    .await;
    alice_ws.shutdown().await.unwrap();
    bounded("initial host run stopped", alice_run)
        .await
        .unwrap()
        .unwrap();
    drop(alice_ws);

    // Open a fresh observer after the old host has stopped. Drain its real
    // backfill to the marker before beginning the restart observation.
    let observer = Client::connect(&daemon_b.socket).await.unwrap();
    let mut bob_ipc = observer.take_events().await.unwrap();
    observer
        .request(Request::Subscribe {
            session,
            since: None,
        })
        .await
        .unwrap();
    bounded("Bob observer backfill", async {
        loop {
            match bob_ipc.recv().await.expect("Bob observer open") {
                Event::ReplayComplete { session: id } if id == session => break,
                _ => {}
            }
        }
    })
    .await
    .unwrap();

    let mut proxy = ReplayProxy::start(daemon_a.socket.clone(), session, bob_peer).await;
    let observations = proxy.observations.clone();
    let bob_observations = observations.clone();
    let observer_task = tokio::spawn(async move {
        while let Some(event) = bob_ipc.recv().await {
            if let Event::Message {
                session: id,
                message,
            } = event
                && id == session
                && matches!(message.action.as_str(), UPGRADE_ACTION | ROTATE_ACTION)
            {
                bob_observations
                    .lock()
                    .unwrap()
                    .push(format!("Bob received {}", message.action));
            }
        }
    });
    // Only the returning constructor can publish these bytes.
    let probe_name = "private_after_revocation.txt";
    let probe_key =
        artel_fs::path_to_key(alice_dir.path(), &alice_dir.path().join(probe_name)).unwrap();
    let mut bob_doc_events = bob_ws.doc().subscribe().await.unwrap();
    let observation_start = SystemTime::now();
    let doc_observations = observations.clone();
    let doc_observer = tokio::spawn(async move {
        while let Some(event) = bob_doc_events.next().await {
            let violation = match event {
                Ok(LiveEvent::NeighborUp(peer)) if peer == alice_id => {
                    Some("Bob established a docs neighbor connection to the restarted host")
                }
                Ok(LiveEvent::SyncFinished(sync))
                    if sync.peer == alice_id
                        && sync.started >= observation_start
                        && sync.result.is_ok() =>
                {
                    Some("Bob completed docs sync with the restarted host")
                }
                Ok(LiveEvent::InsertRemote { entry, .. }) if entry.key() == probe_key => {
                    Some("Bob received the private post-revocation doc entry")
                }
                Err(_) => {
                    doc_observations
                        .lock()
                        .unwrap()
                        .push("Bob doc observation stream failed".into());
                    break;
                }
                _ => None,
            };
            if let Some(violation) = violation {
                doc_observations.lock().unwrap().push(violation.into());
            }
        }
    });
    tokio::fs::write(alice_dir.path().join(probe_name), b"must stay private")
        .await
        .unwrap();
    let restarted_client = Arc::new(Client::connect(&proxy.socket).await.unwrap());
    let mut constructor = {
        let client = restarted_client.clone();
        let root = alice_dir.path().to_path_buf();
        let config = WorkspaceConfig::default()
            .with_state_dir(alice_state.path().to_path_buf())
            .with_endpoint_setup(common::testing_setup(&dns_pkarr))
            .with_daemon_socket(proxy.socket.clone());
        tokio::spawn(async move {
            Workspace::host_with(&client, "alice", root, AttachPolicy::AllowExisting, config).await
        })
    };
    let mut restarted = None;
    let mut constructor_consumed = false;
    let mut failures = Vec::new();
    let outcome: Result<(), String> = async {
        for stage in [Stage::Messages, Stage::Revoke, Stage::Complete] {
            let reached = bounded("replay gate reached", proxy.reached.recv()).await?;
            if reached != Some(stage) {
                return Err(format!("expected {stage:?}, got {reached:?}"));
            }
            eprintln!("holding before {stage:?}");
            // Retain an early successful constructor so even the original
            // implementation gets a graceful shutdown after this assertion.
            if restarted.is_none() {
                match timeout(HELD_BUDGET, &mut constructor).await {
                    Err(_) => {}
                    Ok(Ok(Ok(workspace))) => {
                        constructor_consumed = true;
                        failures.push(format!("constructor completed before {stage:?}"));
                        restarted = Some(workspace);
                    }
                    Ok(result) => {
                        constructor_consumed = true;
                        return Err(format!("constructor failed: {result:?}"));
                    }
                }
            }
            if bob_dir.path().join(probe_name).exists() {
                failures.push(format!("private host data reached Bob at {stage:?}"));
            }
            match stage {
                Stage::Messages => proxy.gates.messages.add_permits(1),
                Stage::Revoke => {
                    if !proxy.gates.saw_mapping.load(Ordering::SeqCst)
                        || !proxy.gates.saw_grant.load(Ordering::SeqCst)
                    {
                        return Err("replay did not include Bob's mapping and RW grant".into());
                    }
                    proxy.gates.revoke.add_permits(1);
                }
                Stage::Complete => proxy.gates.complete.add_permits(1),
            }
        }
        if restarted.is_none() {
            let result = bounded("constructor after ReplayComplete", &mut constructor).await?;
            constructor_consumed = true;
            restarted = Some(
                result
                    .map_err(|error| error.to_string())?
                    .map_err(|error| error.to_string())?,
            );
        }
        let (workspace, events) = restarted.as_mut().unwrap();
        bounded(
            "explicit outbound sync",
            workspace
                .doc()
                .start_sync(vec![iroh::EndpointAddr::new(bob_id)]),
        )
        .await?
        .map_err(|error| error.to_string())?;
        bounded("Outgoing block for Bob", async {
            while let Some(event) = events.recv().await {
                if matches!(
                    event,
                    WorkspaceEvent::RevokedPeerBlocked {
                        peer,
                        direction: Direction::Outgoing,
                    } if peer == bob_peer
                ) {
                    return Ok(());
                }
            }
            Err("workspace event stream closed before Outgoing block".to_string())
        })
        .await??;
        Ok(())
    }
    .await;

    // Release even when a phase failed; cancelling host_with mid-construction
    // would lose ownership of the workspace's background tasks.
    proxy.gates.release_all();
    if !constructor_consumed {
        match timeout(PHASE_BUDGET, &mut constructor).await {
            Ok(Ok(Ok(workspace))) => restarted = Some(workspace),
            Ok(result) => failures.push(format!("constructor failed during cleanup: {result:?}")),
            Err(_) => {
                constructor.abort();
                let _ = constructor.await;
                failures.push("constructor could not be recovered for cleanup".into());
            }
        }
    }
    if let Some((workspace, events)) = restarted {
        common::drain_ws_events(events);
        workspace.shutdown().await.unwrap();
    }
    bob_ws.shutdown().await.unwrap();
    bounded("Bob run stopped", bob_run).await.unwrap().unwrap();
    doc_observer.abort();
    let _ = doc_observer.await;
    observer_task.abort();
    let _ = observer_task.await;
    failures.extend(observations.lock().unwrap().iter().cloned());
    drop(observer);
    drop(restarted_client);
    proxy.stop().await;
    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
    if let Err(error) = outcome {
        failures.push(error);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
