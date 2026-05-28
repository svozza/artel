//! Joiners hard-reject legacy `workspace.ticket` payloads.
//!
//! Earlier hosts shipped the `workspace.ticket` payload as a raw
//! `DocTicket::to_string().into_bytes()`; the current shape is a
//! postcard-encoded [`artel_fs::WorkspaceTicketEnvelope`]. The
//! wire-compat decision is to **hard-reject** old-shape payloads
//! rather than silently fall back — silent fallback re-introduces
//! the wrong-dir hazard `AttachPolicy::RequireEmpty` closes.
//!
//! This test bypasses `Workspace::host_with` and broadcasts an
//! old-shape payload directly via `Request::Send`, then asserts the
//! joiner surfaces `WorkspaceError::TicketEnvelope(Malformed)` and
//! does not bulk-export.

mod common;

use common::testing_setup;

use std::time::Duration;

use artel_client::Client;
use artel_fs::error::WorkspaceError;
use artel_fs::ticket::TicketEnvelopeError;
use artel_fs::{AttachPolicy, TICKET_ACTION, Workspace, WorkspaceConfig};
use artel_protocol::{MessageKind, PeerId, PeerInfo, Request, Response, SendPayload};
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread")]
async fn joiner_rejects_old_shape_doc_ticket_payload() {
    let common::Pair {
        daemon_a,
        daemon_b,
        dns_pkarr,
    } = common::spawn_pair().await;

    // Alice hosts the artel session but does NOT stand up a
    // workspace. Instead she broadcasts a raw `DocTicket`-shaped
    // payload via `Request::Send` to mimic a legacy host.
    let alice = Client::connect(&daemon_a.socket).await.unwrap();
    let alice_peer = PeerInfo::new(PeerId::from_bytes([1; 32]), "alice");
    let (session, artel_ticket) = match alice
        .request(Request::HostSession {
            peer: alice_peer.clone(),
            session: None,
        })
        .await
        .unwrap()
    {
        Response::HostSession { session, ticket } => (session, ticket),
        other => panic!("HostSession: got {other:?}"),
    };

    // The exact bytes don't matter — only that they're not a
    // postcard-encoded `WorkspaceTicketEnvelope`. Use a base32-ish
    // string to reflect the historical `DocTicket::to_string()` shape.
    let old_shape_payload = b"docaaa\
        cbbcaa3aacaaaaaaaaaaiiabaaaaaiabarbjzgaaaaaaaaaaaaaaaaaaaaaa"
        .to_vec();

    match alice
        .request(Request::Send {
            session,
            payload: SendPayload {
                kind: MessageKind::System,
                action: TICKET_ACTION.to_string(),
                payload: old_shape_payload,
            },
        })
        .await
        .unwrap()
    {
        Response::Sent { .. } => {}
        other => panic!("Send: got {other:?}"),
    }

    // Bob joins the artel session, then calls `Workspace::join_with`.
    // The replayed `workspace.ticket` payload fails envelope decode.
    let bob = Client::connect(&daemon_b.socket).await.unwrap();
    let bob_peer = PeerInfo::new(PeerId::from_bytes([2; 32]), "bob");
    let resp = bob
        .request(Request::JoinSession {
            peer: bob_peer,
            ticket: artel_ticket,
        })
        .await
        .unwrap();
    assert!(matches!(resp, Response::JoinSession { .. }), "{resp:?}");

    let bob_dir = tempfile::tempdir().unwrap();

    let result = timeout(
        Duration::from_secs(15),
        Workspace::join_with(
            &bob,
            session,
            bob_dir.path().to_path_buf(),
            AttachPolicy::RequireEmpty,
            WorkspaceConfig::default().with_endpoint_setup(testing_setup(&dns_pkarr)),
        ),
    )
    .await
    .expect("Workspace::join_with should not hang on a malformed ticket");

    let err = result.expect_err("join must fail on old-shape payload");
    match err {
        WorkspaceError::TicketEnvelope(TicketEnvelopeError::Malformed(_)) => {}
        other => panic!("expected TicketEnvelope(Malformed), got {other:?}"),
    }

    // Defence in depth: nothing should have been written to bob_dir
    // beyond the state dir the workspace would normally create. The
    // `RequireEmpty` policy already runs *before* the envelope decode
    // (and would have caught a non-empty pre-test dir); here we
    // simply confirm bulk-export never landed any user file.
    let mut entries = tokio::fs::read_dir(bob_dir.path()).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let name = entry.file_name();
        let s = name.to_string_lossy();
        assert!(
            s == ".artel-fs",
            "unexpected entry in bob_dir after failed join: {s}",
        );
    }

    drop(alice);
    drop(bob);
    daemon_a.stop().await;
    daemon_b.stop().await;
}
