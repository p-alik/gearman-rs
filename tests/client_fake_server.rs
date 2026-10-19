//! Client tests against a hand-scripted fake server rather than real
//! `gearmand`, for behavior only an adversarial/non-conforming peer
//! triggers (a real server never exercises these paths).

use std::time::Duration;

use bytes::Bytes;
use gearman::client::{ClientBuilder, ServerSelection};
use gearman::protocol::{Packet, PacketType};
use gearman::Connection;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Reads `OPTION_REQ "exceptions"`, waits until told to reply (so the test
/// can observe whether `connect()` resolves before that reply exists at
/// all), then replies `ERROR` — simulating a server that doesn't support
/// the option — and serves one `SUBMIT_JOB_BG` with a real `JOB_CREATED` so
/// the test can also check that reply isn't misattributed.
async fn fake_server(listener: TcpListener, release_option_res: oneshot::Receiver<()>) {
    let (stream, _) = listener.accept().await.expect("accept");
    let mut conn = Connection::from_stream(stream);

    let option_req = conn
        .recv()
        .await
        .expect("recv should not error")
        .expect("client should send OPTION_REQ first");
    assert_eq!(option_req.ptype, PacketType::OptionReq);
    assert_eq!(option_req.arg_str(0), Some("exceptions"));

    release_option_res
        .await
        .expect("test should release the OPTION_REQ reply");

    conn.send(Packet::response(
        PacketType::Error,
        vec![
            Bytes::from_static(b"unknown_option"),
            Bytes::from_static(b"unknown server option"),
        ],
    ))
    .await
    .expect("send should not error");

    let submit = conn
        .recv()
        .await
        .expect("recv should not error")
        .expect("client should submit after the OPTION_REQ handshake");
    assert_eq!(submit.ptype, PacketType::SubmitJobBg);

    conn.send(Packet::response(
        PacketType::JobCreated,
        vec![Bytes::from_static(b"H:test:1")],
    ))
    .await
    .expect("send should not error");
}

/// Regression test for a FIFO-desync bug: `OPTION_REQ`'s reply used to go
/// completely unwaited-for — `connected` flipped `true` (making `connect()`
/// resolve) right after the request was *sent*, without reading any reply
/// at all. That left the reply to surface later inside `actor_loop`, where
/// it could be popped off `pending_replies` and misattributed to a totally
/// unrelated, already-queued request. The reply is now consumed by a
/// dedicated handshake step before the connection is considered healthy, so
/// (a) `connect()` must not resolve while the server withholds that reply,
/// and (b) a submit issued right after connecting must still get its own
/// real `JOB_CREATED`, not the earlier `ERROR`.
#[tokio::test]
async fn connect_waits_for_option_req_reply_and_does_not_desync_replies() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind a port");
    let addr = listener.local_addr().unwrap().to_string();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(fake_server(listener, release_rx));

    let mut connect_fut = std::pin::pin!(ClientBuilder::new()
        .servers([addr])
        .with_exceptions(true)
        .connect());

    // While the server withholds its OPTION_REQ reply, connect() must not
    // resolve — the old code reported the connection healthy as soon as
    // the request was written, without waiting for any reply.
    tokio::select! {
        _ = &mut connect_fut => {
            panic!("connect() resolved before the OPTION_REQ reply was even sent");
        }
        _ = tokio::time::sleep(Duration::from_millis(300)) => {}
    }

    release_tx
        .send(())
        .expect("fake server should still be waiting");

    let client = tokio::time::timeout(Duration::from_secs(5), connect_fut)
        .await
        .expect("connect() should resolve promptly once the reply arrives")
        .expect("client should connect even though the server rejects OPTION_REQ");

    let handle = tokio::time::timeout(Duration::from_secs(5), client.submit_bg("echo", "payload"))
        .await
        .expect("submit_bg should not hang")
        .expect("submit_bg should get its own JOB_CREATED reply, not the OPTION_REQ error");

    assert_eq!(handle.as_str(), "H:test:1");

    server.await.expect("fake server task should not panic");
}

/// Answers two foreground `SUBMIT_JOB`s with the *same* handle, as gearmand
/// does when it coalesces submissions sharing a unique id, then sends one
/// `WORK_DATA` and one `WORK_COMPLETE` for that handle.
async fn fake_server_coalescing(listener: TcpListener) {
    let (stream, _) = listener.accept().await.expect("accept");
    let mut conn = Connection::from_stream(stream);

    for _ in 0..2 {
        let submit = conn
            .recv()
            .await
            .expect("recv should not error")
            .expect("client should submit twice");
        assert_eq!(submit.ptype, PacketType::SubmitJob);
        assert_eq!(submit.arg_str(1), Some("same-unique"));
        conn.send(Packet::response(
            PacketType::JobCreated,
            vec![Bytes::from_static(b"H:test:1")],
        ))
        .await
        .expect("send should not error");
    }

    conn.send(Packet::response(
        PacketType::WorkData,
        vec![
            Bytes::from_static(b"H:test:1"),
            Bytes::from_static(b"chunk"),
        ],
    ))
    .await
    .expect("send should not error");
    conn.send(Packet::response(
        PacketType::WorkComplete,
        vec![Bytes::from_static(b"H:test:1"), Bytes::from_static(b"done")],
    ))
    .await
    .expect("send should not error");

    // Keep the connection open until the client has read everything.
    let _ = conn.recv().await;
}

/// Regression test: the client kept one event sender per handle, so when
/// two overlapping foreground submissions got the same (coalesced) handle,
/// the second replaced the first, and the first caller saw its stream end
/// without a result. Both callers must now receive every event.
#[tokio::test]
async fn coalesced_foreground_submissions_all_receive_events() {
    use futures::StreamExt;
    use gearman::client::{JobEvent, SubmitOptions};

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind a port");
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(fake_server_coalescing(listener));

    let client = ClientBuilder::new()
        .servers([addr])
        .connect()
        .await
        .expect("client should connect");

    let mut streams = Vec::new();
    for _ in 0..2 {
        let submitted = client
            .submit(
                "reverse",
                Some("same-unique"),
                Bytes::from_static(b"payload"),
                SubmitOptions::foreground(),
            )
            .await
            .expect("submit should succeed");
        assert_eq!(submitted.handle.as_str(), "H:test:1");
        streams.push(submitted.events.expect("foreground job has events"));
    }

    for mut events in streams {
        let collected = tokio::time::timeout(Duration::from_secs(5), async {
            let mut collected = Vec::new();
            while let Some(event) = events.next().await {
                collected.push(event);
            }
            collected
        })
        .await
        .expect("event stream must end after WORK_COMPLETE");
        assert!(
            matches!(
                collected.as_slice(),
                [JobEvent::Data(d), JobEvent::Complete(c)] if d == "chunk" && c == "done"
            ),
            "unexpected events: {collected:?}"
        );
    }
}

/// Completes the `OPTION_REQ "exceptions"` handshake once `release`
/// fires (immediately when `None`), then holds the connection open.
async fn fake_server_handshake(listener: TcpListener, release: Option<oneshot::Receiver<()>>) {
    let (stream, _) = listener.accept().await.expect("accept");
    let mut conn = Connection::from_stream(stream);
    let option_req = conn
        .recv()
        .await
        .expect("recv should not error")
        .expect("client should send OPTION_REQ first");
    assert_eq!(option_req.ptype, PacketType::OptionReq);
    if let Some(release) = release {
        release.await.expect("test should release the handshake");
    }
    conn.send(Packet::response(
        PacketType::OptionRes,
        vec![Bytes::from_static(b"exceptions")],
    ))
    .await
    .expect("send should not error");
    while let Ok(Some(_)) = conn.recv().await {}
}

/// Regression test: connect() returned as soon as any server was up, so
/// under ByUnique a server still in its handshake looked down and its
/// uniques went to the fail-over server - and, once it came up, to it,
/// creating a second job for the same unique id. connect() must now wait
/// for every server's first attempt to finish.
#[tokio::test]
async fn by_unique_connect_waits_for_every_server_first_attempt() {
    let fast = TcpListener::bind("127.0.0.1:0").await.expect("bind a port");
    let slow = TcpListener::bind("127.0.0.1:0").await.expect("bind a port");
    let addrs = [
        fast.local_addr().unwrap().to_string(),
        slow.local_addr().unwrap().to_string(),
    ];
    let (release_tx, release_rx) = oneshot::channel();
    tokio::spawn(fake_server_handshake(fast, None));
    tokio::spawn(fake_server_handshake(slow, Some(release_rx)));

    let mut connect_fut = std::pin::pin!(ClientBuilder::new()
        .servers(addrs)
        .with_exceptions(true)
        .server_selection(ServerSelection::ByUnique)
        .connect());

    tokio::select! {
        _ = &mut connect_fut => {
            panic!("connect() resolved while a server was still in its handshake");
        }
        _ = tokio::time::sleep(Duration::from_millis(300)) => {}
    }

    release_tx.send(()).expect("server should be waiting");
    tokio::time::timeout(Duration::from_secs(5), connect_fut)
        .await
        .expect("connect() should resolve once every server finished connecting")
        .expect("connect should succeed");
}

/// A server that refuses connections counts as attempted, so ByUnique
/// connect() doesn't wait out its whole deadline for it.
#[tokio::test]
async fn by_unique_connect_does_not_wait_for_a_refused_server() {
    let up = TcpListener::bind("127.0.0.1:0").await.expect("bind a port");
    let up_addr = up.local_addr().unwrap().to_string();
    tokio::spawn(fake_server_handshake(up, None));
    let refused_addr = {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind a port");
        listener.local_addr().unwrap().to_string()
    };

    tokio::time::timeout(
        Duration::from_secs(2),
        ClientBuilder::new()
            .servers([up_addr, refused_addr])
            .with_exceptions(true)
            .server_selection(ServerSelection::ByUnique)
            .connect(),
    )
    .await
    .expect("connect() must not wait for a server that refused the connection")
    .expect("connect should succeed with one server up");
}
