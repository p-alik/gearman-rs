//! Client tests against a hand-scripted fake server rather than real
//! `gearmand`, for behavior only an adversarial/non-conforming peer
//! triggers (a real server never exercises these paths).

use std::time::Duration;

use bytes::Bytes;
use gearman::client::ClientBuilder;
use gearman::protocol::{Packet, PacketType};
use gearman::Connection;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Reads `OPTION_REQ "exceptions"`, waits until told to reply (so the test
/// can observe whether `connect()` resolves before that reply exists at
/// all), then replies `ERROR` — simulating a server that doesn't support
/// the option — and serves one `SUBMIT_JOB_BG` with a real `JOB_CREATED` so
/// the test can also check that reply isn't misattributed.
async fn fake_server(
    listener: TcpListener,
    release_option_res: oneshot::Receiver<()>,
) {
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
        .expect(
            "client should connect even though the server rejects OPTION_REQ",
        );

    let handle = tokio::time::timeout(
        Duration::from_secs(5),
        client.submit_bg("echo", "payload"),
    )
    .await
    .expect("submit_bg should not hang")
    .expect(
        "submit_bg should get its own JOB_CREATED reply, not the OPTION_REQ error",
    );

    assert_eq!(handle.as_str(), "H:test:1");

    server.await.expect("fake server task should not panic");
}
