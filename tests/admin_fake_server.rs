//! Admin client tests against a hand-scripted fake server rather than real
//! `gearmand`, for behavior only a misbehaving/hostile peer triggers.

use std::time::Duration;

use gearman::admin::AdminClient;
use gearman::error::GearmanError;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

/// Floods the connection with a line that never contains a `\n`, well past
/// `AdminClient`'s line-length cap.
async fn fake_server_unterminated_line(listener: TcpListener) {
    let (mut stream, _) = listener.accept().await.expect("accept");
    let payload = vec![b'A'; 128 * 1024];
    let _ = stream.write_all(&payload).await;
    // Keep the socket open briefly so the client has time to read before
    // this task (and the connection) is torn down at test end.
    tokio::time::sleep(Duration::from_millis(500)).await;
}

/// Regression test: `read_line` used to grow its buffer without bound for
/// a peer that withholds the terminating `\n`. It's now capped, so a
/// command must fail with `AdminLineTooLong` instead of hanging or
/// exhausting memory.
#[tokio::test]
async fn read_line_rejects_unterminated_line_past_the_cap() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind a port");
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(fake_server_unterminated_line(listener));

    let mut admin = AdminClient::connect(&addr)
        .await
        .expect("admin client should connect");

    let result = tokio::time::timeout(Duration::from_secs(5), admin.version())
        .await
        .expect("read_line must not hang reading an unterminated line");

    let err = result.expect_err(
        "a line exceeding the cap without a terminator must be rejected",
    );
    assert!(
        matches!(err, GearmanError::AdminLineTooLong(_)),
        "unexpected error: {err:?}"
    );
}
