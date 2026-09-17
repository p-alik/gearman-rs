//! `Worker::shutdown` responsiveness while a connection is backed off after
//! a failed connect, against an address nothing is listening on (no real
//! `gearmand` needed — the point is that the connect attempt fails).

use std::time::{Duration, Instant};

use gearman::worker::WorkerBuilder;
use tokio::net::TcpListener;

/// Regression test: the exponential-backoff sleeps after a failed connect
/// used to be plain, uninterruptible `tokio::time::sleep`s, so
/// `Worker::shutdown` could block for however much of the current backoff
/// (up to `MAX_BACKOFF` = 30s) remained. They're now raced against the
/// shutdown signal via `interruptible_backoff`, so shutdown must return
/// promptly even while a connection is mid-backoff.
#[tokio::test]
async fn shutdown_is_not_blocked_by_reconnect_backoff() {
    // Bind then immediately drop: the port is guaranteed free but nothing
    // is listening on it, so `connect` fails fast (connection refused)
    // rather than timing out slowly.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind a port");
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let worker = WorkerBuilder::new().servers([addr]).concurrency(1).run();

    // Let the first connect attempt fail and its (should-be-interruptible)
    // 100ms backoff sleep begin, so shutdown is called with most of that
    // window still remaining.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let start = Instant::now();
    tokio::time::timeout(Duration::from_millis(500), worker.shutdown())
        .await
        .expect("shutdown() must not hang");
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(80),
        "shutdown() took {elapsed:?}; the reconnect backoff sleep must be \
         interrupted by shutdown, not just checked in between sleeps"
    );
}
