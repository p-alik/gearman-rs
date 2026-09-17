//! `Worker::shutdown` behavior when a job handler panics, against a
//! hand-scripted fake server (no real `gearmand` needed — the fake server
//! just hands out one job assignment directly, without a real submitting
//! client on the other end).

mod common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use common::LogCapture;
use gearman::protocol::{Packet, PacketType};
use gearman::worker::{WorkError, WorkerBuilder, WorkerJob};
use gearman::Connection;
use tokio::net::TcpListener;

async fn boom(_job: WorkerJob) -> Result<Bytes, WorkError> {
    panic!("intentional test panic");
}

/// Hands out one job assignment for "boom" as soon as it's grabbed. The
/// handler panics before ever replying on the wire, so this just holds the
/// connection open afterward — the point under test is the grab-loop task
/// itself unwinding, not anything further on the wire.
async fn fake_server(listener: TcpListener) {
    let (stream, _) = listener.accept().await.expect("accept");
    let mut conn = Connection::from_stream(stream);

    let can_do = conn.recv().await.unwrap().unwrap();
    assert_eq!(can_do.ptype, PacketType::CanDo);
    assert_eq!(can_do.arg_str(0), Some("boom"));

    let grab = conn.recv().await.unwrap().unwrap();
    assert_eq!(grab.ptype, PacketType::GrabJobUniq);
    conn.send(Packet::response(
        PacketType::JobAssignUniq,
        vec![
            Bytes::from_static(b"H:test:1"),
            Bytes::from_static(b"boom"),
            Bytes::from_static(b"uniq-1"),
            Bytes::from_static(b"payload"),
        ],
    ))
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;
}

/// Regression test: a panicking handler unwinds its grab-loop task, and
/// `Worker::shutdown` used to discard the resulting `JoinError` with no log
/// line, so the concurrency loss was silent. It must now (a) still return
/// promptly rather than hang or itself panic, and (b) log the abnormal
/// task exit.
#[tokio::test]
async fn shutdown_completes_and_logs_after_a_panicking_handler() {
    let (captured, _guard) = LogCapture::install();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind a port");
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(fake_server(listener));

    let worker = WorkerBuilder::new()
        .servers([addr])
        .concurrency(1)
        .register("boom", boom)
        .run();

    // Give the worker time to grab the job and let the handler panic.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let start = Instant::now();
    tokio::time::timeout(Duration::from_secs(5), worker.shutdown())
        .await
        .expect(
            "shutdown() must not hang waiting on a task that already panicked",
        );
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "shutdown() took {elapsed:?}, expected it to return promptly"
    );

    assert!(
        captured.contains(
            tracing::Level::ERROR,
            "worker grab-loop task ended abnormally"
        ),
        "expected the panicked task's abnormal exit to be logged, got: {:?}",
        captured.messages()
    );
}
