//! Worker tests against a hand-scripted fake server rather than real
//! `gearmand`, for behavior only an adversarial/non-conforming peer (or an
//! explicit `Worker::unregister` call) triggers.

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::LogCapture;
use gearman::protocol::{Packet, PacketType};
use gearman::worker::{WorkError, WorkerBuilder, WorkerJob};
use gearman::Connection;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

async fn echo(job: WorkerJob) -> Result<Bytes, WorkError> {
    Ok(job.payload)
}

/// After `PRE_SLEEP`, replies with something other than `NOOP`, then checks
/// the worker recovers (re-sends `GRAB_JOB_UNIQ` instead of getting stuck or
/// erroring out) by handing it a real job on the next grab.
async fn fake_server_bad_wakeup(
    listener: TcpListener,
    work_complete_tx: oneshot::Sender<Packet>,
) {
    let (stream, _) = listener.accept().await.expect("accept");
    let mut conn = Connection::from_stream(stream);

    let can_do = conn.recv().await.unwrap().unwrap();
    assert_eq!(can_do.ptype, PacketType::CanDo);

    let grab1 = conn.recv().await.unwrap().unwrap();
    assert_eq!(grab1.ptype, PacketType::GrabJobUniq);
    conn.send(Packet::response(PacketType::NoJob, vec![]))
        .await
        .unwrap();

    let pre_sleep = conn.recv().await.unwrap().unwrap();
    assert_eq!(pre_sleep.ptype, PacketType::PreSleep);
    // Not a NOOP: the server should never send this while a worker is
    // asleep, but the worker must not treat it as one either.
    conn.send(Packet::response(
        PacketType::EchoRes,
        vec![Bytes::from_static(b"unexpected")],
    ))
    .await
    .unwrap();

    let grab2 = conn.recv().await.unwrap().unwrap();
    assert_eq!(grab2.ptype, PacketType::GrabJobUniq);
    conn.send(Packet::response(
        PacketType::JobAssignUniq,
        vec![
            Bytes::from_static(b"H:test:1"),
            Bytes::from_static(b"echo"),
            Bytes::from_static(b"uniq-1"),
            Bytes::from_static(b"payload"),
        ],
    ))
    .await
    .unwrap();

    let work_complete = conn.recv().await.unwrap().unwrap();
    let _ = work_complete_tx.send(work_complete);
}

#[tokio::test]
async fn worker_recovers_from_unexpected_packet_while_sleeping() {
    let (captured, _guard) = LogCapture::install();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind a port");
    let addr = listener.local_addr().unwrap().to_string();
    let (work_complete_tx, work_complete_rx) = oneshot::channel();
    tokio::spawn(fake_server_bad_wakeup(listener, work_complete_tx));

    let _worker = WorkerBuilder::new()
        .servers([addr])
        .concurrency(1)
        .register("echo", echo)
        .run();

    let work_complete = tokio::time::timeout(
        Duration::from_secs(5),
        work_complete_rx,
    )
    .await
    .expect("worker should not get stuck after the unexpected wake-up packet")
    .expect("fake server task should not be dropped without sending");

    assert_eq!(work_complete.ptype, PacketType::WorkComplete);
    assert_eq!(work_complete.arg_str(0), Some("H:test:1"));
    assert_eq!(
        work_complete.arg(1).map(Bytes::as_ref),
        Some(&b"payload"[..])
    );

    assert!(
        captured.contains(tracing::Level::WARN, "expected NOOP wake-up"),
        "expected a warning about the unexpected wake-up packet to be logged, got: {:?}",
        captured.messages()
    );
}

/// Registers `CAN_DO`, then — once the test has confirmed the server saw
/// it and calls `Worker::unregister` — checks `CANT_DO` actually lands on
/// the wire.
async fn fake_server_unregister(
    listener: TcpListener,
    can_do_seen_tx: oneshot::Sender<()>,
    cant_do_tx: oneshot::Sender<Packet>,
) {
    let (stream, _) = listener.accept().await.expect("accept");
    let mut conn = Connection::from_stream(stream);

    let can_do = conn.recv().await.unwrap().unwrap();
    assert_eq!(can_do.ptype, PacketType::CanDo);
    assert_eq!(can_do.arg_str(0), Some("echo"));
    let _ = can_do_seen_tx.send(());

    let grab1 = conn.recv().await.unwrap().unwrap();
    assert_eq!(grab1.ptype, PacketType::GrabJobUniq);
    conn.send(Packet::response(PacketType::NoJob, vec![]))
        .await
        .unwrap();

    let pre_sleep = conn.recv().await.unwrap().unwrap();
    assert_eq!(pre_sleep.ptype, PacketType::PreSleep);
    conn.send(Packet::response(PacketType::Noop, vec![]))
        .await
        .unwrap();

    let cant_do = conn.recv().await.unwrap().unwrap();
    let _ = cant_do_tx.send(cant_do);
}

#[tokio::test]
async fn unregister_sends_cant_do_on_the_wire() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind a port");
    let addr = listener.local_addr().unwrap().to_string();
    let (can_do_tx, can_do_rx) = oneshot::channel();
    let (cant_do_tx, cant_do_rx) = oneshot::channel();
    tokio::spawn(fake_server_unregister(listener, can_do_tx, cant_do_tx));

    let worker = WorkerBuilder::new()
        .servers([addr])
        .concurrency(1)
        .register("echo", echo)
        .run();

    tokio::time::timeout(Duration::from_secs(5), can_do_rx)
        .await
        .expect("server should observe CAN_DO before timing out")
        .expect("fake server task should not be dropped without sending");

    worker.unregister("echo").await;

    let cant_do = tokio::time::timeout(Duration::from_secs(5), cant_do_rx)
        .await
        .expect("server should observe CANT_DO before timing out")
        .expect("fake server task should not be dropped without sending");

    assert_eq!(cant_do.ptype, PacketType::CantDo);
    assert_eq!(cant_do.arg_str(0), Some("echo"));
}
