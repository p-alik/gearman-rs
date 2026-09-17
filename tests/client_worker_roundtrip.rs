//! Full client+worker round trip against a real `gearmand`. The "reverse"
//! test case mirrors the worked example in the gearmand C sources'
//! `PROTOCOL` file byte for byte (function "reverse", payload "test",
//! result "tset"). Soft-skips if `gearmand` isn't available; see
//! `tests/common`.

mod common;

use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use gearman::client::{ClientBuilder, JobEvent};
use gearman::worker::{GrabMode, WorkError, WorkerBuilder, WorkerJob};
use gearman::GearmanError;

async fn reverse(job: WorkerJob) -> Result<Bytes, WorkError> {
    let mut data = job.payload.to_vec();
    data.reverse();
    Ok(Bytes::from(data))
}

/// Registering CAN_DO and the client's first SUBMIT_JOB are two independent
/// connections racing the same server; give the worker a moment to finish
/// registering before submitting, same as any real deployment would.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn reverse_job_round_trips_end_to_end() {
    let Some(gearmand) = common::GearmandProcess::start() else {
        return;
    };

    let worker = WorkerBuilder::new()
        .servers([gearmand.addr.clone()])
        .concurrency(1)
        .register("reverse", reverse)
        .run();

    let client = ClientBuilder::new()
        .servers([gearmand.addr.clone()])
        .connect()
        .await
        .expect("client should connect");
    settle().await;

    let result = client
        .submit_fg("reverse", Bytes::from_static(b"test"))
        .await
        .expect("foreground submit should complete");
    assert_eq!(result.as_ref(), b"tset");

    worker.shutdown().await;
}

#[tokio::test]
async fn worker_failure_reports_work_fail() {
    let Some(gearmand) = common::GearmandProcess::start() else {
        return;
    };

    let worker = WorkerBuilder::new()
        .servers([gearmand.addr.clone()])
        .concurrency(1)
        .register("always_fail", |_job: WorkerJob| async {
            Err(WorkError::Fail)
        })
        .run();

    let client = ClientBuilder::new()
        .servers([gearmand.addr.clone()])
        .connect()
        .await
        .expect("client should connect");
    settle().await;

    let err = client
        .submit_fg("always_fail", Bytes::new())
        .await
        .expect_err("handler always fails");
    assert!(matches!(err, GearmanError::JobFailed { .. }));

    worker.shutdown().await;
}

#[tokio::test]
async fn background_submit_completes_and_status_reflects_it() {
    let Some(gearmand) = common::GearmandProcess::start() else {
        return;
    };

    let worker = WorkerBuilder::new()
        .servers([gearmand.addr.clone()])
        .concurrency(1)
        .register("reverse", reverse)
        .run();

    let client = ClientBuilder::new()
        .servers([gearmand.addr.clone()])
        .connect()
        .await
        .expect("client should connect");
    settle().await;

    let handle = client
        .submit_bg("reverse", Bytes::from_static(b"test"))
        .await
        .expect("background submit should succeed");

    // Background jobs get no WORK_* forwarding, so the client must poll.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let status = client
            .get_status(&handle)
            .await
            .expect("get_status should succeed");
        if !status.known && !status.running {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "background job did not complete in time"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    worker.shutdown().await;
}

#[tokio::test]
async fn concurrent_jobs_all_complete() {
    let Some(gearmand) = common::GearmandProcess::start() else {
        return;
    };

    let worker = WorkerBuilder::new()
        .servers([gearmand.addr.clone()])
        .concurrency(4)
        .register("reverse", reverse)
        .run();

    let client = ClientBuilder::new()
        .servers([gearmand.addr.clone()])
        .connect()
        .await
        .expect("client should connect");
    settle().await;

    let mut tasks = Vec::new();
    for i in 0..8 {
        let client = client.clone();
        let payload = format!("payload-{i}");
        tasks.push(tokio::spawn(async move {
            let result = client
                .submit_fg("reverse", Bytes::from(payload.clone()))
                .await
                .expect("submit_fg should succeed");
            let expected: String = payload.chars().rev().collect();
            assert_eq!(result.as_ref(), expected.as_bytes());
        }));
    }
    for t in tasks {
        t.await.expect("task should not panic");
    }

    worker.shutdown().await;
}

#[tokio::test]
async fn worker_shutdown_returns_promptly_with_no_job_in_flight() {
    let Some(gearmand) = common::GearmandProcess::start() else {
        return;
    };

    let worker = WorkerBuilder::new()
        .servers([gearmand.addr.clone()])
        .concurrency(1)
        .register("reverse", reverse)
        .run();
    settle().await;

    tokio::time::timeout(Duration::from_secs(2), worker.shutdown())
        .await
        .expect("shutdown should not hang once idle");
}

async fn echo_reducer(job: WorkerJob) -> Result<Bytes, WorkError> {
    Ok(Bytes::from(job.reducer.unwrap_or_default()))
}

#[tokio::test]
async fn reduce_job_forwards_reducer_name_to_worker() {
    let _ = tracing_subscriber::fmt::try_init();

    let Some(gearmand) = common::GearmandProcess::start() else {
        return;
    };

    let worker = WorkerBuilder::new()
        .servers([gearmand.addr.clone()])
        .concurrency(1)
        .grab_mode(GrabMode::All)
        .register("echo_reducer", echo_reducer)
        .run();

    let client = ClientBuilder::new()
        .servers([gearmand.addr.clone()])
        .connect()
        .await
        .expect("client should connect");
    settle().await;

    let submitted = client
        .submit_reduce_job(
            "echo_reducer",
            None,
            "my-reducer",
            Bytes::from_static(b"test"),
            false,
        )
        .await
        .expect("reduce job submit should succeed");

    let mut events = submitted
        .events
        .expect("foreground reduce job registers an event stream");
    let result = loop {
        match events.next().await {
            Some(JobEvent::Complete(payload)) => break payload,
            Some(_) => continue,
            None => panic!("connection closed before job completed"),
        }
    };
    assert_eq!(result.as_ref(), b"my-reducer");

    worker.shutdown().await;
}
