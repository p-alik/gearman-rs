//! Admin text protocol coverage against a real `gearmand`. Soft-skips if
//! `gearmand` isn't available; see `tests/common`.

mod common;

use bytes::Bytes;
use gearman::admin::{AdminClient, MaxQueueSize};
use gearman::client::ClientBuilder;
use gearman::worker::{WorkError, WorkerBuilder, WorkerJob};

async fn reverse(job: WorkerJob) -> Result<Bytes, WorkError> {
    let mut data = job.payload.to_vec();
    data.reverse();
    Ok(Bytes::from(data))
}

#[tokio::test]
async fn version_getpid_verbose_roundtrip() {
    let Some(gearmand) = common::GearmandProcess::start() else {
        return;
    };

    let mut admin = AdminClient::connect(&gearmand.addr)
        .await
        .expect("admin client should connect");

    let version = admin.version().await.expect("version should succeed");
    assert!(!version.is_empty());

    let pid = admin.getpid().await.expect("getpid should succeed");
    assert!(pid > 0);

    let verbose = admin.verbose().await.expect("verbose should succeed");
    assert!(!verbose.is_empty());
}

#[tokio::test]
async fn status_and_workers_reflect_registered_worker() {
    let Some(gearmand) = common::GearmandProcess::start() else {
        return;
    };

    let worker = WorkerBuilder::new()
        .servers([gearmand.addr.clone()])
        .concurrency(1)
        .register("reverse", reverse)
        .run();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut admin = AdminClient::connect(&gearmand.addr)
        .await
        .expect("admin client should connect");

    let statuses = admin.status().await.expect("status should succeed");
    assert!(statuses.iter().any(|s| s.function == "reverse"));

    let workers = admin.workers().await.expect("workers should succeed");
    assert!(workers
        .iter()
        .any(|w| w.functions.iter().any(|f| f == "reverse")));

    let priority = admin
        .priority_status()
        .await
        .expect("prioritystatus should succeed");
    assert!(priority.iter().any(|s| s.function == "reverse"));

    worker.shutdown().await;
}

#[tokio::test]
async fn show_jobs_and_cancel_job_roundtrip() {
    let Some(gearmand) = common::GearmandProcess::start() else {
        return;
    };

    let client = ClientBuilder::new()
        .servers([gearmand.addr.clone()])
        .connect()
        .await
        .expect("client should connect");

    // No worker registered for "reverse" here, so the job stays queued and
    // cancellable.
    let handle = client
        .submit_bg("reverse", Bytes::from_static(b"test"))
        .await
        .expect("background submit should succeed");

    let mut admin = AdminClient::connect(&gearmand.addr)
        .await
        .expect("admin client should connect");

    let jobs = admin.show_jobs().await.expect("show jobs should succeed");
    assert!(jobs.iter().any(|j| j.handle == handle.as_str()));

    admin
        .cancel_job(handle.as_str())
        .await
        .expect("cancel job should succeed");
}

#[tokio::test]
async fn create_and_drop_function() {
    let Some(gearmand) = common::GearmandProcess::start() else {
        return;
    };

    let mut admin = AdminClient::connect(&gearmand.addr)
        .await
        .expect("admin client should connect");

    admin
        .create_function("admin_test_fn")
        .await
        .expect("create function should succeed");

    admin
        .set_max_queue("admin_test_fn", MaxQueueSize::Uniform(10))
        .await
        .expect("maxqueue should succeed");

    admin
        .drop_function("admin_test_fn")
        .await
        .expect("drop function should succeed");
}
