//! Client-only integration coverage against a real `gearmand` (no worker
//! involved yet — that lands in Phase 3). Soft-skips if `gearmand` isn't
//! available; see `tests/common`.

mod common;

use bytes::Bytes;
use gearman::client::ClientBuilder;

#[tokio::test]
async fn submit_bg_and_get_status_roundtrip() {
    let Some(gearmand) = common::GearmandProcess::start() else {
        return;
    };

    let client = ClientBuilder::new()
        .servers([gearmand.addr.clone()])
        .connect()
        .await
        .expect("client should connect");

    let handle = client
        .submit_bg("reverse", Bytes::from_static(b"test"))
        .await
        .expect("background submit should succeed");

    // No worker is registered for "reverse" here, so the job just sits
    // queued; this only exercises the submit_bg + get_status round trip.
    let status = client
        .get_status(&handle)
        .await
        .expect("get_status should succeed");
    assert!(!status.running);
}

#[tokio::test]
async fn get_status_unique_roundtrip() {
    let Some(gearmand) = common::GearmandProcess::start() else {
        return;
    };

    let client = ClientBuilder::new()
        .servers([gearmand.addr.clone()])
        .connect()
        .await
        .expect("client should connect");

    let handle = client
        .submit(
            "reverse",
            Some("my-unique-id"),
            Bytes::from_static(b"test"),
            gearman::client::SubmitOptions::background(),
        )
        .await
        .expect("background submit should succeed")
        .handle;

    let status = client
        .get_status_unique(&handle)
        .await
        .expect("get_status_unique should succeed");
    assert!(!status.running);
}

#[tokio::test]
async fn concurrent_submits_get_distinct_handles() {
    let Some(gearmand) = common::GearmandProcess::start() else {
        return;
    };

    let client = ClientBuilder::new()
        .servers([gearmand.addr.clone()])
        .connect()
        .await
        .expect("client should connect");

    let mut handles = Vec::new();
    for _ in 0..10 {
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            client
                .submit_bg("reverse", Bytes::from_static(b"test"))
                .await
                .expect("background submit should succeed")
        }));
    }

    let mut seen = std::collections::HashSet::new();
    for h in handles {
        let handle = h.await.expect("task should not panic");
        assert!(seen.insert(handle.as_str().to_string()), "duplicate handle");
    }
}

#[tokio::test]
async fn submit_epoch_roundtrip() {
    let Some(gearmand) = common::GearmandProcess::start() else {
        return;
    };

    let client = ClientBuilder::new()
        .servers([gearmand.addr.clone()])
        .connect()
        .await
        .expect("client should connect");

    let epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;

    let handle = client
        .submit_epoch("reverse", None, epoch, Bytes::from_static(b"test"))
        .await
        .expect("epoch submit should succeed");

    let status = client
        .get_status(&handle)
        .await
        .expect("get_status should succeed");
    assert!(status.known);
    assert!(!status.running);
}
