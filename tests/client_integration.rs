//! Client-only integration coverage against a real `gearmand` (no worker
//! involved yet — that lands in Phase 3). Soft-skips if `gearmand` isn't
//! available; see `tests/common`.

mod common;

use bytes::Bytes;
use gearman::client::{ClientBuilder, ServerSelection};

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

/// Submits two background jobs per unique id and asserts each pair got the
/// same handle, i.e. both reached the same server and gearmand coalesced
/// them.
async fn assert_uniques_coalesce(client: &gearman::client::Client) {
    for i in 0..20 {
        let unique = format!("by-unique-{i}");
        let mut handles = Vec::new();
        for _ in 0..2 {
            let submitted = client
                .submit(
                    "reverse",
                    Some(&unique),
                    Bytes::from_static(b"test"),
                    gearman::client::SubmitOptions::background(),
                )
                .await
                .expect("background submit should succeed");
            handles.push(submitted.handle);
        }
        // Compare the whole JobHandle, which includes the issuing server:
        // handle strings are only unique per server, so two servers on one
        // host can issue the same string for two separate jobs.
        assert_eq!(handles[0], handles[1], "{unique}");
    }
}

#[tokio::test]
async fn server_selection_by_unique_coalesces_across_servers() {
    let servers: Vec<_> = (0..3)
        .filter_map(|_| common::GearmandProcess::start())
        .collect();
    if servers.len() < 3 {
        return;
    }

    let client = ClientBuilder::new()
        .servers(servers.iter().map(|g| g.addr.clone()))
        .server_selection(ServerSelection::ByUnique)
        .connect()
        .await
        .expect("client should connect");

    assert_uniques_coalesce(&client).await;
}

#[tokio::test]
async fn server_selection_by_unique_fails_over_when_a_server_is_down() {
    let mut servers: Vec<_> = (0..3)
        .filter_map(|_| common::GearmandProcess::start())
        .collect();
    if servers.len() < 3 {
        return;
    }

    let client = ClientBuilder::new()
        .servers(servers.iter().map(|g| g.addr.clone()))
        .server_selection(ServerSelection::ByUnique)
        .connect()
        .await
        .expect("client should connect");

    // Stop one server: uniques it owned must fail over consistently to
    // the next one on the ring instead of failing.
    servers.remove(0).stop();
    // Give the client's connection actor time to notice the closed socket.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    assert_uniques_coalesce(&client).await;
}
