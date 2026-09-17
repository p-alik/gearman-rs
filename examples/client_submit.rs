//! Client smoke test against a real `gearmand`. Submits a background job
//! and polls its status — no worker required (worker support lands in
//! Phase 3, at which point `submit_fg` becomes demonstrable end-to-end).
//!
//! Usage: `cargo run --example client_submit -- [host:port]` (defaults to
//! 127.0.0.1:4730).

use bytes::Bytes;
use gearman::client::ClientBuilder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:4730".to_string());

    let client = ClientBuilder::new().servers([addr]).connect().await?;

    let handle = client
        .submit_bg("reverse", Bytes::from_static(b"hello gearman"))
        .await?;
    println!("submitted background job: {handle}");

    let status = client.get_status(&handle).await?;
    println!(
        "status: known={} running={} progress={}/{}",
        status.known, status.running, status.numerator, status.denominator
    );

    Ok(())
}
