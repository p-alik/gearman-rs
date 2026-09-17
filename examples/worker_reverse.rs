//! Worker smoke test: registers for "reverse" and runs until Ctrl+C. Pair
//! with `client_submit`'s `submit_fg` (or `examples/raw_echo.rs`-style
//! manual `SUBMIT_JOB`) to see the full round trip described in gearmand's
//! `PROTOCOL` file worked example.
//!
//! Usage: `cargo run --example worker_reverse -- [host:port]` (defaults to
//! 127.0.0.1:4730).

use bytes::Bytes;
use gearman::worker::{WorkError, WorkerBuilder, WorkerJob};

async fn reverse(job: WorkerJob) -> Result<Bytes, WorkError> {
    let mut data = job.payload.to_vec();
    data.reverse();
    Ok(Bytes::from(data))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:4730".to_string());

    let worker = WorkerBuilder::new()
        .servers([addr])
        .register("reverse", reverse)
        .run();

    println!("worker running, registered for \"reverse\"; press Ctrl+C to stop");
    tokio::signal::ctrl_c().await?;

    println!("shutting down...");
    worker.shutdown().await;
    Ok(())
}
