//! Lowest-level smoke test against a real `gearmand`: send ECHO_REQ over a
//! raw `Connection` and print the ECHO_RES payload back.
//!
//! Usage: `cargo run --example raw_echo -- [host:port]` (defaults to
//! 127.0.0.1:4730).

use bytes::Bytes;
use gearman::protocol::{Packet, PacketType};
use gearman::Connection;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:4730".to_string());

    let mut conn = Connection::connect(&addr).await?;
    conn.send(Packet::request(
        PacketType::EchoReq,
        vec![Bytes::from_static(b"hello gearman")],
    ))
    .await?;

    match conn.recv().await? {
        Some(pkt) if pkt.ptype == PacketType::EchoRes => {
            let text = pkt.arg_str(0).unwrap_or("<non-utf8>");
            println!("ECHO_RES: {text}");
        }
        Some(pkt) => println!("unexpected reply: {:?}", pkt.ptype),
        None => println!("connection closed with no reply"),
    }

    Ok(())
}
