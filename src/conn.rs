use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::codec::Framed;

use crate::error::Result;
use crate::protocol::{GearmanCodec, Packet};

/// A framed Gearman connection. Generic over the underlying byte stream so
/// a TLS-wrapped stream can be handed to [`Connection::from_stream`] without
/// any change to the framing/codec layer.
pub struct Connection<S = TcpStream> {
    framed: Framed<S, GearmanCodec>,
}

impl Connection<TcpStream> {
    /// Opens a TCP connection and wraps it with the Gearman binary codec.
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self::from_stream(stream))
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Connection<S> {
    /// Wraps an already-established stream (e.g. a TLS stream) with the
    /// Gearman binary codec.
    pub fn from_stream(stream: S) -> Self {
        Self {
            framed: Framed::new(stream, GearmanCodec::new()),
        }
    }

    /// Encodes and writes a packet.
    pub async fn send(&mut self, packet: Packet) -> Result<()> {
        self.framed.send(packet).await
    }

    /// Reads the next packet, or `Ok(None)` if the peer closed the connection.
    pub async fn recv(&mut self) -> Result<Option<Packet>> {
        self.framed.next().await.transpose()
    }
}
