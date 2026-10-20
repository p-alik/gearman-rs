//! Internal connection-establishment helper shared by `Client` and
//! `Worker`. Always routes through a boxed `dyn AsyncStream` so their actor
//! loops have one connection type regardless of whether the `tls` feature
//! is enabled — only this module and `connect` need to know the
//! difference.

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::error::Result;
use crate::Connection;

/// Any byte stream a connection can run over (plain TCP or TLS), so it can
/// be boxed as one type.
pub(crate) trait AsyncStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> AsyncStream for T {}

/// The stream type every `Client`/`Worker` connection uses.
pub(crate) type BoxedStream = Box<dyn AsyncStream>;

/// How to reach a job server: plain TCP, or TLS on top of it.
#[derive(Clone, Default)]
pub(crate) enum Transport {
    /// Plain TCP (the default).
    #[default]
    Plain,
    /// TLS over TCP, established before any Gearman framing.
    #[cfg(feature = "tls")]
    Tls(crate::tls::TlsConfig),
}

/// Opens a connection to `addr` (`host:port`) over `transport`, completing
/// the TLS handshake first when it is TLS.
pub(crate) async fn connect(addr: &str, transport: &Transport) -> Result<Connection<BoxedStream>> {
    let tcp = TcpStream::connect(addr).await?;
    match transport {
        Transport::Plain => Ok(Connection::from_stream(Box::new(tcp) as BoxedStream)),
        #[cfg(feature = "tls")]
        Transport::Tls(config) => {
            let server_name = crate::tls::server_name(addr)?;
            let tls_stream = config.connector.connect(server_name, tcp).await?;
            Ok(Connection::from_stream(Box::new(tls_stream) as BoxedStream))
        }
    }
}
