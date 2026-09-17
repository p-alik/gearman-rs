//! TLS support, built on `tokio-rustls`. gearmand wraps the raw TCP stream
//! in TLS *before* any Gearman framing begins (a plain TLS handshake over
//! the socket, not STARTTLS), so the handshake must complete before the
//! codec attaches — see [`crate::Connection::from_stream`], which is
//! generic over the byte stream for exactly this reason.
//!
//! This crate takes a caller-supplied `rustls::ClientConfig` rather than
//! bundling a root certificate store (native or webpki), so it doesn't
//! force a particular trust source or pull in extra dependencies for
//! callers who don't need TLS.

use std::sync::Arc;

use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::TlsConnector;

use crate::error::{GearmanError, Result};

// `connector` and `server_name` below are only read by `crate::net`, which
// is itself only compiled when the `client` or `worker` feature is also
// enabled (that's the only place a `TlsConfig` can actually be used) — so a
// `tls`-only build sees them as dead code without this.
#[derive(Clone)]
pub struct TlsConfig {
    #[allow(dead_code)]
    pub(crate) connector: TlsConnector,
}

impl TlsConfig {
    pub fn new(client_config: Arc<ClientConfig>) -> Self {
        Self {
            connector: TlsConnector::from(client_config),
        }
    }
}

/// Derives the TLS server name (for SNI and certificate verification) from
/// a `host:port` address string.
#[allow(dead_code)]
pub(crate) fn server_name(addr: &str) -> Result<ServerName<'static>> {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    ServerName::try_from(host.to_string()).map_err(|_| {
        GearmanError::InvalidServerName {
            host: host.to_string(),
        }
    })
}
