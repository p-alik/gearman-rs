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
/// a `host:port` address string. `host` may be a bracketed IPv6 literal
/// (`[::1]:4730`), matching what `std::net::ToSocketAddrs` accepts for the
/// same string — the brackets are stripped since they're wire/URL syntax,
/// not part of the address `rustls::pki_types::ServerName` expects.
#[allow(dead_code)]
pub(crate) fn server_name(addr: &str) -> Result<ServerName<'static>> {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    ServerName::try_from(host.to_string()).map_err(|_| {
        GearmanError::InvalidServerName {
            host: host.to_string(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_port_from_plain_host() {
        let name = server_name("gearman.example.com:4730").unwrap();
        let expected =
            ServerName::try_from("gearman.example.com".to_string()).unwrap();
        assert_eq!(name, expected);
    }

    /// The bug this guards against: `rsplit_once(':')` alone leaves the
    /// brackets in (`"[::1]"`), which `ServerName::try_from` rejects for
    /// both its IP-address and DNS-name variants, even though
    /// `std::net::ToSocketAddrs` accepts the same `"[::1]:4730"` string.
    #[test]
    fn strips_brackets_from_ipv6_literal() {
        let name = server_name("[::1]:4730").unwrap();
        let expected = ServerName::try_from("::1".to_string()).unwrap();
        assert_eq!(name, expected);
    }

    #[test]
    fn rejects_unparseable_host() {
        assert!(server_name("not a valid host:4730").is_err());
    }
}
