//! TLS integration coverage against a real `gearmand` started with
//! `--ssl`. Soft-skips if `gearmand` isn't available; see `tests/common`.
//! Requires the `tls` feature, so the whole file is gated on it.
#![cfg(feature = "tls")]

mod common;

use std::sync::Arc;

use bytes::Bytes;
use gearman::client::ClientBuilder;
use gearman::tls::TlsConfig;
use tokio_rustls::rustls;

const CERT_PEM: &[u8] = include_bytes!("tls_fixtures/cert.pem");
const KEY_PEM: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/tls_fixtures/key.pem");
const CERT_PEM_PATH: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/tls_fixtures/cert.pem");

fn client_tls_config() -> TlsConfig {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let mut reader = std::io::BufReader::new(CERT_PEM);
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<_, _>>()
        .expect("valid PEM certs");

    let mut roots = rustls::RootCertStore::empty();
    for cert in certs {
        roots
            .add(cert)
            .expect("cert should be a valid trust anchor");
    }

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    TlsConfig::new(Arc::new(config))
}

#[tokio::test]
async fn client_submit_over_tls_round_trips() {
    let _ = tracing_subscriber::fmt::try_init();

    let Some(gearmand) = common::GearmandProcess::start_with_args(&[
        "--ssl",
        "--ssl-certificate",
        CERT_PEM_PATH,
        "--ssl-key",
        KEY_PEM,
        "--ssl-ca-file",
        CERT_PEM_PATH,
    ]) else {
        return;
    };

    // The cert's SAN covers "localhost", not the fixture's 127.0.0.1
    // address, so dial it via the hostname to match server-name validation.
    let port = gearmand.addr.rsplit_once(':').unwrap().1;
    let addr = format!("localhost:{port}");

    let client = ClientBuilder::new()
        .servers([addr])
        .tls(client_tls_config())
        .connect()
        .await
        .expect("client should connect over TLS");

    let handle = client
        .submit_bg("reverse", Bytes::from_static(b"test"))
        .await
        .expect("background submit over TLS should succeed");

    let status = client
        .get_status(&handle)
        .await
        .expect("get_status over TLS should succeed");
    assert!(!status.running);
}
