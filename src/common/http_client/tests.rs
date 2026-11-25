use std::io::{self, BufRead as _, BufReader, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use super::*;

/// Test certificates, the server certificate is valid for 127.0.0.1 and signed by the test CA
fn cert_path(file: &str) -> String {
    format!(
        "{}/tests/e2e_tests/test_data/cert/{file}",
        env!("CARGO_MANIFEST_DIR"),
    )
}

fn tls_config(ca_cert: Option<&str>) -> TlsConfig {
    TlsConfig {
        cert: cert_path("cert.pem"),
        key: cert_path("key.pem"),
        ca_cert: ca_cert.map(cert_path),
        cert_ttl: None,
    }
}

/// HTTPS server on localhost that counts the requests it received
struct TestServer {
    url: String,
    requests: Arc<AtomicUsize>,
}

impl TestServer {
    fn start() -> Self {
        Self::start_on("127.0.0.1")
    }

    /// Start the server on the given local IP address, the URL uses that address as host
    fn start_on(host: &str) -> Self {
        // Same provider as in main
        let _ = rustls::crypto::ring::default_provider().install_default();

        let certs = CertificateDer::pem_file_iter(cert_path("cert.pem"))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let key = PrivateKeyDer::from_pem_file(cert_path("key.pem")).unwrap();
        let config = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .unwrap(),
        );

        let listener = TcpListener::bind((host, 0)).unwrap();
        let url = format!("https://{host}:{}/", listener.local_addr().unwrap().port());
        let requests = Arc::new(AtomicUsize::new(0));

        let requests_clone = requests.clone();
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                // Fails if the client rejects our certificate
                let _ = serve(&config, stream, &requests_clone);
            }
        });

        Self { url, requests }
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    async fn get(
        &self,
        tls_config: Option<&TlsConfig>,
        verify_https_client_certificate: bool,
        server_cert_verification: ServerCertVerification,
    ) -> reqwest::Result<reqwest::Response> {
        https_client(
            None,
            tls_config,
            verify_https_client_certificate,
            server_cert_verification,
        )
        .unwrap()
        .get(&self.url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
    }
}

fn serve(
    config: &Arc<rustls::ServerConfig>,
    stream: TcpStream,
    requests: &AtomicUsize,
) -> io::Result<()> {
    let conn = rustls::ServerConnection::new(config.clone()).map_err(io::Error::other)?;
    let mut tls = rustls::StreamOwned::new(conn, stream);

    // Read request head
    let mut reader = BufReader::new(&mut tls);
    let mut line = String::new();
    while line != "\r\n" {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
    }

    requests.fetch_add(1, Ordering::SeqCst);

    tls.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")?;
    tls.conn.send_close_notify();
    tls.flush()
}

const ALL_VERIFICATIONS: [ServerCertVerification; 2] = [
    ServerCertVerification::Strict,
    ServerCertVerification::SkipForPeersWithoutCaCert,
];

/// Without TLS, verify against the system root certificates, which don't have our test CA
#[tokio::test]
async fn test_tls_disabled_verifies_server_cert() {
    let server = TestServer::start();

    for verification in ALL_VERIFICATIONS {
        let result = server.get(None, false, verification).await;
        assert!(result.is_err(), "{verification:?} must reject the server");
    }

    assert_eq!(server.requests(), 0);
}

/// TLS without CA certificate must still verify the server, see GHSA-73xv-xw62-r3h3
#[tokio::test]
async fn test_tls_without_ca_cert_verifies_server_cert() {
    let server = TestServer::start();
    let tls_config = tls_config(None);

    let result = server
        .get(Some(&tls_config), false, ServerCertVerification::Strict)
        .await;
    let err = result.expect_err("must reject the server");

    assert_eq!(server.requests(), 0);

    // The error users get must tell that the certificate was rejected
    let err = StorageError::from(err).to_string();
    assert!(err.contains("invalid peer certificate"), "{err}");
}

/// TLS without CA certificate cannot verify other peers, so it accepts any certificate
#[tokio::test]
async fn test_tls_without_ca_cert_skips_peer_verification() {
    let server = TestServer::start();
    let tls_config = tls_config(None);

    let response = server
        .get(
            Some(&tls_config),
            false,
            ServerCertVerification::SkipForPeersWithoutCaCert,
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    assert_eq!(server.requests(), 1);
}

/// TLS with client certificate verification must verify the server, even without CA certificate
#[tokio::test]
async fn test_tls_with_client_cert_verifies_server_cert() {
    let server = TestServer::start();
    let tls_config = tls_config(None);

    for verification in ALL_VERIFICATIONS {
        let result = server.get(Some(&tls_config), true, verification).await;
        assert!(result.is_err(), "{verification:?} must reject the server");
    }

    assert_eq!(server.requests(), 0);
}

/// TLS with CA certificate verifies the server against it
///
/// Only on Linux, the macOS verifier rejects the 10 year validity of the test certificate.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_tls_with_ca_cert_verifies_server_cert() {
    let server = TestServer::start();
    let tls_config = tls_config(Some("cacert.pem"));

    for verification in ALL_VERIFICATIONS {
        let response = server
            .get(Some(&tls_config), false, verification)
            .await
            .unwrap();
        assert!(response.status().is_success());
    }

    assert_eq!(server.requests(), 2);
}

/// TLS with CA certificate must reject a certificate that is not valid for the requested host
///
/// The server certificate is signed by the CA, but is not valid for 127.0.0.2.
///
/// Only on Linux, other platforms don't route all of 127.0.0.0/8 to the loopback interface.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_tls_with_ca_cert_verifies_server_hostname() {
    let server = TestServer::start_on("127.0.0.2");
    let tls_config = tls_config(Some("cacert.pem"));

    for verification in ALL_VERIFICATIONS {
        let result = server.get(Some(&tls_config), false, verification).await;
        let err = result.expect_err("must reject the server");

        // Must be rejected for the host name, not for an untrusted certificate
        let err = format!("{err:?}");
        assert!(err.contains("NotValidForName"), "{verification:?}: {err}");
    }

    assert_eq!(server.requests(), 0);
}
