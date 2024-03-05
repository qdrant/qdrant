use std::fmt::Debug;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rustls::client::VerifierBuilderError;
use rustls::pki_types::CertificateDer;
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{crypto, RootCertStore, ServerConfig};
use rustls_pemfile::Item;

use crate::settings::{Settings, TlsConfig};

type Result<T> = std::result::Result<T, Error>;

/// A TTL based rotating server certificate resolver
#[derive(Debug)]
struct RotatingCertificateResolver {
    /// TLS configuration used for loading/refreshing certified key
    tls_config: TlsConfig,

    /// TTL for each rotation
    ttl: Option<Duration>,

    /// Current certified key
    key: RwLock<CertifiedKeyWithAge>,
}

impl RotatingCertificateResolver {
    pub fn new(tls_config: TlsConfig, ttl: Option<Duration>) -> Result<Self> {
        let certified_key = load_certified_key(&tls_config)?;

        Ok(Self {
            tls_config,
            ttl,
            key: RwLock::new(CertifiedKeyWithAge::from(certified_key)),
        })
    }

    /// Get certificate key or refresh
    ///
    /// The key is automatically refreshed when the TTL is reached.
    /// If refreshing fails, an error is logged and the old key is persisted.
    fn get_key_or_refresh(&self) -> Arc<CertifiedKey> {
        // Get read-only lock to the key. If TTL is not configured or is not expired, return key.
        let key = self.key.read();
        let ttl = match self.ttl {
            Some(ttl) if key.is_expired(ttl) => ttl,
            _ => return key.key.clone(),
        };
        drop(key);

        // If TTL is expired:
        // - get read-write lock to the key
        // - *re-check that TTL is expired* (to avoid refreshing the key multiple times from concurrent threads)
        // - refresh and return the key
        let mut key = self.key.write();
        if key.is_expired(ttl) {
            if let Err(err) = key.refresh(&self.tls_config) {
                log::error!("Failed to refresh server TLS certificate, keeping current: {err}");
            }
        }

        key.key.clone()
    }
}

impl ResolvesServerCert for RotatingCertificateResolver {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.get_key_or_refresh())
    }
}

#[derive(Debug)]
struct CertifiedKeyWithAge {
    /// Last time the certificate was updated/replaced
    last_update: Instant,

    /// Current certified key
    key: Arc<CertifiedKey>,
}

impl CertifiedKeyWithAge {
    pub fn from(key: Arc<CertifiedKey>) -> Self {
        Self {
            last_update: Instant::now(),
            key,
        }
    }

    pub fn refresh(&mut self, tls_config: &TlsConfig) -> Result<()> {
        *self = Self::from(load_certified_key(tls_config)?);
        Ok(())
    }

    pub fn age(&self) -> Duration {
        self.last_update.elapsed()
    }

    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.age() >= ttl
    }
}

/// Load TLS configuration and construct certified key.
fn load_certified_key(tls_config: &TlsConfig) -> Result<Arc<CertifiedKey>> {
    // Load certificates
    let certs: Vec<CertificateDer> = with_buf_read(&tls_config.cert, |rd| {
        rustls_pemfile::read_all(rd).collect::<io::Result<Vec<_>>>()
    })?
    .into_iter()
    .filter_map(|item| match item {
        Item::X509Certificate(data) => Some(data),
        _ => None,
    })
    .collect();
    if certs.is_empty() {
        return Err(Error::NoServerCert);
    }

    // Load private key
    let private_key_item =
        with_buf_read(&tls_config.key, rustls_pemfile::read_one)?.ok_or(Error::NoPrivateKey)?;
    let private_key = match private_key_item {
        Item::Pkcs1Key(pkey) => rustls_pki_types::PrivateKeyDer::from(pkey),
        Item::Pkcs8Key(pkey) => rustls_pki_types::PrivateKeyDer::from(pkey),
        Item::Sec1Key(pkey) => rustls_pki_types::PrivateKeyDer::from(pkey),
        _ => return Err(Error::InvalidPrivateKey),
    };
    let signing_key = crypto::ring::sign::any_supported_type(&private_key).map_err(Error::Sign)?;

    // Construct certified key
    let certified_key = CertifiedKey::new(certs, signing_key);
    Ok(Arc::new(certified_key))
}

/// Generate an actix server configuration with TLS
///
/// Uses TLS settings as configured in configuration by user.
pub fn actix_tls_server_config(settings: &Settings) -> Result<ServerConfig> {
    let config = ServerConfig::builder();
    let tls_config = settings
        .tls
        .clone()
        .ok_or_else(Settings::tls_config_is_undefined_error)
        .map_err(Error::Io)?;

    // Verify client CA or not
    let config = if settings.service.verify_https_client_certificate {
        let mut root_cert_store = RootCertStore::empty();
        let ca_certs: Vec<CertificateDer> = with_buf_read(&tls_config.ca_cert, |rd| {
            rustls_pemfile::certs(rd).collect()
        })?;
        root_cert_store.add_parsable_certificates(ca_certs);
        let client_cert_verifier = WebPkiClientVerifier::builder(root_cert_store.into())
            .build()
            .map_err(Error::ClientCertVerifier)?;
        config.with_client_cert_verifier(client_cert_verifier)
    } else {
        config.with_no_client_auth()
    };

    // Configure rotating certificate resolver
    let ttl = match tls_config.cert_ttl {
        None | Some(0) => None,
        Some(seconds) => Some(Duration::from_secs(seconds)),
    };
    let cert_resolver = RotatingCertificateResolver::new(tls_config, ttl)?;
    let config = config.with_cert_resolver(Arc::new(cert_resolver));

    Ok(config)
}

fn with_buf_read<T>(path: &str, f: impl FnOnce(&mut dyn BufRead) -> io::Result<T>) -> Result<T> {
    let file = File::open(path).map_err(|err| Error::OpenFile(err, path.into()))?;
    let mut reader = BufReader::new(file);
    let dyn_reader: &mut dyn BufRead = &mut reader;
    f(dyn_reader).map_err(|err| Error::ReadFile(err, path.into()))
}

/// Actix TLS errors.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("TLS file could not be opened: {1}")]
    OpenFile(#[source] io::Error, String),
    #[error("TLS file could not be read: {1}")]
    ReadFile(#[source] io::Error, String),
    #[error("general TLS IO error")]
    Io(#[source] io::Error),
    #[error("no server certificate found")]
    NoServerCert,
    #[error("no private key found")]
    NoPrivateKey,
    #[error("invalid private key")]
    InvalidPrivateKey,
    #[error("TLS signing error")]
    Sign(#[source] rustls::Error),
    #[error("client certificate verification")]
    ClientCertVerifier(#[source] VerifierBuilderError),
}
