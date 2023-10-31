use std::time::{Duration, Instant};
use std::{fs, io, result};

use crate::settings::{Settings, TlsConfig};

#[derive(Debug)]
pub struct HttpClient {
    https_client: Option<HttpsClient>,
}

impl HttpClient {
    pub fn from_settings(settings: &Settings) -> Result<Self> {
        let https_client = HttpsClient::from_settings(settings)?;
        Ok(Self { https_client })
    }

    pub fn get(&self) -> reqwest::Client {
        match &self.https_client {
            Some(https_client) => https_client.get_or_refresh(),
            None => reqwest::Client::new(),
        }
    }
}

#[derive(Debug)]
pub struct HttpsClient {
    client: parking_lot::RwLock<HttpsClientWithAge>,
    tls_config: TlsConfig,
    verify_https_client_certificate: bool,
}

impl HttpsClient {
    pub fn from_settings(settings: &Settings) -> Result<Option<Self>> {
        if !settings.service.enable_tls {
            return Ok(None);
        }

        let Some(tls_config) = settings.tls.clone() else {
            return Err(Error::TlsConfigUndefined);
        };

        let verify_https_client_certificate = settings.service.verify_https_client_certificate;

        let client = HttpsClientWithAge::from_config(&tls_config, verify_https_client_certificate)?;

        let client = Self {
            client: client.into(),
            tls_config,
            verify_https_client_certificate,
        };

        Ok(Some(client))
    }

    pub fn get_or_refresh(&self) -> reqwest::Client {
        {
            let client = self.client.read();

            if client.is_valid(self.ttl()) {
                return client.get();
            }
        }

        let mut client = self.client.write();

        if client.is_expired(self.ttl()) {
            let res = client.refresh(&self.tls_config, self.verify_https_client_certificate);

            if let Err(err) = res {
                log::error!("Failed to refresh HTTPS client, keeping current: {err}");
            }
        }

        client.get()
    }

    pub fn ttl(&self) -> Duration {
        match self.tls_config.cert_ttl {
            None | Some(0) => Duration::MAX,
            Some(secs) => Duration::from_secs(secs),
        }
    }
}

#[derive(Clone, Debug)]
struct HttpsClientWithAge {
    client: reqwest::Client,
    last_update: Instant,
}

impl HttpsClientWithAge {
    pub fn from_config(
        tls_config: &TlsConfig,
        verify_https_client_certificate: bool,
    ) -> Result<Self> {
        let client = Self {
            client: https_client_from_config(tls_config, verify_https_client_certificate)?,
            last_update: Instant::now(),
        };

        Ok(client)
    }

    pub fn refresh(
        &mut self,
        tls_config: &TlsConfig,
        verify_https_client_certificate: bool,
    ) -> Result<()> {
        *self = Self::from_config(tls_config, verify_https_client_certificate)?;
        Ok(())
    }

    pub fn get(&self) -> reqwest::Client {
        self.client.clone()
    }

    pub fn is_valid(&self, ttl: Duration) -> bool {
        self.age() <= ttl
    }

    pub fn is_expired(&self, ttl: Duration) -> bool {
        !self.is_valid(ttl)
    }

    pub fn age(&self) -> Duration {
        self.last_update.elapsed()
    }
}

fn https_client_from_config(
    tls_config: &TlsConfig,
    verify_https_client_certificate: bool,
) -> Result<reqwest::Client> {
    let mut builder =
        reqwest::Client::builder().add_root_certificate(load_https_client_ca_cert(tls_config)?);

    if verify_https_client_certificate {
        builder = builder.identity(load_https_client_identity(tls_config)?);
    }

    let client = builder.build()?;

    Ok(client)
}

fn load_https_client_ca_cert(tls_config: &TlsConfig) -> Result<reqwest::Certificate> {
    let ca_cert_pem = fs::read(&tls_config.ca_cert).map_err(|err| {
        Error::failed_to_read("HTTPS client CA certificate file", &tls_config.ca_cert, err)
    })?;

    let ca_cert = reqwest::Certificate::from_pem(&ca_cert_pem)?;

    Ok(ca_cert)
}

fn load_https_client_identity(tls_config: &TlsConfig) -> Result<reqwest::Identity> {
    let mut identity_pem = fs::read(&tls_config.cert).map_err(|err| {
        Error::failed_to_read("HTTPS client certificate file", &tls_config.cert, err)
    })?;

    let mut key_file = fs::File::open(&tls_config.key)
        .map_err(|err| Error::failed_to_open("HTTPS client key file", &tls_config.key, err))?;

    io::copy(&mut key_file, &mut identity_pem)
        .map_err(|err| Error::failed_to_read("HTTPS client key file", &tls_config.key, err))?;

    let identity = reqwest::Identity::from_pem(&identity_pem)?;

    Ok(identity)
}

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("TLS config is not defined in the Qdrant config file")]
    TlsConfigUndefined,
    #[error("{0}: {1}")]
    Io(String, io::Error),
    #[error("failed to setup HTTPS client: {0}")]
    Reqwest(#[from] reqwest::Error),
}

impl Error {
    pub fn failed_to_open(what: &str, path: &str, source: io::Error) -> Self {
        Self::Io(format!("failed to open {what} {path}"), source)
    }

    pub fn failed_to_read(what: &str, path: &str, source: io::Error) -> Self {
        Self::Io(format!("failed to read {what} {path}"), source)
    }

    pub fn io(context: impl Into<String>, source: io::Error) -> Self {
        Self::Io(context.into(), source)
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        io::Error::new(io::ErrorKind::Other, err)
    }
}
