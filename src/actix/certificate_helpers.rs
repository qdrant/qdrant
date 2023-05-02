use std::fs;
use std::time::{Duration, Instant};

use openssl::error::ErrorStack;
use openssl::ssl::{
    SniError, SslAcceptor, SslAcceptorBuilder, SslContext, SslContextBuilder, SslFiletype,
    SslMethod, SslVerifyMode,
};
use openssl::x509::store::X509StoreBuilder;
use openssl::x509::X509;
use parking_lot::RwLock;

use crate::settings::{Settings, TlsConfig};

struct SslContextHolder {
    ssl_context: SslContext,
    tls_config: TlsConfig,
    verify_client_cert: bool,
    refresh_interval: Duration,
    last_updated: Instant,
}

impl SslContextHolder {
    fn new(
        tls_config: &TlsConfig,
        verify_client_cert: bool,
        refresh_interval: Duration,
    ) -> Result<SslContextHolder, ErrorStack> {
        let ssl_context = build_ssl_context(tls_config, verify_client_cert)?;
        Ok(SslContextHolder {
            ssl_context,
            tls_config: tls_config.clone(),
            verify_client_cert,
            refresh_interval,
            last_updated: Instant::now(),
        })
    }

    fn try_get_ssl_context(&self) -> Option<&SslContext> {
        (self.last_updated.elapsed() < self.refresh_interval).then_some(&self.ssl_context)
    }

    fn get_ssl_context_or_refresh(&mut self) -> Result<&SslContext, ()> {
        if self.last_updated.elapsed() >= self.refresh_interval {
            self.refresh()?;
        }
        Ok(&self.ssl_context)
    }

    fn refresh(&mut self) -> Result<(), ()> {
        log::info!("Refreshing TLS certificates for actix!");
        self.ssl_context =
            build_ssl_context(&self.tls_config, self.verify_client_cert).map_err(|_| ())?;
        Ok(())
    }
}

pub fn build_ssl_acceptor(settings: &Settings) -> std::io::Result<SslAcceptorBuilder> {
    let mut acceptor = SslAcceptor::mozilla_modern_v5(SslMethod::tls())?;

    let tls_config = settings
        .tls
        .clone()
        .ok_or_else(Settings::tls_config_is_undefined_error)?;

    let verify_client_cert = settings.service.verify_https_client_certificate;
    let ssl_context_holder = RwLock::new(SslContextHolder::new(
        &tls_config,
        verify_client_cert,
        Duration::from_secs(tls_config.cert_ttl),
    )?);

    // Use set_servername_callback to implement dynamic certificate loading and refresh
    acceptor.set_servername_callback(move |ssl, _alert| -> Result<(), SniError> {
        {
            let reader = ssl_context_holder.read();
            if let Some(ssl_context) = reader.try_get_ssl_context() {
                ssl.set_ssl_context(ssl_context).map_err(|error_stack| {
                    log::error!("Failed to set SSL context: {}", error_stack);
                    SniError::ALERT_FATAL
                })?;
                return Ok(());
            }
        }

        // If getting the SSL context failed, try to get it again with refreshing
        let mut writer = ssl_context_holder.write();
        let ssl_context = writer
            .get_ssl_context_or_refresh()
            .map_err(|_refresh_error| {
                log::error!("Failed to refresh certificates!");
                SniError::ALERT_FATAL
            })?;
        ssl.set_ssl_context(ssl_context).map_err(|error_stack| {
            log::error!("Failed to set SSL context: {}", error_stack);
            SniError::ALERT_FATAL
        })?;
        Ok(())
    });
    Ok(acceptor)
}

fn build_ssl_context(
    tls_config: &TlsConfig,
    verify_client_cert: bool,
) -> Result<SslContext, ErrorStack> {
    let mut ssl_context_builder = SslContextBuilder::new(SslMethod::tls_server())?;
    // Server TLS config
    ssl_context_builder.set_private_key_file(&tls_config.key, SslFiletype::PEM)?;
    ssl_context_builder.set_certificate_chain_file(&tls_config.cert)?;
    ssl_context_builder.check_private_key()?;

    // Verify client TLS certification
    if verify_client_cert {
        let client_ca =
            fs::read_to_string(&tls_config.ca_cert).expect("Failed to load CA certificate");
        let client_ca = X509::from_pem(client_ca.as_bytes())?;

        let mut x509_client_store_builder = X509StoreBuilder::new()?;
        x509_client_store_builder.add_cert(client_ca)?;
        let client_cert_store = x509_client_store_builder.build();
        ssl_context_builder.set_verify_cert_store(client_cert_store)?;
        ssl_context_builder.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
    }
    Ok(ssl_context_builder.build())
}
