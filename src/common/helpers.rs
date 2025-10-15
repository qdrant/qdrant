use std::cmp::max;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};

use fs_err as fs;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::runtime;
use tokio::runtime::Runtime;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
use validator::Validate;

use crate::settings::{Settings, TlsConfig};

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct LocksOption {
    pub error_message: Option<String>,
    pub write: bool,
}

pub fn create_search_runtime(max_search_threads: usize) -> io::Result<Runtime> {
    let num_threads = common::defaults::search_thread_count(max_search_threads);
    runtime::Builder::new_multi_thread()
        .worker_threads(num_threads)
        .max_blocking_threads(num_threads)
        .enable_all()
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("search-{id}")
        })
        .build()
}

pub fn create_update_runtime(max_optimization_threads: usize) -> io::Result<Runtime> {
    let mut update_runtime_builder = runtime::Builder::new_multi_thread();

    let num_cpus = common::cpu::get_num_cpus();

    update_runtime_builder
        .enable_time()
        .worker_threads(num_cpus)
        .thread_name_fn(move || {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let update_id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("update-{update_id}")
        });

    if max_optimization_threads > 0 {
        // panics if val is not larger than 0.
        update_runtime_builder.max_blocking_threads(max_optimization_threads);
    }
    update_runtime_builder.build()
}

pub fn create_general_purpose_runtime() -> io::Result<Runtime> {
    runtime::Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .worker_threads(max(common::cpu::get_num_cpus(), 2))
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let general_id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("general-{general_id}")
        })
        .build()
}

/// Load client TLS configuration.
pub fn load_tls_client_config(settings: &Settings) -> io::Result<Option<ClientTlsConfig>> {
    if settings.cluster.p2p.enable_tls {
        let tls_config = &settings.tls()?;
        Ok(Some(
            ClientTlsConfig::new()
                .identity(load_identity(tls_config)?)
                .ca_certificate(load_ca_certificate(tls_config)?),
        ))
    } else {
        Ok(None)
    }
}

/// Load server TLS configuration for external gRPC
pub fn load_tls_external_server_config(tls_config: &TlsConfig) -> io::Result<ServerTlsConfig> {
    Ok(ServerTlsConfig::new().identity(load_identity(tls_config)?))
}

/// Load server TLS configuration for internal gRPC, check client certificate against CA
pub fn load_tls_internal_server_config(tls_config: &TlsConfig) -> io::Result<ServerTlsConfig> {
    Ok(ServerTlsConfig::new()
        .identity(load_identity(tls_config)?)
        .client_ca_root(load_ca_certificate(tls_config)?))
}

fn load_identity(tls_config: &TlsConfig) -> io::Result<Identity> {
    let cert = fs::read_to_string(&tls_config.cert)?;
    let key = fs::read_to_string(&tls_config.key)?;
    Ok(Identity::from_pem(cert, key))
}

fn load_ca_certificate(tls_config: &TlsConfig) -> io::Result<Certificate> {
    let Some(ca_cert_path) = &tls_config.ca_cert else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "CA certificate is required for TLS configuration",
        ));
    };
    let pem = fs::read_to_string(ca_cert_path)?;
    Ok(Certificate::from_pem(pem))
}

pub fn tonic_error_to_io_error(err: tonic::transport::Error) -> io::Error {
    io::Error::other(err)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::thread::sleep;
    use std::time::Duration;

    use collection::common::is_ready::IsReady;

    #[test]
    fn test_is_ready() {
        let is_ready = Arc::new(IsReady::default());
        let is_ready_clone = is_ready.clone();
        let join = thread::spawn(move || {
            is_ready_clone.await_ready();
            eprintln!(
                "is_ready_clone.check_ready() = {:#?}",
                is_ready_clone.check_ready()
            );
        });

        sleep(Duration::from_millis(500));
        eprintln!("Making ready");
        is_ready.make_ready();
        sleep(Duration::from_millis(500));
        join.join().unwrap()
    }
}
