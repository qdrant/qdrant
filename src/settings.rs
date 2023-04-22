use std::env;

use api::grpc::transport_channel_pool::{
    DEFAULT_CONNECT_TIMEOUT, DEFAULT_GRPC_TIMEOUT, DEFAULT_POOL_SIZE,
};
use collection::operations::validation;
use config::{Config, ConfigError, Environment, File};
use segment::common::cpu::get_num_cpus;
use serde::Deserialize;
use storage::types::StorageConfig;
use validator::Validate;

#[derive(Debug, Deserialize, Validate, Clone)]
pub struct ServiceConfig {
    #[validate(length(min = 1))]
    #[serde(default = "default_service_host")]
    pub host: String,
    #[serde(default = "default_service_http_port")]
    pub http_port: u16,
    // None means that gRPC is disabled
    // If the user defines a `service` section in `config.yaml`
    // but omits `grpc_port` from it then gRPC is disabled
    pub grpc_port: Option<u16>,
    #[serde(default = "default_service_max_request_size_mb")]
    pub max_request_size_mb: usize,
    pub max_workers: Option<usize>,
    #[serde(default = "default_service_cors")]
    pub enable_cors: bool,
    #[serde(default)]
    pub enable_tls: bool,
    #[serde(default)]
    pub verify_https_client_certificate: bool,
    pub api_key: Option<String>,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            host: default_service_host(),
            http_port: default_service_http_port(),
            // If the user doesn't define a `service` section at all in `config.yaml`
            // then gRPC is enabled
            grpc_port: Some(6334),
            max_request_size_mb: default_service_max_request_size_mb(),
            max_workers: Option::default(),
            enable_cors: default_service_cors(),
            enable_tls: bool::default(),
            verify_https_client_certificate: bool::default(),
            api_key: Option::default(),
        }
    }
}

fn default_service_host() -> String {
    "0.0.0.0".to_string()
}

fn default_service_http_port() -> u16 {
    6333
}

fn default_service_max_request_size_mb() -> usize {
    32
}

fn default_service_cors() -> bool {
    true
}

#[derive(Debug, Deserialize, Clone, Validate)]
pub struct ClusterConfig {
    pub enabled: bool, // disabled by default
    #[serde(default = "default_timeout_ms")]
    #[validate(range(min = 1))]
    pub grpc_timeout_ms: u64,
    #[serde(default = "default_connection_timeout_ms")]
    #[validate(range(min = 1))]
    pub connection_timeout_ms: u64,
    #[serde(default)]
    #[validate]
    pub p2p: P2pConfig,
    #[serde(default)]
    #[validate]
    pub consensus: ConsensusConfig,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: bool::default(),
            grpc_timeout_ms: default_timeout_ms(),
            connection_timeout_ms: default_connection_timeout_ms(),
            p2p: P2pConfig::default(),
            consensus: ConsensusConfig::default(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Validate)]
pub struct P2pConfig {
    #[serde(default = "default_p2p_port")]
    pub port: Option<u16>,
    #[serde(default = "default_connection_pool_size")]
    #[validate(range(min = 1))]
    pub connection_pool_size: usize,
    #[serde(default)]
    pub enable_tls: bool,
}

impl Default for P2pConfig {
    fn default() -> Self {
        P2pConfig {
            port: default_p2p_port(),
            connection_pool_size: default_connection_pool_size(),
            enable_tls: false,
        }
    }
}

#[derive(Debug, Deserialize, Clone, Validate)]
pub struct ConsensusConfig {
    #[serde(default = "default_max_message_queue_size")]
    pub max_message_queue_size: usize, // controls the back-pressure at the Raft level
    #[serde(default = "default_tick_period_ms")]
    #[validate(range(min = 1))]
    pub tick_period_ms: u64,
    #[serde(default = "default_bootstrap_timeout_sec")]
    #[validate(range(min = 1))]
    pub bootstrap_timeout_sec: u64,
    #[validate(range(min = 1))]
    #[serde(default = "default_message_timeout_tics")]
    pub message_timeout_ticks: u64,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        ConsensusConfig {
            max_message_queue_size: default_max_message_queue_size(),
            tick_period_ms: default_tick_period_ms(),
            bootstrap_timeout_sec: default_bootstrap_timeout_sec(),
            message_timeout_ticks: default_message_timeout_tics(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Validate)]
pub struct TlsConfig {
    #[serde(default = "default_tls_cert")]
    pub cert: String,
    #[serde(default = "default_tls_key")]
    pub key: String,
    #[serde(default = "default_tls_ca_cert")]
    pub ca_cert: String,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            cert: default_tls_cert(),
            key: default_tls_key(),
            ca_cert: default_tls_ca_cert(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Validate)]
pub struct Settings {
    #[serde(default = "default_debug")]
    pub debug: bool,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[validate]
    pub storage: StorageConfig,
    #[validate]
    pub service: ServiceConfig,
    #[validate]
    pub cluster: ClusterConfig,
    #[serde(default = "default_telemetry_disabled")]
    pub telemetry_disabled: bool,
    #[validate]
    pub tls: TlsConfig,
    #[serde(skip)]
    pub default_config_warning: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            debug: default_debug(),
            log_level: default_log_level(),
            storage: StorageConfig::default(),
            service: ServiceConfig::default(),
            cluster: ClusterConfig::default(),
            telemetry_disabled: default_telemetry_disabled(),
            tls: TlsConfig::default(),
            default_config_warning: true,
        }
    }
}

impl Settings {
    #[allow(dead_code)]
    pub fn validate_and_warn(&self) {
        if self.default_config_warning {
            log::warn!("Configuration files not found. Using default configuration.");
        }

        if let Err(ref errs) = self.validate() {
            validation::warn_validation_errors("Settings configuration file", errs);
        }
    }
}

fn default_telemetry_disabled() -> bool {
    false
}

fn default_debug() -> bool {
    false
}

fn default_log_level() -> String {
    "INFO".to_string()
}

fn default_timeout_ms() -> u64 {
    DEFAULT_GRPC_TIMEOUT.as_millis() as u64
}

fn default_connection_timeout_ms() -> u64 {
    DEFAULT_CONNECT_TIMEOUT.as_millis() as u64
}

fn default_tick_period_ms() -> u64 {
    100
}

// Should not be less than `DEFAULT_META_OP_WAIT` as bootstrapping perform sync. consensus meta operations.
fn default_bootstrap_timeout_sec() -> u64 {
    15
}

fn default_max_message_queue_size() -> usize {
    100
}

fn default_connection_pool_size() -> usize {
    DEFAULT_POOL_SIZE
}

fn default_message_timeout_tics() -> u64 {
    10
}

fn default_p2p_port() -> Option<u16> {
    Some(6335)
}

fn default_tls_cert() -> String {
    "./tls/cert.pem".to_string()
}

fn default_tls_key() -> String {
    "./tls/key.pem".to_string()
}

fn default_tls_ca_cert() -> String {
    "./tls/cacert.pem".to_string()
}

impl Settings {
    #[allow(dead_code)]
    pub fn new(config_path: Option<String>) -> Result<Self, ConfigError> {
        let config_path = config_path.unwrap_or_else(|| "config/config".into());
        let env: String = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());

        if env::var("RUN_MODE").is_ok()
            && Config::builder()
                .add_source(File::with_name(&config_path))
                .add_source(File::with_name(&format!("config/{env}")))
                .build()
                .is_err()
        {
            return Err(ConfigError::Message("`RUN_MODE` environment variable is set, but couldn't find matching configuration files".to_string()));
        }

        match Config::builder()
            // Start off by merging in the "default" configuration file
            .add_source(File::with_name(&config_path))
            // Add in the current environment file
            // Default to 'development' env
            // Note that this file is _optional_
            .add_source(File::with_name(&format!("config/{env}")).required(false))
            // Add in a local configuration file
            // This file shouldn't be checked in to git
            .add_source(File::with_name("config/local").required(false))
            // Add in settings from the environment (with a prefix of APP)
            // Eg.. `QDRANT_DEBUG=1 ./target/app` would set the `debug` key
            .add_source(Environment::with_prefix("QDRANT").separator("__"))
            .build()
        {
            Ok(s) => {
                // You can deserialize (and thus freeze) the entire configuration as
                s.try_deserialize()
            }
            Err(_) => Ok(Self::default()),
        }
    }
}

/// Returns the number of maximum actix workers.
#[allow(dead_code)]
pub fn max_web_workers(settings: &Settings) -> usize {
    let max_workers = settings
        .service
        .max_workers
        .unwrap_or(settings.storage.performance.max_search_threads);

    if max_workers > 0 {
        max_workers
    } else {
        let num_cpu = get_num_cpus();
        std::cmp::max(1, num_cpu - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let key = "RUN_MODE";
        env::set_var(key, "TEST");

        // Read config
        let config = Settings::new(None).unwrap();

        // Validate
        config.validate().unwrap();
    }
}
