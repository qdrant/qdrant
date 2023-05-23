use std::{env, io};

use api::grpc::transport_channel_pool::{
    DEFAULT_CONNECT_TIMEOUT, DEFAULT_GRPC_TIMEOUT, DEFAULT_POOL_SIZE,
};
use collection::operations::validation;
use config::{Config, ConfigError, Environment, File, FileFormat};
use segment::common::cpu::get_num_cpus;
use serde::Deserialize;
use storage::types::StorageConfig;
use validator::Validate;

const DEFAULT_CONFIG: &str = include_str!("../config/config.yaml");

#[derive(Debug, Deserialize, Validate, Clone)]
pub struct ServiceConfig {
    #[validate(length(min = 1))]
    pub host: String,
    pub http_port: u16,
    pub grpc_port: Option<u16>, // None means that gRPC is disabled
    pub max_request_size_mb: usize,
    pub max_workers: Option<usize>,
    #[serde(default = "default_cors")]
    pub enable_cors: bool,
    #[serde(default)]
    pub enable_tls: bool,
    #[serde(default)]
    pub verify_https_client_certificate: bool,
    pub api_key: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Default, Validate)]
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

#[derive(Debug, Deserialize, Clone, Validate)]
pub struct P2pConfig {
    #[serde(default)]
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
            port: None,
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
    pub cert: String,
    pub key: String,
    pub ca_cert: String,
    #[serde(default = "default_tls_cert_ttl")]
    #[validate(range(min = 1))]
    pub cert_ttl: Option<u64>,
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
    #[serde(default)]
    #[validate]
    pub cluster: ClusterConfig,
    #[serde(default = "default_telemetry_disabled")]
    pub telemetry_disabled: bool,
    #[validate]
    pub tls: Option<TlsConfig>,
    // Can't use `#[serde(skip)]` here. It prevents overriding the value later.
    #[serde(default)]
    pub found_config_files: bool,
}

impl Settings {
    pub fn tls(&self) -> io::Result<&TlsConfig> {
        self.tls
            .as_ref()
            .ok_or_else(Self::tls_config_is_undefined_error)
    }

    pub fn tls_config_is_undefined_error() -> io::Error {
        io::Error::new(
            io::ErrorKind::Other,
            "TLS config is not defined in the Qdrant config file",
        )
    }

    #[allow(dead_code)]
    pub fn validate_and_warn(&self) {
        if !self.found_config_files {
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

fn default_cors() -> bool {
    true
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

const fn default_tls_cert_ttl() -> Option<u64> {
    // Default one hour
    Some(3600)
}

impl Settings {
    #[allow(dead_code)]
    pub fn new(config_path: Option<String>) -> Result<Self, ConfigError> {
        let config_path = config_path.unwrap_or_else(|| "config/config".into());
        let env = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());

        if env::var("RUN_MODE").is_ok()
            && Config::builder()
                .add_source(File::with_name(&config_path))
                .add_source(File::with_name(&format!("config/{env}")))
                .build()
                .is_err()
        {
            return Err(ConfigError::Message("`RUN_MODE` environment variable is set, but couldn't find matching configuration files".to_string()));
        }

        let mut s = Config::builder()
            // Start with the default configuration file contents at compile time
            .add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Yaml));

        let found_config_files = Config::builder()
            .add_source(File::with_name(&config_path))
            .build()
            .is_ok();

        s = s
            // Then merge in the default configuration file contents at run time
            .add_source(File::with_name(&config_path).required(false))
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
            .set_override("found_config_files", found_config_files)?;

        let s = s.build()?;

        // You can deserialize (and thus freeze) the entire configuration as
        s.try_deserialize()
    }
}

/// Returns the number of maximum actix workers.
#[allow(dead_code)]
pub fn max_web_workers(settings: &Settings) -> usize {
    let max_workers = settings.service.max_workers;

    if max_workers == Some(0) {
        let num_cpu = get_num_cpus();
        std::cmp::max(1, num_cpu - 1)
    } else if max_workers.is_none() {
        settings.storage.performance.max_search_threads
    } else {
        max_workers.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use sealed_test::prelude::*;

    use super::*;

    /// Ensure we can successfully deserialize into [`Settings`] with just the default configuration.
    #[test]
    fn test_default_config() {
        Config::builder()
            .add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Yaml))
            .build()
            .expect("failed to build default config")
            .try_deserialize::<Settings>()
            .expect("failed to deserialize default config")
            .validate()
            .expect("failed to validate default config");
    }

    #[sealed_test(files = ["config/config.yaml", "config/development.yaml"])]
    fn test_runtime_development_config() {
        // `sealed_test` copies files into the same directory as the test runs in.
        // We need them in a subdirectory.
        std::fs::create_dir("config").expect("failed to create `config` subdirectory.");
        std::fs::copy("config.yaml", "config/config.yaml").expect("failed to copy `config.yaml`.");
        std::fs::copy("development.yaml", "config/development.yaml")
            .expect("failed to copy `development.yaml`.");

        let key = "RUN_MODE";
        env::set_var(key, "development");

        // Read config
        let config = Settings::new(None).expect("failed to load development config at runtime");

        // Validate
        config
            .validate()
            .expect("failed to validate development config at runtime");
        assert!(config.found_config_files)
    }

    #[sealed_test]
    fn test_no_config_files() {
        let non_existing_config_path = "config/non_existing_config".to_string();
        env::remove_var("RUN_MODE");

        // Read config
        let config = Settings::new(Some(non_existing_config_path))
            .expect("failed to load with non-existing runtime config");

        // Validate
        config
            .validate()
            .expect("failed to validate with non-existing runtime config");
        assert!(!config.found_config_files)
    }
}
