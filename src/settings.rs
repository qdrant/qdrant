use std::{env, io};

use api::grpc::models::PyroscopeConfig;
use api::grpc::transport_channel_pool::{
    DEFAULT_CONNECT_TIMEOUT, DEFAULT_GRPC_TIMEOUT, DEFAULT_POOL_SIZE,
};
use collection::operations::validation;
use config::{Config, ConfigError, Environment, File, FileFormat, Source};
use serde::Deserialize;
use storage::types::StorageConfig;
use validator::Validate;

use crate::tracing;

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
    pub read_only_api_key: Option<String>,
    #[serde(default)]
    pub jwt_rbac: Option<bool>,

    /// Directory where static files are served from.
    /// For example, the Web-UI should be placed here.
    #[serde(default)]
    pub static_content_dir: Option<String>,

    /// If serving of the static content is enabled.
    /// This includes the Web-UI. True by default.
    #[serde(default)]
    pub enable_static_content: Option<bool>,

    /// How much time is considered too long for a query to execute.
    pub slow_query_secs: Option<f32>,
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

#[derive(Debug, Deserialize, Clone)]
pub struct DebugConfig {
    pub pyroscope: PyroscopeConfig,
}

#[derive(Debug, Deserialize, Clone, Validate)]
pub struct Settings {
    #[serde(default)]
    pub log_level: Option<String>,
    #[serde(default)]
    pub logger: tracing::LoggerConfig,
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
    pub debug: Option<DebugConfig>,
    /// A list of messages for errors that happened during loading the configuration. We collect
    /// them and store them here while loading because then our logger is not configured yet.
    /// We therefore need to log these messages later, after the logger is ready.
    #[serde(default, skip)]
    pub load_errors: Vec<LogMsg>,
}

impl Settings {
    #[allow(dead_code)]
    pub fn new(custom_config_path: Option<String>) -> Result<Self, ConfigError> {
        let mut load_errors = vec![];
        let config_exists = |path| File::with_name(path).collect().is_ok();

        // Check if custom config file exists, report error if not
        if let Some(ref path) = custom_config_path {
            if !config_exists(path) {
                load_errors.push(LogMsg::Error(format!(
                    "Config file via --config-path is not found: {path}"
                )));
            }
        }

        let env = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());
        let config_path_env = format!("config/{env}");

        // Report error if main or env config files exist, report warning if not
        // Check if main and env configuration file
        load_errors.extend(
            ["config/config", &config_path_env]
                .into_iter()
                .filter(|path| !config_exists(path))
                .map(|path| LogMsg::Warn(format!("Config file not found: {path}"))),
        );

        // Configuration builder: define different levels of configuration files
        let mut config = Config::builder()
            // Start with compile-time base config
            .add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Yaml))
            // Merge main config: config/config
            .add_source(File::with_name("config/config").required(false))
            // Merge env config: config/{env}
            // Uses RUN_MODE, defaults to 'development'
            .add_source(File::with_name(&config_path_env).required(false))
            // Merge local config, not tracked in git: config/local
            .add_source(File::with_name("config/local").required(false));

        // Merge user provided config with --config-path
        if let Some(path) = custom_config_path {
            config = config.add_source(File::with_name(&path).required(false));
        }

        // Merge environment settings
        // E.g.: `QDRANT_DEBUG=1 ./target/app` would set `debug=true`
        config = config.add_source(Environment::with_prefix("QDRANT").separator("__"));

        // Build and merge config and deserialize into Settings, attach any load errors we had
        let mut settings: Settings = config.build()?.try_deserialize()?;
        settings.load_errors.extend(load_errors);
        Ok(settings)
    }

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
        // Print any load error messages we had
        self.load_errors.iter().for_each(LogMsg::log);

        if let Err(ref errs) = self.validate() {
            validation::warn_validation_errors("Settings configuration file", errs);
        }
    }
}

/// Returns the number of maximum actix workers.
pub fn max_web_workers(settings: &Settings) -> usize {
    match settings.service.max_workers {
        Some(0) => {
            let num_cpu = common::cpu::get_num_cpus();
            std::cmp::max(1, num_cpu - 1)
        }
        Some(max_workers) => max_workers,
        None => settings.storage.performance.max_search_threads,
    }
}

#[derive(Clone, Debug)]
pub enum LogMsg {
    Warn(String),
    Error(String),
}

impl LogMsg {
    fn log(&self) {
        match self {
            Self::Warn(msg) => log::warn!("{msg}"),
            Self::Error(msg) => log::error!("{msg}"),
        }
    }
}

const fn default_telemetry_disabled() -> bool {
    false
}

const fn default_cors() -> bool {
    true
}

const fn default_timeout_ms() -> u64 {
    DEFAULT_GRPC_TIMEOUT.as_millis() as u64
}

const fn default_connection_timeout_ms() -> u64 {
    DEFAULT_CONNECT_TIMEOUT.as_millis() as u64
}

const fn default_tick_period_ms() -> u64 {
    100
}

// Should not be less than `DEFAULT_META_OP_WAIT` as bootstrapping perform sync. consensus meta operations.
const fn default_bootstrap_timeout_sec() -> u64 {
    15
}

const fn default_max_message_queue_size() -> usize {
    100
}

const fn default_connection_pool_size() -> usize {
    DEFAULT_POOL_SIZE
}

const fn default_message_timeout_tics() -> u64 {
    10
}

const fn default_tls_cert_ttl() -> Option<u64> {
    // Default one hour
    Some(3600)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;

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
        env::set_var("RUN_MODE", "development");

        // `sealed_test` copies files into the same directory as the test runs in.
        // We need them in a subdirectory.
        std::fs::create_dir("config").expect("failed to create `config` subdirectory.");
        std::fs::copy("config.yaml", "config/config.yaml").expect("failed to copy `config.yaml`.");
        std::fs::copy("development.yaml", "config/development.yaml")
            .expect("failed to copy `development.yaml`.");

        // Read config
        let config = Settings::new(None).expect("failed to load development config at runtime");

        // Validate
        config
            .validate()
            .expect("failed to validate development config at runtime");
        assert!(config.load_errors.is_empty(), "must not have load errors")
    }

    #[sealed_test]
    fn test_no_config_files() {
        let non_existing_config_path = "config/non_existing_config".to_string();

        // Read config
        let config = Settings::new(Some(non_existing_config_path))
            .expect("failed to load with non-existing runtime config");

        // Validate
        config
            .validate()
            .expect("failed to validate with non-existing runtime config");
        assert!(!config.load_errors.is_empty(), "must have load errors")
    }

    #[sealed_test]
    fn test_custom_config() {
        let path = "config/custom.yaml";

        // Create custom config file
        {
            fs::create_dir("config").unwrap();
            let mut custom = fs::File::create(path).unwrap();
            write!(&mut custom, "service:\n    http_port: 9999").unwrap();
            custom.flush().unwrap();
        }

        // Load settings with custom config
        let config = Settings::new(Some(path.into())).unwrap();

        // Ensure our custom config is the most important
        assert_eq!(config.service.http_port, 9999);
    }
}
