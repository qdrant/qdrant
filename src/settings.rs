use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::env;
use storage::types::StorageConfig;

#[derive(Debug, Deserialize, Clone)]
pub struct ServiceConfig {
    pub host: String,
    pub port: u16,
    pub grpc_port: u16,
    pub max_request_size_mb: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub debug: bool,
    pub log_level: String,
    pub storage: StorageConfig,
    pub service: ServiceConfig,
}

impl Settings {
    #[allow(dead_code)]
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();

        // Start off by merging in the "default" configuration file
        s.merge(File::with_name("config/config"))?;

        // Add in the current environment file
        // Default to 'development' env
        // Note that this file is _optional_
        let env = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());
        s.merge(File::with_name(&format!("config/{}", env)).required(false))?;

        // Add in a local configuration file
        // This file shouldn't be checked in to git
        s.merge(File::with_name("config/local").required(false))?;

        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `QDRANT_DEBUG=1 ./target/app` would set the `debug` key
        s.merge(Environment::with_prefix("QDRANT"))?;

        // You can deserialize (and thus freeze) the entire configuration as
        s.try_into()
    }
}
