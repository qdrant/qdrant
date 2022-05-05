use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::env;
use storage::types::StorageConfig;

#[derive(Debug, Deserialize, Clone)]
pub struct ServiceConfig {
    pub host: String,
    pub http_port: u16,
    pub grpc_port: Option<u16>, // None means that gRPC is disabled
    pub max_request_size_mb: usize,
    pub max_workers: Option<usize>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ClusterConfig {
    pub enabled: bool, // disabled by default
    #[serde(default)]
    pub p2p: P2pConfig,
    #[serde(default)]
    pub consensus: ConsensusConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct P2pConfig {
    #[serde(default)]
    pub p2p_port: Option<u16>,
    #[serde(default = "default_timeout_ms")]
    pub p2p_grpc_timeout_ms: u64,
}

impl Default for P2pConfig {
    fn default() -> Self {
        P2pConfig {
            p2p_port: None,
            p2p_grpc_timeout_ms: default_timeout_ms(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ConsensusConfig {
    #[serde(default = "default_max_message_queue_size")]
    pub max_message_queue_size: usize, // controls the back-pressure at the Raft level
    #[serde(default = "default_tick_period_ms")]
    pub tick_period_ms: u64,
    #[serde(default = "default_timeout_ms")]
    pub message_timeout_ms: u64,
    #[serde(default = "default_bootstrap_timeout_sec")]
    pub bootstrap_timeout_sec: u64,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        ConsensusConfig {
            max_message_queue_size: default_max_message_queue_size(),
            tick_period_ms: default_tick_period_ms(),
            message_timeout_ms: default_timeout_ms(),
            bootstrap_timeout_sec: default_bootstrap_timeout_sec(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub debug: bool,
    pub log_level: String,
    pub storage: StorageConfig,
    pub service: ServiceConfig,
    #[serde(default)]
    pub cluster: ClusterConfig,
}

fn default_timeout_ms() -> u64 {
    1000
}

fn default_tick_period_ms() -> u64 {
    100
}

fn default_bootstrap_timeout_sec() -> u64 {
    5
}

fn default_max_message_queue_size() -> usize {
    100
}

impl Settings {
    #[allow(dead_code)]
    pub fn new() -> Result<Self, ConfigError> {
        let env = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());

        let s = Config::builder()
            // Start off by merging in the "default" configuration file
            .add_source(File::with_name("config/config"))
            // Add in the current environment file
            // Default to 'development' env
            // Note that this file is _optional_
            .add_source(File::with_name(&format!("config/{}", env)).required(false))
            // Add in a local configuration file
            // This file shouldn't be checked in to git
            .add_source(File::with_name("config/local").required(false))
            // Add in settings from the environment (with a prefix of APP)
            // Eg.. `QDRANT_DEBUG=1 ./target/app` would set the `debug` key
            .add_source(Environment::with_prefix("QDRANT").separator("__"))
            .build()?;

        // You can deserialize (and thus freeze) the entire configuration as
        s.try_deserialize()
    }
}

/// Returns the number of maximum actix workers.
#[allow(dead_code)]
pub fn max_web_workers(settings: &Settings) -> usize {
    let max_workers = settings.service.max_workers;

    if max_workers == Some(0) {
        let num_cpu = num_cpus::get();
        std::cmp::max(1, num_cpu - 1)
    } else if max_workers == None {
        settings.storage.performance.max_search_threads
    } else {
        max_workers.unwrap()
    }
}
