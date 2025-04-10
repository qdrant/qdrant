#[cfg(feature = "web")]
mod actix;
mod common;
mod consensus;
mod greeting;
mod issues_setup;
mod migrations;
mod settings;
mod snapshots;
mod startup;
mod tonic;
mod tracing;

use std::io::Error;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use std::path::PathBuf;

use ::common::budget::{ResourceBudget, get_io_budget};
use ::common::cpu::get_cpu_budget;
use ::common::flags::{feature_flags, init_feature_flags};
use ::tonic::transport::Uri;
use api::grpc::transport_channel_pool::TransportChannelPool;
use clap::Parser;
use collection::shards::channel_service::ChannelService;
use consensus::Consensus;
use slog::Drain;
use startup::setup_panic_hook;
use storage::content_manager::consensus::operation_sender::OperationSender;
use storage::content_manager::consensus::persistent::Persistent;
use storage::content_manager::consensus_manager::{ConsensusManager, ConsensusStateRef};
use storage::content_manager::toc::TableOfContent;
use storage::content_manager::toc::dispatcher::TocDispatcher;
use storage::dispatcher::Dispatcher;
use storage::rbac::Access;
#[cfg(all(
    not(target_env = "msvc"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
use tikv_jemallocator::Jemalloc;

use crate::common::helpers::{
    create_general_purpose_runtime, create_search_runtime, create_update_runtime,
    load_tls_client_config,
};
use crate::common::inference::service::InferenceService;
use crate::common::telemetry::TelemetryCollector;
use crate::common::telemetry_reporting::TelemetryReporter;
use crate::greeting::welcome;
use crate::migrations::single_to_cluster::handle_existing_collections;
use crate::settings::{Cli, Settings};
use crate::snapshots::{recover_full_snapshot, recover_snapshots};
use crate::startup::{remove_started_file_indicator, touch_started_file_indicator};

use actix_web::rt::System;
use storage::types::StorageConfig;

#[cfg(all(
    not(target_env = "msvc"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const FULL_ACCESS: Access = Access::full("For main");

/// Qdrant (read: quadrant ) is a vector similarity search engine.
/// It provides a production-ready service with a convenient API to store, search, and manage points - vectors with an additional payload.
///
/// This CLI starts a Qdrant peer/server.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Uri of the peer to bootstrap from in case of multi-peer deployment.
    /// If not specified - this peer will be considered as a first in a new deployment.
    #[arg(long, value_parser, value_name = "URI", env = "QDRANT_BOOTSTRAP")]
    bootstrap: Option<Uri>,
    /// Uri of this peer.
    /// Other peers should be able to reach it by this uri.
    ///
    /// This value has to be supplied if this is the first peer in a new deployment.
    ///
    /// In case this is not the first peer and it bootstraps the value is optional.
    /// If not supplied then qdrant will take internal grpc port from config and derive the IP address of this peer on bootstrap peer (receiving side)
    #[arg(long, value_parser, value_name = "URI", env = "QDRANT_URI")]
    uri: Option<Uri>,

    /// Force snapshot re-creation
    /// If provided - existing collections will be replaced with snapshots.
    /// Default is to not recreate from snapshots.
    #[arg(short, long, action, default_value_t = false)]
    force_snapshot: bool,

    /// List of paths to snapshot files.
    /// Format: <snapshot_file_path>:<target_collection_name>
    ///
    /// WARN: Do not use this option if you are recovering collection in existing distributed cluster.
    /// Use `/collections/<collection-name>/snapshots/recover` API instead.
    #[arg(long, value_name = "PATH:NAME", alias = "collection-snapshot")]
    snapshot: Option<Vec<String>>,

    /// Path to snapshot of multiple collections.
    /// Format: <snapshot_file_path>
    ///
    /// WARN: Do not use this option if you are recovering collection in existing distributed cluster.
    /// Use `/collections/<collection-name>/snapshots/recover` API instead.
    #[arg(long, value_name = "PATH")]
    storage_snapshot: Option<String>,

    /// Path to an alternative configuration file.
    /// Format: <config_file_path>
    ///
    /// Default path: config/config.yaml
    #[arg(long, value_name = "PATH")]
    config_path: Option<String>,

    /// Disable telemetry sending to developers
    /// If provided - telemetry collection will be disabled.
    /// Read more: <https://qdrant.tech/documentation/guides/telemetry>
    #[arg(long, action, default_value_t = false)]
    disable_telemetry: bool,

    /// Run stacktrace collector. Used for debugging.
    #[arg(long, action, default_value_t = false)]
    stacktrace: bool,

    /// Reinit consensus state.
    /// When enabled, the service will assume the consensus should be reinitialized.
    /// The exact behavior depends on if this current node has bootstrap URI or not.
    /// If it has - it'll remove current consensus state and consensus WAL (while keeping peer ID)
    ///             and will try to receive state from the bootstrap peer.
    /// If it doesn't have - it'll remove other peers from voters promote
    ///             the current peer to the leader and the single member of the cluster.
    ///             It'll also compact consensus WAL to force snapshot
    #[arg(long, action, default_value_t = false)]
    reinit: bool,

    /// Enable read-only mode
    /// If provided - all write operations will be rejected
    #[arg(long, action, default_value_t = false)]
    read_only: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let settings = Settings::new(cli.config.clone(), &cli)?;

    // Initialize storage
    let storage_path = cli
        .config
        .map(|p| p.parent().unwrap().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    let snapshots_path = storage_path.join("snapshots");

    // Create storage directories if not in read-only mode
    if !settings.service.read_only {
        std::fs::create_dir_all(&storage_path)?;
        std::fs::create_dir_all(&snapshots_path)?;
    }

    let toc = Arc::new(TableOfContent::new(
        storage_path,
        snapshots_path,
        &settings.storage,
        settings.service.read_only,
    )?);

    let dispatcher = Arc::new(Dispatcher::new(toc));

    // Initialize HTTP server
    let http_server = actix::init(
        dispatcher.clone(),
        Arc::new(parking_lot::Mutex::new(common::telemetry::TelemetryCollector::new())),
        None,
        settings.clone(),
        common::tracing::LoggerHandle::default(),
    )?;

    // Initialize gRPC server
    let grpc_server = tonic::init(
        dispatcher,
        Arc::new(parking_lot::Mutex::new(
            common::telemetry_ops::requests_telemetry::TonicTelemetryCollector::new(),
        )),
        settings,
        settings.service.grpc_port,
        System::new().handle(),
    )?;

    // Run servers
    System::new().block_on(async {
        tokio::select! {
            _ = http_server => {},
            _ = grpc_server => {},
        }
    });

    Ok(())
}
