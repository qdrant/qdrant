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
use crate::settings::Settings;
use crate::snapshots::{recover_full_snapshot, recover_snapshots};
use crate::startup::{remove_started_file_indicator, touch_started_file_indicator};

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
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Run backtrace collector, expected to used by `rstack` crate
    if args.stacktrace {
        #[cfg(all(target_os = "linux", feature = "stacktrace"))]
        {
            let _ = rstack_self::child();
        }
        return Ok(());
    }

    let settings = Settings::new(args.config_path)?;

    // Set global feature flags, sourced from configuration
    init_feature_flags(settings.feature_flags);

    let reporting_enabled = !settings.telemetry_disabled && !args.disable_telemetry;

    let reporting_id = TelemetryCollector::generate_id();

    // Setup logging (no logging before this point)
    let logger_handle = tracing::setup(
        settings
            .logger
            .with_top_level_directive(settings.log_level.clone()),
    )?;

    remove_started_file_indicator();

    setup_panic_hook(reporting_enabled, reporting_id.to_string());

    memory::madvise::set_global(settings.storage.mmap_advice);
    segment::vector_storage::common::set_async_scorer(
        settings
            .storage
            .performance
            .async_scorer
            .unwrap_or_default(),
    );

    welcome(&settings);

    #[cfg(feature = "gpu")]
    if let Some(settings_gpu) = &settings.gpu {
        use segment::index::hnsw_index::gpu::*;

        // initialize GPU devices manager.
        if settings_gpu.indexing {
            set_gpu_force_half_precision(settings_gpu.force_half_precision);
            set_gpu_groups_count(settings_gpu.groups_count);

            let mut gpu_device_manager = GPU_DEVICES_MANAGER.write();
            *gpu_device_manager = match gpu_devices_manager::GpuDevicesMaganer::new(
                &settings_gpu.device_filter,
                settings_gpu.devices.as_deref(),
                settings_gpu.allow_integrated,
                settings_gpu.allow_emulated,
                true, // Currently we always wait for the free gpu device.
                settings_gpu.parallel_indexes.unwrap_or(1),
            ) {
                Ok(gpu_device_manager) => Some(gpu_device_manager),
                Err(err) => {
                    log::error!("Can't initialize GPU devices manager: {err}");
                    None
                }
            }
        }
    }

    if let Some(recovery_warning) = &settings.storage.recovery_mode {
        log::warn!("Qdrant is loaded in recovery mode: {recovery_warning}");
        log::warn!(
            "Read more: https://qdrant.tech/documentation/guides/administration/#recovery-mode"
        );
    }

    // Validate as soon as possible, but we must initialize logging first
    settings.validate_and_warn();

    // Report feature flags that are enabled for easier debugging
    let flags = feature_flags();
    if !flags.is_default() {
        log::debug!("Feature flags: {flags:?}");
    }

    let bootstrap = if args.bootstrap == args.uri {
        if args.bootstrap.is_some() {
            log::warn!(
                "Bootstrap URI is the same as this peer URI. Consider this peer as a first in a new deployment.",
            );
        }
        None
    } else {
        args.bootstrap
    };

    // Saved state of the consensus.
    let persistent_consensus_state = Persistent::load_or_init(
        &settings.storage.storage_path,
        bootstrap.is_none(),
        args.reinit,
    )?;

    let is_distributed_deployment = settings.cluster.enabled;

    let temp_path = settings.storage.temp_path.as_deref();

    let restored_collections = if let Some(full_snapshot) = args.storage_snapshot {
        recover_full_snapshot(
            temp_path,
            &full_snapshot,
            &settings.storage.storage_path,
            args.force_snapshot,
            persistent_consensus_state.this_peer_id(),
            is_distributed_deployment,
        )
    } else if let Some(snapshots) = args.snapshot {
        // recover from snapshots
        recover_snapshots(
            &snapshots,
            args.force_snapshot,
            temp_path,
            &settings.storage.storage_path,
            persistent_consensus_state.this_peer_id(),
            is_distributed_deployment,
        )
    } else {
        vec![]
    };

    // Create and own search runtime out of the scope of async context to ensure correct
    // destruction of it
    let search_runtime = create_search_runtime(settings.storage.performance.max_search_threads)
        .expect("Can't search create runtime.");

    let update_runtime =
        create_update_runtime(settings.storage.performance.max_optimization_threads)
            .expect("Can't optimizer create runtime.");

    let general_runtime =
        create_general_purpose_runtime().expect("Can't optimizer general purpose runtime.");
    let runtime_handle = general_runtime.handle().clone();

    // Use global CPU budget for optimizations based on settings
    let cpu_budget = get_cpu_budget(settings.storage.performance.optimizer_cpu_budget);
    let io_budget = get_io_budget(settings.storage.performance.optimizer_io_budget, cpu_budget);
    let optimizer_resource_budget = ResourceBudget::new(cpu_budget, io_budget);

    // Create a signal sender and receiver. It is used to communicate with the consensus thread.
    let (propose_sender, propose_receiver) = std::sync::mpsc::channel();

    let propose_operation_sender = if settings.cluster.enabled {
        // High-level channel which could be used to send User-space consensus operations
        Some(OperationSender::new(propose_sender))
    } else {
        // We don't need sender for the single-node mode
        None
    };

    // Channel service is used to manage connections between peers.
    // It allocates required number of channels and manages proper reconnection handling
    let mut channel_service =
        ChannelService::new(settings.service.http_port, settings.service.api_key.clone());

    if is_distributed_deployment {
        // We only need channel_service in case if cluster is enabled.
        // So we initialize it with real values here
        let p2p_grpc_timeout = Duration::from_millis(settings.cluster.grpc_timeout_ms);
        let connection_timeout = Duration::from_millis(settings.cluster.connection_timeout_ms);

        let tls_config = load_tls_client_config(&settings)?;

        channel_service.channel_pool = Arc::new(TransportChannelPool::new(
            p2p_grpc_timeout,
            connection_timeout,
            settings.cluster.p2p.connection_pool_size,
            tls_config,
        ));
        channel_service.id_to_address = persistent_consensus_state.peer_address_by_id.clone();
        channel_service.id_to_metadata = persistent_consensus_state.peer_metadata_by_id.clone();
    }

    // Table of content manages the list of collections.
    // It is a main entry point for the storage.
    let toc = TableOfContent::new(
        &settings.storage,
        search_runtime,
        update_runtime,
        general_runtime,
        optimizer_resource_budget,
        channel_service.clone(),
        persistent_consensus_state.this_peer_id(),
        propose_operation_sender.clone(),
    );

    toc.clear_all_tmp_directories()?;

    // Here we load all stored collections.
    runtime_handle.block_on(async {
        for collection in toc.all_collections(&FULL_ACCESS).await {
            log::debug!("Loaded collection: {collection}");
        }
    });

    let toc_arc = Arc::new(toc);
    let storage_path = toc_arc.storage_path();

    // Holder for all actively running threads of the service: web, gPRC, consensus, etc.
    let mut handles: Vec<JoinHandle<Result<(), Error>>> = vec![];

    // Router for external queries.
    // It decides if query should go directly to the ToC or through the consensus.
    let mut dispatcher = Dispatcher::new(toc_arc.clone());

    let (telemetry_collector, dispatcher_arc, health_checker) = if is_distributed_deployment {
        let consensus_state: ConsensusStateRef = ConsensusManager::new(
            persistent_consensus_state,
            toc_arc.clone(),
            propose_operation_sender.unwrap(),
            storage_path,
        )
        .into();
        let is_new_deployment = consensus_state.is_new_deployment();

        dispatcher =
            dispatcher.with_consensus(consensus_state.clone(), settings.cluster.resharding_enabled);

        let toc_dispatcher = TocDispatcher::new(Arc::downgrade(&toc_arc), consensus_state.clone());
        toc_arc.with_toc_dispatcher(toc_dispatcher);

        let dispatcher_arc = Arc::new(dispatcher);

        // Monitoring and telemetry.
        let telemetry_collector =
            TelemetryCollector::new(settings.clone(), dispatcher_arc.clone(), reporting_id);
        let tonic_telemetry_collector = telemetry_collector.tonic_telemetry_collector.clone();

        // `raft` crate uses `slog` crate so it is needed to use `slog_stdlog::StdLog` to forward
        // logs from it to `log` crate
        let slog_logger = slog::Logger::root(slog_stdlog::StdLog.fuse(), slog::o!());

        // Runs raft consensus in a separate thread.
        // Create a pipe `message_sender` to communicate with the consensus
        let health_checker = Arc::new(common::health::HealthChecker::spawn(
            toc_arc.clone(),
            consensus_state.clone(),
            &runtime_handle,
            // NOTE: `wait_for_bootstrap` should be calculated *before* starting `Consensus` thread
            consensus_state.is_new_deployment() && bootstrap.is_some(),
        ));

        let handle = Consensus::run(
            &slog_logger,
            consensus_state.clone(),
            bootstrap,
            args.uri.map(|uri| uri.to_string()),
            settings.clone(),
            channel_service,
            propose_receiver,
            tonic_telemetry_collector,
            toc_arc.clone(),
            runtime_handle.clone(),
            args.reinit,
        )
        .expect("Can't initialize consensus");

        handles.push(handle);

        let toc_arc_clone = toc_arc.clone();
        let consensus_state_clone = consensus_state.clone();
        let _cancel_transfer_handle = runtime_handle.spawn(async move {
            consensus_state_clone.is_leader_established.await_ready();
            match toc_arc_clone
                .cancel_related_transfers("Source or target peer restarted")
                .await
            {
                Ok(_) => {
                    log::debug!("All transfers if any cancelled");
                }
                Err(err) => {
                    log::error!("Can't cancel related transfers: {err}");
                }
            }
        });

        // TODO(resharding): Remove resharding driver?
        //
        // runtime_handle.block_on(async {
        //     toc_arc.resume_resharding_tasks().await;
        // });

        let collections_to_recover_in_consensus = if is_new_deployment {
            let existing_collections =
                runtime_handle.block_on(toc_arc.all_collections(&FULL_ACCESS));
            existing_collections
                .into_iter()
                .map(|pass| pass.name().to_string())
                .collect()
        } else {
            restored_collections
        };

        if !collections_to_recover_in_consensus.is_empty() {
            runtime_handle.block_on(handle_existing_collections(
                toc_arc.clone(),
                consensus_state.clone(),
                dispatcher_arc.clone(),
                consensus_state.this_peer_id(),
                collections_to_recover_in_consensus,
            ));
        }

        (telemetry_collector, dispatcher_arc, Some(health_checker))
    } else {
        log::info!("Distributed mode disabled");
        let dispatcher_arc = Arc::new(dispatcher);

        // Monitoring and telemetry.
        let telemetry_collector =
            TelemetryCollector::new(settings.clone(), dispatcher_arc.clone(), reporting_id);
        (telemetry_collector, dispatcher_arc, None)
    };

    let tonic_telemetry_collector = telemetry_collector.tonic_telemetry_collector.clone();

    //
    // Telemetry reporting
    //

    let reporting_id = telemetry_collector.reporting_id();
    let telemetry_collector = Arc::new(tokio::sync::Mutex::new(telemetry_collector));

    if reporting_enabled {
        log::info!("Telemetry reporting enabled, id: {reporting_id}");

        runtime_handle.spawn(TelemetryReporter::run(telemetry_collector.clone()));
    } else {
        log::info!("Telemetry reporting disabled");
    }

    if settings.service.hardware_reporting == Some(true) {
        log::info!("Hardware reporting enabled");
    }

    // Setup subscribers to listen for issue-able events
    issues_setup::setup_subscribers(&settings);

    // Helper to better log start errors
    let log_err_if_any = |server_name, result| match result {
        Err(err) => {
            log::error!("Error while starting {server_name} server: {err}");
            Err(err)
        }
        ok => ok,
    };

    //
    // Inference Service
    //
    if let Some(inference_config) = settings.inference.clone() {
        match InferenceService::init_global(inference_config) {
            Ok(_) => {
                log::info!("Inference service is configured.");
            }
            Err(err) => {
                log::error!("{err}");
            }
        }
    } else {
        log::info!("Inference service is not configured.");
    }

    //
    // REST API server
    //

    #[cfg(feature = "web")]
    {
        let dispatcher_arc = dispatcher_arc.clone();
        let settings = settings.clone();
        let handle = thread::Builder::new()
            .name("web".to_string())
            .spawn(move || {
                log_err_if_any(
                    "REST",
                    actix::init(
                        dispatcher_arc.clone(),
                        telemetry_collector,
                        health_checker,
                        settings,
                        logger_handle,
                    ),
                )
            })
            .unwrap();
        handles.push(handle);
    }

    //
    // gRPC server
    //

    if let Some(grpc_port) = settings.service.grpc_port {
        let settings = settings.clone();
        let handle = thread::Builder::new()
            .name("grpc".to_string())
            .spawn(move || {
                log_err_if_any(
                    "gRPC",
                    tonic::init(
                        dispatcher_arc,
                        tonic_telemetry_collector,
                        settings,
                        grpc_port,
                        runtime_handle,
                    ),
                )
            })
            .unwrap();
        handles.push(handle);
    } else {
        log::info!("gRPC endpoint disabled");
    }

    #[cfg(feature = "service_debug")]
    {
        use std::fmt::Write;

        use parking_lot::deadlock;

        const DEADLOCK_CHECK_PERIOD: Duration = Duration::from_secs(10);

        thread::Builder::new()
            .name("deadlock_checker".to_string())
            .spawn(move || {
                loop {
                    thread::sleep(DEADLOCK_CHECK_PERIOD);
                    let deadlocks = deadlock::check_deadlock();
                    if deadlocks.is_empty() {
                        continue;
                    }

                    let mut error = format!("{} deadlocks detected\n", deadlocks.len());
                    for (i, threads) in deadlocks.iter().enumerate() {
                        writeln!(error, "Deadlock #{i}").expect("fail to writeln!");
                        for t in threads {
                            writeln!(
                                error,
                                "Thread Id {:#?}\n{:#?}",
                                t.thread_id(),
                                t.backtrace(),
                            )
                            .expect("fail to writeln!");
                        }
                    }
                    log::error!("{error}");
                }
            })
            .unwrap();
    }

    touch_started_file_indicator();

    for handle in handles {
        log::debug!(
            "Waiting for thread {} to finish",
            handle.thread().name().unwrap()
        );
        handle.join().expect("thread is not panicking")?;
    }
    drop(toc_arc);
    drop(settings);
    Ok(())
}
