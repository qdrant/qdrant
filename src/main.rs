#[cfg(feature = "web")]
mod actix;
pub mod common;
mod consensus;
mod greeting;
mod settings;
mod tonic;
mod user_telemetry;

use collection::ChannelService;
use consensus::Consensus;
use log::LevelFilter;
use slog::Drain;
use std::io::Error;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use storage::content_manager::consensus_state::{ConsensusState, ConsensusStateRef, Persistent};
use storage::Dispatcher;

use ::tonic::transport::Uri;
use api::grpc::transport_channel_pool::TransportChannelPool;
use clap::Parser;
use storage::content_manager::toc::TableOfContent;

use crate::common::helpers::create_search_runtime;
use crate::greeting::welcome;
use crate::settings::Settings;
use crate::user_telemetry::UserTelemetryCollector;

/// Qdrant (read: quadrant ) is a vector similarity search engine.
/// It provides a production-ready service with a convenient API to store, search, and manage points - vectors with an additional payload.
///
/// This CLI starts a Qdrant peer/server.
#[derive(Parser, Debug)]
#[clap(version, about)]
struct Args {
    /// Uri of the peer to bootstrap from in case of multi-peer deployment.
    /// If not specified - this peer will be considered as a first in a new deployment.
    #[clap(long, value_parser, value_name = "URI")]
    bootstrap: Option<Uri>,
    /// Uri of this peer.
    /// Other peers should be able to reach it by this uri.
    ///
    /// This value has to be supplied if this is the first peer in a new deployment.
    ///
    /// In case this is not the first peer and it bootstraps the value is optional.
    /// If not supplied then qdrant will take internal grpc port from config and derive the IP address of this peer on bootstrap peer (receiving side)
    #[clap(long, value_parser, value_name = "URI")]
    uri: Option<Uri>,
}

fn main() -> anyhow::Result<()> {
    let settings = Settings::new().expect("Can't read config.");
    let is_info = settings.log_level.to_ascii_uppercase() == "INFO";
    let mut log_builder = env_logger::Builder::new();

    log_builder
        // Timestamp in millis
        .format_timestamp_millis()
        // Parse user defined log level configuration
        .parse_filters(&settings.log_level)
        // h2 is very verbose and we have many network operations,
        // so it is limited to only errors
        .filter_module("h2", LevelFilter::Error)
        .filter_module("tower", LevelFilter::Warn);

    if is_info {
        // Additionally filter verbose modules if no extended logging configuration is provided
        log_builder
            .filter_module("wal", LevelFilter::Warn)
            .filter_module("raft::raft", LevelFilter::Warn);
    };

    log_builder.init();

    welcome();

    // create user telemetry collector and put settings to telemetry
    let mut telemetry_collector = UserTelemetryCollector::new();
    telemetry_collector.put_settings(settings.clone());

    // Create and own search runtime out of the scope of async context to ensure correct
    // destruction of it
    let runtime = create_search_runtime(settings.storage.performance.max_search_threads)
        .expect("Can't create runtime.");
    let runtime_handle = runtime.handle().clone();

    let (propose_sender, propose_receiver) = std::sync::mpsc::channel();
    let args = Args::parse();
    let persistent_consensus_state =
        Persistent::load_or_init(&settings.storage.storage_path, args.bootstrap.is_none())?;
    let mut channel_service = ChannelService::default();
    if settings.cluster.enabled {
        let p2p_grpc_timeout = Duration::from_millis(settings.cluster.grpc_timeout_ms);
        channel_service.channel_pool = Arc::new(TransportChannelPool::new(
            p2p_grpc_timeout,
            settings.cluster.p2p.connection_pool_size,
        ));
        channel_service.id_to_address = persistent_consensus_state.peer_address_by_id.clone();
    }
    let toc = TableOfContent::new(
        &settings.storage,
        runtime,
        channel_service.clone(),
        persistent_consensus_state.this_peer_id(),
    );
    runtime_handle.block_on(async {
        for collection in toc.all_collections().await {
            log::debug!("Loaded collection: {}", collection);
        }
    });

    let toc_arc = Arc::new(toc);
    let storage_path = toc_arc.storage_path();
    let mut handles: Vec<JoinHandle<Result<(), Error>>> = vec![];
    let mut dispatcher = Dispatcher::new(toc_arc.clone());
    let consensus_state: ConsensusStateRef = ConsensusState::new(
        persistent_consensus_state,
        toc_arc.clone(),
        propose_sender,
        storage_path,
    )
    .into();
    if settings.cluster.enabled {
        dispatcher = dispatcher.with_consensus(consensus_state.clone());
    }
    let dispatcher_arc = Arc::new(dispatcher);

    if settings.cluster.enabled {
        // `raft` crate uses `slog` crate so it is needed to use `slog_stdlog::StdLog` to forward
        // logs from it to `log` crate
        let slog_logger = slog::Logger::root(slog_stdlog::StdLog.fuse(), slog::o!());

        let (mut consensus, message_sender) = Consensus::new(
            &slog_logger,
            consensus_state,
            args.bootstrap,
            args.uri.map(|uri| uri.to_string()),
            settings.cluster.p2p.port.map(|port| port as u32),
            settings.cluster.consensus.clone(),
            channel_service.channel_pool,
        )
        .expect("Can't initialize consensus");
        thread::Builder::new()
            .name("consensus".to_string())
            .spawn(move || {
                if let Err(err) = consensus.start() {
                    log::error!("Consensus stopped with error: {err}")
                }
            })?;

        let message_sender_moved = message_sender.clone();
        thread::Builder::new()
            .name("forward-proposals".to_string())
            .spawn(move || {
                while let Ok(entry) = propose_receiver.recv() {
                    if message_sender_moved
                        .send(consensus::Message::FromClient(entry))
                        .is_err()
                    {
                        log::error!("Can not forward new entry to consensus as it was stopped.");
                        break;
                    }
                }
            })?;

        if let Some(internal_grpc_port) = settings.cluster.p2p.port {
            let settings = settings.clone();
            let dispatcher_arc = dispatcher_arc.clone();
            let handle = thread::Builder::new()
                .name("grpc_internal".to_string())
                .spawn(move || {
                    tonic::init_internal(
                        dispatcher_arc.clone(),
                        settings.service.host,
                        internal_grpc_port,
                        message_sender,
                    )
                })
                .unwrap();
            handles.push(handle);
        } else {
            log::info!("gRPC internal endpoint disabled");
        }
    } else {
        log::info!("Distributed mode disabled");
    }

    #[cfg(feature = "web")]
    {
        let dispatcher_arc = dispatcher_arc.clone();
        let settings = settings.clone();
        let handle = thread::Builder::new()
            .name("web".to_string())
            .spawn(move || actix::init(dispatcher_arc.clone(), settings))
            .unwrap();
        handles.push(handle);
    }

    if let Some(grpc_port) = settings.service.grpc_port {
        let settings = settings.clone();
        let handle = thread::Builder::new()
            .name("grpc".to_string())
            .spawn(move || tonic::init(dispatcher_arc, settings.service.host, grpc_port))
            .unwrap();
        handles.push(handle);
    } else {
        log::info!("gRPC endpoint disabled");
    }

    #[cfg(feature = "service_debug")]
    {
        use parking_lot::deadlock;

        const DEADLOCK_CHECK_PERIOD: Duration = Duration::from_secs(10);

        thread::Builder::new()
            .name("deadlock_checker".to_string())
            .spawn(move || loop {
                thread::sleep(DEADLOCK_CHECK_PERIOD);
                let deadlocks = deadlock::check_deadlock();
                if deadlocks.is_empty() {
                    continue;
                }

                let mut error = format!("{} deadlocks detected\n", deadlocks.len());
                for (i, threads) in deadlocks.iter().enumerate() {
                    error.push_str(&format!("Deadlock #{}\n", i));
                    for t in threads {
                        error.push_str(&format!(
                            "Thread Id {:#?}\n{:#?}\n",
                            t.thread_id(),
                            t.backtrace()
                        ));
                    }
                }
                log::error!("{}", error);
            })
            .unwrap();
    }

    for handle in handles.into_iter() {
        handle.join().expect("Couldn't join on the thread")?;
    }
    drop(toc_arc);
    drop(settings);
    Ok(())
}
