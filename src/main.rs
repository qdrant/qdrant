#[cfg(feature = "web")]
mod actix;
pub mod common;
#[cfg(feature = "consensus")]
mod consensus;
mod settings;
mod tonic;

#[cfg(feature = "consensus")]
use consensus::Consensus;
#[cfg(feature = "consensus")]
use slog::Drain;
use std::io::Error;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use ::tonic::transport::Uri;
use clap::Parser;
use storage::content_manager::toc::TableOfContent;

use crate::common::helpers::create_search_runtime;
use crate::settings::Settings;

/// Qdrant (read: quadrant ) is a vector similarity search engine.
/// It provides a production-ready service with a convenient API to store, search, and manage points - vectors with an additional payload.
///
/// This CLI starts a Qdrant peer/server.
#[derive(Parser, Debug)]
#[clap(version, about)]
struct Args {
    /// Uri of the peer to bootstrap from in case of multi-peer deployment.
    /// If not specified - this peer will be considered as a first in a new deployment.
    #[clap(long, value_name = "URI")]
    bootstrap: Option<Uri>,
    /// Uri of this peer.
    /// Other peers should be able to reach it by this uri.
    /// Default is left for single peer deployments only.
    #[clap(long, value_name = "URI", default_value_t=Uri::from_static("127.0.0.1:8080"))]
    uri: Uri,
}

fn main() -> std::io::Result<()> {
    #[cfg(feature = "consensus")]
    let args = Args::parse();
    let settings = Settings::new().expect("Can't read config.");
    std::env::set_var("RUST_LOG", &settings.log_level);
    env_logger::init();

    // Create and own search runtime out of the scope of async context to ensure correct
    // destruction of it
    let runtime = create_search_runtime(settings.storage.performance.max_search_threads)
        .expect("Can't create runtime.");
    let runtime_handle = runtime.handle().clone();

    #[allow(unused_mut)]
    let mut toc = TableOfContent::new(&settings.storage, runtime);
    runtime_handle.block_on(async {
        for collection in toc.all_collections().await {
            log::info!("Loaded collection: {}", collection);
        }
    });

    #[cfg(feature = "consensus")]
    let (propose_sender, propose_receiver) = std::sync::mpsc::channel();
    #[cfg(feature = "consensus")]
    toc.with_propose_sender(propose_sender);

    let toc_arc = Arc::new(toc);
    let mut handles: Vec<JoinHandle<Result<(), Error>>> = vec![];

    #[cfg(feature = "consensus")]
    {
        // `raft` crate uses `slog` crate so it is needed to use `slog_stdlog::StdLog` to forward
        // logs from it to `log` crate
        let slog_logger = slog::Logger::root(slog_stdlog::StdLog.fuse(), slog::o!());

        let (mut consensus, message_sender) = Consensus::new(
            &slog_logger,
            toc_arc.clone().into(),
            args.bootstrap,
            args.uri,
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

        if let Some(internal_grpc_port) = settings.service.internal_grpc_port {
            let toc_arc = toc_arc.clone();
            let settings = settings.clone();
            let handle = thread::Builder::new()
                .name("grpc_internal".to_string())
                .spawn(move || {
                    tonic::init_internal(
                        toc_arc,
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
    }

    #[cfg(feature = "web")]
    {
        let toc_arc = toc_arc.clone();
        let settings = settings.clone();
        let handle = thread::Builder::new()
            .name("web".to_string())
            .spawn(move || actix::init(toc_arc, settings))
            .unwrap();
        handles.push(handle);
    }

    if let Some(grpc_port) = settings.service.grpc_port {
        let toc_arc = toc_arc.clone();
        let settings = settings.clone();
        let handle = thread::Builder::new()
            .name("grpc".to_string())
            .spawn(move || tonic::init(toc_arc, settings.service.host, grpc_port))
            .unwrap();
        handles.push(handle);
    } else {
        log::info!("gRPC endpoint disabled");
    }

    #[cfg(feature = "service_debug")]
    {
        use parking_lot::deadlock;
        use std::time::Duration;

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
