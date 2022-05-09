mod common;

use clap::Parser;
use collection::collection_manager::optimizers::indexing_optimizer::IndexingOptimizer;
use collection::collection_manager::optimizers::segment_optimizer::{
    OptimizerThresholds, SegmentOptimizer,
};
use collection::collection_manager::optimizers::vacuum_optimizer::VacuumOptimizer;
use collection::config::CollectionParams;
use collection::optimizers_builder::OptimizersConfig;
use collection::shard::Shard;
use collection::Collection;
use log::info;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use tokio::runtime::Runtime;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[derive(Parser, Debug)]
#[clap(version, about)]
struct Args {
    /// Path to the collection
    #[clap(short, long)]
    pub collection: String,
}

fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "DEBUG");
    env_logger::init();
    let args: Args = Args::parse();

    let rt = Runtime::new().expect("create runtime");

    let collection_path = Path::new(&args.collection);

    let collection_name = collection_path
        .file_name()
        .expect("Can't resolve a filename of one of the collection files")
        .to_str()
        .expect("A filename of one of the collection files is not a valid UTF-8")
        .to_string();

    {
        #[cfg(feature = "dhat-heap")]
        let _profiler = dhat::Profiler::new_heap();
        {
            eprintln!("collection_name = {:#?}", collection_name);

            let mut collection =
                rt.block_on(Collection::load(collection_name.clone(), &collection_path));
            rt.block_on(collection.before_drop());

            let shard = collection.shard_by_id(0);

            match shard {
                Shard::Local(local_shard) => {
                    let segments_path = local_shard.path.join("segments");
                    let temp_segments_path = local_shard.path.join("temp_segments");

                    let optimizer = VacuumOptimizer::new(
                        0.1,
                        1,
                        OptimizerThresholds {
                            memmap_threshold: 10000000,
                            indexing_threshold: 1000,
                            payload_indexing_threshold: 10000000,
                        },
                        segments_path,
                        temp_segments_path,
                        collection.config.params.clone(),
                        collection.config.hnsw_config.clone(),
                    );

                    sleep(Duration::from_secs(10));

                    let (&sid, _) = local_shard.segments.read().iter().next().unwrap();
                    optimizer.optimize(
                        local_shard.segments.clone(),
                        vec![sid],
                        &AtomicBool::new(false),
                    );

                    let (&sid, _) = local_shard.segments.read().iter().next().unwrap();
                    optimizer.optimize(
                        local_shard.segments.clone(),
                        vec![sid],
                        &AtomicBool::new(false),
                    );

                    sleep(Duration::from_secs(10));
                }
                Shard::Remote(_) => {}
            }
        }
    }

    Ok(())
}
