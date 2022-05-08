mod common;

use clap::Parser;
use collection::collection_manager::optimizers::indexing_optimizer::IndexingOptimizer;
use collection::collection_manager::optimizers::segment_optimizer::{
    OptimizerThresholds, SegmentOptimizer,
};
use collection::collection_manager::optimizers::vacuum_optimizer::VacuumOptimizer;
use collection::optimizers_builder::OptimizersConfig;
use collection::shard::Shard;
use collection::Collection;
use log::info;
use segment::segment_constructor::load_segment;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::thread;
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
    pub collection: Option<String>,
    /// Path to the segment
    #[clap(short, long)]
    pub segment: Option<String>,
}

fn load_collection(path: &str) -> Result<Collection, String> {
    let rt = Runtime::new().expect("create runtime");
    let collection_path = Path::new(path);
    let collection_name = collection_path
        .file_name()
        .expect("Can't resolve a filename of one of the collection files")
        .to_str()
        .expect("A filename of one of the collection files is not a valid UTF-8")
        .to_string();
    eprintln!("collection_name = {:#?}", collection_name);
    let mut collection = rt.block_on(Collection::load(
        collection_name.clone(),
        &collection_path,
        Default::default(),
    ));
    rt.block_on(collection.before_drop());
    Ok(collection)
}

fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "DEBUG");
    env_logger::init();
    let args: Args = Args::parse();

    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();
    {
        if let Some(collection_path) = args.collection {
            let collection = load_collection(&collection_path).unwrap();
        }

        if let Some(segment_path) = args.segment {
            let segment = load_segment(&Path::new(&segment_path)).unwrap();
            let id_tracker = segment.id_tracker.borrow();
        }
    }

    Ok(())
}
