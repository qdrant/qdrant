use collection::collection_builder::simple_collection::build_simple_collection;
use tempdir::TempDir;
use wal::WalOptions;
use collection::collection::Collection;
use collection::operations::types::CollectionConfig;
use segment::types::Distance;
use collection::operations::index_def::Indexes;
use tokio::runtime::Runtime;
use tokio::runtime;
use std::sync::Arc;

pub fn simple_collection_fixture() -> (Runtime, TempDir, TempDir, Collection) {
    let segment_dir = TempDir::new("segment").unwrap();

    let wal_dir = TempDir::new("wal_test").unwrap();
    let wal_options = WalOptions {
        segment_capacity: 100,
        segment_queue_len: 0,
    };

    let collection_config = CollectionConfig {
        vector_size: 4,
        index: Indexes::Hnsw {
            m: 16,
            ef_construct: 128,
        },
        distance: Distance::Dot,
    };


    let threaded_rt: Runtime = runtime::Builder::new()
        .threaded_scheduler()
        .max_threads(2)
        .build().unwrap();


    // ToDo: Create simple optimizer here
    let optimizers = Arc::new(vec![]);

    let collection = build_simple_collection(
        5,
        segment_dir.path(),
        wal_dir.path(),
        &wal_options,
        &collection_config,
        threaded_rt.handle().clone(),
        threaded_rt.handle().clone(),
        optimizers,
    ).unwrap();

    return (threaded_rt, wal_dir, segment_dir, collection);
}