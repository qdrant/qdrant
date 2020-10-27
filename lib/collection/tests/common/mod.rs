use collection::collection_builder::collection_builder::build_collection;
use wal::WalOptions;
use collection::collection::Collection;
use segment::types::{Distance, SegmentConfig, Indexes};
use tokio::runtime::Runtime;
use tokio::runtime;
use std::sync::Arc;
use std::path::Path;
use collection::collection_builder::optimizers_builder::OptimizersConfig;
use collection::collection_builder::collection_loader::load_collection;


pub const TEST_OPTIMIZERS_CONFIG: OptimizersConfig = OptimizersConfig {
    deleted_threshold: 0.9,
    vacuum_min_vector_number: 1000,
    max_segment_number: 10,
    memmap_threshold: 10_000,
    indexing_threshold: 10_000,
    flush_interval_sec: 30,
};


pub fn load_collection_fixture(collection_path: &Path) -> (Arc<Runtime>, Collection) {
    let wal_options = WalOptions {
        segment_capacity: 100,
        segment_queue_len: 0,
    };

    let threaded_rt = Arc::new(runtime::Builder::new_multi_thread()
        .max_threads(2)
        .build().unwrap());


    let collection = load_collection(
        collection_path,
        &wal_options,
        threaded_rt.clone(),
        &TEST_OPTIMIZERS_CONFIG,
    );

    return (threaded_rt, collection);
}

pub fn simple_collection_fixture(collection_path: &Path) -> (Arc<Runtime>, Collection) {
    let wal_options = WalOptions {
        segment_capacity: 100,
        segment_queue_len: 0,
    };

    let collection_config = SegmentConfig {
        vector_size: 4,
        index: Indexes::Hnsw {
            m: 16,
            ef_construct: 128,
        },
        distance: Distance::Dot,
        storage_type: Default::default(),
    };

    let threaded_rt = Arc::new(runtime::Builder::new_multi_thread()
        .max_threads(2)
        .build().unwrap());


    let collection = build_collection(
        collection_path,
        &wal_options,
        &collection_config,
        threaded_rt.clone(),
        &TEST_OPTIMIZERS_CONFIG,
    ).unwrap();

    return (threaded_rt, collection);
}