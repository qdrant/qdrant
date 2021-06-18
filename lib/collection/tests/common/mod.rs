use collection::collection_builder::collection_builder::build_collection;
use collection::collection::Collection;
use segment::types::{Distance};
use tokio::runtime::Runtime;
use tokio::runtime;
use std::path::Path;
use collection::collection_builder::optimizers_builder::OptimizersConfig;
use collection::collection_builder::collection_loader::load_collection;
use collection::config::{WalConfig, CollectionParams};


pub const TEST_OPTIMIZERS_CONFIG: OptimizersConfig = OptimizersConfig {
    deleted_threshold: 0.9,
    vacuum_min_vector_number: 1000,
    max_segment_number: 10,
    memmap_threshold: 100_000,
    indexing_threshold: 50_000,
    payload_indexing_threshold: 20_000,
    flush_interval_sec: 30,
};


#[allow(dead_code)]
pub fn load_collection_fixture(collection_path: &Path) -> (Runtime, Collection) {
    let threaded_rt = runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .build().unwrap();


    let collection = load_collection(
        collection_path,
        threaded_rt.handle().clone(),
    );

    return (threaded_rt, collection);
}

pub fn simple_collection_fixture(collection_path: &Path) -> (Runtime, Collection) {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0
    };

    let collection_params = CollectionParams {
        vector_size: 4,
        distance: Distance::Dot,
    };

    let threaded_rt = runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .build().unwrap();


    let collection = build_collection(
        collection_path,
        &wal_config,
        &collection_params,
        threaded_rt.handle().clone(),
        &TEST_OPTIMIZERS_CONFIG,
        &Default::default()
    ).unwrap();

    return (threaded_rt, collection);
}