use collection::collection_builder::collection_builder::build_collection;
use wal::WalOptions;
use collection::collection::Collection;
use segment::types::{Distance, SegmentConfig, Indexes};
use tokio::runtime::Runtime;
use tokio::runtime;
use std::sync::Arc;
use std::path::Path;


pub fn simple_collection_fixture(collection_path: &Path) -> (Runtime, Collection) {

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


    let threaded_rt: Runtime = runtime::Builder::new()
        .threaded_scheduler()
        .max_threads(2)
        .build().unwrap();


    // ToDo: Create simple optimizer here
    let optimizers = Arc::new(vec![]);

    let collection = build_collection(
        collection_path,
        &wal_options,
        &collection_config,
        threaded_rt.handle().clone(),
        threaded_rt.handle().clone(),
        optimizers,
        30,
    ).unwrap();

    return (threaded_rt, collection);
}