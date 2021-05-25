use crate::collection::{Collection, CollectionResult, CollectionError};
use crate::segment_manager::holders::segment_holder::SegmentHolder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use std::path::Path;
use crate::wal::SerdeWal;
use crate::operations::CollectionUpdateOperations;
use std::sync::Arc;
use tokio::runtime::Runtime;
use crate::segment_manager::simple_segment_searcher::SimpleSegmentSearcher;
use crate::segment_manager::simple_segment_updater::SimpleSegmentUpdater;
use crossbeam_channel::unbounded;
use crate::update_handler::update_handler::{UpdateHandler, Optimizer};
use segment::types::{HnswConfig};
use std::fs::create_dir_all;
use parking_lot::{RwLock, Mutex};
use crate::collection_builder::optimizers_builder::build_optimizers;
use crate::collection_builder::optimizers_builder::OptimizersConfig;
use atomicwrites::OverwriteBehavior::AllowOverwrite;
use atomicwrites::AtomicFile;
use std::io::Write;
use tokio::runtime;
use crate::config::{CollectionConfig, WalConfig, CollectionParams};

pub const COLLECTION_CONFIG_FILE: &str = "config.json";


fn save_config(path: &Path, config: &CollectionConfig) -> CollectionResult<()> {
    let config_path = path.join(COLLECTION_CONFIG_FILE);
    let af = AtomicFile::new(&config_path, AllowOverwrite);
    let state_bytes = serde_json::to_vec(config).unwrap();
    af.write(|f| {
        f.write_all(&state_bytes)
    }).or_else(move |err|
        Err(CollectionError::ServiceError {
            error: format!("Can't write {:?}, error: {}", config_path, err)
        })
    )?;
    Ok(())
}

pub fn construct_collection(
    segment_holder: SegmentHolder,
    config: CollectionConfig,
    wal: SerdeWal<CollectionUpdateOperations>,
    search_runtime: Arc<Runtime>,  // from service
    optimizers: Arc<Vec<Box<Optimizer>>>,
) -> Collection {
    let segment_holder = Arc::new(RwLock::new(segment_holder));

    let optimize_runtime = Arc::new(runtime::Builder::new_multi_thread()
        .max_threads(2)
        .build().unwrap());

    let locked_wal = Arc::new(Mutex::new(wal));

    let searcher = SimpleSegmentSearcher::new(
        segment_holder.clone(),
        search_runtime,
    );

    let updater = SimpleSegmentUpdater::new(segment_holder.clone());
    // ToDo: Move tx-rx into updater, so Collection should not know about it.
    let (tx, rx) = unbounded();

    let update_handler = Arc::new(UpdateHandler::new(
        optimizers,
        rx,
        optimize_runtime.clone(),
        segment_holder.clone(),
        locked_wal.clone(),
        config.optimizer_config.flush_interval_sec,
    ));

    let collection = Collection {
        segments: segment_holder.clone(),
        config,
        wal: locked_wal,
        searcher: Arc::new(searcher),
        update_handler,
        updater: Arc::new(updater),
        runtime_handle: optimize_runtime,
        update_sender: tx,
    };

    return collection;
}


/// Creates new empty collection with given configuration
pub fn build_collection(
    collection_path: &Path,
    wal_config: &WalConfig,  // from config
    collection_params: &CollectionParams,  //  from user
    search_runtime: Arc<Runtime>,  // from service
    optimizers_config: &OptimizersConfig,
    hnsw_config: &HnswConfig,
) -> CollectionResult<Collection> {
    let wal_path = collection_path
        .join("wal");

    create_dir_all(&wal_path)
        .or_else(|err| Err(CollectionError::ServiceError {
            error: format!("Can't create collection directory. Error: {}", err)
        }))?;

    let segments_path = collection_path.join("segments");

    create_dir_all(&segments_path)
        .or_else(|err| Err(CollectionError::ServiceError {
            error: format!("Can't create collection directory. Error: {}", err)
        }))?;

    let mut segment_holder = SegmentHolder::new();

    for _sid in 0..optimizers_config.max_segment_number {
        let segment = build_simple_segment(
            segments_path.as_path(),
            collection_params.vector_size,
            collection_params.distance)?;
        segment_holder.add(segment);
    }

    let wal: SerdeWal<CollectionUpdateOperations> = SerdeWal::new(wal_path.to_str().unwrap(), &wal_config.into())?;

    let collection_config = CollectionConfig {
        params: collection_params.clone(),
        hnsw_config: hnsw_config.clone(),
        optimizer_config: optimizers_config.clone(),
        wal_config: wal_config.clone()
    };

    save_config(collection_path, &collection_config)?;

    let optimizers = build_optimizers(
        collection_path,
        &collection_params,
        &optimizers_config,
        &collection_config.hnsw_config
    );

    let collection = construct_collection(
        segment_holder,
        collection_config,
        wal,
        search_runtime,
        optimizers,
    );

    Ok(collection)
}

