use crate::collection::{Collection, CollectionResult, CollectionError};
use crate::segment_manager::holders::segment_holder::SegmentHolder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use std::path::Path;
use crate::wal::SerdeWal;
use crate::operations::CollectionUpdateOperations;
use wal::WalOptions;
use std::sync::Arc;
use tokio::runtime::Runtime;
use crate::segment_manager::simple_segment_searcher::SimpleSegmentSearcher;
use crate::segment_manager::simple_segment_updater::SimpleSegmentUpdater;
use crossbeam_channel::unbounded;
use crate::update_handler::update_handler::{UpdateHandler, Optimizer};
use segment::types::SegmentConfig;
use std::fs::create_dir_all;
use parking_lot::{RwLock, Mutex};
use crate::collection_builder::optimizers_builder::build_optimizers;
use crate::collection_builder::optimizers_builder::OptimizersConfig;
use atomicwrites::OverwriteBehavior::AllowOverwrite;
use atomicwrites::AtomicFile;
use std::io::Write;

const DEFAULT_SEGMENT_NUMBER: usize = 5;

pub const COLLECTION_CONFIG_FILE: &str = "config.json";


fn save_config(path: &Path, config: &SegmentConfig) -> CollectionResult<()> {
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
    config: &SegmentConfig,
    wal: SerdeWal<CollectionUpdateOperations>,
    search_runtime: Arc<Runtime>,  // from service
    optimize_runtime: Arc<Runtime>,  // from service
    optimizers: Arc<Vec<Box<Optimizer>>>,
    flush_interval_sec: u64,
) -> Collection {
    let segment_holder = Arc::new(RwLock::new(segment_holder));


    let locked_wal = Arc::new(Mutex::new(wal));

    let searcher = SimpleSegmentSearcher::new(
        segment_holder.clone(),
        search_runtime,
    );

    let updater = SimpleSegmentUpdater::new(segment_holder.clone());

    let (tx, rx) = unbounded();

    let update_handler = Arc::new(UpdateHandler::new(
        optimizers,
        rx,
        optimize_runtime.clone(),
        segment_holder.clone(),
        locked_wal.clone(),
        flush_interval_sec,
    ));

    let collection = Collection {
        segments: segment_holder.clone(),
        config: config.clone(),
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
    wal_options: &WalOptions,  // from config
    segment_config: &SegmentConfig,  //  from user
    search_runtime: Arc<Runtime>,  // from service
    optimize_runtime: Arc<Runtime>,  // from service
    optimizers_config: &OptimizersConfig,
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

    for _sid in 0..DEFAULT_SEGMENT_NUMBER {
        let segment = build_simple_segment(
            segments_path.as_path(),
            segment_config.vector_size,
            segment_config.distance.clone())?;
        segment_holder.add(segment);
    }

    let wal: SerdeWal<CollectionUpdateOperations> = SerdeWal::new(wal_path.to_str().unwrap(), wal_options)?;

    save_config(collection_path, &segment_config)?;

    let optimizers = build_optimizers(
        collection_path,
        &segment_config,
        &optimizers_config,
    );

    let collection = construct_collection(
        segment_holder,
        segment_config,
        wal,
        search_runtime,
        optimize_runtime,
        optimizers,
        optimizers_config.flush_interval_sec,
    );

    Ok(collection)
}

