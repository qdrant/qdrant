use std::fs::create_dir_all;
use std::path::Path;
use std::sync::Arc;

use crossbeam_channel::unbounded;
use parking_lot::{Mutex, RwLock};
use tokio::runtime;

use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::HnswConfig;

use crate::collection::Collection;
use crate::collection_builder::optimizers_builder::build_optimizers;
use crate::collection_builder::optimizers_builder::OptimizersConfig;
use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::config::{CollectionConfig, CollectionParams, WalConfig};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::CollectionUpdateOperations;
use crate::update_handler::{Optimizer, UpdateHandler};
use crate::wal::SerdeWal;

pub fn construct_collection(
    segment_holder: SegmentHolder,
    config: CollectionConfig,
    wal: SerdeWal<CollectionUpdateOperations>,
    optimizers: Arc<Vec<Box<Optimizer>>>,
    collection_path: &Path,
) -> Collection {
    let segment_holder = Arc::new(RwLock::new(segment_holder));

    let optimize_runtime = runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .unwrap();

    let locked_wal = Arc::new(Mutex::new(wal));

    // ToDo: Move tx-rx into updater, so Collection should not know about it.
    let (tx, rx) = unbounded();

    let update_handler = UpdateHandler::new(
        optimizers,
        rx,
        optimize_runtime.handle().clone(),
        segment_holder.clone(),
        locked_wal.clone(),
        config.optimizer_config.flush_interval_sec,
    );

    Collection::new(
        segment_holder,
        config,
        locked_wal,
        update_handler,
        optimize_runtime,
        tx,
        collection_path.to_owned(),
    )
}

/// Creates new empty collection with given configuration
pub fn build_collection(
    collection_path: &Path,
    wal_config: &WalConfig,               // from config
    collection_params: &CollectionParams, //  from user
    optimizers_config: &OptimizersConfig,
    hnsw_config: &HnswConfig,
) -> CollectionResult<Collection> {
    let wal_path = collection_path.join("wal");

    create_dir_all(&wal_path).map_err(|err| CollectionError::ServiceError {
        error: format!("Can't create collection directory. Error: {}", err),
    })?;

    let segments_path = collection_path.join("segments");

    create_dir_all(&segments_path).map_err(|err| CollectionError::ServiceError {
        error: format!("Can't create collection directory. Error: {}", err),
    })?;

    let mut segment_holder = SegmentHolder::default();

    for _sid in 0..optimizers_config.max_segment_number {
        let segment = build_simple_segment(
            segments_path.as_path(),
            collection_params.vector_size,
            collection_params.distance,
        )?;
        segment_holder.add(segment);
    }

    let wal: SerdeWal<CollectionUpdateOperations> =
        SerdeWal::new(wal_path.to_str().unwrap(), &wal_config.into())?;

    let collection_config = CollectionConfig {
        params: collection_params.clone(),
        hnsw_config: *hnsw_config,
        optimizer_config: optimizers_config.clone(),
        wal_config: wal_config.clone(),
    };

    collection_config.save(collection_path)?;

    let optimizers = build_optimizers(
        collection_path,
        collection_params,
        optimizers_config,
        &collection_config.hnsw_config,
    );

    let collection = construct_collection(
        segment_holder,
        collection_config,
        wal,
        optimizers,
        collection_path,
    );

    Ok(collection)
}
