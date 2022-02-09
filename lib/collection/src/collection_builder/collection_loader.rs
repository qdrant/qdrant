use futures::executor::block_on;
use segment::payload_storage::schema_storage::SchemaStorage;
use std::fs::{read_dir, remove_dir_all};
use std::path::Path;
use std::sync::Arc;

use segment::segment_constructor::load_segment;

use crate::collection::Collection;
use crate::collection_builder::construct_collection;
use crate::collection_builder::optimizers_builder::build_optimizers;
use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::config::CollectionConfig;
use crate::operations::CollectionUpdateOperations;
use crate::wal::SerdeWal;

pub fn load_collection(collection_path: &Path) -> Collection {
    let wal_path = collection_path.join("wal");
    let segments_path = collection_path.join("segments");
    let mut segment_holder = SegmentHolder::default();

    let collection_config = CollectionConfig::load(collection_path).unwrap_or_else(|err| {
        panic!(
            "Can't read collection config due to {}\nat {}",
            err,
            collection_path.to_str().unwrap()
        )
    });

    let wal: SerdeWal<CollectionUpdateOperations> = SerdeWal::new(
        wal_path.to_str().unwrap(),
        &(&collection_config.wal_config).into(),
    )
    .expect("Can't read WAL");

    let schema_storage = Arc::new(SchemaStorage::new());

    let segment_dirs = read_dir(&segments_path).unwrap_or_else(|err| {
        panic!(
            "Can't read segments directory due to {}\nat {}",
            err,
            segments_path.to_str().unwrap()
        )
    });

    for entry in segment_dirs {
        let segments_path = entry.unwrap().path();
        if segments_path.ends_with("deleted") {
            remove_dir_all(&segments_path).unwrap_or_else(|_| {
                panic!(
                    "Can't remove marked-for-remove segment {}",
                    segments_path.to_str().unwrap()
                )
            });
            continue;
        }
        let segment = match load_segment(&segments_path, schema_storage.clone()) {
            Ok(x) => x,
            Err(err) => panic!(
                "Can't load segments from {}, error: {}",
                segments_path.to_str().unwrap(),
                err
            ),
        };
        segment_holder.add(segment);
    }

    let optimizers = build_optimizers(
        collection_path,
        &collection_config.params,
        &collection_config.optimizer_config,
        &collection_config.hnsw_config,
        schema_storage.clone(),
    );

    let collection = construct_collection(
        segment_holder,
        collection_config,
        wal,
        optimizers,
        collection_path,
        schema_storage,
    );

    block_on(collection.load_from_wal());

    collection
}
