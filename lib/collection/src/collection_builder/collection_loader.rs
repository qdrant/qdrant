use std::fs::{read_dir, remove_dir_all};
use std::path::Path;

use indicatif::ProgressBar;
use tokio::runtime::Handle;

use segment::segment_constructor::segment_constructor::load_segment;

use crate::collection::Collection;
use crate::collection_builder::collection_builder::construct_collection;
use crate::collection_builder::optimizers_builder::build_optimizers;
use crate::config::CollectionConfig;
use crate::operations::types::CollectionError;
use crate::operations::CollectionUpdateOperations;
use crate::segment_manager::holders::segment_holder::SegmentHolder;
use crate::wal::SerdeWal;

pub fn load_collection(
    collection_path: &Path,
    search_runtime: Handle, // from service
) -> Collection {
    let wal_path = collection_path.join("wal");
    let segments_path = collection_path.join("segments");
    let mut segment_holder = SegmentHolder::new();

    let collection_config = CollectionConfig::load(&collection_path).expect(&format!(
        "Can't read collection config at {}",
        collection_path.to_str().unwrap()
    ));

    let wal: SerdeWal<CollectionUpdateOperations> = SerdeWal::new(
        wal_path.to_str().unwrap(),
        &(&collection_config.wal_config).into(),
    )
    .expect("Can't read WAL");

    let segment_dirs = read_dir(segments_path.as_path()).expect(&format!(
        "Can't read segments directory {}",
        segments_path.to_str().unwrap()
    ));

    for entry in segment_dirs {
        let segments_path = entry.unwrap().path();
        if segments_path.ends_with("deleted") {
            remove_dir_all(segments_path.as_path()).expect(&format!(
                "Can't remove marked-for-remove segment {}",
                segments_path.to_str().unwrap()
            ));
            continue;
        }
        let segment = match load_segment(segments_path.as_path()) {
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
    );

    let collection = construct_collection(
        segment_holder,
        collection_config,
        wal,
        search_runtime,
        optimizers,
        collection_path,
    );

    {
        let wal = collection.wal.lock();
        let bar = ProgressBar::new(wal.len());
        bar.set_message("Recovering collection");

        for (op_num, update) in wal.read_all() {
            // Panic only in case of internal error. If wrong formatting - skip
            match collection.updater.update(op_num, update) {
                Ok(_) => {}
                Err(err) => match err {
                    CollectionError::ServiceError { error } => {
                        panic!("Can't apply WAL operation: {}", error)
                    }
                    _ => {}
                },
            }
            bar.inc(1);
        }

        collection.flush_all().unwrap();
        bar.finish();
    }

    collection
}
