use crate::collection::Collection;
use std::path::Path;
use tokio::runtime::Handle;
use crate::segment_manager::holders::segment_holder::SegmentHolder;
use crate::wal::SerdeWal;
use crate::operations::CollectionUpdateOperations;
use wal::WalOptions;
use std::fs::read_dir;
use segment::segment_constructor::segment_constructor::load_segment;
use crate::collection_builder::collection_builder::construct_collection;
use indicatif::ProgressBar;
use crate::collection_builder::optimizers_builder::OptimizersConfig;
use crate::collection_builder::optimizers_builder::build_optimizers;


pub fn load_collection(
    collection_path: &Path,
    wal_options: &WalOptions,  // from config
    search_runtime: Handle,  // from service
    optimize_runtime: Handle,  // from service
    optimizers_config: &OptimizersConfig
) -> Collection {
    let wal_path = collection_path.join("wal");
    let segments_path = collection_path.join("segments");
    let mut segment_holder = SegmentHolder::new();

    let wal: SerdeWal<CollectionUpdateOperations> = SerdeWal::new(wal_path.to_str().unwrap(), wal_options).expect("Can't read WAL");

    let segment_dirs = read_dir(segments_path.as_path())
        .expect(&format!("Can't read segments directory {}", segments_path.to_str().unwrap()));

    for entry in segment_dirs {
        let segments_path = entry.unwrap().path();
        let segment = match load_segment(segments_path.as_path()) {
            Ok(x) => x,
            Err(err) => panic!(
                format!("Can't load segments from {}, error: {}", segments_path.to_str().unwrap(), err)
            ),
        };
        segment_holder.add(segment);
    };

    let segment_config = segment_holder.random_segment()
        .expect("Expected at least one segment in collection")
        .get()
        .read()
        .config();

    let optimizers = build_optimizers(
        collection_path,
        &segment_config,
        &optimizers_config
    );

    let collection = construct_collection(
        segment_holder,
        wal,
        search_runtime,
        optimize_runtime,
        optimizers,
        optimizers_config.flush_interval_sec
    );

    {
        let wal = collection.wal.lock();
        let bar = ProgressBar::new(wal.len());
        bar.set_message("Recovering collection");

        for (op_num, update) in wal.read_all() {
            collection.updater.update(op_num, &update).unwrap();
            bar.inc(1);
        }

        bar.finish();
    }

    collection
}

