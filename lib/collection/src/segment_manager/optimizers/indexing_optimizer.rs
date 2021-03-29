use std::path::PathBuf;
use segment::types::{SegmentConfig, SegmentType, StorageType, Indexes, PayloadIndexType};
use crate::segment_manager::holders::segment_holder::{LockedSegmentHolder, SegmentId, LockedSegment};
use std::cmp::min;
use crate::segment_manager::optimizers::segment_optimizer::SegmentOptimizer;
use crate::collection::CollectionResult;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::segment_constructor::segment_constructor::build_segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;

pub struct IndexingOptimizer {
    memmap_threshold: usize,
    indexing_threshold: usize,
    segments_path: PathBuf,
    config: SegmentConfig,
}

impl IndexingOptimizer {
    pub fn new(
        memmap_threshold: usize,
        indexing_threshold: usize,
        segments_path: PathBuf,
        config: SegmentConfig,
    ) -> Self {
        IndexingOptimizer {
            memmap_threshold,
            indexing_threshold,
            segments_path,
            config,
        }
    }

    fn worst_segment(&self, segments: LockedSegmentHolder) -> Option<(SegmentId, LockedSegment)> {
        segments.read().iter()
            .filter_map(|(idx, segment)| {
                let segment_entry = segment.get();
                let read_segment = segment_entry.read();
                // Apply indexing to plain segments which have grown too big
                let is_plain = read_segment.segment_type() == SegmentType::Plain;
                let is_big = read_segment.vectors_count() >= min(self.memmap_threshold, self.indexing_threshold);
                match is_plain && is_big {
                    true => Some((*idx, read_segment.vectors_count())),
                    false => None
                }
            })
            .max_by_key(|(_, num_vectors)| *num_vectors)
            .and_then(|(idx, _)| Some((idx, segments.read().get(idx).unwrap().clone())))
    }
}

impl SegmentOptimizer for IndexingOptimizer {
    fn check_condition(&self, segments: LockedSegmentHolder) -> Vec<SegmentId> {
        match self.worst_segment(segments) {
            None => vec![],
            Some((segment_id, _segment)) => vec![segment_id],
        }
    }

    fn temp_segment(&self) -> CollectionResult<LockedSegment> {
        Ok(LockedSegment::new(build_simple_segment(
            self.segments_path.as_path(),
            self.config.vector_size,
            self.config.distance,
        )?))
    }

    fn optimized_segment_builder(&self, optimizing_segments: &Vec<LockedSegment>) -> CollectionResult<SegmentBuilder> {
        let optimizing_segment = optimizing_segments.get(0).unwrap();
        let mut optimized_config = optimizing_segment.get().read().config();
        let total_vectors: usize = optimizing_segment.get().read().vectors_count();

        if total_vectors < self.memmap_threshold {
            optimized_config.storage_type = StorageType::InMemory;
        } else {
            optimized_config.storage_type = StorageType::Mmap;
        }

        if total_vectors < self.indexing_threshold {
            optimized_config.index = Indexes::Plain {};
            optimized_config.payload_index = Some(PayloadIndexType::Plain)
        } else {
            optimized_config.payload_index = Some(PayloadIndexType::Struct);
            optimized_config.index = match optimized_config.index {
                Indexes::Plain {} => Indexes::default_hnsw(),
                _ => optimized_config.index
            }
        }

        Ok(SegmentBuilder::new(build_segment(self.segments_path.as_path(), &optimized_config)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    use crate::segment_manager::holders::segment_holder::SegmentHolder;
    use crate::segment_manager::fixtures::random_segment;
    use std::sync::Arc;
    use parking_lot::lock_api::RwLock;
    use itertools::Itertools;
    use crate::segment_manager::simple_segment_updater::SimpleSegmentUpdater;
    use crate::operations::FieldIndexOperations;


    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn test_indexing_optimizer() {
        init();

        let mut holder = SegmentHolder::new();

        let payload_field = "number".to_owned();

        let dim = 4;

        let segments_dir = TempDir::new("segments_dir").unwrap();

        let small_segment = random_segment(segments_dir.path(), 101, 25, dim);
        let middle_segment = random_segment(segments_dir.path(), 102, 100, dim);
        let large_segment = random_segment(segments_dir.path(), 103, 200, dim);

        let segment_config = small_segment.segment_config.clone();

        let small_segment_id = holder.add(small_segment);
        let middle_segment_id = holder.add(middle_segment);
        let large_segment_id = holder.add(large_segment);

        let index_optimizer = IndexingOptimizer::new(
            150,
            50,
            segments_dir.path().to_owned(),
            SegmentConfig {
                vector_size: segment_config.vector_size,
                index: Default::default(),
                payload_index: Some(Default::default()),
                distance: segment_config.distance,
                storage_type: StorageType::default(),
            },
        );

        // debug!("Created segments: {:#?}", vec![small_segment_id, middle_segment_id, large_segment_id]);

        let locked_holder = Arc::new(RwLock::new(holder));

        let updater = SimpleSegmentUpdater::new(locked_holder.clone());

        updater.process_field_index_operation(104, &FieldIndexOperations::CreateIndex(payload_field.clone())).unwrap();

        let suggested_to_optimize = index_optimizer.check_condition(locked_holder.clone());
        assert!(suggested_to_optimize.contains(&large_segment_id));
        index_optimizer.optimize(locked_holder.clone(), suggested_to_optimize).unwrap();

        let suggested_to_optimize = index_optimizer.check_condition(locked_holder.clone());
        assert!(suggested_to_optimize.contains(&middle_segment_id));
        index_optimizer.optimize(locked_holder.clone(), suggested_to_optimize).unwrap();

        let suggested_to_optimize = index_optimizer.check_condition(locked_holder.clone());
        assert!(suggested_to_optimize.is_empty());

        assert_eq!(locked_holder.read().len(), 3);

        let infos = locked_holder.read().iter().map(|(_sid, segment)| segment.get().read().info()).collect_vec();
        let configs = locked_holder.read().iter().map(|(_sid, segment)| segment.get().read().config()).collect_vec();

        let indexed_count = infos.iter().filter(|info| info.segment_type == SegmentType::Indexed).count();
        assert_eq!(indexed_count, 2);

        let mmap_count = configs.iter().filter(|config| config.storage_type == StorageType::Mmap).count();
        assert_eq!(mmap_count, 1);

        let segment_dirs = segments_dir.path().read_dir().unwrap().collect_vec();
        assert_eq!(segment_dirs.len(), locked_holder.read().len());

        eprintln!("infos = {:#?}", infos);

        for info in infos.iter() {
            assert!(info.schema.contains_key(&payload_field));
            assert!(info.schema[&payload_field].indexed);
        }
    }
}