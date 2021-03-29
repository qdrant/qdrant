use crate::segment_manager::optimizers::segment_optimizer::SegmentOptimizer;
use crate::segment_manager::holders::segment_holder::{LockedSegmentHolder, SegmentId, LockedSegment};
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{SegmentType, SegmentConfig, StorageType, Indexes, PayloadIndexType};

use itertools::Itertools;
use segment::segment_constructor::segment_constructor::build_segment;
use std::path::PathBuf;
use crate::collection::CollectionResult;
use segment::segment_constructor::segment_builder::SegmentBuilder;


pub struct MergeOptimizer {
    max_segments: usize,
    memmap_threshold: usize,
    indexing_threshold: usize,
    segments_path: PathBuf,
    config: SegmentConfig,
}

impl MergeOptimizer {
    pub fn new(
        max_segments: usize,
        memmap_threshold: usize,
        indexing_threshold: usize,
        segments_path: PathBuf,
        config: SegmentConfig) -> Self {
        return MergeOptimizer { max_segments, memmap_threshold, indexing_threshold, segments_path, config };
    }
}


impl SegmentOptimizer for MergeOptimizer {
    fn check_condition(&self, segments: LockedSegmentHolder) -> Vec<SegmentId> {
        let read_segments = segments.read();

        if read_segments.len() <= self.max_segments {
            return vec![];
        }

        // Find top-3 smallest segments to join.
        // We need 3 segments because in this case we can guarantee that total segments number will be less

        read_segments.iter()
            .filter_map(|(idx, segment)| {
                let segment_entry = segment.get();
                let read_segment = segment_entry.read();
                match read_segment.segment_type() != SegmentType::Special {
                    true => Some((*idx, read_segment.vectors_count())),
                    false => None
                }
            })
            .sorted_by_key(|(_, size)| *size)
            .take(3)
            .map(|x| x.0)
            .collect()
    }

    fn temp_segment(&self) -> CollectionResult<LockedSegment> {
        Ok(LockedSegment::new(build_simple_segment(
            self.segments_path.as_path(),
            self.config.vector_size,
            self.config.distance,
        )?))
    }

    fn optimized_segment_builder(&self, optimizing_segments: &Vec<LockedSegment>) -> CollectionResult<SegmentBuilder> {
        let total_vectors: usize = optimizing_segments.iter()
            .map(|s| s.get().read().vectors_count()).sum();

        let mut optimized_config = self.config.clone();

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
                Indexes::Plain { } => Indexes::default_hnsw(),
                _ => optimized_config.index
            }
        }

        Ok(SegmentBuilder::new(build_segment(self.segments_path.as_path(), &optimized_config)?))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_manager::fixtures::{random_segment};
    use crate::segment_manager::holders::segment_holder::SegmentHolder;
    use segment::types::{Distance, Indexes};
    use std::sync::{Arc};
    use tempdir::TempDir;
    use parking_lot::RwLock;

    #[test]
    fn test_merge_optimizer() {
        let dir = TempDir::new("segment_dir").unwrap();

        let mut holder = SegmentHolder::new();


        let mut segments_to_merge = vec![];

        segments_to_merge.push(holder.add(random_segment(dir.path(), 100, 3, 4)));
        segments_to_merge.push(holder.add(random_segment(dir.path(), 100, 3, 4)));
        segments_to_merge.push(holder.add(random_segment(dir.path(), 100, 3, 4)));


        let mut other_segment_ids: Vec<SegmentId> = vec![];

        other_segment_ids.push(holder.add(random_segment(dir.path(), 100, 20, 4)));
        other_segment_ids.push(holder.add(random_segment(dir.path(), 100, 20, 4)));
        other_segment_ids.push(holder.add(random_segment(dir.path(), 100, 20, 4)));
        other_segment_ids.push(holder.add(random_segment(dir.path(), 100, 20, 4)));


        let merge_optimizer = MergeOptimizer::new(
            5,
            10000,
            100000,
            dir.path().to_owned(), SegmentConfig {
                vector_size: 4,
                index: Indexes::Plain {},
                payload_index: Some(Default::default()),
                distance: Distance::Dot,
                storage_type: Default::default(),
            });

        let locked_holder = Arc::new(RwLock::new(holder));

        let suggested_for_merge = merge_optimizer.check_condition(locked_holder.clone());

        assert_eq!(suggested_for_merge.len(), 3);

        for segment_in in suggested_for_merge.iter() {
            assert!(segments_to_merge.contains(&segment_in));
        }

        let old_path =segments_to_merge.iter()
            .map(|sid| {
                match locked_holder.read().get(*sid).unwrap() {
                    LockedSegment::Original(x) => x.read().current_path.clone(),
                    LockedSegment::Proxy(_) => panic!("Not expected"),
                }
            }).collect_vec();

        merge_optimizer.optimize(locked_holder.clone(), suggested_for_merge).unwrap();

        let after_optimization_segments = locked_holder
            .read()
            .iter()
            .map(|(x, _)| *x)
            .collect_vec();


        // Check proper number of segments after optimization
        assert_eq!(after_optimization_segments.len(), 5);

        // Check other segments are untouched
        for segment_id in other_segment_ids.iter() {
            assert!(after_optimization_segments.contains(&segment_id))
        }

        // Check new optimized segment have all vectors in it
        for segment_id in after_optimization_segments {
            if !other_segment_ids.contains(&segment_id) {
                let holder_guard = locked_holder.read();
                let new_segment = holder_guard.get(segment_id).unwrap();
                assert_eq!(new_segment.get().read().vectors_count(), 3 * 3);
            }
        }

        // Check if optimized segments removed from disk
        old_path.into_iter().for_each(|x| assert!(!x.exists()));
    }
}