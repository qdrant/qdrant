use crate::segment_manager::optimizers::segment_optimizer::{SegmentOptimizer, OptimizerThresholds};
use crate::segment_manager::holders::segment_holder::{LockedSegmentHolder, SegmentId, LockedSegment};
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{SegmentType, SegmentConfig, StorageType, Indexes, PayloadIndexType};

use itertools::Itertools;
use segment::segment_constructor::segment_constructor::build_segment;
use std::path::{PathBuf, Path};
use crate::collection::CollectionResult;
use segment::segment_constructor::segment_builder::SegmentBuilder;

/// Optimizer that tries to reduce number of segments until it fits configured value
pub struct MergeOptimizer {
    max_segments: usize,
    thresholds_config: OptimizerThresholds,
    segments_path: PathBuf,
    collection_temp_dir: PathBuf,
    config: SegmentConfig,
}

impl MergeOptimizer {
    pub fn new(
        max_segments: usize,
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        collection_temp_dir: PathBuf,
        config: SegmentConfig) -> Self {
        return MergeOptimizer {
            max_segments,
            thresholds_config,
            segments_path,
            collection_temp_dir,
            config,
        };
    }
}


impl SegmentOptimizer for MergeOptimizer {
    fn collection_path(&self) -> &Path {
        self.segments_path.as_path()
    }

    fn temp_path(&self) -> &Path {
        self.collection_temp_dir.as_path()
    }

    fn base_segment_config(&self) -> SegmentConfig {
        self.config.clone()
    }

    fn threshold_config(&self) -> &OptimizerThresholds {
        &self.thresholds_config
    }


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
        let temp_dir = TempDir::new("segment_temp_dir").unwrap();

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
            OptimizerThresholds{
                memmap_threshold: 1000000,
                indexing_threshold: 1000000,
                payload_indexing_threshold: 1000000
            },
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            SegmentConfig {
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

        let old_path = segments_to_merge.iter()
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