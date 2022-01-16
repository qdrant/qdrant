use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentId,
};
use crate::collection_manager::optimizers::segment_optimizer::{
    OptimizerThresholds, SegmentOptimizer,
};
use crate::config::CollectionParams;
use itertools::Itertools;
use segment::types::{HnswConfig, SegmentType};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Optimizer that tries to reduce number of segments until it fits configured value.
/// It merges 3 smallest segments into a single large segment.
/// Merging 3 segments instead of 2 guarantees that after the optimization the number of segments
/// will be less than before.
pub struct MergeOptimizer {
    max_segments: usize,
    thresholds_config: OptimizerThresholds,
    segments_path: PathBuf,
    collection_temp_dir: PathBuf,
    collection_params: CollectionParams,
    hnsw_config: HnswConfig,
}

impl MergeOptimizer {
    pub fn new(
        max_segments: usize,
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        collection_temp_dir: PathBuf,
        collection_params: CollectionParams,
        hnsw_config: HnswConfig,
    ) -> Self {
        MergeOptimizer {
            max_segments,
            thresholds_config,
            segments_path,
            collection_temp_dir,
            collection_params,
            hnsw_config,
        }
    }
}

impl SegmentOptimizer for MergeOptimizer {
    fn collection_path(&self) -> &Path {
        self.segments_path.as_path()
    }

    fn temp_path(&self) -> &Path {
        self.collection_temp_dir.as_path()
    }

    fn collection_params(&self) -> CollectionParams {
        self.collection_params.clone()
    }

    fn hnsw_config(&self) -> HnswConfig {
        self.hnsw_config
    }

    fn threshold_config(&self) -> &OptimizerThresholds {
        &self.thresholds_config
    }

    fn check_condition(
        &self,
        segments: LockedSegmentHolder,
        excluded_ids: &HashSet<SegmentId>,
    ) -> Vec<SegmentId> {
        let read_segments = segments.read();

        let raw_segments = read_segments
            .iter()
            .filter(|(sid, segment)| {
                matches!(segment, LockedSegment::Original(_)) && !excluded_ids.contains(sid)
            })
            .collect_vec();

        if raw_segments.len() <= self.max_segments {
            return vec![];
        }

        // Find top-3 smallest segments to join.
        // We need 3 segments because in this case we can guarantee that total segments number will be less

        raw_segments
            .iter()
            .cloned()
            .filter_map(|(idx, segment)| {
                let segment_entry = segment.get();
                let read_segment = segment_entry.read();
                match read_segment.segment_type() != SegmentType::Special {
                    true => Some((*idx, read_segment.vectors_count())),
                    false => None,
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
    use crate::collection_manager::fixtures::{get_merge_optimizer, random_segment};
    use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder};
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempdir::TempDir;

    #[test]
    fn test_merge_optimizer() {
        let dir = TempDir::new("segment_dir").unwrap();
        let temp_dir = TempDir::new("segment_temp_dir").unwrap();

        let mut holder = SegmentHolder::default();

        let segments_to_merge = vec![
            holder.add(random_segment(dir.path(), 100, 3, 4)),
            holder.add(random_segment(dir.path(), 100, 3, 4)),
            holder.add(random_segment(dir.path(), 100, 3, 4)),
        ];

        let other_segment_ids: Vec<SegmentId> = vec![
            holder.add(random_segment(dir.path(), 100, 20, 4)),
            holder.add(random_segment(dir.path(), 100, 20, 4)),
            holder.add(random_segment(dir.path(), 100, 20, 4)),
            holder.add(random_segment(dir.path(), 100, 20, 4)),
        ];

        let merge_optimizer = get_merge_optimizer(dir.path(), temp_dir.path());

        let locked_holder = Arc::new(RwLock::new(holder));

        let suggested_for_merge =
            merge_optimizer.check_condition(locked_holder.clone(), &Default::default());

        assert_eq!(suggested_for_merge.len(), 3);

        for segment_in in &suggested_for_merge {
            assert!(segments_to_merge.contains(segment_in));
        }

        let old_path = segments_to_merge
            .iter()
            .map(|sid| match locked_holder.read().get(*sid).unwrap() {
                LockedSegment::Original(x) => x.read().current_path.clone(),
                LockedSegment::Proxy(_) => panic!("Not expected"),
            })
            .collect_vec();

        merge_optimizer
            .optimize(locked_holder.clone(), suggested_for_merge)
            .unwrap();

        let after_optimization_segments =
            locked_holder.read().iter().map(|(x, _)| *x).collect_vec();

        // Check proper number of segments after optimization
        assert_eq!(after_optimization_segments.len(), 5);

        // Check other segments are untouched
        for segment_id in &other_segment_ids {
            assert!(after_optimization_segments.contains(segment_id))
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
