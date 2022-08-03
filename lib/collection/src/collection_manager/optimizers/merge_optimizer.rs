use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use segment::telemetry::TelemetryOperationAggregator;
use segment::types::{HnswConfig, SegmentType, VECTOR_ELEMENT_SIZE};

use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentId,
};
use crate::collection_manager::optimizers::segment_optimizer::{
    OptimizerThresholds, SegmentOptimizer,
};
use crate::config::CollectionParams;
use crate::telemetry::OptimizerTelemetry;

const BYTES_IN_KB: usize = 1024;

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
    optimizations_telemetry_counter: Arc<Mutex<TelemetryOperationAggregator>>,
}

impl MergeOptimizer {
    #[allow(clippy::too_many_arguments)]
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
            optimizations_telemetry_counter: TelemetryOperationAggregator::new(),
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
        let max_candidates = raw_segments.len() - self.max_segments + 2;

        // Find at least top-3 smallest segments to join.
        // We need 3 segments because in this case we can guarantee that total segments number will be less

        let candidates: Vec<_> = raw_segments
            .iter()
            .cloned()
            .filter_map(|(idx, segment)| {
                let segment_entry = segment.get();
                let read_segment = segment_entry.read();
                match read_segment.segment_type() != SegmentType::Special {
                    true => Some((
                        *idx,
                        read_segment.points_count()
                            * read_segment.vector_dim()
                            * VECTOR_ELEMENT_SIZE,
                    )),
                    false => None,
                }
            })
            .sorted_by_key(|(_, size)| *size)
            .scan(0, |size_sum, (sid, size)| {
                *size_sum += size; // produce a cumulative sum of segment sizes starting from smallest
                Some((sid, *size_sum))
            })
            .take_while(|(_, size)| *size < self.thresholds_config.max_segment_size * BYTES_IN_KB)
            .take(max_candidates)
            .map(|x| x.0)
            .collect();

        if candidates.len() < 3 {
            return vec![];
        }
        log::debug!("Merge candidates: {:?}", candidates);
        candidates
    }

    fn get_telemetry_data(&self) -> OptimizerTelemetry {
        OptimizerTelemetry::Merge {
            optimizations: self.get_telemetry_counter().lock().get_statistics(),
        }
    }

    fn get_telemetry_counter(&self) -> Arc<Mutex<TelemetryOperationAggregator>> {
        self.optimizations_telemetry_counter.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use parking_lot::RwLock;
    use tempdir::TempDir;

    use super::*;
    use crate::collection_manager::fixtures::{get_merge_optimizer, random_segment};
    use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder};

    #[test]
    fn test_max_merge_size() {
        let dir = TempDir::new("segment_dir").unwrap();
        let temp_dir = TempDir::new("segment_temp_dir").unwrap();

        let mut holder = SegmentHolder::default();
        let dim = 256;

        let _segments_to_merge = vec![
            holder.add(random_segment(dir.path(), 100, 40, dim)),
            holder.add(random_segment(dir.path(), 100, 50, dim)),
            holder.add(random_segment(dir.path(), 100, 60, dim)),
        ];

        let mut merge_optimizer = get_merge_optimizer(dir.path(), temp_dir.path(), dim);

        let locked_holder = Arc::new(RwLock::new(holder));

        merge_optimizer.max_segments = 1;

        merge_optimizer.thresholds_config.max_segment_size = 100;

        let check_result_empty =
            merge_optimizer.check_condition(locked_holder.clone(), &Default::default());

        assert!(check_result_empty.is_empty());

        merge_optimizer.thresholds_config.max_segment_size = 200;

        let check_result = merge_optimizer.check_condition(locked_holder, &Default::default());

        assert_eq!(check_result.len(), 3);
    }

    #[test]
    fn test_merge_optimizer() {
        let dir = TempDir::new("segment_dir").unwrap();
        let temp_dir = TempDir::new("segment_temp_dir").unwrap();

        let mut holder = SegmentHolder::default();
        let dim = 256;

        let segments_to_merge = vec![
            holder.add(random_segment(dir.path(), 100, 3, dim)),
            holder.add(random_segment(dir.path(), 100, 3, dim)),
            holder.add(random_segment(dir.path(), 100, 3, dim)),
            holder.add(random_segment(dir.path(), 100, 10, dim)),
        ];

        let other_segment_ids: Vec<SegmentId> = vec![
            holder.add(random_segment(dir.path(), 100, 20, dim)),
            holder.add(random_segment(dir.path(), 100, 20, dim)),
            holder.add(random_segment(dir.path(), 100, 20, dim)),
        ];

        let merge_optimizer = get_merge_optimizer(dir.path(), temp_dir.path(), dim);

        let locked_holder: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));

        let suggested_for_merge =
            merge_optimizer.check_condition(locked_holder.clone(), &Default::default());

        assert_eq!(suggested_for_merge.len(), 4);

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
            .optimize(
                locked_holder.clone(),
                suggested_for_merge,
                &AtomicBool::new(false),
            )
            .unwrap();

        let after_optimization_segments =
            locked_holder.read().iter().map(|(x, _)| *x).collect_vec();

        // Check proper number of segments after optimization
        assert!(after_optimization_segments.len() <= 5);
        assert!(after_optimization_segments.len() > 3);

        // Check other segments are untouched
        for segment_id in &other_segment_ids {
            assert!(after_optimization_segments.contains(segment_id))
        }

        // Check new optimized segment have all vectors in it
        for segment_id in after_optimization_segments {
            if !other_segment_ids.contains(&segment_id) {
                let holder_guard = locked_holder.read();
                let new_segment = holder_guard.get(segment_id).unwrap();
                assert_eq!(new_segment.get().read().points_count(), 3 * 3 + 10);
            }
        }

        // Check if optimized segments removed from disk
        old_path.into_iter().for_each(|x| assert!(!x.exists()));
    }
}
