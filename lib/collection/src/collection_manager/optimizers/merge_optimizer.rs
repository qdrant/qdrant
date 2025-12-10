use std::path::{Path, PathBuf};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use segment::common::operation_time_statistics::OperationDurationsAggregator;
use segment::entry::SegmentEntry;
use segment::types::{HnswConfig, HnswGlobalConfig, QuantizationConfig};

use crate::collection_manager::optimizers::segment_optimizer::{
    OptimizationPlanner, OptimizerThresholds, SegmentOptimizer,
};
use crate::config::CollectionParams;

const BYTES_IN_KB: usize = 1024;

/// Optimizer that tries to reduce number of segments until it fits configured value.
///
/// It merges 3 smallest segments into a single large segment.
/// Merging 3 segments instead of 2 guarantees that after the optimization the number of segments
/// will be less than before.
pub struct MergeOptimizer {
    default_segments_number: usize,
    thresholds_config: OptimizerThresholds,
    segments_path: PathBuf,
    collection_temp_dir: PathBuf,
    collection_params: CollectionParams,
    hnsw_config: HnswConfig,
    hnsw_global_config: HnswGlobalConfig,
    quantization_config: Option<QuantizationConfig>,
    telemetry_durations_aggregator: Arc<Mutex<OperationDurationsAggregator>>,
}

impl MergeOptimizer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        default_segments_number: usize,
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        collection_temp_dir: PathBuf,
        collection_params: CollectionParams,
        hnsw_config: HnswConfig,
        hnsw_global_config: HnswGlobalConfig,
        quantization_config: Option<QuantizationConfig>,
    ) -> Self {
        MergeOptimizer {
            default_segments_number,
            thresholds_config,
            segments_path,
            collection_temp_dir,
            collection_params,
            hnsw_config,
            hnsw_global_config,
            quantization_config,
            telemetry_durations_aggregator: OperationDurationsAggregator::new(),
        }
    }
}

impl SegmentOptimizer for MergeOptimizer {
    fn name(&self) -> &str {
        "merge"
    }

    fn segments_path(&self) -> &Path {
        self.segments_path.as_path()
    }

    fn temp_path(&self) -> &Path {
        self.collection_temp_dir.as_path()
    }

    fn collection_params(&self) -> CollectionParams {
        self.collection_params.clone()
    }

    fn hnsw_config(&self) -> &HnswConfig {
        &self.hnsw_config
    }

    fn hnsw_global_config(&self) -> &HnswGlobalConfig {
        &self.hnsw_global_config
    }

    fn quantization_config(&self) -> Option<QuantizationConfig> {
        self.quantization_config.clone()
    }

    fn threshold_config(&self) -> &OptimizerThresholds {
        &self.thresholds_config
    }

    fn plan_optimizations(&self, planner: &mut OptimizationPlanner) {
        let mut candidates = planner
            .remaining()
            .iter()
            .map(|(&segment_id, segment)| {
                let size = segment
                    .read()
                    .max_available_vectors_size_in_bytes()
                    .unwrap_or_default();
                (segment_id, size)
            })
            .collect_vec();

        candidates.sort_by_key(|(_segment_id, size)| *size);
        let threshold = self
            .thresholds_config
            .max_segment_size_kb
            .saturating_mul(BYTES_IN_KB);

        let mut taken_candidates = 0;
        let mut last_candidate =
            (planner.expected_segments_number() + 2).saturating_sub(self.default_segments_number);
        while taken_candidates < last_candidate.min(candidates.len()) {
            let batch = candidates[taken_candidates..last_candidate.min(candidates.len())]
                .iter()
                .scan(0, |size_sum, &(segment_id, size)| {
                    *size_sum += size;
                    (*size_sum < threshold).then_some(segment_id)
                })
                .collect_vec();

            // We need at least 3 segments because in this case we can guarantee
            // that total segments number will be less.
            if batch.len() < 3 {
                return;
            }
            taken_candidates += batch.len();
            last_candidate += 1;
            planner.plan(batch);
        }
    }

    fn get_telemetry_counter(&self) -> &Mutex<OperationDurationsAggregator> {
        &self.telemetry_durations_aggregator
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use common::budget::ResourceBudget;
    use common::progress_tracker::ProgressTracker;
    use parking_lot::RwLock;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use segment::index::hnsw_index::num_rayon_threads;
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::{get_merge_optimizer, random_segment};
    use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder};

    #[test]
    fn test_max_merge_size() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

        let mut holder = SegmentHolder::default();
        let dim = 256;

        let _segments_to_merge = [
            holder.add_new(random_segment(dir.path(), 100, 40, dim)),
            holder.add_new(random_segment(dir.path(), 100, 50, dim)),
            holder.add_new(random_segment(dir.path(), 100, 60, dim)),
        ];

        let mut merge_optimizer = get_merge_optimizer(dir.path(), temp_dir.path(), dim, None);

        let locked_holder = Arc::new(RwLock::new(holder));

        merge_optimizer.default_segments_number = 1;

        merge_optimizer.thresholds_config.max_segment_size_kb = 100;

        let check_result_empty = merge_optimizer.plan_optimizations_for_test(&locked_holder);

        assert!(check_result_empty.is_empty());

        merge_optimizer.thresholds_config.max_segment_size_kb = 200;

        let check_result = merge_optimizer.plan_optimizations_for_test(&locked_holder);
        let check_result = check_result.into_iter().exactly_one().unwrap();

        assert_eq!(check_result.len(), 3);
    }

    #[test]
    fn test_merge_optimizer() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

        let mut holder = SegmentHolder::default();
        let dim = 256;

        let segments_to_merge = [
            holder.add_new(random_segment(dir.path(), 100, 3, dim)),
            holder.add_new(random_segment(dir.path(), 100, 3, dim)),
            holder.add_new(random_segment(dir.path(), 100, 3, dim)),
            holder.add_new(random_segment(dir.path(), 100, 10, dim)),
        ];

        let other_segment_ids = [
            holder.add_new(random_segment(dir.path(), 100, 20, dim)),
            holder.add_new(random_segment(dir.path(), 100, 20, dim)),
            holder.add_new(random_segment(dir.path(), 100, 20, dim)),
        ];

        let merge_optimizer = get_merge_optimizer(dir.path(), temp_dir.path(), dim, None);

        let locked_holder: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));

        let suggested_to_merge = merge_optimizer.plan_optimizations_for_test(&locked_holder);
        let suggested_for_merge = suggested_to_merge.into_iter().exactly_one().unwrap();
        assert_eq!(suggested_for_merge.len(), 4);

        for segment_in in &suggested_for_merge {
            assert!(segments_to_merge.contains(segment_in));
        }

        let old_path = segments_to_merge
            .iter()
            .map(|sid| match locked_holder.read().get(*sid).unwrap() {
                LockedSegment::Original(x) => x.read().segment_path.clone(),
                LockedSegment::Proxy(_) => panic!("Not expected"),
            })
            .collect_vec();

        let permit_cpu_count = num_rayon_threads(0);
        let budget = ResourceBudget::new(permit_cpu_count, permit_cpu_count);
        let permit = budget.try_acquire(0, permit_cpu_count).unwrap();

        merge_optimizer
            .optimize(
                locked_holder.clone(),
                suggested_for_merge,
                permit,
                budget,
                &AtomicBool::new(false),
                ProgressTracker::new_for_test(),
                Box::new(|| ()),
            )
            .unwrap();

        let after_optimization_segments = locked_holder.read().iter().map(|(x, _)| x).collect_vec();

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
                assert_eq!(new_segment.get().read().available_point_count(), 3 * 3 + 10);
            }
        }

        // Check if optimized segments removed from disk
        old_path.into_iter().for_each(|x| assert!(!x.exists()));
    }

    #[rustfmt::skip]
    const TEST_TABLE: &[(usize, usize, &str)] = &[
        ( 1,  5, ""),
        ( 1, 33, ""),

        (17, 50, "10+11+12+13 =46 | 14+15+16 =45"),
        (18, 50, "10+11+12+13 =46"),
        (20, 50, "10+11+12 =33"),

        ( 1, 54, "10+11+12+13 =46 | 14+15+16 =45"),
        ( 1, 55, "10+11+12+13 =46 | 14+15+16 =45 | 17+18+19 =54"),
        ( 1, 60, "10+11+12+13 =46 | 14+15+16 =45 | 17+18+19 =54"),
        ( 1, 61, "10+11+12+13+14 =60 | 15+16+17 =48 | 18+19+20 =57"),
        ( 1, 66, "10+11+12+13+14 =60 | 15+16+17 =48 | 18+19+20 =57"),
        ( 1, 67, "10+11+12+13+14 =60 | 15+16+17+18 =66 | 19+20+21 =60"),
        ( 1, 70, "10+11+12+13+14 =60 | 15+16+17+18 =66 | 19+20+21 =60 | 22+23+24 =69"),

        ( 5, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21+22+23+24 =129 | 25+26+27+28+29 =135"),
        ( 6, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21+22+23+24 =129 | 25+26+27+28 =106"),
        ( 7, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21+22+23+24 =129 | 25+26+27 =78"),
        ( 8, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21+22+23+24 =129"),
        ( 9, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21+22+23+24 =129"),
        (10, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21+22+23 =105"),
        (11, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21+22 =82"),
        (12, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21 =60"),

        ( 2, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231 | 24+25+26+27+28+29+30 =189"),
        ( 3, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231 | 24+25+26+27+28+29+30 =189"),
        ( 4, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231 | 24+25+26+27+28+29 =159"),
        ( 5, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231 | 24+25+26+27+28 =130"),
        ( 6, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231 | 24+25+26+27 =102"),
        ( 7, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231 | 24+25+26 =75"),
        ( 8, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231"),
        ( 9, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231"),
        (10, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22 =208"),
        (11, 233, "10+11+12+13+14+15+16+17+18+19+20+21 =186"),
        (12, 233, "10+11+12+13+14+15+16+17+18+19+20 =165"),
    ];

    #[test]
    fn test_merge_optimizer_test_table() {
        let mut rng = StdRng::seed_from_u64(42);
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
        let dim = 256; // 1 KiB per vector

        let mut segment_sizes = (10..=30).collect_vec();
        segment_sizes.shuffle(&mut rng);
        let mut holder = SegmentHolder::default();
        let mut segment_id_to_size = HashMap::new();
        for &segment_size in &segment_sizes {
            let segment_id = holder.add_new(random_segment(dir.path(), 100, segment_size, dim));
            segment_id_to_size.insert(segment_id, segment_size);
        }
        let locked_holder = Arc::new(RwLock::new(holder));

        for &(default_segment_number, max_segment_size, expected) in TEST_TABLE {
            let mut merge_optimizer = get_merge_optimizer(dir.path(), temp_dir.path(), dim, None);
            merge_optimizer.thresholds_config.max_segment_size_kb = max_segment_size; // 1 KiB == 1 vector
            merge_optimizer.default_segments_number = default_segment_number;
            let result = merge_optimizer
                .plan_optimizations_for_test(&locked_holder)
                .into_iter()
                .map(|batch| {
                    let it = batch.iter().map(|id| segment_id_to_size[id]);
                    format!("{} ={}", it.clone().join("+"), it.clone().sum::<u64>())
                })
                .join(" | ");
            assert_eq!(
                result, expected,
                "Failed on ({default_segment_number}, {max_segment_size})"
            );
        }
    }
}
