use std::path::{Path, PathBuf};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use segment::common::operation_time_statistics::OperationDurationsAggregator;
use segment::entry::NonAppendableSegmentEntry as _;
use segment::types::{HnswConfig, HnswGlobalConfig};

use super::config::SegmentOptimizerConfig;
use super::segment_optimizer::{OptimizationPlanner, SegmentOptimizer};
use crate::operations::optimization::OptimizerThresholds;

const BYTES_IN_KB: usize = 1024;

/// Optimizer that tries to reduce number of segments until it fits configured
/// value.
///
/// ```text
/// Suppose we have a set of mergeable segments, sorted by size.
/// `A` is smallest, `M` is largest.
///
///     A B C D E F G H I J K L M
///
/// MergeOptimizer greedily arranges them in batches up to the size threshold.
///
///     [A B C D] [E F G] [H I J] K L M
///     └───X───┘ └──Y──┘ └──Z──┘
///
/// After merging these batches, our segments would look like this:
///
///     ∅ X Y Z K L M
///
/// `∅` is the newly created appendable segment that Qdrant could potentially
/// create because MergeOptimizer merged the last appendable segment.
///
/// To guarantee that the number of segments will be reduced after the merge,
/// either merge a batch of at least 3 segments, or merge at least two batches.
///
/// - bad:   [A B]        →  ∅ X    (segment count is the same)
/// - good:  [A B C]      →  ∅ X    (one segment less)
/// - good:  [A B] [C D]  →  ∅ X Y  (one segment less)
/// ```
pub struct MergeOptimizer {
    default_segments_number: usize,
    thresholds_config: OptimizerThresholds,
    segments_path: PathBuf,
    temp_path: PathBuf,
    segment_config: SegmentOptimizerConfig,
    hnsw_config: HnswConfig,
    hnsw_global_config: HnswGlobalConfig,
    telemetry_durations_aggregator: Arc<Mutex<OperationDurationsAggregator>>,
}

impl MergeOptimizer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        default_segments_number: usize,
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        temp_path: PathBuf,
        segment_config: SegmentOptimizerConfig,
        hnsw_config: HnswConfig,
        hnsw_global_config: HnswGlobalConfig,
    ) -> Self {
        Self {
            default_segments_number,
            thresholds_config,
            segments_path,
            temp_path,
            segment_config,
            hnsw_config,
            hnsw_global_config,
            telemetry_durations_aggregator: OperationDurationsAggregator::new(),
        }
    }
}

impl SegmentOptimizer for MergeOptimizer {
    fn name(&self) -> &'static str {
        "merge"
    }

    fn segments_path(&self) -> &Path {
        self.segments_path.as_path()
    }

    fn temp_path(&self) -> &Path {
        self.temp_path.as_path()
    }

    fn segment_config(&self) -> &SegmentOptimizerConfig {
        &self.segment_config
    }

    fn hnsw_config(&self) -> &HnswConfig {
        &self.hnsw_config
    }

    fn hnsw_global_config(&self) -> &HnswGlobalConfig {
        &self.hnsw_global_config
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

        let mut first_batch = None;
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

            if batch.len() < 2 {
                return;
            }
            let is_first_batch = taken_candidates == 0;
            taken_candidates += batch.len();
            last_candidate += 1;
            if is_first_batch && batch.len() < 3 {
                // First batch has length 2. To guarantee that the number of
                // segments will be reduced, we need another batch.
                // So, hold the first batch until we find the second one.
                first_batch = Some(batch);
                continue;
            }
            if let Some(first_batch) = first_batch.take() {
                planner.plan(first_batch);
            }
            planner.plan(batch);
        }
    }

    fn get_telemetry_counter(&self) -> &Mutex<OperationDurationsAggregator> {
        &self.telemetry_durations_aggregator
    }
}
