use std::path::{Path, PathBuf};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use segment::common::operation_time_statistics::OperationDurationsAggregator;
use segment::entry::ReadSegmentEntry as _;
use segment::types::HnswGlobalConfig;

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
    segment_optimizer_config: SegmentOptimizerConfig,
    hnsw_global_config: HnswGlobalConfig,
    telemetry_durations_aggregator: Arc<Mutex<OperationDurationsAggregator>>,
}

impl MergeOptimizer {
    pub fn new(
        default_segments_number: usize,
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        temp_path: PathBuf,
        segment_config: SegmentOptimizerConfig,
        hnsw_global_config: HnswGlobalConfig,
    ) -> Self {
        Self {
            default_segments_number,
            thresholds_config,
            segments_path,
            temp_path,
            segment_optimizer_config: segment_config,
            hnsw_global_config,
            telemetry_durations_aggregator: OperationDurationsAggregator::new(),
        }
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn threshold_config_mut_for_test(&mut self) -> &mut OptimizerThresholds {
        &mut self.thresholds_config
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn set_default_segments_number_for_test(&mut self, value: usize) {
        self.default_segments_number = value;
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

    fn segment_optimizer_config(&self) -> &SegmentOptimizerConfig {
        &self.segment_optimizer_config
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

        // Segments below the HNSW full-scan boundary are effectively scanned in
        // full by every search request, so they must not be left behind
        // permanently. When the segment-count target is already reached (the
        // loop below would plan nothing), merge all non-empty sub-threshold
        // tails together with the smallest regular segment: merging only
        // reduces the segment count, so the target is not violated, and the
        // resulting segment ends up above the boundary, so this converges.
        if planner.expected_segments_number() <= self.default_segments_number {
            // `size` above is the LARGEST vector field of a segment
            // (`max_available_vectors_size_in_bytes`), so pair it with the
            // SMALLEST per-field threshold: a segment qualifies as a tail only
            // when even its biggest field stays under the lowest boundary.
            // That is the conservative half of the comparison — it may leave
            // some multi-vector tails unmerged, but never merges a segment
            // that no index would full-scan.
            let full_scan_bytes = self
                .segment_optimizer_config
                .dense_vector
                .values()
                .map(|cfg| cfg.hnsw_config.full_scan_threshold)
                .min()
                .unwrap_or(0)
                .saturating_mul(BYTES_IN_KB);
            // Only frozen (non-appendable) segments qualify as tails: appendable
            // segments are still receiving writes and will grow past the
            // boundary or be optimized on their own.
            // Sizes ride along with the ids: the running sum is needed right
            // below, and recovering it from `candidates` afterwards would mean
            // looking up every id again.
            let tails = candidates
                .iter()
                .filter(|&&(segment_id, size)| {
                    size > 0
                        && size < full_scan_bytes
                        && planner
                            .remaining()
                            .get(&segment_id)
                            .is_some_and(|segment| !segment.read().is_appendable())
                })
                .scan(0, |size_sum, &(segment_id, size)| {
                    *size_sum += size;
                    (*size_sum < threshold).then_some((segment_id, size))
                })
                .collect_vec();
            if !tails.is_empty() {
                let tails_size: usize = tails.iter().map(|&(_, size)| size).sum();
                let mut batch = tails
                    .iter()
                    .map(|&(segment_id, _)| segment_id)
                    .collect_vec();
                // Merge the tails into the smallest regular segment when one
                // fits, so the result lands above the full-scan boundary.
                if let Some(&(segment_id, _)) = candidates.iter().find(|&&(segment_id, size)| {
                    size >= full_scan_bytes
                        && tails_size.saturating_add(size) < threshold
                        && planner
                            .remaining()
                            .get(&segment_id)
                            .is_some_and(|segment| !segment.read().is_appendable())
                }) {
                    batch.push(segment_id);
                }
                if batch.len() >= 2 {
                    planner.plan(batch);
                }
            }
            return;
        }

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
