use std::path::{Path, PathBuf};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use segment::common::operation_time_statistics::OperationDurationsAggregator;
use segment::entry::ReadSegmentEntry as _;
use segment::types::HnswGlobalConfig;

use super::config::SegmentOptimizerConfig;
use super::segment_optimizer::{OptimizationPlanner, PlanMode, SegmentOptimizer};
use crate::operations::optimization::OptimizerThresholds;

const BYTES_IN_KB: usize = 1024;

/// A candidate joins a merge batch only if it is at most this many times the
/// size of the batch so far (below the floor of one indexing threshold the
/// check does not apply). Without it, segments are taken smallest first and a
/// batch of two fresh segments gets padded with a near-full one, which then is
/// rewritten in full to absorb a few thousand points.
const MAX_MERGE_SIZE_RATIO: usize = 2;

/// A candidate that breaks `MAX_MERGE_SIZE_RATIO` may still join a batch if the
/// merge fills it to at least this percentage of `max_segment_size`. Under the
/// ratio alone, a segment between 2/3 of the size cap and the cap can never
/// merge again (it needs a batch of at least half its size, which overflows the
/// cap), so large segments would freeze well below the cap and leave more
/// segments than needed once uploads stop. Only segments below this fill level
/// qualify, so each segment takes at most one such merge.
const TOP_UP_PERCENT: usize = 90;

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
///
/// A batch never takes a segment larger than `MAX_MERGE_SIZE_RATIO` times the
/// batch so far (once the batch is past one indexing threshold):
///
/// - bad:   [2 7 971]    →  971 is rewritten to absorb 9
/// - good:  [2 7] held until more small segments arrive, or [300 400 450]
///
/// Unless the merge fills the candidate to `TOP_UP_PERCENT` of the size cap
/// (here with a cap of 2000):
///
/// - good:  [150 300 1400] →  1400 breaks the ratio, but grows to 1850
/// - bad:   [20 30 1900]   →  1900 is already full, rewritten to absorb 50
///
/// The ratio only applies while uploads are ongoing (`PlanMode::Ingest`). It
/// leaves a few segments that are too far apart in size to merge:
///
///     20 100 400 1900 1900 ...
///
/// Once uploads stop (`PlanMode::Idle`) no better batch will arrive, so they
/// are merged regardless of the ratio: [20 100 400].
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

        // Below this size a batch may absorb any candidate, so fresh (about
        // indexing-threshold sized) and empty segments still merge freely.
        let ratio_floor = self
            .thresholds_config
            .indexing_threshold_kb
            .min(self.thresholds_config.max_segment_size_kb)
            .saturating_mul(BYTES_IN_KB);

        // Holding a batch back only pays off if more segments are coming.
        let limit_size_ratio = match planner.mode() {
            PlanMode::Ingest => true,
            PlanMode::Idle => false,
        };

        // A merge reaching this size may break the ratio, see `TOP_UP_PERCENT`.
        let top_up_size = threshold / 100 * TOP_UP_PERCENT;

        let mut first_batch = None;
        let mut found_batch = false;
        let mut taken_candidates = 0;
        let mut last_candidate =
            (planner.expected_segments_number() + 2).saturating_sub(self.default_segments_number);
        while taken_candidates < last_candidate.min(candidates.len()) {
            let batch = candidates[taken_candidates..last_candidate.min(candidates.len())]
                .iter()
                .scan(None, |size_sum: &mut Option<usize>, &(segment_id, size)| {
                    // Size-tiered merging: don't rewrite a large segment just to
                    // absorb a few small ones. Each merge then grows its largest
                    // input by a meaningful fraction, which bounds how often a
                    // point is rewritten while a segment grows to the size cap.
                    if limit_size_ratio && let Some(sum) = *size_sum {
                        let fits_ratio =
                            size <= MAX_MERGE_SIZE_RATIO.saturating_mul(sum.max(ratio_floor));
                        let tops_up = size < top_up_size && sum.saturating_add(size) >= top_up_size;
                        if !fits_ratio && !tops_up {
                            return None;
                        }
                    }
                    let sum = size_sum.unwrap_or(0) + size;
                    *size_sum = Some(sum);
                    (sum < threshold).then_some(segment_id)
                })
                .collect_vec();

            if batch.len() < 2 {
                // Skip the head instead of ending planning: a lone small
                // segment (often the appendable one) must not block larger
                // similar-size segments from merging.
                taken_candidates += 1;
                last_candidate += 1;
                continue;
            }
            let is_first_batch = !found_batch;
            found_batch = true;
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
