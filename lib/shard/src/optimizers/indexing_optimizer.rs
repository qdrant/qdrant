use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use segment::common::operation_time_statistics::OperationDurationsAggregator;
use segment::entry::ReadSegmentEntry as _;
use segment::segment::Segment;
use segment::types::HnswGlobalConfig;

use super::config::SegmentOptimizerConfig;
use super::segment_optimizer::{OptimizationPlanner, SegmentOptimizer};
use crate::operations::optimization::OptimizerThresholds;
use crate::segment_holder::SegmentId;

const BYTES_IN_KB: usize = 1024;

/// Looks for the segments, which require to be indexed.
///
/// If segment is too large, but still does not have indexes - it is time to create some indexes.
/// The process of index creation is slow and CPU-bounded, so it is convenient to perform
/// index building in a same way as segment re-creation.
pub struct IndexingOptimizer {
    default_segments_number: usize,
    thresholds_config: OptimizerThresholds,
    segments_path: PathBuf,
    temp_path: PathBuf,
    segment_optimizer_config: SegmentOptimizerConfig,
    hnsw_global_config: HnswGlobalConfig,
    telemetry_durations_aggregator: Arc<Mutex<OperationDurationsAggregator>>,
}

impl IndexingOptimizer {
    pub fn new(
        default_segments_number: usize,
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        temp_path: PathBuf,
        segment_config: SegmentOptimizerConfig,
        hnsw_global_config: HnswGlobalConfig,
    ) -> Self {
        IndexingOptimizer {
            default_segments_number,
            thresholds_config,
            segments_path,
            temp_path,
            segment_optimizer_config: segment_config,
            hnsw_global_config,
            telemetry_durations_aggregator: OperationDurationsAggregator::new(),
        }
    }

    fn is_optimization_required(&self, segment: &Segment) -> bool {
        let segment_data_config = segment.config();
        let indexing_threshold_bytes = self
            .thresholds_config
            .indexing_threshold_kb
            .saturating_mul(BYTES_IN_KB);
        let mmap_threshold_bytes = self
            .thresholds_config
            .memmap_threshold_kb
            .saturating_mul(BYTES_IN_KB);

        let has_deferred_points = segment.has_deferred_points();

        for (vector_name, vector_cfg) in &self.segment_optimizer_config.dense_vector {
            if let Some(vector_data) = segment_data_config.vector_data.get(vector_name) {
                let is_indexed = vector_data.index.is_indexed();
                let is_on_disk = vector_data.storage_type.is_on_disk();
                let storage_size_bytes = segment
                    .available_vectors_size_in_bytes(vector_name)
                    .unwrap_or_default();

                let is_big_for_index = storage_size_bytes >= indexing_threshold_bytes;
                let is_big_for_mmap = storage_size_bytes >= mmap_threshold_bytes;

                let optimize_for_index = is_big_for_index && !is_indexed;
                let optimize_for_mmap = if let Some(on_disk_config) = vector_cfg.on_disk {
                    on_disk_config && !is_on_disk
                } else {
                    is_big_for_mmap && !is_on_disk
                };

                if optimize_for_index || optimize_for_mmap || has_deferred_points {
                    return true;
                }
            }
        }

        for sparse_vector_name in self.segment_optimizer_config.sparse_vector.keys() {
            if let Some(sparse_vector_data) = segment_data_config
                .sparse_vector_data
                .get(sparse_vector_name)
            {
                let is_index_immutable = sparse_vector_data.index.index_type.is_immutable();

                let storage_size = segment
                    .available_vectors_size_in_bytes(sparse_vector_name)
                    .unwrap_or_default();

                let is_big_for_index = storage_size >= indexing_threshold_bytes;
                let is_big_for_mmap = storage_size >= mmap_threshold_bytes;

                let is_big = is_big_for_index || is_big_for_mmap;

                if is_big && !is_index_immutable {
                    return true;
                }
            }
        }

        false
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn threshold_config_mut_for_test(&mut self) -> &mut OptimizerThresholds {
        &mut self.thresholds_config
    }

    /// The indexing optimizer normally builds indexes for segments that have
    /// grown past the indexing threshold. This is a special case going the
    /// other way: non-appendable segments below the HNSW full-scan boundary
    /// are scanned in full by every search request, so we merge them into a
    /// bigger segment to give their points a usable HNSW graph. Merging only
    /// ever reduces the segment count, and the result lands above the
    /// boundary, so this converges.
    fn plan_sub_full_scan_tail_merges(&self, planner: &mut OptimizationPlanner) {
        // While the segment count is above the target, the merge optimizer is
        // still coalescing small segments and picks the tails up itself.
        if planner.expected_segments_number() > self.default_segments_number {
            return;
        }

        let max_segment_size_bytes = self
            .thresholds_config
            .max_segment_size_kb
            .saturating_mul(BYTES_IN_KB);

        // Per-field full-scan boundaries in bytes, in a fixed field order
        // that the per-segment size vectors below follow as well.
        let field_thresholds = self
            .segment_optimizer_config
            .dense_vector
            .iter()
            .map(|(name, cfg)| {
                (
                    name,
                    cfg.hnsw_config
                        .full_scan_threshold
                        .saturating_mul(BYTES_IN_KB),
                )
            })
            .collect_vec();
        let Some(full_scan_bytes) = field_thresholds.iter().map(|&(_, t)| t).min() else {
            return;
        };

        let mut candidates = planner
            .remaining()
            .iter()
            .map(|(&segment_id, segment)| {
                let segment = segment.read();
                let size = segment
                    .max_available_vectors_size_in_bytes()
                    .unwrap_or_default();
                let field_sizes = field_thresholds
                    .iter()
                    .map(|(name, _)| {
                        segment
                            .available_vectors_size_in_bytes(name)
                            .unwrap_or_default()
                    })
                    .collect_vec();
                (segment_id, size, field_sizes, segment.is_appendable())
            })
            .collect_vec();
        candidates.sort_by_key(|(_segment_id, size, ..)| *size);

        // `size` is the LARGEST vector field of a segment
        // (`max_available_vectors_size_in_bytes`), so pair it with the
        // SMALLEST per-field threshold: a segment qualifies as a tail only
        // when even its biggest field stays under the lowest boundary.
        // That is the conservative half of the comparison — it may leave
        // some multi-vector tails unmerged, but never merges a segment
        // that no index would full-scan.
        //
        // Only frozen (non-appendable) segments qualify as tails: appendable
        // segments are still receiving writes and will grow past the
        // boundary or be optimized on their own.
        let tails = candidates
            .iter()
            .filter(|(_, size, _, appendable)| !*appendable && *size > 0 && *size < full_scan_bytes)
            .scan(0, |size_sum, candidate| {
                *size_sum += candidate.1;
                (*size_sum < max_segment_size_bytes).then_some(candidate)
            })
            .collect_vec();
        if tails.is_empty() {
            return;
        }
        let tails_size: usize = tails.iter().map(|(_, size, ..)| *size).sum();
        let mut batch = tails
            .iter()
            .map(|(segment_id, ..)| *segment_id)
            .collect_vec();
        // The crossing check below must hold for an actual vector field, not
        // for a sum of per-segment maxima: with several fields the maxima can
        // alternate between fields and overstate every individual field's
        // merged size, planning a merge whose result is still scanned in
        // full.
        let mut field_sums = vec![0usize; field_thresholds.len()];
        for (_, _, field_sizes, _) in &tails {
            for (sum, size) in field_sums.iter_mut().zip(field_sizes) {
                *sum += size;
            }
        }
        let crosses = |field_sums: &[usize]| {
            field_sums
                .iter()
                .zip(&field_thresholds)
                .any(|(&sum, &(_, threshold))| sum >= threshold)
        };
        // Merge the tails into the smallest regular segment that fits and
        // lands some field of the result above that field's own boundary.
        // With a single vector field any regular segment qualifies — it is
        // above the boundary by itself.
        if let Some((segment_id, _, field_sizes, _)) =
            candidates
                .iter()
                .find(|(_, size, field_sizes, appendable)| {
                    !*appendable
                        && *size >= full_scan_bytes
                        && tails_size.saturating_add(*size) < max_segment_size_bytes
                        && crosses(
                            &field_sums
                                .iter()
                                .zip(field_sizes)
                                .map(|(&sum, &size)| sum.saturating_add(size))
                                .collect_vec(),
                        )
                })
        {
            batch.push(*segment_id);
            for (sum, size) in field_sums.iter_mut().zip(field_sizes) {
                *sum = sum.saturating_add(*size);
            }
        }
        // Plan only when some field of the merged result actually crosses its
        // boundary (with a partner that holds by construction; tails alone
        // must add up to it). Merging keeps the scanned point count the same,
        // so a batch that stays below the boundary would reduce the segment
        // count — possibly far below the target — without making anything
        // searchable by graph. In particular, when `full_scan_threshold`
        // exceeds `max_segment_size` no segment can ever cross, and this
        // plans nothing at all.
        if batch.len() >= 2 && crosses(&field_sums) {
            planner.plan(batch);
        }
    }
}

impl SegmentOptimizer for IndexingOptimizer {
    fn name(&self) -> &'static str {
        "indexing"
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
        let max_segment_size_bytes = self
            .thresholds_config
            .max_segment_size_kb
            .saturating_mul(BYTES_IN_KB);

        let mut unindexed = VecDeque::<(SegmentId, usize)>::new();
        let mut indexed = VecDeque::<(SegmentId, usize)>::new();
        for (&segment_id, segment) in planner.remaining().iter() {
            let segment = segment.read();
            let vector_size_bytes = segment
                .max_available_vectors_size_in_bytes()
                .unwrap_or_default();
            if self.is_optimization_required(&segment) {
                unindexed.push_back((segment_id, vector_size_bytes));
            }

            let segment_config = segment.config();
            if segment_config.is_any_vector_indexed() || segment_config.is_any_on_disk() {
                indexed.push_back((segment_id, vector_size_bytes));
            }
        }
        unindexed.make_contiguous().sort_by_key(|(_, size)| *size);
        indexed.make_contiguous().sort_by_key(|(_, size)| *size);

        // Select the largest unindexed segment
        while let Some((selected_segment_id, selected_segment_size)) = unindexed.pop_back() {
            if !planner.remaining().contains_key(&selected_segment_id) {
                continue;
            }

            // If the number of segments if equal or bigger than the default_segments_number
            // We want to make sure that we at least do not increase number of segments after optimization, thus we take more than one segment to optimize
            if planner.expected_segments_number() < self.default_segments_number {
                planner.plan(vec![selected_segment_id]);
                continue;
            }

            // It is better for scheduling if indexing optimizer optimizes 2 segments.
            // Because result of the optimization is usually 2 segment - it should preserve
            // overall count of segments.

            // Find the smallest unindexed to check if we can index together
            if let Some(&(segment_id, size)) = unindexed.front()
                && planner.remaining().contains_key(&segment_id)
                && selected_segment_size + size < max_segment_size_bytes
            {
                unindexed.pop_front();
                planner.plan(vec![selected_segment_id, segment_id]);
                continue;
            }

            // Find smallest indexed to check if we can reindex together
            if let Some(&(segment_id, size)) = indexed.front()
                && planner.remaining().contains_key(&segment_id)
                && segment_id != selected_segment_id
                && selected_segment_size + size < max_segment_size_bytes
            {
                indexed.pop_front();
                planner.plan(vec![selected_segment_id, segment_id]);
                continue;
            }

            planner.plan(vec![selected_segment_id]);
        }

        self.plan_sub_full_scan_tail_merges(planner);
    }

    fn get_telemetry_counter(&self) -> &Mutex<OperationDurationsAggregator> {
        &self.telemetry_durations_aggregator
    }
}
