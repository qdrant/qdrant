use std::path::{Path, PathBuf};
use std::sync::Arc;

use itertools::Itertools;
use ordered_float::OrderedFloat;
use parking_lot::Mutex;
use segment::common::operation_time_statistics::OperationDurationsAggregator;
use segment::entry::entry_point::NonAppendableSegmentEntry;
use segment::index::VectorIndex;
use segment::segment::Segment;
use segment::types::{HnswConfig, HnswGlobalConfig};
use segment::vector_storage::VectorStorage;

use super::config::SegmentOptimizerConfig;
use super::segment_optimizer::{OptimizationPlanner, SegmentOptimizer};
use crate::operations::optimization::OptimizerThresholds;

/// Optimizer which looks for segments with high amount of soft-deleted points or vectors
///
/// Since the creation of a segment, a lot of points or vectors may have been soft-deleted. This
/// results in the index slowly breaking apart, and unnecessary storage usage.
///
/// This optimizer will look for the worst segment to rebuilt the index and minimize storage usage.
pub struct VacuumOptimizer {
    deleted_threshold: f64,
    min_vectors_number: usize,
    thresholds_config: OptimizerThresholds,
    segments_path: PathBuf,
    temp_path: PathBuf,
    segment_config: SegmentOptimizerConfig,
    hnsw_config: HnswConfig,
    hnsw_global_config: HnswGlobalConfig,
    telemetry_durations_aggregator: Arc<Mutex<OperationDurationsAggregator>>,
}

impl VacuumOptimizer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        deleted_threshold: f64,
        min_vectors_number: usize,
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        temp_path: PathBuf,
        segment_config: SegmentOptimizerConfig,
        hnsw_config: HnswConfig,
        hnsw_global_config: HnswGlobalConfig,
    ) -> Self {
        VacuumOptimizer {
            deleted_threshold,
            min_vectors_number,
            thresholds_config,
            segments_path,
            temp_path,
            segment_config,
            hnsw_config,
            hnsw_global_config,
            telemetry_durations_aggregator: OperationDurationsAggregator::new(),
        }
    }

    /// Calculate littered ratio for segment on point level
    ///
    /// Returns `None` if littered ratio did not reach vacuum thresholds.
    fn littered_ratio_segment(&self, segment: &Segment) -> Option<f64> {
        let littered_ratio =
            segment.deleted_point_count() as f64 / segment.total_point_count() as f64;
        let is_big = segment.total_point_count() >= self.min_vectors_number;
        let is_littered = littered_ratio > self.deleted_threshold;

        (is_big && is_littered).then_some(littered_ratio)
    }

    /// Calculate littered ratio for segment on vector index level
    ///
    /// If a segment has multiple named vectors, it checks each one.
    /// We are only interested in indexed vectors, as they are the ones affected by soft-deletes.
    ///
    /// This finds the maximum deletion ratio for a named vector. The ratio is based on the number
    /// of deleted vectors versus the number of indexed vectors.
    ///
    /// Returns `None` if littered ratio did not reach vacuum thresholds for no named vectors.
    fn littered_vectors_index_ratio(&self, segment: &Segment) -> Option<f64> {
        // Segment must have any index
        let segment_config = segment.config();
        if !segment_config.is_any_vector_indexed() {
            return None;
        }

        // In this segment, check the index of each named vector for a high deletion ratio.
        // Return the worst ratio.
        segment
            .vector_data
            .values()
            .filter(|vector_data| vector_data.vector_index.borrow().is_index())
            .filter_map(|vector_data| {
                // We use the number of now available vectors against the number of indexed vectors
                // to determine how many are soft-deleted from the index.
                let vector_index = vector_data.vector_index.borrow();
                let vector_storage = vector_data.vector_storage.borrow();
                let indexed_vector_count = vector_index.indexed_vector_count();
                let deleted_from_index =
                    indexed_vector_count.saturating_sub(vector_storage.available_vector_count());
                let deleted_ratio = if indexed_vector_count != 0 {
                    deleted_from_index as f64 / indexed_vector_count as f64
                } else {
                    0.0
                };

                let reached_minimum = deleted_from_index >= self.min_vectors_number;
                let reached_ratio = deleted_ratio > self.deleted_threshold;
                (reached_minimum && reached_ratio).then_some(deleted_ratio)
            })
            .max_by_key(|ratio| OrderedFloat(*ratio))
    }
}

impl SegmentOptimizer for VacuumOptimizer {
    fn name(&self) -> &'static str {
        "vacuum"
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
        let to_optimize = planner
            .remaining()
            .iter()
            .filter_map(|(&segment_id, segment)| {
                let segment = segment.read();
                let littered_ratio_segment = self.littered_ratio_segment(&segment);
                let littered_ratio_vectors = self.littered_vectors_index_ratio(&segment);
                let worst_ratio = std::iter::chain(littered_ratio_segment, littered_ratio_vectors)
                    .max_by_key(|ratio| OrderedFloat(*ratio));
                worst_ratio.map(|ratio| (segment_id, ratio))
            })
            .sorted_by_key(|(_, ratio)| OrderedFloat(-ratio))
            .collect_vec();
        for (segment_id, _) in to_optimize {
            planner.plan(vec![segment_id]);
        }
    }

    fn get_telemetry_counter(&self) -> &Mutex<OperationDurationsAggregator> {
        &self.telemetry_durations_aggregator
    }
}
