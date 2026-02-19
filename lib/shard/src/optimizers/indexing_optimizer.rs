use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use segment::common::operation_time_statistics::OperationDurationsAggregator;
use segment::entry::NonAppendableSegmentEntry as _;
use segment::segment::Segment;
use segment::types::{HnswConfig, HnswGlobalConfig};

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
    segment_config: SegmentOptimizerConfig,
    hnsw_config: HnswConfig,
    hnsw_global_config: HnswGlobalConfig,
    telemetry_durations_aggregator: Arc<Mutex<OperationDurationsAggregator>>,
}

impl IndexingOptimizer {
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
        IndexingOptimizer {
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

        for (vector_name, vector_cfg) in &self.segment_config.dense_vector {
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

                if optimize_for_index || optimize_for_mmap {
                    return true;
                }
            }
        }

        for sparse_vector_name in self.segment_config.sparse_vector.keys() {
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
    }

    fn get_telemetry_counter(&self) -> &Mutex<OperationDurationsAggregator> {
        &self.telemetry_durations_aggregator
    }
}
