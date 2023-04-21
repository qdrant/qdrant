use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use segment::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator,
};
use segment::types::{HnswConfig, QuantizationConfig, SegmentType};
use segment::vector_storage::VectorStorage;

use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentId,
};
use crate::collection_manager::optimizers::segment_optimizer::{
    OptimizerThresholds, SegmentOptimizer,
};
use crate::config::CollectionParams;

/// Looks for segments with vector storage that has a high disbalance of index and available
/// vectors
///
/// Since the index for a vector storage has been created, a lot of vectors in it may have been
/// deleted. That results in the index slowly breaking apart. This will find indexes with a high
/// ratio of deletions in it, and will schedule the segment to re-create to rebuild the index more
/// efficiently.
///
/// The process of index creation is slow and CPU-bounded, so it is convenient to perform
/// index building in a same way as segment re-creation.
pub struct SparseIndexOptimizer {
    /// Required minimum ratio of deleted / total vectors in a vector storage index to start reindexing.
    deleted_threshold: f64,
    /// Required minimum number of deleted vectors in a vector storage index to start reindexing.
    min_vectors_number: usize,
    thresholds_config: OptimizerThresholds,
    segments_path: PathBuf,
    collection_temp_dir: PathBuf,
    collection_params: CollectionParams,
    hnsw_config: HnswConfig,
    quantization_config: Option<QuantizationConfig>,
    telemetry_durations_aggregator: Arc<Mutex<OperationDurationsAggregator>>,
}

impl SparseIndexOptimizer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        deleted_threshold: f64,
        min_vectors_number: usize,
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        collection_temp_dir: PathBuf,
        collection_params: CollectionParams,
        hnsw_config: HnswConfig,
        quantization_config: Option<QuantizationConfig>,
    ) -> Self {
        SparseIndexOptimizer {
            deleted_threshold,
            min_vectors_number,
            thresholds_config,
            segments_path,
            collection_temp_dir,
            collection_params,
            hnsw_config,
            quantization_config,
            telemetry_durations_aggregator: OperationDurationsAggregator::new(),
        }
    }

    fn worst_segment(
        &self,
        segments: LockedSegmentHolder,
        excluded_ids: &HashSet<SegmentId>,
    ) -> Vec<SegmentId> {
        let segments_read_guard = segments.read();
        let candidate = segments_read_guard
            .iter()
            .filter(|(idx, _segment)| !excluded_ids.contains(idx))
            .filter_map(|(idx, segment)| {
                let segment_entry = segment.get();
                let read_segment = segment_entry.read();

                // Never optimize special segments
                if read_segment.segment_type() == SegmentType::Special {
                    return None;
                }

                // Segment must have an index
                let segment_config = read_segment.config();
                if !segment_config.is_vector_indexed() {
                    return None;
                }

                // We can only work with original segments
                let real_segment = match segment {
                    LockedSegment::Original(segment) => segment,
                    LockedSegment::Proxy(_) => return None,
                };

                // In this segment, check the index of each vector storage for a high deletion
                // count (sparse index). Count all deleted vectors of those that reach a threshold.
                let read_real_segment = real_segment.read();
                let deleted_vector_count = read_real_segment
                    .vector_data
                    .values()
                    .filter(|vector_data| vector_data.vector_index.borrow().is_index())
                    .filter_map(|vector_data| {
                        // We asume the storage and its index are created at the same time. We
                        // therefore know the number of deleted vectors then and now. Based on that we
                        // can determine what ratio of points from the index is deleted.
                        // TODO: problem: point/vector deletes overlap
                        let vector_storage = vector_data.vector_storage.borrow();
                        let create_deleted_vec_count = vector_storage.create_deleted_vec_count();
                        let full_index_count =
                            vector_storage.total_vector_count() - create_deleted_vec_count;
                        let deleted_index_count =
                            vector_storage.deleted_vec_count() - create_deleted_vec_count;
                        let deleted_ratio = if full_index_count != 0 {
                            deleted_index_count as f64 / full_index_count as f64
                        } else {
                            0.0
                        };

                        let reached_minimum = deleted_index_count >= self.min_vectors_number;
                        let reached_ratio = deleted_ratio > self.deleted_threshold;
                        (reached_minimum && reached_ratio).then_some(deleted_index_count)
                    })
                    .sum::<usize>();

                (deleted_vector_count > 0).then_some((*idx, deleted_vector_count))
            })
            .max_by_key(|(_, vector_size)| *vector_size)
            .map(|(idx, _)| idx);
        Vec::from_iter(candidate)
    }
}

impl SegmentOptimizer for SparseIndexOptimizer {
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

    fn quantization_config(&self) -> Option<QuantizationConfig> {
        self.quantization_config.clone()
    }

    fn threshold_config(&self) -> &OptimizerThresholds {
        &self.thresholds_config
    }

    fn check_condition(
        &self,
        segments: LockedSegmentHolder,
        excluded_ids: &HashSet<SegmentId>,
    ) -> Vec<SegmentId> {
        self.worst_segment(segments, excluded_ids)
    }

    fn get_telemetry_data(&self) -> OperationDurationStatistics {
        self.get_telemetry_counter().lock().get_statistics()
    }

    fn get_telemetry_counter(&self) -> Arc<Mutex<OperationDurationsAggregator>> {
        self.telemetry_durations_aggregator.clone()
    }
}
