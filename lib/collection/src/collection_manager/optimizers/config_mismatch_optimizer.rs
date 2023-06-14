use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use segment::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator,
};
use segment::types::{HnswConfig, Indexes, QuantizationConfig, SegmentType, VECTOR_ELEMENT_SIZE};

use crate::collection_manager::holders::segment_holder::{LockedSegmentHolder, SegmentId};
use crate::collection_manager::optimizers::segment_optimizer::{
    OptimizerThresholds, SegmentOptimizer,
};
use crate::config::CollectionParams;
use crate::operations::config_diff::DiffConfig;

/// Looks for segments having a mismatch between configured and actual parameters
///
/// For example, a user may change the HNSW parameters for a collection. A segment that was already
/// indexed with different parameters now has a mismatch. This segment should be optimized (and
/// indexed) again in order to update the effective configuration.
pub struct ConfigMismatchOptimizer {
    thresholds_config: OptimizerThresholds,
    segments_path: PathBuf,
    collection_temp_dir: PathBuf,
    collection_params: CollectionParams,
    hnsw_config: HnswConfig,
    quantization_config: Option<QuantizationConfig>,
    telemetry_durations_aggregator: Arc<Mutex<OperationDurationsAggregator>>,
}

impl ConfigMismatchOptimizer {
    pub fn new(
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        collection_temp_dir: PathBuf,
        collection_params: CollectionParams,
        hnsw_config: HnswConfig,
        quantization_config: Option<QuantizationConfig>,
    ) -> Self {
        ConfigMismatchOptimizer {
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
        let candidates: Vec<_> = segments_read_guard
            .iter()
            // Excluded externally, might already be scheduled for optimization
            .filter(|(idx, _)| !excluded_ids.contains(idx))
            .filter_map(|(idx, segment)| {
                let segment_entry = segment.get();
                let read_segment = segment_entry.read();
                let point_count = read_segment.available_point_count();
                let vector_size = point_count
                    * read_segment
                        .vector_dims()
                        .values()
                        .max()
                        .copied()
                        .unwrap_or(0)
                    * VECTOR_ELEMENT_SIZE;

                let segment_config = read_segment.config();

                if read_segment.segment_type() == SegmentType::Special {
                    return None; // Never optimize already optimized segment
                }

                // Determine whether segment has mismatch
                let has_mismatch =
                    segment_config
                        .vector_data
                        .iter()
                        .any(|(vector_name, vector_data)| {
                            // Check HNSW mismatch
                            match &vector_data.index {
                                Indexes::Plain {} => {}
                                Indexes::Hnsw(effective_hnsw) => {
                                    // Select vector specific target HNSW config
                                    let target_hnsw_collection = &self.hnsw_config;
                                    let target_hnsw_vector = self
                                        .collection_params
                                        .vectors
                                        .get_params(vector_name)
                                        .and_then(|vector_params| vector_params.hnsw_config)
                                        .map(|vector_hnsw| vector_hnsw.update(target_hnsw_collection))
                                        .and_then(|hnsw| match hnsw {
                                            Ok(hnsw) => Some(hnsw),
                                            Err(err) => {
                                                log::warn!("Failed to merge collection and vector HNSW config, ignoring: {err}");
                                                None
                                            }
                                        });
                                    let target_hnsw = target_hnsw_vector
                                        .as_ref()
                                        .unwrap_or(target_hnsw_collection);

                                    // Select segment if we have an HNSW mismatch that requires rebuild
                                    if effective_hnsw.mismatch_requires_rebuild(target_hnsw) {
                                        return true;
                                    }
                                }
                            }

                            false
                        });

                has_mismatch.then_some((*idx, vector_size))
            })
            .collect();

        // Select segment with largest vector size
        candidates
            .into_iter()
            .max_by_key(|(_, vector_size)| *vector_size)
            .map(|(segment_id, _)| segment_id)
            .into_iter()
            .collect()
    }
}

impl SegmentOptimizer for ConfigMismatchOptimizer {
    fn collection_path(&self) -> &Path {
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
