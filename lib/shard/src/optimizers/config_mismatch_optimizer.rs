use std::cmp::Reverse;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use segment::common::operation_time_statistics::OperationDurationsAggregator;
use segment::entry::NonAppendableSegmentEntry;
use segment::index::sparse_index::sparse_index_config::SparseIndexType;
use segment::types::{HnswConfig, HnswGlobalConfig, Indexes, VectorName};

use super::config::SegmentOptimizerConfig;
use super::segment_optimizer::{OptimizationPlanner, SegmentOptimizer};
use crate::operations::optimization::OptimizerThresholds;

/// Looks for segments having a mismatch between configured and actual parameters
///
/// For example, a user may change the HNSW parameters for a collection. A segment that was already
/// indexed with different parameters now has a mismatch. This segment should be optimized (and
/// indexed) again in order to update the effective configuration.
pub struct ConfigMismatchOptimizer {
    thresholds_config: OptimizerThresholds,
    segments_path: PathBuf,
    temp_path: PathBuf,
    segment_config: SegmentOptimizerConfig,
    hnsw_config: HnswConfig,
    hnsw_global_config: HnswGlobalConfig,
    telemetry_durations_aggregator: Arc<Mutex<OperationDurationsAggregator>>,
}

impl ConfigMismatchOptimizer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        temp_path: PathBuf,
        segment_config: SegmentOptimizerConfig,
        hnsw_config: HnswConfig,
        hnsw_global_config: HnswGlobalConfig,
    ) -> Self {
        ConfigMismatchOptimizer {
            thresholds_config,
            segments_path,
            temp_path,
            segment_config,
            hnsw_config,
            hnsw_global_config,
            telemetry_durations_aggregator: OperationDurationsAggregator::new(),
        }
    }

    /// Check if current configuration requires vectors to be stored on disk
    fn check_if_vectors_on_disk(&self, vector_name: &VectorName) -> Option<bool> {
        self.segment_config
            .dense_vector
            .get(vector_name)
            .and_then(|cfg| cfg.on_disk)
    }

    /// Check if current configuration requires sparse vectors index to be stored on disk
    fn check_if_sparse_vectors_index_on_disk(&self, vector_name: &VectorName) -> Option<bool> {
        self.segment_config
            .sparse_vector
            .get(vector_name)
            .and_then(|cfg| cfg.on_disk)
    }

    fn has_config_mismatch(&self, segment: &dyn NonAppendableSegmentEntry) -> bool {
        let segment_config = segment.config();

        if self.segment_config.payload_storage_type.is_on_disk()
            != segment_config.payload_storage_type.is_on_disk()
        {
            return true; // Optimize segment due to payload storage mismatch
        }

        // Determine whether dense data in segment has mismatch
        let dense_has_mismatch =
            segment_config
                .vector_data
                .iter()
                .any(|(vector_name, vector_data)| {
                    // Check HNSW mismatch
                    match &vector_data.index {
                        Indexes::Plain {} => {}
                        Indexes::Hnsw(effective_hnsw) => {
                            // Select segment if we have an HNSW mismatch that requires rebuild
                            let target_hnsw = self
                                .segment_config
                                .dense_vector
                                .get(vector_name)
                                .map(|cfg| cfg.hnsw_config)
                                .unwrap_or(self.hnsw_config);
                            if effective_hnsw.mismatch_requires_rebuild(&target_hnsw) {
                                return true;
                            }
                        }
                    }

                    if let Some(is_required_on_disk) = self.check_if_vectors_on_disk(vector_name)
                        && is_required_on_disk != vector_data.storage_type.is_on_disk()
                    {
                        return true;
                    }

                    // Check quantization mismatch
                    let target_quantization = self
                        .segment_config
                        .dense_vector
                        .get(vector_name)
                        .and_then(|cfg| cfg.quantization_config.as_ref());

                    vector_data
                        .quantization_config
                        .as_ref()
                        .zip(target_quantization)
                        // Rebuild if current parameters differ from target parameters
                        .map(|(current, target)| current.mismatch_requires_rebuild(target))
                        // Or rebuild if we now change the enabled state on an indexed segment
                        .unwrap_or_else(|| {
                            let vector_data_quantization_appendable = vector_data
                                .quantization_config
                                .as_ref()
                                .map(|q| q.supports_appendable())
                                .unwrap_or(false);
                            let target_quantization_appendable = target_quantization
                                .map(|q| q.supports_appendable())
                                .unwrap_or(false);
                            // If segment is unindexed, only appendable quantization is applied.
                            // So that we check if any config is appendable to avoid infinity loop here.
                            let unindexed_changed = common::flags::feature_flags()
                                .appendable_quantization
                                && (vector_data_quantization_appendable
                                    || target_quantization_appendable);
                            (vector_data.quantization_config.is_some()
                                != target_quantization.is_some())
                                && (vector_data.index.is_indexed() || unindexed_changed)
                        })
                });

        // Determine whether sparse data in segment has mismatch
        let sparse_has_mismatch =
            segment_config
                .sparse_vector_data
                .iter()
                .any(|(vector_name, vector_data)| {
                    let Some(is_required_on_disk) =
                        self.check_if_sparse_vectors_index_on_disk(vector_name)
                    else {
                        return false; // Do nothing if not specified
                    };

                    match vector_data.index.index_type {
                        SparseIndexType::MutableRam => false, // Do nothing for mutable RAM
                        SparseIndexType::ImmutableRam => is_required_on_disk, // Rebuild if we require on disk
                        SparseIndexType::Mmap => !is_required_on_disk, // Rebuild if we require in RAM
                    }
                });

        sparse_has_mismatch || dense_has_mismatch
    }
}

impl SegmentOptimizer for ConfigMismatchOptimizer {
    fn name(&self) -> &'static str {
        "config mismatch"
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
                self.has_config_mismatch(&*segment).then(|| {
                    let vector_size = segment
                        .max_available_vectors_size_in_bytes()
                        .unwrap_or_default();
                    (segment_id, vector_size)
                })
            })
            // Segments with largest vector size come first
            .sorted_by_key(|(_segment_id, vector_size)| Reverse(*vector_size))
            .collect_vec();
        for (segment_id, _) in to_optimize {
            planner.plan(vec![segment_id]);
        }
    }

    fn get_telemetry_counter(&self) -> &Mutex<OperationDurationsAggregator> {
        &self.telemetry_durations_aggregator
    }
}
