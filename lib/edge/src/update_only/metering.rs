use segment::common::operation_error::OperationResult;
use segment::data_types::fully_qualified_point::FullyQualifiedPoint;
use segment::data_types::update_metering;
use segment::json_path::JsonPath;
use segment::types::SegmentConfig;
use shard::optimizers::config::SegmentOptimizerConfig;
pub use update_metering::UpdateBytes;

use crate::EdgeConfig;

/// Uses the configured indexes regardless of current segment optimization.
pub struct UpdateMetering(update_metering::UpdateMetering);

impl UpdateMetering {
    pub fn new(config: &EdgeConfig, payload_fields: impl IntoIterator<Item = JsonPath>) -> Self {
        let optimizer_config = config.segment_optimizer_config();
        let SegmentConfig {
            vector_data: _,
            sparse_vector_data,
            payload_storage_type,
            id_tracker_memory,
        } = optimizer_config.plain_segment_config();
        let SegmentOptimizerConfig {
            payload_storage_type: _,
            id_tracker_memory: _,
            dense_vectors,
            sparse_vectors: _,
            live_vector_names: _,
            payload_cache_after_build: _,
        } = optimizer_config;
        let indexed = SegmentConfig {
            vector_data: dense_vectors
                .into_iter()
                .map(|(name, config)| (name, config.indexed()))
                .collect(),
            sparse_vector_data,
            payload_storage_type,
            id_tracker_memory,
        };
        Self::from_segment_config(&indexed, payload_fields)
    }

    pub fn from_segment_config(
        config: &SegmentConfig,
        payload_fields: impl IntoIterator<Item = JsonPath>,
    ) -> Self {
        Self(update_metering::UpdateMetering::new(config, payload_fields))
    }

    pub(super) fn measure(&self, point: &FullyQualifiedPoint) -> OperationResult<UpdateBytes> {
        self.0.measure(point)
    }
}
