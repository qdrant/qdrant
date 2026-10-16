use segment::common::operation_error::OperationResult;
use segment::data_types::fully_qualified_point::FullyQualifiedPoint;
use segment::data_types::update_metering;
use segment::json_path::JsonPath;
use segment::types::SegmentConfig;
pub use update_metering::UpdateBytes;

use crate::EdgeConfig;

/// Logical update sizes under the collection's intended indexing configuration.
///
/// Dense vectors with no HNSW links contribute no indexing bytes. Sparse vectors
/// are always indexed. Payload sizes use UTF-8 JSON; indexed fields count their
/// values, including array contents. Storage multipliers are not applied.
pub struct UpdateMetering(update_metering::UpdateMetering);

impl UpdateMetering {
    pub fn new(config: &EdgeConfig, payload_fields: impl IntoIterator<Item = JsonPath>) -> Self {
        let mut indexed = config.plain_segment_config();
        indexed.vector_data = config
            .segment_optimizer_config()
            .dense_vectors
            .into_iter()
            .map(|(name, config)| (name, config.indexed()))
            .collect();
        Self::from_segment_config(&indexed, payload_fields)
    }

    /// Use a segment's existing schema when the intended collection config is unknown.
    /// Plain dense vectors have no indexing charge under this fallback.
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
