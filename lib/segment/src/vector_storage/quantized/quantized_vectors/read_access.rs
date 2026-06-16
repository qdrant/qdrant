use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::QuantizedVectorsConfig;
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::QueryVector;
use crate::vector_storage::RawScorer;
use crate::vector_storage::quantized::quantized_query_scorer::InternalScorerUnsupported;

/// Shared read/scoring interface over quantized vectors.
///
/// Implemented by both the read-write [`QuantizedVectors`] and the read-only
/// [`QuantizedVectorsRead`], so search code can be generic over the backend.
///
/// [`QuantizedVectors`]: super::QuantizedVectors
/// [`QuantizedVectorsRead`]: super::ReadOnlyQuantizedVectors
pub trait QuantizedVectorsRead {
    fn config(&self) -> &QuantizedVectorsConfig;

    fn default_rescoring(&self) -> bool;

    fn raw_scorer<'a>(
        &'a self,
        query: QueryVector,
        hardware_counter: HardwareCounterCell,
    ) -> OperationResult<Box<dyn RawScorer + 'a>>;

    /// Build a raw scorer for the specified `point_id`.
    /// If not supported, return [`InternalScorerUnsupported`] with the original `hardware_counter`.
    fn raw_internal_scorer<'a>(
        &'a self,
        point_id: PointOffsetType,
        hardware_counter: HardwareCounterCell,
    ) -> Result<Box<dyn RawScorer + 'a>, InternalScorerUnsupported>;
}
