use common::counter::hardware_counter::HardwareCounterCell;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use quantization::turboquant::EncodedQueryTQ;

use crate::data_types::vectors::VectorElementType;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::turbo::TurboVectorStorage;
use crate::vector_storage::vector_storage_base::VectorStorageRead;

/// Asymmetric raw scorer for [`TurboVectorStorage`].
///
/// Holds the query precomputed once (rotation + SIMD encoding) and scores it
/// against the stored TurboQuant bytes, delegating the actual arithmetic and
/// the metric sign convention to the storage.
pub struct TurboQueryScorer<'a> {
    query: EncodedQueryTQ,
    storage: &'a TurboVectorStorage,
    hardware_counter: HardwareCounterCell,
}

impl<'a> TurboQueryScorer<'a> {
    pub fn new(
        query: EncodedQueryTQ,
        storage: &'a TurboVectorStorage,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self {
        // Disk reads only cost vector IO when the encoded blob is not resident.
        hardware_counter.set_vector_io_read_multiplier(usize::from(storage.is_on_disk()));

        Self {
            query,
            storage,
            hardware_counter,
        }
    }
}

impl QueryScorer for TurboQueryScorer<'_> {
    type TVector = [VectorElementType];

    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.storage
            .score_encoded_query(&self.query, idx, &self.hardware_counter)
    }

    fn score(&self, _v2: &[VectorElementType]) -> ScoreType {
        unimplemented!("This method is not expected to be called for turbo scorer");
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.storage
            .score_internal_encoded(point_a, point_b, &self.hardware_counter)
    }

    type SupportsBytes = False;
    fn score_bytes(&self, enabled: Self::SupportsBytes, _bytes: &[u8]) -> ScoreType {
        match enabled {}
    }
}
