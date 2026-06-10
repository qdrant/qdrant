use common::counter::hardware_counter::HardwareCounterCell;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use quantization::turboquant::EncodedQueryTQ;

use crate::data_types::vectors::{DenseVector, VectorElementType};
use crate::vector_storage::DenseTQVectorStorage;
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
        query: DenseVector,
        storage: &'a TurboVectorStorage,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self {
        // Preprocess (per distance) and precompute the query once, so the
        // Hadamard rotation runs here rather than per scored point.
        let query = storage.preprocess_query(query);

        hardware_counter.set_cpu_multiplier(storage.quantized_vector_size());
        if storage.is_on_disk() {
            hardware_counter.set_vector_io_read_multiplier(storage.quantized_vector_size());
        } else {
            hardware_counter.set_vector_io_read_multiplier(0);
        }

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
        let bytes = self.storage.get_quantized_vector(idx);
        self.hardware_counter.vector_io_read().incr();
        self.hardware_counter.cpu_counter().incr();
        self.storage.score_query_bytes(&self.query, &bytes)
    }

    fn score(&self, _v2: &[VectorElementType]) -> ScoreType {
        unimplemented!("This method is not expected to be called for turbo scorer");
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.hardware_counter.cpu_counter().incr();
        self.storage.score_internal_encoded(point_a, point_b)
    }

    // TODO(TQDT): add inline scoring support
    type SupportsBytes = False;
    fn score_bytes(&self, enabled: Self::SupportsBytes, _bytes: &[u8]) -> ScoreType {
        match enabled {}
    }
}
