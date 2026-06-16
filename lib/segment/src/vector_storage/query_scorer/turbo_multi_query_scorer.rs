use common::counter::hardware_counter::HardwareCounterCell;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use quantization::turboquant::EncodedQueryTQ;

use crate::data_types::vectors::MultiDenseVectorInternal;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::turbo::multi::TurboMultiVectorStorage;
use crate::vector_storage::vector_storage_base::VectorStorageRead;

/// Asymmetric MaxSim raw scorer for [`TurboMultiVectorStorage`].
///
/// Holds every inner query vector precomputed once (rotation + SIMD encoding)
/// and scores the multi-query against a stored point's TurboQuant records via
/// MaxSim, delegating the arithmetic and the sign convention to the storage.
pub struct TurboMultiQueryScorer<'a> {
    query: Vec<EncodedQueryTQ>,
    storage: &'a TurboMultiVectorStorage,
    hardware_counter: HardwareCounterCell,
}

impl<'a> TurboMultiQueryScorer<'a> {
    pub fn new(
        raw_query: &MultiDenseVectorInternal,
        storage: &'a TurboMultiVectorStorage,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self {
        // Preprocess (per distance) and precompute each inner query vector once.
        let query = storage.preprocess_query(raw_query);

        hardware_counter.set_vector_io_read_multiplier(usize::from(storage.is_on_disk()));

        Self {
            query,
            storage,
            hardware_counter,
        }
    }
}

impl QueryScorer for TurboMultiQueryScorer<'_> {
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.storage
            .score_point_max_similarity(&self.query, idx, &self.hardware_counter)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.storage
            .score_internal_max_similarity(point_a, point_b, &self.hardware_counter)
    }

    type SupportsBytes = False;
    fn score_bytes(&self, enabled: Self::SupportsBytes, _bytes: &[u8]) -> ScoreType {
        match enabled {}
    }
}
