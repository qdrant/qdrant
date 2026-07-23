use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use quantization::turboquant::EncodedQueryTQ;

use crate::data_types::vectors::MultiDenseVectorInternal;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::turbo::multi::TurboMultiScoring;

/// Asymmetric MaxSim raw scorer for a multivector TurboQuant storage
/// ([`TurboMultiScoring`]).
///
/// Holds every inner query vector precomputed once (rotation + SIMD encoding)
/// and scores the multi-query against a stored point's TurboQuant records via
/// MaxSim, delegating the arithmetic and the sign convention to the storage.
pub struct TurboMultiQueryScorer<'a, TStorage: TurboMultiScoring> {
    query: Vec<EncodedQueryTQ>,
    storage: &'a TStorage,
    hardware_counter: HardwareCounterCell,
}

impl<'a, TStorage: TurboMultiScoring> TurboMultiQueryScorer<'a, TStorage> {
    pub fn new(
        raw_query: &MultiDenseVectorInternal,
        storage: &'a TStorage,
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

impl<TStorage: TurboMultiScoring> QueryScorer for TurboMultiQueryScorer<'_, TStorage> {
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.storage
            .score_point_max_similarity(&self.query, idx, &self.hardware_counter)
    }

    fn score_stored_batch(&self, ids: &[PointOffsetType], scores: &mut [ScoreType]) {
        let keys = ids.iter().copied().enumerate();

        let hw_counter = &self.hardware_counter;

        self.storage
            .for_each_record_range::<Random, _>(keys, |idx, _id, records| {
                hw_counter.vector_io_read().incr_delta(records.len());

                hw_counter
                    .cpu_counter()
                    .incr_delta(records.len() * self.query.len());

                scores[idx] = self
                    .storage
                    .score_records_max_similarity(&self.query, records);
            })
            .expect("Failed to score stored batch");
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
