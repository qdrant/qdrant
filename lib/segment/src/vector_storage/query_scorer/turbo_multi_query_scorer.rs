use common::ambient::hw::HwScale;
use common::generic_consts::Random;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use quantization::turboquant::EncodedQueryTQ;

use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::MultiDenseVectorInternal;
use crate::vector_storage::TurboMultiScoring;
use crate::vector_storage::query_scorer::QueryScorer;

/// Asymmetric MaxSim raw scorer for a multivector TurboQuant storage
/// ([`TurboMultiScoring`]).
///
/// Holds every inner query vector precomputed once (rotation + SIMD encoding)
/// and scores the multi-query against a stored point's TurboQuant records via
/// MaxSim, delegating the arithmetic and the sign convention to the storage.
pub struct TurboMultiQueryScorer<'a, TStorage: TurboMultiScoring> {
    hw: HwScale,
    query: Vec<EncodedQueryTQ>,
    storage: &'a TStorage,
}

impl<'a, TStorage: TurboMultiScoring> TurboMultiQueryScorer<'a, TStorage> {
    pub fn new(raw_query: &MultiDenseVectorInternal, storage: &'a TStorage) -> Self {
        // Preprocess (per distance) and precompute each inner query vector once.
        let query = storage.preprocess_query(raw_query);

        Self {
            hw: HwScale {
                cpu: 1,
                vector_io_read: usize::from(storage.is_cold()),
            },
            query,
            storage,
        }
    }
}

impl<TStorage: TurboMultiScoring> QueryScorer for TurboMultiQueryScorer<'_, TStorage> {
    fn score_stored(&self, idx: PointOffsetType) -> OperationResult<ScoreType> {
        Ok(self.storage.score_point_max_similarity(&self.query, idx))
    }

    fn score_stored_batch(
        &self,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
    ) -> OperationResult<()> {
        let keys = ids.iter().copied().enumerate();

        self.storage
            .for_each_record_range::<Random, _>(keys, |idx, _id, records| {
                self.hw.vector_io_read(records.len());

                self.hw.cpu(records.len() * self.query.len());

                scores[idx] = self
                    .storage
                    .score_records_max_similarity(&self.query, records);
            })
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.storage.score_internal_max_similarity(point_a, point_b)
    }

    type SupportsBytes = False;
    fn score_bytes(&self, enabled: Self::SupportsBytes, _bytes: &[u8]) -> ScoreType {
        match enabled {}
    }
}
