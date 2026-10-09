use common::ambient::hw::HwScale;
use common::typelevel::True;
use common::types::{PointOffsetType, ScoreType};
use quantization::turboquant::EncodedQueryTQ;

use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::DenseVector;
use crate::vector_storage::TurboScoring;
use crate::vector_storage::query_scorer::QueryScorer;

/// Asymmetric raw scorer for a dense TurboQuant storage ([`TurboScoring`]).
///
/// Holds the query precomputed once (rotation + SIMD encoding) and scores it
/// against the stored TurboQuant bytes, delegating the actual arithmetic and
/// the metric sign convention to the storage.
pub struct TurboQueryScorer<'a, TStorage: TurboScoring> {
    hw: HwScale,
    query: EncodedQueryTQ,
    storage: &'a TStorage,
}

impl<'a, TStorage: TurboScoring> TurboQueryScorer<'a, TStorage> {
    pub fn new(query: DenseVector, storage: &'a TStorage) -> Self {
        // Preprocess (per distance) and precompute the query once, so the
        // Hadamard rotation runs here rather than per scored point.
        let query = storage.preprocess_query(query);

        Self {
            hw: HwScale {
                cpu: storage.quantized_vector_size(),
                vector_io_read: if storage.is_cold() {
                    storage.quantized_vector_size()
                } else {
                    0
                },
            },
            query,
            storage,
        }
    }
}

impl<TStorage: TurboScoring> QueryScorer for TurboQueryScorer<'_, TStorage> {
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let bytes = self.storage.get_quantized_vector(idx);
        self.hw.vector_io_read(1);
        self.hw.cpu(1);
        self.storage.score_query_bytes(&self.query, &bytes)
    }

    #[inline]
    fn score_stored_batch(
        &self,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
    ) -> OperationResult<()> {
        debug_assert_eq!(ids.len(), scores.len());

        self.hw.vector_io_read(ids.len());
        self.hw.cpu(ids.len());

        self.storage.score_query_batch(&self.query, ids, scores);
        Ok(())
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.hw.cpu(1);
        self.storage.score_internal_encoded(point_a, point_b)
    }

    type SupportsBytes = True;
    fn score_bytes(&self, _: Self::SupportsBytes, bytes: &[u8]) -> ScoreType {
        // `bytes` are an already-fetched TQ-encoded vector: one vector of CPU
        // work, no IO (the caller owns the read).
        self.hw.cpu(1);
        self.storage.score_query_bytes(&self.query, bytes)
    }
}
