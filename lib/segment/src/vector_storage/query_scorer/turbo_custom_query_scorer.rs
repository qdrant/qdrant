use common::counter::hw::HwScale;
use common::typelevel::True;
use common::types::{PointOffsetType, ScoreType};
use quantization::turboquant::EncodedQueryTQ;

use crate::data_types::vectors::DenseVector;
use crate::vector_storage::TurboScoring;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;

/// Raw scorer for multi-vector queries (reco / discover / context / feedback)
/// against a dense TurboQuant storage ([`TurboScoring`]).
///
/// Each sub-query vector is preprocessed and precomputed once at construction;
/// scoring a stored point reads its encoded bytes a single time and folds the
/// per-example similarities through the query's own combinator (`score_by`).
pub struct TurboCustomQueryScorer<'a, TStorage, TQuery>
where
    TStorage: TurboScoring,
    TQuery: Query<EncodedQueryTQ>,
{
    hw: HwScale,
    query: TQuery,
    storage: &'a TStorage,
}

impl<'a, TStorage, TQuery> TurboCustomQueryScorer<'a, TStorage, TQuery>
where
    TStorage: TurboScoring,
    TQuery: Query<EncodedQueryTQ>,
{
    pub fn new<TInputQuery>(raw_query: TInputQuery, storage: &'a TStorage) -> Self
    where
        TInputQuery: Query<DenseVector> + TransformInto<TQuery, DenseVector, EncodedQueryTQ>,
    {
        // Preprocess (per distance) and precompute each sub-query vector once;
        // `preprocess_query` folds both steps together.
        let query: TQuery = raw_query
            .transform(&|raw_vector| Ok(storage.preprocess_query(raw_vector)))
            .unwrap();

        Self {
            hw: HwScale {
                cpu: storage.quantized_vector_size(),
                vector_io_read: if storage.is_on_disk() {
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

impl<TStorage, TQuery> QueryScorer for TurboCustomQueryScorer<'_, TStorage, TQuery>
where
    TStorage: TurboScoring,
    TQuery: Query<EncodedQueryTQ>,
{
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        // Read the stored vector once (one vector of IO), then score every
        // sub-query against it — each sub-query is one vector of CPU work.
        // The per-vector byte cost is the counter multiplier set in `new`.
        let bytes = self.storage.get_quantized_vector(idx);
        self.hw.vector_io_read(1);

        self.query.score_by(|query| {
            self.hw.cpu(1);
            self.storage.score_query_bytes(query, &bytes)
        })
    }

    #[inline]
    fn score_stored_batch(&self, ids: &[PointOffsetType], scores: &mut [ScoreType]) {
        debug_assert_eq!(ids.len(), scores.len());

        // One vector of IO per point; CPU is counted per sub-query below,
        // matching `score_stored`.
        self.hw.vector_io_read(ids.len());

        self.storage
            .for_each_in_dense_tq_batch(ids, |idx, bytes| {
                scores[idx] = self.query.score_by(|query| {
                    self.hw.cpu(1);
                    self.storage.score_query_bytes(query, bytes)
                });
            })
            .expect("read TQ vectors");
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer compares against multiple vectors, not just one");
    }

    type SupportsBytes = True;
    fn score_bytes(&self, _: Self::SupportsBytes, bytes: &[u8]) -> ScoreType {
        // `bytes` are an already-fetched TQ-encoded vector: no IO, and one
        // vector of CPU work per sub-query (matching `score_stored`).
        self.query.score_by(|query| {
            self.hw.cpu(1);
            self.storage.score_query_bytes(query, bytes)
        })
    }
}
