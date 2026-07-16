use common::counter::hardware_counter::HardwareCounterCell;
use common::typelevel::True;
use common::types::{PointOffsetType, ScoreType};
use quantization::turboquant::EncodedQueryTQ;

use crate::data_types::vectors::DenseVector;
use crate::vector_storage::DenseTQVectorStorage;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::turbo::TurboVectorStorage;
use crate::vector_storage::vector_storage_base::VectorStorageRead;

/// Raw scorer for multi-vector queries (reco / discover / context / feedback)
/// against a [`TurboVectorStorage`].
///
/// Each sub-query vector is preprocessed and precomputed once at construction;
/// scoring a stored point reads its encoded bytes a single time and folds the
/// per-example similarities through the query's own combinator (`score_by`).
pub struct TurboCustomQueryScorer<'a, TQuery>
where
    TQuery: Query<EncodedQueryTQ>,
{
    query: TQuery,
    storage: &'a TurboVectorStorage,
    hardware_counter: HardwareCounterCell,
}

impl<'a, TQuery> TurboCustomQueryScorer<'a, TQuery>
where
    TQuery: Query<EncodedQueryTQ>,
{
    pub fn new<TInputQuery>(
        raw_query: TInputQuery,
        storage: &'a TurboVectorStorage,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self
    where
        TInputQuery: Query<DenseVector> + TransformInto<TQuery, DenseVector, EncodedQueryTQ>,
    {
        // Preprocess (per distance) and precompute each sub-query vector once;
        // `preprocess_query` folds both steps together.
        let query: TQuery = raw_query
            .transform(|raw_vector| Ok(storage.preprocess_query(raw_vector)))
            .unwrap();

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

impl<TQuery> QueryScorer for TurboCustomQueryScorer<'_, TQuery>
where
    TQuery: Query<EncodedQueryTQ>,
{
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        // Read the stored vector once (one vector of IO), then score every
        // sub-query against it — each sub-query is one vector of CPU work.
        // The per-vector byte cost is the counter multiplier set in `new`.
        let bytes = self.storage.get_quantized_vector(idx);
        self.hardware_counter.vector_io_read().incr();

        let cpu_counter = self.hardware_counter.cpu_counter();
        self.query.score_by(|query| {
            cpu_counter.incr();
            self.storage.score_query_bytes(query, &bytes)
        })
    }

    #[inline]
    fn score_stored_batch(&self, ids: &[PointOffsetType], scores: &mut [ScoreType]) {
        debug_assert_eq!(ids.len(), scores.len());

        // One vector of IO per point; CPU is counted per sub-query below,
        // matching `score_stored`.
        self.hardware_counter.vector_io_read().incr_delta(ids.len());
        let cpu_counter = self.hardware_counter.cpu_counter();

        self.storage
            .for_each_in_dense_tq_batch(ids, |idx, bytes| {
                scores[idx] = self.query.score_by(|query| {
                    cpu_counter.incr();
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
        let cpu_counter = self.hardware_counter.cpu_counter();
        self.query.score_by(|query| {
            cpu_counter.incr();
            self.storage.score_query_bytes(query, bytes)
        })
    }
}
