use common::counter::hardware_counter::HardwareCounterCell;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use quantization::turboquant::EncodedQueryTQ;

use crate::data_types::vectors::{DenseVector, VectorElementType};
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
    type TVector = [VectorElementType];

    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        // Read the stored vector once, then score every sub-query against it.
        let bytes = self.storage.get_quantized_vector(idx);
        self.hardware_counter
            .vector_io_read()
            .incr_delta(bytes.len());

        let cpu_counter = self.hardware_counter.cpu_counter();
        self.query.score_by(|query| {
            cpu_counter.incr();
            self.storage.score_query_bytes(query, &bytes)
        })
    }

    fn score(&self, _v2: &[VectorElementType]) -> ScoreType {
        unimplemented!("This method is not expected to be called for turbo scorer");
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer compares against multiple vectors, not just one");
    }

    // TODO(TQDT): add inline scoring support
    type SupportsBytes = False;
    fn score_bytes(&self, enabled: Self::SupportsBytes, _bytes: &[u8]) -> ScoreType {
        match enabled {}
    }
}
