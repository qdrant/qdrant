use common::counter::hardware_counter::HardwareCounterCell;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use quantization::turboquant::EncodedQueryTQ;

use crate::data_types::vectors::MultiDenseVectorInternal;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::turbo::multi::TurboMultiVectorStorage;
use crate::vector_storage::vector_storage_base::VectorStorageRead;

/// Raw scorer for multi-vector queries (reco / discover / context / feedback)
/// against a [`TurboMultiVectorStorage`].
///
/// Each sub-query multivector is preprocessed and precomputed once at
/// construction; scoring a stored point reads its encoded records once and
/// folds the per-example MaxSim similarities through the query's combinator.
pub struct TurboMultiCustomQueryScorer<'a, TQuery>
where
    TQuery: Query<Vec<EncodedQueryTQ>>,
{
    query: TQuery,
    storage: &'a TurboMultiVectorStorage,
    hardware_counter: HardwareCounterCell,
}

impl<'a, TQuery> TurboMultiCustomQueryScorer<'a, TQuery>
where
    TQuery: Query<Vec<EncodedQueryTQ>>,
{
    pub fn new<TInputQuery>(
        raw_query: TInputQuery,
        storage: &'a TurboMultiVectorStorage,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self
    where
        TInputQuery: Query<MultiDenseVectorInternal>
            + TransformInto<TQuery, MultiDenseVectorInternal, Vec<EncodedQueryTQ>>,
    {
        // Preprocess and precompute every inner vector of each sub-query once.
        let query: TQuery = raw_query
            .transform(|multi| Ok(storage.preprocess_query(&multi)))
            .unwrap();

        hardware_counter.set_vector_io_read_multiplier(usize::from(storage.is_on_disk()));

        Self {
            query,
            storage,
            hardware_counter,
        }
    }
}

impl<TQuery> QueryScorer for TurboMultiCustomQueryScorer<'_, TQuery>
where
    TQuery: Query<Vec<EncodedQueryTQ>>,
{
    type TVector = ();

    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        // Account the point read once, then fold per-example MaxSim over it.
        self.storage.account_point_read(idx, &self.hardware_counter);
        self.query.score_by(|query| {
            self.storage
                .score_point_max_similarity(query, idx, &self.hardware_counter)
        })
    }

    fn score(&self, _v2: &()) -> ScoreType {
        unimplemented!("This method is not expected to be called for turbo scorer");
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer compares against multiple vectors, not just one");
    }

    type SupportsBytes = False;
    fn score_bytes(&self, enabled: Self::SupportsBytes, _bytes: &[u8]) -> ScoreType {
        match enabled {}
    }
}
