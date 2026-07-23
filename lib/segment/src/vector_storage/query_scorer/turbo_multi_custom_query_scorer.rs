use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use quantization::turboquant::EncodedQueryTQ;

use crate::data_types::vectors::MultiDenseVectorInternal;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::turbo::multi::TurboMultiScoring;

/// Raw scorer for multi-vector queries (reco / discover / context / feedback)
/// against a multivector TurboQuant storage ([`TurboMultiScoring`]).
///
/// Each sub-query multivector is preprocessed and precomputed once at
/// construction; scoring a stored point reads its encoded records once and
/// folds the per-example MaxSim similarities through the query's combinator.
pub struct TurboMultiCustomQueryScorer<'a, TStorage, TQuery>
where
    TStorage: TurboMultiScoring,
    TQuery: Query<Vec<EncodedQueryTQ>>,
{
    query: TQuery,
    storage: &'a TStorage,
    hardware_counter: HardwareCounterCell,
}

impl<'a, TStorage, TQuery> TurboMultiCustomQueryScorer<'a, TStorage, TQuery>
where
    TStorage: TurboMultiScoring,
    TQuery: Query<Vec<EncodedQueryTQ>>,
{
    pub fn new<TInputQuery>(
        raw_query: TInputQuery,
        storage: &'a TStorage,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self
    where
        TInputQuery: Query<MultiDenseVectorInternal>
            + TransformInto<TQuery, MultiDenseVectorInternal, Vec<EncodedQueryTQ>>,
    {
        // Preprocess and precompute every inner vector of each sub-query once.
        let query: TQuery = raw_query
            .transform(&|multi| Ok(storage.preprocess_query(&multi)))
            .unwrap();

        hardware_counter.set_vector_io_read_multiplier(usize::from(storage.is_on_disk()));

        Self {
            query,
            storage,
            hardware_counter,
        }
    }
}

impl<TStorage, TQuery> QueryScorer for TurboMultiCustomQueryScorer<'_, TStorage, TQuery>
where
    TStorage: TurboMultiScoring,
    TQuery: Query<Vec<EncodedQueryTQ>>,
{
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.query.score_by(|query| {
            self.storage
                .score_point_max_similarity(query, idx, &self.hardware_counter)
        })
    }

    fn score_stored_batch(&self, ids: &[PointOffsetType], scores: &mut [ScoreType]) {
        let keys = ids.iter().copied().enumerate();

        let hw_counter = &self.hardware_counter;

        self.storage
            .for_each_record_range::<Random, _>(keys, |idx, _id, records| {
                hw_counter.vector_io_read().incr_delta(records.len());

                scores[idx] = self.query.score_by(|query| {
                    hw_counter
                        .cpu_counter()
                        .incr_delta(records.len() * query.len());

                    self.storage.score_records_max_similarity(query, records)
                });
            })
            .expect("Failed to score stored batch");
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer compares against multiple vectors, not just one");
    }

    type SupportsBytes = False;
    fn score_bytes(&self, enabled: Self::SupportsBytes, _bytes: &[u8]) -> ScoreType {
        match enabled {}
    }
}
