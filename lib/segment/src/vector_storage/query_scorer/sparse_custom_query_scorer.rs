use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::{DimId, DimWeight};

use crate::vector_storage::SparseVectorStorage;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;

pub struct SparseCustomQueryScorer<
    'a,
    TVectorStorage: SparseVectorStorage,
    TQuery: Query<SparseVector>,
> {
    vector_storage: &'a TVectorStorage,
    query: TQuery,
    hardware_counter: HardwareCounterCell,
}

impl<
    'a,
    TVectorStorage: SparseVectorStorage,
    TQuery: Query<SparseVector> + TransformInto<TQuery, SparseVector, SparseVector>,
> SparseCustomQueryScorer<'a, TVectorStorage, TQuery>
{
    pub fn new(
        query: TQuery,
        vector_storage: &'a TVectorStorage,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self {
        let query: TQuery = TransformInto::transform(query, |mut vector| {
            vector.sort_by_indices();
            Ok(vector)
        })
        .unwrap();

        hardware_counter.set_cpu_multiplier(size_of::<DimWeight>());

        if vector_storage.is_on_disk() {
            hardware_counter.set_vector_io_read_multiplier(size_of::<DimId>());
        } else {
            hardware_counter.set_vector_io_read_multiplier(0);
        }

        Self {
            vector_storage,
            query,
            hardware_counter,
        }
    }
}

impl<TVectorStorage: SparseVectorStorage, TQuery: Query<SparseVector>> QueryScorer<SparseVector>
    for SparseCustomQueryScorer<'_, TVectorStorage, TQuery>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self
            .vector_storage
            .get_sparse(idx)
            .expect("Failed to get sparse vector");

        self.hardware_counter
            .vector_io_read()
            .incr_delta(stored.indices.len() + stored.values.len());

        self.query.score_by(|example| {
            let cpu_units = example.indices.len() + stored.indices.len();
            self.hardware_counter.cpu_counter().incr_delta(cpu_units);
            stored.score(example).unwrap_or(0.0)
        })
    }

    fn score(&self, v: &SparseVector) -> ScoreType {
        self.query.score_by(|example| {
            let cpu_units = v.indices.len() + example.indices.len();
            self.hardware_counter.cpu_counter().incr_delta(cpu_units);
            example.score(v).unwrap_or(0.0)
        })
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer can compare against multiple vectors, not just one")
    }
}
