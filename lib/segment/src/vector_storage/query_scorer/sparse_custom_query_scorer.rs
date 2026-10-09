use common::counter::hw::HwScale;
use common::generic_consts::Random;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::{DimId, DimWeight};

use crate::common::operation_error::OperationResult;
use crate::vector_storage::SparseVectorStorageRead;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;

pub struct SparseCustomQueryScorer<
    'a,
    TVectorStorage: SparseVectorStorageRead,
    TQuery: Query<SparseVector>,
> {
    hw: HwScale,
    vector_storage: &'a TVectorStorage,
    query: TQuery,
}

impl<
    'a,
    TVectorStorage: SparseVectorStorageRead,
    TQuery: Query<SparseVector> + TransformInto<TQuery, SparseVector, SparseVector>,
> SparseCustomQueryScorer<'a, TVectorStorage, TQuery>
{
    pub fn new(query: TQuery, vector_storage: &'a TVectorStorage) -> Self {
        let query: TQuery = TransformInto::transform(query, &|mut vector| {
            vector.sort_by_indices();
            Ok(vector)
        })
        .unwrap();

        Self {
            hw: HwScale {
                cpu: size_of::<DimWeight>(),
                vector_io_read: if vector_storage.is_cold() {
                    size_of::<DimId>()
                } else {
                    0
                },
            },
            vector_storage,
            query,
        }
    }
}

impl<TVectorStorage: SparseVectorStorageRead, TQuery: Query<SparseVector>>
    SparseCustomQueryScorer<'_, TVectorStorage, TQuery>
{
    fn score(&self, v: &SparseVector) -> ScoreType {
        self.query.score_by(|example| {
            let cpu_units = v.indices.len() + example.indices.len();
            self.hw.cpu(cpu_units);
            example.score(v).unwrap_or(0.0)
        })
    }
}

impl<TVectorStorage: SparseVectorStorageRead, TQuery: Query<SparseVector>> QueryScorer
    for SparseCustomQueryScorer<'_, TVectorStorage, TQuery>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self
            .vector_storage
            .get_sparse::<Random>(idx)
            .expect("Failed to get sparse vector");

        // not exactly correct for Gridstore where the indices are compressed into u8
        self.hw
            .vector_io_read(stored.indices.len() + stored.values.len());

        self.score(&stored)
    }

    fn score_stored_batch(
        &self,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
    ) -> OperationResult<()> {
        debug_assert_eq!(ids.len(), scores.len());

        self.vector_storage
            .for_each_in_sparse_batch(ids, |idx, vector| {
                // not exactly correct for Gridstore where the indices are compressed into u8
                self.hw
                    .vector_io_read(vector.indices.len() + vector.values.len());

                scores[idx] = self.score(&vector);
            })
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer can compare against multiple vectors, not just one")
    }

    type SupportsBytes = False;
    fn score_bytes(&self, enabled: Self::SupportsBytes, _: &[u8]) -> ScoreType {
        match enabled {}
    }
}
