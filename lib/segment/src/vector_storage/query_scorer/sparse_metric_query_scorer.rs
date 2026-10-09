use common::ambient::hw::HwScale;
use common::generic_consts::Random;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use sparse::common::sparse_vector::SparseVector;

use crate::common::operation_error::OperationResult;
use crate::vector_storage::SparseVectorStorageRead;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::sparse::volatile_sparse_vector_storage::VolatileSparseVectorStorage;

pub struct SparseMetricQueryScorer<'a> {
    hw: HwScale,
    vector_storage: &'a VolatileSparseVectorStorage,
    query: SparseVector,
}

impl<'a> SparseMetricQueryScorer<'a> {
    pub fn new(query: SparseVector, vector_storage: &'a VolatileSparseVectorStorage) -> Self {
        // We will count the number of intersections per pair of vectors.
        // We don't measure `vector_io_read` because we are dealing with a volatile storage,
        //   which is always in memory.
        //   If we refactor this into accepting on_disk storages, we would set `vector_io_read_multiplier`
        //   to 0 or 1 here, and measure it accordingly.

        Self {
            hw: HwScale {
                cpu: 1,
                vector_io_read: 1,
            },
            vector_storage,
            query,
        }
    }

    fn score_sparse(&self, a: &SparseVector, b: &SparseVector) -> ScoreType {
        // Calculate the amount of comparisons needed for sparse vector scoring.
        self.hw.cpu(std::cmp::min(a.len(), b.len()));

        a.score(b).unwrap_or_default()
    }

    fn score_ref(&self, v2: &SparseVector) -> ScoreType {
        self.score_sparse(&self.query, v2)
    }
}

impl QueryScorer for SparseMetricQueryScorer<'_> {
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> OperationResult<ScoreType> {
        let stored = self.vector_storage.get_sparse::<Random>(idx)?;

        Ok(self.score_ref(&stored))
    }

    #[inline]
    fn score_stored_batch(
        &self,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
    ) -> OperationResult<()> {
        debug_assert_eq!(ids.len(), scores.len());

        self.vector_storage
            .for_each_in_sparse_batch(ids, |idx, vector| {
                scores[idx] = self.score_ref(&vector);
            })
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let v1 = self
            .vector_storage
            .get_sparse::<Random>(point_a)
            .expect("Sparse vector not found");
        let v2 = self
            .vector_storage
            .get_sparse::<Random>(point_b)
            .expect("Sparse vector not found");

        self.score_sparse(&v1, &v2)
    }

    type SupportsBytes = False;
    fn score_bytes(&self, enabled: Self::SupportsBytes, _: &[u8]) -> ScoreType {
        match enabled {}
    }
}

#[cfg(test)]
mod tests {
    use common::ambient;
    use sparse::common::sparse_vector::SparseVector;

    use crate::data_types::vectors::{QueryVector, VectorInternal, VectorRef};
    use crate::vector_storage::sparse::volatile_sparse_vector_storage::new_volatile_sparse_vector_storage;
    use crate::vector_storage::{VectorStorage, new_raw_scorer};

    #[test]
    fn score_point_reports_a_missing_vector() {
        let _scope = ambient::test_guard();
        let mut storage = new_volatile_sparse_vector_storage();
        let vector = SparseVector::new(vec![1, 2], vec![0.5, 0.5]).unwrap();
        storage.insert_vector(0, VectorRef::from(&vector)).unwrap();

        let query = QueryVector::Nearest(VectorInternal::from(vector));
        let scorer = new_raw_scorer(query, &storage).unwrap();
        assert!(scorer.score_point(0).is_ok());
        assert!(scorer.score_point(3).is_err());
    }
}
