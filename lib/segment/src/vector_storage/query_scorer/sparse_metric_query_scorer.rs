use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use sparse::common::sparse_vector::SparseVector;

use crate::vector_storage::SparseVectorStorageRead;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::sparse::volatile_sparse_vector_storage::VolatileSparseVectorStorage;

pub struct SparseMetricQueryScorer<'a> {
    vector_storage: &'a VolatileSparseVectorStorage,
    query: SparseVector,
    hardware_counter: HardwareCounterCell,
}

impl<'a> SparseMetricQueryScorer<'a> {
    pub fn new(
        mut query: SparseVector,
        vector_storage: &'a VolatileSparseVectorStorage,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self {
        if !query.is_sorted() {
            query.sort_by_indices();
        }

        // We will count the number of intersections per pair of vectors.
        hardware_counter.set_cpu_multiplier(1);
        // We don't measure `vector_io_read` because we are dealing with a volatile storage,
        //   which is always in memory.
        //   If we refactor this into accepting on_disk storages, we would set `vector_io_read_multiplier`
        //   to 0 or 1 here, and measure it accordingly.

        Self {
            vector_storage,
            query,
            hardware_counter,
        }
    }

    fn score_sparse(&self, a: &SparseVector, b: &SparseVector) -> ScoreType {
        self.hardware_counter
            .cpu_counter()
            // Calculate the amount of comparisons needed for sparse vector scoring.
            .incr_delta(std::cmp::min(a.len(), b.len()));

        a.score(b).unwrap_or_default()
    }

    fn score_ref(&self, v2: &SparseVector) -> ScoreType {
        self.score_sparse(&self.query, v2)
    }
}

impl QueryScorer for SparseMetricQueryScorer<'_> {
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self
            .vector_storage
            .get_sparse::<Random>(idx)
            .expect("Sparse vector not found");

        self.score_ref(&stored)
    }

    #[inline]
    fn score_stored_batch(&self, ids: &[PointOffsetType], scores: &mut [ScoreType]) {
        debug_assert_eq!(ids.len(), scores.len());

        self.vector_storage
            .for_each_in_sparse_batch(ids, |idx, vector| {
                scores[idx] = self.score_ref(&vector);
            })
            .expect("sparse vectors read");
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
    use super::*;
    use crate::vector_storage::VectorStorage;
    use crate::vector_storage::query_scorer::QueryScorer;

    #[test]
    fn unsorted_sparse_query_keeps_score() {
        let mut vector_storage = VolatileSparseVectorStorage::default();
        let stored = SparseVector::new(vec![1, 3], vec![5.0, 7.0]).unwrap();
        vector_storage
            .insert_vector(0, (&stored).into(), &HardwareCounterCell::new())
            .unwrap();

        let sorted = SparseVector::new(vec![1, 3], vec![5.0, 7.0]).unwrap();
        let unsorted = SparseVector::new(vec![3, 1], vec![7.0, 5.0]).unwrap();

        let sorted_score =
            SparseMetricQueryScorer::new(sorted, &vector_storage, HardwareCounterCell::new())
                .score_stored(0);
        let unsorted_score =
            SparseMetricQueryScorer::new(unsorted, &vector_storage, HardwareCounterCell::new())
                .score_stored(0);
        let mut batch_scores = [0.0];
        SparseMetricQueryScorer::new(
            SparseVector::new(vec![3, 1], vec![7.0, 5.0]).unwrap(),
            &vector_storage,
            HardwareCounterCell::new(),
        )
        .score_stored_batch(&[0], &mut batch_scores);

        assert_eq!(sorted_score, unsorted_score);
        assert_eq!(sorted_score, batch_scores[0]);
    }
}
