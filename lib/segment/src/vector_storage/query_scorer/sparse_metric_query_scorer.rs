use common::counter::hardware_counter::HardwareCounterCell;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use sparse::common::sparse_vector::SparseVector;

use crate::vector_storage::SparseVectorStorage;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::sparse::volatile_sparse_vector_storage::VolatileSparseVectorStorage;

pub struct SparseMetricQueryScorer<'a> {
    vector_storage: &'a VolatileSparseVectorStorage,
    query: SparseVector,
    hardware_counter: HardwareCounterCell,
}

impl<'a> SparseMetricQueryScorer<'a> {
    pub fn new(
        query: SparseVector,
        vector_storage: &'a VolatileSparseVectorStorage,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self {
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
    type TVector = SparseVector;

    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self
            .vector_storage
            .get_sparse(idx)
            .expect("Sparse vector not found");

        self.score_ref(&stored)
    }

    #[inline]
    fn score(&self, v2: &SparseVector) -> ScoreType {
        self.score_ref(v2)
    }

    fn score_stored_batch(&self, ids: &[PointOffsetType], scores: &mut [ScoreType]) {
        debug_assert_eq!(ids.len(), scores.len());

        for idx in 0..ids.len() {
            scores[idx] = self.score_ref(
                &self
                    .vector_storage
                    .get_sparse(ids[idx])
                    .expect("Sparse vector not found"),
            );
        }
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let v1 = self
            .vector_storage
            .get_sparse(point_a)
            .expect("Sparse vector not found");
        let v2 = self
            .vector_storage
            .get_sparse(point_b)
            .expect("Sparse vector not found");

        self.score_sparse(&v1, &v2)
    }

    type SupportsBytes = False;
    fn score_bytes(&self, enabled: Self::SupportsBytes, _: &[u8]) -> ScoreType {
        match enabled {}
    }
}
