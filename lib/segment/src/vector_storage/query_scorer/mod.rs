use std::marker::PhantomData;

use bytemuck::TransparentWrapper;
use common::typelevel::{TBool, TOption};
use common::types::{PointOffsetType, ScoreType};

use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{TypedMultiDenseVector, TypedMultiDenseVectorRef};
use crate::spaces::metric::Metric;
use crate::types::{MultiVectorComparator, MultiVectorConfig};
use crate::vector_storage::VectorOffset;

pub mod custom_query_scorer;
pub mod metric_query_scorer;
pub mod multi_custom_query_scorer;
pub mod multi_metric_query_scorer;
pub mod packed_max_sim;
pub mod sparse_custom_query_scorer;
pub mod sparse_metric_query_scorer;
pub mod turbo_custom_query_scorer;
pub mod turbo_multi_custom_query_scorer;
pub mod turbo_multi_query_scorer;
pub mod turbo_query_scorer;

pub trait QueryScorer {
    /// Score a batch of points
    ///
    /// Enables underlying storage to optimize pre-fetching of data.
    /// Fallible because the vectors may come from remote storage. A single
    /// point is scored as a batch of one, see
    /// [`RawScorer::score_point`](crate::vector_storage::RawScorer::score_point).
    fn score_stored_batch(
        &self,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
    ) -> OperationResult<()>;

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType;

    type SupportsBytes: TBool;
    fn score_bytes(&self, _: Self::SupportsBytes, bytes: &[u8]) -> ScoreType;
}

pub trait QueryScorerBytes {
    fn score_bytes(&self, bytes: &[u8]) -> ScoreType;
}

#[derive(TransparentWrapper)]
#[repr(transparent)]
pub struct QueryScorerBytesImpl<TQueryScorer: QueryScorer>(
    <TQueryScorer::SupportsBytes as TBool>::TOption<TQueryScorer>,
);

impl<TQueryScorer: QueryScorer> QueryScorerBytesImpl<TQueryScorer> {
    pub fn new(query_scorer: &TQueryScorer) -> Option<&Self> {
        TQueryScorer::SupportsBytes::then_some_ref(query_scorer).map(Self::wrap_ref)
    }
}

impl<TQueryScorer: QueryScorer> QueryScorerBytes for QueryScorerBytesImpl<TQueryScorer> {
    fn score_bytes(&self, bytes: &[u8]) -> ScoreType {
        self.0.get().score_bytes(self.0.is_some(), bytes)
    }
}

/// Colbert MaxSim metric, metric for multi-dense vectors
/// <https://arxiv.org/pdf/2112.01488.pdf>, figure 1
/// This metric is also implemented in `QuantizedMultivectorStorage` structure for quantized data.
///
/// Disclaimer: this score is not equivalent to the original Colbert metric for Euclidean space because we do not apply the post-processing step.
/// In that case the score value will be different but the ranking will be the same.
/// We do that because of performance reasons and complex sort ordering of the vectors when applying the post-processing step.
///
/// `multi_dense_a` is packed per call when the batched kernel is faster (see
/// [`packed_max_sim`]); a query scored against many points is prepared once
/// instead, as a [`MultiDenseQuery`].
pub fn score_max_similarity<T: PrimitiveVectorElement, TMetric: Metric<T>>(
    multi_dense_a: TypedMultiDenseVectorRef<'_, T>,
    multi_dense_b: TypedMultiDenseVectorRef<'_, T>,
) -> ScoreType {
    let distance = TMetric::distance();
    if packed_max_sim::pays::<T>(
        distance,
        multi_dense_a.dim,
        multi_dense_a.vectors_count(),
        true,
    ) && let Some(score) =
        packed_max_sim::score_max_similarity_packed(multi_dense_a, multi_dense_b, distance)
    {
        return score;
    }
    score_max_similarity_pairwise::<T, TMetric>(multi_dense_a, multi_dense_b)
}

/// [`score_max_similarity`] one token pair at a time.
pub fn score_max_similarity_pairwise<T: PrimitiveVectorElement, TMetric: Metric<T>>(
    multi_dense_a: TypedMultiDenseVectorRef<'_, T>,
    multi_dense_b: TypedMultiDenseVectorRef<'_, T>,
) -> ScoreType {
    debug_assert!(!multi_dense_a.is_empty());
    debug_assert!(!multi_dense_b.is_empty());
    let mut sum = 0.0;
    for dense_a in multi_dense_a.multi_vectors() {
        let mut max_sim = ScoreType::NEG_INFINITY;
        // manual `max_by` for performance
        for dense_b in multi_dense_b.multi_vectors() {
            let sim = TMetric::similarity(dense_a, dense_b);
            if sim > max_sim {
                max_sim = sim;
            }
        }
        // sum of max similarity
        sum += max_sim;
    }
    sum
}

/// A multi-dense query prepared once for MaxSim against stored multi-vectors:
/// packed when the batched kernel is faster (see [`packed_max_sim`]), else
/// scored pair by pair as [`score_max_similarity_pairwise`] does.
pub struct MultiDenseQuery<T: PrimitiveVectorElement, TMetric> {
    kernel: MultiDenseKernel<T>,
    metric: PhantomData<TMetric>,
}

enum MultiDenseKernel<T: PrimitiveVectorElement> {
    PerPair(TypedMultiDenseVector<T>),
    Packed(packed_max_sim::PackedMultiQuery),
}

impl<T: PrimitiveVectorElement, TMetric: Metric<T>> MultiDenseQuery<T, TMetric> {
    pub fn new(query: TypedMultiDenseVector<T>) -> Self {
        let packed = packed_max_sim::PackedMultiQuery::new_if_faster(
            TypedMultiDenseVectorRef::from(&query),
            TMetric::distance(),
        );
        let kernel = match packed {
            Some(packed) => MultiDenseKernel::Packed(packed),
            None => MultiDenseKernel::PerPair(query),
        };
        Self {
            kernel,
            metric: PhantomData,
        }
    }

    /// Number of inner vectors.
    pub fn vectors_count(&self) -> usize {
        match &self.kernel {
            MultiDenseKernel::PerPair(query) => query.vectors_count(),
            MultiDenseKernel::Packed(query) => query.vectors_count(),
        }
    }

    /// MaxSim of this query against one stored multi-vector.
    pub fn score_max_similarity(&self, stored: TypedMultiDenseVectorRef<'_, T>) -> ScoreType {
        match &self.kernel {
            MultiDenseKernel::PerPair(query) => score_max_similarity_pairwise::<T, TMetric>(
                TypedMultiDenseVectorRef::from(query),
                stored,
            ),
            MultiDenseKernel::Packed(query) => query.score_max_similarity(stored),
        }
    }
}

fn score_multi<T: PrimitiveVectorElement, TMetric: Metric<T>>(
    multi_vector_config: &MultiVectorConfig,
    multi_dense_a: TypedMultiDenseVectorRef<'_, T>,
    multi_dense_b: TypedMultiDenseVectorRef<'_, T>,
) -> ScoreType {
    match multi_vector_config.comparator {
        MultiVectorComparator::MaxSim => {
            score_max_similarity::<T, TMetric>(multi_dense_a, multi_dense_b)
        }
    }
}

/// Check if ids are rather contiguous to enable further optimizations
/// TODO: this can be smarter, but requires experiments with actual mmap behaviour
/// TODO: For example
///
/// - If the whole batch is less than one page - don't use prefetch
/// - If one vector is bigger than the prefetch size - don't use prefetch
/// - ???
pub fn is_read_with_prefetch_efficient<O: VectorOffset>(ids: &[O]) -> bool {
    let mut min = usize::MAX;
    let mut max = 0;
    let mut n = 0;

    for id in ids {
        let id = id.offset();
        if id < min {
            min = id;
        }
        if id > max {
            max = id;
        }
        n += 1;
    }

    if n < 2 {
        return false;
    }

    let diff = max.saturating_sub(min);

    diff < n * 2
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::vectors::MultiDenseVectorInternal;
    use crate::spaces::simple::{DotProductMetric, EuclidMetric};

    /// A prepared query is packed past the crossover only, and scores as pair by pair.
    #[test]
    fn multi_dense_query_packs_past_the_crossover() {
        let dim = 128;
        let make = |count: usize, seed: f32| {
            let values = (0..count * dim)
                .map(|i| ((i as f32 + seed) * 0.37).sin())
                .collect();
            MultiDenseVectorInternal::new(values, dim)
        };
        #[cfg(target_arch = "x86_64")]
        let simd = std::is_x86_feature_detected!("avx2");
        #[cfg(not(target_arch = "x86_64"))]
        let simd = false;
        let stored = make(30, 0.5);
        for (count, packed) in [(1, false), (40, simd)] {
            let query = MultiDenseQuery::<f32, DotProductMetric>::new(make(count, 0.0));
            assert_eq!(
                matches!(query.kernel, MultiDenseKernel::Packed(_)),
                packed,
                "{count} vectors"
            );
            let want = score_max_similarity_pairwise::<f32, DotProductMetric>(
                (&make(count, 0.0)).into(),
                (&stored).into(),
            );
            let got = query.score_max_similarity((&stored).into());
            assert!(
                (got - want).abs() <= 1e-3,
                "{count} vectors: {got} vs {want}"
            );
        }
    }

    #[test]
    fn test_check_ids_rather_contiguous() {
        assert!(!is_read_with_prefetch_efficient::<u32>(&[]));
        assert!(!is_read_with_prefetch_efficient::<u32>(&[1]));
        assert!(is_read_with_prefetch_efficient::<u32>(&[1, 2]));
        assert!(is_read_with_prefetch_efficient::<u32>(&[2, 1]));
        assert!(is_read_with_prefetch_efficient::<u32>(&[1, 2, 3, 9, 10]));
        assert!(is_read_with_prefetch_efficient::<u32>(&[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        ]));
        assert!(is_read_with_prefetch_efficient::<u32>(&[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 11
        ]));
        assert!(!is_read_with_prefetch_efficient::<u32>(&[
            1, 2, 3, 4, 9, 1000, 12, 14
        ]));
    }

    #[test]
    fn test_score_multi_euclidean() {
        let a = MultiDenseVectorInternal::try_from(vec![
            vec![1.0, 2.0, 3.0],
            vec![3.0, 3.0, 3.0],
            vec![4.0, 5.0, 6.0],
        ])
        .unwrap();
        // distance to itself
        let score = score_max_similarity::<f32, EuclidMetric>((&a).into(), (&a).into());
        assert_eq!(score, -0.0);

        let b = MultiDenseVectorInternal::try_from(vec![vec![3.0, 3.0, 3.0], vec![4.0, 2.0, 1.0]])
            .unwrap();
        let score = score_max_similarity::<f32, EuclidMetric>((&a).into(), (&b).into());
        // proper value according to theory should be `5.9777255` but we do not apply post-processing step
        assert_eq!(score, -19.);
    }
}
