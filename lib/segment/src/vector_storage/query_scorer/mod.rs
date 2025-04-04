use common::types::{PointOffsetType, ScoreType};

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::TypedMultiDenseVectorRef;
use crate::spaces::metric::Metric;
use crate::types::{MultiVectorComparator, MultiVectorConfig};
use crate::vector_storage::chunked_vector_storage::VectorOffsetType;
use crate::vector_storage::common::VECTOR_READ_BATCH_SIZE;

pub mod custom_query_scorer;
pub mod metric_query_scorer;
pub mod multi_custom_query_scorer;
pub mod multi_metric_query_scorer;
pub mod sparse_custom_query_scorer;

pub trait QueryScorer<TVector: ?Sized> {
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType;

    /// Score a batch of points
    ///
    /// Enable underlying storage to optimize pre-fetching of data
    fn score_stored_batch(&self, ids: &[PointOffsetType], scores: &mut [ScoreType]) {
        debug_assert!(ids.len() <= VECTOR_READ_BATCH_SIZE);
        debug_assert_eq!(ids.len(), scores.len());

        // no specific implementation for batch scoring
        for (idx, id) in ids.iter().enumerate() {
            scores[idx] = self.score_stored(*id);
        }
    }

    fn score(&self, v2: &TVector) -> ScoreType;

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType;
}

/// Colbert MaxSim metric, metric for multi-dense vectors
/// <https://arxiv.org/pdf/2112.01488.pdf>, figure 1
/// This metric is also implemented in `QuantizedMultivectorStorage` structure for quantized data.
///
/// Disclaimer: this score is not equivalent to the original Colbert metric for Euclidean space because we do not apply the post-processing step.
/// In that case the score value will be different but the ranking will be the same.
/// We do that because of performance reasons and complex sort ordering of the vectors when applying the post-processing step.
pub fn score_max_similarity<T: PrimitiveVectorElement, TMetric: Metric<T>>(
    multi_dense_a: TypedMultiDenseVectorRef<T>,
    multi_dense_b: TypedMultiDenseVectorRef<T>,
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

fn score_multi<T: PrimitiveVectorElement, TMetric: Metric<T>>(
    multi_vector_config: &MultiVectorConfig,
    multi_dense_a: TypedMultiDenseVectorRef<T>,
    multi_dense_b: TypedMultiDenseVectorRef<T>,
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
/// - If one vector is bigger then the prefetch size - don't use prefetch
/// - ???
pub fn is_read_with_prefetch_efficient_points(ids: &[PointOffsetType]) -> bool {
    is_read_with_prefetch_efficient(ids.iter().map(|x| *x as usize))
}

pub fn is_read_with_prefetch_efficient_vectors(ids: &[VectorOffsetType]) -> bool {
    is_read_with_prefetch_efficient(ids.iter().copied())
}

fn is_read_with_prefetch_efficient(ids: impl IntoIterator<Item = usize>) -> bool {
    let mut min = usize::MAX;
    let mut max = 0;
    let mut n = 0;

    for id in ids {
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
    use crate::spaces::simple::EuclidMetric;

    #[test]
    fn test_check_ids_rather_contiguous() {
        assert!(!is_read_with_prefetch_efficient_points(&[]));
        assert!(!is_read_with_prefetch_efficient_points(&[1]));
        assert!(is_read_with_prefetch_efficient_points(&[1, 2]));
        assert!(is_read_with_prefetch_efficient_points(&[2, 1]));
        assert!(is_read_with_prefetch_efficient_points(&[1, 2, 3, 9, 10]));
        assert!(is_read_with_prefetch_efficient_points(&[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        ]));
        assert!(is_read_with_prefetch_efficient_points(&[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 11
        ]));
        assert!(!is_read_with_prefetch_efficient_points(&[
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
