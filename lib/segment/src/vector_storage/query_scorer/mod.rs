use common::types::{PointOffsetType, ScoreType};

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::TypedMultiDenseVectorRef;
use crate::spaces::metric::Metric;
use crate::types::{MultiVectorComparator, MultiVectorConfig};

pub mod custom_query_scorer;
pub mod metric_query_scorer;
pub mod multi_custom_query_scorer;
pub mod multi_metric_query_scorer;
pub mod sparse_custom_query_scorer;

pub trait QueryScorer<TVector: ?Sized> {
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType;

    fn score(&self, v2: &TVector) -> ScoreType;

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType;
}

/// Colbert MaxSim metric, metric for multi-dense vectors
/// https://arxiv.org/pdf/2112.01488.pdf, figure 1
/// This metric is also implemented in `QuantizedMultivectorStorage` structure for quantized data.
pub fn score_max_similarity<T: PrimitiveVectorElement, TMetric: Metric<T>>(
    multi_dense_a: TypedMultiDenseVectorRef<T>,
    multi_dense_b: TypedMultiDenseVectorRef<T>,
) -> ScoreType {
    debug_assert!(!multi_dense_a.is_empty());
    debug_assert!(!multi_dense_b.is_empty());

    multi_dense_a
        .multi_vectors()
        .map(|dense_a| {
            // For each 'dense_a', compute the maximum similarity with vectors in 'multi_dense_b'
            multi_dense_b
                .multi_vectors()
                .map(|dense_b| TMetric::similarity(dense_a, dense_b))
                // Find the maximum similarity value for this 'dense_a'
                .fold(ScoreType::NEG_INFINITY, |a, b| if a > b { a } else { b })
        })
        // Sum up the maximum similarities for all vectors in 'multi_dense_a'
        .sum()
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
