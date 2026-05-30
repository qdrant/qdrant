use common::types::{PointOffsetType, ScoreType};
use ordered_float::OrderedFloat;

use crate::data_types::vectors::{MultiDenseVector, VectorElementType};
use crate::spaces::metric::Metric;
use crate::types::MultiVectorConfig;

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
pub fn score_max_similarity<TMetric: Metric<VectorElementType>>(
    multi_dense_a: &MultiDenseVector,
    multi_dense_b: &MultiDenseVector,
) -> ScoreType {
    // TODO(colbert) add user input validation
    debug_assert!(!multi_dense_a.is_empty());
    debug_assert!(!multi_dense_b.is_empty());
    let mut sum = 0.0;
    for dense_a in multi_dense_a.multi_vectors() {
        let mut max_sim = OrderedFloat(ScoreType::NEG_INFINITY);
        // manual `max_by` for performance
        for dense_b in multi_dense_b.multi_vectors() {
            let sim = OrderedFloat(TMetric::similarity(dense_a, dense_b));
            if sim > max_sim {
                max_sim = sim;
            }
        }
        // sum of max similarity
        sum += max_sim.into_inner();
    }
    sum
}

fn score_multi<TMetric: Metric<VectorElementType>>(
    multi_vector_config: &MultiVectorConfig,
    multi_dense_a: &MultiDenseVector,
    multi_dense_b: &MultiDenseVector,
) -> ScoreType {
    match multi_vector_config {
        MultiVectorConfig::MaxSim(_) => {
            score_max_similarity::<TMetric>(multi_dense_a, multi_dense_b)
        }
    }
}
