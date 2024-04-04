use common::types::{PointOffsetType, ScoreType};

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
    let mut sum = 0.0;
    for dense_a in multi_dense_a.iter() {
        sum += multi_dense_b
            .iter()
            .map(|dense_b| TMetric::similarity(dense_a, dense_b))
            .fold(ScoreType::NEG_INFINITY, |a, b| if a > b { a } else { b });
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
