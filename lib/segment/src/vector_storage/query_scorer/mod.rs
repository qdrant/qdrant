use common::types::{PointOffsetType, ScoreType};

use crate::data_types::vectors::MultiDenseVector;
use crate::spaces::metric::Metric;

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
pub fn score_multi<TMetric: Metric>(
    multidense_a: &MultiDenseVector,
    multidense_b: &MultiDenseVector,
) -> ScoreType {
    let mut sum = 0.0;
    for dense_a in multidense_a.iter() {
        sum += multidense_b
            .iter()
            .map(|dense_b| TMetric::similarity(dense_a, dense_b))
            .fold(0.0, |a, b| if a > b { a } else { b });
    }
    sum
}
