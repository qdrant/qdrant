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

pub fn score_multi<TMetric: Metric>(a: &MultiDenseVector, b: &MultiDenseVector) -> ScoreType {
    let mut sum = 0.0;
    for a in a.iter() {
        sum += b
            .iter()
            .map(|b| TMetric::similarity(a, b))
            .fold(0.0, |a, b| if a > b { a } else { b });
    }
    sum
}
