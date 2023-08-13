use crate::data_types::vectors::VectorElementType;
use crate::types::ScoreType;

pub mod metric_query_scorer;

pub trait QueryScorer {
    fn score(&self, v2: &[VectorElementType]) -> ScoreType;

    fn score_raw(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType;
}
