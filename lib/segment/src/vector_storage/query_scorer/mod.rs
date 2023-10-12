use common::types::{PointOffsetType, ScoreType};

use crate::data_types::vectors::VectorElementType;

pub mod discovery_query_scorer;
pub mod metric_query_scorer;
pub mod reco_query_scorer;

pub trait QueryScorer {
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType;

    fn score(&self, v2: &[VectorElementType]) -> ScoreType;

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType;
}
