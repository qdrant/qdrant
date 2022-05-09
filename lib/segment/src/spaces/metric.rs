use crate::types::{Distance, ScoreType, VectorElementType};

/// Defines how to compare vectors
pub trait Metric {
    fn distance() -> Distance;

    /// Greater the value - closer the vectors
    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType;

    /// Necessary vector transformations performed before adding it to the collection (like normalization)
    /// Return None if metric does not required preprocessing
    fn preprocess(vector: &[VectorElementType]) -> Option<Vec<VectorElementType>>;

    /// correct metric score for displaying
    fn postprocess(score: ScoreType) -> ScoreType;
}
