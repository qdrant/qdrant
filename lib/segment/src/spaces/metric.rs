use common::types::ScoreType;

use crate::data_types::vectors::{DenseVector, VectorElementType};
use crate::types::Distance;

/// Defines how to compare vectors
pub trait Metric {
    fn distance() -> Distance;

    /// Greater the value - closer the vectors
    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType;

    /// Necessary vector transformations performed before adding it to the collection (like normalization)
    /// If no transformation is needed - returns the same vector
    fn preprocess(vector: DenseVector) -> DenseVector;

    /// correct metric score for displaying
    fn postprocess(score: ScoreType) -> ScoreType;
}
