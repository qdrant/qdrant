use common::types::ScoreType;

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::DenseVector;
use crate::types::Distance;

/// Defines how to compare vectors
pub trait Metric<T: PrimitiveVectorElement> {
    fn distance() -> Distance;

    /// Greater the value - closer the vectors
    fn similarity(v1: &[T], v2: &[T]) -> ScoreType;

    /// [`Self::similarity`] of `v1` with four vectors, bit for bit the same scores.
    /// Metrics with a kernel that shares the loads of `v1` across the four override it.
    fn similarity_4(v1: &[T], v2: [&[T]; 4]) -> [ScoreType; 4] {
        v2.map(|v2| Self::similarity(v1, v2))
    }

    /// Necessary vector transformations performed before adding it to the collection (like normalization)
    /// If no transformation is needed - returns the same vector
    fn preprocess(vector: DenseVector) -> DenseVector;
}

pub trait MetricPostProcessing {
    /// correct metric score for displaying
    fn postprocess(score: ScoreType) -> ScoreType;
}
