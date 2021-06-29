use crate::types::{Distance, ScoreType, VectorElementType};
use ndarray::Array1;

pub trait Metric {
    fn distance(&self) -> Distance;

    /// Greater the value - closer the vectors
    fn similarity(&self, v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType;

    /// Same as similarity, but using BLAS-supported functions
    fn blas_similarity(
        &self,
        v1: &Array1<VectorElementType>,
        v2: &Array1<VectorElementType>,
    ) -> ScoreType;

    /// Necessary vector transformations performed before adding it to the collection (like normalization)
    fn preprocess(&self, vector: Vec<VectorElementType>) -> Vec<VectorElementType>;
}
