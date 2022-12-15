use crate::data_types::vectors::VectorElementType;
use crate::types::{Distance, ScoreType};

pub const METRIC_SCORING_CHUNK: usize = 4;

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

    fn similarity_chunk(q_ptr: *const f32, v_ptrs: [*const f32; 4], dim: usize) -> [f32; 4];
}
