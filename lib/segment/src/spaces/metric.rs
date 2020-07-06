use crate::types::{ScoreType, Distance};




pub trait Metric<El> {
    fn distance(&self) -> Distance;

    /// Greater the value - closer the vectors
    fn similarity(&self, v1: &[El], v2: &[El]) -> ScoreType;

    fn similarity_batch(&self, vector: &[El], other_vectors: &[&[El]]) -> Vec<ScoreType>;
    
    /// Necessary vector transformations performed before adding it to the collection (like normalization)
    fn preprocess(&self, vector: Vec<El>) -> Vec<El>;
}
