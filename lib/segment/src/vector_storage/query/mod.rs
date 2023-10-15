use common::types::ScoreType;

use crate::data_types::vectors::VectorType;

pub mod discovery_query;
pub mod reco_query;

pub trait TransformInto<Output, T = VectorType, U = VectorType> {
    /// Change the underlying type of the query, or just process it in some way.
    fn transform<F>(self, f: F) -> Output
    where
        F: FnMut(T) -> U;
}

pub trait Query<T> {
    /// Compares the vectors of the query against a single vector via a similarity function,
    /// then folds the similarites into a single score.
    fn score_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType;
}
