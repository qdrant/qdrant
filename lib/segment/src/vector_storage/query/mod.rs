use common::types::ScoreType;

use crate::data_types::vectors::VectorType;

pub mod discovery_query;
pub mod reco_query;

pub trait TransformInto<Output, T = VectorType, U = VectorType> {
    fn transform<F>(self, f: F) -> Output
    where
        F: FnMut(T) -> U;
}

pub trait Query<T> {
    fn score_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType;
}
