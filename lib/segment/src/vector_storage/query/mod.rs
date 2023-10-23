use common::types::ScoreType;

use crate::data_types::vectors::VectorType;

pub mod context_query;
pub mod discovery_query;
pub mod reco_query;

pub trait TransformInto<Output, T = VectorType, U = VectorType> {
    /// Change the underlying type of the query, or just process it in some way.
    fn transform<F>(self, f: F) -> Output
    where
        F: FnMut(T) -> U;

    fn transform_into(self) -> Output
    where
        Self: Sized,
        T: Into<U>,
    {
        self.transform(|v| v.into())
    }
}

pub trait Query<T> {
    /// Compares the vectors of the query against a single vector via a similarity function,
    /// then folds the similarites into a single score.
    fn score_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType;
}
