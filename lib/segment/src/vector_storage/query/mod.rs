use common::types::ScoreType;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::VectorType;

pub mod context_query;
pub mod discovery_query;
pub mod reco_query;

pub trait TransformInto<Output, T = VectorType, U = VectorType> {
    /// Change the underlying type of the query, or just process it in some way.
    fn transform<F>(self, f: F) -> OperationResult<Output>
    where
        F: FnMut(T) -> OperationResult<U>;

    fn transform_into(self) -> OperationResult<Output>
    where
        Self: Sized,
        T: TryInto<U, Error = OperationError>,
    {
        self.transform(|v| v.try_into())
    }
}

pub trait Query<T> {
    /// Compares the vectors of the query against a single vector via a similarity function,
    /// then folds the similarites into a single score.
    fn score_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType;
}
