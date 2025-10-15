use common::types::ScoreType;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::DenseVector;

mod context_query;
mod discovery_query;
mod feedback_query;
mod reco_query;

pub use context_query::{ContextPair, ContextQuery};
pub use discovery_query::DiscoveryQuery;
pub use feedback_query::{FeedbackItem, FeedbackQueryInternal, SimpleFeedbackStrategy};
pub use reco_query::{RecoBestScoreQuery, RecoQuery, RecoSumScoresQuery};

pub trait TransformInto<Output, T = DenseVector, U = DenseVector> {
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
