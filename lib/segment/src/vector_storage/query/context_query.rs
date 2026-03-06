use std::hash::Hash;
use std::iter::{self, Chain, Once};

use common::math::fast_sigmoid;
use common::types::ScoreType;
use itertools::Itertools;
use serde::Serialize;

use super::{Query, TransformInto};
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::{QueryVector, VectorInternal};

#[derive(Debug, Clone, PartialEq, Serialize, Hash)]
pub struct ContextPair<T> {
    pub positive: T,
    pub negative: T,
}

impl<T> ContextPair<T> {
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        iter::once(&self.positive).chain(iter::once(&self.negative))
    }

    pub fn transform<F, U>(self, mut f: F) -> OperationResult<ContextPair<U>>
    where
        F: FnMut(T) -> OperationResult<U>,
    {
        Ok(ContextPair {
            positive: f(self.positive)?,
            negative: f(self.negative)?,
        })
    }

    /// In the first stage of discovery search, the objective is to get the best entry point
    /// for the search. This is done by using a smooth loss function instead of hard ranking
    /// to approach the best zone, once the best zone is reached, score will be same for all
    /// points inside that zone.
    /// e.g.:
    /// ```text
    ///                   │
    ///                   │
    ///                   │    +0
    ///                   │             +0
    ///                   │
    ///         n         │         p
    ///                   │
    ///   ─►          ─►  │
    ///  -0.4        -0.1 │   +0
    ///                   │
    /// ```
    /// Simple 2D model:
    /// <https://www.desmos.com/calculator/lbxycyh2hs>
    pub fn loss_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType {
        const MARGIN: ScoreType = ScoreType::EPSILON;

        let positive = similarity(&self.positive);
        let negative = similarity(&self.negative);

        let difference = positive - negative - MARGIN;

        fast_sigmoid(ScoreType::min(difference, 0.0))
    }
}

impl<T> IntoIterator for ContextPair<T> {
    type Item = T;

    type IntoIter = Chain<Once<T>, Once<T>>;

    fn into_iter(self) -> Self::IntoIter {
        iter::once(self.positive).chain(iter::once(self.negative))
    }
}

#[cfg(test)]
impl<T> From<(T, T)> for ContextPair<T> {
    fn from(pair: (T, T)) -> Self {
        Self {
            positive: pair.0,
            negative: pair.1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Hash)]
pub struct ContextQuery<T> {
    pub pairs: Vec<ContextPair<T>>,
}

impl<T> ContextQuery<T> {
    pub fn new(pairs: Vec<ContextPair<T>>) -> Self {
        Self { pairs }
    }

    pub fn flat_iter(&self) -> impl Iterator<Item = &T> {
        self.pairs.iter().flat_map(|pair| pair.iter())
    }
}

impl<T, U> TransformInto<ContextQuery<U>, T, U> for ContextQuery<T> {
    fn transform<F>(self, mut f: F) -> OperationResult<ContextQuery<U>>
    where
        F: FnMut(T) -> OperationResult<U>,
    {
        Ok(ContextQuery::new(
            self.pairs
                .into_iter()
                .map(|pair| pair.transform(&mut f))
                .try_collect()?,
        ))
    }
}

impl<T> Query<T> for ContextQuery<T> {
    fn score_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType {
        self.pairs
            .iter()
            .map(|pair| pair.loss_by(&similarity))
            .sum()
    }
}

impl<T> From<Vec<ContextPair<T>>> for ContextQuery<T> {
    fn from(pairs: Vec<ContextPair<T>>) -> Self {
        ContextQuery::new(pairs)
    }
}

impl From<ContextQuery<VectorInternal>> for QueryVector {
    fn from(query: ContextQuery<VectorInternal>) -> Self {
        QueryVector::Context(query)
    }
}

#[cfg(test)]
mod test {
    use common::types::ScoreType;
    use proptest::prelude::*;

    use super::*;

    fn dummy_similarity(x: &f32) -> ScoreType {
        *x as ScoreType
    }

    /// Possible similarities
    fn sim() -> impl Strategy<Value = f32> {
        (-100.0..=100.0).prop_map(|x| x as f32)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        /// Checks that the loss is between 0 and -1
        #[test]
        fn loss_is_not_more_than_1_per_pair((p, n) in (sim(), sim())) {
            let query = ContextQuery::new(vec![ContextPair::from((p, n))]);

            let score = query.score_by(dummy_similarity);
            assert!(score <= 0.0, "similarity: {score}");
            assert!(score > -1.0, "similarity: {score}");
        }
    }
}
