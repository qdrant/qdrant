use std::cmp::min_by;

use common::math::scaled_fast_sigmoid;
use common::types::ScoreType;

use super::discovery_query::DiscoveryPair;
use super::{Query, TransformInto};
use crate::data_types::vectors::{QueryVector, VectorType};

impl<T> DiscoveryPair<T> {
    /// In the first stage of discovery search, the objective is to get the best entry point
    /// for the search. This is done by using a smooth loss function instead of hard ranking
    /// to approach the best zone, once the best zone is reached, score will be same for all
    /// points inside that zone.
    ///
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
    ///
    /// Output will always be in the range (-1, 0]
    pub fn loss_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType {
        let positive = scaled_fast_sigmoid(similarity(&self.positive));
        let negative = scaled_fast_sigmoid(similarity(&self.negative));

        min_by(positive - negative - ScoreType::EPSILON, 0.0, |a, b| {
            a.total_cmp(b)
        })
    }
}

#[derive(Debug, Clone)]
pub struct ContextQuery<T> {
    pub pairs: Vec<DiscoveryPair<T>>,
}

impl<T> ContextQuery<T> {
    pub fn new(pairs: Vec<DiscoveryPair<T>>) -> Self {
        Self { pairs }
    }

    pub fn iter_all(&self) -> impl Iterator<Item = &T> {
        self.pairs.iter().flat_map(|pair| pair.iter())
    }
}

impl<T, U> TransformInto<ContextQuery<U>, T, U> for ContextQuery<T> {
    fn transform<F>(self, mut f: F) -> ContextQuery<U>
    where
        F: FnMut(T) -> U,
    {
        ContextQuery::new(
            self.pairs
                .into_iter()
                .map(|pair| pair.transform(&mut f))
                .collect(),
        )
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

impl From<ContextQuery<VectorType>> for QueryVector {
    fn from(query: ContextQuery<VectorType>) -> Self {
        QueryVector::Context(query)
    }
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;

    use common::types::ScoreType;
    use rstest::rstest;

    use super::*;

    fn dummy_similarity(x: &isize) -> ScoreType {
        *x as ScoreType
    }

    /// Compares the score of a query against a fixed score
    ///
    //TODO(luis): set actual cases
    #[rstest]
    #[case::no_pairs(vec![], Ordering::Less)]
    #[case::just_above(vec![(1,0),(1,0)], Ordering::Greater)]
    #[case::just_below(vec![(1,0),(1,0)], Ordering::Less)]
    #[case::bad_target_good_context(vec![(1,0),(1,0),(1, 0)], Ordering::Greater)]
    #[case::good_target_bad_context(vec![(1,0),(0,1)], Ordering::Less)]
    fn score_better(#[case] pairs: Vec<(isize, isize)>, #[case] expected: Ordering) {
        let fixed_score: f32 = -0.5;

        let pairs = pairs.into_iter().map(DiscoveryPair::from).collect();

        let query = ContextQuery::new(pairs);

        let score = query.score_by(dummy_similarity);

        assert_eq!(
            score.total_cmp(&fixed_score),
            expected,
            "Comparison is incorrect, expected {expected:?} for {score} against {fixed_score}"
        );
    }
}
