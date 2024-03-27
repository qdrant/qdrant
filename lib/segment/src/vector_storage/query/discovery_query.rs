use std::iter;

use common::math::scaled_fast_sigmoid;
use common::types::ScoreType;
use itertools::Itertools;

use super::context_query::ContextPair;
use super::{Query, TransformInto};
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::{QueryVector, Vector};

type RankType = i32;

impl<T> ContextPair<T> {
    /// Calculates on which side of the space the point is, with respect to this pair
    fn rank_by(&self, similarity: impl Fn(&T) -> ScoreType) -> RankType {
        let positive_similarity = similarity(&self.positive);
        let negative_similarity = similarity(&self.negative);

        // if closer to positive, return 1, else -1
        positive_similarity.total_cmp(&negative_similarity) as RankType
    }
}

#[derive(Debug, Clone)]
pub struct DiscoveryQuery<T> {
    pub target: T,
    pub pairs: Vec<ContextPair<T>>,
}

impl<T> DiscoveryQuery<T> {
    pub fn new(target: T, pairs: Vec<ContextPair<T>>) -> Self {
        Self { target, pairs }
    }

    pub fn flat_iter(&self) -> impl Iterator<Item = &T> {
        let pairs_iter = self.pairs.iter().flat_map(|pair| pair.iter());

        iter::once(&self.target).chain(pairs_iter)
    }

    fn rank_by(&self, similarity: impl Fn(&T) -> ScoreType) -> RankType {
        self.pairs
            .iter()
            .map(|pair| pair.rank_by(&similarity))
            // get overall rank
            .sum()
    }
}

impl<T, U> TransformInto<DiscoveryQuery<U>, T, U> for DiscoveryQuery<T> {
    fn transform<F>(self, mut f: F) -> OperationResult<DiscoveryQuery<U>>
    where
        F: FnMut(T) -> OperationResult<U>,
    {
        Ok(DiscoveryQuery::new(
            f(self.target)?,
            self.pairs
                .into_iter()
                .map(|pair| pair.transform(&mut f))
                .try_collect()?,
        ))
    }
}

impl<T> Query<T> for DiscoveryQuery<T> {
    fn score_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType {
        let rank = self.rank_by(&similarity);

        let target_similarity = similarity(&self.target);
        let sigmoid_similarity = scaled_fast_sigmoid(target_similarity);

        ScoreType::from(rank) + sigmoid_similarity
    }
}

impl From<DiscoveryQuery<Vector>> for QueryVector {
    fn from(query: DiscoveryQuery<Vector>) -> Self {
        QueryVector::Discovery(query)
    }
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;

    use common::types::ScoreType;
    use itertools::Itertools;
    use proptest::prelude::*;
    use rstest::rstest;

    use super::*;

    fn dummy_similarity(x: &isize) -> ScoreType {
        *x as ScoreType
    }

    /// Considers each "vector" as the actual score from the similarity function by
    /// using a dummy identity function.
    #[rstest]
    #[case::no_pairs(vec![], 0)]
    #[case::closer_to_positive(vec![(10, 4)], 1)]
    #[case::closer_to_negative(vec![(4, 10)], -1)]
    #[case::equal_scores(vec![(11, 11)], 0)]
    #[case::neutral_zone(vec![(10, 4), (4, 10)], 0)]
    #[case::best_zone(vec![(10, 4), (4, 2)], 2)]
    #[case::worst_zone(vec![(4, 10), (2, 4)], -2)]
    #[case::many_pairs(vec![(1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (0, 4)], 4)]
    fn context_ranking(#[case] pairs: Vec<(isize, isize)>, #[case] expected: RankType) {
        let pairs = pairs.into_iter().map(ContextPair::from).collect();

        let _target = 42;

        let query = DiscoveryQuery::new(_target, pairs);

        let rank = query.rank_by(dummy_similarity);

        assert_eq!(
            rank, expected,
            "Ranking is incorrect, expected {}, but got {rank}",
            expected
        );
    }

    /// Compares the score of a query against a fixed score
    #[rstest]
    #[case::no_pairs(1, vec![], Ordering::Less)]
    #[case::just_above(1, vec![(1,0),(1,0)], Ordering::Greater)]
    #[case::just_below(-1, vec![(1,0),(1,0)], Ordering::Less)]
    #[case::bad_target_good_context(-1000, vec![(1,0),(1,0),(1, 0)], Ordering::Greater)]
    #[case::good_target_bad_context(1000, vec![(1,0),(0,1)], Ordering::Less)]
    fn score_better(
        #[case] target: isize,
        #[case] pairs: Vec<(isize, isize)>,
        #[case] expected: Ordering,
    ) {
        let fixed_score: f32 = 2.5;

        let pairs = pairs.into_iter().map(ContextPair::from).collect();

        let query = DiscoveryQuery::new(target, pairs);

        let score = query.score_by(dummy_similarity);

        assert_eq!(
            score.total_cmp(&fixed_score),
            expected,
            "Comparison is incorrect, expected {expected:?} for {score} against {fixed_score}"
        );
    }

    proptest! {
        #[test]
        fn same_target_only_changes_rank(
            target in -1000f32..1000f32,
            pairs1 in prop::collection::vec((0f32..1000f32, 0.0f32..1000f32), 0..10),
            pairs2 in prop::collection::vec((0f32..1000f32, 0.0f32..1000f32), 0..10),
        ) {
            let dummy_similarity = |x: &ScoreType| *x as ScoreType;

            let pairs1 = pairs1.into_iter().map(ContextPair::from).collect();
            let query1 = DiscoveryQuery::new(target, pairs1);
            let score1 = query1.score_by(dummy_similarity);

            let pairs2 = pairs2.into_iter().map(ContextPair::from).collect();
            let query2 = DiscoveryQuery::new(target, pairs2);
            let score2 = query2.score_by(dummy_similarity);

            let target_part1 = score1 - score1.floor();
            let target_part2 = score2 - score2.floor();

            assert!((target_part1 - target_part2).abs() <= 1.0e-6, "Target part of score is not similar, score1: {score1}, score2: {score2}");
        }

        #[test]
        fn same_context_only_changes_target(
            target1 in -1000f32..1000f32,
            target2 in -1000f32..1000f32,
            pairs in prop::collection::vec((0f32..1000f32, 0.0f32..1000f32), 0..10),
        )
        {
            let dummy_similarity = |x: &ScoreType| *x as ScoreType;

            let pairs = pairs.into_iter().map(ContextPair::from).collect_vec();
            let query1 = DiscoveryQuery::new(target1, pairs.clone());
            let score1 = query1.score_by(dummy_similarity);

            let query2 = DiscoveryQuery::new(target2, pairs);
            let score2 = query2.score_by(dummy_similarity);

            let context_part1 = score1.floor();
            let context_part2 = score2.floor();

            assert_eq!(context_part1, context_part2,"Context part of score isn't equal, score1: {score1}, score2: {score2}");
        }
    }
}
