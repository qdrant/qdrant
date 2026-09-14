use std::hash::Hash;
use std::iter::{self, Chain, Once};

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

    pub fn transform<U>(
        self,
        f: &dyn Fn(T) -> OperationResult<U>,
    ) -> OperationResult<ContextPair<U>> {
        Ok(ContextPair {
            positive: f(self.positive)?,
            negative: f(self.negative)?,
        })
    }

    /// In context search or the first stage of discovery search, the objective is to get points
    /// in the best zone where positive examples are preferred over negative examples.
    /// This is done using a loss function: if a point is closer to a negative example than
    /// to a positive example, it receives a negative loss equal to the similarity difference.
    /// Points inside the positive zone receive a score of 0.0.
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
    pub fn loss_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType {
        const MARGIN: ScoreType = ScoreType::EPSILON;

        let positive = similarity(&self.positive);
        let negative = similarity(&self.negative);

        let difference = positive - negative - MARGIN;

        ScoreType::min(difference, 0.0)
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
    fn transform(self, f: &dyn Fn(T) -> OperationResult<U>) -> OperationResult<ContextQuery<U>> {
        Ok(ContextQuery::new(
            self.pairs
                .into_iter()
                .map(|pair| pair.transform(f))
                .try_collect()?,
        ))
    }
}

impl<T> Query<T> for ContextQuery<T> {
    fn score_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType {
        let mut sum = 0.0;
        for pair in &self.pairs {
            sum += pair.loss_by(&similarity);
        }
        sum
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

        /// Checks that the loss is non-positive and matches the documented formula min(p - n - EPSILON, 0.0)
        #[test]
        fn loss_matches_formula((p, n) in (sim(), sim())) {
            let query = ContextQuery::new(vec![ContextPair::from((p, n))]);

            let score = query.score_by(dummy_similarity);
            let expected = ScoreType::min(p - n - ScoreType::EPSILON, 0.0);
            assert!(score <= 0.0, "similarity: {score}");
            assert!((score - expected).abs() < 1e-6, "score: {score}, expected: {expected}");
        }

        /// Scale invariance: scaling similarity differences by c > 0 scales the total loss proportionally
        #[test]
        fn scale_invariance(
            (p1, n1) in (sim(), sim()),
            (p2, n2) in (sim(), sim()),
            scale in 0.01f32..100.0f32,
        ) {
            let query_raw = ContextQuery::new(vec![
                ContextPair::from((p1, n1)),
                ContextPair::from((p2, n2)),
            ]);
            let query_scaled = ContextQuery::new(vec![
                ContextPair::from((p1 * scale, n1 * scale)),
                ContextPair::from((p2 * scale, n2 * scale)),
            ]);

            let score_raw = query_raw.score_by(dummy_similarity);
            let score_scaled = query_scaled.score_by(dummy_similarity);

            assert!(
                (score_raw * scale - score_scaled).abs() <= 1e-4 * scale.max(1.0),
                "score_raw: {score_raw}, scaled: {score_scaled}, scale: {scale}"
            );
        }
    }

    #[test]
    fn test_issue_10612_exact_loss_scores() {
        // Reproduce exact values from issue #10612
        // positive context: [1.0, 0.0]
        // negative context: [-1.0, 0.0]
        let dot = |v1: &[f32; 2], v2: &[f32; 2]| v1[0] * v2[0] + v1[1] * v2[1];
        let pos = [1.0, 0.0];
        let neg = [-1.0, 0.0];
        let pair = ContextPair {
            positive: pos,
            negative: neg,
        };
        let query = ContextQuery::new(vec![pair]);

        // Candidate 0: [0.8, 0.6] -> pos = 0.8, neg = -0.8 -> diff = 1.6 > 0 -> score = 0.0
        let s0 = query.score_by(|v| dot(v, &[0.8, 0.6]));
        assert_eq!(s0, 0.0);

        // Candidate 2: [-0.8, 0.6] -> pos = -0.8, neg = 0.8 -> diff = -1.6 -> score ≈ -1.6
        let s2 = query.score_by(|v| dot(v, &[-0.8, 0.6]));
        assert!((s2 - (-1.6)).abs() < 1e-5, "got {s2}, expected -1.6");

        // Candidate 3: [-0.2, 0.979_795_9] -> pos = -0.2, neg = 0.2 -> diff = -0.4 -> score ≈ -0.4
        let s3 = query.score_by(|v| dot(v, &[-0.2, 0.979_795_9]));
        assert!((s3 - (-0.4)).abs() < 1e-5, "got {s3}, expected -0.4");

        // Mutant 1: [-0.5, 0.866_025_4] -> pos = -0.5, neg = 0.5 -> diff = -1.0 -> score ≈ -1.0
        let s_mutant = query.score_by(|v| dot(v, &[-0.5, 0.866_025_4]));
        assert!(
            (s_mutant - (-1.0)).abs() < 1e-5,
            "got {s_mutant}, expected -1.0"
        );
    }
}
