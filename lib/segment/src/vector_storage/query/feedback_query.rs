use std::hash::Hash;

use common::types::ScoreType;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde::Serialize;

use super::{Query, TransformInto};
use crate::common::operation_error::OperationResult;

#[derive(Debug, Clone, PartialEq, Serialize, Hash)]
pub struct FeedbackPair<T> {
    /// A vector with higher feedback score.
    pub positive: T,

    /// A vector with lower feedback score.
    pub negative: T,

    /// The difference in feedback score between the pair of vectors.
    pub confidence: OrderedFloat<f32>,
}

impl<T> FeedbackPair<T> {
    pub fn new(positive: T, negative: T, confidence: OrderedFloat<f32>) -> Self {
        Self {
            positive,
            negative,
            confidence,
        }
    }

    pub fn transform<F, U>(self, mut f: F) -> OperationResult<FeedbackPair<U>>
    where
        F: FnMut(T) -> OperationResult<U>,
    {
        Ok(FeedbackPair {
            positive: f(self.positive)?,
            negative: f(self.negative)?,
            confidence: self.confidence,
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        [&self.positive, &self.negative].into_iter()
    }
}

/// Trained coefficients for the formula. Specific to a triplet of dataset-smallmodel-bigmodel.
#[derive(Debug, Clone, PartialEq, Serialize, Hash)]
pub struct SimpleFeedbackStrategy {
    /// Trained coefficient `a`
    pub a: OrderedFloat<f32>,
    /// Trained coefficient `b`
    pub b: OrderedFloat<f32>,
    /// Trained coefficient `c`
    pub c: OrderedFloat<f32>,
}

/// Query for relevance feedback scoring
#[derive(Debug, Clone, PartialEq, Serialize, Hash)]
pub struct FeedbackQuery<T, TStrategy> {
    /// The original query vector.
    pub target: T,

    /// Pairs of results with higher difference in their golden score.
    pub feedback_pairs: Vec<FeedbackPair<T>>,

    /// Formula to use.
    pub strategy: TStrategy,
}

impl<T, TStrategy> FeedbackQuery<T, TStrategy> {
    pub fn new(target: T, feedback_pairs: Vec<FeedbackPair<T>>, strategy: TStrategy) -> Self {
        Self {
            target,
            feedback_pairs,
            strategy,
        }
    }

    pub fn flat_iter(&self) -> impl Iterator<Item = &T> {
        self.feedback_pairs
            .iter()
            .flat_map(|pair| pair.iter())
            .chain(std::iter::once(&self.target))
    }
}

impl<T, U, TStrategy> TransformInto<FeedbackQuery<U, TStrategy>, T, U>
    for FeedbackQuery<T, TStrategy>
{
    fn transform<F>(self, mut f: F) -> OperationResult<FeedbackQuery<U, TStrategy>>
    where
        F: FnMut(T) -> OperationResult<U>,
    {
        let Self {
            target,
            feedback_pairs,
            strategy,
        } = self;
        Ok(FeedbackQuery::new(
            f(target)?,
            feedback_pairs
                .into_iter()
                .map(|pair| pair.transform(&mut f))
                .try_collect()?,
            strategy,
        ))
    }
}

impl SimpleFeedbackStrategy {
    #[inline]
    fn pair_score(&self, confidence: f32, delta: f32) -> f32 {
        let Self {
            a: _, // `a` is used for the other part of the formula, not here
            b: OrderedFloat(b),
            c: OrderedFloat(c),
        } = self;

        confidence.powf(*b) * c * delta
    }
}

impl<T> Query<T> for FeedbackQuery<T, SimpleFeedbackStrategy> {
    /// This follows the following formula:
    ///
    /// $ a * score + \sum{confidence_pair ^b * c * delta_pair} $
    ///
    /// where
    /// - `confidence_pair` means the difference in feedback score of the pair,
    /// - `delta_pair` is the difference in similarity score between the target
    ///   and positive/negative vectors e.g. `similarity(positive) - similarity(negative)`
    fn score_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType {
        let Self {
            target,
            feedback_pairs,
            strategy,
        } = self;

        let mut score = strategy.a.0 * similarity(target);

        for pair in feedback_pairs {
            let FeedbackPair {
                positive,
                negative,
                confidence,
            } = pair;

            let delta = similarity(positive) - similarity(negative);

            score += strategy.pair_score(confidence.0, delta);
        }

        score
    }
}
