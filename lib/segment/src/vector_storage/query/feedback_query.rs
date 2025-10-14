use std::hash::Hash;

use common::types::ScoreType;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde::Serialize;

use super::{Query, TransformInto};
use crate::common::operation_error::OperationResult;

const DEFAULT_MAX_PAIRS: usize = 3;

#[derive(Clone, Debug)]
pub struct FeedbackItem<T> {
    vector: T,
    score: ScoreType,
}

impl<T> FeedbackItem<T> {
    pub fn transform<F, U>(self, mut f: F) -> OperationResult<FeedbackItem<U>>
    where
        F: FnMut(T) -> OperationResult<U>,
    {
        Ok(FeedbackItem {
            vector: f(self.vector)?,
            score: self.score,
        })
    }
}

#[derive(Clone, Debug)]
pub struct FeedbackQueryInternal<T, TStrategy> {
    pub target: T,
    pub feedback: Vec<FeedbackItem<T>>,
    pub strategy: TStrategy,
}

impl<T: Clone> FeedbackQueryInternal<T, SimpleFeedbackStrategy> {
    pub fn into_scorer(self) -> FeedbackScorer<T, SimpleFeedbackStrategy> {
        FeedbackScorer::new(self.target, self.feedback, self.strategy)
    }
}

impl<T, TStrategy> FeedbackQueryInternal<T, TStrategy> {
    pub fn flat_iter(&self) -> impl Iterator<Item = &T> {
        self.feedback
            .iter()
            .map(|item| &item.vector)
            .chain(std::iter::once(&self.target))
    }
}

impl<T, U, TStrategy> TransformInto<FeedbackQueryInternal<U, TStrategy>, T, U>
    for FeedbackQueryInternal<T, TStrategy>
{
    fn transform<F>(self, mut f: F) -> OperationResult<FeedbackQueryInternal<U, TStrategy>>
    where
        F: FnMut(T) -> OperationResult<U>,
    {
        let Self {
            target,
            feedback,
            strategy,
        } = self;
        Ok(FeedbackQueryInternal {
            target: f(target)?,
            feedback: feedback
                .into_iter()
                .map(|item| item.transform(&mut f))
                .try_collect()?,
            strategy,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Hash)]
pub struct PrecomputedFeedbackPair<T> {
    /// A vector with higher feedback score.
    pub positive: T,
    /// A vector with lower feedback score.
    pub negative: T,
    /// Partial computation related to this pair.
    pub partial_computation: OrderedFloat<f32>,
}

impl<T> PrecomputedFeedbackPair<T> {
    pub fn iter_vectors(&self) -> impl Iterator<Item = &T> {
        [&self.positive, &self.negative].into_iter()
    }

    pub fn transform<F, U>(self, mut f: F) -> OperationResult<PrecomputedFeedbackPair<U>>
    where
        F: FnMut(T) -> OperationResult<U>,
    {
        Ok(PrecomputedFeedbackPair {
            positive: f(self.positive)?,
            negative: f(self.negative)?,
            partial_computation: self.partial_computation,
        })
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

impl SimpleFeedbackStrategy {
    /// Extracts pairs of points, ranked by score difference in descending order.
    ///
    /// Assumes scoring order is BiggerIsBetter
    fn extract_feedback_pairs<TVector: Clone>(
        &self,
        mut feedback: Vec<FeedbackItem<TVector>>,
        num_pairs: usize,
    ) -> Vec<PrecomputedFeedbackPair<TVector>> {
        feedback.sort_by_key(|item| OrderedFloat(-item.score));

        if feedback.len() < 2 {
            return Vec::new();
        }

        // Pair front and back items until we run out of them
        let mut front_idx = 0;
        let mut back_idx = feedback.len() - 1;

        let max_num_pairs = num_pairs.min(feedback.len() / 2);
        let mut feedback_pairs = Vec::with_capacity(max_num_pairs);

        while front_idx < back_idx && feedback_pairs.len() < max_num_pairs {
            let front = &feedback[front_idx];
            let back = &feedback[back_idx];

            let confidence = front.score - back.score;

            let partial_computation = confidence.powf(self.b.0) * self.c.0;
            feedback_pairs.push(PrecomputedFeedbackPair {
                positive: front.vector.clone(),
                negative: back.vector.clone(),
                partial_computation: partial_computation.into(),
            });

            front_idx += 1;
            back_idx -= 1;
        }

        feedback_pairs
    }
}

/// Query for relevance feedback scoring
#[derive(Debug, Clone, PartialEq, Serialize, Hash)]
pub struct FeedbackScorer<TVector, TStrategy> {
    /// The original query vector.
    pub target: TVector,

    /// Pairs of results with higher difference in their golden score.
    pub feedback_pairs: Vec<PrecomputedFeedbackPair<TVector>>,

    /// Formula to use.
    pub strategy: TStrategy,
}

impl<TVector: Clone> FeedbackScorer<TVector, SimpleFeedbackStrategy> {
    pub fn new(
        target: TVector,
        feedback: Vec<FeedbackItem<TVector>>,
        strategy: SimpleFeedbackStrategy,
    ) -> Self {
        let feedback_pairs = strategy.extract_feedback_pairs(feedback, DEFAULT_MAX_PAIRS);

        Self {
            target,
            feedback_pairs,
            strategy,
        }
    }
}

impl<T, U, TStrategy> TransformInto<FeedbackScorer<U, TStrategy>, T, U>
    for FeedbackScorer<T, TStrategy>
{
    fn transform<F>(self, mut f: F) -> OperationResult<FeedbackScorer<U, TStrategy>>
    where
        F: FnMut(T) -> OperationResult<U>,
    {
        let Self {
            target,
            feedback_pairs,
            strategy,
        } = self;
        Ok(FeedbackScorer {
            target: f(target)?,
            feedback_pairs: feedback_pairs
                .into_iter()
                .map(|pair| pair.transform(&mut f))
                .try_collect()?,
            strategy,
        })
    }
}

impl<T> Query<T> for FeedbackScorer<T, SimpleFeedbackStrategy> {
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
            let PrecomputedFeedbackPair {
                positive,
                negative,
                partial_computation,
            } = pair;

            let delta = similarity(positive) - similarity(negative);

            score += partial_computation.0 * delta;
        }

        score
    }
}
