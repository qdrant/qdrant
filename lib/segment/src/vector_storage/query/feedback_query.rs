use std::hash::Hash;

use common::types::ScoreType;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde::Serialize;

use super::{Query, TransformInto};
use crate::common::operation_error::OperationResult;

#[derive(Clone, Debug, Serialize, Hash, PartialEq)]
pub struct FeedbackItem<T> {
    pub vector: T,
    pub score: OrderedFloat<ScoreType>,
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

/// Akin to external representation of the query. Unoptimized for scoring.
///
/// Call `into_query` to get the type implementing `Query` trait.
#[derive(Clone, Debug, Serialize, Hash, PartialEq)]
pub struct NaiveFeedbackQuery<T> {
    /// The original query vector.
    pub target: T,

    /// Vectors scored by the feedback model.
    pub feedback: Vec<FeedbackItem<T>>,

    /// How to handle the feedback
    pub coefficients: NaiveFeedbackCoefficients,
}

impl<T: Clone> NaiveFeedbackQuery<T> {
    pub fn into_query(self) -> FeedbackQuery<T> {
        FeedbackQuery::new(self.target, self.feedback, self.coefficients)
    }
}

impl<T> NaiveFeedbackQuery<T> {
    pub fn flat_iter(&self) -> impl Iterator<Item = &T> {
        self.feedback
            .iter()
            .map(|item| &item.vector)
            .chain(std::iter::once(&self.target))
    }
}

impl<T, U> TransformInto<NaiveFeedbackQuery<U>, T, U> for NaiveFeedbackQuery<T> {
    fn transform<F>(self, mut f: F) -> OperationResult<NaiveFeedbackQuery<U>>
    where
        F: FnMut(T) -> OperationResult<U>,
    {
        let Self {
            target,
            feedback,
            coefficients,
        } = self;
        Ok(NaiveFeedbackQuery {
            target: f(target)?,
            feedback: feedback
                .into_iter()
                .map(|item| item.transform(&mut f))
                .try_collect()?,
            coefficients,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Hash)]
pub struct ContextPair<T> {
    /// A vector with higher feedback score.
    pub positive: T,
    /// A vector with lower feedback score.
    pub negative: T,
    /// Partial computation related to this pair.
    pub partial_computation: OrderedFloat<f32>,
}

impl<T> ContextPair<T> {
    pub fn transform<F, U>(self, mut f: F) -> OperationResult<ContextPair<U>>
    where
        F: FnMut(T) -> OperationResult<U>,
    {
        Ok(ContextPair {
            positive: f(self.positive)?,
            negative: f(self.negative)?,
            partial_computation: self.partial_computation,
        })
    }
}

/// Trained coefficients for the formula. Specific to a triplet of dataset-smallmodel-bigmodel.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize)]
pub struct NaiveFeedbackCoefficients {
    /// Trained coefficient `a`
    pub a: OrderedFloat<f32>,
    /// Trained coefficient `b`
    pub b: OrderedFloat<f32>,
    /// Trained coefficient `c`
    pub c: OrderedFloat<f32>,
}

impl NaiveFeedbackCoefficients {
    /// Extracts pairs of points which have more than `margin` in their difference of scores
    ///
    /// Assumes scoring order is BiggerIsBetter
    fn extract_context_pairs<TVector: Clone>(
        &self,
        feedback: Vec<FeedbackItem<TVector>>,
        margin: f32,
    ) -> Vec<ContextPair<TVector>> {
        if feedback.len() < 2 {
            // return early as pairs cannot be formed
            return Vec::new();
        }

        let mut feedback_pairs = Vec::new();
        for permutation in feedback.iter().permutations(2) {
            let (positive, negative) = (permutation[0], permutation[1]);
            let confidence = positive.score - negative.score;

            if confidence.0 <= margin {
                continue;
            }

            let partial_computation = confidence.powf(self.b.0) * self.c.0;
            feedback_pairs.push(ContextPair {
                positive: positive.vector.clone(),
                negative: negative.vector.clone(),
                partial_computation: partial_computation.into(),
            });
        }
        feedback_pairs
    }
}

/// Query for relevance feedback scoring
#[derive(Debug, Clone, PartialEq, Serialize, Hash)]
pub struct FeedbackQuery<TVector> {
    /// The original query vector.
    target: TVector,

    /// Pairs of results with higher difference in their feedback score.
    context_pairs: Vec<ContextPair<TVector>>,

    /// How to handle the feedback
    coefficients: NaiveFeedbackCoefficients,
}

impl<TVector: Clone> FeedbackQuery<TVector> {
    pub fn new(
        target: TVector,
        feedback: Vec<FeedbackItem<TVector>>,
        coefficients: NaiveFeedbackCoefficients,
    ) -> Self {
        let context_pairs = coefficients.extract_context_pairs(feedback, 0.0);

        Self {
            target,
            context_pairs,
            coefficients,
        }
    }
}

impl<T, U> TransformInto<FeedbackQuery<U>, T, U> for FeedbackQuery<T> {
    fn transform<F>(self, mut f: F) -> OperationResult<FeedbackQuery<U>>
    where
        F: FnMut(T) -> OperationResult<U>,
    {
        let Self {
            target,
            context_pairs,
            coefficients,
        } = self;
        Ok(FeedbackQuery {
            target: f(target)?,
            context_pairs: context_pairs
                .into_iter()
                .map(|pair| pair.transform(&mut f))
                .try_collect()?,
            coefficients,
        })
    }
}

impl<T> Query<T> for FeedbackQuery<T> {
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
            context_pairs,
            coefficients,
        } = self;

        let mut score = coefficients.a.0 * similarity(target);

        for pair in context_pairs {
            let ContextPair {
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
