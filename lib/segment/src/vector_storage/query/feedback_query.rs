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
pub struct LinearFeedbackFormula {
    pub a: OrderedFloat<f32>,
    pub b: OrderedFloat<f32>,
    pub c: OrderedFloat<f32>,
}

/// Query for relevance feedback scoring
#[derive(Debug, Clone, PartialEq, Serialize, Hash)]
pub struct FeedbackQuery<T, TFormula> {
    /// The original query vector.
    pub query: T,

    /// Pairs of results with higher difference in their golden score.
    pub feedback_pairs: Vec<FeedbackPair<T>>,

    /// Trained coefficients for the formula.
    pub formula: TFormula,
}

impl<T, TFormula> FeedbackQuery<T, TFormula> {
    pub fn new(query: T, feedback_pairs: Vec<FeedbackPair<T>>, formula: TFormula) -> Self {
        Self {
            query,
            feedback_pairs,
            formula,
        }
    }

    pub fn flat_iter(&self) -> impl Iterator<Item = &T> {
        self.feedback_pairs
            .iter()
            .flat_map(|pair| pair.iter())
            .chain(std::iter::once(&self.query))
    }
}

impl<T, U, TFormula> TransformInto<FeedbackQuery<U, TFormula>, T, U>
    for FeedbackQuery<T, TFormula>
{
    fn transform<F>(self, mut f: F) -> OperationResult<FeedbackQuery<U, TFormula>>
    where
        F: FnMut(T) -> OperationResult<U>,
    {
        let Self {
            query,
            feedback_pairs,
            formula,
        } = self;
        Ok(FeedbackQuery::new(
            f(query)?,
            feedback_pairs
                .into_iter()
                .map(|pair| pair.transform(&mut f))
                .try_collect()?,
            formula,
        ))
    }
}

impl LinearFeedbackFormula {
    #[inline]
    fn pair_score(&self, confidence: f32, delta: f32) -> f32 {
        let Self {
            a: _,
            b: OrderedFloat(b),
            c: OrderedFloat(c),
        } = self;

        confidence.powf(*b) * c * delta
    }
}

impl<T> Query<T> for FeedbackQuery<T, LinearFeedbackFormula> {
    fn score_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType {
        let Self {
            query,
            feedback_pairs,
            formula,
        } = self;

        let mut score = formula.a.0 * similarity(query);

        for pair in feedback_pairs {
            let FeedbackPair {
                positive,
                negative,
                confidence,
            } = pair;

            let delta = similarity(positive) - similarity(negative);

            score += formula.pair_score(confidence.0, delta);
        }

        score
    }
}
