use std::hash::Hash;

use common::types::ScoreType;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde::Serialize;

use super::{Query, TransformInto};
use crate::common::operation_error::OperationResult;

#[derive(Debug, Clone, PartialEq, Serialize, Hash)]
pub struct FeedbackPair<T> {
    /// A vector with higher by golden score.
    pub positive: T,

    /// A vector with lower by golden score.
    pub negative: T,

    /// The difference in score between the pair of vectors.
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
pub struct TrainedCoefficients {
    pub a: OrderedFloat<f32>,
    pub b: OrderedFloat<f32>,
    pub c: OrderedFloat<f32>,
}

/// Query for relevance feedback scoring
#[derive(Debug, Clone, PartialEq, Serialize, Hash)]
pub struct FeedbackQuery<T> {
    /// The original query vector.
    pub query: T,

    /// Pairs of results with higher difference in their golden score.
    pub feedback_pairs: Vec<FeedbackPair<T>>,

    /// Trained coefficients for the formula.
    pub coefficients: TrainedCoefficients,
}

impl<T> FeedbackQuery<T> {
    pub fn new(
        query: T,
        feedback_pairs: Vec<FeedbackPair<T>>,
        coefficients: TrainedCoefficients,
    ) -> Self {
        Self {
            query,
            feedback_pairs,
            coefficients,
        }
    }

    pub fn flat_iter(&self) -> impl Iterator<Item = &T> {
        self.feedback_pairs
            .iter()
            .flat_map(|pair| pair.iter())
            .chain(std::iter::once(&self.query))
    }
}

impl<T, U> TransformInto<FeedbackQuery<U>, T, U> for FeedbackQuery<T> {
    fn transform<F>(self, mut f: F) -> OperationResult<FeedbackQuery<U>>
    where
        F: FnMut(T) -> OperationResult<U>,
    {
        let Self {
            query,
            feedback_pairs,
            coefficients,
        } = self;
        Ok(FeedbackQuery::new(
            f(query)?,
            feedback_pairs
                .into_iter()
                .map(|pair| pair.transform(&mut f))
                .try_collect()?,
            coefficients,
        ))
    }
}

#[inline]
fn pair_formula(confidence: f32, delta: f32, coefficients: &TrainedCoefficients) -> f32 {
    let TrainedCoefficients {
        a: OrderedFloat(_a),
        b: OrderedFloat(b),
        c: OrderedFloat(c),
    } = coefficients;

    confidence.powf(*b) * c * delta
}

impl<T> Query<T> for FeedbackQuery<T> {
    fn score_by(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType {
        let Self {
            query,
            feedback_pairs,
            coefficients,
        } = self;

        let mut score = coefficients.a.0 * similarity(query);

        for pair in feedback_pairs {
            let FeedbackPair {
                positive,
                negative,
                confidence,
            } = pair;

            let delta = similarity(positive) - similarity(negative);

            score += pair_formula(confidence.0, delta, coefficients);
        }

        score
    }
}
