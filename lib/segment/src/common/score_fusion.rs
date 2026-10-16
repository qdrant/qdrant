use std::iter;

use ahash::AHashMap;
use common::types::ScoreType;
use itertools::{Itertools, MinMaxResult};
use ordered_float::OrderedFloat;

use crate::types::{Order, PointIdType, ScoredPoint};

pub struct ScoreFusion {
    /// Defines how to combine the scores of the same point in different lists
    pub method: Aggregation,
    /// Defines how to normalize the scores in each list
    pub norm: Normalization,
    /// Multipliers for each list of scores
    pub weights: Vec<f32>,
    /// Final ordering of the results
    pub order: Order,
}

impl ScoreFusion {
    /// Params for the distribution-based score fusion
    pub fn dbsf() -> Self {
        Self {
            method: Aggregation::Sum,
            norm: Normalization::Distr,
            weights: vec![],
            order: Order::LargeBetter,
        }
    }
}

/// Defines how to combine the scores of the same point in different lists
pub enum Aggregation {
    /// Sums the scores
    Sum,
}

pub enum Normalization {
    /// Uses the minimum and maximum scores as extremes
    MinMax,
    /// Uses the 3rd standard deviation as extremes
    Distr,
}

pub fn score_fusion(
    all_results: impl IntoIterator<Item = Vec<ScoredPoint>>,
    params: ScoreFusion,
) -> Vec<ScoredPoint> {
    let ScoreFusion {
        method,
        norm,
        weights,
        order,
    } = params;

    let weights = weights.into_iter().chain(iter::repeat(1.0));

    all_results
        .into_iter()
        // normalize
        .map(|points| match norm {
            Normalization::MinMax => min_max_norm(points),
            Normalization::Distr => distr_norm(points),
        })
        // weight each list of points
        .zip(weights)
        .flat_map(|(points, weight)| {
            points.into_iter().map(move |p| ScoredPoint {
                score: p.score * weight,
                ..p
            })
        })
        // combine to deduplicate
        .fold(
            AHashMap::<PointIdType, ScoredPoint>::new(),
            |mut acc, point| {
                acc.entry(point.id)
                    .and_modify(|entry| match method {
                        Aggregation::Sum => entry.score += point.score,
                    })
                    .or_insert(point);

                acc
            },
        )
        // sort and return
        .into_values()
        .sorted_by(|a, b| match order {
            Order::SmallBetter => a.cmp(b),
            Order::LargeBetter => b.cmp(a),
        })
        .collect()
}

/// Normalizes the scores of the given points between 0.0 and 1.0, using the given minimum and maximum scores as extremes.
fn norm(mut points: Vec<ScoredPoint>, min: ScoreType, max: ScoreType) -> Vec<ScoredPoint> {
    // Protect against division by zero
    if min == max {
        points.iter_mut().for_each(|p| p.score = 0.5);
        return points;
    }

    points.iter_mut().for_each(|p| {
        p.score = (p.score - min) / (max - min);
    });

    points
}

pub fn min_max_norm(points: Vec<ScoredPoint>) -> Vec<ScoredPoint> {
    let (min, max) = match points.iter().map(|p| OrderedFloat(p.score)).minmax() {
        MinMaxResult::NoElements | MinMaxResult::OneElement(_) => return points,
        MinMaxResult::MinMax(min, max) => (min.0, max.0),
    };

    norm(points, min, max)
}

/// Welford's method for stable one-pass mean and variance calculation.
/// <https://jonisalonen.com/2013/deriving-welfords-method-for-computing-variance/>
///
/// # Panics
///
/// Panics if the given vector of points has less than 2 elements.
fn welfords_mean_variance(points: &[ScoredPoint]) -> (f32, f32) {
    debug_assert!(
        points.len() > 1,
        "Not enough points to calculate mean and variance"
    );

    let mut mean = 0.0;
    let mut aggregate = 0.0;
    for (p, k) in points.iter().zip(1usize..) {
        let old_delta = p.score - mean;
        mean += old_delta / (k as f32);

        let delta = p.score - mean;
        aggregate += old_delta * delta;
    }

    let sample_variance = aggregate / (points.len() as f32 - 1.0);

    (mean, sample_variance)
}

/// Estimates mean and variance to normalize scores between 0 and 1, using three standard deviations as extremes.
pub fn distr_norm(mut points: Vec<ScoredPoint>) -> Vec<ScoredPoint> {
    if points.len() < 2 {
        if points.len() == 1 {
            points[0].score = 0.5;
        }
        return points;
    }

    // Prefetch results are ordered by their source metric, so endpoints reveal score direction.
    let small_better = points
        .first()
        .zip(points.last())
        .is_some_and(|(first, last)| first.score < last.score);
    let (mean, variance) = welfords_mean_variance(&points);

    let std_dev = variance.sqrt();
    let min = mean - 3.0 * std_dev;
    let max = mean + 3.0 * std_dev;

    let mut normalized = norm(points, min, max);
    if small_better {
        normalized
            .iter_mut()
            .for_each(|point| point.score = 1.0 - point.score);
    }

    normalized
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn point(id: usize, score: ScoreType) -> ScoredPoint {
        ScoredPoint {
            id: PointIdType::NumId(id as u64),
            version: 0,
            score,
            payload: None,
            vector: None,
            shard_key: None,
            order_value: None,
        }
    }

    #[test]
    fn dbsf_preserves_small_better_ranking() {
        let seed = vec![point(0, 0.0), point(1, 0.2), point(2, 0.4), point(3, 0.8)];
        let fused_seed = score_fusion(vec![seed.clone(), seed], ScoreFusion::dbsf());
        assert_eq!(
            fused_seed.iter().map(|point| point.id).collect_vec(),
            vec![
                PointIdType::NumId(0),
                PointIdType::NumId(1),
                PointIdType::NumId(2),
                PointIdType::NumId(3),
            ],
        );

        let mutant = vec![point(0, 0.0), point(2, 0.4), point(1, 0.7), point(3, 0.8)];
        let fused_mutant = score_fusion(vec![mutant.clone(), mutant], ScoreFusion::dbsf());
        assert_eq!(
            fused_mutant.iter().map(|point| point.id).collect_vec(),
            vec![
                PointIdType::NumId(0),
                PointIdType::NumId(2),
                PointIdType::NumId(1),
                PointIdType::NumId(3),
            ],
        );
    }

    #[test]
    fn dbsf_preserves_large_better_ranking() {
        let source = vec![point(3, 0.8), point(2, 0.4), point(1, 0.2), point(0, 0.0)];
        let fused = score_fusion(vec![source.clone(), source], ScoreFusion::dbsf());
        assert_eq!(
            fused.iter().map(|point| point.id).collect_vec(),
            vec![
                PointIdType::NumId(3),
                PointIdType::NumId(2),
                PointIdType::NumId(1),
                PointIdType::NumId(0),
            ],
        );
    }

    fn assert_close(a: f32, b: f32) {
        // Choose the more relaxed tolerance, absolute or relative based on the values.
        let abs_tolerance = 1e-5f32;
        let rel_tolerance = 1e-4f32;

        let diff = (a - b).abs();
        let max_val = a.abs().max(b.abs());
        let tolerance = abs_tolerance.max(rel_tolerance * max_val);

        assert!(
            diff <= tolerance,
            "{a} is not close to {b}: difference {diff} exceeds tolerance {tolerance}"
        );
    }

    proptest! {
        #[test]
        fn welford_calc_vs_naive(scores in prop::collection::vec(-100.0..100.0f32, 2..1000)) {
            let naive_mean = scores.iter().sum::<f32>() / scores.len() as f32;
            let naive_variance = scores.iter().map(|p| (p - naive_mean).powi(2)).sum::<f32>()
                / (scores.len() - 1) as f32;

            let points = scores
                .into_iter()
                .enumerate()
                .map(|(i, s)| point(i, s))
                .collect_vec();
            let (mean, variance) = welfords_mean_variance(&points);

            assert_close(mean, naive_mean);
            assert_close(variance, naive_variance);
        }
    }
}
