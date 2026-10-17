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
    /// Order for each input list of scores (default: LargeBetter)
    pub source_orders: Vec<Order>,
}

impl ScoreFusion {
    /// Params for the distribution-based score fusion
    pub fn dbsf() -> Self {
        Self {
            method: Aggregation::Sum,
            norm: Normalization::Distr,
            weights: vec![],
            order: Order::LargeBetter,
            source_orders: vec![],
        }
    }

    pub fn dbsf_with_orders(source_orders: Vec<Order>) -> Self {
        Self {
            method: Aggregation::Sum,
            norm: Normalization::Distr,
            weights: vec![],
            order: Order::LargeBetter,
            source_orders,
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
        source_orders,
    } = params;

    let weights = weights.into_iter().chain(iter::repeat(1.0));

    all_results
        .into_iter()
        .enumerate()
        .map(|(idx, points)| {
            let src_order = source_orders.get(idx).copied().unwrap_or_else(|| {
                // If order was not explicitly specified, infer from the input order of points.
                // Qdrant search results are always ordered from best to worst.
                // If the first point has a smaller score than the last point, smaller is better (e.g. Euclidean / Manhattan).
                if let (Some(first), Some(last)) = (points.first(), points.last()) {
                    if first.score < last.score {
                        Order::SmallBetter
                    } else {
                        Order::LargeBetter
                    }
                } else {
                    Order::LargeBetter
                }
            });
            match norm {
                Normalization::MinMax => min_max_norm(points, src_order),
                Normalization::Distr => distr_norm(points, src_order),
            }
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
fn norm(
    mut points: Vec<ScoredPoint>,
    min: ScoreType,
    max: ScoreType,
    order: Order,
) -> Vec<ScoredPoint> {
    // Protect against division by zero
    if min == max {
        points.iter_mut().for_each(|p| p.score = 0.5);
        return points;
    }

    match order {
        Order::LargeBetter => {
            points.iter_mut().for_each(|p| {
                p.score = (p.score - min) / (max - min);
            });
        }
        Order::SmallBetter => {
            points.iter_mut().for_each(|p| {
                p.score = (max - p.score) / (max - min);
            });
        }
    }

    points
}

pub fn min_max_norm(points: Vec<ScoredPoint>, order: Order) -> Vec<ScoredPoint> {
    let (min, max) = match points.iter().map(|p| OrderedFloat(p.score)).minmax() {
        MinMaxResult::NoElements | MinMaxResult::OneElement(_) => return points,
        MinMaxResult::MinMax(min, max) => (min.0, max.0),
    };

    norm(points, min, max, order)
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

/// Estimates the mean and variance of the given points and normalizes them between 0.0 and 1.0, using the 3rd
/// standard deviation as extremes.
pub fn distr_norm(mut points: Vec<ScoredPoint>, order: Order) -> Vec<ScoredPoint> {
    if points.len() < 2 {
        if points.len() == 1 {
            points[0].score = 0.5;
        }
        return points;
    }

    let (mean, variance) = welfords_mean_variance(&points);

    let std_dev = variance.sqrt();
    let min = mean - 3.0 * std_dev;
    let max = mean + 3.0 * std_dev;

    norm(points, min, max, order)
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

    #[test]
    fn test_dbsf_euclid_identical_retrievers() {
        // Reproduces Issue #10611: Fusing identical Euclidean retrievers must preserve ranking
        let r1 = vec![point(0, 0.0), point(1, 0.2), point(2, 0.4), point(3, 0.8)];
        let r2 = r1.clone();

        // 1. With explicit source orders
        let fused_explicit = score_fusion(
            vec![r1.clone(), r2.clone()],
            ScoreFusion::dbsf_with_orders(vec![Order::SmallBetter, Order::SmallBetter]),
        );
        let ids_explicit: Vec<u64> = fused_explicit
            .into_iter()
            .map(|p| match p.id {
                PointIdType::NumId(id) => id,
                _ => panic!("Expected NumId"),
            })
            .collect();
        assert_eq!(ids_explicit, vec![0, 1, 2, 3]);

        // 2. With inferred source orders (fallback when not explicitly provided)
        let fused_inferred = score_fusion(vec![r1, r2], ScoreFusion::dbsf());
        let ids_inferred: Vec<u64> = fused_inferred
            .into_iter()
            .map(|p| match p.id {
                PointIdType::NumId(id) => id,
                _ => panic!("Expected NumId"),
            })
            .collect();
        assert_eq!(ids_inferred, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_dbsf_small_better_ordering() {
        let points = vec![point(0, 0.1), point(1, 0.5), point(2, 0.9)];

        let fused = score_fusion(
            vec![points],
            ScoreFusion::dbsf_with_orders(vec![Order::SmallBetter]),
        );

        assert_eq!(fused.len(), 3);
        assert!(fused[0].score > fused[1].score);
        assert!(fused[1].score > fused[2].score);
        assert_eq!(fused[0].id, PointIdType::NumId(0));
        assert_eq!(fused[1].id, PointIdType::NumId(1));
        assert_eq!(fused[2].id, PointIdType::NumId(2));
    }

    #[test]
    fn test_dbsf_hybrid_large_and_small_better() {
        // Source 1 (e.g. Cosine): larger is better
        let r_cosine = vec![point(0, 0.95), point(1, 0.50), point(2, 0.10)];
        // Source 2 (e.g. Euclid): smaller is better
        let r_euclid = vec![point(0, 0.05), point(1, 0.40), point(2, 0.90)];

        let fused = score_fusion(
            vec![r_cosine, r_euclid],
            ScoreFusion::dbsf_with_orders(vec![Order::LargeBetter, Order::SmallBetter]),
        );

        let ids: Vec<u64> = fused
            .into_iter()
            .map(|p| match p.id {
                PointIdType::NumId(id) => id,
                _ => panic!("Expected NumId"),
            })
            .collect();
        assert_eq!(ids, vec![0, 1, 2]);
    }

    #[test]
    fn test_min_max_norm_small_better() {
        let points = vec![point(0, 1.0), point(1, 3.0), point(2, 5.0)];

        let normalized = min_max_norm(points, Order::SmallBetter);
        assert_close(normalized[0].score, 1.0); // closest -> 1.0
        assert_close(normalized[1].score, 0.5); // middle -> 0.5
        assert_close(normalized[2].score, 0.0); // farthest -> 0.0
    }
}
