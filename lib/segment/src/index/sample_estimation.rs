use std::cmp::{max, min};

use crate::types::PointOffsetType;

const MAX_ESTIMATED_POINTS: usize = 1000;

/// How many points do we need to check in order to estimate expected query cardinality.
/// Based on <https://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval>
#[allow(dead_code)]
fn estimate_required_sample_size(total: usize, confidence_interval: usize) -> usize {
    let confidence_interval = min(confidence_interval, total);
    let z = 1.96; // percentile 0.95 of normal distribution
    let index_fraction = confidence_interval as f64 / total as f64 / 2.0;
    let h = 0.5; // success rate which requires most number of estimations
    let estimated_size = h * (1. - h) / (index_fraction / z).powi(2);
    max(estimated_size as usize, 10)
}

/// Returns (expected cardinality ± confidence interval at 0.99)
/// Based on <https://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval#Agresti%E2%80%93Coull_interval>
fn confidence_agresti_coull_interval(trials: usize, positive: usize, total: usize) -> (i64, i64) {
    let z = 2.; // heuristics
    let n_hat = trials as f64 + z * z;
    let phat = (positive as f64 + z * z / 2.) / n_hat;
    let interval = z * ((phat / n_hat) * (1. - phat)).sqrt();

    let expected = (phat * total as f64) as i64;
    let delta = (interval * total as f64) as i64;
    (expected, delta)
}

/// Tests if given `query` have cardinality higher than the `threshold`
/// Iteratively samples points until the decision could be made with confidence
pub fn sample_check_cardinality(
    sample_points: impl Iterator<Item = PointOffsetType>,
    checker: impl Fn(PointOffsetType) -> bool,
    threshold: usize,
    total_points: usize,
) -> bool {
    let mut matched_points = 0;
    let mut total_checked = 0;

    let mut exp = 0;
    let mut interval;
    for idx in sample_points.take(MAX_ESTIMATED_POINTS) {
        matched_points += checker(idx) as usize;
        total_checked += 1;

        let estimation =
            confidence_agresti_coull_interval(total_checked, matched_points, total_points);
        exp = estimation.0;
        interval = estimation.1;

        if exp - interval > threshold as i64 {
            return true;
        }

        if exp + interval < threshold as i64 {
            return false;
        }
    }

    exp > threshold as i64
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    #[test]
    fn test_confidence_interval() {
        let mut rng = StdRng::seed_from_u64(42);
        let total = 100_000;
        let true_p = 0.25;

        let mut delta = 100_000;
        let mut positive = 0;
        for i in 1..=101 {
            positive += rng.gen_bool(true_p) as usize;
            if i % 20 == 1 {
                let interval = confidence_agresti_coull_interval(i, positive, total);
                assert!(interval.1 < delta);
                delta = interval.1;
                eprintln!(
                    "confidence_agresti_coull_interval({i}, {positive}, {total}) = {interval:#?}"
                );
            }
        }
    }

    #[test]
    fn test_sample_check_cardinality() {
        let res = sample_check_cardinality(
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].into_iter(),
            |idx| idx % 2 == 0,
            10_000,
            100_000,
        );

        assert!(res)
    }
}
