//! Balanced Log-Odds (BLO) fusion combines scores from multiple retrievers using
//! sigmoid score-to-probability conversion, logit transform, and min-max normalization.
//!
//! The approach:
//! 1. Auto-calibrate sigmoid parameters (alpha, beta) per retriever from score distribution
//! 2. Convert raw scores to logit space: logit(sigmoid(alpha * (s - beta))) = alpha * (s - beta)
//! 3. Min-max normalize logits within each retriever to [0, 1]
//! 4. Combine via weighted mean over participating retrievers (missing docs are skipped)

use std::collections::hash_map::Entry;

use ahash::AHashMap;
use itertools::Either;
use ordered_float::OrderedFloat;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::{ExtendedPointId, ScoredPoint};

const LOGIT_MIN_MAX_EPSILON: f64 = 1e-12;

/// Compute the standard deviation of a slice of f64 values.
/// Returns 0.0 if fewer than 2 elements.
fn std_dev(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0);
    variance.sqrt()
}

/// Compute the median of a mutable slice of f64 values.
/// The slice will be partially reordered.
/// Returns 0.0 if empty.
fn median(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mid = values.len() / 2;
    values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

/// Balanced Log-Odds fusion.
///
/// For each retriever source:
///   - Compute beta = median(scores), alpha = 1/std(scores) (or 1.0 if std == 0)
///   - Transform each score to logit space: alpha * (score - beta)
///   - Min-max normalize logits within this source to [0, 1]
///
/// For each document in the union of all sources:
///   - Compute weighted mean of normalized logits over participating sources only
///
/// Returns results sorted descending by fused score.
pub fn blo_fusion(
    responses: Vec<Vec<ScoredPoint>>,
    weights: Option<&[f32]>,
) -> OperationResult<Vec<ScoredPoint>> {
    let weights = if let Some(weights) = weights {
        if weights.len() != responses.len() {
            return Err(OperationError::validation_error(format!(
                "Number of weights in BLO should match number of pre-fetches: got {}, expected {}",
                weights.len(),
                responses.len()
            )));
        }
        Either::Left(weights.iter().copied())
    } else {
        Either::Right(std::iter::repeat(1.0f32))
    };

    // Per-document accumulator: (weighted_sum, total_weight, representative ScoredPoint)
    let mut acc: AHashMap<ExtendedPointId, (f64, f64, ScoredPoint)> = AHashMap::new();

    for (response, weight) in responses.into_iter().zip(weights) {
        if response.is_empty() || weight <= 0.0 {
            continue;
        }

        let w = weight as f64;

        // Collect raw scores as f64 for statistical computation
        let mut raw_scores: Vec<f64> = response.iter().map(|p| p.score as f64).collect();

        // Compute stats before median mutates the array
        let sd = std_dev(&raw_scores);
        let alpha = if sd < f64::EPSILON { 1.0 } else { 1.0 / sd };
        let beta = median(&mut raw_scores);

        // Transform to logit space: alpha * (score - beta), using original point scores
        let logits: Vec<f64> = response
            .iter()
            .map(|p| alpha * (p.score as f64 - beta))
            .collect();

        // Min-max normalize logits
        let lo = logits
            .iter()
            .copied()
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(0.0);
        let hi = logits
            .iter()
            .copied()
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(0.0);

        let range = hi - lo;
        let can_normalize = range > LOGIT_MIN_MAX_EPSILON;

        for (mut point, logit) in response.into_iter().zip(logits.iter()) {
            let normalized = if can_normalize {
                (logit - lo) / range
            } else {
                0.5 // All scores identical in this source
            };

            match acc.entry(point.id) {
                Entry::Occupied(mut entry) => {
                    let (sum, tw, _) = entry.get_mut();
                    *sum += w * normalized;
                    *tw += w;
                }
                Entry::Vacant(entry) => {
                    point.score = 0.0; // Will be overwritten
                    entry.insert((w * normalized, w, point));
                }
            }
        }
    }

    let mut results: Vec<ScoredPoint> = acc
        .into_values()
        .map(|(sum, tw, mut point)| {
            point.score = if tw > 0.0 { (sum / tw) as f32 } else { 0.0 };
            point
        })
        .collect();

    results.sort_unstable_by(|a, b| OrderedFloat(b.score).cmp(&OrderedFloat(a.score)));

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ScoredPoint;

    fn make_point(id: u64, score: f32) -> ScoredPoint {
        ScoredPoint {
            id: id.into(),
            version: 0,
            score,
            payload: None,
            vector: None,
            shard_key: None,
            order_value: None,
        }
    }

    #[test]
    fn test_blo_empty() {
        let result = blo_fusion(vec![], None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_blo_single_source() {
        let responses = vec![vec![
            make_point(1, 0.9),
            make_point(2, 0.5),
            make_point(3, 0.1),
        ]];
        let result = blo_fusion(responses, None).unwrap();
        assert_eq!(result.len(), 3);
        // Should be sorted descending
        assert!(result[0].score >= result[1].score);
        assert!(result[1].score >= result[2].score);
        // Point 1 had the highest raw score
        assert_eq!(result[0].id, 1.into());
    }

    #[test]
    fn test_blo_two_sources_overlap() {
        let responses = vec![
            vec![make_point(1, 0.9), make_point(2, 0.5)],
            vec![make_point(2, 0.8), make_point(3, 0.3)],
        ];
        let result = blo_fusion(responses, None).unwrap();
        assert_eq!(result.len(), 3);
        // Point 2 appears in both sources, should rank higher than point 3
        let p2 = result.iter().find(|p| p.id == 2.into()).unwrap();
        let p3 = result.iter().find(|p| p.id == 3.into()).unwrap();
        assert!(p2.score > p3.score);
    }

    #[test]
    fn test_blo_weights_length_mismatch() {
        let responses = vec![vec![make_point(1, 0.9)], vec![make_point(2, 0.9)]];
        let weights = [1.0, 2.0, 3.0];
        let result = blo_fusion(responses, Some(&weights));
        assert!(result.is_err());
    }

    #[test]
    fn test_blo_zero_weight_source_ignored() {
        let responses = vec![vec![make_point(1, 0.9)], vec![make_point(2, 0.9)]];
        let weights = [1.0, 0.0];
        let result = blo_fusion(responses, Some(&weights)).unwrap();
        // Point 2 from source with weight=0 should still appear but with score from
        // only zero-weight source, so it's skipped entirely
        let p2 = result.iter().find(|p| p.id == 2.into());
        assert!(p2.is_none());
    }

    #[test]
    fn test_blo_identical_scores_in_source() {
        // All scores the same -> std=0 -> all normalized to 0.5
        let responses = vec![vec![
            make_point(1, 0.5),
            make_point(2, 0.5),
            make_point(3, 0.5),
        ]];
        let result = blo_fusion(responses, None).unwrap();
        assert_eq!(result.len(), 3);
        for p in &result {
            assert!((p.score - 0.5).abs() < 1e-5);
        }
    }

    #[test]
    fn test_blo_single_element_source() {
        // Single element: std=0, normalized to 0.5
        let responses = vec![vec![make_point(1, 42.0)]];
        let result = blo_fusion(responses, None).unwrap();
        assert_eq!(result.len(), 1);
        assert!((result[0].score - 0.5).abs() < 1e-5);
    }

    #[test]
    fn test_blo_weighted_fusion() {
        // Two sources with different weights
        // Source 1 (weight 3): point A=high, point B=low
        // Source 2 (weight 1): point B=high, point A=low
        // With 3:1 weighting, point A should rank higher
        let responses = vec![
            vec![make_point(1, 0.9), make_point(2, 0.1)],
            vec![make_point(2, 0.9), make_point(1, 0.1)],
        ];
        let weights = [3.0, 1.0];
        let result = blo_fusion(responses, Some(&weights)).unwrap();
        assert_eq!(result[0].id, 1.into());
        assert!(result[0].score > result[1].score);
    }

    #[test]
    fn test_median_odd() {
        let mut v = vec![3.0, 1.0, 2.0];
        assert_eq!(median(&mut v), 2.0);
    }

    #[test]
    fn test_median_even() {
        let mut v = vec![4.0, 1.0, 3.0, 2.0];
        assert_eq!(median(&mut v), 2.5);
    }

    #[test]
    fn test_std_dev_basic() {
        let v = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let sd = std_dev(&v);
        assert!((sd - 2.138).abs() < 0.01);
    }
}
