//! Reciprocal Rank Fusion (RRF) is a method for combining rankings from multiple sources.
//! See <https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf>

use std::collections::hash_map::Entry;

use ahash::AHashMap;
use itertools::Either;
use ordered_float::OrderedFloat;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::{ExtendedPointId, ScoredPoint};

/// Mitigates the impact of high rankings by outlier systems
pub const DEFAULT_RRF_K: usize = 2;

/// Compute the RRF score for a given position with optional weight.
///
/// The formula is: `1.0 / ((position + 1) as f32 / weight + k as f32 - 1.0)`
///
/// With weight=1.0 (default), this becomes the standard RRF formula: `1.0 / (position + k)`
///
/// Higher or lower weight means the positions are "compressed" or "stretched":
///
/// weight=3.0 is equivalent to dividing the position by 3,
///            so element at pos 2 contributes like pos 0 would with weight=1.0.
///
/// `(position + 1)` accounts for 0-based indexing,
///  so weight affects the score of the top-ranked item (pos=0) as well.
///
/// This means a 3:1 weight ratio is equivalent to "for each 3 results of first prefetch,
/// have one result of second".
fn position_score(position: usize, k: usize, weight: f32) -> f32 {
    // Avoid division by zero - if weight is 0, treat as negligible contribution
    if weight <= 0.0 {
        return 0.0;
    }

    1.0 / ((position + 1) as f32 / weight + k as f32 - 1.0)
}

/// Compute RRF scores for multiple results from different sources.
/// Each response can have a different length.
/// The input scores are irrelevant, only the order matters.
///
/// # Arguments
/// * `responses` - Iterator of response vectors from different sources
/// * `k` - The RRF K parameter (default is 2)
/// * `weights` - Optional weights for each source. If provided, must match the number of sources.
///   Higher weight = more influence on final ranking.
///   If None, all sources are weighted equally (weight = 1.0).
///
/// The output is a single sorted list of ScoredPoint.
/// Does not break ties.
pub fn rrf_scoring(
    responses: Vec<Vec<ScoredPoint>>,
    k: usize,
    weights: Option<&[f32]>,
) -> OperationResult<Vec<ScoredPoint>> {
    // track scored points by id
    let mut points_by_id: AHashMap<ExtendedPointId, ScoredPoint> = AHashMap::new();

    let weights = if let Some(weights) = weights {
        if weights.len() != responses.len() {
            return Err(OperationError::validation_error(format!(
                "Number of weights in RRF should match number of pre-fetches: got {}, expected {}",
                weights.len(),
                responses.len()
            )));
        }
        Either::Left(weights.iter().copied())
    } else {
        Either::Right(std::iter::repeat(1.0f32))
    };

    for (response, weight) in responses.into_iter().zip(weights) {
        for (pos, mut point) in response.into_iter().enumerate() {
            let rrf_score = position_score(pos, k, weight);
            match points_by_id.entry(point.id) {
                Entry::Occupied(mut entry) => {
                    // accumulate score
                    entry.get_mut().score += rrf_score;
                }
                Entry::Vacant(entry) => {
                    point.score = rrf_score;
                    // init score
                    entry.insert(point);
                }
            }
        }
    }

    let mut scores: Vec<_> = points_by_id.into_values().collect();
    scores.sort_unstable_by(|a, b| {
        // sort by score descending
        OrderedFloat(b.score).cmp(&OrderedFloat(a.score))
    });

    Ok(scores)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ScoredPoint;

    fn make_scored_point(id: u64, score: f32) -> ScoredPoint {
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
    fn test_rrf_scoring_empty() {
        let responses = vec![];
        let scored_points = rrf_scoring(responses, DEFAULT_RRF_K, None).unwrap();
        assert_eq!(scored_points.len(), 0);
    }

    #[test]
    fn test_rrf_scoring_one() {
        let responses = vec![vec![make_scored_point(1, 0.9)]];
        let scored_points = rrf_scoring(responses, DEFAULT_RRF_K, None).unwrap();
        assert_eq!(scored_points.len(), 1);
        assert_eq!(scored_points[0].id, 1.into());
        assert_eq!(scored_points[0].score, 0.5); // 1 / (0 + 2)
    }

    #[test]
    fn test_rrf_scoring() {
        let responses = vec![
            vec![make_scored_point(2, 0.9), make_scored_point(1, 0.8)],
            vec![
                make_scored_point(1, 0.7),
                make_scored_point(2, 0.6),
                make_scored_point(3, 0.5),
            ],
            vec![
                make_scored_point(5, 0.9),
                make_scored_point(3, 0.5),
                make_scored_point(1, 0.4),
            ],
        ];

        // top 10
        let scored_points = rrf_scoring(responses, DEFAULT_RRF_K, None).unwrap();
        assert_eq!(scored_points.len(), 4);
        // assert that the list is sorted
        assert!(scored_points.windows(2).all(|w| w[0].score >= w[1].score));

        assert_eq!(scored_points.len(), 4);
        assert_eq!(scored_points[0].id, 1.into());
        assert_eq!(scored_points[0].score, 1.0833334);

        assert_eq!(scored_points[1].id, 2.into());
        assert_eq!(scored_points[1].score, 0.8333334);

        assert_eq!(scored_points[2].id, 3.into());
        assert_eq!(scored_points[2].score, 0.5833334);

        assert_eq!(scored_points[3].id, 5.into());
        assert_eq!(scored_points[3].score, 0.5);
    }

    #[test]
    fn test_rrf_scoring_weighted() {
        // Two sources: first with weight 3, second with weight 1
        // This should give 3x more influence to the first source
        let responses = vec![
            vec![make_scored_point(1, 0.9), make_scored_point(2, 0.8)],
            vec![make_scored_point(2, 0.9), make_scored_point(1, 0.8)],
        ];

        // Without weights - both equal
        let scored_points = rrf_scoring(responses.clone(), DEFAULT_RRF_K, None).unwrap();
        assert_eq!(scored_points[0].score, scored_points[1].score);

        // With weights [3.0, 1.0] - first source has 3x weight
        // Higher weight means positions are "compressed" - position N with weight W
        // contributes like position N/W would with weight 1.
        let weights = [3.0, 1.0];
        let scored_points = rrf_scoring(responses, DEFAULT_RRF_K, Some(&weights)).unwrap();

        // Point 2 scores higher because:
        // - Being at pos 1 in high-weight source (w=3) costs less (effective pos = 1/3)
        // - Being at pos 0 in low-weight source still gives full 1/k score
        // So the weighted RRF favors items that rank well across sources,
        // with higher-weight sources having their position penalties reduced.
        assert!(scored_points[0].id == 2.into());
        assert!(scored_points[0].score > scored_points[1].score);
    }

    #[test]
    fn test_rrf_scoring_weighted_ratio() {
        // Test that weight ratio of 3:1 means position 3 in source 1 equals position 1 in source 2
        let k = 60; // Use higher k for clearer demonstration

        // Source 1: item A at position 0, item B at position 3
        // Source 2: item B at position 0, item A at position 1
        let responses = vec![
            vec![
                make_scored_point(11, 0.0),
                make_scored_point(12, 0.0),
                make_scored_point(13, 0.0),
                make_scored_point(14, 0.0),
                make_scored_point(15, 0.0),
                make_scored_point(16, 0.0),
                make_scored_point(17, 0.0),
                make_scored_point(18, 0.0),
            ],
            vec![
                make_scored_point(21, 0.0),
                make_scored_point(22, 0.0),
                make_scored_point(23, 0.0),
                make_scored_point(24, 0.0),
                make_scored_point(25, 0.0),
                make_scored_point(26, 0.0),
                make_scored_point(27, 0.0),
                make_scored_point(28, 0.0),
            ],
        ];

        let weights = [3.0, 1.0];
        let scored_points = rrf_scoring(responses, k, Some(&weights)).unwrap();

        // Check that points from the first group appear 3 times more frequently in the top ranks than points from the second group
        let top_10 = &scored_points[..10];
        let count_source_1 = top_10
            .iter()
            .filter(|p| p.id.as_u64() >= 10 && p.id.as_u64() < 20)
            .count();
        let count_source_2 = top_10
            .iter()
            .filter(|p| p.id.as_u64() >= 20 && p.id.as_u64() < 30)
            .count();

        // With a 3:1 weight ratio, we expect the count of source 1 items in the top 10 to be roughly 3 times that of source 2
        assert!(count_source_1 >= 2 * count_source_2); // Allow some variance due to tie-breaking and small sample size
    }

    #[test]
    fn test_rrf_scoring_weights_length_mismatch() {
        let responses = vec![
            vec![make_scored_point(1, 0.9)],
            vec![make_scored_point(2, 0.9)],
        ];

        // 3 weights for 2 responses should fail
        let weights = [1.0, 2.0, 3.0];
        let result = rrf_scoring(responses.clone(), DEFAULT_RRF_K, Some(&weights));
        assert!(result.is_err());

        // 1 weight for 2 responses should fail
        let weights = [1.0];
        let result = rrf_scoring(responses, DEFAULT_RRF_K, Some(&weights));
        assert!(result.is_err());
    }

    #[test]
    fn test_rrf_scoring_zero_weight() {
        // Test that zero weight source contributes nothing
        let responses = vec![
            vec![make_scored_point(1, 0.9)],
            vec![make_scored_point(2, 0.9)],
        ];

        let weights = [1.0, 0.0];
        let scored_points = rrf_scoring(responses, DEFAULT_RRF_K, Some(&weights)).unwrap();

        // Only point 1 should have a score, point 2 should have 0
        let p1 = scored_points.iter().find(|p| p.id == 1.into()).unwrap();
        let p2 = scored_points.iter().find(|p| p.id == 2.into()).unwrap();

        assert_eq!(p1.score, 0.5); // 1/(0+2)
        assert_eq!(p2.score, 0.0); // zero weight
    }
}
