//! Reciprocal Rank Fusion (RRF) is a method for combining rankings from multiple sources.
//! See https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf

use std::collections::hash_map::Entry;

use ahash::{HashMap, HashMapExt};
use ordered_float::OrderedFloat;

use crate::types::{ExtendedPointId, ScoredPoint};

/// Mitigates the impact of high rankings by outlier systems
const RFF_RANKING_K: f32 = 2.0;

/// Compute the RRF score for a given position.
fn position_score(position: usize) -> f32 {
    1.0 / (position as f32 + RFF_RANKING_K)
}

/// Compute RRF scores for multiple results from different sources.
/// Each response can have a different length.
/// The input scores are irrelevant, only the order matters.
///
/// The output is a single sorted list of ScoredPoint.
/// Does not break ties.
pub fn rrf_scoring(responses: Vec<Vec<ScoredPoint>>, limit: usize) -> Vec<ScoredPoint> {
    // track scored points by id
    let mut points_by_id: HashMap<ExtendedPointId, ScoredPoint> = HashMap::new();

    for response in responses {
        for (pos, mut point) in response.into_iter().enumerate() {
            let rrf_score = position_score(pos);
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

    let mut scores = points_by_id.into_iter().collect::<Vec<_>>();
    scores.sort_unstable_by(|a, b| {
        // sort by score descending
        OrderedFloat(b.1.score).cmp(&OrderedFloat(a.1.score))
    });

    // materialized updated scored points
    scores.into_iter().take(limit).map(|(_, v)| v).collect()
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
        }
    }

    #[test]
    fn test_rrf_scoring_empty() {
        let responses = vec![];
        let scored_points = rrf_scoring(responses, 10);
        assert_eq!(scored_points.len(), 0);
    }

    #[test]
    fn test_rrf_scoring_one() {
        let responses = vec![vec![make_scored_point(1, 0.9)]];
        let scored_points = rrf_scoring(responses, 10);
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
        let scored_points = rrf_scoring(responses.clone(), 10);
        assert_eq!(scored_points.len(), 4);
        // assert that the list is sorted
        assert!(scored_points.windows(2).all(|w| w[0].score >= w[1].score));

        // top 1
        let scored_points = rrf_scoring(responses.clone(), 1);
        assert_eq!(scored_points.len(), 1);
        assert_eq!(scored_points[0].id, 1.into());
        assert_eq!(scored_points[0].score, 1.0833334);

        // top 2
        let scored_points = rrf_scoring(responses.clone(), 2);
        assert_eq!(scored_points.len(), 2);
        assert_eq!(scored_points[0].id, 1.into());
        assert_eq!(scored_points[0].score, 1.0833334);

        assert_eq!(scored_points[1].id, 2.into());
        assert_eq!(scored_points[1].score, 0.8333334);

        // top 3
        let scored_points = rrf_scoring(responses.clone(), 3);
        assert_eq!(scored_points.len(), 3);
        assert_eq!(scored_points[0].id, 1.into());
        assert_eq!(scored_points[0].score, 1.0833334);

        assert_eq!(scored_points[1].id, 2.into());
        assert_eq!(scored_points[1].score, 0.8333334);

        assert_eq!(scored_points[2].id, 3.into());
        assert_eq!(scored_points[2].score, 0.5833334);

        // top 4
        let scored_points = rrf_scoring(responses, 4);
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
}
