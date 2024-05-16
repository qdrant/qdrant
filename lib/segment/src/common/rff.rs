use std::collections::hash_map::Entry;

use ahash::{HashMap, HashMapExt};
use ordered_float::OrderedFloat;

use crate::types::{ExtendedPointId, ScoredPoint};

/// Reciprocal Rankings Fusion (RRF) is a method for combining rankings from multiple sources.
/// See https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf

/// Mitigates the impact of high rankings by outlier systems
const RFF_RANKING_K: f32 = 2.0;

/// Compute RFF scores for multiple results from different sources.
/// Each response can have a different length.
/// Each response must be sorted by score in descending order.
///
/// The output is a single sorted list of ScoredPoint.
/// Does not break ties.
#[allow(dead_code)]
pub fn rff_scoring(responses: Vec<Vec<ScoredPoint>>, limit: usize) -> Vec<ScoredPoint> {
    // ensure each input is sorted
    debug_assert!(responses
        .iter()
        .all(|r| r.windows(2).all(|w| w[0].score >= w[1].score)));
    fn position_score(position: usize) -> f32 {
        1.0 / (position as f32 + RFF_RANKING_K)
    }

    // track RFF scores per point id
    let mut scores_by_id: HashMap<ExtendedPointId, f32> = HashMap::new();

    // track scored points by id
    let mut points_by_id: HashMap<ExtendedPointId, ScoredPoint> = HashMap::new();

    for response in responses {
        for (pos, score) in response.into_iter().enumerate() {
            let rff_score = position_score(pos);
            match scores_by_id.entry(score.id) {
                Entry::Occupied(mut entry) => {
                    // accumulate score
                    *entry.get_mut() += rff_score;
                }
                Entry::Vacant(entry) => {
                    // init score
                    entry.insert(rff_score);
                    // track point
                    points_by_id.insert(score.id, score);
                }
            }
        }
    }

    // sort by tracked scores
    let mut scores = scores_by_id.into_iter().collect::<Vec<_>>();
    scores.sort_by(|a, b| OrderedFloat(b.1).cmp(&OrderedFloat(a.1)));

    // discard lower scores
    scores.truncate(limit);

    // materialized updated scored points
    let mut results = Vec::with_capacity(limit);
    for (id, score) in scores {
        let mut point = points_by_id.remove(&id).expect("missing point");
        // update score
        point.score = score;
        results.push(point);
    }
    results
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
    fn test_rff_scoring_empty() {
        let responses = vec![];
        let scored_points = rff_scoring(responses, 10);
        assert_eq!(scored_points.len(), 0);
    }

    #[test]
    fn test_rff_scoring_one() {
        let responses = vec![vec![make_scored_point(1, 0.9)]];
        let scored_points = rff_scoring(responses, 10);
        assert_eq!(scored_points.len(), 1);
        assert_eq!(scored_points[0].id, 1.into());
        assert_eq!(scored_points[0].score, 0.5); // 1 / (0 + 2)
    }

    #[test]
    fn test_rff_scoring() {
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
        let scored_points = rff_scoring(responses.clone(), 10);
        assert_eq!(scored_points.len(), 4);
        // assert that the list is sorted
        assert!(scored_points.windows(2).all(|w| w[0].score >= w[1].score));

        // top 1
        let scored_points = rff_scoring(responses.clone(), 1);
        assert_eq!(scored_points.len(), 1);
        assert_eq!(scored_points[0].id, 1.into());
        assert_eq!(scored_points[0].score, 1.0833334);

        // top 2
        let scored_points = rff_scoring(responses.clone(), 2);
        assert_eq!(scored_points.len(), 2);
        assert_eq!(scored_points[0].id, 1.into());
        assert_eq!(scored_points[0].score, 1.0833334);

        assert_eq!(scored_points[1].id, 2.into());
        assert_eq!(scored_points[1].score, 0.8333334);

        // top 3
        let scored_points = rff_scoring(responses.clone(), 3);
        assert_eq!(scored_points.len(), 3);
        assert_eq!(scored_points[0].id, 1.into());
        assert_eq!(scored_points[0].score, 1.0833334);

        assert_eq!(scored_points[1].id, 2.into());
        assert_eq!(scored_points[1].score, 0.8333334);

        assert_eq!(scored_points[2].id, 3.into());
        assert_eq!(scored_points[2].score, 0.5833334);

        // top 4
        let scored_points = rff_scoring(responses, 4);
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
