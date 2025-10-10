use ordered_float::OrderedFloat;
use segment::vector_storage::query::FeedbackPair;

use crate::operations::universal_query::collection_query::ScoredItem;

/// Extracts pairs of points, ranked by score difference in descending order.
///
/// Assumes scoring order is BiggerIsBetter
pub fn extract_feedback_pairs<TVector: Clone>(
    mut feedback: Vec<ScoredItem<TVector>>,
) -> Vec<FeedbackPair<TVector>> {
    feedback.sort_by_key(|item| OrderedFloat(-item.score));

    if feedback.len() < 2 {
        return Vec::new()
    }

    // Pair front and back items until we run out of them
    let mut front_idx = 0;
    let mut back_idx = feedback.len() - 1;

    let mut feedback_pairs = Vec::with_capacity(feedback.len() / 2);

    while front_idx < back_idx {
        let front = &feedback[front_idx];
        let back = &feedback[back_idx];

        let score_diff = front.score - back.score;

        feedback_pairs.push(FeedbackPair {
            positive: front.item.clone(),
            negative: back.item.clone(),
            confidence: score_diff.into(),
        });

        front_idx += 1;
        back_idx -= 1;
    }

    feedback_pairs
}

#[cfg(test)]
mod tests {
    use ordered_float::OrderedFloat;

    use super::extract_feedback_pairs;
    use crate::operations::universal_query::collection_query::ScoredItem;

    #[test]
    fn test_extract_feedback_pairs_empty() {
        let feedback: Vec<ScoredItem<i32>> = vec![];
        let pairs = extract_feedback_pairs(feedback);
        assert!(pairs.is_empty());
    }

    #[test]
    fn test_extract_feedback_pairs_single_item() {
        let feedback = vec![ScoredItem {
            item: 1,
            score: 0.5,
        }];
        let pairs = extract_feedback_pairs(feedback);
        assert!(pairs.is_empty());
    }

    #[test]
    fn test_extract_feedback_pairs_two_items() {
        let feedback = vec![
            ScoredItem {
                item: 1,
                score: 0.8,
            },
            ScoredItem {
                item: 2,
                score: 0.2,
            },
        ];
        let pairs = extract_feedback_pairs(feedback);

        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].positive, 1);
        assert_eq!(pairs[0].negative, 2);
        assert_eq!(pairs[0].confidence, OrderedFloat(0.6));
    }

    #[test]
    fn test_extract_feedback_pairs_even_count() {
        let feedback = vec![
            ScoredItem {
                item: "a",
                score: 0.9,
            },
            ScoredItem {
                item: "b",
                score: 0.6,
            },
            ScoredItem {
                item: "c",
                score: 0.3,
            },
            ScoredItem {
                item: "d",
                score: 0.2,
            },
        ];
        let pairs = extract_feedback_pairs(feedback);

        assert_eq!(pairs.len(), 2);

        // First pair: highest with lowest
        assert_eq!(pairs[0].positive, "a");
        assert_eq!(pairs[0].negative, "d");
        assert_eq!(pairs[0].confidence, OrderedFloat(0.7));

        // Second pair: second highest with second lowest
        assert_eq!(pairs[1].positive, "b");
        assert_eq!(pairs[1].negative, "c");
        assert_eq!(pairs[1].confidence, OrderedFloat(0.3));
    }

    #[test]
    fn test_extract_feedback_pairs_odd_count() {
        let feedback = vec![
            ScoredItem {
                item: 1,
                score: 1.0,
            },
            ScoredItem {
                item: 2,
                score: 0.8,
            },
            ScoredItem {
                item: 3,
                score: 0.5,  // Middle item - should be ignored
            },
            ScoredItem {
                item: 4,
                score: 0.3,
            },
            ScoredItem {
                item: 5,
                score: 0.1,
            },
        ];
        let pairs = extract_feedback_pairs(feedback);

        assert_eq!(pairs.len(), 2);

        // First pair
        assert_eq!(pairs[0].positive, 1);
        assert_eq!(pairs[0].negative, 5);
        assert_eq!(pairs[0].confidence, OrderedFloat(0.9));

        // Second pair
        assert_eq!(pairs[1].positive, 2);
        assert_eq!(pairs[1].negative, 4);
        assert_eq!(pairs[1].confidence, OrderedFloat(0.5));
    }

    #[test]
    fn test_extract_feedback_pairs_unsorted_input() {
        let feedback = vec![
            ScoredItem {
                item: "low",
                score: 0.1,
            },
            ScoredItem {
                item: "high",
                score: 0.9,
            },
            ScoredItem {
                item: "mid",
                score: 0.5,
            },
            ScoredItem {
                item: "very_low",
                score: 0.0,
            },
        ];
        let pairs = extract_feedback_pairs(feedback);

        assert_eq!(pairs.len(), 2);

        // Should be sorted before pairing
        assert_eq!(pairs[0].positive, "high");
        assert_eq!(pairs[0].negative, "very_low");
        assert_eq!(pairs[0].confidence, OrderedFloat(0.9));

        assert_eq!(pairs[1].positive, "mid");
        assert_eq!(pairs[1].negative, "low");
        assert_eq!(pairs[1].confidence, OrderedFloat(0.4));
    }

    #[test]
    fn test_extract_feedback_pairs_negative_scores() {
        let feedback = vec![
            ScoredItem {
                item: 1,
                score: 0.5,
            },
            ScoredItem {
                item: 2,
                score: -0.3,
            },
            ScoredItem {
                item: 3,
                score: -0.8,
            },
            ScoredItem {
                item: 4,
                score: 0.2,
            },
        ];
        let pairs = extract_feedback_pairs(feedback);

        assert_eq!(pairs.len(), 2);

        // First pair: highest positive with lowest negative
        assert_eq!(pairs[0].positive, 1);
        assert_eq!(pairs[0].negative, 3);
        assert_eq!(pairs[0].confidence, OrderedFloat(1.3));

        // Second pair
        assert_eq!(pairs[1].positive, 4);
        assert_eq!(pairs[1].negative, 2);
        assert_eq!(pairs[1].confidence, OrderedFloat(0.5));
    }

    #[test]
    fn test_extract_feedback_pairs_identical_scores() {
        let feedback = vec![
            ScoredItem {
                item: "a",
                score: 0.5,
            },
            ScoredItem {
                item: "b",
                score: 0.5,
            },
            ScoredItem {
                item: "c",
                score: 0.5,
            },
            ScoredItem {
                item: "d",
                score: 0.5,
            },
        ];
        let pairs = extract_feedback_pairs(feedback);

        assert_eq!(pairs.len(), 2);

        // All pairs should have zero confidence since scores are identical
        assert_eq!(pairs[0].confidence, OrderedFloat(0.0));
        assert_eq!(pairs[1].confidence, OrderedFloat(0.0));
    }
}
