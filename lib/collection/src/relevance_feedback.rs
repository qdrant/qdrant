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
