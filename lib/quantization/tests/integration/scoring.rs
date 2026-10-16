use common::types::PointOffsetType;
use quantization::EncodedVectors;

/// Scores one point through the batched [`EncodedVectors::score_points`].
pub trait ScorePoint: EncodedVectors {
    fn score_point(&self, query: &Self::EncodedQuery, id: PointOffsetType) -> f32 {
        let mut score = [0.0];
        self.score_points(query, &[id], &mut score).unwrap();
        score[0]
    }
}

impl<T: EncodedVectors> ScorePoint for T {}
