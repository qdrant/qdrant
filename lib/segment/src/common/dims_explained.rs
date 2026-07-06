//! Per-dimension score explanation for dense vectors.
//!
//! Decomposes the similarity (or distance) between a query vector and a point vector
//! into per-dimension terms, such that the sum of all terms equals the score reported
//! to the user (for Euclid, the sum equals the *squared* distance).
//!
//! | Distance  | Per-dimension term    | Sum of terms          |
//! |-----------|-----------------------|-----------------------|
//! | Dot       | `q_i * v_i`           | score                 |
//! | Cosine    | `q̂_i * v̂_i`           | score                 |
//! | Euclid    | `(q_i - v_i)^2`       | score^2               |
//! | Manhattan | `abs(q_i - v_i)`      | score                 |
//!
//! For Cosine, both vectors are L2-normalized before computing the terms. This is a
//! no-op for vectors which are already stored normalized.

use common::types::ScoreType;
use ordered_float::OrderedFloat;

use crate::data_types::vectors::{DenseVector, VectorElementType};
use crate::spaces::simple::cosine_preprocess;
use crate::types::Distance;

/// Dimension index within a dense vector.
pub type DimId = u32;

/// Top contributing dimensions with their contribution to the score.
/// Sorted by descending absolute contribution.
pub type DimsExplained = Vec<(DimId, ScoreType)>;

/// Computes per-dimension score contributions of dense vectors against a fixed query vector.
#[derive(Debug, Clone)]
pub struct DimsExplainedCalculator {
    /// Query vector, L2-normalized in case of Cosine distance.
    query: DenseVector,
    distance: Distance,
}

impl DimsExplainedCalculator {
    pub fn new(query: DenseVector, distance: Distance) -> Self {
        let query = match distance {
            // Normalizing is a no-op if the vector is already normalized.
            Distance::Cosine => cosine_preprocess(query),
            Distance::Euclid | Distance::Dot | Distance::Manhattan => query,
        };
        Self { query, distance }
    }

    pub fn query_len(&self) -> usize {
        self.query.len()
    }

    /// Per-dimension term of the score decomposition for a single dimension.
    #[inline]
    fn term(&self, query_value: ScoreType, point_value: ScoreType) -> ScoreType {
        match self.distance {
            Distance::Cosine | Distance::Dot => query_value * point_value,
            Distance::Euclid => (query_value - point_value).powi(2),
            Distance::Manhattan => (query_value - point_value).abs(),
        }
    }

    /// For Cosine distance the point vector must be normalized before computing terms.
    ///
    /// Stored vectors are normalized at insertion time for `f32` storages, in which case
    /// this is a no-op. Returns `None` if no normalization is needed.
    fn normalized_point(&self, point: &[VectorElementType]) -> Option<DenseVector> {
        match self.distance {
            Distance::Cosine => Some(cosine_preprocess(point.to_vec())),
            Distance::Euclid | Distance::Dot | Distance::Manhattan => None,
        }
    }

    /// Returns the top `top` dimensions by absolute contribution to the score,
    /// sorted by descending absolute contribution.
    pub fn top_contributions(&self, point: &[VectorElementType], top: usize) -> DimsExplained {
        let normalized;
        let point = match self.normalized_point(point) {
            Some(vector) => {
                normalized = vector;
                normalized.as_slice()
            }
            None => point,
        };

        let mut contributions: Vec<(DimId, ScoreType)> = self
            .query
            .iter()
            .zip(point)
            .enumerate()
            .map(|(dim, (&query_value, &point_value))| {
                (dim as DimId, self.term(query_value, point_value))
            })
            .collect();

        let top = top.min(contributions.len());
        if top < contributions.len() {
            contributions
                .select_nth_unstable_by_key(top, |(_, term)| std::cmp::Reverse(OrderedFloat(term.abs())));
            contributions.truncate(top);
        }
        contributions.sort_unstable_by_key(|(_, term)| std::cmp::Reverse(OrderedFloat(term.abs())));
        contributions
    }

    /// Returns the top `top` contributions restricted to the given dimensions,
    /// sorted by descending absolute contribution.
    ///
    /// Dimensions outside of the vector bounds are ignored.
    pub fn top_contributions_for_dims(
        &self,
        point: &[VectorElementType],
        dims: &[DimId],
        top: usize,
    ) -> DimsExplained {
        let normalized;
        let point = match self.normalized_point(point) {
            Some(vector) => {
                normalized = vector;
                normalized.as_slice()
            }
            None => point,
        };

        let mut contributions: Vec<(DimId, ScoreType)> = dims
            .iter()
            .filter_map(|&dim| {
                let query_value = *self.query.get(dim as usize)?;
                let point_value = *point.get(dim as usize)?;
                Some((dim, self.term(query_value, point_value)))
            })
            .collect();

        let top = top.min(contributions.len());
        if top < contributions.len() {
            contributions
                .select_nth_unstable_by_key(top, |(_, term)| std::cmp::Reverse(OrderedFloat(term.abs())));
            contributions.truncate(top);
        }
        contributions.sort_unstable_by_key(|(_, term)| std::cmp::Reverse(OrderedFloat(term.abs())));
        contributions
    }

    /// Score of the point against the query, considering only the given dimensions.
    ///
    /// The score follows the same conventions as regular (post-processed) search scores:
    /// higher is better for Dot/Cosine, lower is better for Euclid/Manhattan,
    /// and Euclid scores are square-rooted distances.
    ///
    /// Dimensions outside of the vector bounds are ignored.
    pub fn score_for_dims(&self, point: &[VectorElementType], dims: &[DimId]) -> ScoreType {
        let normalized;
        let point = match self.normalized_point(point) {
            Some(vector) => {
                normalized = vector;
                normalized.as_slice()
            }
            None => point,
        };

        let sum: ScoreType = dims
            .iter()
            .filter_map(|&dim| {
                let query_value = *self.query.get(dim as usize)?;
                let point_value = *point.get(dim as usize)?;
                Some(self.term(query_value, point_value))
            })
            .sum();

        match self.distance {
            Distance::Cosine | Distance::Dot | Distance::Manhattan => sum,
            // Report a distance, consistent with `EuclidMetric::postprocess`
            Distance::Euclid => sum.abs().sqrt(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spaces::simple::{dot_similarity, euclid_similarity, manhattan_similarity};

    const EPS: f32 = 1e-5;

    fn query() -> DenseVector {
        vec![0.5, -1.0, 2.0, 0.0, 3.0]
    }

    fn point() -> DenseVector {
        vec![1.0, 1.0, -0.5, 4.0, 2.0]
    }

    #[test]
    fn test_dot_contributions_sum_to_score() {
        let calculator = DimsExplainedCalculator::new(query(), Distance::Dot);
        let contributions = calculator.top_contributions(&point(), usize::MAX);
        let sum: ScoreType = contributions.iter().map(|(_, term)| term).sum();
        let expected = dot_similarity(&query(), &point());
        assert!((sum - expected).abs() < EPS);
    }

    #[test]
    fn test_cosine_contributions_sum_to_score() {
        let calculator = DimsExplainedCalculator::new(query(), Distance::Cosine);
        let contributions = calculator.top_contributions(&point(), usize::MAX);
        let sum: ScoreType = contributions.iter().map(|(_, term)| term).sum();
        let expected = dot_similarity(
            &cosine_preprocess(query()),
            &cosine_preprocess(point()),
        );
        assert!((sum - expected).abs() < EPS);
    }

    #[test]
    fn test_euclid_contributions_sum_to_squared_score() {
        let calculator = DimsExplainedCalculator::new(query(), Distance::Euclid);
        let contributions = calculator.top_contributions(&point(), usize::MAX);
        let sum: ScoreType = contributions.iter().map(|(_, term)| term).sum();
        let expected = -euclid_similarity(&query(), &point());
        assert!((sum - expected).abs() < EPS);
    }

    #[test]
    fn test_manhattan_contributions_sum_to_score() {
        let calculator = DimsExplainedCalculator::new(query(), Distance::Manhattan);
        let contributions = calculator.top_contributions(&point(), usize::MAX);
        let sum: ScoreType = contributions.iter().map(|(_, term)| term).sum();
        let expected = -manhattan_similarity(&query(), &point());
        assert!((sum - expected).abs() < EPS);
    }

    #[test]
    fn test_top_selection_is_sorted_by_abs_contribution() {
        let calculator = DimsExplainedCalculator::new(query(), Distance::Dot);
        let top = calculator.top_contributions(&point(), 3);
        assert_eq!(top.len(), 3);
        // terms: [0.5, -1.0, -1.0, 0.0, 6.0] -> top 3 by |term|: dim 4, then dims 1 and 2
        assert_eq!(top[0], (4, 6.0));
        for pair in top.windows(2) {
            assert!(pair[0].1.abs() >= pair[1].1.abs());
        }
        assert!(!top.iter().any(|&(dim, _)| dim == 3));
    }

    #[test]
    fn test_score_for_dims_dot() {
        let calculator = DimsExplainedCalculator::new(query(), Distance::Dot);
        let score = calculator.score_for_dims(&point(), &[0, 4]);
        assert!((score - (0.5 + 6.0)).abs() < EPS);
    }

    #[test]
    fn test_score_for_dims_euclid_is_partial_distance() {
        let calculator = DimsExplainedCalculator::new(query(), Distance::Euclid);
        let score = calculator.score_for_dims(&point(), &[0, 1]);
        let expected = (0.25f32 + 4.0).sqrt();
        assert!((score - expected).abs() < EPS);
    }

    #[test]
    fn test_out_of_bounds_dims_are_ignored() {
        let calculator = DimsExplainedCalculator::new(query(), Distance::Dot);
        let score = calculator.score_for_dims(&point(), &[0, 100]);
        assert!((score - 0.5).abs() < EPS);

        let contributions = calculator.top_contributions_for_dims(&point(), &[0, 100], 10);
        assert_eq!(contributions.len(), 1);
        assert_eq!(contributions[0].0, 0);
    }
}
