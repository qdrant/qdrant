use sparse::index::inverted_index::InvertedIndex;

use super::SparseVectorIndexReadView;
use crate::data_types::vectors::QueryVector;
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::field_index::CardinalityEstimation;
use crate::index::query_estimator::adjust_to_available_vectors;
use crate::index::query_optimization::filter_checks::FilterChecks;
use crate::types::DEFAULT_SPARSE_FULL_SCAN_THRESHOLD;
use crate::vector_storage::{RawScorerBuilder, VectorStorageRead};

impl<I, V, P, TInvertedIndex> SparseVectorIndexReadView<'_, I, V, P, TInvertedIndex>
where
    I: IdTrackerRead,
    V: VectorStorageRead + RawScorerBuilder,
    P: PayloadIndexRead,
    TInvertedIndex: InvertedIndex,
{
    /// Filter checks [`Self::search`] makes for a filter estimated at `cardinality` points,
    /// following the same strategy choice.
    pub fn filter_checks(
        &self,
        query_vectors: &[QueryVector],
        cardinality: &CardinalityEstimation,
        top: usize,
    ) -> FilterChecks {
        if top == 0 {
            return FilterChecks::NONE;
        }

        let available_vectors = self.vector_storage.available_vector_count();
        let CardinalityEstimation {
            primary_clauses: _,
            min,
            exp,
            max,
        } = *cardinality;
        let cardinality = adjust_to_available_vectors(
            CardinalityEstimation {
                primary_clauses: vec![],
                min,
                exp,
                max,
            },
            available_vectors,
            self.id_tracker.available_point_count(),
        );
        let threshold = self
            .config
            .full_scan_threshold
            .unwrap_or(DEFAULT_SPARSE_FULL_SCAN_THRESHOLD);

        query_vectors
            .iter()
            .map(|query_vector| match query_vector {
                QueryVector::Nearest(_) if cardinality.max < threshold => {
                    FilterChecks::ALL_CANDIDATES
                }
                // The inverted index checks a point once its score beats the current top:
                // until `top` matches are found, about `top / selectivity` points
                QueryVector::Nearest(_) => {
                    let checks = top
                        .saturating_mul(available_vectors)
                        .checked_div(cardinality.exp)
                        .unwrap_or(available_vectors)
                        .min(available_vectors);
                    FilterChecks::lazy(checks)
                }
                // Scored against every point matching the filter
                QueryVector::RecommendBestScore(_)
                | QueryVector::RecommendSumScores(_)
                | QueryVector::Discover(_)
                | QueryVector::Context(_)
                | QueryVector::FeedbackNaive(_) => FilterChecks::ALL_CANDIDATES,
            })
            .fold(FilterChecks::NONE, |sum, checks| sum + checks)
    }
}
