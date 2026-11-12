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
    /// How [`Self::search`] would use a filter that matches about `cardinality` points. Makes
    /// the same choice of strategy as [`Self::search`].
    pub fn filter_checks(
        &self,
        query_vectors: &[QueryVector],
        cardinality: &CardinalityEstimation,
        top: usize,
    ) -> FilterChecks {
        if top == 0 || query_vectors.is_empty() {
            return FilterChecks::Lazy { expected_checks: 0 };
        }

        // Queries other than nearest are scored against every point matching the filter.
        // A search call holds a single search type, so they don't mix with nearest queries
        for query_vector in query_vectors {
            match query_vector {
                QueryVector::Nearest(_) => {}
                QueryVector::RecommendBestScore(_)
                | QueryVector::RecommendSumScores(_)
                | QueryVector::Discover(_)
                | QueryVector::Context(_)
                | QueryVector::FeedbackNaive(_) => return FilterChecks::Full,
            }
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

        if cardinality.max < threshold {
            return FilterChecks::Full;
        }

        // The inverted index checks a point once its score beats the current top: until `top`
        // matches are found, about `top / selectivity` points per query
        let per_query = top
            .saturating_mul(available_vectors)
            .checked_div(cardinality.exp)
            .unwrap_or(available_vectors)
            .min(available_vectors);
        FilterChecks::Lazy {
            expected_checks: per_query.saturating_mul(query_vectors.len()),
        }
    }
}
