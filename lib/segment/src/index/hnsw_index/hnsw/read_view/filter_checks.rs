use common::universal_io::UniversalRead;

use super::HNSWIndexReadView;
use crate::data_types::vectors::QueryVector;
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::field_index::CardinalityEstimation;
use crate::index::query_estimator::adjust_to_available_vectors;
use crate::index::query_optimization::filter_checks::FilterChecks;
use crate::index::sample_estimation::MAX_ESTIMATED_POINTS;
use crate::types::SearchParams;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectorsRead;
use crate::vector_storage::{RawScorerBuilder, VectorStorageRead};

impl<I, V, Q, P, S> HNSWIndexReadView<'_, I, V, Q, P, S>
where
    I: IdTrackerRead,
    V: VectorStorageRead + RawScorerBuilder,
    Q: QuantizedVectorsRead,
    P: PayloadIndexRead,
    S: UniversalRead,
{
    /// How [`Self::search`] would use a filter that matches about `cardinality` points. Makes
    /// the same choice of strategy as [`Self::search`].
    pub(crate) fn filter_checks(
        &self,
        query_vectors: &[QueryVector],
        cardinality: &CardinalityEstimation,
        top: usize,
        params: Option<&SearchParams>,
    ) -> FilterChecks {
        if top == 0 || query_vectors.is_empty() {
            return FilterChecks::Lazy { expected_checks: 0 };
        }

        let is_hnsw_disabled = self.config.m == 0 && self.config.payload_m.unwrap_or(0) == 0;
        let exact = params.is_some_and(|params| params.exact);
        if exact || is_hnsw_disabled {
            return FilterChecks::Full;
        }

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
            self.vector_storage.available_vector_count(),
            self.id_tracker.available_point_count(),
        );

        let threshold = self.config.full_scan_threshold;
        if cardinality.max < threshold {
            return FilterChecks::Full;
        }

        let mut checks = self.graph_filter_checks(query_vectors.len(), top, params);
        if cardinality.min <= threshold {
            // Sampling picks the strategy first, one check per sample
            checks = checks.saturating_add(MAX_ESTIMATED_POINTS);
        }
        FilterChecks::Lazy {
            expected_checks: checks,
        }
    }

    /// Expected filter checks of a graph search over `query_vectors` query vectors.
    ///
    /// A graph search checks the neighbours of the points it expands: about `ef` expansions of
    /// up to `m0` neighbours each, for every query vector.
    pub(crate) fn graph_filter_checks(
        &self,
        query_vectors: usize,
        top: usize,
        params: Option<&SearchParams>,
    ) -> usize {
        let ef = params
            .and_then(|params| params.hnsw_ef)
            .unwrap_or(self.config.ef)
            .max(top);
        let per_query = ef.saturating_mul(self.config.m0.max(1));
        per_query.saturating_mul(query_vectors)
    }
}
