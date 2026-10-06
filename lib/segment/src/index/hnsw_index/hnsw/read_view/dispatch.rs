use common::condition_checker::ConditionChecker;
use common::types::ScoredPointOffset;
use common::universal_io::UniversalRead;

use super::HNSWIndexReadView;
use crate::common::operation_error::OperationResult;
use crate::common::operation_time_statistics::ScopeDurationMeasurer;
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::QueryVector;
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::field_index::CardinalityEstimation;
use crate::index::hnsw_index::graph_layers::SearchAlgorithm;
use crate::index::query_estimator::adjust_to_available_vectors;
use crate::index::sample_estimation::sample_check_cardinality;
use crate::types::{ACORN_MAX_SELECTIVITY_DEFAULT, Filter, QuantizationSearchParams, SearchParams};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectorsRead;
use crate::vector_storage::{RawScorerBuilder, VectorStorageRead};

impl<'a, I, V, Q, P, S> HNSWIndexReadView<'a, I, V, Q, P, S>
where
    I: IdTrackerRead,
    V: VectorStorageRead + RawScorerBuilder,
    Q: QuantizedVectorsRead,
    P: PayloadIndexRead,
    S: UniversalRead,
{
    pub(crate) fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        if top == 0 {
            return Ok(vec![vec![]; vectors.len()]);
        }

        // If neither `m` nor `payload_m` is set, HNSW doesn't have any links.
        // And if so, we need to fall back to plain search (optionally, with quantization).

        let is_hnsw_disabled = self.config.m == 0 && self.config.payload_m.unwrap_or(0) == 0;
        let exact = params.is_some_and(|params| params.exact);

        let exact_params = if exact {
            params.map(|params| {
                let mut params = params.clone();
                params.quantization = Some(QuantizationSearchParams {
                    ignore: true,
                    rescore: Some(false),
                    oversampling: None,
                }); // disable quantization for exact search
                params
            })
        } else {
            None
        };

        match filter {
            None => {
                // Determine whether to do a plain or graph search, and pick search timer aggregator
                // Because an HNSW graph is built, we'd normally always assume to search the graph.
                // But because a lot of points may be deleted in this graph, it may just be faster
                // to do a plain search instead.
                let plain_search = exact
                    || is_hnsw_disabled
                    || self.vector_storage.available_vector_count()
                        < self.config.full_scan_threshold;

                // Do plain or graph search
                if plain_search {
                    let _timer = ScopeDurationMeasurer::new(if exact {
                        &self.searches_telemetry.exact_unfiltered
                    } else {
                        &self.searches_telemetry.unfiltered_plain
                    });

                    let params_ref = if exact { exact_params.as_ref() } else { params };
                    self.search_plain_unfiltered_batched(vectors, top, params_ref, query_context)
                } else {
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.unfiltered_hnsw);
                    self.search_vectors_with_graph(
                        vectors,
                        None,
                        top,
                        params,
                        SearchAlgorithm::Hnsw,
                        query_context,
                    )
                }
            }
            Some(query_filter) => {
                // depending on the amount of filtered-out points the optimal strategy could be
                // - to retrieve possible points and score them after
                // - to use HNSW index with filtering condition

                let available_vector_count = self.vector_storage.available_vector_count();

                let hw_counter = query_context.hardware_counter();

                // Estimated once for every strategy below: for a filter whose conditions
                // resolve ids (`has_id`), the estimation performs the external->internal
                // resolution, and the plain search reuses the resolved offsets.
                let query_point_cardinality = self
                    .payload_index
                    .estimate_cardinality(query_filter, &hw_counter)?;
                let query_cardinality = adjust_to_available_vectors(
                    query_point_cardinality,
                    available_vector_count,
                    self.id_tracker.available_point_count(),
                );
                let full_scan_threshold = self.filtered_full_scan_threshold(params, top);

                // if exact search is requested, we should not use HNSW index
                if exact || is_hnsw_disabled {
                    let _timer = ScopeDurationMeasurer::new(if exact {
                        &self.searches_telemetry.exact_filtered
                    } else {
                        &self.searches_telemetry.filtered_plain
                    });

                    let params_ref = if exact { exact_params.as_ref() } else { params };

                    return self.search_vectors_plain(
                        vectors,
                        query_filter,
                        &query_cardinality,
                        top,
                        params_ref,
                        query_context,
                    );
                }

                if query_cardinality.max < full_scan_threshold {
                    // if cardinality is small - use plain index
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.small_cardinality);
                    return self.search_vectors_plain(
                        vectors,
                        query_filter,
                        &query_cardinality,
                        top,
                        params,
                        query_context,
                    );
                }

                if query_cardinality.min > full_scan_threshold {
                    // if cardinality is high enough - use HNSW index
                    return self.search_vectors_with_graph_filtered(
                        vectors,
                        query_filter,
                        &query_cardinality,
                        top,
                        params,
                        query_context,
                    );
                }

                // Fast cardinality estimation is not enough, do sample estimation of cardinality.
                // The filter context's lifetime is tied to the payload view, which is already
                // held by this read view.
                let use_graph = {
                    let filter_context = self
                        .payload_index
                        .filter_context(query_filter, &hw_counter)?;
                    sample_check_cardinality(
                        self.id_tracker
                            .sample_ids(Some(self.vector_storage.deleted_vector_bitslice())),
                        |idx| filter_context.check(idx),
                        full_scan_threshold,
                        available_vector_count, // Check cardinality among available vectors
                    )?
                };

                if use_graph {
                    // if cardinality is high enough - use HNSW index
                    self.search_vectors_with_graph_filtered(
                        vectors,
                        query_filter,
                        &query_cardinality,
                        top,
                        params,
                        query_context,
                    )
                } else {
                    // if cardinality is small - use plain index
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.small_cardinality);
                    self.search_vectors_plain(
                        vectors,
                        query_filter,
                        &query_cardinality,
                        top,
                        params,
                        query_context,
                    )
                }
            }
        }
    }

    /// The point count below which a filtered search scores the filter's matches
    /// instead of walking the graph.
    ///
    /// Without `ef_aware_filtered_planner`, the configured `full_scan_threshold`. With
    /// it, the calibrated break-even (`calibrated_filtered_threshold`) for the `ef` the
    /// walk would run with (`hnsw_ef`, at least `top`) and this segment's vector size.
    fn filtered_full_scan_threshold(&self, params: Option<&SearchParams>, top: usize) -> usize {
        let threshold = self.config.full_scan_threshold;
        if !common::flags::feature_flags().ef_aware_filtered_planner {
            return threshold;
        }
        let ef = params
            .and_then(|params| params.hnsw_ef)
            .unwrap_or(self.config.ef)
            .max(top);
        let vector_bytes = self
            .vector_storage
            .size_of_available_vectors_in_bytes()
            .checked_div(self.vector_storage.available_vector_count())
            .unwrap_or(0);
        calibrated_filtered_threshold(threshold, vector_bytes, ef)
    }

    /// Filtered graph search, timed under the counter of the algorithm it runs.
    fn search_vectors_with_graph_filtered(
        &self,
        vectors: &[&QueryVector],
        filter: &Filter,
        query_cardinality: &CardinalityEstimation,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        let algorithm = self.filtered_graph_algorithm(query_cardinality, params);
        let _timer = ScopeDurationMeasurer::new(match algorithm {
            SearchAlgorithm::Hnsw => &self.searches_telemetry.large_cardinality,
            SearchAlgorithm::Acorn => &self.searches_telemetry.acorn,
        });
        self.search_vectors_with_graph(vectors, Some(filter), top, params, algorithm, query_context)
    }

    /// ACORN runs when it is enabled and the filter is selective enough.
    fn filtered_graph_algorithm(
        &self,
        query_cardinality: &CardinalityEstimation,
        params: Option<&SearchParams>,
    ) -> SearchAlgorithm {
        let Some(acorn) = params.and_then(|params| params.acorn) else {
            return SearchAlgorithm::Hnsw;
        };
        if !acorn.enable || self.config.m0 == 0 {
            return SearchAlgorithm::Hnsw;
        }
        // NOTE: technically we also might want to use ACORN for unfiltered
        // searches for segments with a lot of deleted points. But in
        // practice, such segments most likely to be picked by an optimizer
        // soon.

        let available_vector_count = self.vector_storage.available_vector_count();
        let selectivity = if available_vector_count == 0 {
            1.0
        } else {
            query_cardinality.exp as f64 / available_vector_count as f64
        };

        let max_selectivity = acorn
            .max_selectivity
            .map_or(ACORN_MAX_SELECTIVITY_DEFAULT, |v| *v);
        if selectivity <= max_selectivity {
            SearchAlgorithm::Acorn
        } else {
            SearchAlgorithm::Hnsw
        }
    }
}

/// The break-even match count of the calibration below, at `CALIBRATION_EF` and the
/// default `full_scan_threshold_kb` (10,000) for 2048-byte vectors (512 x f32).
const CALIBRATED_BREAK_EVEN: f64 = 1_300.0;
const CALIBRATION_EF: f64 = 100.0;
const CALIBRATION_VECTOR_BYTES: f64 = 2048.0;
const DEFAULT_FULL_SCAN_THRESHOLD_KB: f64 = 10_000.0;
/// The break-even grows as `ef^0.9`: a walk costs more with `ef` (about `ef^0.4` to
/// `ef^0.65`) and also with how many neighbours the filter admits, so the match count
/// a scan can afford grows almost in proportion.
const BREAK_EVEN_EF_EXPONENT: f64 = 0.9;
/// And as `bytes^-0.5`: scoring a match costs less than proportionally more for a wider
/// vector, and a walk barely more up to 2 KB. `full_scan_threshold_kb` assumes `bytes^-1`.
const BREAK_EVEN_BYTES_EXPONENT: f64 = -0.5;

/// The match count below which scoring a filter's matches beats walking the graph, for
/// a walk at `ef` over vectors of `vector_bytes`, given the configured threshold in points
/// (`full_scan_threshold_kb x 1024 / vector_bytes`, as `derive_config` computes it).
///
/// Calibrated on Qdrant's own filtered search (uniform keyword filters at 0.5% to 10%
/// selectivity, unquantized f32 at d = 128, 512 and 1536, m = 16, `ef` 32 to 512): at
/// `ef` 100 the measured break-even is about 2,600, 1,300 and 750 points, where the
/// default 10,000 KB threshold reads 20,000, 5,000 and 1,666. The configured threshold
/// keeps its meaning as a scale: twice the default threshold, twice the break-even.
fn calibrated_filtered_threshold(threshold_points: usize, vector_bytes: usize, ef: usize) -> usize {
    if vector_bytes == 0 {
        return threshold_points;
    }
    let bytes = vector_bytes as f64;
    let threshold_kb = threshold_points as f64 * bytes / 1024.0;
    let break_even = CALIBRATED_BREAK_EVEN
        * (threshold_kb / DEFAULT_FULL_SCAN_THRESHOLD_KB)
        * (ef.max(1) as f64 / CALIBRATION_EF).powf(BREAK_EVEN_EF_EXPONENT)
        * (bytes / CALIBRATION_VECTOR_BYTES).powf(BREAK_EVEN_BYTES_EXPONENT);
    break_even.round().max(1.0) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `derive_config`'s conversion of the default 10,000 KB to points.
    fn default_points(vector_bytes: usize) -> usize {
        10_000 * 1024 / vector_bytes
    }

    #[test]
    fn test_reproduces_the_calibration_at_ef_100() {
        for (dim, want) in [(128, 2_600), (512, 1_300), (1536, 750)] {
            let bytes = dim * 4;
            let got = calibrated_filtered_threshold(default_points(bytes), bytes, 100);
            assert!(
                got.abs_diff(want) <= want / 50,
                "d={dim}: {got} against {want}"
            );
        }
    }

    #[test]
    fn test_break_even_grows_with_ef_and_stays_monotonic() {
        let at = |ef| calibrated_filtered_threshold(default_points(2048), 2048, ef);
        assert!(at(32) < at(64) && at(64) < at(128) && at(128) < at(512));
        // ef^0.9: five times the `ef`, about 4.3 times the break-even.
        let ratio = at(500) as f64 / at(100) as f64;
        assert!((4.2..4.4).contains(&ratio), "{ratio}");
    }

    #[test]
    fn test_the_configured_threshold_still_scales_it() {
        let base = calibrated_filtered_threshold(default_points(2048), 2048, 100);
        let doubled = calibrated_filtered_threshold(2 * default_points(2048), 2048, 100);
        assert!(
            doubled.abs_diff(2 * base) <= 1,
            "{doubled} against {}",
            2 * base
        );
    }

    #[test]
    fn test_degenerate_inputs() {
        // No vectors to size: the configured threshold, as without the flag.
        assert_eq!(calibrated_filtered_threshold(5_000, 0, 100), 5_000);
        // `ef` 0 is read as 1, and the threshold never reaches zero.
        assert!(calibrated_filtered_threshold(5_000, 2048, 0) >= 1);
        assert!(calibrated_filtered_threshold(0, 2048, 100) >= 1);
    }
}
