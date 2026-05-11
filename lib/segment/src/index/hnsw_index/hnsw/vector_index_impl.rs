use std::collections::HashMap;
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoredPointOffset, TelemetryDetail};
use sparse::common::types::DimId;

use super::HNSWIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::operation_time_statistics::ScopeDurationMeasurer;
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorRef};
use crate::id_tracker::IdTrackerRead;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::query_estimator::adjust_to_available_vectors;
use crate::index::sample_estimation::sample_check_cardinality;
use crate::index::{PayloadIndexRead, VectorIndex, VectorIndexRead};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, QuantizationSearchParams, SearchParams};
use crate::vector_storage::VectorStorageRead;

impl VectorIndexRead for HNSWIndex {
    fn search(
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
                let mut params = *params;
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
                let vector_storage = self.vector_storage.borrow();

                // Determine whether to do a plain or graph search, and pick search timer aggregator
                // Because an HNSW graph is built, we'd normally always assume to search the graph.
                // But because a lot of points may be deleted in this graph, it may just be faster
                // to do a plain search instead.
                let plain_search = exact
                    || is_hnsw_disabled
                    || vector_storage.available_vector_count() < self.config.full_scan_threshold;

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
                    self.search_vectors_with_graph(vectors, None, top, params, query_context)
                }
            }
            Some(query_filter) => {
                // depending on the amount of filtered-out points the optimal strategy could be
                // - to retrieve possible points and score them after
                // - to use HNSW index with filtering condition

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
                        top,
                        params_ref,
                        query_context,
                    );
                }

                let payload_index = self.payload_index.borrow();
                let vector_storage = self.vector_storage.borrow();
                let id_tracker = self.id_tracker.borrow();
                let available_vector_count = vector_storage.available_vector_count();

                let hw_counter = query_context.hardware_counter();

                let query_point_cardinality = payload_index
                    .with_view(|v| v.estimate_cardinality(query_filter, &hw_counter))?;
                let query_cardinality = adjust_to_available_vectors(
                    query_point_cardinality,
                    available_vector_count,
                    id_tracker.available_point_count(),
                );

                if query_cardinality.max < self.config.full_scan_threshold {
                    // if cardinality is small - use plain index
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.small_cardinality);
                    return self.search_vectors_plain(
                        vectors,
                        query_filter,
                        top,
                        params,
                        query_context,
                    );
                }

                if query_cardinality.min > self.config.full_scan_threshold {
                    // if cardinality is high enough - use HNSW index
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.large_cardinality);
                    return self.search_vectors_with_graph(
                        vectors,
                        filter,
                        top,
                        params,
                        query_context,
                    );
                }

                // Fast cardinality estimation is not enough, do sample estimation of cardinality.
                // The filter context's lifetime is tied to the view, so the sample check
                // must run inside `with_view` -- the recursive search dispatches re-borrow
                // payload_index on their own.
                let use_graph = payload_index.with_view(|v| {
                    let filter_context = v.filter_context(query_filter, &hw_counter)?;
                    Ok::<_, OperationError>(sample_check_cardinality(
                        id_tracker.sample_ids(Some(vector_storage.deleted_vector_bitslice())),
                        |idx| filter_context.check(idx),
                        self.config.full_scan_threshold,
                        available_vector_count, // Check cardinality among available vectors
                    ))
                })?;

                if use_graph {
                    // if cardinality is high enough - use HNSW index
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.large_cardinality);
                    self.search_vectors_with_graph(vectors, filter, top, params, query_context)
                } else {
                    // if cardinality is small - use plain index
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.small_cardinality);
                    self.search_vectors_plain(vectors, query_filter, top, params, query_context)
                }
            }
        }
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> VectorIndexSearchesTelemetry {
        let tm = &self.searches_telemetry;
        VectorIndexSearchesTelemetry {
            index_name: None,
            unfiltered_plain: tm.unfiltered_plain.lock().get_statistics(detail),
            filtered_plain: tm.filtered_plain.lock().get_statistics(detail),
            unfiltered_hnsw: tm.unfiltered_hnsw.lock().get_statistics(detail),
            filtered_small_cardinality: tm.small_cardinality.lock().get_statistics(detail),
            filtered_large_cardinality: tm.large_cardinality.lock().get_statistics(detail),
            filtered_exact: tm.exact_filtered.lock().get_statistics(detail),
            filtered_sparse: Default::default(),
            unfiltered_exact: tm.exact_unfiltered.lock().get_statistics(detail),
            unfiltered_sparse: Default::default(),
        }
    }

    fn indexed_vector_count(&self) -> usize {
        self.config
            .indexed_vector_count
            // If indexed vector count is unknown, fall back to number of points
            .unwrap_or_else(|| self.graph.num_points())
    }

    fn size_of_searchable_vectors_in_bytes(&self) -> usize {
        self.vector_storage
            .borrow()
            .size_of_available_vectors_in_bytes()
    }

    fn fill_idf_statistics(
        &self,
        _idf: &mut HashMap<DimId, usize>,
        _hw_counter: &HardwareCounterCell,
    ) {
        // HNSW (dense) index doesn't track IDF.
    }

    fn is_index(&self) -> bool {
        true
    }
}

impl VectorIndex for HNSWIndex {
    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.graph.files(&self.path);
        let config_path = HnswGraphConfig::get_config_path(&self.path);
        if config_path.exists() {
            files.push(config_path);
        }
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.files() // All HNSW index files are immutable 😎
    }

    fn update_vector(
        &mut self,
        _id: PointOffsetType,
        _vector: Option<VectorRef>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Err(OperationError::service_error("Cannot update HNSW index"))
    }
}
