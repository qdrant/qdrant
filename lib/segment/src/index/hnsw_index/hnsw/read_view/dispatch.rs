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
                    // Deleted nodes can be graph bridges even without a payload filter.
                    // Use deletion-aware traversal until the optimizer rebuilds the graph.
                    let indexed_vectors = self
                        .config
                        .indexed_vector_count
                        .unwrap_or_else(|| self.vector_storage.total_vector_count());
                    // Point tombstones do not necessarily delete the stored vector.
                    let algorithm = if self.id_tracker.deleted_point_count() != 0
                        || self.vector_storage.available_vector_count() < indexed_vectors
                        || query_context.deleted_points().is_some()
                    {
                        SearchAlgorithm::Acorn
                    } else {
                        SearchAlgorithm::Hnsw
                    };
                    let _timer = ScopeDurationMeasurer::new(match algorithm {
                        SearchAlgorithm::Hnsw => &self.searches_telemetry.unfiltered_hnsw,
                        SearchAlgorithm::Acorn => &self.searches_telemetry.acorn,
                    });
                    self.search_vectors_with_graph(
                        vectors,
                        None,
                        top,
                        params,
                        algorithm,
                        query_context,
                    )
                }
            }
            Some(query_filter) => {
                // depending on the amount of filtered-out points the optimal strategy could be
                // - to retrieve possible points and score them after
                // - to use HNSW index with filtering condition

                let available_vector_count = self.vector_storage.available_vector_count();

                // Estimated once for every strategy below: for a filter whose conditions
                // resolve ids (`has_id`), the estimation performs the external->internal
                // resolution, and the plain search reuses the resolved offsets.
                let query_point_cardinality =
                    self.payload_index.estimate_cardinality(query_filter)?;
                let query_cardinality = adjust_to_available_vectors(
                    query_point_cardinality,
                    available_vector_count,
                    self.id_tracker.available_point_count(),
                );

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

                if query_cardinality.max < self.config.full_scan_threshold {
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

                if query_cardinality.min > self.config.full_scan_threshold {
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
                    let filter_context = self.payload_index.filter_context(query_filter)?;
                    sample_check_cardinality(
                        self.id_tracker
                            .sample_ids(Some(self.vector_storage.deleted_vector_bitslice())),
                        |idx| filter_context.check(idx),
                        self.config.full_scan_threshold,
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
        if !acorn.enable {
            return SearchAlgorithm::Hnsw;
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::ambient;
    use common::bitvec::{BitSlice, BitVec};
    use common::types::PointOffsetType;
    use common::universal_io::MmapFile;
    use rstest::rstest;

    use super::*;
    use crate::data_types::query_context::QueryContext;
    use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
    use crate::entry::entry_point::{NonAppendableSegmentEntry, SegmentEntry};
    use crate::index::hnsw_index::HnswM;
    use crate::index::hnsw_index::config::HnswGraphConfig;
    use crate::index::hnsw_index::entry_points::EntryPoints;
    use crate::index::hnsw_index::graph::HnswGraph;
    use crate::index::hnsw_index::graph_layers::GraphLayers;
    use crate::index::hnsw_index::graph_links::{
        GraphLinks, GraphLinksFormatParam, GraphLinksResidency,
    };
    use crate::index::hnsw_index::hnsw::telemetry::HNSWSearchesTelemetry;
    use crate::index::visited_pool::VisitedPool;
    use crate::segment::Segment;
    use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
    use crate::types::Distance;
    use crate::vector_storage::VectorStorage;
    use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

    #[derive(Clone, Copy)]
    enum Deletion {
        Point,
        Vector,
        Proxy,
    }

    #[rstest]
    #[case::points(Deletion::Point)]
    #[case::vectors(Deletion::Vector)]
    #[case::proxy(Deletion::Proxy)]
    fn unfiltered_search_reaches_live_points_after_deletions(
        #[case] deletion: Deletion,
        #[values(true, false)] with_indexed_count: bool,
    ) {
        let _scope = ambient::test_guard();
        let dir = tempfile::tempdir().unwrap();
        let mut segment = build_simple_segment(dir.path(), 2, Distance::Dot).unwrap();
        for (point, vector) in [[0.0, 1.0], [0.5, 0.5], [1.0, 0.0]].iter().enumerate() {
            segment
                .upsert_point(
                    point as u64,
                    (point as u64).into(),
                    only_default_vector(vector),
                )
                .unwrap();
        }

        // The entry stays live. Point 1 is the only path to the unique self-match, point 2.
        let hnsw_m = HnswM::new2(2);
        let mut entry_points = EntryPoints::new(1);
        entry_points.new_point(0, 0, |_| true);
        let graph = HnswGraph::<MmapFile>::Direct(Arc::new(GraphLayers {
            hnsw_m,
            links: GraphLinks::new_from_edges(
                vec![vec![vec![1]], vec![vec![0, 2]], vec![vec![1]]],
                GraphLinksFormatParam::Plain,
                hnsw_m,
            )
            .unwrap(),
            entry_points,
            visited_pool: VisitedPool::new(),
            residency: GraphLinksResidency::Pinned,
        }));
        let mut config = HnswGraphConfig::new(2, 8, 0, 1, None, 3);
        if !with_indexed_count {
            config.indexed_vector_count = None;
        }
        let telemetry = HNSWSearchesTelemetry::new();
        let query = QueryVector::Nearest(vec![1.0, 0.0].into());
        let search = |segment: &Segment, exact, deleted: &BitSlice| {
            let id_tracker = segment.id_tracker.borrow();
            let vector_storage = segment.vector_data[DEFAULT_VECTOR_NAME]
                .vector_storage
                .borrow();
            let query_context = QueryContext::default();
            let mut segment_context = query_context.get_segment_query_context();
            if matches!(deletion, Deletion::Proxy) {
                segment_context = segment_context.with_deleted_points(deleted);
            }
            let vector_context = segment_context.get_vector_context(DEFAULT_VECTOR_NAME, None);
            segment
                .payload_index
                .borrow()
                .with_view(|payload_index| {
                    HNSWIndexReadView {
                        id_tracker: &*id_tracker,
                        vector_storage: &*vector_storage,
                        quantized_vectors: None::<&QuantizedVectors>,
                        payload_index,
                        config: &config,
                        graph: &graph,
                        searches_telemetry: &telemetry,
                    }
                    .search(
                        &[&query],
                        None,
                        1,
                        Some(&SearchParams {
                            exact,
                            ..Default::default()
                        }),
                        &vector_context,
                    )
                })
                .unwrap()
                .remove(0)
        };
        let delete =
            |segment: &mut Segment, deleted: &mut BitVec, point: PointOffsetType, version| {
                match deletion {
                    Deletion::Point => {
                        segment
                            .delete_point(version, (point as u64).into())
                            .unwrap();
                    }
                    Deletion::Vector => {
                        segment.vector_data[DEFAULT_VECTOR_NAME]
                            .vector_storage
                            .borrow_mut()
                            .delete_vector(point)
                            .unwrap();
                    }
                    Deletion::Proxy => deleted.set(point as usize, true),
                }
            };

        let expected = vec![ScoredPointOffset { idx: 2, score: 1.0 }];
        let mut deleted = BitVec::repeat(false, 3);
        assert_eq!(search(&segment, false, &deleted), expected);
        delete(&mut segment, &mut deleted, 1, 10);
        assert_eq!(search(&segment, true, &deleted), expected);
        assert_eq!(search(&segment, false, &deleted), expected);

        delete(&mut segment, &mut deleted, 0, 11);
        assert_eq!(search(&segment, true, &deleted), expected);
        assert_eq!(search(&segment, false, &deleted), expected);

        delete(&mut segment, &mut deleted, 2, 12);
        assert!(search(&segment, true, &deleted).is_empty());
        assert!(search(&segment, false, &deleted).is_empty());
    }
}
