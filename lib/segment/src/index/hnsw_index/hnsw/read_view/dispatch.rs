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
                // Payload-only graphs can be disconnected and leave vectors outside all blocks.
                let plain_search = exact
                    || self.config.m == 0
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
                    let algorithm = if params.is_some_and(|params| params.acorn.is_some()) {
                        self.graph_algorithm(
                            &CardinalityEstimation::exact(
                                self.vector_storage.available_vector_count(),
                            ),
                            params,
                        )
                    } else if self.id_tracker.deleted_point_count() != 0
                        || self.vector_storage.available_vector_count() < indexed_vectors
                        || query_context.deleted_points().is_some()
                    {
                        SearchAlgorithm::Acorn
                    } else {
                        SearchAlgorithm::Hnsw
                    };
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.unfiltered_hnsw);
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
                    // Samples are checked one at a time, so conditions on cold indexes are
                    // read up front: a few sequential reads instead of a random read per sample.
                    let filter_context = self
                        .payload_index
                        .filter_context_for_serial_checks(query_filter)?;
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
        let algorithm = self.graph_algorithm(query_cardinality, params);
        let _timer = ScopeDurationMeasurer::new(match algorithm {
            SearchAlgorithm::Hnsw => &self.searches_telemetry.large_cardinality,
            SearchAlgorithm::Acorn => &self.searches_telemetry.acorn,
        });
        self.search_vectors_with_graph(vectors, Some(filter), top, params, algorithm, query_context)
    }

    /// ACORN runs when it is enabled and the filter is selective enough.
    fn graph_algorithm(
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
    use std::borrow::Cow;
    use std::cell::Cell;
    use std::sync::Arc;

    use common::ambient;
    use common::bitvec::{BitSlice, BitVec};
    use common::fs::atomic_save_bin;
    use common::types::{PointOffsetType, TelemetryDetail};
    use common::universal_io::{MmapFile, MmapFs};
    use rstest::rstest;

    use super::*;
    use crate::data_types::query_context::QueryContext;
    use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
    use crate::entry::entry_point::{NonAppendableSegmentEntry, SegmentEntry};
    use crate::index::hnsw_index::HnswM;
    use crate::index::hnsw_index::config::HnswGraphConfig;
    use crate::index::hnsw_index::entry_points::EntryPoints;
    use crate::index::hnsw_index::graph::{GraphSearchArgs, HnswGraph, SearchScorers};
    use crate::index::hnsw_index::graph_layers::{GraphLayerData, GraphLayers};
    use crate::index::hnsw_index::graph_layers_batched::GraphLayersBatched;
    use crate::index::hnsw_index::graph_links::{
        GraphLinks, GraphLinksFormatParam, GraphLinksResidency,
    };
    use crate::index::hnsw_index::hnsw::telemetry::HNSWSearchesTelemetry;
    use crate::index::hnsw_index::point_scorer::FilteredScorer;
    use crate::index::visited_pool::VisitedPool;
    use crate::segment::Segment;
    use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
    use crate::types::{AcornSearchParams, Distance};
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
        #[values(1, 2)] bridge_count: usize,
        #[values(false, true)] batched: bool,
    ) {
        let _scope = ambient::test_guard();
        let dir = tempfile::tempdir().unwrap();
        let mut segment = build_simple_segment(dir.path(), 2, Distance::Dot).unwrap();
        let point_count = bridge_count + 2;
        for point in 0..point_count {
            let x = point as f32 / (point_count - 1) as f32;
            segment
                .upsert_point(
                    point as u64,
                    (point as u64).into(),
                    only_default_vector(&[x, 1.0 - x]),
                )
                .unwrap();
        }

        // The entry stays live, with one or two deleted bridges before the self-match.
        let links = (0..point_count)
            .map(|point| {
                let mut neighbors = Vec::new();
                if point > 0 {
                    neighbors.push((point - 1) as PointOffsetType);
                }
                if point + 1 < point_count {
                    neighbors.push((point + 1) as PointOffsetType);
                }
                vec![neighbors]
            })
            .collect();
        let hnsw_m = HnswM::new2(2);
        let mut entry_points = EntryPoints::new(1);
        entry_points.new_point(0, 0, |_| true);
        let layers = GraphLayers {
            hnsw_m,
            links: GraphLinks::new_from_edges(
                links,
                if batched {
                    GraphLinksFormatParam::Compressed
                } else {
                    GraphLinksFormatParam::Plain
                },
                hnsw_m,
            )
            .unwrap(),
            entry_points,
            visited_pool: VisitedPool::new(),
            residency: GraphLinksResidency::Pinned,
        };
        let graph = if batched {
            atomic_save_bin(
                &GraphLayers::get_path(dir.path()),
                &GraphLayerData {
                    m: hnsw_m.m,
                    m0: hnsw_m.m0,
                    ef_construct: 8,
                    entry_points: Cow::Borrowed(&layers.entry_points),
                },
            )
            .unwrap();
            fs_err::write(
                GraphLayers::get_links_path(dir.path(), layers.links.format()),
                layers.links.as_bytes().unwrap(),
            )
            .unwrap();
            HnswGraph::<MmapFile>::Batched(Arc::new(
                GraphLayersBatched::open(&MmapFs, dir.path(), GraphLinksResidency::Cold).unwrap(),
            ))
        } else {
            HnswGraph::Direct(Arc::new(layers))
        };
        let mut config = HnswGraphConfig::new(2, 8, 0, 1, None, point_count);
        if !with_indexed_count {
            config.indexed_vector_count = None;
        }
        let telemetry = HNSWSearchesTelemetry::new();
        let query = QueryVector::Nearest(vec![1.0, 0.0].into());
        let query_context = QueryContext::default();
        let search = |segment: &Segment, exact, deleted: &BitSlice, acorn| {
            let id_tracker = segment.id_tracker.borrow();
            let vector_storage = segment.vector_data[DEFAULT_VECTOR_NAME]
                .vector_storage
                .borrow();
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
                            acorn,
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
                            .delete_point(version, u64::from(point).into())
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

        let expected = vec![ScoredPointOffset {
            idx: (point_count - 1) as PointOffsetType,
            score: 1.0,
        }];
        let mut deleted = BitVec::repeat(false, point_count);
        assert_eq!(search(&segment, false, &deleted, None), expected);
        for point in 1..point_count - 1 {
            delete(
                &mut segment,
                &mut deleted,
                point as PointOffsetType,
                10 + point as u64,
            );
        }
        assert_eq!(search(&segment, true, &deleted, None), expected);
        assert_eq!(search(&segment, false, &deleted, None), expected);

        // Explicit requests can retain ordinary HNSW's one-hop traversal.
        let one_hop_result = vec![ScoredPointOffset { idx: 0, score: 0.0 }];
        for acorn in [
            AcornSearchParams {
                enable: false,
                max_selectivity: None,
            },
            AcornSearchParams {
                enable: true,
                max_selectivity: Some(0.0.into()),
            },
            AcornSearchParams {
                enable: true,
                max_selectivity: None,
            },
        ] {
            assert_eq!(
                search(&segment, false, &deleted, Some(acorn)),
                one_hop_result,
                "{acorn:?}",
            );
        }
        assert_eq!(
            search(
                &segment,
                false,
                &deleted,
                Some(AcornSearchParams {
                    enable: true,
                    max_selectivity: Some(0.5.into())
                })
            ),
            one_hop_result,
        );
        assert_eq!(
            search(
                &segment,
                false,
                &deleted,
                Some(AcornSearchParams {
                    enable: true,
                    max_selectivity: Some(1.0.into()),
                })
            ),
            expected,
        );

        delete(&mut segment, &mut deleted, 0, 20);
        assert_eq!(search(&segment, true, &deleted, None), expected);
        assert_eq!(search(&segment, false, &deleted, None), expected);

        {
            let id_tracker = segment.id_tracker.borrow();
            let vector_storage = segment.vector_data[DEFAULT_VECTOR_NAME]
                .vector_storage
                .borrow();
            let deleted_points = if matches!(deletion, Deletion::Proxy) {
                deleted.as_bitslice()
            } else {
                id_tracker.deleted_point_bitslice()
            };
            let scorer = FilteredScorer::new(
                query.clone(),
                &*vector_storage,
                None::<&QuantizedVectors>,
                None,
                deleted_points,
            )
            .unwrap();
            let requested_seeds = Cell::new(None);
            let no_seeds = |n: usize| {
                requested_seeds.set(Some(n));
                Ok(Vec::new())
            };
            // The reader can return no seeds, so the huge beam never needs allocation.
            let result = graph
                .search(GraphSearchArgs {
                    top: 1,
                    ef: usize::MAX / 2 + 1,
                    algorithm: SearchAlgorithm::Acorn,
                    scorers: SearchScorers::Regular(scorer),
                    custom_entry_points: None,
                    filtered_points_reader: Some(&no_seeds),
                    is_stopped: &crate::vector_storage::DEFAULT_STOPPED,
                })
                .unwrap();
            assert!(result.is_empty());
            assert_eq!(requested_seeds.get(), Some(usize::MAX));
        }

        delete(
            &mut segment,
            &mut deleted,
            (point_count - 1) as PointOffsetType,
            21,
        );
        assert!(search(&segment, true, &deleted, None).is_empty());
        assert!(search(&segment, false, &deleted, None).is_empty());
        if matches!(deletion, Deletion::Vector) {
            // A cancelled context detects any needless scan of the remaining point mappings.
            query_context
                .is_stopped_handle()
                .store(true, std::sync::atomic::Ordering::Relaxed);
            assert!(search(&segment, false, &deleted, None).is_empty());
        }
        assert_eq!(
            telemetry
                .acorn
                .lock()
                .get_statistics(TelemetryDetail::default())
                .count,
            0
        );
        assert_eq!(
            telemetry
                .unfiltered_hnsw
                .lock()
                .get_statistics(TelemetryDetail::default())
                .count,
            if matches!(deletion, Deletion::Vector) {
                10
            } else {
                9
            }
        );
    }

    #[test]
    fn unfiltered_payload_only_search_includes_vectors_outside_payload_blocks() {
        let _scope = ambient::test_guard();
        let dir = tempfile::tempdir().unwrap();
        let mut segment = build_simple_segment(dir.path(), 2, Distance::Dot).unwrap();
        for (point, vector) in [[0.0, 1.0], [0.1, 0.9], [0.2, 0.8], [1.0, 0.0]]
            .iter()
            .enumerate()
        {
            segment
                .upsert_point(
                    point as u64,
                    (point as u64).into(),
                    only_default_vector(vector),
                )
                .unwrap();
        }
        let hnsw_m = HnswM::new2(0);
        let mut entry_points = EntryPoints::new(1);
        entry_points.new_point(0, 0, |_| true);
        let graph = HnswGraph::<MmapFile>::Direct(Arc::new(GraphLayers {
            hnsw_m,
            links: GraphLinks::new_from_edges(
                vec![vec![vec![]]; 4],
                GraphLinksFormatParam::Plain,
                hnsw_m,
            )
            .unwrap(),
            entry_points,
            visited_pool: VisitedPool::new(),
            residency: GraphLinksResidency::Pinned,
        }));
        segment.delete_point(10, 0.into()).unwrap();
        let config = HnswGraphConfig::new(0, 8, 0, 1, Some(2), 4);
        let telemetry = HNSWSearchesTelemetry::new();
        let id_tracker = segment.id_tracker.borrow();
        let vector_storage = segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_storage
            .borrow();
        let query_context = QueryContext::default();
        let segment_context = query_context.get_segment_query_context();
        let vector_context = segment_context.get_vector_context(DEFAULT_VECTOR_NAME, None);
        let query = QueryVector::Nearest(vec![1.0, 0.0].into());

        let result = segment
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
                        hnsw_ef: Some(1),
                        ..Default::default()
                    }),
                    &vector_context,
                )
            })
            .unwrap();
        assert_eq!(result, vec![vec![ScoredPointOffset { idx: 3, score: 1.0 }]]);
        assert_eq!(
            telemetry
                .unfiltered_plain
                .lock()
                .get_statistics(TelemetryDetail::default())
                .count,
            1
        );
        assert_eq!(
            telemetry
                .unfiltered_hnsw
                .lock()
                .get_statistics(TelemetryDetail::default())
                .count,
            0
        );
    }
}
