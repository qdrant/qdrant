use std::cmp::max;

use common::types::ScoredPointOffset;
use itertools::Itertools;
use rand::prelude::StdRng;
use rand::SeedableRng;

use crate::fixtures::index_fixtures::random_vector;
use crate::index::hnsw_index::graph_layers::GraphLayersBase;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::graph_links::GraphLinksRam;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::hnsw_index::tests::create_graph_layer_builder_fixture;
use crate::spaces::simple::CosineMetric;

fn search_in_builder(
    builder: &GraphLayersBuilder,
    top: usize,
    ef: usize,
    mut points_scorer: FilteredScorer,
) -> Vec<ScoredPointOffset> {
    let entry_point = match builder
        .get_entry_points()
        .get_entry_point(|point_id| points_scorer.check_vector(point_id))
    {
        None => return vec![],
        Some(ep) => ep,
    };

    let zero_level_entry = builder.search_entry(
        entry_point.point_id,
        entry_point.level,
        0,
        &mut points_scorer,
    );

    let nearest = builder.search_on_level(zero_level_entry, 0, max(top, ef), &mut points_scorer);
    nearest.into_iter().take(top).collect_vec()
}

#[test]
/// Check that HNSW index with raw and compacted links gives the same results
fn test_compact_graph_layers() {
    let num_vectors = 1000;
    let num_queries = 100;
    let m = 16;
    let dim = 8;
    let top = 5;
    let ef = 100;

    let mut rng = StdRng::seed_from_u64(42);

    let (vector_holder, graph_layers_builder) =
        create_graph_layer_builder_fixture::<CosineMetric, _>(num_vectors, m, dim, false, &mut rng);

    let queries = (0..num_queries)
        .map(|_| random_vector(&mut rng, dim))
        .collect_vec();

    let reference_results = queries
        .iter()
        .map(|query| {
            let raw_scorer = vector_holder.get_raw_scorer(query.clone()).unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), None);
            search_in_builder(&graph_layers_builder, top, ef, scorer)
        })
        .collect_vec();

    let graph_layers = graph_layers_builder
        .into_graph_layers::<GraphLinksRam>(None)
        .unwrap();

    let results = queries
        .iter()
        .map(|query| {
            let raw_scorer = vector_holder.get_raw_scorer(query.clone()).unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), None);
            graph_layers.search(top, ef, scorer, None)
        })
        .collect_vec();

    assert_eq!(reference_results, results);
}
