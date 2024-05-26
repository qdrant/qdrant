mod test_compact_graph_layer;
mod test_graph_connectivity;

use std::path::Path;

use common::types::PointOffsetType;
use rand::Rng;

use super::graph_links::GraphLinksRam;
use crate::data_types::vectors::VectorElementType;
use crate::fixtures::index_fixtures::{FakeFilterContext, TestRawScorerProducer};
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::spaces::metric::Metric;

pub(crate) fn create_graph_layer_builder_fixture<TMetric: Metric<VectorElementType>, R>(
    num_vectors: usize,
    m: usize,
    dim: usize,
    use_heuristic: bool,
    rng: &mut R,
) -> (TestRawScorerProducer<TMetric>, GraphLayersBuilder)
where
    R: Rng + ?Sized,
{
    let ef_construct = 16;
    let entry_points_num = 10;

    let vector_holder = TestRawScorerProducer::<TMetric>::new(dim, num_vectors, rng);

    let mut graph_layers_builder = GraphLayersBuilder::new(
        num_vectors,
        m,
        m * 2,
        ef_construct,
        entry_points_num,
        use_heuristic,
    )
    .unwrap();

    for idx in 0..(num_vectors as PointOffsetType) {
        let fake_filter_context = FakeFilterContext {};
        let added_vector = vector_holder.vectors.get(idx).to_vec();
        let raw_scorer = vector_holder.get_raw_scorer(added_vector.clone()).unwrap();
        let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
        let level = graph_layers_builder.get_random_layer(rng);
        graph_layers_builder.set_levels(idx, level);
        graph_layers_builder.link_new_point(idx, scorer).unwrap();
    }
    (vector_holder, graph_layers_builder)
}

pub(crate) fn create_graph_layer_fixture<TMetric: Metric<VectorElementType>, R>(
    num_vectors: usize,
    m: usize,
    dim: usize,
    use_heuristic: bool,
    rng: &mut R,
    links_path: Option<&Path>,
) -> (TestRawScorerProducer<TMetric>, GraphLayers<GraphLinksRam>)
where
    R: Rng + ?Sized,
{
    let (vector_holder, graph_layers_builder) =
        create_graph_layer_builder_fixture(num_vectors, m, dim, use_heuristic, rng);

    (
        vector_holder,
        graph_layers_builder.into_graph_layers(links_path).unwrap(),
    )
}
