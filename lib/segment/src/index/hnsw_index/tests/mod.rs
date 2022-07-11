use crate::fixtures::index_fixtures::{FakeFilterContext, TestRawScorerProducer};
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::spaces::metric::Metric;
use crate::types::PointOffsetType;
use rand::Rng;

pub(crate) fn create_graph_layer_fixture<TMetric: Metric, R>(
    num_vectors: usize,
    m: usize,
    dim: usize,
    use_heuristic: bool,
    rng: &mut R,
) -> (TestRawScorerProducer<TMetric>, GraphLayers)
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
    );

    for idx in 0..(num_vectors as PointOffsetType) {
        let fake_filter_context = FakeFilterContext {};
        let added_vector = vector_holder.vectors.get(idx).to_vec();
        let raw_scorer = vector_holder.get_raw_scorer(added_vector.clone());
        let scorer = FilteredScorer::new(&raw_scorer, Some(&fake_filter_context));
        let level = graph_layers_builder.get_random_layer(rng);
        graph_layers_builder.set_levels(idx, level);
        graph_layers_builder.link_new_point(idx, scorer);
    }

    (vector_holder, graph_layers_builder.into_graph_layers())
}
