mod test_compact_graph_layer;
mod test_graph_connectivity;

use common::types::PointOffsetType;
use rand::Rng;

use super::graph_links::GraphLinksFormat;
use crate::fixtures::index_fixtures::TestRawScorerProducer;
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::types::Distance;

pub(crate) fn create_graph_layer_builder_fixture<R: Rng + ?Sized>(
    num_vectors: usize,
    m: usize,
    dim: usize,
    use_heuristic: bool,
    use_quantization: bool,
    distance: Distance,
    rng: &mut R,
) -> (TestRawScorerProducer, GraphLayersBuilder) {
    let ef_construct = 16;
    let entry_points_num = 10;

    let vector_holder =
        TestRawScorerProducer::new(dim, distance, num_vectors, use_quantization, rng);

    let mut graph_layers_builder = GraphLayersBuilder::new(
        num_vectors,
        HnswM::new2(m),
        ef_construct,
        entry_points_num,
        use_heuristic,
    );

    for idx in 0..(num_vectors as PointOffsetType) {
        let level = graph_layers_builder.get_random_layer(rng);
        graph_layers_builder.set_levels(idx, level);
        graph_layers_builder.link_new_point(idx, vector_holder.internal_scorer(idx));
    }
    (vector_holder, graph_layers_builder)
}

#[expect(clippy::too_many_arguments)]
pub(crate) fn create_graph_layer_fixture<R: Rng + ?Sized>(
    num_vectors: usize,
    m: usize,
    dim: usize,
    format: GraphLinksFormat,
    use_heuristic: bool,
    use_quantization: bool,
    distance: Distance,
    rng: &mut R,
) -> (TestRawScorerProducer, GraphLayers) {
    let (vector_holder, graph_layers_builder) = create_graph_layer_builder_fixture(
        num_vectors,
        m,
        dim,
        use_heuristic,
        use_quantization,
        distance,
        rng,
    );
    let graph_layers = graph_layers_builder
        .into_graph_layers_ram(format.with_param_for_tests(vector_holder.quantized_vectors()));
    (vector_holder, graph_layers)
}
