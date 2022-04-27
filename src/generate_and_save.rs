use rand::rngs::StdRng;
use rand::{thread_rng, SeedableRng};
use segment::fixtures::index_fixtures::{random_vector, FakeFilterContext, TestRawScorerProducer};
use segment::index::hnsw_index::graph_layers::GraphLayers;
use segment::index::hnsw_index::point_scorer::FilteredScorer;
use segment::spaces::simple::DotProductMetric;
use segment::types::PointOffsetType;
use std::time::{Duration, Instant};

const NUM_VECTORS: usize = 100_000;
const DIM: usize = 10;
const M: usize = 16;
const TOP: usize = 10;
const EF_CONSTRUCT: usize = 100;
const EF: usize = 100;
const USE_HEURISTIC: bool = true;

fn main() {
    println!("Start {:?}", NUM_VECTORS);
    let mut rng = StdRng::seed_from_u64(42);

    let mut search : Vec<Vec<f32>> = vec![];
    for _ in 0..1000 {
        search.push(random_vector(&mut rng, DIM));
    }

    let vector_holder = TestRawScorerProducer::new(DIM, NUM_VECTORS, DotProductMetric {}, &mut rng);
    let mut rng = thread_rng();
    let fake_filter_context = FakeFilterContext {};

    let start = Instant::now();

    let mut graph_layers = GraphLayers::new(NUM_VECTORS, M, M * 2, EF_CONSTRUCT, 10, USE_HEURISTIC);
    for idx in 0..(NUM_VECTORS as PointOffsetType) {
        let added_vector = vector_holder.vectors.get(idx).to_vec();
        let raw_scorer = vector_holder.get_raw_scorer(added_vector);
        let scorer = FilteredScorer::new(&raw_scorer, Some(&fake_filter_context));
        let level = graph_layers.get_random_layer(&mut rng);
        graph_layers.link_new_point(idx, level, scorer);
    }

    let duration = start.elapsed();
    println!("Build time: {:?}", duration.as_millis());

    graph_layers.save(&std::path::Path::new("./graph.bin")).unwrap();
}
