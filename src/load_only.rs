use rand::rngs::StdRng;
use rand::{thread_rng, SeedableRng};
use segment::fixtures::index_fixtures::{random_vector, FakeFilterContext, TestRawScorerProducer};
use segment::index::hnsw_index::graph_layers::GraphLayers;
use segment::index::hnsw_index::point_scorer::FilteredScorer;
use segment::spaces::simple::DotProductMetric;
use segment::types::PointOffsetType;
use std::time::{Duration, Instant};

const NUM_VECTORS: usize = 500_000;
const DIM: usize = 32;
const M: usize = 16;
const TOP: usize = 10;
const EF_CONSTRUCT: usize = 100;
const EF: usize = 100;
const USE_HEURISTIC: bool = true;

fn main() {
    //println!("input");
    //let mut input = String::new();
    //std::io::stdin().read_line(&mut input);
    println!("Start {:?}", NUM_VECTORS);
    let mut rng = StdRng::seed_from_u64(42);

    let mut search: Vec<Vec<f32>> = vec![];
    for _ in 0..10000 {
        search.push(random_vector(&mut rng, DIM));
    }

    let vector_holder = TestRawScorerProducer::new(DIM, NUM_VECTORS, DotProductMetric {}, &mut rng);
    let fake_filter_context = FakeFilterContext {};

    let graph_layers = GraphLayers::load(&std::path::Path::new("./graph.bin")).unwrap();
}
