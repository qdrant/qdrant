use rand::rngs::StdRng;
use rand::{thread_rng, SeedableRng};
use segment::fixtures::index_fixtures::{random_vector, FakeFilterContext, TestRawScorerProducer};
use segment::index::hnsw_index::graph_layers::GraphLayers;
use segment::index::hnsw_index::point_scorer::FilteredScorer;
use segment::spaces::simple::DotProductMetric;
use segment::types::PointOffsetType;
use std::time::{Duration, Instant};

const SEARCH_VECTORS: usize = 10_000;
const NUM_VECTORS: usize = 500_000;
const DIM: usize = 32;
const TOP: usize = 10;
const EF: usize = 100;

fn main() {
    println!("Start {:?}", NUM_VECTORS);
    let mut rng = StdRng::seed_from_u64(42);

    let mut search: Vec<Vec<f32>> = vec![];
    for _ in 0..SEARCH_VECTORS {
        search.push(random_vector(&mut rng, DIM));
    }

    let vector_holder = TestRawScorerProducer::new(DIM, NUM_VECTORS, DotProductMetric {}, &mut rng);
    let fake_filter_context = FakeFilterContext {};

    let graph_layers = GraphLayers::load(&std::path::Path::new("./graph.bin")).unwrap();

    let start = Instant::now();

    for i in 0..search.len() {
        let raw_scorer = vector_holder.get_raw_scorer(search[i].clone());
        let scorer = FilteredScorer::new(&raw_scorer, Some(&fake_filter_context));
        graph_layers.search(TOP, EF, scorer).len();
    }

    let duration = start.elapsed();
    println!(
        "Search time: {:?}",
        duration.as_micros() / search.len() as u128,
    );
    println!("");
}
