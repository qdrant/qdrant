use rand::rngs::StdRng;
use rand::{thread_rng, SeedableRng};
use segment::fixtures::index_fixtures::{random_vector, FakeFilterContext, TestRawScorerProducer};
use segment::index::hnsw_index::graph_layers::GraphLayers;
use segment::index::hnsw_index::point_scorer::FilteredScorer;
use segment::spaces::simple::DotProductMetric;
use segment::types::PointOffsetType;
use std::time::{Duration, Instant};

const NUM_VECTORS: usize = 500_000;
const SEARCH_VECTORS: usize = 10_000;
const DIM: usize = 32;
const M: usize = 16;
const TOP: usize = 10;
const EF_CONSTRUCT: usize = 100;
const EF: usize = 100;
const USE_HEURISTIC: bool = true;

fn hnsw_benchmark(count: usize) {
    println!("Start {:?}", count);
    let mut rng = StdRng::seed_from_u64(42);

    let mut search: Vec<Vec<f32>> = vec![];
    for _ in 0..SEARCH_VECTORS {
        search.push(random_vector(&mut rng, DIM));
    }

    let vector_holder = TestRawScorerProducer::new(DIM, count, DotProductMetric {}, &mut rng);
    let mut rng = thread_rng();
    let fake_filter_context = FakeFilterContext {};

    let start = Instant::now();

    let mut graph_layers = GraphLayers::new(count, M, M * 2, EF_CONSTRUCT, 10, USE_HEURISTIC);
    for idx in 0..(count as PointOffsetType) {
        let added_vector = vector_holder.vectors.get(idx).to_vec();
        let raw_scorer = vector_holder.get_raw_scorer(added_vector);
        let scorer = FilteredScorer::new(&raw_scorer, Some(&fake_filter_context));
        let level = graph_layers.get_random_layer(&mut rng);
        graph_layers.link_new_point(idx, level, scorer);
    }

    let duration = start.elapsed();
    println!("Build time: {:?}", duration.as_millis());

    let start = Instant::now();

    for i in 0..search.len() / 10 {
        let raw_scorer = vector_holder.get_raw_scorer(search[i].clone());
        let scorer = FilteredScorer::new(&raw_scorer, Some(&fake_filter_context));
        graph_layers.search(TOP, EF, scorer);
    }

    let duration = start.elapsed();
    println!(
        "Search time: {:?}",
        10 * duration.as_micros() / search.len() as u128
    );
    println!("");
}

fn main() {
    hnsw_benchmark(NUM_VECTORS);
}
