//! HNSW graph-search benchmark on a real dataset (TEXMEX `.fvecs` files).
//!
//! Graphs built from random vectors have untypically low degree (the pruning
//! heuristic removes most edges), so results measured on them may not
//! transfer to real data. This bench builds the graph from dataset vectors
//! and searches with dataset queries instead.
//!
//! Usage (SIFT1M, <http://corpus-texmex.irisa.fr/>; the canonical FTP source
//! is often unresponsive, the Hugging Face mirror below serves the same
//! files over HTTPS):
//!
//! ```bash
//! for f in sift_base.fvecs sift_query.fvecs; do
//!     curl -sL -o $f "https://huggingface.co/datasets/qbo-odp/sift1m/resolve/main/$f"
//! done
//! FVECS_BASE=sift_base.fvecs FVECS_QUERY=sift_query.fvecs \
//!     cargo bench --bench hnsw_search_graph_dataset
//! ```
//!
//! Env vars:
//! - `FVECS_BASE` (required): base vectors, the graph is built from these
//! - `FVECS_QUERY` (required): query vectors, cycled through during search
//! - `FVECS_DISTANCE` (default `euclid`): `euclid`, `cosine`, or `dot`
//! - `FVECS_LIMIT` (default all): cap on the number of base vectors

#[cfg(not(target_os = "windows"))]
mod prof;

use std::hint::black_box;
use std::path::Path;

use criterion::{Criterion, criterion_group, criterion_main};
use segment::data_types::vectors::VectorElementType;
use segment::fixtures::index_fixtures::TestRawScorerProducer;
use segment::index::hnsw_index::graph_layers::SearchAlgorithm;
use segment::types::Distance;
use segment::vector_storage::DEFAULT_STOPPED;

const M: usize = 16;
const TOP: usize = 10;
const EF_CONSTRUCT: usize = 100;
const EF: usize = 100;
const USE_HEURISTIC: bool = true;

mod fixture;

fn hnsw_dataset_benchmark(c: &mut Criterion) {
    let Ok(base_path) = std::env::var("FVECS_BASE") else {
        eprintln!("FVECS_BASE not set, skipping dataset benchmark (see file header)");
        return;
    };
    let query_path = std::env::var("FVECS_QUERY").expect("FVECS_QUERY must be set");
    let distance = match std::env::var("FVECS_DISTANCE").as_deref() {
        Err(_) | Ok("euclid") => Distance::Euclid,
        Ok("cosine") => Distance::Cosine,
        Ok("dot") => Distance::Dot,
        Ok(other) => panic!("unsupported FVECS_DISTANCE: {other}"),
    };
    let limit = std::env::var("FVECS_LIMIT")
        .map(|limit| limit.parse().expect("FVECS_LIMIT must be a number"))
        .unwrap_or(usize::MAX);

    let base = fixture::read_fvecs(Path::new(&base_path), limit);
    let num_vectors = base.len();
    let dim = base[0].len();
    let base: Vec<_> = base
        .into_iter()
        .map(|vector| distance.preprocess_vector::<VectorElementType>(vector))
        .collect();
    let queries = fixture::read_fvecs(Path::new(&query_path), usize::MAX);
    eprintln!(
        "{num_vectors} base vectors of dim {dim}, {} queries, {distance:?}",
        queries.len()
    );

    let vector_holder = TestRawScorerProducer::from_dense_vectors(dim, distance, &base);

    let dataset = Path::new(&base_path).file_stem().unwrap().to_string_lossy();
    let cache_key =
        format!("{dataset}-{num_vectors}-{dim}-{M}-{EF_CONSTRUCT}-{USE_HEURISTIC}-{distance:?}");
    let mut graph_layers = fixture::build_or_load_graph(
        &cache_key,
        &vector_holder,
        num_vectors,
        M,
        EF_CONSTRUCT,
        USE_HEURISTIC,
    );

    let (_mmap_tmp, mmap_holder) = fixture::make_memmap_producer_from_vectors(&base, dim, distance);

    let mut group = c.benchmark_group("hnsw-search-graph-dataset");

    // Search the same graph over in-RAM and mmap-backed vector storage.
    for (name, holder) in [("uncompressed", &vector_holder), ("mmap", &mmap_holder)] {
        let mut query_idx = 0usize;
        group.bench_function(name, |b| {
            b.iter(|| {
                let query = queries[query_idx % queries.len()].clone();
                query_idx += 1;

                let scorer = holder.scorer(query);

                black_box(
                    graph_layers
                        .search(
                            TOP,
                            EF,
                            SearchAlgorithm::Hnsw,
                            scorer,
                            None,
                            &DEFAULT_STOPPED,
                        )
                        .unwrap(),
                );
            })
        });
    }

    graph_layers.compress_ram();
    let mut query_idx = 0usize;
    group.bench_function("compressed", |b| {
        b.iter(|| {
            let query = queries[query_idx % queries.len()].clone();
            query_idx += 1;

            let scorer = vector_holder.scorer(query);

            black_box(
                graph_layers
                    .search(
                        TOP,
                        EF,
                        SearchAlgorithm::Hnsw,
                        scorer,
                        None,
                        &DEFAULT_STOPPED,
                    )
                    .unwrap(),
            );
        })
    });

    group.finish();
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = hnsw_dataset_benchmark
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = hnsw_dataset_benchmark
}

criterion_main!(benches);
