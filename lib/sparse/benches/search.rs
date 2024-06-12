use std::borrow::Cow;
use std::io;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::types::PointOffsetType;
use criterion::measurement::Measurement;
use criterion::{criterion_group, criterion_main, Criterion};
use dataset::Dataset;
use indicatif::{ProgressBar, ProgressDrawTarget};
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use sparse::common::scores_memory_pool::ScoresMemoryPool;
use sparse::common::sparse_vector::{RemappedSparseVector, SparseVector};
use sparse::common::sparse_vector_fixture::{random_positive_sparse_vector, random_sparse_vector};
use sparse::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use sparse::index::inverted_index::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
use sparse::index::inverted_index::inverted_index_mmap::InvertedIndexMmap;
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use sparse::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;
use sparse::index::inverted_index::InvertedIndex;
use sparse::index::loaders::{self, Csr};
use sparse::index::search_context::SearchContext;
mod prof;

const NUM_QUERIES: usize = 2048;
const MAX_SPARSE_DIM: usize = 30_000;
const TOP: usize = 10;

pub fn bench_search(c: &mut Criterion) {
    bench_uniform_random(c, "random-50k", 50_000);
    bench_uniform_random(c, "random-500k", 500_000);

    {
        let query_vectors =
            loaders::load_csr_vecs(Dataset::NeurIps2023Queries.download().unwrap()).unwrap();

        let index_1m = load_csr_index(Dataset::NeurIps2023_1M.download().unwrap(), 1.0).unwrap();
        run_bench(c, "neurips2023-1M", index_1m, &query_vectors);

        let index_full =
            load_csr_index(Dataset::NeurIps2023Full.download().unwrap(), 0.25).unwrap();
        run_bench(c, "neurips2023-full-25pct", index_full, &query_vectors);
    }

    bench_movies(c);
}

fn bench_uniform_random(c: &mut Criterion, name: &str, num_vectors: usize) {
    let mut rnd = StdRng::seed_from_u64(42);

    let index = InvertedIndexBuilder::build_from_iterator((0..num_vectors).map(|idx| {
        (
            idx as PointOffsetType,
            random_sparse_vector(&mut rnd, MAX_SPARSE_DIM).into_remapped(),
        )
    }));

    let query_vectors = (0..NUM_QUERIES)
        .map(|_| random_positive_sparse_vector(&mut rnd, MAX_SPARSE_DIM))
        .collect::<Vec<_>>();

    run_bench(c, name, index, &query_vectors);
}

pub fn bench_movies(c: &mut Criterion) {
    let mut iter =
        loaders::JsonReader::open(Dataset::SpladeWikiMovies.download().unwrap()).unwrap();

    // Use the first NUM_QUERIES vectors as queries, and the rest as index.

    let query_vectors = (0..NUM_QUERIES)
        .map(|_| iter.next().unwrap().unwrap())
        .collect_vec();

    let index = InvertedIndexBuilder::build_from_iterator(
        iter.enumerate()
            .map(|(idx, vec)| (idx as PointOffsetType, vec.unwrap().into_remapped())),
    );

    run_bench(c, "movies", index, &query_vectors);
}

pub fn run_bench(
    c: &mut Criterion,
    name: &str,
    index: InvertedIndexRam,
    query_vectors: &[SparseVector],
) {
    let hottest_id = index
        .postings
        .iter()
        .enumerate()
        .map(|(i, p)| (i, p.elements.len()))
        .max_by_key(|(_, len)| *len)
        .unwrap()
        .0 as u32;

    let average_elements = index
        .postings
        .iter()
        .map(|p| p.elements.len())
        .sum::<usize>() as f64
        / index.postings.len() as f64;

    eprintln!(
        "Hottest id: {hottest_id} (elements: {}), average elements: {average_elements}",
        index.postings[hottest_id as usize].elements.len(),
    );

    let hottest_query_vectors = query_vectors
        .iter()
        .cloned()
        .map(|mut vec| {
            vec.indices.truncate(4);
            vec.values.truncate(4);
            if let Err(idx) = vec.indices.binary_search(&hottest_id) {
                if idx < vec.indices.len() {
                    vec.indices[idx] = hottest_id;
                    vec.values[idx] = 1.0;
                } else {
                    vec.indices.push(hottest_id);
                    vec.values.push(1.0);
                }
            }
            vec.into_remapped()
        })
        .collect::<Vec<_>>();

    run_bench2(
        c.benchmark_group(format!("search/ram/{name}")),
        &index,
        query_vectors,
        &hottest_query_vectors,
    );

    run_bench2(
        c.benchmark_group(format!("search/ram_c32/{name}")),
        &InvertedIndexCompressedImmutableRam::<f32>::from_ram_index(
            Cow::Borrowed(&index),
            "nonexistent/path",
        )
        .unwrap(),
        query_vectors,
        &hottest_query_vectors,
    );

    run_bench2(
        c.benchmark_group(format!("search/ram_c16/{name}")),
        &InvertedIndexCompressedImmutableRam::<half::f16>::from_ram_index(
            Cow::Borrowed(&index),
            "nonexistent/path",
        )
        .unwrap(),
        query_vectors,
        &hottest_query_vectors,
    );

    run_bench2(
        c.benchmark_group(format!("search/mmap/{name}")),
        &InvertedIndexMmap::from_ram_index(
            Cow::Borrowed(&index),
            tempfile::Builder::new()
                .prefix("test_index_dir")
                .tempdir()
                .unwrap()
                .path(),
        )
        .unwrap(),
        query_vectors,
        &hottest_query_vectors,
    );

    run_bench2(
        c.benchmark_group(format!("search/mmap_c32/{name}")),
        &InvertedIndexCompressedMmap::<f32>::from_ram_index(
            Cow::Borrowed(&index),
            tempfile::Builder::new()
                .prefix("test_index_dir")
                .tempdir()
                .unwrap()
                .path(),
        )
        .unwrap(),
        query_vectors,
        &hottest_query_vectors,
    );

    run_bench2(
        c.benchmark_group(format!("search/mmap_c16/{name}")),
        &InvertedIndexCompressedMmap::<half::f16>::from_ram_index(
            Cow::Borrowed(&index),
            tempfile::Builder::new()
                .prefix("test_index_dir")
                .tempdir()
                .unwrap()
                .path(),
        )
        .unwrap(),
        query_vectors,
        &hottest_query_vectors,
    );
}

fn run_bench2(
    mut group: criterion::BenchmarkGroup<'_, impl Measurement>,
    index: &impl InvertedIndex,
    query_vectors: &[SparseVector],
    hottest_query_vectors: &[RemappedSparseVector],
) {
    let pool = ScoresMemoryPool::new();
    let stopped = AtomicBool::new(false);

    let mut it = query_vectors.iter().cycle();
    group.bench_function("basic", |b| {
        b.iter_batched(
            || it.next().unwrap().clone().into_remapped(),
            |vec| SearchContext::new(vec, TOP, index, pool.get(), &stopped).search(&|_| true),
            criterion::BatchSize::SmallInput,
        )
    });

    let mut it = hottest_query_vectors.iter().cycle();
    group.bench_function("hottest", |b| {
        b.iter_batched(
            || it.next().unwrap().clone(),
            |vec| SearchContext::new(vec, TOP, index, pool.get(), &stopped).search(&|_| true),
            criterion::BatchSize::SmallInput,
        )
    });
}

fn load_csr_index(path: impl AsRef<Path>, ratio: f32) -> io::Result<InvertedIndexRam> {
    let csr = Csr::open(path.as_ref())?;
    let mut builder = InvertedIndexBuilder::new();
    assert!(ratio > 0.0 && ratio <= 1.0);
    let count = (csr.len() as f32 * ratio) as usize;
    let bar =
        ProgressBar::with_draw_target(Some(count as u64), ProgressDrawTarget::stderr_with_hz(12));
    for (row, vec) in bar.wrap_iter(csr.iter().take(count).enumerate()) {
        builder.add(
            row as u32,
            vec.map(|v| v.into_remapped())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        );
    }
    bar.finish_and_clear();
    Ok(builder.build())
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = bench_search,
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_search,
}

criterion_main!(benches);
