use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;
use criterion::measurement::Measurement;
use criterion::{Criterion, criterion_group, criterion_main};
use dataset::Dataset;
use fs_err as fs;
use indicatif::{ProgressBar, ProgressDrawTarget};
use itertools::Itertools;
use rand::SeedableRng as _;
use rand::rngs::StdRng;
use sha2::Digest;
use sparse::SearchScratchPool;
use sparse::common::sparse_vector::{RemappedSparseVector, SparseVector};
use sparse::common::sparse_vector_fixture::{random_positive_sparse_vector, random_sparse_vector};
use sparse::common::types::{QuantizedU8, Weight};
use sparse::index::inverted_index::InvertedIndex;
use sparse::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use sparse::index::inverted_index::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use sparse::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;
use sparse::index::loaders::{self, Csr};
use sparse::index::posting_list::PostingList;
use sparse::index::posting_list_common::PostingElementEx;
use sparse::index::search_context::SearchContext;
use zerocopy::IntoBytes;
mod prof;

const NUM_QUERIES: usize = 2048;
const MAX_SPARSE_DIM: usize = 30_000;
const TOP: usize = 10;

pub fn bench_search(c: &mut Criterion) {
    bench_uniform_random(c, "random-50k", 50_000);
    bench_uniform_random(c, "random-500k", 500_000);

    {
        let query_vectors = Csr::open(Dataset::NeurIps2023Queries.download().unwrap())
            .unwrap()
            .iter()
            .unwrap()
            .collect::<Vec<_>>();

        let name = "neurips2023-1M";
        let index_1m = cached_ram_index(name, || {
            load_csr_index(Dataset::NeurIps2023_1M.download().unwrap(), 1.0)
        });
        run_bench(c, name, index_1m, &query_vectors);

        let name = "neurips2023-full-25pct";
        let index_full = cached_ram_index(name, || {
            load_csr_index(Dataset::NeurIps2023Full.download().unwrap(), 0.25)
        });
        run_bench(c, name, index_full, &query_vectors);
    }

    bench_movies(c);
}

fn bench_uniform_random(c: &mut Criterion, name: &str, num_vectors: usize) {
    let mut rnd = StdRng::seed_from_u64(42);

    let index = cached_ram_index(name, || {
        let mut rnd = rnd.fork();
        InvertedIndexBuilder::build_from_iterator((0..num_vectors).map(|idx| {
            let vec = random_sparse_vector(&mut rnd, MAX_SPARSE_DIM).into_remapped();
            (idx as PointOffsetType, vec)
        }))
    });

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

    macro_rules! run_bench2 {
        ($name:literal, $type:ty) => {
            let index_path = cached_compressed_index::<$type>(&index, name);

            run_bench2(
                c.benchmark_group(format!("search/ram_{}/{name}", $name)),
                &InvertedIndexCompressedImmutableRam::<$type>::open(&index_path).unwrap(),
                query_vectors,
                &hottest_query_vectors,
            );

            run_bench2(
                c.benchmark_group(format!("search/mmap_{}/{name}", $name)),
                &InvertedIndexCompressedMmap::<$type, MmapFile>::open(&index_path).unwrap(),
                query_vectors,
                &hottest_query_vectors,
            );
        };
    }

    run_bench2!("c32", f32);
    run_bench2!("c16", half::f16);
    // run_bench2!("c8", u8);
    run_bench2!("q8", QuantizedU8);
}

fn run_bench2(
    mut group: criterion::BenchmarkGroup<'_, impl Measurement>,
    index: &impl InvertedIndex,
    query_vectors: &[SparseVector],
    hottest_query_vectors: &[RemappedSparseVector],
) {
    let pool = SearchScratchPool::new();
    let stopped = AtomicBool::new(false);

    let mut it = query_vectors.iter().cycle();

    let hardware_counter = HardwareCounterCell::new();

    group.bench_function("basic", |b| {
        b.iter_batched(
            || it.next().unwrap().clone().into_remapped(),
            |vec| {
                let mut scratch = pool.get();
                SearchContext::new(vec, TOP, index, &mut scratch, &stopped, &hardware_counter)
                    .unwrap()
                    .search(&|_| true)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    let hardware_counter = HardwareCounterCell::new();

    let mut it = hottest_query_vectors.iter().cycle();
    group.bench_function("hottest", |b| {
        b.iter_batched(
            || it.next().unwrap().clone(),
            |vec| {
                let mut scratch = pool.get();
                SearchContext::new(vec, TOP, index, &mut scratch, &stopped, &hardware_counter)
                    .unwrap()
                    .search(&|_| true)
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn load_csr_index(path: impl AsRef<Path>, ratio: f32) -> InvertedIndexRam {
    let csr = Csr::open(path.as_ref()).unwrap();
    let mut builder = InvertedIndexBuilder::new();
    assert!(ratio > 0.0 && ratio <= 1.0);
    let count = (csr.len() as f32 * ratio) as usize;
    let bar =
        ProgressBar::with_draw_target(Some(count as u64), ProgressDrawTarget::stderr_with_hz(12));
    for (row, vec) in bar.wrap_iter(csr.iter().unwrap().take(count).enumerate()) {
        builder.add(row as u32, vec.into_remapped());
    }
    bar.finish_and_clear();
    builder.build()
}

fn cache_dir() -> PathBuf {
    Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(env!("CARGO_PKG_NAME"))
        .join(env!("CARGO_CRATE_NAME"))
}

/// Load an [`InvertedIndexRam`] from the cache.
/// If not exists, calls `build()` to create it.
fn cached_ram_index(name: &str, build: impl FnOnce() -> InvertedIndexRam) -> InvertedIndexRam {
    let path = cache_dir().join(name);
    if !path.exists() {
        eprintln!("Building cache: {path:?}");
        let index = build();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        index.test_save(&path.with_extension("tmp")).unwrap();
        fs::rename(path.with_extension("tmp"), &path).unwrap();
    }
    eprintln!("Using cache: {path:?}");
    InvertedIndexRam::test_load(&path).unwrap()
}

/// Loads [`InvertedIndexCompressedMmap`] from the cache.
/// If not exists, converts it from the given [`InvertedIndexRam`].
fn cached_compressed_index<W: Weight>(index: &InvertedIndexRam, name: &str) -> PathBuf {
    let path = cache_dir().join(format!(
        "{name}-{}-{hash}",
        W::NAME,
        hash = inverted_index_partial_hash(index)
    ));

    if !path.exists() {
        eprintln!("Building cache: {path:?}");
        let tmp_path = path.with_extension("tmp");
        if tmp_path.exists() {
            fs::remove_dir_all(&tmp_path).unwrap();
        }
        fs::create_dir_all(&tmp_path).unwrap();
        InvertedIndexCompressedMmap::<W, MmapFile>::from_ram_index(Cow::Borrowed(index), &tmp_path)
            .unwrap();
        fs::rename(tmp_path, &path).unwrap();
    }
    eprintln!("Using cache: {path:?}");
    path
}

/// Compute hash of the given [`InvertedIndexRam`].
/// For performance reasons, use only a subset of the data.
fn inverted_index_partial_hash(index: &InvertedIndexRam) -> String {
    let mut hasher = sha2::Sha256::new();
    let InvertedIndexRam {
        postings,
        vector_count,
        total_sparse_size,
    } = index;
    hasher.update(vector_count.as_bytes());
    hasher.update(total_sparse_size.as_bytes());

    let mut hash_posting = |posting: &PostingList| {
        let PostingList { elements } = posting;
        for element in elements.iter() {
            let PostingElementEx {
                record_id,
                weight,
                max_next_weight,
            } = element;
            hasher.update(record_id.as_bytes());
            hasher.update(weight.as_bytes());
            hasher.update(max_next_weight.as_bytes());
        }
    };

    // For performance reasons, hash only first and last 100 postings.
    let mut postings = postings.iter();
    for posting in postings.by_ref().take(100) {
        hash_posting(posting);
    }
    for posting in postings.rev().take(100) {
        hash_posting(posting);
    }

    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
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
