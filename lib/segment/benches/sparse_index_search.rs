#[cfg(not(target_os = "windows"))]
mod prof;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common::cpu::CpuPermit;
use common::types::PointOffsetType;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use dataset::Dataset;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use itertools::Itertools as _;
use rand::rngs::StdRng;
use rand::SeedableRng;
use segment::fixtures::sparse_fixtures::fixture_sparse_index_ram_from_iter;
use segment::index::hnsw_index::num_rayon_threads;
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::index::sparse_index::sparse_vector_index::{
    SparseVectorIndex, SparseVectorIndexOpenArgs,
};
use segment::index::{PayloadIndex, VectorIndex};
use segment::types::PayloadSchemaType::Keyword;
use segment::types::{Condition, FieldCondition, Filter, Payload};
use serde_json::json;
use sparse::common::sparse_vector::SparseVector;
use sparse::common::sparse_vector_fixture::{random_positive_sparse_vector, random_sparse_vector};
use sparse::index::inverted_index::inverted_index_mmap::InvertedIndexMmap;
use sparse::index::loaders::Csr;
use tempfile::Builder;

const NUM_VECTORS: usize = 50_000;
const MAX_SPARSE_DIM: usize = 30_000;
const NUM_QUERIES: usize = 2048;
const TOP: usize = 10;
const FULL_SCAN_THRESHOLD: usize = 1; // low value to trigger index usage by default

fn sparse_vector_index_search_benchmark(c: &mut Criterion) {
    let mut rnd = StdRng::seed_from_u64(0);
    let query_vectors = (0..NUM_QUERIES)
        // Positive values to test pruning.
        .map(|_| random_positive_sparse_vector(&mut rnd, MAX_SPARSE_DIM))
        .collect::<Vec<_>>();

    let mut rnd = StdRng::seed_from_u64(42);
    let random_vectors = (0..NUM_VECTORS).map(|_| random_sparse_vector(&mut rnd, MAX_SPARSE_DIM));
    sparse_vector_index_search_benchmark_impl(c, "random-50k", random_vectors, &query_vectors);

    let dataset_vectors = Csr::open(Dataset::NeurIps2023_1M.download().unwrap()).unwrap();
    let query_vectors = Csr::open(Dataset::NeurIps2023Queries.download().unwrap())
        .unwrap()
        .iter()
        .map(|v| v.unwrap())
        .collect_vec();
    sparse_vector_index_search_benchmark_impl(
        c,
        "neurips2023-1M",
        dataset_vectors.iter().map(|v| v.unwrap()),
        &query_vectors,
    );
}

fn sparse_vector_index_search_benchmark_impl(
    c: &mut Criterion,
    group: &str,
    vectors: impl ExactSizeIterator<Item = SparseVector>,
    query_vectors: &[SparseVector],
) {
    let mut group = c.benchmark_group(format!("sparse_vector_index_search/{}", group));
    group.sample_size(10);

    let vectors_len = vectors.len();

    let stopped = AtomicBool::new(false);

    let data_dir = Builder::new().prefix("data_dir").tempdir().unwrap();

    let sparse_vector_index = fixture_sparse_index_ram_from_iter(
        progress("Indexing (1/3)", vectors_len).wrap_iter(vectors),
        FULL_SCAN_THRESHOLD,
        data_dir.path(),
        &stopped,
        || {
            let pb = progress("Indexing (2/3)", vectors_len);
            move || pb.inc(1)
        },
    );

    // adding payload on field
    let field_name = "field";
    let field_value = "important value";
    let payload: Payload = json!({
        field_name: field_value,
    })
    .into();

    // all points have the same payload
    let mut payload_index = sparse_vector_index.payload_index().borrow_mut();
    for idx in 0..NUM_VECTORS {
        payload_index
            .assign(idx as PointOffsetType, &payload, &None)
            .unwrap();
    }
    drop(payload_index);

    let mut query_vector_it = query_vectors.iter().cycle();

    let permit_cpu_count = num_rayon_threads(0);
    let permit = Arc::new(CpuPermit::dummy(permit_cpu_count as u32));

    // mmap inverted index
    let mmap_index_dir = Builder::new().prefix("mmap_index_dir").tempdir().unwrap();
    let sparse_index_config =
        SparseIndexConfig::new(Some(FULL_SCAN_THRESHOLD), SparseIndexType::Mmap);
    let mut sparse_vector_index_mmap: SparseVectorIndex<InvertedIndexMmap> =
        SparseVectorIndex::open(SparseVectorIndexOpenArgs {
            config: sparse_index_config,
            id_tracker: sparse_vector_index.id_tracker().clone(),
            vector_storage: sparse_vector_index.vector_storage().clone(),
            payload_index: sparse_vector_index.payload_index().clone(),
            path: mmap_index_dir.path(),
            stopped: &stopped,
        })
        .unwrap();
    let pb = progress("Indexing (3/3)", vectors_len);
    sparse_vector_index_mmap
        .build_index_with_progress(permit, &stopped, || pb.inc(1))
        .unwrap();
    pb.finish_and_clear();
    assert_eq!(sparse_vector_index_mmap.indexed_vector_count(), vectors_len);

    // intent: bench `search` without filter on mmap inverted index
    group.bench_function("mmap-inverted-index-search", |b| {
        b.iter_batched(
            || query_vector_it.next().unwrap().clone().into(),
            |vec| {
                let results = sparse_vector_index_mmap
                    .search(&[&vec], None, TOP, None, &Default::default())
                    .unwrap();

                assert_eq!(results[0].len(), TOP);
            },
            BatchSize::SmallInput,
        )
    });

    // intent: bench `search` without filter
    group.bench_function("inverted-index-search", |b| {
        b.iter_batched(
            || query_vector_it.next().unwrap().clone().into(),
            |vec| {
                let results = sparse_vector_index
                    .search(&[&vec], None, TOP, None, &Default::default())
                    .unwrap();

                assert_eq!(results[0].len(), TOP);
            },
            BatchSize::SmallInput,
        )
    });

    // filter by field
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        field_name.parse().unwrap(),
        field_value.to_owned().into(),
    )));

    // intent: bench plain search when the filtered payload key is not indexed
    if vectors_len < 100_000 {
        group.bench_function("inverted-index-filtered-plain", |b| {
            b.iter_batched(
                || query_vector_it.next().unwrap(),
                |vec| {
                    let mut prefiltered_points = None;
                    let results = sparse_vector_index
                        .search_plain(
                            vec,
                            &filter,
                            TOP,
                            &mut prefiltered_points,
                            &Default::default(),
                        )
                        .unwrap();

                    assert_eq!(results.len(), TOP);
                },
                BatchSize::SmallInput,
            )
        });
    }

    let mut payload_index = sparse_vector_index.payload_index().borrow_mut();

    // create payload field index
    payload_index
        .set_indexed(&field_name.parse().unwrap(), Keyword.into())
        .unwrap();

    drop(payload_index);

    // intent: bench `search` when the filtered payload key is indexed
    group.bench_function("inverted-index-filtered-payload-index", |b| {
        b.iter_batched(
            || query_vector_it.next().unwrap().clone().into(),
            |vec| {
                let results = sparse_vector_index
                    .search(&[&vec], Some(&filter), TOP, None, &Default::default())
                    .unwrap();

                assert_eq!(results[0].len(), TOP);
            },
            BatchSize::SmallInput,
        );
    });

    // intent: bench plain search when the filtered payload key is indexed
    if vectors_len < 100_000 {
        group.bench_function("plain-filtered-payload-index", |b| {
            b.iter_batched(
                || query_vector_it.next().unwrap(),
                |vec| {
                    let mut prefiltered_points = None;
                    let results = sparse_vector_index
                        .search_plain(
                            vec,
                            &filter,
                            TOP,
                            &mut prefiltered_points,
                            &Default::default(),
                        )
                        .unwrap();

                    assert_eq!(results.len(), TOP);
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn progress(name: &str, len: usize) -> ProgressBar {
    let pb =
        ProgressBar::with_draw_target(Some(len as u64), ProgressDrawTarget::stderr_with_hz(12));
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} {wide_bar} {pos}/{len} (eta:{eta})")
            .unwrap(),
    );
    pb.set_message(name.to_owned());
    pb
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = sparse_vector_index_search_benchmark
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = sparse_vector_index_search_benchmark,
}

criterion_main!(benches);
