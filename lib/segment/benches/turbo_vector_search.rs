//! mmap vs io_uring batch scoring for `TurboVectorStorage` (TQDT).
//!
//! Three modes over the same single-file on-disk dataset:
//! - `unbatched-mmap`: per-point `score_point` loop — the pre-batching behavior;
//! - `batched-mmap`: `score_stored_batch` over the mmap backend;
//! - `batched-uring`: `score_stored_batch` over the io_uring backend (Linux).
//!
//! Groups:
//! - `tq-scoring-warm` / `tq-scoring-cold`: the fair pair — both score the same
//!   shuffled random-id subset (HNSW-like scatter); the ONLY difference is that
//!   the cold group drops the page cache before every iteration.
//! - `tq-scoring-scan-warm`: full ascending scan, page-cache warm — the plain
//!   (non-indexed) search shape. Context only: sequential cached reads are
//!   mmap's absolute best case, so this group is not comparable to the
//!   scattered-read groups above.
//!
//! The dataset lives under `CARGO_TARGET_TMPDIR`, NOT the system tempdir:
//! `/tmp` is commonly tmpfs, where `clear_cache()` cannot evict anything and
//! cold numbers would silently measure RAM.

use std::hint::black_box;
use std::path::Path;
use std::time::Duration;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, Criterion, criterion_group, criterion_main};
use rand::distr::StandardUniform;
use rand::rngs::SmallRng;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::{Rng, RngExt};
use segment::data_types::vectors::{DenseVector, QueryVector};
use segment::fixtures::payload_context_fixture::create_id_tracker_fixture;
use segment::id_tracker::IdTrackerRead;
use segment::index::hnsw_index::point_scorer::BatchFilteredSearcher;
use segment::types::Distance;
use segment::vector_storage::turbo::{
    open_appendable_turbo_vector_storage, open_turbo_vector_storage_with_uring,
};
use segment::vector_storage::{
    DEFAULT_STOPPED, DenseTQVectorStorage, VectorStorage, VectorStorageEnum, new_raw_scorer,
};
use tempfile::TempDir;

const DIM: usize = 1024;
const DISTANCE: Distance = Distance::Dot;
const VECTORS: usize = 200_000;
/// Random ids scored per subset iteration — models HNSW's scattered reads
/// while keeping one iteration tractable even in the fully serial unbatched
/// cold mode.
const SUBSET: usize = 4096;
const TOP: usize = 10;

fn random_vector(rng: &mut impl Rng, size: usize) -> DenseVector {
    rng.sample_iter(StandardUniform).take(size).collect()
}

fn random_query() -> QueryVector {
    QueryVector::from(random_vector(&mut rand::make_rng::<SmallRng>(), DIM))
}

/// Ids to score in one subset iteration: a shuffled sample without replacement.
fn subset_ids() -> Vec<PointOffsetType> {
    let mut rng = rand::make_rng::<SmallRng>();
    let mut ids: Vec<PointOffsetType> = (0..VECTORS as PointOffsetType).sample(&mut rng, SUBSET);
    ids.shuffle(&mut rng);
    ids
}

/// Build the single-file TQ dataset once: encode through an in-RAM appendable
/// storage, then bulk-append the encoded bytes into the single-file layout,
/// exactly as the optimizer does.
fn build_dataset(dir: &Path) {
    let mut rng = rand::make_rng::<SmallRng>();
    let hw_counter = HardwareCounterCell::new();

    let encoder_dir = TempDir::new().expect("encoder tempdir created");
    let mut encoder = open_appendable_turbo_vector_storage(encoder_dir.path(), DIM, DISTANCE, true)
        .expect("encoder storage created");
    for i in 0..VECTORS {
        let vector = random_vector(&mut rng, DIM);
        encoder
            .insert_vector(i as PointOffsetType, vector.as_slice().into(), &hw_counter)
            .expect("vector inserted");
    }

    let mut storage = open_turbo_vector_storage_with_uring(dir, DIM, DISTANCE, false, false)
        .expect("single-file storage created");
    let mut encoded =
        (0..VECTORS as PointOffsetType).map(|key| (encoder.get_quantized_vector(key), false));
    DenseTQVectorStorage::update_from(&mut storage, &mut encoded, &DEFAULT_STOPPED)
        .expect("dataset built");
}

/// Score `ids` one point at a time — the exact pre-batching read pattern.
fn score_unbatched(storage: &VectorStorageEnum, ids: impl Iterator<Item = PointOffsetType>) {
    let scorer = new_raw_scorer(random_query(), storage, HardwareCounterCell::new())
        .expect("scorer created");
    let mut acc = 0.0;
    for id in ids {
        acc += scorer.score_point(id);
    }
    black_box(acc);
}

/// Score the shuffled subset in every mode. Warm and cold both run through
/// here so the two groups differ in nothing but `clear_cache`.
fn bench_subset(
    group: &mut BenchmarkGroup<'_, WallTime>,
    modes: &[(&str, bool, &VectorStorageEnum)],
    point_deleted: &BitSlice,
    clear_cache: bool,
) {
    for &(label, batched, storage) in modes {
        if !clear_cache {
            storage.populate().expect("storage populated");
        }
        group.bench_function(label, |b| {
            b.iter_batched(
                || {
                    if clear_cache {
                        storage.clear_cache().expect("cache cleared");
                    }
                    subset_ids()
                },
                |ids| {
                    if batched {
                        let results = BatchFilteredSearcher::new_for_test(
                            std::slice::from_ref(&random_query()),
                            storage,
                            point_deleted,
                            TOP,
                        )
                        .peek_top_iter(ids.iter().copied(), &DEFAULT_STOPPED)
                        .expect("points scored");
                        black_box(results);
                    } else {
                        score_unbatched(storage, ids.iter().copied());
                    }
                },
                BatchSize::PerIteration,
            )
        });
    }
}

fn benchmark(c: &mut Criterion) {
    let data_dir = tempfile::Builder::new()
        .prefix("turbo-vector-search-bench")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("bench data dir created");
    build_dataset(data_dir.path());

    let mmap_storage = VectorStorageEnum::DenseTurbo(Box::new(
        open_turbo_vector_storage_with_uring(data_dir.path(), DIM, DISTANCE, false, false)
            .expect("mmap storage opened"),
    ));

    let modes: Vec<(&str, bool, &VectorStorageEnum)> = vec![
        ("unbatched-mmap", false, &mmap_storage),
        ("batched-mmap", true, &mmap_storage),
    ];

    cfg_select! {
        target_os = "linux" => {
            let uring_storage = VectorStorageEnum::DenseTurbo(Box::new(
                open_turbo_vector_storage_with_uring(data_dir.path(), DIM, DISTANCE, false, true)
                    .expect("uring storage opened"),
            ));

            let mut modes = modes;
            modes.push(("batched-uring", true, &uring_storage));
        }
        _ => {}
    };

    let id_tracker = create_id_tracker_fixture(VECTORS);

    let mut warm = c.benchmark_group("tq-scoring-warm");
    warm.sample_size(20);
    bench_subset(
        &mut warm,
        &modes,
        id_tracker.deleted_point_bitslice(),
        false,
    );
    warm.finish();

    let mut cold = c.benchmark_group("tq-scoring-cold");
    cold.sample_size(10);
    cold.warm_up_time(Duration::from_secs(1));
    bench_subset(&mut cold, &modes, id_tracker.deleted_point_bitslice(), true);
    cold.finish();

    // Context group: full ascending scan over everything, page-cache warm —
    // the plain (non-indexed) search shape and mmap's best case.
    let mut scan = c.benchmark_group("tq-scoring-scan-warm");
    scan.sample_size(20);
    for &(label, batched, storage) in &modes {
        storage.populate().expect("storage populated");
        scan.bench_function(label, |b| {
            b.iter_batched(
                random_query,
                |query| {
                    if batched {
                        let results = BatchFilteredSearcher::new_for_test(
                            std::slice::from_ref(&query),
                            storage,
                            id_tracker.deleted_point_bitslice(),
                            TOP,
                        )
                        .peek_top_all(&DEFAULT_STOPPED)
                        .expect("points scored");
                        black_box(results);
                    } else {
                        score_unbatched(storage, 0..VECTORS as PointOffsetType);
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    scan.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = benchmark,
}

criterion_main!(benches);
