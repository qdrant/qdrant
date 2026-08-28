//! Exhaustive (plain-index) search over 4-bit TurboQuant storages.
//!
//! Runs the exact driver a non-indexed search uses —
//! `BatchFilteredSearcher::peek_top_visible` over every live point — so the
//! fixed per-point cost of the scan (id harvesting, batching, scoring
//! dispatch, top-k maintenance) shows up next to the kernel cost.  Two
//! storages over the same normalized random dataset:
//!
//! - `datatype`: Turbo4 as the vector datatype (appendable chunked storage, in
//!   RAM);
//! - `quantized`: Turbo4 as quantization over a RAM dense storage.
//!
//! Throughput is reported per scored point.

#![allow(deprecated)]

use std::hint::black_box;

use common::bitvec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::distr::StandardUniform;
use rand::rngs::SmallRng;
use rand::{Rng, RngExt, SeedableRng};
use segment::data_types::vectors::{DenseVector, QueryVector};
use segment::index::hnsw_index::point_scorer::BatchFilteredSearcher;
use segment::types::{
    Distance, QuantizationConfig, TurboQuantBitSize, TurboQuantQuantizationConfig,
    TurboQuantization,
};
use segment::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use segment::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use segment::vector_storage::turbo::open_appendable_turbo_vector_storage;
use segment::vector_storage::{DEFAULT_STOPPED, VectorStorage, VectorStorageEnum};
use tempfile::TempDir;

const DIMS: &[usize] = &[64, 128, 256, 512, 1024];
const DISTANCE: Distance = Distance::Dot;
const VECTORS: usize = 200_000;
const TOP: usize = 10;

fn random_unit_vector(rng: &mut impl Rng, dim: usize) -> DenseVector {
    let vector: DenseVector = rng.sample_iter(StandardUniform).take(dim).collect();
    let norm = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    vector.into_iter().map(|x| x / norm).collect()
}

struct Dataset {
    dense: VectorStorageEnum,
    quantized: QuantizedVectors,
    turbo: VectorStorageEnum,
    query: QueryVector,
    point_deleted: BitVec,
    _dirs: [TempDir; 2],
}

fn build_dataset(dim: usize) -> Dataset {
    let mut rng = SmallRng::seed_from_u64(dim as u64);
    let hw_counter = HardwareCounterCell::new();

    let turbo_dir = TempDir::new().expect("turbo tempdir created");
    let mut turbo = VectorStorageEnum::DenseTurboAppendableMemmap(Box::new(
        open_appendable_turbo_vector_storage(turbo_dir.path(), dim, DISTANCE, true)
            .expect("turbo storage created"),
    ));
    let mut dense = new_volatile_dense_vector_storage(dim, DISTANCE);
    for i in 0..VECTORS as PointOffsetType {
        let vector = random_unit_vector(&mut rng, dim);
        dense
            .insert_vector(i, vector.as_slice().into(), &hw_counter)
            .expect("dense vector inserted");
        turbo
            .insert_vector(i, vector.as_slice().into(), &hw_counter)
            .expect("turbo vector inserted");
    }

    let quantized_dir = TempDir::new().expect("quantization tempdir created");
    let config = QuantizationConfig::Turbo(TurboQuantization {
        turbo: TurboQuantQuantizationConfig {
            always_ram: None,
            memory: None,
            bits: Some(TurboQuantBitSize::Bits4),
        },
    });
    let quantized = QuantizedVectors::create(
        &dense,
        &config,
        QuantizedVectorsStorageType::Immutable,
        quantized_dir.path(),
        4,
        &DEFAULT_STOPPED,
    )
    .expect("quantized vectors created");

    Dataset {
        dense,
        quantized,
        turbo,
        query: QueryVector::from(random_unit_vector(&mut rng, dim)),
        point_deleted: BitVec::repeat(false, VECTORS),
        _dirs: [turbo_dir, quantized_dir],
    }
}

/// One exhaustive search: build the searcher (query preprocessing) and scan
/// every point through the visible-scan driver.
fn full_scan(dataset: &Dataset, quantized: bool) {
    let queries = [&dataset.query];
    let empty = BitVec::new();
    let searcher = if quantized {
        BatchFilteredSearcher::new(
            &queries,
            &dataset.dense,
            Some(&dataset.quantized),
            None,
            TOP,
            &dataset.point_deleted,
            HardwareCounterCell::new(),
        )
    } else {
        BatchFilteredSearcher::new(
            &queries,
            &dataset.turbo,
            None::<&QuantizedVectors>,
            None,
            TOP,
            &dataset.point_deleted,
            HardwareCounterCell::new(),
        )
    }
    .expect("searcher created");
    let results = searcher
        .peek_top_visible(None, &empty, &empty, &DEFAULT_STOPPED)
        .expect("scan completed");
    black_box(results);
}

/// Dims to run: `TURBO_SCAN_DIMS=64,128` overrides [`DIMS`], so a single
/// dataset can be rebuilt and measured while iterating on the scan path.
fn dims() -> Vec<usize> {
    match std::env::var("TURBO_SCAN_DIMS") {
        Ok(dims) => dims
            .split(',')
            .map(|dim| {
                dim.trim()
                    .parse()
                    .expect("TURBO_SCAN_DIMS: dims are integers")
            })
            .collect(),
        Err(_) => DIMS.to_vec(),
    }
}

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("turbo4_full_scan");
    group.sample_size(20);
    for dim in dims() {
        let dataset = build_dataset(dim);
        group.throughput(Throughput::Elements(VECTORS as u64));
        group.bench_with_input(BenchmarkId::new("datatype", dim), &dim, |b, _| {
            b.iter(|| full_scan(&dataset, false));
        });
        group.bench_with_input(BenchmarkId::new("quantized", dim), &dim, |b, _| {
            b.iter(|| full_scan(&dataset, true));
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = benchmark,
}

criterion_main!(benches);
