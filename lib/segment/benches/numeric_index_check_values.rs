use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use segment::common::operation_error::OperationResult;
use segment::index::field_index::numeric_index::mmap_numeric_index::MmapNumericIndex;
use segment::index::field_index::numeric_index::mutable_numeric_index::InMemoryNumericIndex;
use tempfile::Builder;

mod prof;

const NUM_POINTS: usize = 100000;
const VALUES_PER_POINT: usize = 2;

fn get_random_payloads(rng: &mut StdRng, num_points: usize) -> Vec<(PointOffsetType, f64)> {
    let mut payloads = Vec::with_capacity(num_points);
    for i in 0..num_points {
        for _ in 0..VALUES_PER_POINT {
            let value: f64 = rng.random_range(0.0..1.0f64);
            payloads.push((i as PointOffsetType, value));
        }
    }
    payloads
}

pub fn struct_numeric_check_values(c: &mut Criterion) {
    let seed = 42;
    let mut rng = StdRng::seed_from_u64(seed);
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    let mut group = c.benchmark_group("numeric-check-values");

    let payloads: Vec<(PointOffsetType, f64)> = get_random_payloads(&mut rng, NUM_POINTS);
    let mutable_index: InMemoryNumericIndex<f64> = payloads
        .into_iter()
        .map(Ok)
        .collect::<OperationResult<InMemoryNumericIndex<_>>>()
        .unwrap();

    let hw_counter = HardwareCounterCell::new();

    let mut count = 0;
    group.bench_function("numeric-index", |b| {
        b.iter(|| {
            let random_index = rng.random_range(0..NUM_POINTS) as PointOffsetType;

            if mutable_index.check_values_any(random_index, |value| *value > 0.5) {
                count += 1;
            }
        })
    });

    let mmap_index = MmapNumericIndex::build(mutable_index, dir.path(), false).unwrap();

    group.bench_function("mmap-numeric-index", |b| {
        b.iter(|| {
            let random_index = rng.random_range(0..NUM_POINTS) as PointOffsetType;

            if mmap_index.check_values_any(random_index, |value| *value > 0.5, &hw_counter) {
                count += 1;
            }
        })
    });

    group.finish();
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = struct_numeric_check_values
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = struct_numeric_check_values
}

criterion_main!(benches);
