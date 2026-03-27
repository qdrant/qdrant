use std::hint::black_box;

use common::atomic_bitvec::AtomicBitVec;
use common::bitvec::BitVec;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};

const BENCH_SIZE: usize = 1_000_000;

fn make_bitvec(rng: &mut StdRng) -> BitVec {
    rng.sample_iter::<bool, _>(rand::distr::StandardUniform)
        .take(BENCH_SIZE)
        .collect()
}

fn make_atomic_bitvec(rng: &mut StdRng) -> AtomicBitVec {
    let bv = make_bitvec(rng);
    let len = bv.len();
    let mut av = AtomicBitVec::from_slice(bv.as_raw_slice());
    av.resize(len, false);
    av
}

pub fn bench_bitvec(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let bv = make_bitvec(&mut rng);
    let mut group = c.benchmark_group("bitvec");

    group.bench_function("count_ones", |b| b.iter(|| black_box(&bv).count_ones()));

    group.bench_function("iter_count", |b| {
        b.iter(|| black_box(&bv).iter().by_vals().filter(|&v| v).count())
    });

    group.bench_function("get", |b| {
        let mut rng = StdRng::seed_from_u64(1);
        b.iter_batched(
            || rng.random_range(0..BENCH_SIZE),
            |idx| black_box(&bv).get(idx).map(|r| *r),
            BatchSize::SmallInput,
        )
    });

    let mut bv = bv;
    group.bench_function("set", |b| {
        let mut rng = StdRng::seed_from_u64(1);
        b.iter_batched(
            || (rng.random_range(0..BENCH_SIZE), rng.random::<bool>()),
            |(idx, val)| black_box(&mut bv).set(idx, val),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

pub fn bench_atomic_bitvec(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let av = make_atomic_bitvec(&mut rng);
    let mut group = c.benchmark_group("atomic_bitvec");

    group.bench_function("count_ones_scanning", |b| {
        b.iter(|| black_box(av.as_slice()).count_bits_in_range(0, av.len()))
    });

    group.bench_function("count_ones_caching", |b| {
        b.iter(|| black_box(&av).count_ones())
    });

    group.bench_function("iter_count", |b| {
        b.iter(|| black_box(av.as_slice()).iter().filter(|&v| v).count())
    });

    group.bench_function("get", |b| {
        let mut rng = StdRng::seed_from_u64(1);
        b.iter_batched(
            || rng.random_range(0..BENCH_SIZE),
            |idx| black_box(&av).get(idx),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("replace_concurrent", |b| {
        let mut rng = StdRng::seed_from_u64(1);
        b.iter_batched(
            || (rng.random_range(0..BENCH_SIZE), rng.random::<bool>()),
            |(idx, val)| black_box(&av).replace_concurrent(idx, val),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(benches, bench_bitvec, bench_atomic_bitvec);
criterion_main!(benches);
