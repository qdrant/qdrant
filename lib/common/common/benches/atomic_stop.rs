use std::hint::black_box;
use std::sync::atomic::Ordering;

use common::iterator_ext::IteratorExt;
use common::iterator_ext::stoppable_iter::StoppableIter;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::RngExt;

fn bench_atomic_stop(c: &mut Criterion) {
    // Generate random number from 1 to 1_000_000
    let mut rng = rand::rng();

    c.bench_function("Sum regular iterator", |b| {
        b.iter(|| {
            let size = rng.random_range(1_000_000..=2_000_000);
            // Sum regular iterator
            let sum = (0..size).map(|x| black_box(x + 1)).sum::<u64>();
            black_box(sum);
        });
    });

    let stop_flag = std::sync::atomic::AtomicBool::new(false);

    c.bench_function("Sum with check every 100", |b| {
        b.iter(|| {
            let size = rng.random_range(1_000_000..=2_000_000);
            let sum = (0..size)
                .check_stop_every(100, || stop_flag.load(Ordering::Relaxed))
                .map(|x| black_box(x + 1))
                .sum::<u64>();
            black_box(sum);
        });
    });

    c.bench_function("Sum with stoppable", |b| {
        b.iter(|| {
            let size = rng.random_range(1_000_000..=2_000_000);
            let sum = StoppableIter::new(0..size, &stop_flag)
                .map(|x| black_box(x + 1))
                .sum::<u64>();
            black_box(sum);
        });
    });
}

criterion_group!(atomic_stop, bench_atomic_stop);
criterion_main!(atomic_stop);
