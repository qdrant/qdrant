use std::iter;
use std::sync::atomic::AtomicBool;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use segment::common::operation_error::check_process_stopped;
use segment::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;
use tempfile::tempdir;

const FLAG_COUNT: usize = 50_000_000;

fn dynamic_mmap_flag_count(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let dir = tempdir().unwrap();
    let random_flags: Vec<bool> = iter::repeat_with(|| rng.gen()).take(FLAG_COUNT).collect();
    let stopped = AtomicBool::new(false);

    // Build dynamic mmap flags with random deletions
    let mut dynamic_flags = DynamicMmapFlags::open(dir.path()).unwrap();
    dynamic_flags.set_len(FLAG_COUNT).unwrap();
    random_flags
        .iter()
        .enumerate()
        .filter(|(_, flag)| **flag)
        .for_each(|(i, _)| assert!(!dynamic_flags.set(i, true)));
    dynamic_flags.flusher()().unwrap();
    let real_count = random_flags.iter().filter(|&&flag| flag).count();

    let mut group = c.benchmark_group("dynamic-mmap-flag-count");

    group.bench_function("manual-count-loop-stoppable", |b| {
        b.iter(|| {
            let mut count = 0;
            for i in 0..FLAG_COUNT {
                if dynamic_flags.get(i) {
                    count += 1;
                }
                check_process_stopped(&stopped).unwrap();
            }
            assert_eq!(count, real_count);
            black_box(count)
        });
    });

    group.bench_function("manual-count-loop", |b| {
        b.iter(|| {
            let mut count = 0;
            for i in 0..FLAG_COUNT {
                if dynamic_flags.get(i) {
                    count += 1;
                }
            }
            assert_eq!(count, real_count);
            black_box(count)
        });
    });

    group.bench_function("count-ones", |b| {
        b.iter(|| {
            let count = dynamic_flags.count_flags().unwrap();
            assert_eq!(count, real_count);
            black_box(count)
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = dynamic_mmap_flag_count
}

criterion_main!(benches);
