use std::hint::black_box;
use std::iter;

use common::universal_io::{MmapFile, MmapFs, OpenOptions};
use criterion::{Criterion, criterion_group, criterion_main};
use rand::prelude::*;
use rand::rngs::SmallRng;
use segment::common::buffered_update_bitslice::{
    BitmaskFormat, BitmaskPaths, BufferedUpdateBitSlice,
};
use tempfile::tempdir;

const SIZE: usize = 4 * 1024 * 1024;
const FLAG_COUNT: usize = 1_000_000;
const LOOKUP_COUNT: usize = 1_000_000;

fn buffered_update_bitslice(c: &mut Criterion) {
    let mut rng = SmallRng::seed_from_u64(42);
    let dir = tempdir().unwrap();
    let paths = BitmaskPaths::new(
        dir.path().join("bitslice.bin"),
        dir.path().join("bitslice.mask"),
    );

    let buffered_update_bitslice = BufferedUpdateBitSlice::<MmapFile>::create(
        &MmapFs,
        &paths,
        OpenOptions::new_for_test(),
        BitmaskFormat::Raw,
        SIZE,
        [],
    )
    .unwrap();

    // Set random flags and persist
    for _ in 0..FLAG_COUNT {
        buffered_update_bitslice.set(rng.random::<u64>() as usize % SIZE, rng.random());
    }
    buffered_update_bitslice.flusher()().unwrap();

    let mut group = c.benchmark_group("buffered-update-bitslice");

    let lookups: Vec<_> = iter::repeat_with(|| rng.random::<u64>() as usize % SIZE)
        .take(LOOKUP_COUNT)
        .collect();

    group.bench_function("lookup-without-pending-changes", |b| {
        b.iter(|| {
            for lookup in &lookups {
                black_box(buffered_update_bitslice.get(*lookup).unwrap());
            }
        });
    });

    // Set random flags and keep them in pending changes list
    for _ in 0..FLAG_COUNT {
        buffered_update_bitslice.set(rng.random::<u64>() as usize % SIZE, rng.random());
    }

    group.bench_function("lookup-with-pending-changes", |b| {
        b.iter(|| {
            for lookup in &lookups {
                black_box(buffered_update_bitslice.get(*lookup).unwrap());
            }
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = buffered_update_bitslice
}

criterion_main!(benches);
