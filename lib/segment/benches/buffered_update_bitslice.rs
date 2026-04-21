use std::hint::black_box;
use std::iter;

use common::mmap::create_and_ensure_length;
use common::stored_bitslice::MmapBitSlice;
use common::universal_io::OpenOptions;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::prelude::*;
use rand::rngs::StdRng;
use segment::common::buffered_update_bitslice::BufferedUpdateBitSlice;
use tempfile::tempdir;

const SIZE: usize = 4 * 1024 * 1024;
const FLAG_COUNT: usize = 1_000_000;
const LOOKUP_COUNT: usize = 1_000_000;

fn buffered_update_bitslice(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let dir = tempdir().unwrap();
    let path = dir.path().join("bitslice.bin");

    let _ = create_and_ensure_length(
        &path,
        SIZE.div_ceil(u8::BITS as usize)
            .next_multiple_of(size_of::<u64>()),
    )
    .unwrap();

    let bitslice_storage = MmapBitSlice::open(&path, OpenOptions::default()).unwrap();
    let buffered_update_bitslice = BufferedUpdateBitSlice::new(bitslice_storage);

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
