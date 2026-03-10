use std::hint::black_box;
use std::iter;

use common::mmap::create_and_ensure_length;
use common::universal_io::OpenOptions;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::prelude::*;
use rand::rngs::StdRng;
use segment::common::mmap_bitslice_buffered_update_wrapper::MmapBitSliceBufferedUpdateWrapper;
use segment::common::stored_bitslice::MmapBitSlice;
use tempfile::tempdir;

const SIZE: usize = 4 * 1024 * 1024;
const FLAG_COUNT: usize = 1_000_000;
const LOOKUP_COUNT: usize = 1_000_000;

fn mmap_bitslice_buffered_update_wrapper(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let dir = tempdir().unwrap();
    let path = dir.path().join("bitslice.mmap");

    let _ = create_and_ensure_length(
        &path,
        SIZE.div_ceil(u8::BITS as usize)
            .next_multiple_of(size_of::<u64>()),
    )
    .unwrap();

    let bitslice_storage = MmapBitSlice::open(&path, OpenOptions::default()).unwrap();
    let mmap_bitslice_buffered_update_wrapper =
        MmapBitSliceBufferedUpdateWrapper::new(bitslice_storage);

    // Set random flags and persist
    for _ in 0..FLAG_COUNT {
        mmap_bitslice_buffered_update_wrapper
            .set(rng.random::<u64>() as usize % SIZE, rng.random());
    }
    mmap_bitslice_buffered_update_wrapper.flusher()().unwrap();

    let mut group = c.benchmark_group("mmap-bitslice-buffered-update-wrapper");

    let lookups: Vec<_> = iter::repeat_with(|| rng.random::<u64>() as usize % SIZE)
        .take(LOOKUP_COUNT)
        .collect();

    group.bench_function("lookup-without-pending-changes", |b| {
        b.iter(|| {
            for lookup in &lookups {
                black_box(mmap_bitslice_buffered_update_wrapper.get(*lookup).unwrap());
            }
        });
    });

    // Set random flags and keep them in pending changes list
    for _ in 0..FLAG_COUNT {
        mmap_bitslice_buffered_update_wrapper
            .set(rng.random::<u64>() as usize % SIZE, rng.random());
    }

    group.bench_function("lookup-with-pending-changes", |b| {
        b.iter(|| {
            for lookup in &lookups {
                black_box(mmap_bitslice_buffered_update_wrapper.get(*lookup).unwrap());
            }
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = mmap_bitslice_buffered_update_wrapper
}

criterion_main!(benches);
