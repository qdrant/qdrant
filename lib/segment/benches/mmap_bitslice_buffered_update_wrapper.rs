use std::fs::File;
use std::iter;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use memmap2::MmapMut;
use memory::mmap_type::MmapBitSlice;
use rand::prelude::*;
use rand::rngs::StdRng;
use segment::common::mmap_bitslice_buffered_update_wrapper::MmapBitSliceBufferedUpdateWrapper;
use tempfile::tempdir;

const SIZE: usize = 4 * 1024 * 1024;
const FLAG_COUNT: usize = 1_000_000;
const LOOKUP_COUNT: usize = 1_000_000;

fn mmap_bitslice_buffered_update_wrapper(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let dir = tempdir().unwrap();
    let path = dir.path().join("bitslice.mmap");

    let file = File::create_new(path).unwrap();
    file.set_len(SIZE as u64).unwrap();
    file.sync_all().unwrap();

    let mmap_mut = unsafe { MmapMut::map_mut(&file).unwrap() };
    let mmap_bitslice = MmapBitSlice::from(mmap_mut, 0);
    let mmap_bitslice_buffered_update_wrapper =
        MmapBitSliceBufferedUpdateWrapper::new(mmap_bitslice);

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
