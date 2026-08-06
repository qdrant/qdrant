use std::hint::black_box;
use std::io::Write as _;
use std::path::PathBuf;

use common::bench_cache::{build_once, cache_path};
use common::universal_io::disk_cache::{CacheController, CachedSlice};
use criterion::{Criterion, criterion_group, criterion_main};
use fs_err as fs;

const FILE_SIZE_BYTES: u64 = 10 * 1024 * 1024; // 10mb file

fn benches(c: &mut Criterion) {
    let (cache_file, data_file) = make_cache_and_file();

    let controller = CacheController::new(&cache_file, FILE_SIZE_BYTES).unwrap();
    let slice = CachedSlice::open(&controller, &data_file).unwrap();

    // warm up the cache so blocks are continuously loaded into the mmap
    let _ = slice
        .get_range_bytes(0..FILE_SIZE_BYTES as usize, 8)
        .unwrap();

    let mut group = c.benchmark_group("disk_cache");

    // Bench single block fast path
    group.bench_function("single_block_hit", |b| {
        b.iter(|| {
            // Read 4kb from inside the first block
            let data = slice.get_range_bytes(100..4196, 8).unwrap();
            black_box(data);
        })
    });

    // Bench multi-block contiguous fast path
    group.bench_function("multi_block_contiguous_hit", |b| {
        b.iter(|| {
            // Read 4 blocks across boundaries
            // since they are contiguous, it avoids allocating and copying
            let data = slice.get_range_bytes(100..60 * 1024 + 100, 8).unwrap();
            black_box(data);
        })
    });
}

fn make_cache_and_file() -> (PathBuf, PathBuf) {
    let dir = build_once(cache_path!("disk_cache_bench"), |path| {
        fs::create_dir_all(path).unwrap();

        let mut file = fs::File::create(path.join("cold.bin")).unwrap();
        let buffer = vec![42u8; FILE_SIZE_BYTES as usize];
        file.write_all(&buffer).unwrap();
        file.flush().unwrap();
    });

    (dir.join("cache.bin"), dir.join("cold.bin"))
}

criterion_group! {
    name = bench_group;
    config = Criterion::default();
    targets = benches
}

criterion_main!(bench_group);
