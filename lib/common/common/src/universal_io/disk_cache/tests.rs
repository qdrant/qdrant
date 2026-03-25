use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use fs_err as fs;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use super::{BLOCK_SIZE, CacheController, CachedSlice};

#[test]
fn test_cacher() {
    let dir = tempfile::Builder::new().prefix("cacher").tempdir().unwrap();
    let cacher =
        CacheController::new(&dir.path().join("cache.bin"), BLOCK_SIZE as u64 * 5).unwrap();

    create_test_file(&dir.path().join("cold.0"), "cold.0", 10);

    let fd = CachedSlice::<u8>::open(&cacher, &dir.path().join("cold.0")).unwrap();

    eprintln!(
        "data0: {}",
        fancy_decode(&fd.get_range(BLOCK_SIZE * 3..BLOCK_SIZE * 3).unwrap())
    );

    eprintln!(
        "data0a: {}",
        fancy_decode(
            &fd.get_range(BLOCK_SIZE * 3 + 3..BLOCK_SIZE * 3 + 14)
                .unwrap()
        )
    );

    eprintln!(
        "data1: {}",
        fancy_decode(&fd.get_range(BLOCK_SIZE * 3..BLOCK_SIZE * 4 - 1).unwrap())
    );

    eprintln!(
        "data2: {}",
        fancy_decode(&fd.get_range(BLOCK_SIZE * 3..BLOCK_SIZE * 4).unwrap())
    );

    eprintln!(
        "data3: {}",
        fancy_decode(&fd.get_range(BLOCK_SIZE * 3..BLOCK_SIZE * 4 + 1).unwrap())
    );
}

fn create_test_file(path: &Path, name: &str, size_blocks: usize) {
    let mut f = fs::File::create(path).unwrap();
    let mut block = [0u8; BLOCK_SIZE];
    for i in 0..size_blocks {
        write!(&mut block[..], "{name}:{i}").unwrap();
        f.write_all(&block).unwrap();
    }
}

/// Just like [`String::from_utf8_lossy`], but also counts NUL bytes.
/// For debugging purposes.
fn fancy_decode(data: &[u8]) -> String {
    let mut out = format!("[{} bytes] ", data.len());
    let mut i = 0;

    while i < data.len() {
        let byte = data[i];
        if byte.is_ascii_graphic() || byte == b' ' {
            out.push(byte as char);
            i += 1;
            continue;
        }

        let mut count = 1;
        while i + count < data.len() && data[i + count] == byte {
            count += 1;
        }
        out.push_str(&format!("\x1b[33m[0x{byte:02X} × {count}]\x1b[m"));
        i += count;
    }

    out
}

/// Number of vectors in the test file.
const NUM_VECTORS: usize = 2000;
/// Dimensionality of each vector. Unaligned with BLOCK_SIZE to exercise edge cases.
const VECTOR_DIM: usize = 500;
/// Total number of f32 elements across all vectors.
const TOTAL_FLOATS: usize = NUM_VECTORS * VECTOR_DIM;

/// Generate `NUM_VECTORS` vectors of `VECTOR_DIM` f32s using a seeded RNG.
fn generate_vectors(seed: u64) -> Vec<f32> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..TOTAL_FLOATS).map(|_| rng.random::<f32>()).collect()
}

/// Write a flat slice of f32s to a binary file (little-endian, which is native on x86/ARM).
fn write_vectors_to_file(path: &Path, data: &[f32]) {
    let bytes: &[u8] = bytemuck::cast_slice(data);
    fs::write(path, bytes).unwrap();
}

#[test]
fn test_cached_slice_vectors_sequential() {
    let dir = tempfile::Builder::new()
        .prefix("cached_slice_vectors")
        .tempdir()
        .unwrap();

    let vectors = generate_vectors(42);
    let vectors_path = dir.path().join("vectors.bin");
    write_vectors_to_file(&vectors_path, &vectors);

    // Use a cache smaller than the file to exercise eviction.
    // File size = 2000 * 500 * 4 = 4,000,000 bytes ≈ 244 blocks.
    // Cache = 128 blocks forces many evictions.
    let cacher =
        CacheController::new(&dir.path().join("cache.bin"), BLOCK_SIZE as u64 * 128).unwrap();

    let cached_slice = CachedSlice::<f32>::open(&cacher, &vectors_path).unwrap();

    // Verify total length matches.
    assert_eq!(cached_slice.len(), TOTAL_FLOATS);

    // Sequential iteration: compare every element.
    for (idx, vector) in vectors.iter().enumerate().take(TOTAL_FLOATS) {
        let cached_val = cached_slice.get(idx).unwrap();
        assert_eq!(
            cached_val.as_ref(),
            vector,
            "Mismatch at flat index {idx} (vector {}, dim {})",
            idx / VECTOR_DIM,
            idx % VECTOR_DIM,
        );
    }

    // Sequential vector-at-a-time access via get_range.
    for vec_idx in 0..NUM_VECTORS {
        let start = vec_idx * VECTOR_DIM;
        let end = start + VECTOR_DIM;
        let cached_vec = cached_slice.get_range(start..end).unwrap();
        assert_eq!(
            cached_vec.as_ref(),
            &vectors[start..end],
            "Vector {vec_idx} does not match",
        );
    }
}

#[test]
fn test_cached_slice_vectors_random_access() {
    let dir = tempfile::Builder::new()
        .prefix("cached_slice_random")
        .tempdir()
        .unwrap();

    let vectors = generate_vectors(42);
    let vectors_path = dir.path().join("vectors.bin");
    write_vectors_to_file(&vectors_path, &vectors);

    // Small cache to stress eviction under random access patterns.
    let cacher =
        CacheController::new(&dir.path().join("cache.bin"), BLOCK_SIZE as u64 * 64).unwrap();

    let cached_slice = CachedSlice::<f32>::open(&cacher, &vectors_path).unwrap();

    let mut rng = StdRng::seed_from_u64(123);

    // Random single-element access.
    for _ in 0..5000 {
        let idx = rng.random_range(0..TOTAL_FLOATS);
        let cached_val = cached_slice.get(idx).unwrap();
        assert_eq!(
            *cached_val, vectors[idx],
            "Random access mismatch at flat index {idx}",
        );
    }

    // Random vector access (full 500-dim vectors at random positions).
    for _ in 0..1000 {
        let vec_idx = rng.random_range(0..NUM_VECTORS);
        let start = vec_idx * VECTOR_DIM;
        let end = start + VECTOR_DIM;
        let cached_vec = cached_slice.get_range(start..end).unwrap();
        assert_eq!(
            cached_vec.as_ref(),
            &vectors[start..end],
            "Random vector access mismatch at vector {vec_idx}",
        );
    }

    // Random sub-range access spanning partial vectors.
    for _ in 0..1000 {
        let a = rng.random_range(0..TOTAL_FLOATS);
        let max_len = (TOTAL_FLOATS - a).min(VECTOR_DIM * 3);
        if max_len == 0 {
            continue;
        }
        let b = a + rng.random_range(1..=max_len);
        let cached_range = cached_slice.get_range(a..b).unwrap();
        assert_eq!(
            cached_range.as_ref(),
            &vectors[a..b],
            "Sub-range mismatch at [{a}..{b}]",
        );
    }
}

/// Concurrent access test: multiple threads exhaust the pool faster.
///
/// With a small cache (blocks < UNUSED_BLOCKS_MARGIN), the cache
/// never evicts, so concurrent threads each consume blocks from the pool
/// with no blocks being returned. The pool empties quickly.
#[test]
fn test_no_more_blocks_concurrent_exhaustion() {
    let dir = tempfile::Builder::new()
        .prefix("no_blocks_concurrent")
        .tempdir()
        .unwrap();

    let num_cache_blocks: u64 = 16;
    let blocks_per_file = 8;
    let num_files = 8;

    let cacher = CacheController::new(
        &dir.path().join("cache.bin"),
        BLOCK_SIZE as u64 * num_cache_blocks,
    )
    .unwrap();

    let fds: Vec<_> = (0..num_files)
        .map(|i| {
            let path = dir.path().join(format!("cold.{i}"));
            create_test_file(&path, &format!("cold.{i}"), blocks_per_file);
            Arc::new(CachedSlice::<u8>::open(&cacher, &path).unwrap())
        })
        .collect();

    // Total unique blocks = 8 * 8 = 64, far exceeding the 16 cache blocks.
    let barrier = Arc::new(std::sync::Barrier::new(num_files));

    let handles: Vec<_> = (0..num_files)
        .map(|t| {
            let fd = Arc::clone(&fds[t]);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                for block in 0..blocks_per_file {
                    fd.get_range(block * BLOCK_SIZE..(block + 1) * BLOCK_SIZE)
                        .unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}
