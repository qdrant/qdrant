use std::io::Write;
use std::path::Path;

use fs_err as fs;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{BLOCK_SIZE, CacheController, CachedSlice};

#[test]
fn test_cacher() {
    let dir = tempfile::Builder::new().prefix("cacher").tempdir().unwrap();
    let cacher =
        CacheController::new(&dir.path().join("cache.bin"), BLOCK_SIZE as u64 * 5).unwrap();

    create_test_file(&dir.path().join("cold.0"), "cold.0", 10);

    let fd = cacher.open_file(&dir.path().join("cold.0")).unwrap();

    eprintln!(
        "data0: {}",
        fancy_decode(&fd.get_range(BLOCK_SIZE * 3..BLOCK_SIZE * 3))
    );

    eprintln!(
        "data0a: {}",
        fancy_decode(&fd.get_range(BLOCK_SIZE * 3 + 3..BLOCK_SIZE * 3 + 14))
    );

    eprintln!(
        "data1: {}",
        fancy_decode(&fd.get_range(BLOCK_SIZE * 3..BLOCK_SIZE * 4 - 1))
    );

    eprintln!(
        "data2: {}",
        fancy_decode(&fd.get_range(BLOCK_SIZE * 3..BLOCK_SIZE * 4))
    );

    eprintln!(
        "data3: {}",
        fancy_decode(&fd.get_range(BLOCK_SIZE * 3..BLOCK_SIZE * 4 + 1))
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
    let bytes: &[u8] =
        unsafe { std::slice::from_raw_parts(data.as_ptr().cast::<u8>(), data.len() * 4) };
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
    // File size = 2000 * 512 * 4 = 4,096,000 bytes = 1000 blocks.
    // Cache = 128 blocks forces many evictions.
    let cacher =
        CacheController::new(&dir.path().join("cache.bin"), BLOCK_SIZE as u64 * 128).unwrap();

    let cached_file = cacher.open_file(&vectors_path).unwrap();
    let cached_slice: CachedSlice<f32> = CachedSlice::new(cached_file);

    // Verify total length matches.
    assert_eq!(cached_slice.len(), TOTAL_FLOATS);

    // Sequential iteration: compare every element.
    for (idx, cached_val) in cached_slice.iter().enumerate() {
        assert_eq!(
            *cached_val,
            vectors[idx],
            "Mismatch at flat index {idx} (vector {}, dim {})",
            idx / VECTOR_DIM,
            idx % VECTOR_DIM,
        );
    }

    // Sequential vector-at-a-time access via get_range.
    for vec_idx in 0..NUM_VECTORS {
        let start = vec_idx * VECTOR_DIM;
        let end = start + VECTOR_DIM;
        let cached_vec = cached_slice.get_range(start..end);
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

    let cached_file = cacher.open_file(&vectors_path).unwrap();
    let cached_slice: CachedSlice<f32> = CachedSlice::new(cached_file);

    let mut rng = StdRng::seed_from_u64(123);

    // Random single-element access.
    for _ in 0..5000 {
        let idx = rng.random_range(0..TOTAL_FLOATS);
        let cached_val = cached_slice.get(idx);
        assert_eq!(
            *cached_val, vectors[idx],
            "Random access mismatch at flat index {idx}",
        );
    }

    // Random vector access (full 512-dim vectors at random positions).
    for _ in 0..1000 {
        let vec_idx = rng.random_range(0..NUM_VECTORS);
        let start = vec_idx * VECTOR_DIM;
        let end = start + VECTOR_DIM;
        let cached_vec = cached_slice.get_range(start..end);
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
        let cached_range = cached_slice.get_range(a..b);
        assert_eq!(
            cached_range.as_ref(),
            &vectors[a..b],
            "Sub-range mismatch at [{a}..{b}]",
        );
    }
}
