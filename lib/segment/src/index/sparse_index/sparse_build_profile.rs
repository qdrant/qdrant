//! Isolated storage-to-compressed-index benchmark. Run with:
//! cargo test -p segment --profile perf --lib profile_sparse_storage_build -- --ignored --nocapture
//! `perf` uses opt-level 3 without LTO for iteration. Use --release instead for full release LTO.
//! Optional SPARSE_PROFILE_POINTS (200000), SPARSE_PROFILE_REPEATS (3), and
//! SPARSE_PROFILE_THREADS (comma-separated; 1,4,8,16,32) select the matrix.
//! Uses warm storage pages; peak RSS includes storage mappings and ID tracker, but excludes
//! corpus generation, other variants, and result verification. No disk-cache eviction is done.

use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{self, Write as _};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Sequential;
use common::universal_io::MmapFs;
use sparse::common::sparse_vector::SparseVector;
use sparse::index::inverted_index::InvertedIndex;
use sparse::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use sparse::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;

use super::{IndicesTracker, build_ram_index};
use crate::id_tracker::IdTracker;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::vector_storage::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
use crate::vector_storage::{VectorStorage, VectorStorageRead};

/// Hash the debug representation without retaining it in memory.
fn hash_debug(value: &impl fmt::Debug, hash: &mut DefaultHasher) {
    struct HashWriter<'a>(&'a mut DefaultHasher);

    impl fmt::Write for HashWriter<'_> {
        fn write_str(&mut self, value: &str) -> fmt::Result {
            self.0.write(value.as_bytes());
            Ok(())
        }
    }

    write!(HashWriter(hash), "{value:?}").unwrap();
    // Match str::hash's terminator once, not once per formatting fragment.
    hash.write_u8(0xff);
}

fn fixture(path: &Path, count: u32, skew: bool) {
    let mut storage = MmapSparseVectorStorage::open_or_create(path).unwrap();
    let counter = HardwareCounterCell::disposable();
    for id in 0..count {
        // 100 unique terms/vector, 10k dimensions. Skewed vectors share 20 hot terms.
        let mut terms = (0..100u32)
            .map(|term| {
                let dim = if skew && term < 20 {
                    term
                } else if skew {
                    20 + ((id.wrapping_mul(7919) + term * 97) % 9980)
                } else {
                    (id.wrapping_mul(7919) + term * 97) % 10000
                };
                (dim * 32, ((id + term) % 101) as f32 / 7.0)
            })
            .collect::<Vec<_>>();
        terms.sort_unstable_by_key(|&(dim, _)| dim);
        let vector: SparseVector = terms.try_into().unwrap();
        storage
            .insert_vector(id, (&vector).into(), &counter)
            .unwrap();
    }
    storage.flusher()().unwrap();
}

fn build(
    path: &Path,
    count: u32,
    mode: &str,
    threads: usize,
) -> (
    InvertedIndexCompressedImmutableRam<f32>,
    IndicesTracker,
    f64,
    f64,
    f64,
    u64,
) {
    let storage = MmapSparseVectorStorage::open_or_create(path).unwrap();
    let mut ids = InMemoryIdTracker::new();
    for id in 0..count {
        ids.set_link(u64::from(id).into(), id).unwrap();
    }
    // Explicitly warm the same storage for each variant, outside the measured interval.
    storage.read_vectors::<Sequential, _>((0..count).map(|id| ((), id)), |(), _, _| {});
    let started = Instant::now();
    let (ram, tracker) = if mode == "buffered" {
        let mut tracker = IndicesTracker::default();
        let mut vectors = Vec::new();
        storage.read_vectors::<Sequential, _>((0..count).map(|id| ((), id)), |(), id, vector| {
            let vector: &SparseVector = vector.as_vec_ref().try_into().unwrap();
            if !vector.is_empty() {
                tracker.register_indices(vector);
                vectors.push((id, tracker.remap_vector(vector.clone())));
            }
        });
        let scan_elapsed = started.elapsed();
        let accumulate_started = Instant::now();
        let builder =
            InvertedIndexBuilder::from_vectors_with_threads(vectors, tracker.map.len(), threads);
        let accumulate_elapsed = accumulate_started.elapsed();
        let finalize_started = Instant::now();
        let ram = builder.build_with_threads(threads);
        log::info!(
            "sparse index build: buffered scan/remap in {scan_elapsed:.1?}; accumulated postings in {accumulate_elapsed:.1?}; finalized postings in {:.1?}",
            finalize_started.elapsed(),
        );
        (ram, tracker)
    } else {
        build_ram_index(
            &ids,
            &storage,
            &AtomicBool::new(false),
            if mode == "serial" { 1 } else { threads },
            || {},
        )
        .unwrap()
    };
    let ram_ms = started.elapsed().as_secs_f64() * 1000.0;
    let output = tempfile::tempdir().unwrap();
    let compress_started = Instant::now();
    let compressed = InvertedIndexCompressedImmutableRam::<f32>::from_ram_index_parallel(
        &MmapFs,
        Cow::Owned(ram),
        output.path(),
        if mode == "serial" { 1 } else { threads },
    )
    .unwrap();
    let compress_ms = compress_started.elapsed().as_secs_f64() * 1000.0;
    let write_started = Instant::now();
    compressed.save(output.path()).unwrap();
    let write_ms = write_started.elapsed().as_secs_f64() * 1000.0;
    let rss = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmHWM:")
                    .and_then(|v| v.split_whitespace().next()?.parse::<u64>().ok())
            })
        })
        .unwrap_or(0);
    (compressed, tracker, ram_ms, compress_ms, write_ms, rss)
}

#[test]
fn sparse_storage_build_paths_match() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path(), 2053, true);
    let (expected, mapping, ..) = build(dir.path(), 2053, "serial", 1);
    // Only this small regression test allocates the old representation, to verify that
    // incremental hashing preserves the benchmark checksum across formatting fragments.
    let mut original_hash = DefaultHasher::new();
    format!("{expected:?}").hash(&mut original_hash);
    let mut incremental_hash = DefaultHasher::new();
    hash_debug(&expected, &mut incremental_hash);
    assert_eq!(original_hash.finish(), incremental_hash.finish());
    for threads in [2, 4, 16, 32] {
        for mode in ["buffered", "streaming"] {
            let (actual, tracker, ..) = build(dir.path(), 2053, mode, threads);
            assert_eq!(tracker, mapping);
            assert_eq!(actual, expected, "{mode}, threads={threads}");
        }
    }
}

#[test]
#[ignore]
fn profile_sparse_storage_build() {
    if let Ok(path) = std::env::var("SPARSE_PROFILE_CHILD_STORAGE") {
        // Each sample is a fresh process. Enable the existing stage timers explicitly;
        // --nocapture alone does not initialize a logger.
        env_logger::Builder::new()
            .filter_level(log::LevelFilter::Off)
            .filter_module("segment::index::sparse_index", log::LevelFilter::Info)
            .filter_module("sparse::index::inverted_index", log::LevelFilter::Info)
            .format_timestamp(None)
            .init();
        let count = std::env::var("SPARSE_PROFILE_POINTS")
            .unwrap()
            .parse()
            .unwrap();
        let mode = std::env::var("SPARSE_PROFILE_MODE").unwrap();
        let threads = std::env::var("SPARSE_PROFILE_CHILD_THREADS")
            .unwrap()
            .parse()
            .unwrap();
        let (output, tracker, ram, compression, write, rss) =
            build(Path::new(&path), count, &mode, threads);
        // Hash after sampling RSS; verification streams directly into the hasher.
        let mut hash = DefaultHasher::new();
        hash_debug(&output, &mut hash);
        let mut mapping = tracker.map.into_iter().collect::<Vec<_>>();
        mapping.sort_unstable();
        mapping.hash(&mut hash);
        println!(
            "SPARSE_SAMPLE {ram} {compression} {rss} {} {write}",
            hash.finish()
        );
        return;
    }
    assert!(
        !cfg!(debug_assertions),
        "run performance measurements with --release"
    );
    let count: u32 = std::env::var("SPARSE_PROFILE_POINTS")
        .unwrap_or("200000".into())
        .parse()
        .unwrap();
    let repeats: usize = std::env::var("SPARSE_PROFILE_REPEATS")
        .unwrap_or("3".into())
        .parse()
        .unwrap();
    assert!(repeats > 0 && count > 0);
    let threads = std::env::var("SPARSE_PROFILE_THREADS")
        .unwrap_or("1,4,8,16,32".into())
        .split(',')
        .map(|v| v.parse::<usize>().unwrap())
        .collect::<Vec<_>>();
    assert!(threads.iter().all(|&n| n > 0));
    for skew in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        fixture(dir.path(), count, skew);
        let mut cases = vec![("serial", 1)];
        for &n in &threads {
            cases.push(("buffered", n));
            if n > 1 {
                cases.push(("streaming", n));
            }
        }
        let mut samples = vec![Vec::new(); cases.len()];
        let mut expected_hash = None;
        for repeat in 0..repeats {
            // Rotate order between repetitions to reduce systematic order bias.
            for offset in 0..cases.len() {
                let index = (offset + repeat) % cases.len();
                let (mode, n) = cases[index];
                let output = std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["profile_sparse_storage_build", "--ignored", "--nocapture"])
                    .env("SPARSE_PROFILE_CHILD_STORAGE", dir.path())
                    .env("SPARSE_PROFILE_POINTS", count.to_string())
                    .env("SPARSE_PROFILE_MODE", mode)
                    .env("SPARSE_PROFILE_CHILD_THREADS", n.to_string())
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                eprintln!(
                    "STAGES skew={skew} points={count} mode={mode} threads={n} repeat={repeat}"
                );
                eprint!("{}", String::from_utf8_lossy(&output.stderr));
                let stdout = String::from_utf8(output.stdout).unwrap();
                let line = stdout
                    .lines()
                    .find(|line| line.starts_with("SPARSE_SAMPLE "))
                    .unwrap();
                let fields = line.split_whitespace().collect::<Vec<_>>();
                let digest = fields[4].parse::<u64>().unwrap();
                assert_eq!(
                    *expected_hash.get_or_insert(digest),
                    digest,
                    "output differs: {mode}/{n}"
                );
                let ram = fields[1].parse::<f64>().unwrap();
                let compression = fields[2].parse::<f64>().unwrap();
                let rss = fields[3].parse::<u64>().unwrap();
                let write = fields[5].parse::<f64>().unwrap();
                samples[index].push(ram + compression + write);
                eprintln!(
                    "skew={skew} points={count} mode={mode} threads={n} repeat={repeat} ram_ms={ram:.3} compress_ms={compression:.3} write_ms={write:.3} total_ms={:.3} peak_rss_kib={rss}",
                    ram + compression + write,
                );
            }
        }
        for ((mode, n), mut values) in cases.into_iter().zip(samples) {
            values.sort_by(f64::total_cmp);
            eprintln!(
                "SUMMARY skew={skew} mode={mode} threads={n} median_ms={:.3} min_ms={:.3} max_ms={:.3}",
                values[values.len() / 2],
                values[0],
                values[values.len() - 1]
            );
        }
    }
}
