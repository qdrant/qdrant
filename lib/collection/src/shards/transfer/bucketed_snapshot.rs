//! File-identity-based partitioning for parallel shard-transfer snapshots.
//!
//! Prior approaches sliced *one* tar by byte offset: either after fully
//! materializing it to disk (Update 2, deployed, correct, 1.9x speedup) or
//! while it was still being written (Update 3, found to have a real
//! correctness bug and rolled back -- a gap between the sender's
//! flush-watermark and the receiver's completeness check produced a tar
//! that extracted without an I/O error but failed Qdrant's own structural
//! shard validation, because validity is a property of the *whole* tar, not
//! of an arbitrary byte range within it).
//!
//! This module instead partitions the shard's on-disk entries (segment
//! directories, the WAL directory, and the handful of other top-level
//! files) into a fixed number of buckets, and each bucket becomes its own
//! independent, self-contained tar transferred over its own connection.
//! There is no cross-stream reassembly, no watermark, and no ordering logic
//! for a correctness bug to attach to: each bucket's tar is valid and
//! unpackable the instant its own transfer finishes, independent of the
//! other buckets.
//!
//! Bucket assignment is size-aware greedy bin-packing (largest entry first,
//! always placed into the currently lightest bucket), not a hash of each
//! entry's name -- segment sizes in a real shard vary widely, so a
//! count-balanced-only scheme would let one oversized segment dominate a
//! transfer's total time regardless of how many buckets exist. This means
//! bucket assignment is *not* independently derivable by both sides from
//! just the entry names (unlike, say, a hash of a UUID) -- it depends on
//! each entry's size, which only the sender knows upfront. The sender
//! computes the assignment once and sends it to the receiver as part of the
//! transfer's start message; this small upfront manifest is the deliberate
//! coordination cost of balancing buckets by size rather than by count.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Number of parallel buckets/streams used for a bucketed snapshot transfer.
pub const BUCKET_COUNT: usize = 16;

/// Bucket index reserved for every shard entry that isn't an individually
/// sized, hashable segment directory -- the WAL directory, segment
/// manifest, clock/sequence files, shard config, etc. These are always
/// present and typically small compared to segment data, so pinning them to
/// a fixed bucket avoids needing any size/identity scheme for a handful of
/// miscellaneous paths; the greedy packer below only has to balance segment
/// directories, which is where essentially all shard data actually lives.
pub const FIXED_ENTRY_BUCKET: usize = 0;

/// One top-level entry directly under a shard's root directory that a
/// bucketed transfer moves as a unit -- either a segment directory
/// (identified by its UUID name) or one of the fixed miscellaneous paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardEntry {
    /// Path relative to the shard's root directory, e.g. `segments/<uuid>`
    /// or `wal`. Always exactly one path component below `segments/` for
    /// segment directories, or a direct child of the shard root otherwise --
    /// nothing nested deeper is treated as its own entry; a segment
    /// directory's own internal files travel with it as a unit.
    pub relative_path: String,
    /// Total size in bytes of everything under this entry: a single file's
    /// size, or the recursive size of a directory's contents.
    pub size_bytes: u64,
    /// Whether this entry is a segment directory (its name parses as a
    /// UUID). Used only to decide fixed-vs-greedy placement in
    /// [`assign_buckets`]; carried on the type mainly so tests and callers
    /// can assert on it directly instead of re-deriving it from the name.
    pub is_segment: bool,
}

/// List the shard's data entries: every segment directory under
/// `shard_path/segments/`, plus the WAL directory and any other direct
/// children of `shard_path` (segment manifest, clock files, shard config,
/// ...). Directory sizes are computed recursively.
///
/// Deliberately does *not* delegate to `shard::files::get_shard_data_files`
/// (which returns a fixed set of well-known single paths, not an
/// enumeration of segment directories) -- this needs to see the actual,
/// variable set of segment UUIDs present on disk right now.
pub fn list_shard_entries(shard_path: &Path) -> std::io::Result<Vec<ShardEntry>> {
    let mut entries = Vec::new();

    let segments_dir = shard_path.join("segments");
    if segments_dir.is_dir() {
        for dir_entry in fs_err::read_dir(&segments_dir)? {
            let dir_entry = dir_entry?;
            let metadata = dir_entry.metadata()?;
            let name = dir_entry.file_name().to_string_lossy().into_owned();
            let is_segment = metadata.is_dir() && uuid::Uuid::parse_str(&name).is_ok();
            let size_bytes = if metadata.is_dir() {
                dir_size_recursive(&dir_entry.path())?
            } else {
                metadata.len()
            };
            entries.push(ShardEntry {
                relative_path: format!("segments/{name}"),
                size_bytes,
                is_segment,
            });
        }
    }

    for dir_entry in fs_err::read_dir(shard_path)? {
        let dir_entry = dir_entry?;
        let name = dir_entry.file_name().to_string_lossy().into_owned();
        if name == "segments" {
            // Handled above, one entry per segment rather than one entry
            // for the whole `segments/` directory.
            continue;
        }
        let metadata = dir_entry.metadata()?;
        let size_bytes = if metadata.is_dir() {
            dir_size_recursive(&dir_entry.path())?
        } else {
            metadata.len()
        };
        entries.push(ShardEntry {
            relative_path: name,
            size_bytes,
            is_segment: false,
        });
    }

    Ok(entries)
}

fn dir_size_recursive(path: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    for entry in fs_err::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += dir_size_recursive(&entry.path())?;
        } else {
            total += metadata.len();
        }
    }
    Ok(total)
}

/// Partition `entries` into `bucket_count` buckets using size-aware greedy
/// packing: process entries largest-first, always placing the next entry
/// into whichever bucket currently has the least total size assigned.
/// Non-segment entries (WAL, manifest/clock/config files, ...) are always
/// pinned to [`FIXED_ENTRY_BUCKET`] instead of participating in greedy
/// packing -- see the module docs for why.
///
/// Returns one `Vec<String>` of relative paths per bucket, indexed
/// `0..bucket_count`; a bucket with no entries assigned is an empty vec, not
/// omitted, so callers can always index by bucket number directly.
pub fn assign_buckets(entries: &[ShardEntry], bucket_count: usize) -> Vec<Vec<String>> {
    assert!(bucket_count > 0, "bucket_count must be positive");

    let mut buckets: Vec<Vec<String>> = vec![Vec::new(); bucket_count];
    let mut bucket_sizes: Vec<u64> = vec![0; bucket_count];

    let mut fixed: Vec<&ShardEntry> = Vec::new();
    let mut sizable: Vec<&ShardEntry> = Vec::new();
    for entry in entries {
        if entry.is_segment {
            sizable.push(entry);
        } else {
            fixed.push(entry);
        }
    }

    for entry in fixed {
        buckets[FIXED_ENTRY_BUCKET].push(entry.relative_path.clone());
        bucket_sizes[FIXED_ENTRY_BUCKET] += entry.size_bytes;
    }

    // Largest-first: greedy bin-packing gets meaningfully worse if it
    // processes small items before large ones (a large item arriving late
    // can land on top of an already-fairly-loaded bucket with nowhere
    // better to go).
    sizable.sort_by_key(|entry| std::cmp::Reverse(entry.size_bytes));

    // Min-heap of (current total size, bucket index): "the currently
    // lightest bucket" is always one pop away, even though
    // `FIXED_ENTRY_BUCKET` may already carry weight from the fixed entries
    // placed above.
    let mut heap: BinaryHeap<Reverse<(u64, usize)>> = bucket_sizes
        .iter()
        .enumerate()
        .map(|(i, &size)| Reverse((size, i)))
        .collect();

    for entry in sizable {
        let Reverse((size, idx)) = heap.pop().expect("bucket_count > 0, heap never empties");
        buckets[idx].push(entry.relative_path.clone());
        heap.push(Reverse((size + entry.size_bytes, idx)));
    }

    buckets
}

/// Write a tar archive containing exactly `entries` (paths relative to
/// `shard_path`, as produced by [`list_shard_entries`]/[`assign_buckets`])
/// into `tar`.
///
/// Uses [`common::tar_ext::BuilderExt`]'s existing blocking append helpers
/// (the same ones Qdrant's own full/partial shard snapshot code uses) rather
/// than driving `tar::Builder` directly, so this gets the same tar-writing
/// conventions (permissions, streaming-vs-seekable output handling) as the
/// rest of the codebase for free.
///
/// Must be called from a blocking context (e.g. inside
/// `tokio::task::spawn_blocking`) when `tar` wraps an async sink, exactly
/// like the existing full-shard streaming snapshot path does.
///
/// # Known limitation
///
/// This does not take any lock on the shard's segment holder the way
/// [`crate::shards::replica_set::ShardReplicaSet::create_snapshot`] does.
/// It relies on the shard already being queue-proxified (as
/// `transfer_snapshot` arranges before calling this) to keep segment
/// merges/optimizations from mutating the on-disk layout out from under a
/// bucket's read -- it does not independently defend against that itself.
pub fn write_bucket_tar(
    tar: &common::tar_ext::BuilderExt,
    shard_path: &Path,
    entries: &[String],
) -> std::io::Result<()> {
    for entry in entries {
        let src = shard_path.join(entry);
        let dst = Path::new(entry);
        let metadata = fs_err::metadata(&src)?;
        if metadata.is_dir() {
            tar.blocking_append_dir_all(&src, dst)?;
        } else {
            tar.blocking_append_file(&src, dst)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    fn entry(name: &str, size: u64, is_segment: bool) -> ShardEntry {
        ShardEntry {
            relative_path: name.to_string(),
            size_bytes: size,
            is_segment,
        }
    }

    #[test]
    fn fixed_entries_always_land_in_fixed_bucket() {
        let entries = vec![
            entry("wal", 100, false),
            entry("segments_manifest.json", 10, false),
            entry(
                "segments/11111111-1111-1111-1111-111111111111",
                1_000_000,
                true,
            ),
        ];
        let buckets = assign_buckets(&entries, BUCKET_COUNT);
        assert!(buckets[FIXED_ENTRY_BUCKET].contains(&"wal".to_string()));
        assert!(buckets[FIXED_ENTRY_BUCKET].contains(&"segments_manifest.json".to_string()));
        // the one segment must have gone somewhere, but since it's the only
        // sizable entry it always lands in the (tied-lightest, index-0)
        // bucket too here -- use a second segment to prove segments aren't
        // *always* forced into bucket 0.
    }

    #[test]
    fn segments_distribute_away_from_fixed_bucket_when_multiple() {
        let mut entries = vec![entry("wal", 10, false)];
        for i in 0..BUCKET_COUNT {
            entries.push(entry(&format!("segments/seg-{i}"), 1_000_000, true));
        }
        let buckets = assign_buckets(&entries, BUCKET_COUNT);
        // BUCKET_COUNT equally-sized segments across BUCKET_COUNT buckets:
        // every bucket should get exactly one segment, including bucket 0
        // getting exactly one *in addition to* the fixed "wal" entry.
        for bucket in &buckets {
            let segment_count = bucket.iter().filter(|p| p.starts_with("segments/")).count();
            assert_eq!(
                segment_count, 1,
                "bucket {bucket:?} should have exactly one segment"
            );
        }
        assert!(buckets[FIXED_ENTRY_BUCKET].contains(&"wal".to_string()));
    }

    #[test]
    fn greedy_packing_balances_widely_varying_sizes() {
        // One huge segment plus many tiny ones -- greedy packing should
        // isolate the huge one and spread the tiny ones across the
        // remaining buckets, keeping the max bucket size close to the huge
        // segment's own size rather than compounding it with tiny ones on
        // top.
        let mut entries = vec![entry("segments/huge", 1_000_000_000, true)];
        for i in 0..200 {
            entries.push(entry(&format!("segments/tiny-{i}"), 1_000, true));
        }
        let buckets = assign_buckets(&entries, BUCKET_COUNT);

        let bucket_of = |name: &str| {
            buckets
                .iter()
                .position(|b| b.iter().any(|p| p == name))
                .unwrap()
        };
        let huge_bucket = bucket_of("segments/huge");

        let sizes: Vec<u64> = buckets
            .iter()
            .map(|b| {
                b.iter()
                    .map(|p| {
                        if p == "segments/huge" {
                            1_000_000_000
                        } else {
                            1_000
                        }
                    })
                    .sum()
            })
            .collect();

        let max_size = *sizes.iter().max().unwrap();
        // The huge segment alone already exceeds any plausible sum of tiny
        // ones piled on any other bucket (200 * 1000 = 200_000, negligible
        // next to 1_000_000_000), so the max bucket must be (approximately)
        // exactly the huge bucket's size, not some multiple of it.
        assert_eq!(sizes[huge_bucket], max_size);
        assert!(
            max_size < 1_000_000_000 + 20_000,
            "huge bucket grew suspiciously beyond just the huge segment: {max_size}"
        );
    }

    #[test]
    fn no_entry_duplicated_or_dropped_across_buckets() {
        let mut entries = vec![
            entry("wal", 10, false),
            entry("shard_config.json", 5, false),
        ];
        for i in 0..37 {
            entries.push(entry(
                &format!("segments/seg-{i}"),
                (i as u64 + 1) * 777,
                true,
            ));
        }
        let buckets = assign_buckets(&entries, BUCKET_COUNT);

        let mut seen: Vec<&String> = buckets.iter().flatten().collect();
        seen.sort();
        let mut expected: Vec<&String> = entries.iter().map(|e| &e.relative_path).collect();
        expected.sort();
        assert_eq!(
            seen, expected,
            "every input entry must appear in exactly one bucket"
        );
    }

    #[test]
    fn single_bucket_gets_everything() {
        let entries = vec![
            entry("wal", 10, false),
            entry("segments/a", 100, true),
            entry("segments/b", 200, true),
        ];
        let buckets = assign_buckets(&entries, 1);
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].len(), 3);
    }

    #[test]
    fn more_buckets_than_segments_leaves_some_empty() {
        let entries = vec![entry("segments/a", 100, true)];
        let buckets = assign_buckets(&entries, BUCKET_COUNT);
        let non_empty = buckets.iter().filter(|b| !b.is_empty()).count();
        assert_eq!(non_empty, 1);
    }

    /// End-to-end round trip: build a fake shard directory with a mix of
    /// segment directories (of very different sizes) and fixed files,
    /// partition it into buckets, write each bucket as its own independent
    /// tar into an in-memory buffer using the same `write_bucket_tar` the
    /// real transfer path uses, then unpack every bucket's tar into a fresh
    /// target directory and assert the reconstructed tree is byte-for-byte
    /// identical to the original -- proving the partition is lossless (no
    /// gaps, no overlaps) independent of any network/HTTP layer.
    #[test]
    fn bucketed_round_trip_reproduces_original_tree() {
        let source_dir = tempfile::tempdir().unwrap();
        let shard_path = source_dir.path();

        std::fs::create_dir_all(shard_path.join("segments")).unwrap();
        std::fs::create_dir_all(shard_path.join("wal")).unwrap();
        std::fs::write(shard_path.join("wal/0.wal"), b"wal-bytes-here").unwrap();
        std::fs::write(shard_path.join("segments_manifest.json"), b"{}").unwrap();

        let segment_uuids = [
            "11111111-1111-1111-1111-111111111111",
            "22222222-2222-2222-2222-222222222222",
            "33333333-3333-3333-3333-333333333333",
            "44444444-4444-4444-4444-444444444444",
        ];
        for (i, uuid) in segment_uuids.iter().enumerate() {
            let seg_dir = shard_path.join("segments").join(uuid);
            std::fs::create_dir_all(&seg_dir).unwrap();
            // Deliberately different sizes per segment.
            let payload = vec![b'x'; (i + 1) * 4096];
            std::fs::write(seg_dir.join("data.bin"), &payload).unwrap();
            std::fs::write(seg_dir.join("meta.json"), format!("{{\"id\":{i}}}")).unwrap();
        }

        let entries = list_shard_entries(shard_path).unwrap();
        assert_eq!(entries.len(), 2 + segment_uuids.len()); // wal + manifest + 4 segments

        let buckets = assign_buckets(&entries, BUCKET_COUNT);

        let target_dir = tempfile::tempdir().unwrap();

        for bucket_entries in &buckets {
            if bucket_entries.is_empty() {
                continue;
            }
            let tar_file = tempfile::NamedTempFile::new().unwrap();
            {
                let file = std::fs::File::create(tar_file.path()).unwrap();
                let tar = common::tar_ext::BuilderExt::new_seekable_owned(file);
                write_bucket_tar(&tar, shard_path, bucket_entries).unwrap();
                tar.blocking_finish().unwrap();
            }
            let file = std::fs::File::open(tar_file.path()).unwrap();
            let mut archive = tar::Archive::new(file);
            archive.unpack(target_dir.path()).unwrap();
        }

        // Compare full reconstructed tree against the original, file by file.
        fn collect_files(root: &Path, base: &Path, out: &mut Vec<(String, Vec<u8>)>) {
            for entry in std::fs::read_dir(root).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_dir() {
                    collect_files(&path, base, out);
                } else {
                    let rel = path
                        .strip_prefix(base)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned();
                    let contents = std::fs::read(&path).unwrap();
                    out.push((rel, contents));
                }
            }
        }

        let mut original = Vec::new();
        collect_files(shard_path, shard_path, &mut original);
        original.sort();

        let mut reconstructed = Vec::new();
        collect_files(target_dir.path(), target_dir.path(), &mut reconstructed);
        reconstructed.sort();

        assert_eq!(
            original, reconstructed,
            "reconstructed shard tree must exactly match the original"
        );
    }

    /// The correctness-critical scenario Update 3 got wrong was concurrency:
    /// multiple streams racing to produce/consume data with only implicit
    /// coordination. This exercises real concurrent writers (building
    /// several buckets' tars on separate blocking threads at the same time)
    /// and real concurrent readers (unpacking several buckets into the
    /// *same* target directory at the same time), without a network layer,
    /// to isolate whether the "many independent streams into one directory
    /// tree" part of this design is safe under genuine concurrency.
    #[test]
    fn concurrent_bucket_build_and_unpack_is_safe() {
        let source_dir = tempfile::tempdir().unwrap();
        let shard_path = source_dir.path().to_path_buf();

        std::fs::create_dir_all(shard_path.join("segments")).unwrap();
        std::fs::create_dir_all(shard_path.join("wal")).unwrap();
        std::fs::write(shard_path.join("wal/0.wal"), vec![b'w'; 4096]).unwrap();

        let n_segments = 40;
        for i in 0..n_segments {
            let uuid = uuid::Uuid::from_u128(i as u128).to_string();
            let seg_dir = shard_path.join("segments").join(&uuid);
            std::fs::create_dir_all(&seg_dir).unwrap();
            std::fs::write(
                seg_dir.join("data.bin"),
                vec![(i % 256) as u8; 8192 + i * 37],
            )
            .unwrap();
        }

        let entries = list_shard_entries(&shard_path).unwrap();
        let buckets = assign_buckets(&entries, BUCKET_COUNT);

        // Concurrently build every non-empty bucket's tar on its own thread,
        // each into its own temp file.
        let tar_dir = tempfile::tempdir().unwrap();
        let tars: Vec<(usize, std::path::PathBuf)> = std::thread::scope(|scope| {
            let handles: Vec<_> = buckets
                .iter()
                .enumerate()
                .filter(|(_, entries)| !entries.is_empty())
                .map(|(i, bucket_entries)| {
                    let shard_path = shard_path.clone();
                    let tar_path = tar_dir.path().join(format!("bucket-{i}.tar"));
                    scope.spawn(move || {
                        {
                            let file = std::fs::File::create(&tar_path).unwrap();
                            let tar = common::tar_ext::BuilderExt::new_seekable_owned(file);
                            write_bucket_tar(&tar, &shard_path, bucket_entries).unwrap();
                            tar.blocking_finish().unwrap();
                        }
                        (i, tar_path)
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        let target_dir = tempfile::tempdir().unwrap();
        let target_path = target_dir.path().to_path_buf();

        // Concurrently unpack every bucket's tar into the *same* target
        // directory at the same time.
        std::thread::scope(|scope| {
            let handles: Vec<_> = tars
                .iter()
                .map(|(_, tar_path)| {
                    let target_path = target_path.clone();
                    let tar_path = tar_path.clone();
                    scope.spawn(move || {
                        let file = std::fs::File::open(&tar_path).unwrap();
                        let mut archive = tar::Archive::new(file);
                        archive.unpack(&target_path).unwrap();
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        });

        // Every segment must have made it across exactly once, with correct
        // contents -- if concurrent unpacking corrupted anything (partial
        // writes interleaving, directory creation races, ...) this would
        // show up as a mismatch here.
        for i in 0..n_segments {
            let uuid = uuid::Uuid::from_u128(i as u128).to_string();
            let expected = vec![(i % 256) as u8; 8192 + i * 37];
            let actual = std::fs::read(target_path.join("segments").join(&uuid).join("data.bin"))
                .unwrap_or_else(|e| panic!("segment {uuid} missing or unreadable: {e}"));
            assert_eq!(
                actual, expected,
                "segment {uuid} corrupted after concurrent transfer"
            );
        }
        let wal_contents = std::fs::read(target_path.join("wal/0.wal")).unwrap();
        assert_eq!(wal_contents, vec![b'w'; 4096]);
    }

    #[test]
    fn write_bucket_tar_produces_valid_tar_for_a_single_bucket() {
        let source_dir = tempfile::tempdir().unwrap();
        let shard_path = source_dir.path();
        std::fs::create_dir_all(shard_path.join("segments/aaaa")).unwrap();
        std::fs::write(shard_path.join("segments/aaaa/f.bin"), b"hello").unwrap();

        let tar_file = tempfile::NamedTempFile::new().unwrap();
        {
            let file = std::fs::File::create(tar_file.path()).unwrap();
            let tar = common::tar_ext::BuilderExt::new_seekable_owned(file);
            write_bucket_tar(&tar, shard_path, &["segments/aaaa".to_string()]).unwrap();
            tar.blocking_finish().unwrap();
        }

        let file = std::fs::File::open(tar_file.path()).unwrap();
        let mut archive = tar::Archive::new(file);
        let mut found = false;
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().to_string_lossy().into_owned();
            if path.ends_with("f.bin") {
                let mut contents = Vec::new();
                entry.read_to_end(&mut contents).unwrap();
                assert_eq!(contents, b"hello");
                found = true;
            }
        }
        assert!(found, "expected file not found in tar");
    }
}
