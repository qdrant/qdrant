//! Persistence of the on-disk field indexes' build-time "no values" mask.
//!
//! Written once at build, never mutated afterwards. The format is gated by
//! [`FeatureFlags::compact_bitmask`]: compact [`StoredBitmask`] when on, the
//! legacy raw bitslice otherwise. Reading accepts both formats regardless of
//! the flag — the compact file is tried first, then the index-specific
//! legacy file.
//!
//! [`FeatureFlags::compact_bitmask`]: common::flags::FeatureFlags::compact_bitmask

use std::ops::BitOrAssign;
use std::path::{Path, PathBuf};

use common::bitvec::BitVec;
use common::flags::feature_flags;
use common::mmap::{AdviceSetting, create_and_ensure_length};
use common::stored_bitmask::{BitmaskContent, StoredBitmask, save_bitmask};
use common::stored_bitslice::{MmapBitSlice, StoredBitSlice};
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, MmapFs, OkNotFound, OpenOptions, Populate, UniversalRead, UniversalReadFs,
};
use roaring::RoaringBitmap;

use crate::common::operation_error::{OperationError, OperationResult};

/// File name of the compact mask; legacy dense-bitslice file names are
/// index-specific.
pub(super) const DELETED_MASK_FILE: &str = "deleted_mask.bin";

pub(super) fn deleted_mask_path(dir: &Path) -> PathBuf {
    dir.join(DELETED_MASK_FILE)
}

/// Path of the file backing the mask, compact or legacy per the `compact`
/// value recorded at open time by [`bitor_deleted_mask`].
pub(super) fn deleted_mask_file(dir: &Path, compact: bool, legacy_file: &str) -> PathBuf {
    if compact {
        deleted_mask_path(dir)
    } else {
        dir.join(legacy_file)
    }
}

/// Persist the mask: compact format when the `compact_bitmask` feature flag
/// is on, legacy dense bitslice otherwise.
///
/// `empty_points` are the ascending offsets of points with no values;
/// `num_points` is the index's total point count. The file is always
/// created, even when `empty_points` is empty.
pub(super) fn save_deleted_mask(
    dir: &Path,
    legacy_file: &str,
    num_points: usize,
    empty_points: impl IntoIterator<Item = PointOffsetType>,
) -> OperationResult<()> {
    save_deleted_mask_format(
        dir,
        legacy_file,
        num_points,
        empty_points,
        feature_flags().compact_bitmask,
    )
}

fn save_deleted_mask_format(
    dir: &Path,
    legacy_file: &str,
    num_points: usize,
    empty_points: impl IntoIterator<Item = PointOffsetType>,
    compact: bool,
) -> OperationResult<()> {
    if compact {
        let ones = RoaringBitmap::from_sorted_iter(empty_points).map_err(|err| {
            OperationError::service_error(format!("deleted mask offsets are not ascending: {err}"))
        })?;
        save_bitmask(&MmapFs, &deleted_mask_path(dir), num_points as u64, ones)?;
        return Ok(());
    }

    // Legacy layout: raw bitslice, zero-padded up to whole u64 words.
    let legacy_path = dir.join(legacy_file);
    let _ = create_and_ensure_length(
        &legacy_path,
        num_points
            .div_ceil(u8::BITS as usize)
            .next_multiple_of(size_of::<u64>()),
    )?;
    let mut deleted = MmapBitSlice::open(
        &MmapFs,
        &legacy_path,
        OpenOptions {
            writeable: true,
            need_sequential: false,
            populate: Populate::Auto,
            advice: AdviceSetting::Global,
        },
        (),
    )?;
    deleted.set_ascending_bits_batch(empty_points.into_iter().map(|idx| (u64::from(idx), true)))?;
    deleted.flusher()()?;
    Ok(())
}

/// OR the persisted mask into `deleted`, which the caller has already resized
/// to the index's point count. Returns whether the compact file was used.
pub(super) fn bitor_deleted_mask<S, Fs>(
    fs: &Fs,
    dir: &Path,
    legacy_file: &str,
    options: OpenOptions,
    deleted: &mut BitVec,
) -> OperationResult<bool>
where
    S: UniversalRead,
    Fs: UniversalReadFs<File = S>,
{
    let compact = StoredBitmask::<S>::open(fs, deleted_mask_path(dir), options, Default::default())
        .ok_not_found()?;

    let Some(mask) = compact else {
        let legacy =
            StoredBitSlice::<S>::open(fs, dir.join(legacy_file), options, Default::default())?;
        // The legacy file is padded up to whole u64 words; the padding bits are
        // always clear, so OR-ing the full slice is harmless.
        deleted.bitor_assign(legacy.read_all()?.as_ref());
        return Ok(false);
    };

    match mask.read()? {
        BitmaskContent::Dense(bits) => deleted.bitor_assign(bits.as_ref()),
        BitmaskContent::Ones(ones) => {
            for idx in ones {
                let idx = idx as usize;
                if idx < deleted.len() {
                    deleted.set(idx, true);
                }
            }
        }
        BitmaskContent::Zeros(zeros) => {
            // Everything in `0..bit_len` except `zeros` is set: fill the gaps
            // between consecutive zero positions instead of setting the
            // (majority) ones one by one.
            let end = (mask.bit_len() as usize).min(deleted.len());
            let mut cursor = 0usize;
            for zero in zeros {
                let zero = zero as usize;
                if zero >= end {
                    break;
                }
                deleted[cursor..zero].fill(true);
                cursor = zero + 1;
            }
            deleted[cursor..end].fill(true);
        }
    }
    Ok(true)
}

/// Schedule background prefetch of whichever mask file exists.
pub(super) fn preopen_deleted_mask<S>(
    fs: &impl CachedReadFs<File = S>,
    dir: &Path,
    legacy_file: &str,
    options: OpenOptions,
) -> OperationResult<()>
where
    S: UniversalRead,
{
    if fs
        .schedule_prefetch(&deleted_mask_path(dir), Some(options), None)
        .ok_not_found()?
        .is_some()
    {
        return Ok(());
    }
    fs.schedule_prefetch(&dir.join(legacy_file), Some(options), None)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use common::mmap::{AdviceSetting, create_and_ensure_length};
    use common::universal_io::{MmapFile, MmapFs, Populate};
    use tempfile::TempDir;

    use super::*;

    const LEGACY_FILE: &str = "deleted.bin";

    fn options() -> OpenOptions {
        OpenOptions::new_for_test()
    }

    fn read_mask(dir: &Path, num_points: usize) -> (BitVec, bool) {
        let mut deleted = BitVec::repeat(false, num_points);
        let compact =
            bitor_deleted_mask::<MmapFile, _>(&MmapFs, dir, LEGACY_FILE, options(), &mut deleted)
                .unwrap();
        (deleted, compact)
    }

    #[test]
    fn compact_roundtrip_sparse() {
        let dir = TempDir::new().unwrap();
        let empty_points = [3u32, 17, 64, 9_999];
        save_deleted_mask_format(dir.path(), LEGACY_FILE, 10_000, empty_points, true).unwrap();

        let (deleted, compact) = read_mask(dir.path(), 10_000);
        assert!(compact);
        for idx in 0..10_000u32 {
            assert_eq!(deleted[idx as usize], empty_points.contains(&idx));
        }
    }

    #[test]
    fn compact_roundtrip_mostly_empty_uses_gap_fill() {
        // A field present on only a few points: the mask is mostly ones and is
        // stored as roaring-of-zeros, exercising the gap-fill merge path.
        let dir = TempDir::new().unwrap();
        let num_points = 100_000usize;
        let with_values = [0u32, 1, 50_000, 99_999];
        save_deleted_mask_format(
            dir.path(),
            LEGACY_FILE,
            num_points,
            (0..num_points as u32).filter(|idx| !with_values.contains(idx)),
            true,
        )
        .unwrap();

        let (deleted, compact) = read_mask(dir.path(), num_points);
        assert!(compact);
        for idx in 0..num_points as u32 {
            assert_eq!(
                deleted[idx as usize],
                !with_values.contains(&idx),
                "mismatch at {idx}",
            );
        }
    }

    #[test]
    fn flag_off_writes_legacy_format() {
        let dir = TempDir::new().unwrap();
        let empty_points = [3u32, 17, 64, 9_999];
        save_deleted_mask_format(dir.path(), LEGACY_FILE, 10_000, empty_points, false).unwrap();

        assert!(dir.path().join(LEGACY_FILE).exists());
        assert!(!deleted_mask_path(dir.path()).exists());

        let (deleted, compact) = read_mask(dir.path(), 10_000);
        assert!(!compact);
        for idx in 0..10_000u32 {
            assert_eq!(deleted[idx as usize], empty_points.contains(&idx));
        }
    }

    #[test]
    fn merge_preserves_existing_deletions() {
        let dir = TempDir::new().unwrap();
        save_deleted_mask_format(dir.path(), LEGACY_FILE, 100, [7u32], true).unwrap();

        let mut deleted = BitVec::repeat(false, 100);
        deleted.set(42, true); // pre-existing id-tracker deletion
        bitor_deleted_mask::<MmapFile, _>(
            &MmapFs,
            dir.path(),
            LEGACY_FILE,
            options(),
            &mut deleted,
        )
        .unwrap();
        assert!(deleted[7]);
        assert!(deleted[42]);
        assert_eq!(deleted.count_ones(), 2);
    }

    #[test]
    fn falls_back_to_legacy_dense_file() {
        let dir = TempDir::new().unwrap();
        let legacy_path = dir.path().join(LEGACY_FILE);

        // Legacy layout: raw bitslice padded to whole u64 words.
        let num_points = 100usize;
        create_and_ensure_length(
            &legacy_path,
            num_points
                .div_ceil(u8::BITS as usize)
                .next_multiple_of(size_of::<u64>()),
        )
        .unwrap();
        let mut legacy: StoredBitSlice<MmapFile> = StoredBitSlice::open(
            &MmapFs,
            &legacy_path,
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate: Populate::Auto,
                advice: AdviceSetting::Global,
            },
            (),
        )
        .unwrap();
        legacy
            .set_ascending_bits_batch([(5u64, true), (99, true)])
            .unwrap();
        legacy.flusher()().unwrap();
        drop(legacy);

        let (deleted, compact) = read_mask(dir.path(), num_points);
        assert!(!compact);
        assert!(deleted[5]);
        assert!(deleted[99]);
        assert_eq!(deleted.count_ones(), 2);
    }
}
