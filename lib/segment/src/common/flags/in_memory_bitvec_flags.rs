use std::path::{Path, PathBuf};

use common::bitvec::{BitSlice, BitVec};
use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OpenOptions, Populate, TypedStorage, UniversalRead, UniversalReadFs,
};

use super::dynamic_stored_flags::{DynamicFlagsStatus, FLAGS_FILE, status_file};
use crate::common::operation_error::{OperationError, OperationResult};

/// In-memory counterpart of `BitvecFlags`: persisted flags materialized into an
/// owned `BitVec`, no write path.
///
/// The only consumer is the vector storages' `deleted` set. On live-reload the
/// id-tracker delta supplies whole-point deletions ([`Self::insert_all`]), but
/// an appended point can also carry a per-vector deletion recorded only in the
/// on-disk flags file — a missing named vector is stored as a placeholder and
/// its slot deleted. So the backing directory is retained and
/// [`Self::reload_appended`] reopens the flags file to read the persisted bit
/// of each appended offset. The set only ever grows, so a whole-point deletion
/// already folded in but not yet flushed to the flags file is never lost to a
/// re-read.
#[derive(Debug)]
#[allow(dead_code)] // pending: read-only vector storages will hold `deleted` as this
pub struct InMemoryBitvecFlags {
    /// Flags, materialized on open and patched in place on live-reload.
    bitvec: BitVec,
    /// Set-flag count, kept in sync with `bitvec`.
    count: usize,
    /// Backing directory of the dynamic flags file, so [`Self::reload_appended`]
    /// can reopen it. `None` for flags built via [`Self::from_bitvec`].
    directory: Option<PathBuf>,
}

/// Read-only mmap options: never writable, lazily paged, nothing populated.
fn bitslice_open_options(populate: Populate) -> OpenOptions {
    OpenOptions {
        writeable: false,
        need_sequential: false,
        populate,
        advice: AdviceSetting::Global,
    }
}

impl InMemoryBitvecFlags {
    /// Schedule background prefetch of the two files [`Self::open`] reads.
    pub fn preopen(fs: &impl CachedReadFs, directory: &Path) -> OperationResult<()> {
        // Status file
        fs.schedule_prefetch(
            &status_file(directory),
            Some(bitslice_open_options(Populate::PreferBackground)),
            None,
        )?;

        // Bitslice
        fs.schedule_prefetch(
            &directory.join(FLAGS_FILE),
            Some(bitslice_open_options(Populate::PreferBackground)),
            None,
        )?;
        Ok(())
    }

    /// Open persisted flags read-only into an owned `BitVec`; creates and writes
    /// nothing. The flags file is padded past the logical length (held in the
    /// status file), so the bitvec is truncated to it and `count` is exact.
    pub fn open<S: UniversalRead>(
        fs: &impl UniversalReadFs<File = S>,
        directory: &Path,
    ) -> OperationResult<Self> {
        // Length via TypedStorage; StoredStruct is write-bound.
        let status = TypedStorage::<S, DynamicFlagsStatus>::new(fs.open(
            status_file(directory),
            bitslice_open_options(Populate::No),
            Default::default(),
        )?);
        let len = status
            .read_whole()?
            .first()
            .map_or(0, DynamicFlagsStatus::len);

        let flags_path = directory.join(FLAGS_FILE);
        let flags = StoredBitSlice::<S>::open(
            fs,
            &flags_path,
            bitslice_open_options(Populate::No),
            Default::default(),
        )?;
        let bits = flags.read_all()?;
        let bitvec = bits.get(..len).map(BitVec::from_bitslice).ok_or_else(|| {
            OperationError::service_error(format!(
                "Flags file {} holds fewer than {len} bits",
                flags_path.display(),
            ))
        })?;
        let count = bitvec.count_ones();

        Ok(Self {
            bitvec,
            count,
            directory: Some(directory.to_path_buf()),
        })
    }

    /// Wrap an already-materialized deletion `bitvec`, computing the set-flag
    /// count. For flags coming from an on-disk format other than the dynamic
    /// flags read by [`Self::open`] (e.g. the immutable dense `deleted.dat`).
    pub fn from_bitvec(bitvec: BitVec) -> Self {
        let count = bitvec.count_ones();
        Self {
            bitvec,
            count,
            directory: None,
        }
    }

    /// Whether the flag at `key` is set; out-of-range keys read as unset.
    pub fn get(&self, key: PointOffsetType) -> bool {
        self.bitvec.get(key as usize).is_some_and(|bit| *bit)
    }

    /// Number of set flags.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Set flags as a `BitSlice`.
    pub fn as_bitslice(&self) -> &BitSlice {
        self.bitvec.as_bitslice()
    }

    /// Set `points`, growing as needed and keeping `count` in sync. Folds a
    /// live-reload deletion delta; live offsets aren't passed (they read unset).
    pub fn insert_all(&mut self, points: &[PointOffsetType]) {
        for &point in points {
            let index = point as usize;
            if index >= self.bitvec.len() {
                self.bitvec.resize(index + 1, false);
            }
            if !self.bitvec.replace(index, true) {
                self.count += 1;
            }
        }
    }

    /// Fold the persisted deletion bit of each appended offset into the set.
    ///
    /// An appended point can carry a deleted vector slot whose flag lives only
    /// in the on-disk flags file, not the id-tracker delta (see the type doc).
    /// The flags file is reopened and the persisted bit of every `new_point` is
    /// read back; live offsets read unset and change nothing. A no-op for flags
    /// built via [`Self::from_bitvec`], which have no dynamic flags file.
    pub fn reload_appended<S: UniversalRead>(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
        new_points: &[PointOffsetType],
    ) -> OperationResult<()> {
        let Some(directory) = self.directory.clone() else {
            return Ok(());
        };
        if new_points.is_empty() {
            return Ok(());
        }

        let flags = StoredBitSlice::<S>::open(
            fs,
            &directory.join(FLAGS_FILE),
            bitslice_open_options(Populate::No),
            Default::default(),
        )?;

        let mut deleted = Vec::new();
        for &point in new_points {
            if flags.get_bit(u64::from(point))?.unwrap_or(false) {
                deleted.push(point);
            }
        }
        self.insert_all(&deleted);

        Ok(())
    }
}

#[allow(clippy::default_constructed_unit_structs)]
#[duplicate::duplicate_item(
    tests_mod       S               Fs              cfg_predicate;
    [tests_mmap]    [MmapFile]      [MmapFs]        [cfg(all())];
    [tests_uring]   [IoUringFile]   [IoUringFs]     [cfg(target_os = "linux")];
)]
#[cfg_predicate]
#[cfg(test)]
mod tests_mod {
    use std::iter;

    #[cfg_predicate]
    use common::universal_io::{Fs, S};
    use rand::prelude::StdRng;
    use rand::{RngExt, SeedableRng};
    use tempfile::Builder;

    use super::*;
    use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;

    /// Persist `flags` via the writable storage so the read-only path can open it.
    fn persist(fs: &Fs, dir: &Path, flags: &[bool]) {
        let mut dynamic_flags = DynamicStoredFlags::<S>::open(fs, dir, Populate::No).unwrap();
        dynamic_flags.set_len(fs, flags.len()).unwrap();
        flags
            .iter()
            .enumerate()
            .filter(|(_, flag)| **flag)
            .for_each(|(i, _)| assert!(!dynamic_flags.set(i, true).unwrap()));
        dynamic_flags.flusher()().unwrap();
    }

    #[test]
    fn open_materializes_persisted_flags() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let num_flags = 5003; // Prime number, not byte aligned
        let mut rng = StdRng::seed_from_u64(42);
        let random_flags: Vec<bool> = iter::repeat_with(|| rng.random()).take(num_flags).collect();

        persist(&Fs::default(), dir.path(), &random_flags);

        let flags = InMemoryBitvecFlags::open::<S>(&Fs::default(), dir.path()).unwrap();

        let expected_count = random_flags.iter().filter(|flag| **flag).count();
        assert_eq!(flags.count(), expected_count);
        for (i, &flag) in random_flags.iter().enumerate() {
            assert_eq!(flags.get(i as PointOffsetType), flag);
        }
    }

    #[test]
    fn insert_all_folds_deletion_delta() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let num_flags = 1000;
        let mut rng = StdRng::seed_from_u64(7);
        let random_flags: Vec<bool> = iter::repeat_with(|| rng.random()).take(num_flags).collect();

        persist(&Fs::default(), dir.path(), &random_flags);
        let mut flags = InMemoryBitvecFlags::open::<S>(&Fs::default(), dir.path()).unwrap();
        let base_count = flags.count();

        // One offset already set, one unset in range, one past the end (grows).
        let already_set = random_flags.iter().position(|flag| *flag).unwrap();
        let unset = random_flags.iter().position(|flag| !*flag).unwrap();
        let beyond = num_flags as PointOffsetType + 5;
        flags.insert_all(&[
            already_set as PointOffsetType,
            unset as PointOffsetType,
            beyond,
        ]);

        // Only the two newly-set offsets bump the count.
        assert_eq!(flags.count(), base_count + 2);
        assert!(flags.get(already_set as PointOffsetType));
        assert!(flags.get(unset as PointOffsetType));
        assert!(flags.get(beyond));
        assert!(!flags.get(beyond + 1));
    }
}
