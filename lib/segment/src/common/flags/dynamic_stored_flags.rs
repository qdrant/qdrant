use std::borrow::Cow;
use std::cmp::max;
use std::fmt;
use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::mmap::{AdviceSetting, create_and_ensure_length};
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{OpenOptions, StoredStruct, UniversalWrite};
use fs_err as fs;
use itertools::Either;

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};

#[cfg(debug_assertions)]
const MINIMAL_MMAP_SIZE: usize = 128; // 128 bytes -> 1024 flags
#[cfg(not(debug_assertions))]
const MINIMAL_MMAP_SIZE: usize = 1024 * 1024; // 1Mb

const FLAGS_FILE: &str = "flags_a.dat";
const FLAGS_FILE_LEGACY: &str = "flags_b.dat";

const STATUS_FILE_NAME: &str = "status.dat";

fn status_file(directory: &Path) -> PathBuf {
    directory.join(STATUS_FILE_NAME)
}

#[derive(Debug, Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
pub struct DynamicFlagsStatus {
    /// Amount of flags (bits)
    len: usize,

    /// Should be 0 in the current version.  Old versions used it to indicate which flags file
    /// (flags_a.dat or flags_b.dat) is currently in use.
    current_file_id: usize,
}

/// Mutable persisted bitslice. This uses no buffering for updates.
///
/// For buffered variants, check
/// - [`RoaringFlags`][1] - with a RoaringBitmap for reads
/// - [`BitvecFlags`][2] - with a BitVec for reads
/// - [`BufferedDynamicFlags`][3] - no reads, only buffered persistence
///
/// [1]: super::roaring_flags::RoaringFlags
/// [2]: super::bitvec_flags::BitvecFlags
/// [3]: super::buffered_dynamic_flags::BufferedDynamicFlags
pub struct DynamicStoredFlags<S> {
    /// On-disk BitSlice for flags
    flags: StoredBitSlice<S>,
    status: StoredStruct<S, DynamicFlagsStatus>,
    directory: PathBuf,
}

impl<S: fmt::Debug> fmt::Debug for DynamicStoredFlags<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamicMmapFlags")
            .field("flags", &self.flags)
            .field("status", &self.status)
            .field("directory", &self.directory)
            .finish_non_exhaustive()
    }
}

/// Based on the number of flags determines the size in bytes for the storage file.
fn file_size_for(num_flags: usize) -> usize {
    let number_of_bytes = num_flags.div_ceil(u8::BITS as usize);

    max(MINIMAL_MMAP_SIZE, number_of_bytes.next_power_of_two())
}

impl<S> DynamicStoredFlags<S>
where
    S: UniversalWrite + Send + 'static,
{
    pub fn len(&self) -> usize {
        self.status.len
    }

    pub fn is_empty(&self) -> bool {
        self.status.len == 0
    }

    fn ensure_status_file(directory: &Path) -> OperationResult<PathBuf> {
        let status_file = status_file(directory);
        if !S::exists(&status_file)? {
            let length = std::mem::size_of::<DynamicFlagsStatus>();
            //TODO(uio): migrate when UniversalWriteFileOps is available
            create_and_ensure_length(&status_file, length)?;
        }
        Ok(status_file)
    }

    pub fn open(directory: &Path, populate: bool) -> OperationResult<Self> {
        fs::create_dir_all(directory)?;
        let status_path = Self::ensure_status_file(directory)?;

        let mut status: StoredStruct<S, DynamicFlagsStatus> = StoredStruct::open(
            &status_path,
            OpenOptions {
                writeable: true,
                need_sequential: false,
                disk_parallel: None,
                populate: Some(false),
                advice: None,
                prevent_caching: None,
            },
        )?;

        if status.current_file_id != 0 {
            // Migrate
            fs::copy(
                directory.join(FLAGS_FILE_LEGACY),
                directory.join(FLAGS_FILE),
            )?;
            status.current_file_id = 0;
            status.flusher()()?;
        }

        // Open storage
        let flags = Self::open_storage(status.len, directory, populate)?;
        Ok(Self {
            flags,
            status,
            directory: directory.to_owned(),
        })
    }

    fn open_storage(
        num_flags: usize,
        directory: &Path,
        populate: bool,
    ) -> OperationResult<StoredBitSlice<S>> {
        let capacity_bytes = file_size_for(num_flags);
        let path = directory.join(FLAGS_FILE);

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        file.set_len(capacity_bytes as u64)?;
        drop(file);

        let options = OpenOptions {
            writeable: true,
            populate: Some(populate),
            advice: Some(AdviceSetting::Global),
            ..Default::default()
        };
        let flags = StoredBitSlice::open(&path, options)?;
        Ok(flags)
    }

    /// Set the length of the vector to the given value.
    /// If the vector is grown, the new elements will be set to `false`.
    ///
    /// NOTE: capacity can be up to 2x the current length.
    ///
    /// Errors if the vector is shrunk.
    pub fn set_len(&mut self, new_len: usize) -> OperationResult<()> {
        debug_assert!(new_len >= self.status.len);
        if new_len == self.status.len {
            return Ok(());
        }

        if new_len < self.status.len {
            return Err(OperationError::service_error(format!(
                "Cannot shrink the mmap flags from {} to {new_len}",
                self.status.len,
            )));
        }

        // Capacity can be up to 2x the current length
        let current_capacity = usize::try_from(self.flags.bit_len()).expect("bit_len fits usize");

        if new_len > current_capacity {
            // Flush the current mmaps before resizing
            self.flags.flusher()()?;

            // Don't read the whole file on resize
            let populate = false;
            let flags = Self::open_storage(new_len, &self.directory, populate)?;

            // Swap operation. It is important this section is not interrupted by errors.
            self.flags = flags;
        }

        self.status.len = new_len;
        Ok(())
    }

    pub fn get(&self, index: usize) -> OperationResult<bool> {
        if index >= self.status.len {
            return Ok(false);
        }
        Ok(self.flags.get_bit(index as u64)?.unwrap_or(false))
    }

    /// Count number of set flags
    pub fn count_flags(&self) -> OperationResult<usize> {
        // Take a bitslice of our set length, count ones in it
        // This uses bit-indexing, returning a new bitslice, extra bits within capacity are not counted
        let bits = self.flags.read_all()?;
        Ok(bits
            .get(..self.status.len)
            .map(|s| s.count_ones())
            .unwrap_or(0))
    }

    /// Set the `true` value of the flag at the given index.
    /// Ignore the call if the index is out of bounds.
    ///
    /// Returns previous value of the flag.
    #[cfg(any(test, feature = "testing"))]
    pub fn set(&mut self, index: usize, value: bool) -> OperationResult<bool> {
        debug_assert!(index < self.status.len);
        if index >= self.status.len {
            return Ok(false);
        }
        Ok(self.flags.replace_bit(index as u64, value)?)
    }

    pub fn set_ascending_bits(
        &mut self,
        updates: impl IntoIterator<Item = (u64, bool)>,
    ) -> OperationResult<()> {
        Ok(self.flags.set_ascending_bits_batch(updates)?)
    }

    pub fn flusher(&self) -> Flusher {
        Box::new({
            let flags_flusher = self.flags.flusher();
            let status_flusher = self.status.flusher();
            move || {
                flags_flusher()?;
                status_flusher()?;
                Ok(())
            }
        })
    }

    pub fn get_bitslice(&self) -> OperationResult<Cow<'_, BitSlice>> {
        // Take subslice with actual length, bitslice may be larger due to extra allocated capacity
        // See `file_size_for`
        let len = self.len();
        Ok(match self.flags.read_all()? {
            Cow::Borrowed(bitslice) => Cow::Borrowed(&bitslice[..len]),
            Cow::Owned(mut bitvec) => {
                bitvec.truncate(len);
                Cow::Owned(bitvec)
            }
        })
    }

    // no immutable files, everything is mutable
    pub fn files(&self) -> Vec<PathBuf> {
        vec![
            status_file(&self.directory),
            self.directory.join(FLAGS_FILE),
        ]
    }

    /// Iterate over all "true" flags
    pub fn iter_trues(&self) -> OperationResult<impl Iterator<Item = PointOffsetType> + '_> {
        Ok(match self.flags.read_all()? {
            Cow::Borrowed(bitslice) => {
                Either::Left(bitslice.iter_ones().map(|x| x as PointOffsetType))
            }
            Cow::Owned(bitvec) => {
                // Owned path: backend doesn't support zero-copy; materialize into Vec
                // so we don't return a reference to a local
                let indices: Vec<PointOffsetType> =
                    bitvec.iter_ones().map(|x| x as PointOffsetType).collect();
                Either::Right(indices.into_iter())
            }
        })
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        self.flags.populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            flags,
            status: _,
            directory: _,
        } = self;
        flags.clear_ram_cache()?;
        Ok(())
    }
}

#[duplicate::duplicate_item(
    tests_mod       S               cfg_predicate;
    [tests_mmap]    [MmapFile]      [cfg(all())];
    [tests_uring]   [IoUringFile]   [cfg(target_os = "linux")];
)]
#[cfg_predicate]
#[cfg(test)]
mod tests_mod {
    use std::iter;

    #[cfg_predicate]
    use common::universal_io::S;
    use rand::prelude::StdRng;
    use rand::{RngExt, SeedableRng};
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_bitflags_saving() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let num_flags = 5000;
        let mut rng = StdRng::seed_from_u64(42);

        let random_flags: Vec<bool> = iter::repeat_with(|| rng.random()).take(num_flags).collect();

        {
            let mut dynamic_flags = DynamicStoredFlags::<S>::open(dir.path(), false).unwrap();
            dynamic_flags.set_len(num_flags).unwrap();
            random_flags
                .iter()
                .enumerate()
                .filter(|(_, flag)| **flag)
                .for_each(|(i, _)| assert!(!dynamic_flags.set(i, true).unwrap()));

            dynamic_flags.set_len(num_flags * 2).unwrap();
            random_flags
                .iter()
                .enumerate()
                .filter(|(_, flag)| !*flag)
                .for_each(|(i, _)| assert!(!dynamic_flags.set(num_flags + i, true).unwrap()));

            dynamic_flags.flusher()().unwrap();
        }

        {
            let dynamic_flags = DynamicStoredFlags::<S>::open(dir.path(), true).unwrap();
            assert_eq!(dynamic_flags.status.len, num_flags * 2);
            for (i, flag) in random_flags.iter().enumerate() {
                assert_eq!(dynamic_flags.get(i).unwrap(), *flag);
                assert_eq!(dynamic_flags.get(num_flags + i).unwrap(), !*flag);
            }
        }
    }

    #[test]
    fn test_bitflags_counting() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let num_flags = 5003; // Prime number, not byte aligned
        let mut rng = StdRng::seed_from_u64(42);

        // Create randomized dynamic mmap flags to test counting
        let mut dynamic_flags = DynamicStoredFlags::<S>::open(dir.path(), true).unwrap();
        dynamic_flags.set_len(num_flags).unwrap();
        let random_flags: Vec<bool> = iter::repeat_with(|| rng.random()).take(num_flags).collect();
        random_flags
            .iter()
            .enumerate()
            .filter(|(_, flag)| **flag)
            .for_each(|(i, _)| assert!(!dynamic_flags.set(i, true).unwrap()));
        dynamic_flags.flusher()().unwrap();

        // Test count flags method
        let count = dynamic_flags.count_flags().unwrap();

        // Compare against manually counting every flag
        let mut manual_count = 0;
        for i in 0..num_flags {
            if dynamic_flags.get(i).unwrap() {
                manual_count += 1;
            }
        }

        assert_eq!(count, manual_count);
    }

    #[test]
    fn test_capacity() {
        assert_eq!(file_size_for(0), 128);
        assert_eq!(file_size_for(1), 128);
        assert_eq!(file_size_for(1023), 128);
        assert_eq!(file_size_for(1024), 128);
        assert_eq!(file_size_for(1025), 256);
        assert_eq!(file_size_for(10000), 2048);
    }
}
