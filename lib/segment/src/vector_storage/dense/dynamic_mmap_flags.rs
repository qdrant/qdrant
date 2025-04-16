use std::cmp::max;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, fs};

use bitvec::prelude::BitSlice;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::types::PointOffsetType;
use memmap2::MmapMut;
use memory::fadvise::clear_disk_cache;
use memory::madvise::{self, AdviceSetting, Madviseable as _};
use memory::mmap_ops::{create_and_ensure_length, open_write_mmap};
use memory::mmap_type::{MmapBitSlice, MmapFlusher, MmapType};
use parking_lot::Mutex;

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

#[repr(C)]
struct DynamicMmapStatus {
    /// Amount of flags (bits)
    len: usize,

    /// Should be 0 in the current version.  Old versions used it to indicate which flags file
    /// (flags_a.dat or flags_b.dat) is currently in use.
    current_file_id: usize,
}

fn ensure_status_file(directory: &Path) -> OperationResult<MmapMut> {
    let status_file = status_file(directory);
    if !status_file.exists() {
        let length = std::mem::size_of::<DynamicMmapStatus>();
        create_and_ensure_length(&status_file, length)?;
    }
    Ok(open_write_mmap(&status_file, AdviceSetting::Global, false)?)
}

pub struct DynamicMmapFlags {
    /// Current mmap'ed BitSlice for flags
    flags: MmapBitSlice,
    /// Flusher to flush current flags mmap
    flags_flusher: Arc<Mutex<Option<MmapFlusher>>>,
    status: MmapType<DynamicMmapStatus>,
    directory: PathBuf,
}

impl fmt::Debug for DynamicMmapFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamicMmapFlags")
            .field("flags", &self.flags)
            .field("status", &self.status)
            .field("directory", &self.directory)
            .finish_non_exhaustive()
    }
}

/// Based on the number of flags determines the size of the mmap file.
fn mmap_capacity_bytes(num_flags: usize) -> usize {
    let number_of_bytes = num_flags.div_ceil(u8::BITS as usize);

    max(MINIMAL_MMAP_SIZE, number_of_bytes.next_power_of_two())
}

/// Based on the current length determines how many flags can fit into the mmap file without resizing it.
fn mmap_max_current_size(len: usize) -> usize {
    let mmap_capacity_bytes = mmap_capacity_bytes(len);
    mmap_capacity_bytes * u8::BITS as usize
}

impl DynamicMmapFlags {
    pub fn len(&self) -> usize {
        self.status.len
    }

    pub fn is_empty(&self) -> bool {
        self.status.len == 0
    }

    pub fn open(directory: &Path, populate: bool) -> OperationResult<Self> {
        fs::create_dir_all(directory)?;
        let status_mmap = ensure_status_file(directory)?;
        let mut status: MmapType<DynamicMmapStatus> = unsafe { MmapType::try_from(status_mmap)? };

        if status.current_file_id != 0 {
            // Migrate
            fs::copy(
                directory.join(FLAGS_FILE_LEGACY),
                directory.join(FLAGS_FILE),
            )?;
            status.current_file_id = 0;
            status.flusher()()?;
        }

        // Open first mmap
        let (flags, flags_flusher) = Self::open_mmap(status.len, directory, populate)?;
        Ok(Self {
            flags,
            flags_flusher: Arc::new(Mutex::new(Some(flags_flusher))),
            status,
            directory: directory.to_owned(),
        })
    }

    fn open_mmap(
        num_flags: usize,
        directory: &Path,
        populate: bool,
    ) -> OperationResult<(MmapBitSlice, MmapFlusher)> {
        let capacity_bytes = mmap_capacity_bytes(num_flags);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join(FLAGS_FILE))?;
        file.set_len(capacity_bytes as u64)?;

        let flags_mmap = unsafe { MmapMut::map_mut(&file)? };
        drop(file);

        flags_mmap.madvise(madvise::get_global())?;

        if populate {
            flags_mmap.populate();
        } else {
            #[cfg(unix)]
            if let Err(err) = flags_mmap.advise(memmap2::Advice::WillNeed) {
                log::error!("Failed to advise MADV_WILLNEED for deleted flags: {err}");
            }
        }

        let flags = MmapBitSlice::try_from(flags_mmap, 0)?;
        let flusher = flags.flusher();
        Ok((flags, flusher))
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
        let current_capacity = mmap_max_current_size(self.status.len);

        if new_len > current_capacity {
            // Don't read the whole file on resize
            let populate = false;
            let (flags, flags_flusher) = Self::open_mmap(new_len, &self.directory, populate)?;

            // Swap operation. It is important this section is not interrupted by errors.
            {
                let mut flags_flusher_lock = self.flags_flusher.lock();
                self.flags = flags;
                flags_flusher_lock.replace(flags_flusher);
            }
        }

        self.status.len = new_len;
        Ok(())
    }

    pub fn get<TKey>(&self, key: TKey) -> bool
    where
        TKey: num_traits::cast::AsPrimitive<usize>,
    {
        let key: usize = key.as_();
        if key >= self.status.len {
            return false;
        }
        self.flags[key]
    }

    /// Count number of set flags
    pub fn count_flags(&self) -> usize {
        // Take a bitslice of our set length, count ones in it
        // This uses bit-indexing, returning a new bitslice, extra bits within capacity are not counted
        self.flags[..self.status.len].count_ones()
    }

    /// Set the `true` value of the flag at the given index.
    /// Ignore the call if the index is out of bounds.
    ///
    /// Returns previous value of the flag.
    pub fn set<TKey>(&mut self, key: TKey, value: bool) -> bool
    where
        TKey: num_traits::cast::AsPrimitive<usize>,
    {
        let key: usize = key.as_();
        debug_assert!(key < self.status.len);
        if key >= self.status.len {
            return false;
        }
        self.flags.replace(key, value)
    }

    /// This method will set the flag at the given index to the given value.
    /// If current length is not enough, it will resize the flags with amortized cost (x2)
    /// All new flags will be set to false.
    ///
    /// Returns previous value of the flag.
    pub fn set_with_resize<TKey>(
        &mut self,
        key: TKey,
        value: bool,
        hw_counter_ref: HwMetricRefCounter,
    ) -> OperationResult<bool>
    where
        TKey: num_traits::cast::AsPrimitive<usize>,
    {
        // Measure write of single bool.
        hw_counter_ref.incr_delta(size_of::<bool>());

        let key: usize = key.as_();
        if key >= self.status.len {
            if value {
                let new_len = key + 1;
                hw_counter_ref.incr_delta(new_len - self.status.len);
                self.set_len(new_len)?;
            } else {
                // Default value is false, so we don't need to resize
                return Ok(false);
            }
        }

        Ok(self.flags.replace(key, value))
    }

    pub fn flusher(&self) -> Flusher {
        Box::new({
            let flags_flusher = self.flags_flusher.clone();
            let status_flusher = self.status.flusher();
            move || {
                // Maybe we shouldn't take flusher here: FnOnce() -> Fn()
                if let Some(flags_flusher) = flags_flusher.lock().take() {
                    flags_flusher()?;
                }
                status_flusher()?;
                Ok(())
            }
        })
    }

    pub fn get_bitslice(&self) -> &BitSlice {
        &self.flags
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![
            status_file(&self.directory),
            self.directory.join(FLAGS_FILE),
        ]
    }

    /// Iterate over all "true" flags
    pub fn iter_trues(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
        self.flags.iter_ones().map(|x| x as PointOffsetType)
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        self.flags.populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        let flags_file = self.directory.join(FLAGS_FILE);
        clear_disk_cache(&flags_file)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::iter;

    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng};
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_bitflags_saving() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let num_flags = 5000;
        let mut rng = StdRng::seed_from_u64(42);

        let random_flags: Vec<bool> = iter::repeat_with(|| rng.random()).take(num_flags).collect();

        {
            let mut dynamic_flags = DynamicMmapFlags::open(dir.path(), false).unwrap();
            dynamic_flags.set_len(num_flags).unwrap();
            random_flags
                .iter()
                .enumerate()
                .filter(|(_, flag)| **flag)
                .for_each(|(i, _)| assert!(!dynamic_flags.set(i, true)));

            dynamic_flags.set_len(num_flags * 2).unwrap();
            random_flags
                .iter()
                .enumerate()
                .filter(|(_, flag)| !*flag)
                .for_each(|(i, _)| assert!(!dynamic_flags.set(num_flags + i, true)));

            dynamic_flags.flusher()().unwrap();
        }

        {
            let dynamic_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            assert_eq!(dynamic_flags.status.len, num_flags * 2);
            for (i, flag) in random_flags.iter().enumerate() {
                assert_eq!(dynamic_flags.get(i), *flag);
                assert_eq!(dynamic_flags.get(num_flags + i), !*flag);
            }
        }
    }

    #[test]
    fn test_bitflags_counting() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let num_flags = 5003; // Prime number, not byte aligned
        let mut rng = StdRng::seed_from_u64(42);

        // Create randomized dynamic mmap flags to test counting
        let mut dynamic_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
        dynamic_flags.set_len(num_flags).unwrap();
        let random_flags: Vec<bool> = iter::repeat_with(|| rng.random()).take(num_flags).collect();
        random_flags
            .iter()
            .enumerate()
            .filter(|(_, flag)| **flag)
            .for_each(|(i, _)| assert!(!dynamic_flags.set(i, true)));
        dynamic_flags.flusher()().unwrap();

        // Test count flags method
        let count = dynamic_flags.count_flags();

        // Compare against manually counting every flag
        let mut manual_count = 0;
        for i in 0..num_flags {
            if dynamic_flags.get(i) {
                manual_count += 1;
            }
        }

        assert_eq!(count, manual_count);
    }

    #[test]
    fn test_capacity() {
        assert_eq!(mmap_capacity_bytes(0), 128);
        assert_eq!(mmap_capacity_bytes(1), 128);
        assert_eq!(mmap_capacity_bytes(1023), 128);
        assert_eq!(mmap_capacity_bytes(1024), 128);
        assert_eq!(mmap_capacity_bytes(1025), 256);
        assert_eq!(mmap_capacity_bytes(10000), 2048);
    }
}
