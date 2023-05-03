use std::cmp::max;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};

use bitvec::prelude::BitSlice;
use memmap2::MmapMut;

use crate::common::error_logging::LogError;
use crate::common::mmap_ops::{
    create_and_ensure_length, open_write_mmap, read_from_mmap, write_to_mmap,
};
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::vector_storage::chunked_utils::{ensure_status_file, MmapStatus};
use crate::vector_storage::div_ceil;
use crate::vector_storage::mmap_vectors::mmap_to_bitslice;

#[cfg(test)]
const MINIMAL_MMAP_SIZE: usize = 1024; // 1Kb

#[cfg(not(test))]
const MINIMAL_MMAP_SIZE: usize = 1024 * 1024; // 1Mb

const FLAGS_FILE: &str = "flags.dat";

pub struct DynamicMmapFlags {
    status: MmapStatus,
    _flags_mmap: Option<MmapMut>,
    flags: Option<&'static mut BitSlice>,
    status_mmap: MmapMut,
    directory: PathBuf,
}

/// Based on the number of flags determines the size of the mmap file.
fn mmap_capacity_bytes(len: usize) -> usize {
    let number_of_bytes = div_ceil(len, 8);

    max(MINIMAL_MMAP_SIZE, number_of_bytes.next_power_of_two())
}

/// Based on the current length determines how many flags can fit into the mmap file without resizing it.
fn mmap_max_current_size(len: usize) -> usize {
    let mmap_capacity_bytes = mmap_capacity_bytes(len);
    mmap_capacity_bytes * 8
}

impl DynamicMmapFlags {
    fn mmap_file_path(directory: &Path) -> PathBuf {
        directory.join(FLAGS_FILE)
    }

    pub fn open(directory: &Path) -> OperationResult<Self> {
        create_dir_all(directory)?;
        let status_mmap = ensure_status_file(directory)?;
        let len = *read_from_mmap(&status_mmap, 0);

        let status = MmapStatus { len };

        let mut flags = Self {
            status,
            _flags_mmap: None,
            flags: None,
            status_mmap,
            directory: directory.to_owned(),
        };

        flags.reopen_mmap(len)?;

        Ok(flags)
    }

    pub fn reopen_mmap(&mut self, length: usize) -> OperationResult<()> {
        self.flags.take(); // Drop the bit slice
        self._flags_mmap.take(); // Drop the mmap
        let capacity_bytes = mmap_capacity_bytes(length);
        let mmap_path = Self::mmap_file_path(&self.directory);
        create_and_ensure_length(&mmap_path, capacity_bytes)?;
        let mut flags_mmap = open_write_mmap(&mmap_path).describe("Open mmap flags for writing")?;
        #[cfg(unix)]
        if let Err(err) = flags_mmap.advise(memmap2::Advice::WillNeed) {
            log::error!("Failed to advise MADV_WILLNEED for deleted flags: {}", err,);
        }
        let flags = unsafe { mmap_to_bitslice(&mut flags_mmap, 0) };
        self._flags_mmap = Some(flags_mmap);
        self.flags = Some(flags);
        Ok(())
    }

    /// Set the length of the vector to the given value.
    /// If the vector is grown, the new elements will be set to `false`.
    /// Errors if the vector is shrunk.
    pub fn set_len(&mut self, new_len: usize) -> OperationResult<()> {
        debug_assert!(new_len >= self.status.len);
        if new_len == self.status.len {
            return Ok(());
        }

        let current_capacity = mmap_max_current_size(self.status.len);

        if new_len > current_capacity {
            self.reopen_mmap(new_len)?;
        }

        if new_len < self.status.len {
            return Err(OperationError::service_error(format!(
                "Cannot shrink the mmap flags from {} to {}",
                self.status.len, new_len
            )));
        }

        self.status.len = new_len;
        write_to_mmap(&mut self.status_mmap, 0, self.status.len);
        Ok(())
    }

    pub fn get<TKey>(&self, key: TKey) -> bool
    where
        TKey: num_traits::cast::AsPrimitive<usize>,
    {
        let key: usize = key.as_();
        self.flags.as_ref().map(|flags| flags[key]).unwrap_or(false)
    }

    /// Set the `true` value of the flag at the given index.
    /// Ignore the call if the index is out of bounds.
    ///
    /// Returns previous value of the flag.
    pub fn set_true<TKey>(&mut self, key: TKey) -> bool
    where
        TKey: num_traits::cast::AsPrimitive<usize>,
    {
        let key: usize = key.as_();
        debug_assert!(key < self.status.len);
        if key >= self.status.len {
            return false;
        }

        if let Some(flags) = self.flags.as_mut() {
            return flags.replace(key, true);
        }

        false
    }

    pub fn flush(&self) -> OperationResult<()> {
        self.status_mmap.flush()?;
        if let Some(flags_mmap) = &self._flags_mmap {
            flags_mmap.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng};
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_bitflags_saving() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let num_flags = 5000;
        let mut rng = StdRng::seed_from_u64(42);

        let random_flags: Vec<bool> = (0..num_flags).map(|_| rng.gen_bool(0.5)).collect();

        {
            let mut dynamic_flags = DynamicMmapFlags::open(dir.path()).unwrap();
            dynamic_flags.set_len(num_flags).unwrap();
            for (i, flag) in random_flags.iter().enumerate() {
                if *flag {
                    assert!(!dynamic_flags.set_true(i));
                }
            }

            dynamic_flags.set_len(num_flags * 2).unwrap();
            for (i, flag) in random_flags.iter().enumerate() {
                if !*flag {
                    assert!(!dynamic_flags.set_true(num_flags + i));
                }
            }

            dynamic_flags.flush().unwrap();
        }

        {
            let dynamic_flags = DynamicMmapFlags::open(dir.path()).unwrap();
            assert_eq!(dynamic_flags.status.len, num_flags * 2);
            for (i, flag) in random_flags.iter().enumerate() {
                assert_eq!(dynamic_flags.get(i), *flag);
                assert_eq!(dynamic_flags.get(num_flags + i), !*flag);
            }
        }
    }

    #[test]
    fn test_capacity() {
        assert_eq!(mmap_capacity_bytes(0), 1024);
        assert_eq!(mmap_capacity_bytes(1), 1024);
        assert_eq!(mmap_capacity_bytes(1023), 1024);
        assert_eq!(mmap_capacity_bytes(1024), 1024);
        assert_eq!(mmap_capacity_bytes(1025), 1024);
        assert_eq!(mmap_capacity_bytes(10000), 2048);
    }
}
