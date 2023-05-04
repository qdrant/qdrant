use std::cmp::max;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bitvec::prelude::BitSlice;
use memmap2::MmapMut;
use parking_lot::Mutex;

use crate::common::error_logging::LogError;
use crate::common::mmap_ops::{
    create_and_ensure_length, open_write_mmap, transmute_from_u8_to_mut,
};
use crate::common::Flusher;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::vector_storage::div_ceil;
use crate::vector_storage::mmap_vectors::mmap_to_bitslice;

#[cfg(debug_assertions)]
const MINIMAL_MMAP_SIZE: usize = 128; // 128 bytes -> 1024 flags

#[cfg(not(debug_assertions))]
const MINIMAL_MMAP_SIZE: usize = 1024 * 1024; // 1Mb

// We need to switch between files to prevent loss of mmap in case of a error.
const FLAGS_FILE_A: &str = "flags_a.dat";
const FLAGS_FILE_B: &str = "flags_b.dat";

const STATUS_FILE_NAME: &str = "status.dat";

pub fn status_file(directory: &Path) -> PathBuf {
    directory.join(STATUS_FILE_NAME)
}

#[derive(Clone)]
struct RemovableMmap {
    mmap: Arc<Mutex<Option<MmapMut>>>,
}

impl RemovableMmap {
    fn new() -> Self {
        Self {
            mmap: Arc::new(Mutex::new(None)),
        }
    }

    fn is_empty(&self) -> bool {
        self.mmap.lock().is_none()
    }

    fn drop_mmap(&self) {
        self.mmap.lock().take();
    }

    fn set_mmap(&self, mmap: MmapMut) {
        self.mmap.lock().replace(mmap);
    }

    fn flush(&self) -> OperationResult<()> {
        if let Some(mmap) = self.mmap.lock().as_mut() {
            mmap.flush()?;
        }
        Ok(())
    }
}

#[repr(C)]
pub struct DynamicMmapStatus {
    pub len: usize,
    pub current_file_id: usize, // use usize for alignment
}

fn ensure_status_file(directory: &Path) -> OperationResult<MmapMut> {
    let status_file = status_file(directory);
    if !status_file.exists() {
        {
            let length = std::mem::size_of::<DynamicMmapStatus>();
            create_and_ensure_length(&status_file, length)?;
        }
        let mmap = open_write_mmap(&status_file)?;
        Ok(mmap)
    } else {
        open_write_mmap(&status_file)
    }
}

pub struct DynamicMmapFlags {
    _flags_mmap: RemovableMmap,
    flags: Option<&'static mut BitSlice>,
    _status_mmap: Arc<MmapMut>,
    status: &'static mut DynamicMmapStatus,
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
    fn mmap_file_path_a(directory: &Path) -> PathBuf {
        directory.join(FLAGS_FILE_A)
    }

    fn mmap_file_path_b(directory: &Path) -> PathBuf {
        directory.join(FLAGS_FILE_B)
    }

    fn current_to_file(&self, current: usize) -> PathBuf {
        match current {
            0 => Self::mmap_file_path_a(&self.directory),
            1 => Self::mmap_file_path_b(&self.directory),
            _ => unreachable!("current can be only 0 or 1"),
        }
    }

    pub fn len(&self) -> usize {
        self.status.len
    }

    pub fn open(directory: &Path) -> OperationResult<Self> {
        create_dir_all(directory)?;
        let status_mmap = ensure_status_file(directory)?;
        let status = transmute_from_u8_to_mut(&status_mmap);

        let mut flags = Self {
            _status_mmap: Arc::new(status_mmap),
            status,
            _flags_mmap: RemovableMmap::new(),
            flags: None,
            directory: directory.to_owned(),
        };

        flags.reopen_mmap(flags.status.len, flags.status.current_file_id)?;

        Ok(flags)
    }

    pub fn reopen_mmap(&mut self, length: usize, current_file: usize) -> OperationResult<()> {
        // We can only open file which is not currently used
        // self._flags_mmap.is_empty() - means that no files are open, we can open any file
        debug_assert!(self._flags_mmap.is_empty() || current_file != self.status.current_file_id);

        let capacity_bytes = mmap_capacity_bytes(length);
        let mmap_path = self.current_to_file(current_file);
        create_and_ensure_length(&mmap_path, capacity_bytes)?;
        let mut flags_mmap = open_write_mmap(&mmap_path).describe("Open mmap flags for writing")?;
        #[cfg(unix)]
        if let Err(err) = flags_mmap.advise(memmap2::Advice::WillNeed) {
            log::error!("Failed to advise MADV_WILLNEED for deleted flags: {}", err,);
        }
        let flags = unsafe { mmap_to_bitslice(&mut flags_mmap, 0) };

        // Very important, that this section is not interrupted by any errors.
        // Otherwise we can end up with inconsistent state
        {
            self.flags.take(); // Drop the bit slice. Important to do before dropping the mmap
            self._flags_mmap.drop_mmap(); // Drop the mmap
            self._flags_mmap.set_mmap(flags_mmap);
            self.flags = Some(flags);
        }

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

        if new_len < self.status.len {
            return Err(OperationError::service_error(format!(
                "Cannot shrink the mmap flags from {} to {}",
                self.status.len, new_len
            )));
        }

        let current_capacity = mmap_max_current_size(self.status.len);

        if new_len > current_capacity {
            // 0 -> 1
            // 1 -> 0
            // (CC), copilot
            let new_file_id = (self.status.current_file_id + 1) % 2;

            let old_mmap_file = self.current_to_file(self.status.current_file_id);
            let new_mmap_file = self.current_to_file(new_file_id);

            self._flags_mmap.flush()?;

            // copy the old file to the new one
            std::fs::copy(old_mmap_file, new_mmap_file)?;

            self.reopen_mmap(new_len, new_file_id)?;
            self.status.current_file_id = new_file_id;
        }

        self.status.len = new_len;
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
    pub fn set<TKey>(&mut self, key: TKey, value: bool) -> bool
    where
        TKey: num_traits::cast::AsPrimitive<usize>,
    {
        let key: usize = key.as_();
        debug_assert!(key < self.status.len);
        if key >= self.status.len {
            return false;
        }

        if let Some(flags) = self.flags.as_mut() {
            return flags.replace(key, value);
        }

        false
    }

    pub fn flusher(&self) -> Flusher {
        Box::new({
            let status_mmap = self._status_mmap.clone();
            let flags_mmap = self._flags_mmap.clone();
            move || {
                flags_mmap.flush()?;
                status_mmap.flush()?;
                Ok(())
            }
        })
    }

    pub fn get_bitslice(&self) -> &BitSlice {
        self.flags.as_ref().unwrap()
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![
            status_file(&self.directory),
            self.current_to_file(self.status.current_file_id),
        ]
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
                    assert!(!dynamic_flags.set(i, true));
                }
            }
            // File swapping happens every 1024 (MINIMAL_MMAP_SIZE) flags
            // < 1024 -> 0
            // < 2048 -> 1
            // < 4096 -> 0
            // < 8192 -> 1
            // < 16384 -> 0
            let expected_current_file_id = 1;
            assert_eq!(
                dynamic_flags.status.current_file_id,
                expected_current_file_id
            );

            dynamic_flags.set_len(num_flags * 2).unwrap();
            for (i, flag) in random_flags.iter().enumerate() {
                if !*flag {
                    assert!(!dynamic_flags.set(num_flags + i, true));
                }
            }

            let expected_current_file_id = 0;
            assert_eq!(
                dynamic_flags.status.current_file_id,
                expected_current_file_id
            );

            dynamic_flags.flusher()().unwrap();
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
        assert_eq!(mmap_capacity_bytes(0), 128);
        assert_eq!(mmap_capacity_bytes(1), 128);
        assert_eq!(mmap_capacity_bytes(1023), 128);
        assert_eq!(mmap_capacity_bytes(1024), 128);
        assert_eq!(mmap_capacity_bytes(1025), 256);
        assert_eq!(mmap_capacity_bytes(10000), 2048);
    }
}
