use std::path::{Path, PathBuf};

use common::fs::clear_disk_cache;
use common::mmap::{
    Advice, AdviceSetting, MULTI_MMAP_IS_SUPPORTED, Madviseable, create_and_ensure_length,
    open_read_mmap, open_write_mmap,
};
use fs_err as fs;
use memmap2::{Mmap, MmapMut};

use crate::Result;
use crate::error::GridstoreError;
use crate::tracker::BlockOffset;

#[derive(Debug)]
pub(crate) struct Page {
    path: PathBuf,
    /// Main data mmap for read/write
    ///
    /// Best suited for random reads.
    mmap: MmapMut,
    /// Read-only mmap best suited for sequential reads
    ///
    /// `None` on platforms that do not support multiple memory maps to the same file.
    /// Use [`mmap_seq`] utility function to access this mmap if available.
    _mmap_seq: Option<Mmap>,
}

impl Page {
    /// Flushes outstanding memory map modifications to disk.
    pub(crate) fn flush(&self) -> std::io::Result<()> {
        self.mmap.flush()
    }

    /// Create a new page at the given path
    pub fn new(path: &Path, size: usize) -> Result<Page> {
        create_and_ensure_length(path, size)?;
        let mmap = open_write_mmap(path, AdviceSetting::from(Advice::Random), false)?;

        // Only open second mmap for sequential reads if supported
        let mmap_seq = if *MULTI_MMAP_IS_SUPPORTED {
            Some(open_read_mmap(
                path,
                AdviceSetting::from(Advice::Sequential),
                false,
            )?)
        } else {
            None
        };

        let path = path.to_path_buf();

        Ok(Page {
            path,
            mmap,
            _mmap_seq: mmap_seq,
        })
    }

    /// Open an existing page at the given path
    /// If the file does not exist, return None
    pub fn open(path: &Path) -> Result<Page> {
        if !path.exists() {
            return Err(GridstoreError::service_error(format!(
                "Page file does not exist: {}",
                path.display()
            )));
        }
        let mmap = open_write_mmap(path, AdviceSetting::from(Advice::Random), false)?;

        // Only open second mmap for sequential reads if supported
        let mmap_seq = if *MULTI_MMAP_IS_SUPPORTED {
            Some(open_read_mmap(
                path,
                AdviceSetting::from(Advice::Sequential),
                false,
            )?)
        } else {
            None
        };

        let path = path.to_path_buf();
        Ok(Page {
            path,
            mmap,
            _mmap_seq: mmap_seq,
        })
    }

    /// Helper to get a slice suited for sequential reads if available, otherwise use the main mmap
    #[inline]
    fn mmap_seq(&self) -> &[u8] {
        #[expect(clippy::used_underscore_binding)]
        self._mmap_seq
            .as_ref()
            .map(|m| m.as_ref())
            .unwrap_or(self.mmap.as_ref())
    }

    /// Write a value into the page
    ///
    /// # Returns
    /// Amount of bytes that didn't fit into the page
    ///
    /// # Corruption
    ///
    /// If the block_offset and length of the value are already taken, this function will still overwrite the data.
    pub fn write_value(
        &mut self,
        block_offset: u32,
        value: &[u8],
        block_size_bytes: usize,
    ) -> usize {
        // The size of the data cell containing the value
        let value_size = value.len();

        let value_start = block_offset as usize * block_size_bytes;

        let value_end = value_start + value_size;
        // only write what fits in the page
        let unwritten_tail = value_end.saturating_sub(self.mmap.len());

        // set value region
        self.mmap[value_start..value_end - unwritten_tail]
            .copy_from_slice(&value[..value_size - unwritten_tail]);

        unwritten_tail
    }

    /// Read a value from the page
    ///
    /// # Arguments
    /// - block_offset: The offset of the value in blocks
    /// - length: The number of blocks the value occupies
    /// - READ_SEQUENTIAL: Whether to read mmap pages ahead to optimize sequential access
    ///
    /// # Returns
    /// - None if the value is not within the page
    /// - Some(slice) if the value was successfully read
    ///
    /// # Panics
    ///
    /// If the `block_offset` starts after the page ends.
    pub fn read_value<const READ_SEQUENTIAL: bool>(
        &self,
        block_offset: BlockOffset,
        length: u32,
        block_size_bytes: usize,
    ) -> (&[u8], usize) {
        if READ_SEQUENTIAL {
            Self::read_value_with_generic_storage(
                self.mmap_seq(),
                block_offset,
                length,
                block_size_bytes,
            )
        } else {
            Self::read_value_with_generic_storage(
                &self.mmap,
                block_offset,
                length,
                block_size_bytes,
            )
        }
    }

    fn read_value_with_generic_storage(
        mmap: &[u8],
        block_offset: BlockOffset,
        length: u32,
        block_size_bytes: usize,
    ) -> (&[u8], usize) {
        let value_start = block_offset as usize * block_size_bytes;

        let mmap_len = mmap.len();

        assert!(value_start < mmap_len);

        let value_end = value_start + length as usize;

        let unread_tail = value_end.saturating_sub(mmap_len);

        // read value region
        (&mmap[value_start..value_end - unread_tail], unread_tail)
    }

    /// Delete the page from the filesystem.
    #[allow(dead_code)]
    pub fn delete_page(self) {
        #[expect(clippy::used_underscore_binding)]
        drop((self.mmap, self._mmap_seq));
        fs::remove_file(&self.path).unwrap();
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) {
        #[expect(clippy::used_underscore_binding)]
        if let Some(mmap_seq) = &self._mmap_seq {
            mmap_seq.populate();
        }
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> std::io::Result<()> {
        clear_disk_cache(&self.path)?;
        Ok(())
    }
}
