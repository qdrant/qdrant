use std::path::{Path, PathBuf};

use memmap2::MmapMut;
use memory::madvise::{Advice, AdviceSetting};
use memory::mmap_ops::{create_and_ensure_length, open_write_mmap};

use crate::tracker::BlockOffset;

#[derive(Debug)]
pub(crate) struct Page {
    path: PathBuf,
    mmap: MmapMut,
}

impl Page {
    /// Flushes outstanding memory map modifications to disk.
    pub(crate) fn flush(&self) -> std::io::Result<()> {
        self.mmap.flush()
    }

    /// Create a new page at the given path
    pub fn new(path: &Path, size: usize) -> Result<Page, String> {
        create_and_ensure_length(path, size).map_err(|err| err.to_string())?;
        let mmap = open_write_mmap(path, AdviceSetting::from(Advice::Normal), false)
            .map_err(|err| err.to_string())?;
        let path = path.to_path_buf();
        Ok(Page { path, mmap })
    }

    /// Open an existing page at the given path
    /// If the file does not exist, return None
    pub fn open(path: &Path) -> Result<Page, String> {
        if !path.exists() {
            return Err(format!("Page file does not exist: {}", path.display()));
        }
        let mmap = open_write_mmap(path, AdviceSetting::from(Advice::Normal), false)
            .map_err(|err| err.to_string())?;
        let path = path.to_path_buf();
        Ok(Page { path, mmap })
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
    ///
    /// # Returns
    /// - None if the value is not within the page
    /// - Some(slice) if the value was successfully read
    ///
    /// # Panics
    ///
    /// When the `block_offset` starts after the page ends
    ///
    pub fn read_value(
        &self,
        block_offset: BlockOffset,
        length: u32,
        block_size_bytes: usize,
    ) -> (&[u8], usize) {
        let value_start = block_offset as usize * block_size_bytes;

        let mmap_len = self.mmap.len();

        assert!(value_start < mmap_len);

        let value_end = value_start + length as usize;

        let unread_tail = value_end.saturating_sub(mmap_len);

        // read value region
        (
            &self.mmap[value_start..value_end - unread_tail],
            unread_tail,
        )
    }

    /// Delete the page from the filesystem.
    #[allow(dead_code)]
    pub fn delete_page(self) {
        drop(self.mmap);
        std::fs::remove_file(&self.path).unwrap();
    }
}
