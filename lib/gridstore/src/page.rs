use std::borrow::Cow;
use std::path::{Path, PathBuf};

use common::fs::clear_disk_cache;
use common::mmap::create_and_ensure_length;
use common::universal_io::mmap::MmapUniversal;
use common::universal_io::{BytesRange, Flusher, UniversalRead, UniversalWrite};
use fs_err as fs;

use crate::Result;
use crate::error::GridstoreError;
use crate::tracker::BlockOffset;

#[derive(Debug)]
pub(crate) struct Page<S: UniversalWrite<u8>> {
    path: PathBuf,
    file_access: S,
}

impl<S: UniversalWrite<u8>> Page<S> {
    pub(crate) fn flusher(&self) -> Flusher {
        self.file_access.flusher()
    }

    /// Create a new page at the given path
    pub fn new(path: &Path, size: usize) -> Result<Page<MmapUniversal<u8>>> {
        create_and_ensure_length(path, size)?;

        let file_access = MmapUniversal::open(
            path,
            common::universal_io::OpenOptions {
                need_sequential: true,
                disk_parallel: None,
                populate: Some(false),
            },
        )?;

        let path = path.to_path_buf();

        Ok(Page { path, file_access })
    }

    /// Open an existing page at the given path
    /// If the file does not exist, return None
    pub fn open(path: &Path) -> Result<Page<MmapUniversal<u8>>> {
        if !path.exists() {
            return Err(GridstoreError::service_error(format!(
                "Page file does not exist: {}",
                path.display()
            )));
        }

        let file_access = MmapUniversal::open(
            path,
            common::universal_io::OpenOptions {
                need_sequential: true,
                disk_parallel: None,
                populate: Some(false),
            },
        )?;

        let path = path.to_path_buf();
        Ok(Page { path, file_access })
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
    ) -> Result<usize> {
        // The size of the data cell containing the value
        let value_size = value.len();

        let value_start = block_offset as usize * block_size_bytes;

        let value_end = value_start + value_size;
        let page_len = self.file_access.len()? as usize;

        // only write what fits in the page
        let unwritten_tail = value_end.saturating_sub(page_len);

        self.file_access
            .write(value_start as u64, &value[..value_size - unwritten_tail])?;

        Ok(unwritten_tail)
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
    ) -> Result<(Cow<'_, [u8]>, u64)> {
        Self::read_value_with_generic_storage::<READ_SEQUENTIAL>(
            &self.file_access,
            block_offset,
            length,
            block_size_bytes,
        )
    }

    fn read_value_with_generic_storage<const SEQUENTIAL: bool>(
        file_access: &impl UniversalRead<u8>,
        block_offset: BlockOffset,
        length: u32,
        block_size_bytes: usize,
    ) -> Result<(Cow<'_, [u8]>, u64)> {
        let value_start = u64::from(block_offset) * block_size_bytes as u64;

        let page_len = file_access.len()?;

        assert!(value_start < page_len);

        let value_end = value_start + u64::from(length);

        let unread_tail = value_end.saturating_sub(page_len);

        let data = file_access.read::<SEQUENTIAL>(BytesRange {
            start: value_start,
            length: (value_end - value_start) - unread_tail,
        })?;

        Ok((data, unread_tail))
    }

    /// Delete the page from the filesystem.
    #[allow(dead_code)]
    pub fn delete_page(self) {
        drop(self.file_access);
        fs::remove_file(&self.path).unwrap();
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> Result<()> {
        self.file_access.populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> std::io::Result<()> {
        clear_disk_cache(&self.path)?;
        Ok(())
    }
}
