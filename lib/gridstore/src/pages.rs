use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};

use ahash::HashSet;
use common::generic_consts::AccessPattern;
use common::maybe_uninit::assume_init_vec;
use common::universal_io::{
    FileIndex, Flusher, OpenOptions, ReadRange, UniversalRead, UniversalWrite,
};
use smallvec::SmallVec;

use crate::Result;
use crate::config::StorageConfig;
use crate::tracker::{PageId, ValuePointer};

type PageRanges = SmallVec<[(FileIndex, ReadRange); 2]>;
type BufferOffsets = SmallVec<[usize; 2]>;

pub fn page_path(base_path: &Path, page_id: PageId) -> PathBuf {
    base_path.join(format!("page_{page_id}.dat"))
}

#[derive(Debug)]
pub(crate) struct Pages<S> {
    base_path: PathBuf,
    pages: Vec<S>,
}

impl<S> Pages<S> {
    pub fn clear(&mut self) {
        self.pages.clear();
    }
}

impl<S: UniversalRead<u8>> Pages<S> {
    pub fn new(base_path: PathBuf) -> Self {
        Self {
            base_path,
            pages: Vec::new(),
        }
    }

    pub fn open(dir: &Path) -> Result<Self> {
        let mut pages = Self::new(dir.to_path_buf());

        let page_files: HashSet<_> = S::list_files(&dir.join("page_"))?.into_iter().collect();

        for page_id in 0.. {
            let page_path = pages.page_path(page_id);
            if !page_files.contains(&page_path) {
                break;
            }
            pages.attach_page(&page_path)?;
        }

        Ok(pages)
    }

    pub fn attach_page(&mut self, path: &Path) -> Result<()> {
        let options = OpenOptions {
            writeable: true,
            need_sequential: true,
            disk_parallel: None,
            populate: Some(false),
            advice: None,
            prevent_caching: None,
        };

        let page = S::open(path, options)?;
        self.pages.push(page);

        Ok(())
    }

    pub fn num_pages(&self) -> usize {
        self.pages.len()
    }

    pub fn page_path(&self, page_id: PageId) -> PathBuf {
        page_path(&self.base_path, page_id)
    }

    /// Computes the page ranges and relative offsets for reading or writing a value described by
    /// the given [`ValuePointer`].
    ///
    /// A value may span across two consecutive pages if it starts near the end of a page. This
    /// method returns the list of `(page_index, range)` pairs that together cover the full extent
    /// of the value, along with the corresponding byte offsets into the value buffer so that each
    /// section can be read into or written from the correct position.
    ///
    /// Returns a tuple of:
    /// - `SmallVec<[(FileIndex, ReadRange); 2]>` — the per-page file ranges to access.
    /// - `SmallVec<[usize; 2]>` — the offset within the value buffer that each page range
    ///   corresponds to.
    fn get_page_value_ranges(
        pointer: ValuePointer,
        config: &StorageConfig,
    ) -> (PageRanges, BufferOffsets) {
        let ValuePointer {
            page_id,
            block_offset,
            length,
        } = pointer;

        let page_len = config.page_size_bytes as u64;
        let block_size_bytes = config.block_size_bytes as u64;
        let total_length = u64::from(length);

        let value_start = u64::from(block_offset) * block_size_bytes;
        assert!(value_start < page_len);

        // Do not expect payload to span more than 2 pages, but can be easily extended if needed
        let mut page_ranges = PageRanges::new();
        // Store relative offsets to fill the raw_value buffer after fetching in batch
        let mut buffer_offsets = BufferOffsets::new();

        let mut length_so_far: u64 = 0;
        let mut page_idx = page_id as FileIndex;
        let mut start = value_start;

        while length_so_far < total_length {
            let remaining = total_length - length_so_far;
            let available_on_page = page_len - start;
            let section_len = remaining.min(available_on_page);

            page_ranges.push((
                page_idx,
                ReadRange {
                    byte_offset: start,
                    length: section_len,
                },
            ));
            buffer_offsets.push(length_so_far as usize);

            length_so_far += section_len;
            page_idx += 1;
            start = 0;
        }

        (page_ranges, buffer_offsets)
    }

    pub fn read_from_pages<P: AccessPattern>(
        &self,
        pointer: ValuePointer,
        config: &StorageConfig,
    ) -> Result<Vec<u8>> {
        // Avoid initializing buffer with zeros, as it will be overwritten by file access;
        let mut raw_value = vec![MaybeUninit::<u8>::uninit(); pointer.length as usize];

        let (read_ranges, buffer_offsets) = Self::get_page_value_ranges(pointer, config);

        S::read_multi::<P>(self.pages.as_slice(), read_ranges, |idx, _, slice| {
            let offset = buffer_offsets[idx];
            raw_value[offset..offset + slice.len()].write_copy_of_slice(slice);
            Ok(())
        })?;

        Ok(unsafe { assume_init_vec(raw_value) })
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> Result<()> {
        for page in &self.pages {
            page.populate()?;
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> Result<()> {
        for page in &self.pages {
            page.clear_ram_cache()?;
        }
        Ok(())
    }

    /// This method reloads the pages storage from "disk", so that
    /// it should make newly written data is readable.
    ///
    /// Important assumptions:
    ///
    /// - Should only be called on read-only instances of the Pages.
    /// - Only appending new data is supported, for modifications of existing data there are no consistency guarantees.
    /// - Partial writes are possible, it is up to the caller to read only fully written data.
    pub fn live_reload(&mut self) -> Result<()> {
        let num_pages = self.pages.len();
        let next_page_id = num_pages as PageId;

        if num_pages > 0 {
            // Re-attach the last page, which should have the latest data.
            self.pages.pop();
            let page_path = self.page_path((num_pages - 1) as PageId);
            self.attach_page(&page_path)?;
        }

        for page_id in next_page_id.. {
            let page_path = self.page_path(page_id);
            if !S::exists(&page_path)? {
                break;
            }
            self.attach_page(&page_path)?;
        }

        Ok(())
    }
}

impl<S: UniversalWrite<u8>> Pages<S> {
    pub fn write_to_pages(
        &mut self,
        pointer: ValuePointer,
        value: &[u8],
        config: &StorageConfig,
    ) -> Result<()> {
        // Compute write ranges
        let (page_ranges, buffer_offsets) = Self::get_page_value_ranges(pointer, config);

        let writes = page_ranges
            .iter()
            .zip(&buffer_offsets)
            .map(|(&(file_idx, range), &start)| {
                (
                    file_idx,
                    range.byte_offset,
                    &value[start..start + range.length as usize],
                )
            });

        // Execute writes (mutable borrow of self.pages)
        S::write_multi(self.pages.as_mut_slice(), writes)?;

        Ok(())
    }

    pub fn flusher(&self) -> Flusher {
        let mut flushers = Vec::with_capacity(self.pages.len());
        for page in &self.pages {
            flushers.push(page.flusher());
        }
        Box::new(move || {
            for flusher in flushers {
                flusher()?;
            }
            Ok(())
        })
    }
}
