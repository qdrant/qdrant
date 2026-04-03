use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};

use ahash::HashSet;
use common::generic_consts::AccessPattern;
use common::maybe_uninit::assume_init_vec;
use common::universal_io::{
    FileIndex, Flusher, OpenOptions, ReadRange, UniversalRead, UniversalWrite,
};

use crate::Result;
use crate::config::StorageConfig;
use crate::tracker::{PageId, ValuePointer};

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

    /// Computes the page ranges for reading or writing a value described by the given
    /// [`ValuePointer`].
    ///
    /// A value may span across two consecutive pages if it starts near the end of a page.
    /// Returns `(buffer_offset, page_index, range)` entries that together cover the full
    /// extent of the value.
    ///
    /// - `buffer_offset` offset in the data buffer
    /// - `page_index` index of the page to read from / write to
    /// - `range` in the page to read from / write to.
    ///    Length of the range matches length of the part of buffer
    ///
    ///VALUE BUFFER
    /// ----------------------------------------------------------------------
    /// buffer index:   0    1    2    3    4    5    6    7
    ///                 [ A ][ A ][ A ][ A ][ A ][ A ][ A ][ A ]
    ///
    /// PAGE 42  (value starts near end)
    /// ----------------------------------------------------------------------
    /// page index:     0    1    2    3    4    5    6    7    8    9   10   11
    ///                 [ . ][ . ][ . ][ . ][ . ][ . ][ . ][ . ][ . ][ A ][ A ][ A ]
    ///                                                                ^^^^^^^^^^^
    ///                                                                range = 9..12
    ///                                                                (3 bytes)
    ///
    /// PAGE 43  (continuation)
    /// ----------------------------------------------------------------------
    /// page index:     0    1    2    3    4    5    6    7    8    9   10   11
    ///                 [ A ][ A ][ A ][ A ][ A ][ . ][ . ][ . ][ . ][ . ][ . ][ . ]
    ///                 ^^^^^^^^^^^^^^^^^^^^^
    ///                 range = 0..5
    ///                 (remaining 5 bytes)
    ///
    /// MAPPING
    /// ----------------------------------------------------------------------
    /// buffer_offset = 0  --->  page 42[9..12]
    /// buffer_offset = 3  --->  page 43[0..5]
    fn get_page_value_ranges(
        pointer: ValuePointer,
        config: &StorageConfig,
    ) -> impl Iterator<Item = (usize, FileIndex, ReadRange)> {
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

        let mut length_so_far = 0;
        let mut page_idx = page_id as FileIndex;
        let mut start = value_start;

        std::iter::from_fn(move || {
            if length_so_far >= total_length {
                return None;
            }
            let remaining = total_length - length_so_far;
            let available_on_page = page_len - start;
            let section_len = remaining.min(available_on_page);

            let range = ReadRange {
                byte_offset: start,
                length: section_len,
            };
            let result = (length_so_far as usize, page_idx, range);

            length_so_far += section_len;
            page_idx += 1;
            start = 0;
            Some(result)
        })
    }

    pub fn read_from_pages<P: AccessPattern>(
        &self,
        pointer: ValuePointer,
        config: &StorageConfig,
    ) -> Result<Vec<u8>> {
        // Avoid initializing buffer with zeros, as it will be overwritten by file access;
        let mut raw_value = vec![MaybeUninit::<u8>::uninit(); pointer.length as usize];

        let reads = Self::get_page_value_ranges(pointer, config)
            .map(|(buf_offset, page_idx, range)| (buf_offset, &self.pages[page_idx], range));

        S::read_multi::<P, _>(reads, |offset, slice| {
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
        let writes =
            Self::get_page_value_ranges(pointer, config).map(|(start, file_idx, range)| {
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
