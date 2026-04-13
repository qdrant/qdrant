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

    /// Computes the page ranges for reading or writing a value described by the
    /// given [`ValuePointer`].
    ///
    /// Returns an iterator of tuples that together cover the full extent of the
    /// value. Each tuple contains information about its chunk.
    ///
    /// Typically, the iterator contains only one entry (if value is within a
    /// single page), or two entries if the value spans across two consecutive
    /// pages.
    ///
    /// # Example
    ///
    /// Assume each page is 8×1K blocks. The requested [`ValuePointer`] spans
    /// across two pages, so this function returns two tuples.
    ///
    /// ```text
    ///                            ┌── ValuePointer ───┐
    ///                            │ page_id:      123 │
    ///                            │ block_offset: 6   │  ←  requested value
    ///                            │ length:       5K  │
    ///                            └─────────┬─────────┘
    ///                           ╭──────────┴──────────╮
    /// ┐ ┌───┬───┬── page 123 ───┬───┬───┐ ┌───┬───┬── page 124 ───┬───┬───┐ ┌
    /// │ │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │ 7 │ │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │ 7 │ │
    /// ┘ └───┴───┴───┴───┴───┴───┴───┴───┘ └───┴───┴───┴───┴───┴───┴───┴───┘ └
    ///                           ╰───┬───╯ ╰─────┬─────╯
    ///              ┌────── tuple 0 ─┴───┐ ┌─────┴ tuple 1 ─────┐
    ///              │ buf_offset: 0      │ │ buf_offset: 2K     │  ←  returned
    ///              │ page_id:    123    │ │ page_id:    124    │      tuples
    ///              │ read_range: 6K..8K │ │ read_range: 0K..2K │
    ///              └─────────────────┬──┘ └────┬───────────────┘
    ///                            ┌───┴───┬─────┴─────┐
    ///                            │ 0..2K │ 2K..5K    │  ←  value buffer
    ///                            └───────┴───────────┘
    /// ```
    fn get_page_value_ranges(
        pointer: ValuePointer,
        config: &StorageConfig,
    ) -> impl Iterator<
        Item = (
            usize, // buf_offset - byte offset within the value buffer
            PageId,
            ReadRange,
        ),
    > {
        let ValuePointer {
            mut page_id,
            block_offset,
            length,
        } = pointer;

        let page_len = config.page_size_bytes as u64;
        let block_size_bytes = config.block_size_bytes as u64;
        let total_length = u64::from(length);

        let value_start = u64::from(block_offset) * block_size_bytes;
        assert!(value_start < page_len);

        let mut buf_offset = 0;
        let mut start = value_start;

        std::iter::from_fn(move || {
            if buf_offset >= total_length {
                return None;
            }
            let remaining = total_length - buf_offset;
            let available_on_page = page_len - start;
            let section_len = remaining.min(available_on_page);

            let read_range = ReadRange {
                byte_offset: start,
                length: section_len,
            };
            let result = (buf_offset as usize, page_id, read_range);

            buf_offset += section_len;
            page_id += 1;
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
            .map(|(buf_offset, page, range)| (buf_offset, &self.pages[page as usize], range));

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
        let Self {
            base_path: _,
            pages,
        } = self;
        for page in pages {
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
            Self::get_page_value_ranges(pointer, config).map(|(buf_offset, page, range)| {
                let data = &value[buf_offset..buf_offset + range.length as usize];
                (page as FileIndex, range.byte_offset, data)
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
