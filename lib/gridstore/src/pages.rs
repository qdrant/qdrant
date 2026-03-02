use std::mem::MaybeUninit;
use std::path::Path;

use common::maybe_uninit::assume_init_vec;
use common::universal_io::{ElementsRange, OpenOptions, UniversalReadMulti};
use smallvec::SmallVec;

use crate::Result;
use crate::error::GridstoreError;
use crate::tracker::{BlockOffset, PageId, ValuePointer};

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct Pages<S> {
    pages: Vec<S>,
}

impl<S> Default for Pages<S> {
    fn default() -> Self {
        Self { pages: Vec::new() }
    }
}

#[allow(dead_code)]
impl<S: UniversalReadMulti<u8>> Pages<S> {
    pub fn open(dir: &Path) -> Result<Self> {
        let mut pages = Self::default();

        for page_id in 0.. {
            let page_path = dir.join(format!("page_{page_id}.dat"));

            if !page_path.exists() {
                break;
            }

            pages.attach_page(&page_path)?;
        }

        Ok(pages)
    }

    pub fn attach_page(&mut self, path: &Path) -> Result<()> {
        let options = OpenOptions {
            need_sequential: true,
            disk_parallel: None,
            populate: Some(false),
            advice: None,
        };

        let page = S::open(path, options)?;
        self.pages.push(page);

        Ok(())
    }

    pub fn num_pages(&self) -> usize {
        self.pages.len()
    }

    fn page_access_range(
        &self,
        page_id: PageId,
        block_offset: BlockOffset,
        length: u32,
        block_size_bytes: usize,
    ) -> Result<(ElementsRange, u64)> {
        let page_len = self
            .pages
            .get(page_id as usize)
            .ok_or(GridstoreError::PageNotFound { page_id })?
            .len()?;

        let length = u64::from(length);

        let value_start = u64::from(block_offset) * block_size_bytes as u64;
        assert!(value_start < page_len);

        let value_end = value_start + length;
        let unread_tail = value_end.saturating_sub(page_len);

        Ok((
            ElementsRange {
                start: value_start,
                length: length - unread_tail,
            },
            unread_tail,
        ))
    }

    pub fn read_from_pages<const READ_SEQUENTIAL: bool>(
        &self,
        pointer: ValuePointer,
        block_size_bytes: usize,
    ) -> Result<Vec<u8>> {
        let ValuePointer {
            page_id: start_page_id,
            mut block_offset,
            mut length,
        } = pointer;

        // Avoid initializing buffer with zeros, as it will be overwritten by file access;
        let mut raw_sections = vec![MaybeUninit::<u8>::uninit(); length as usize];

        // Do not expect payload to span more than 2 pages, but can be easily extended if needed
        let mut read_ranges = SmallVec::<[_; 2]>::new();
        // Store relative offsets to fill the raw_sections buffer in the callback
        let mut range_offset = SmallVec::<[_; 2]>::new();

        let mut length_so_far = 0;
        for page_id in start_page_id.. {
            let (element_range, unread_bytes) =
                self.page_access_range(page_id, block_offset, length, block_size_bytes)?;

            range_offset.push(length_so_far as usize);
            length_so_far += element_range.length;

            read_ranges.push((page_id as _, element_range));

            block_offset = 0;
            length = unread_bytes as u32;
        }

        S::read_multi::<READ_SEQUENTIAL>(self.pages.as_slice(), read_ranges, |idx, _, slice| {
            let offset = range_offset[idx];
            raw_sections[offset..offset + slice.len()].write_copy_of_slice(slice);
            Ok(())
        })?;

        Ok(unsafe { assume_init_vec(raw_sections) })
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
}
