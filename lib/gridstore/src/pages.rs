use std::mem::MaybeUninit;
use std::path::Path;

use common::maybe_uninit::assume_init_vec;
use common::universal_io::{ElementsRange, MultiUniversalRead, SourceId};
use smallvec::SmallVec;

use crate::Result;
use crate::tracker::{BlockOffset, PageId, ValuePointer};

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct Pages<S> {
    num_pages: usize,
    file_access: S,
}

#[allow(dead_code)]
impl<S: MultiUniversalRead<u8>> Pages<S> {
    pub fn open(dir: &Path) -> Result<Self> {
        let open_options = common::universal_io::OpenOptions {
            need_sequential: true,
            disk_parallel: None,
            populate: Some(false),
            advice: None,
        };

        let mut file_access = S::new(open_options);

        let mut page_id: usize = 0;
        loop {
            // ToDo: should be abstracted by file_access
            // ToDo: should list files to avoid round-trips to disk
            let page_path = dir.join(format!("page_{page_id}.dat"));
            if !page_path.exists() {
                break;
            }
            file_access.attach(&page_path)?;
            page_id += 1;
        }

        Ok(Self {
            num_pages: page_id,
            file_access,
        })
    }

    pub fn num_pages(&self) -> usize {
        self.num_pages
    }

    pub fn attach_page(&mut self, path: &Path) -> Result<()> {
        self.file_access.attach(path)?;
        self.num_pages += 1;
        Ok(())
    }

    fn page_access_range(
        &self,
        page_id: PageId,
        block_offset: BlockOffset,
        length: u32,
        block_size_bytes: usize,
    ) -> Result<(ElementsRange, u64)> {
        let length = u64::from(length);
        let page_len = self.file_access.source_len(SourceId(page_id as usize))?;
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

            read_ranges.push((SourceId(page_id as usize), element_range));

            block_offset = 0;
            length = unread_bytes as u32;
        }

        self.file_access
            .read_batch_multi::<READ_SEQUENTIAL>(read_ranges, |idx, slice| {
                let offset = range_offset[idx];
                raw_sections[offset..offset + slice.len()].write_copy_of_slice(slice);
                Ok(())
            })?;

        Ok(unsafe { assume_init_vec(raw_sections) })
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> Result<()> {
        self.file_access.populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> Result<()> {
        self.file_access.clear_ram_cache()?;
        Ok(())
    }
}
