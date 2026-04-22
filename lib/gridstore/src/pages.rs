use std::borrow::Cow;
use std::collections::HashMap;
use std::iter;
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

    pub fn value_len_pages(pointer: ValuePointer, config: &StorageConfig) -> usize {
        let ValuePointer {
            page_id: _,
            block_offset,
            length: value_length,
        } = pointer;

        let page_size_bytes = config.page_size_bytes as u64;
        let block_size_bytes = config.block_size_bytes as u64;

        let block_offset = u64::from(block_offset);
        let value_length = u64::from(value_length);

        let byte_offset = block_offset * block_size_bytes;
        assert!(byte_offset < page_size_bytes);

        let pages = (byte_offset + value_length).div_ceil(page_size_bytes);
        pages as usize
    }

    pub fn read_from_pages<P: AccessPattern>(
        &self,
        pointer: ValuePointer,
        config: &StorageConfig,
    ) -> Result<Cow<'_, [u8]>> {
        let reads = Self::get_page_value_ranges(pointer, config)
            .map(|(buf_offset, page, range)| (buf_offset, &self.pages[page as usize], range));

        let pages = Self::value_len_pages(pointer, config);

        let mut buffer = if pages == 1 {
            // Avoid allocating buffer, if value fits within single page
            Vec::new()
        } else {
            // Avoid initializing buffer with zeros, as it will be overwritten by file read
            vec![MaybeUninit::uninit(); pointer.length as _]
        };

        for result in S::read_multi_iter::<P, _>(reads)? {
            let (offset, bytes) = result?;

            if pages == 1 {
                return Ok(bytes);
            }

            buffer[offset..offset + bytes.len()].write_copy_of_slice(&bytes);
        }

        Ok(Cow::Owned(unsafe { assume_init_vec(buffer) }))
    }

    pub fn read_batch_from_pages<P, I>(
        &self,
        pointers: I,
        config: &StorageConfig,
    ) -> Result<impl Iterator<Item = Result<(usize, Cow<'_, [u8]>)>>>
    where
        P: AccessPattern,
        I: IntoIterator<Item = ValuePointer>,
    {
        struct ReadMeta {
            value_idx: usize,
            buffer_offset: usize,
            len_bytes: usize,
            len_pages: usize,
        }

        let reads = pointers
            .into_iter()
            .enumerate()
            .flat_map(move |(value_idx, pointer)| {
                let len_bytes = pointer.length as usize;
                let len_pages = Self::value_len_pages(pointer, config);

                Self::get_page_value_ranges(pointer, config).map(
                    move |(buffer_offset, page_idx, range)| {
                        let meta = ReadMeta {
                            value_idx,
                            buffer_offset,
                            len_bytes,
                            len_pages,
                        };
                        let page = &self.pages[page_idx as usize];
                        (meta, page, range)
                    },
                )
            });

        let mut chunks = S::read_multi_iter::<P, _>(reads)?;
        let mut values = HashMap::new();

        let iter = iter::from_fn(move || {
            for result in chunks.by_ref() {
                let (
                    ReadMeta {
                        value_idx,
                        buffer_offset,
                        len_bytes,
                        len_pages,
                    },
                    bytes,
                ) = match result {
                    Ok(chunk) => chunk,
                    Err(err) => return Some(Err(err.into())),
                };

                if len_pages == 1 {
                    return Some(Ok((value_idx, bytes)));
                }

                let (value_buffer, pages_read) = values
                    .entry(value_idx)
                    .or_insert_with(|| (vec![MaybeUninit::uninit(); len_bytes], 0));

                value_buffer[buffer_offset..buffer_offset + bytes.len()]
                    .write_copy_of_slice(&bytes);

                *pages_read += 1;

                if *pages_read >= len_pages {
                    let (value_buffer, _) = values.remove(&value_idx).expect("value exists");
                    let value_buffer = unsafe { assume_init_vec(value_buffer) };

                    return Some(Ok((value_idx, Cow::Owned(value_buffer))));
                }
            }

            None
        });

        Ok(iter)
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
