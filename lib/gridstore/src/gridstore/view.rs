use std::cmp::min;
use std::ops::Deref;

use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::universal_io::UniversalRead;
use lz4_flex::compress_prepend_size;

use crate::Result;
use crate::blob::Blob;
use crate::config::{Compression, StorageConfig};
use crate::error::GridstoreError;
use crate::page::Page;
use crate::tracker::{BlockOffset, PageId, PointOffset, Tracker, ValuePointer};

#[inline]
pub(super) fn compress_lz4(value: &[u8]) -> Vec<u8> {
    compress_prepend_size(value)
}

#[inline]
pub(super) fn decompress_lz4(value: &[u8]) -> Vec<u8> {
    lz4_flex::decompress_size_prepended(value).unwrap()
}

/// A non-owning view into gridstore data.
///
/// Holds borrowed references to pages and tracker, and contains all reading logic.
/// Generic over the page storage backend `S`, so it works with any `UniversalRead<u8>`
/// implementation (mmap, memory buffers, etc.).
///
/// Constructed from either [`super::Gridstore`] or [`super::GridstoreReader`].
pub struct GridstoreView<'a, V, S: UniversalRead<u8>> {
    pub(super) config: &'a StorageConfig,
    pub(super) tracker: &'a Tracker,
    pub(super) pages: &'a [Page<S>],
    pub(super) _value_type: std::marker::PhantomData<V>,
}

impl<'a, V, S: UniversalRead<u8>> GridstoreView<'a, V, S> {
    pub(crate) fn new(
        config: &'a StorageConfig,
        tracker: &'a Tracker,
        pages: &'a [Page<S>],
    ) -> Self {
        Self {
            config,
            tracker,
            pages,
            _value_type: std::marker::PhantomData,
        }
    }

    pub fn max_point_offset(&self) -> PointOffset {
        self.tracker.pointer_count()
    }

    fn get_pointer(&self, point_offset: PointOffset) -> Option<ValuePointer> {
        self.tracker.get(point_offset)
    }

    /// Return the storage size in bytes (approximate: total page capacity).
    pub fn get_storage_size_bytes(&self) -> usize {
        self.pages.len() * self.config.page_size_bytes
    }

    /// Read raw value from the pages, considering that values can span more than one page.
    pub fn read_from_pages<const READ_SEQUENTIAL: bool>(
        &self,
        start_page_id: PageId,
        mut block_offset: BlockOffset,
        mut length: u32,
    ) -> Result<Vec<u8>> {
        let mut raw_sections = Vec::with_capacity(length as usize);

        for page_id in start_page_id.. {
            let page = &self.pages.get(page_id as usize).ok_or_else(|| {
                GridstoreError::service_error(format!(
                    "Expected existing page {page_id}, but wasn't found"
                ))
            })?;

            let (raw, unread_bytes) = page.read_value::<READ_SEQUENTIAL>(
                block_offset,
                length,
                self.config.block_size_bytes,
            )?;

            raw_sections.extend(raw.deref());

            if unread_bytes == 0 {
                break;
            }

            block_offset = 0;
            length = unread_bytes as u32;
        }

        Ok(raw_sections)
    }
}

impl<'a, V: Blob, S: UniversalRead<u8>> GridstoreView<'a, V, S> {
    pub(super) fn compress(&self, value: Vec<u8>) -> Vec<u8> {
        match self.config.compression {
            Compression::None => value,
            Compression::LZ4 => compress_lz4(&value),
        }
    }

    pub(super) fn decompress(&self, value: Vec<u8>) -> Vec<u8> {
        match self.config.compression {
            Compression::None => value,
            Compression::LZ4 => decompress_lz4(&value),
        }
    }

    /// Get the value for a given point offset.
    pub fn get_value<const READ_SEQUENTIAL: bool>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        let Some(pointer) = self.get_pointer(point_offset) else {
            return Ok(None);
        };

        let ValuePointer {
            page_id,
            block_offset,
            length,
        } = pointer;

        let raw = self.read_from_pages::<READ_SEQUENTIAL>(page_id, block_offset, length)?;
        hw_counter.payload_io_read_counter().incr_delta(raw.len());

        let decompressed = self.decompress(raw);
        let value = V::from_bytes(&decompressed);

        Ok(Some(value))
    }

    /// Iterate over all the values in the storage.
    ///
    /// Stops when the callback returns `Ok(false)`.
    pub fn iter<F, E>(
        &self,
        mut callback: F,
        hw_counter: HwMetricRefCounter,
    ) -> std::result::Result<(), E>
    where
        F: FnMut(PointOffset, V) -> std::result::Result<bool, E>,
        E: From<GridstoreError>,
    {
        const BUFFER_SIZE: usize = 128;
        let max_point_offset = self.max_point_offset();

        let mut from = 0;
        let mut buffer = Vec::with_capacity(min(BUFFER_SIZE, max_point_offset as usize));

        loop {
            buffer.clear();
            buffer.extend(
                self.tracker
                    .iter_pointers(from)
                    .filter_map(|(point_offset, opt_pointer)| {
                        opt_pointer.map(|pointer| (point_offset, pointer))
                    })
                    .take(BUFFER_SIZE),
            );

            if buffer.is_empty() {
                break;
            }

            from = buffer.last().unwrap().0 + 1;

            for &(point_offset, pointer) in &buffer {
                let ValuePointer {
                    page_id,
                    block_offset,
                    length,
                } = pointer;

                let raw = self.read_from_pages::<true>(page_id, block_offset, length)?;

                hw_counter.incr_delta(raw.len());

                let decompressed = self.decompress(raw);
                let value = V::from_bytes(&decompressed);
                if !callback(point_offset, value)? {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}
