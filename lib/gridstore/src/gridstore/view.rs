use std::ops::ControlFlow;

use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::generic_consts::{AccessPattern, Sequential};
use common::universal_io::UniversalRead;
use lz4_flex::compress_prepend_size;

use crate::Result;
use crate::blob::Blob;
use crate::config::{Compression, StorageConfig};
use crate::error::GridstoreError;
use crate::pages::Pages;
use crate::tracker::{PointOffset, Tracker, ValuePointer};

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
/// Generic over the storage backend `S` for both pages and tracker (same as [`Pages<S>`] and
/// [`Tracker<S>`]).
///
/// Constructed from either [`super::Gridstore`] or [`super::GridstoreReader`].
pub struct GridstoreView<'a, V, S: UniversalRead<u8>> {
    pub(super) config: &'a StorageConfig,
    pub(super) tracker: &'a Tracker<S>,
    pub(super) pages: &'a Pages<S>,
    pub(super) _value_type: std::marker::PhantomData<V>,
}

impl<'a, V, S: UniversalRead<u8>> GridstoreView<'a, V, S> {
    pub(crate) fn new(
        config: &'a StorageConfig,
        tracker: &'a Tracker<S>,
        pages: &'a Pages<S>,
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

    fn get_pointer(&self, point_offset: PointOffset) -> Result<Option<ValuePointer>> {
        self.tracker.get(point_offset)
    }

    /// Return the storage size in bytes (approximate: total page capacity).
    pub fn get_storage_size_bytes(&self) -> usize {
        self.pages.num_pages() * self.config.page_size_bytes
    }

    /// Read raw value from the pages, considering that values can span more than one page.
    pub fn read_from_pages<P: AccessPattern>(&self, pointer: ValuePointer) -> Result<Vec<u8>> {
        self.pages.read_from_pages::<P>(pointer, self.config)
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
    pub fn get_value<P: AccessPattern>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        let Some(pointer) = self.get_pointer(point_offset)? else {
            return Ok(None);
        };

        let raw = self.read_from_pages::<P>(pointer)?;
        hw_counter.payload_io_read_counter().incr_delta(raw.len());

        let decompressed = self.decompress(raw);
        let value = V::from_bytes(&decompressed);

        Ok(Some(value))
    }

    /// Iterate over all the values in the storage.
    ///
    /// Stops when any of these conditions is true:
    /// - the callback returns `Ok(false)`,
    /// - it has iterated `max_iters` times
    /// - there are no more results.
    ///
    /// ## Control Flow
    /// Returns `Ok(Continue(next_offset))` if iteration can be continued starting from `next_offset`
    ///  (i.e. `max_iters` was reached, but there are more results).
    ///
    /// Returns `Ok(Break())` when any of:
    /// - `callback` returned false
    /// - there are no more results.
    ///
    /// Returns `Err(e)` if reading from the tracker fails (no silent skip).
    pub fn iter<F, E>(
        &self,
        from_id: PointOffset,
        max_id: PointOffset,
        max_iters: usize,
        mut callback: F,
        hw_counter: HwMetricRefCounter,
    ) -> std::result::Result<ControlFlow<(), PointOffset>, E>
    where
        F: FnMut(PointOffset, V) -> std::result::Result<bool, E>,
        E: From<GridstoreError>,
    {
        let mut nth = 0;
        for (point_offset, res) in self.tracker.iter_pointers(from_id, max_id) {
            let pointer = match res {
                Ok(Some(p)) => p,
                Ok(None) => continue,
                Err(e) => return Err(e.into()),
            };

            if nth == max_iters {
                return Ok(ControlFlow::Continue(point_offset));
            }
            nth += 1;

            let raw = self.read_from_pages::<Sequential>(pointer)?;

            hw_counter.incr_delta(raw.len());

            let decompressed = self.decompress(raw);
            let value = V::from_bytes(&decompressed);
            if !callback(point_offset, value)? {
                return Ok(ControlFlow::Break(()));
            }
        }
        Ok(ControlFlow::Break(()))
    }
}
