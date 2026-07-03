use std::borrow::Cow;

use common::counter::counter_cell::CounterCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::universal_io::{UniversalRead, UserData};
use lz4_flex::compress_prepend_size;

use super::serverless::ServerlessGridstoreView;
use crate::Result;
use crate::blob::Blob;
use crate::config::{Compression, StorageConfig};
use crate::error::GridstoreError;
use crate::pages::Pages;
use crate::tracker::{PointOffset, PointerItem, ReadOnlyTracker, TrackerRead, ValuePointer};

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
/// Holds borrowed references to the underlying storage, and contains all reading logic.
///
/// Constructed from either [`super::Gridstore`] or [`super::GridstoreReader`].
pub struct GridstoreView<'a, V, S: UniversalRead> {
    variant: ViewVariant<'a, V, S>,
}

/// Mode specific implementation of the view, see [`crate::config::Mode`].
enum ViewVariant<'a, V, S: UniversalRead> {
    Dynamic(DynamicGridstoreView<'a, V, S, ReadOnlyTracker<S>>),
    Serverless(ServerlessGridstoreView<'a, V, S>),
}

impl<'a, V, S: UniversalRead> GridstoreView<'a, V, S> {
    pub(super) fn from_dynamic(view: DynamicGridstoreView<'a, V, S, ReadOnlyTracker<S>>) -> Self {
        Self {
            variant: ViewVariant::Dynamic(view),
        }
    }

    pub(super) fn from_serverless(view: ServerlessGridstoreView<'a, V, S>) -> Self {
        Self {
            variant: ViewVariant::Serverless(view),
        }
    }

    /// Exclusive upper bound of point offsets that may have a value.
    ///
    /// In dynamic mode this is the stored header count as of the last reload —
    /// see [`TrackerRead::max_point_offset`].
    pub fn max_point_offset(&self) -> Result<PointOffset> {
        match &self.variant {
            ViewVariant::Dynamic(view) => view.max_point_offset(),
            ViewVariant::Serverless(view) => Ok(view.max_point_offset()),
        }
    }

    /// Return the storage size in bytes.
    ///
    /// Approximate (total page capacity) in dynamic mode, exact in serverless mode.
    pub fn get_storage_size_bytes(&self) -> usize {
        match &self.variant {
            ViewVariant::Dynamic(view) => view.get_storage_size_bytes(),
            ViewVariant::Serverless(view) => view.get_storage_size_bytes(),
        }
    }

    /// Read the raw value bytes at the given pointer.
    pub fn read_from_pages<P: AccessPattern>(
        &self,
        pointer: ValuePointer,
    ) -> Result<Cow<'_, [u8]>> {
        match &self.variant {
            ViewVariant::Dynamic(view) => view.read_from_pages::<P>(pointer),
            ViewVariant::Serverless(view) => Ok(Cow::Owned(view.read_from_page(pointer)?)),
        }
    }
}

impl<'a, V: Blob, S: UniversalRead> GridstoreView<'a, V, S> {
    /// Get the value for a given point offset.
    pub fn get_value<P: AccessPattern>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        match &self.variant {
            ViewVariant::Dynamic(view) => view.get_value::<P>(point_offset, hw_counter),
            ViewVariant::Serverless(view) => view.get_value::<P>(point_offset, hw_counter),
        }
    }

    /// Iterate over all given values and execute callback for each one.
    ///
    /// Return `false` from the callback to stop iteration early.
    pub fn read_values<P, U, E>(
        &self,
        point_offsets: impl Iterator<Item = (U, PointOffset)>,
        callback: impl FnMut(U, PointOffset, Option<V>) -> Result<bool, E>,
        hw_counter_cell: &CounterCell,
    ) -> Result<bool, E>
    where
        P: AccessPattern,
        U: UserData,
        E: From<GridstoreError>,
    {
        match &self.variant {
            ViewVariant::Dynamic(view) => {
                view.read_values::<P, U, E>(point_offsets, callback, hw_counter_cell)
            }
            ViewVariant::Serverless(view) => {
                view.read_values::<P, U, E>(point_offsets, callback, hw_counter_cell)
            }
        }
    }
}

/// A non-owning view into gridstore data in dynamic mode.
///
/// Holds borrowed references to pages and tracker, and contains all reading logic.
/// Generic over the storage backend `S` (same as [`Pages<S>`]) and over the
/// tracker type `T` — the writable [`Tracker`](crate::tracker::Tracker) for
/// [`super::Gridstore`], the [`ReadOnlyTracker`](crate::tracker::ReadOnlyTracker)
/// for [`super::GridstoreReader`].
pub(super) struct DynamicGridstoreView<'a, V, S: UniversalRead, T: TrackerRead<S>> {
    pub(super) config: &'a StorageConfig,
    pub(super) tracker: &'a T,
    pub(super) pages: &'a Pages<S>,
    pub(super) _value_type: std::marker::PhantomData<V>,
}

impl<'a, V, S: UniversalRead, T: TrackerRead<S>> DynamicGridstoreView<'a, V, S, T> {
    pub(crate) fn new(config: &'a StorageConfig, tracker: &'a T, pages: &'a Pages<S>) -> Self {
        Self {
            config,
            tracker,
            pages,
            _value_type: std::marker::PhantomData,
        }
    }

    /// Exclusive upper bound of point offsets that may have a value.
    ///
    /// Exact for the writable tracker; the stored header count as of the last
    /// reload for the read-only one — see [`TrackerRead::max_point_offset`].
    pub fn max_point_offset(&self) -> Result<PointOffset> {
        self.tracker.max_point_offset()
    }

    fn get_pointer(&self, point_offset: PointOffset) -> Result<Option<ValuePointer>> {
        self.tracker.get(point_offset)
    }

    /// Return the storage size in bytes (approximate: total page capacity).
    pub fn get_storage_size_bytes(&self) -> usize {
        self.pages.num_pages() * self.config.page_size_bytes
    }

    /// Read raw value from the pages, considering that values can span more than one page.
    pub fn read_from_pages<P: AccessPattern>(
        &self,
        pointer: ValuePointer,
    ) -> Result<Cow<'_, [u8]>> {
        self.pages.read_from_pages::<P>(pointer, self.config)
    }
}

impl<'a, V: Blob, S: UniversalRead, T: TrackerRead<S>> DynamicGridstoreView<'a, V, S, T> {
    pub(super) fn compress(&self, value: Vec<u8>) -> Vec<u8> {
        match self.config.compression {
            Compression::None => value,
            Compression::LZ4 => compress_lz4(&value),
        }
    }

    pub(super) fn decompress<'val>(&self, value: Cow<'val, [u8]>) -> Cow<'val, [u8]> {
        match self.config.compression {
            Compression::None => value,
            Compression::LZ4 => decompress_lz4(&value).into(),
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

    pub fn read_values<P, U, E>(
        &self,
        point_offsets: impl Iterator<Item = (U, PointOffset)>,
        mut callback: impl FnMut(U, PointOffset, Option<V>) -> Result<bool, E>,
        hw_counter_cell: &CounterCell,
    ) -> Result<bool, E>
    where
        P: AccessPattern,
        U: UserData,
        E: From<GridstoreError>,
    {
        let point_offsets = point_offsets
            .map(|(user_data, point_offset)| ((user_data, point_offset), point_offset));

        let mut pointers = Vec::new();

        for result in self.tracker.iter(point_offsets)? {
            let ((user_data, point_offset), pointer) = result?;

            let PointerItem::Valid(pointer) = pointer else {
                if !callback(user_data, point_offset, None)? {
                    return Ok(false);
                }

                continue;
            };

            pointers.push(((user_data, point_offset), pointer));
        }

        self.pages.read_batch_from_pages::<P, _, _>(
            self.config,
            pointers.into_iter(),
            |(user_data, point_offset), bytes| {
                hw_counter_cell.incr_delta(bytes.len());

                let decompressed = self.decompress(bytes);
                let value = V::from_bytes(&decompressed);
                callback(user_data, point_offset, Some(value))
            },
        )
    }
}
