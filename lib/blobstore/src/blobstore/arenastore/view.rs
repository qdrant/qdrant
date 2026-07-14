use std::borrow::Cow;
use std::marker::PhantomData;

use common::counter::counter_cell::CounterCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::referenced_counter::HwMetricRefCounter;
use common::generic_consts::{AccessPattern, Sequential};
use common::universal_io::{UniversalRead, UserData};

use super::page::AppendOnlyPages;
use crate::Result;
use crate::blob::Blob;
use crate::config::StorageConfig;
use crate::error::GridstoreError;
use crate::tracker::append_only::AppendOnlyTracker;
use crate::tracker::{PointOffset, ValuePointer};

/// A non-owning view into arenastore data.
///
/// Holds borrowed references to the tracker and pages, and contains all reading logic.
///
/// Value data is read through the universal IO backend `S`, the tracker is read directly.
pub(crate) struct ArenastoreView<'a, V, S: UniversalRead> {
    config: &'a StorageConfig,
    tracker: &'a AppendOnlyTracker,
    pages: &'a AppendOnlyPages<S>,
    _phantom: PhantomData<V>,
}

impl<'a, V, S: UniversalRead> ArenastoreView<'a, V, S> {
    pub(super) fn new(
        config: &'a StorageConfig,
        tracker: &'a AppendOnlyTracker,
        pages: &'a AppendOnlyPages<S>,
    ) -> Self {
        Self {
            config,
            tracker,
            pages,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn max_point_offset(&self) -> PointOffset {
        self.tracker.pointer_count()
    }

    /// Return the storage size in bytes (precise, the exact amount of appended value data).
    pub(crate) fn get_storage_size_bytes(&self) -> usize {
        self.pages.len() as usize
    }

    /// Read the raw value bytes at the given pointer.
    pub(crate) fn read_from_pages<P: AccessPattern>(
        &self,
        pointer: ValuePointer,
    ) -> Result<Cow<'_, [u8]>> {
        self.pages.read_value::<P>(pointer)
    }
}

impl<'a, V: Blob, S: UniversalRead> ArenastoreView<'a, V, S> {
    /// Get the value for a given point offset.
    pub(crate) fn get_value<P: AccessPattern>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        let Some(pointer) = self.tracker.get(point_offset)? else {
            return Ok(None);
        };

        let raw = self.read_from_pages::<P>(pointer)?;
        hw_counter.payload_io_read_counter().incr_delta(raw.len());

        let decompressed = self.config.compression.decompress(raw);
        Ok(Some(V::from_bytes(&decompressed)))
    }

    /// Iterate over all given values and execute callback for each one.
    ///
    /// Return `false` from the callback to stop iteration early.
    pub(crate) fn read_values<P, U, E>(
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
        for (user_data, point_offset) in point_offsets {
            let value = match self.tracker.get(point_offset).map_err(E::from)? {
                None => None,
                Some(pointer) => {
                    let raw = self.read_from_pages::<P>(pointer).map_err(E::from)?;
                    hw_counter_cell.incr_delta(raw.len());

                    let decompressed = self.config.compression.decompress(raw);
                    Some(V::from_bytes(&decompressed))
                }
            };

            if !callback(user_data, point_offset, value)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Iterate over a contiguous range of point offsets and execute callback for each existing
    /// value. Missing values are skipped.
    ///
    /// The mappings for the whole range are fetched with a single batched read.
    ///
    /// Return `false` from the callback to stop iteration early. Returns whether iteration should
    /// continue.
    pub(crate) fn iter_range<F, E>(
        &self,
        point_offsets: std::ops::Range<PointOffset>,
        mut callback: F,
        hw_counter: HwMetricRefCounter,
    ) -> Result<bool, E>
    where
        F: FnMut(PointOffset, V) -> Result<bool, E>,
        E: From<GridstoreError>,
    {
        let start = point_offsets.start;
        let pointers = self.tracker.get_range(point_offsets).map_err(E::from)?;

        for (index, pointer) in pointers.into_iter().enumerate() {
            let Some(pointer) = pointer else {
                continue;
            };

            let raw = self
                .read_from_pages::<Sequential>(pointer)
                .map_err(E::from)?;
            hw_counter.incr_delta(raw.len());

            let decompressed = self.config.compression.decompress(raw);
            let value = V::from_bytes(&decompressed);

            if !callback(start + index as PointOffset, value)? {
                return Ok(false);
            }
        }

        Ok(true)
    }
}
