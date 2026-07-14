use std::borrow::Cow;

use common::counter::counter_cell::CounterCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::universal_io::{UniversalRead, UserData};

use super::arenastore::ArenastoreView;
use super::gridstore::GridstoreView;
use crate::Result;
use crate::blob::Blob;
use crate::error::GridstoreError;
use crate::tracker::{PointOffset, ReadOnlyTracker, ValuePointer};

/// A non-owning view into gridstore data.
///
/// Holds borrowed references to the underlying storage, and contains all reading logic.
///
/// Constructed from either [`super::Blobstore`] or [`super::BlobstoreReader`].
pub struct BlobstoreView<'a, V, S: UniversalRead> {
    variant: ViewVariant<'a, V, S>,
}

/// Mode specific implementation of the view, see [`crate::config::Mode`].
enum ViewVariant<'a, V, S: UniversalRead> {
    Gridstore(GridstoreView<'a, V, S, ReadOnlyTracker<S>>),
    Arenastore(ArenastoreView<'a, V, S>),
}

impl<'a, V, S: UniversalRead> BlobstoreView<'a, V, S> {
    pub(super) fn from_gridstore(view: GridstoreView<'a, V, S, ReadOnlyTracker<S>>) -> Self {
        Self {
            variant: ViewVariant::Gridstore(view),
        }
    }

    pub(super) fn from_arenastore(view: ArenastoreView<'a, V, S>) -> Self {
        Self {
            variant: ViewVariant::Arenastore(view),
        }
    }

    /// Exclusive upper bound of point offsets that may have a value.
    ///
    /// In mutable mode this is the stored header count as of the last reload —
    /// see [`TrackerRead::max_point_offset`](crate::tracker::TrackerRead::max_point_offset).
    pub fn max_point_offset(&self) -> Result<PointOffset> {
        match &self.variant {
            ViewVariant::Gridstore(view) => view.max_point_offset(),
            ViewVariant::Arenastore(view) => Ok(view.max_point_offset()),
        }
    }

    /// Return the storage size in bytes.
    ///
    /// Approximate (total page capacity) in mutable mode, exact in append-only mode.
    pub fn get_storage_size_bytes(&self) -> usize {
        match &self.variant {
            ViewVariant::Gridstore(view) => view.get_storage_size_bytes(),
            ViewVariant::Arenastore(view) => view.get_storage_size_bytes(),
        }
    }

    /// Read the raw value bytes at the given pointer.
    pub fn read_from_pages<P: AccessPattern>(
        &self,
        pointer: ValuePointer,
    ) -> Result<Cow<'_, [u8]>> {
        match &self.variant {
            ViewVariant::Gridstore(view) => view.read_from_pages::<P>(pointer),
            ViewVariant::Arenastore(view) => view.read_from_pages::<P>(pointer),
        }
    }
}

impl<'a, V: Blob, S: UniversalRead> BlobstoreView<'a, V, S> {
    /// Get the value for a given point offset.
    pub fn get_value<P: AccessPattern>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<V>> {
        match &self.variant {
            ViewVariant::Gridstore(view) => view.get_value::<P>(point_offset, hw_counter),
            ViewVariant::Arenastore(view) => view.get_value::<P>(point_offset, hw_counter),
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
            ViewVariant::Gridstore(view) => {
                view.read_values::<P, U, E>(point_offsets, callback, hw_counter_cell)
            }
            ViewVariant::Arenastore(view) => {
                view.read_values::<P, U, E>(point_offsets, callback, hw_counter_cell)
            }
        }
    }
}
