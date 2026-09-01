use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs};
use futures::future::BoxFuture;

use crate::common::operation_error::OperationResult;

/// Common live-reload surface shared by the read-only, gridstore-backed stores —
/// the read-only field-index variants and the read-only payload storage.
///
/// A read-only store is opened over a [`UniversalRead`] backend while a writer
/// keeps appending to the same files. `live_reload` refreshes the in-memory
/// view to the current on-disk state without a full re-open. Implementers fall
/// into a few shapes:
///
/// - immutable mmap field-index variants only re-apply the authoritative
///   `deleted_points` to their in-memory deletion bitmap — `fs` and
///   `new_points` are unused because no on-disk state changes after build;
/// - appendable gridstore field-index variants reload the backing storage
///   through `fs`, drop `deleted_points` from the in-memory index, then ingest
///   `new_points` from the refreshed storage view;
/// - stores with no separate in-memory index (e.g. the payload storage) only
///   reload the backing storage through `fs`; deletions and newly written
///   points are served straight from the refreshed gridstore, so
///   `deleted_points` / `new_points` are unused.
///
/// `deleted_points` / `new_points` are supplied by the caller (typically the
/// segment's id-tracker diff accumulated since the previous reload).
///
/// [`UniversalRead`]: common::universal_io::UniversalRead
pub(crate) trait LiveReload {
    type File: UniversalRead;

    /// Stage everything the next [`Self::live_reload`] needs: schedule
    /// reopens on kept handles, (re)schedule prefetches for swapped and new
    /// files. Shared access; must not wait on any fetch. Returns the futures
    /// driving the staged fetches — the caller polls them, concurrently
    /// across components.
    fn live_preload<Fs: CachedReadFs<File = Self::File>>(
        &self,
        cached_fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>>;

    fn live_reload<Fs: UniversalReadFs<File = Self::File>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;
}
