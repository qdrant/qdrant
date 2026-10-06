//! [`ReadOnlyTracker`]: header-less, read-only counterpart of
//! [`Tracker`](super::Tracker) for follower reloads.

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs, UserData};

use super::iter::Iter;
use super::{
    PointOffset, PointerItem, PointerUpdates, Tracker, TrackerRead, ValuePointer, read_slot,
    read_slots,
};
use crate::Result;

/// Read-only counterpart of [`Tracker`].
///
/// Holds nothing but the storage handle. Reads don't need header *state* —
/// slot addressing is positional, and unwritten slots read as `None` (the
/// file is zero-initialized) — nor a pending-updates buffer, so neither is
/// kept. The stored header is only ever read as plain data, to answer
/// [`TrackerRead::max_point_offset`]; it never gates a reload.
///
/// The tracker file is preallocated and mutated in place (header rewrites,
/// slot updates), which a held handle's `reopen()` — an append-only-growth
/// contract — never picks up on caching backends. [`Self::live_reload`]
/// therefore refreshes by opening a *fresh* handle (a fresh open always
/// mirrors the current remote bytes) and swapping it in.
#[derive(Debug)]
pub struct ReadOnlyTracker<S> {
    /// Path to the tracker file.
    path: PathBuf,
    /// Storage handle; replaced wholesale on [`Self::live_reload`].
    storage: S,
    /// How to populate the storage on (re)open.
    populate: Populate,
}

/// The pending updates of a read-only tracker: always empty, readers have no
/// write buffer. Shared so [`Iter`] can borrow it for its lifetime.
fn no_pending_updates() -> &'static AHashMap<PointOffset, PointerUpdates> {
    static EMPTY: OnceLock<AHashMap<PointOffset, PointerUpdates>> = OnceLock::new();
    EMPTY.get_or_init(AHashMap::new)
}

impl<S: UniversalRead> ReadOnlyTracker<S> {
    /// Open an existing tracker file in the given dir, read-only.
    /// If the file does not exist, return an error.
    pub fn open<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        base_path: &Path,
        populate: Populate,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Self> {
        let path = Tracker::<S>::tracker_file_name(base_path);
        let storage = Tracker::open_storage(fs, &path, populate, false, hw_counter)?;
        Ok(Self {
            path,
            storage,
            populate,
        })
    }

    pub(crate) fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
        hw_counter: &HardwareCounterCell,
    ) {
        Tracker::preopen(fs, &self.path, self.populate, hw_counter);
    }

    /// Refresh to the current on-disk state by opening a fresh storage handle
    /// and swapping it in. See the type docs for why `reopen()` is not enough.
    pub fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        hw_counter: &HardwareCounterCell,
    ) -> Result<()> {
        self.storage = Tracker::open_storage(fs, &self.path, self.populate, false, hw_counter)?;
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }
}

impl<S: UniversalRead> TrackerRead for ReadOnlyTracker<S> {
    /// The writer-maintained count from the stored header, read as plain data
    /// through the current handle — i.e. as of the last
    /// [`Self::live_reload`]. Not kept as in-memory state.
    fn max_point_offset(&self, hw_counter: &HardwareCounterCell) -> Result<PointOffset> {
        Ok(Tracker::read_header(&self.storage, hw_counter)?.next_pointer_offset)
    }

    fn get<P: AccessPattern>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<ValuePointer>> {
        read_slot::<P, _>(&self.storage, point_offset, hw_counter)
    }

    fn get_range<P: AccessPattern>(
        &self,
        point_offsets: Range<PointOffset>,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Vec<Option<ValuePointer>>> {
        read_slots::<P, _>(&self.storage, point_offsets, hw_counter)
    }

    fn iter<'a, U, I>(
        &'a self,
        point_offsets: I,
        hw_counter: &'a HardwareCounterCell,
    ) -> Result<impl Iterator<Item = Result<(U, PointerItem)>>>
    where
        U: UserData,
        I: Iterator<Item = (U, PointOffset)>,
    {
        Iter::new(
            point_offsets,
            &self.storage,
            no_pending_updates(),
            hw_counter,
        )
    }
}
