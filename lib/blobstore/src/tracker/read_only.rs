//! [`ReadOnlyTracker`]: header-less, read-only counterpart of
//! [`Tracker`](super::Tracker) for follower reloads.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use ahash::AHashMap;
use common::universal_io::{Populate, UniversalRead, UniversalReadFs, UserData};

use super::iter::Iter;
use super::{PointOffset, PointerUpdates, Tracker, TrackerRead, ValuePointer, read_slot};
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
    ) -> Result<Self> {
        let path = Tracker::<S>::tracker_file_name(base_path);
        let storage = Tracker::open_storage(fs, &path, populate, false)?;
        Ok(Self {
            path,
            storage,
            populate,
        })
    }

    /// Refresh to the current on-disk state by opening a fresh storage handle
    /// and swapping it in. See the type docs for why `reopen()` is not enough.
    pub fn live_reload<Fs: UniversalReadFs<File = S>>(&mut self, fs: &Fs) -> Result<()> {
        self.storage = Tracker::open_storage(fs, &self.path, self.populate, false)?;
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }
}

impl<S: UniversalRead> TrackerRead<S> for ReadOnlyTracker<S> {
    /// The writer-maintained count from the stored header, read as plain data
    /// through the current handle — i.e. as of the last
    /// [`Self::live_reload`]. Not kept as in-memory state.
    fn max_point_offset(&self) -> Result<PointOffset> {
        Ok(Tracker::read_header(&self.storage)?.next_pointer_offset)
    }

    fn get(&self, point_offset: PointOffset) -> Result<Option<ValuePointer>> {
        read_slot(&self.storage, point_offset)
    }

    fn iter<U, I>(&self, point_offsets: I) -> Result<Iter<'_, U, I, S>>
    where
        U: UserData,
        I: Iterator<Item = (U, PointOffset)>,
    {
        Iter::new(point_offsets, &self.storage, no_pending_updates())
    }
}
