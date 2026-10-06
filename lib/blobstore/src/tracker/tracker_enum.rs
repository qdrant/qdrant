use std::ops::Range;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::universal_io::{
    CachedReadFs, Populate, UniversalAppend, UniversalRead, UniversalReadFs, UniversalWriteFs,
    UserData,
};
use itertools::Either;

use crate::Result;
use crate::blobstore::Flusher;
use crate::tracker::append_only::AppendOnlyTracker;
use crate::tracker::compacted::CompactedTracker;
use crate::tracker::{PointOffset, PointerItem, TrackerRead, ValuePointer};

/// Tracker of the append-only storage mode, in whichever format is on disk.
///
/// A storage holds exactly one of the two tracker files. The compacted one is written for
/// storages that are complete, and rewrites its whole file on every flush that has changes.
#[derive(Debug)]
pub enum TrackerEnum<S> {
    AppendOnly(AppendOnlyTracker<S>),
    Compacted(CompactedTracker),
}

impl<S: UniversalRead> TrackerEnum<S> {
    /// Schedule a prefetch for the tracker file a subsequent [`open_read_only`](Self::open_read_only)
    /// reads.
    pub fn preopen<Fs: CachedReadFs<File = S>>(
        fs: &Fs,
        dir: &Path,
        populate: Populate,
        hw_counter: &HardwareCounterCell,
    ) -> Result<()> {
        if CompactedTracker::exists(fs, dir, hw_counter)? {
            CompactedTracker::preopen(fs, dir, hw_counter);
        } else {
            AppendOnlyTracker::<S>::preopen(fs, dir, populate, hw_counter);
        }
        Ok(())
    }

    /// Open the tracker in the given directory read-only, in the format found on disk.
    ///
    /// If no tracker file exists, return an error.
    pub fn open_read_only<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        dir: &Path,
        populate: Populate,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Self> {
        if CompactedTracker::exists(fs, dir, hw_counter)? {
            Ok(Self::Compacted(CompactedTracker::open(
                fs, dir, hw_counter,
            )?))
        } else {
            let tracker = AppendOnlyTracker::open_read_only(fs, dir, populate, hw_counter)?;
            Ok(Self::AppendOnly(tracker))
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        match self {
            Self::AppendOnly(tracker) => tracker.files(),
            Self::Compacted(tracker) => tracker.files(),
        }
    }

    /// Number of mappings, including pending ones: one past the highest point offset that was
    /// ever set.
    pub fn pointer_count(&self) -> PointOffset {
        match self {
            Self::AppendOnly(tracker) => tracker.pointer_count(),
            Self::Compacted(tracker) => tracker.pointer_count(),
        }
    }

    /// Heap RAM held beyond the tracker file: all mappings of the compacted tracker. The
    /// append-only tracker reads its mappings from the file, and only buffers them between puts
    /// and the next flush.
    pub fn ram_usage_bytes(&self) -> usize {
        match self {
            Self::AppendOnly(_) => 0,
            Self::Compacted(tracker) => tracker.ram_usage_bytes(),
        }
    }

    /// Populate the tracker file into the RAM cache. The compacted tracker lives in RAM already.
    pub fn populate(&self, hw_counter: &HardwareCounterCell) -> Result<()> {
        match self {
            Self::AppendOnly(tracker) => tracker.populate(hw_counter),
            Self::Compacted(_) => Ok(()),
        }
    }

    /// Ask to evict the tracker file from the RAM cache. The compacted tracker lives in RAM and
    /// has nothing to evict.
    pub fn clear_cache(&self) -> Result<()> {
        match self {
            Self::AppendOnly(tracker) => tracker.clear_cache(),
            Self::Compacted(_) => Ok(()),
        }
    }
}

impl<S: UniversalAppend> TrackerEnum<S> {
    /// Create a new empty tracker in the given directory, always in the append-only format.
    ///
    /// The directory must exist already.
    pub fn new<Fs>(fs: &Fs, dir: &Path, hw_counter: &HardwareCounterCell) -> Result<Self>
    where
        Fs: UniversalWriteFs<AppendFile = S> + UniversalReadFs<File = S>,
    {
        Ok(Self::AppendOnly(AppendOnlyTracker::new(
            fs, dir, hw_counter,
        )?))
    }

    /// Open the tracker in the given directory for writing, in the format found on disk.
    ///
    /// If no tracker file exists, return an error.
    pub fn open_writable<Fs>(
        fs: &Fs,
        dir: &Path,
        populate: Populate,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Self>
    where
        Fs: UniversalWriteFs<AppendFile = S> + UniversalReadFs<File = S>,
    {
        if CompactedTracker::exists(fs, dir, hw_counter)? {
            let tracker = CompactedTracker::open_writable(fs, dir, hw_counter)?;
            Ok(Self::Compacted(tracker))
        } else {
            let tracker = AppendOnlyTracker::open_writable(fs, dir, populate, hw_counter)?;
            Ok(Self::AppendOnly(tracker))
        }
    }

    /// Set the mapping for the given point offset, see [`AppendOnlyTracker::set`].
    pub fn set(&mut self, point_offset: PointOffset, pointer: ValuePointer) -> Result<()> {
        match self {
            Self::AppendOnly(tracker) => tracker.set(point_offset, pointer),
            Self::Compacted(tracker) => {
                tracker.set(point_offset, pointer);
                Ok(())
            }
        }
    }

    /// Write pending mappings below `target`, see [`AppendOnlyTracker::write_pending`]. A
    /// compacted tracker writes its whole file in the flusher instead.
    pub fn write_pending(
        &mut self,
        target: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<()> {
        match self {
            Self::AppendOnly(tracker) => tracker.write_pending(target, hw_counter),
            Self::Compacted(_) => Ok(()),
        }
    }

    /// Create a closure that persists the mappings below `target`, a point offset count.
    ///
    /// The append-only tracker syncs what [`Self::write_pending`] wrote up to `target`, the
    /// compacted tracker rewrites its file with the mappings below `target`.
    pub fn flusher(&self, target: PointOffset, hw_counter: &HardwareCounterCell) -> Flusher {
        match self {
            Self::AppendOnly(tracker) => tracker.flusher(),
            Self::Compacted(tracker) => tracker.flusher(target, hw_counter),
        }
    }
}

impl<S: UniversalRead> TrackerRead for TrackerEnum<S> {
    fn max_point_offset(&self, hw_counter: &HardwareCounterCell) -> Result<PointOffset> {
        match self {
            Self::AppendOnly(tracker) => tracker.max_point_offset(hw_counter),
            Self::Compacted(tracker) => tracker.max_point_offset(hw_counter),
        }
    }

    fn get<P: AccessPattern>(
        &self,
        point_offset: PointOffset,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Option<ValuePointer>> {
        match self {
            Self::AppendOnly(tracker) => tracker.get::<P>(point_offset, hw_counter),
            Self::Compacted(tracker) => tracker.get::<P>(point_offset, hw_counter),
        }
    }

    fn get_range<P: AccessPattern>(
        &self,
        point_offsets: Range<PointOffset>,
        hw_counter: &HardwareCounterCell,
    ) -> Result<Vec<Option<ValuePointer>>> {
        match self {
            Self::AppendOnly(tracker) => tracker.get_range::<P>(point_offsets, hw_counter),
            Self::Compacted(tracker) => tracker.get_range::<P>(point_offsets, hw_counter),
        }
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
        Ok(match self {
            Self::AppendOnly(tracker) => Either::Left(tracker.iter(point_offsets, hw_counter)?),
            Self::Compacted(tracker) => Either::Right(tracker.iter(point_offsets, hw_counter)?),
        })
    }
}
