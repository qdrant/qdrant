use std::ops::Range;
use std::path::{Path, PathBuf};

use common::generic_consts::AccessPattern;
use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs, UserData};
use itertools::Either;

use crate::Result;
use crate::tracker::append_only::AppendOnlyTracker;
use crate::tracker::compacted::CompactedTracker;
use crate::tracker::{PointOffset, PointerItem, TrackerRead, ValuePointer};

/// Read-only tracker of the append-only storage mode, in whichever format is on disk.
///
/// A storage holds exactly one of the two tracker files. The compacted one is written for
/// storages that are complete and never change afterwards.
#[derive(Debug)]
pub enum TrackerEnum<S> {
    AppendOnly(AppendOnlyTracker<S>),
    Compacted(CompactedTracker),
}

impl<S: UniversalRead> TrackerEnum<S> {
    /// Schedule a prefetch for the tracker file a subsequent [`open`](Self::open) reads.
    pub fn preopen<Fs: CachedReadFs<File = S>>(
        fs: &Fs,
        dir: &Path,
        populate: Populate,
    ) -> Result<()> {
        if CompactedTracker::exists(fs, dir)? {
            CompactedTracker::preopen(fs, dir);
        } else {
            AppendOnlyTracker::<S>::preopen(fs, dir, populate);
        }
        Ok(())
    }

    /// Open the tracker in the given directory read-only, in the format found on disk.
    ///
    /// If no tracker file exists, return an error.
    pub fn open<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        dir: &Path,
        populate: Populate,
    ) -> Result<Self> {
        if CompactedTracker::exists(fs, dir)? {
            Ok(Self::Compacted(CompactedTracker::open(fs, dir)?))
        } else {
            let tracker = AppendOnlyTracker::open_read_only(fs, dir, populate)?;
            Ok(Self::AppendOnly(tracker))
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        match self {
            Self::AppendOnly(tracker) => tracker.files(),
            Self::Compacted(tracker) => tracker.files(),
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

impl<S: UniversalRead> TrackerRead for TrackerEnum<S> {
    fn max_point_offset(&self) -> Result<PointOffset> {
        match self {
            Self::AppendOnly(tracker) => tracker.max_point_offset(),
            Self::Compacted(tracker) => tracker.max_point_offset(),
        }
    }

    fn get<P: AccessPattern>(&self, point_offset: PointOffset) -> Result<Option<ValuePointer>> {
        match self {
            Self::AppendOnly(tracker) => tracker.get::<P>(point_offset),
            Self::Compacted(tracker) => tracker.get::<P>(point_offset),
        }
    }

    fn get_range<P: AccessPattern>(
        &self,
        point_offsets: Range<PointOffset>,
    ) -> Result<Vec<Option<ValuePointer>>> {
        match self {
            Self::AppendOnly(tracker) => tracker.get_range::<P>(point_offsets),
            Self::Compacted(tracker) => tracker.get_range::<P>(point_offsets),
        }
    }

    fn iter<U, I>(&self, point_offsets: I) -> Result<impl Iterator<Item = Result<(U, PointerItem)>>>
    where
        U: UserData,
        I: Iterator<Item = (U, PointOffset)>,
    {
        Ok(match self {
            Self::AppendOnly(tracker) => Either::Left(tracker.iter(point_offsets)?),
            Self::Compacted(tracker) => Either::Right(tracker.iter(point_offsets)?),
        })
    }
}
