//! Flags of a read-only bool index, without the points the segment deleted.

use std::path::PathBuf;
use std::sync::OnceLock;

use common::bitvec::BitSlice;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, PreloadFuture, UniversalRead, UniversalReadFs};
use roaring::RoaringBitmap;

use crate::common::flags::read_only_flags::ReadOnlyFlags;
use crate::common::flags::roaring_flags::RoaringFlagsRead;
use crate::common::live_reload::LiveReload as _;
use crate::common::operation_error::OperationResult;

/// Stored flags with the deleted points of the segment cleared.
///
/// The stored flags can keep the values of deleted points: an immutable index drops
/// them in RAM only, when it opens and on deletion. These flags drop them the same way,
/// so every read of the index sees only the points of the segment that exist.
pub struct LiveFlags<S: UniversalRead> {
    stored: ReadOnlyFlags<S>,
    /// Points of the segment that are deleted
    deleted: RoaringBitmap,
    /// The stored bitmap without `deleted`, derived on first use when they intersect
    live: OnceLock<RoaringBitmap>,
}

impl<S: UniversalRead> LiveFlags<S> {
    pub(super) fn new(stored: ReadOnlyFlags<S>, deleted: &BitSlice) -> Self {
        Self {
            stored,
            deleted: deleted
                .iter_ones()
                .map(|point| point as PointOffsetType)
                .collect(),
            live: OnceLock::new(),
        }
    }

    pub(super) fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<PreloadFuture>> {
        self.stored.live_preload(fs)
    }

    /// Resync the stored flags, and drop the points deleted since the last reload.
    pub(super) fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        self.stored.live_reload(fs)?;
        self.deleted.extend(deleted_points.iter().copied());
        for &point in new_points.iter() {
            self.deleted.remove(point);
        }
        self.live = OnceLock::new();
        Ok(())
    }
}

impl<S: UniversalRead> RoaringFlagsRead for LiveFlags<S> {
    fn len(&self) -> usize {
        self.stored.len()
    }

    fn get_bitmap(&self) -> OperationResult<&RoaringBitmap> {
        if let Some(live) = self.live.get() {
            return Ok(live);
        }
        let stored = self.stored.get_bitmap()?;
        if stored.is_disjoint(&self.deleted) {
            return Ok(stored);
        }
        Ok(self.live.get_or_init(|| stored - &self.deleted))
    }

    fn bitmap_if_materialized(&self) -> Option<&RoaringBitmap> {
        self.live
            .get()
            .or_else(|| self.stored.bitmap_if_materialized())
    }

    fn ram_usage_bytes(&self) -> usize {
        let live = self.live.get().map_or(0, RoaringBitmap::serialized_size);
        let deleted = if self.deleted.is_empty() {
            0
        } else {
            self.deleted.serialized_size()
        };
        self.stored.ram_usage_bytes() + live + deleted
    }

    fn populate(&self) -> OperationResult<()> {
        self.stored.populate()
    }

    fn clear_cache(&self) -> OperationResult<()> {
        self.stored.clear_cache()
    }

    fn files(&self) -> Vec<PathBuf> {
        self.stored.files()
    }
}
