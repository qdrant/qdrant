//! Delete-only shard: finds and deletes points over a set of segments, opening
//! only their id trackers.
//!
//! Built for removing outdated copies after a rebuild: while a point's newer
//! copy is deferred, its older copies stay visible, and once a rebuild
//! publishes the newer copy elsewhere, the older ones are duplicates.

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use ahash::AHashMap;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppendFs;
use rayon::ThreadPool;
use rayon::prelude::*;
use segment::common::operation_error::OperationResult;
use segment::segment::update_only::{TrackerLookup, UpdateOnlySegmentEnum};
use segment::types::{PointIdType, SeqNumberType};
use uuid::Uuid;

use crate::read_only::{ListedSegment, SegmentEnumerator};
use crate::read_view::build_segment_pool;
use crate::update_only::locate::{PointLocations, locate_in, merge_locations};

/// Finds and deletes points in the segments it was opened on. Opens id
/// trackers only.
pub struct DeleteOnlyEdgeShard<Fs: UniversalAppendFs> {
    fs: Fs,
    segments: HashMap<Uuid, TrackerLookup<Fs>>,
    pool: Arc<ThreadPool>,
}

impl<Fs: UniversalAppendFs> DeleteOnlyEdgeShard<Fs> {
    /// Open the id trackers of the listed segments. Writes nothing: opening an
    /// appendable writer would drop slots claimed by an in-flight batch, so
    /// writers are opened in [`delete_points`](Self::delete_points).
    pub fn open(fs: Fs, enumerator: impl SegmentEnumerator) -> OperationResult<Self> {
        let pool = build_segment_pool(
            "edge-delete",
            common::defaults::search_thread_count(0),
            None,
        )?;
        let listed: Vec<(Uuid, ListedSegment)> = enumerator.list_segments()?.into_iter().collect();
        let segments = pool.install(|| {
            listed
                .into_par_iter()
                .map(|(uuid, ListedSegment { path, writable: _ })| {
                    Ok((uuid, TrackerLookup::open(fs.clone(), &path)?))
                })
                .collect::<OperationResult<HashMap<_, _>>>()
        })?;
        Ok(Self { fs, segments, pool })
    }

    /// Reload the id trackers to pick up changes written since the last open
    /// or reload.
    pub fn live_reload(&mut self) -> OperationResult<()> {
        self.pool.install(|| {
            self.segments
                .par_iter_mut()
                .try_for_each(|(_, segment)| segment.live_reload())
        })
    }

    /// Points in `versions` whose copies in this shard are all older than the
    /// given version, based on the trackers' last read.
    ///
    /// For example, with segment A holding point 1 at version 3 and point 2 at
    /// version 5, and segment B holding point 1 at version 7:
    ///
    /// - `{1: 8}` gives `[1]`: both copies, at 3 and 7, are older than 8.
    /// - `{1: 6}` gives `[]`: B's copy at 7 is newer than 6.
    /// - `{2: 5}` gives `[]`: the copy at 5 is not older than 5.
    /// - `{3: 9}` gives `[]`: the shard holds no copy of point 3.
    pub fn find_outdated(
        &self,
        versions: &HashMap<PointIdType, SeqNumberType>,
    ) -> OperationResult<Vec<PointIdType>> {
        let ids: Vec<PointIdType> = versions.keys().copied().collect();
        Ok(self
            .locate(&ids)?
            .into_iter()
            .filter(|(id, located)| located.newest.version < versions[id])
            .map(|(id, _)| id)
            .collect())
    }

    /// Delete every copy of `ids` in this shard. Ids the shard does not hold
    /// are ignored. Returns how many copies were deleted.
    ///
    /// The writers resume from the trackers' last read, so call
    /// [`live_reload`](Self::live_reload) first, under the shard's write lock,
    /// and again before any later call. Decide on `ids` after that reload: a
    /// copy written since the last read would be deleted too. On error, some
    /// segments may already be written.
    pub fn delete_points(&mut self, ids: &[PointIdType]) -> OperationResult<usize> {
        let mut by_segment: HashMap<Uuid, Vec<(PointIdType, PointOffsetType)>> = HashMap::new();
        for (id, located) in self.locate(ids)? {
            for (segment, internal_id) in located.slots {
                by_segment
                    .entry(segment)
                    .or_default()
                    .push((id, internal_id));
            }
        }

        let mut deleted = 0;
        for (uuid, points) in by_segment {
            let segment = &self.segments[&uuid];
            UpdateOnlySegmentEnum::open(
                self.fs.clone(),
                &segment.segment_path,
                &segment.segment_config,
                segment.writer_state(),
            )?
            .tombstone_points(&points)?;
            deleted += points.len();
        }
        Ok(deleted)
    }

    /// Every copy of `ids` across the segments, with the newest marked.
    fn locate(
        &self,
        ids: &[PointIdType],
    ) -> OperationResult<AHashMap<PointIdType, PointLocations>> {
        let per_segment = self.pool.install(|| {
            self.segments
                .par_iter()
                .map(|(uuid, segment)| locate_in(*uuid, segment, ids))
                .collect::<OperationResult<Vec<_>>>()
        })?;
        Ok(merge_locations(per_segment))
    }
}
