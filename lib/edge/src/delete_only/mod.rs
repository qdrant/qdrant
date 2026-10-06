//! Delete-only shard: removes old copies of points after a rebuild publishes
//! newer ones.
//!
//! While a point's newer copy is deferred, its older copies stay visible. Once
//! a rebuild publishes the newer copy, the older ones are duplicates.

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

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
use crate::update_only::locate::{locate_in, merge_locations};

/// Deletes outdated point copies in the segments it was opened on. Opens id
/// trackers only.
pub struct DeleteOnlyEdgeShard<Fs: UniversalAppendFs> {
    fs: Fs,
    segments: HashMap<Uuid, TrackerLookup<Fs>>,
    pool: Arc<ThreadPool>,
}

impl<Fs: UniversalAppendFs> DeleteOnlyEdgeShard<Fs> {
    /// Open the id trackers of the listed segments. Writes nothing: opening an
    /// appendable writer would drop slots claimed by an in-flight batch, so
    /// writers are opened in [`delete_outdated`](Self::delete_outdated).
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

    /// Delete the outdated copies of the points in `published`, the versions
    /// a rebuild published elsewhere. A point's copies here are outdated when
    /// all of them are older than its published version; if any copy is at
    /// least as new, it is a deferred head and all copies stay. Returns the
    /// number of copies deleted.
    ///
    /// The writers resume from the trackers' last read, so call
    /// [`live_reload`](Self::live_reload) first, under the shard's write lock,
    /// and again before any later call. On error, some segments may already
    /// be written.
    pub fn delete_outdated(
        &mut self,
        published: &HashMap<PointIdType, SeqNumberType>,
    ) -> OperationResult<usize> {
        let mut deleted = 0;
        for (uuid, points) in self.find_outdated(published)? {
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

    /// Dry run of [`delete_outdated`](Self::delete_outdated) against the
    /// trackers' last read: the copies it would delete, as `(point id,
    /// internal id)` pairs per segment.
    pub fn find_outdated(
        &self,
        published: &HashMap<PointIdType, SeqNumberType>,
    ) -> OperationResult<HashMap<Uuid, Vec<(PointIdType, PointOffsetType)>>> {
        let ids: Vec<PointIdType> = published.keys().copied().collect();
        let per_segment = self.pool.install(|| {
            self.segments
                .par_iter()
                .map(|(uuid, segment)| locate_in(*uuid, segment, &ids))
                .collect::<OperationResult<Vec<_>>>()
        })?;

        let mut by_segment: HashMap<Uuid, Vec<(PointIdType, PointOffsetType)>> = HashMap::new();
        for (id, located) in merge_locations(per_segment) {
            if located.newest.version >= published[&id] {
                continue;
            }
            for (segment, internal_id) in located.slots {
                by_segment
                    .entry(segment)
                    .or_default()
                    .push((id, internal_id));
            }
        }
        Ok(by_segment)
    }
}
