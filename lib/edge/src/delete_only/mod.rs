//! Delete-only shard: removes old copies of points after a rebuild publishes
//! newer ones.
//!
//! While a point's newer copy is deferred, its older copies stay visible. Once
//! a rebuild publishes the newer copy, the older ones are duplicates.

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
    /// writers are opened in [`retire_superseded`](Self::retire_superseded).
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

    /// How many copies [`retire_superseded`](Self::retire_superseded) would
    /// delete, based on the last read.
    pub fn superseded_count(
        &self,
        newer: &HashMap<PointIdType, SeqNumberType>,
    ) -> OperationResult<usize> {
        Ok(self.superseded(newer)?.values().map(Vec::len).sum())
    }

    /// Reload the trackers, then delete every copy of a point when all its
    /// copies here are older than its version in `newer`. If any copy is at
    /// least that new, it is a deferred head and all copies stay. Returns the
    /// number of copies deleted.
    ///
    /// Call under the shard's write lock. On error, some segments may already
    /// be written; reopen and retry.
    pub fn retire_superseded(
        &mut self,
        newer: &HashMap<PointIdType, SeqNumberType>,
    ) -> OperationResult<usize> {
        self.pool.install(|| {
            self.segments
                .par_iter_mut()
                .try_for_each(|(_, segment)| segment.live_reload())
        })?;

        let superseded = self.superseded(newer)?;
        let mut retired = 0;
        for (uuid, points) in superseded {
            let segment = self
                .segments
                .get_mut(&uuid)
                .expect("superseded copies come from opened segments");
            UpdateOnlySegmentEnum::open(
                self.fs.clone(),
                &segment.segment_path,
                &segment.segment_config,
                segment.writer_state(),
            )?
            .tombstone_points(&points)?;
            // So a later call starts from this write.
            segment.live_reload()?;
            retired += points.len();
        }
        Ok(retired)
    }

    /// Find the outdated copies of the points in `newer`.
    ///
    /// A point's copies here are outdated when all of them are older than the
    /// point's version in `newer`. Returns, per segment, the `(point id,
    /// internal id)` pairs to tombstone there.
    fn superseded(
        &self,
        newer: &HashMap<PointIdType, SeqNumberType>,
    ) -> OperationResult<AHashMap<Uuid, Vec<(PointIdType, PointOffsetType)>>> {
        let ids: Vec<PointIdType> = newer.keys().copied().collect();
        let per_segment = self.pool.install(|| {
            self.segments
                .par_iter()
                .map(|(uuid, segment)| locate_in(*uuid, segment, &ids))
                .collect::<OperationResult<Vec<_>>>()
        })?;

        let mut by_segment: AHashMap<Uuid, Vec<(PointIdType, PointOffsetType)>> = AHashMap::new();
        for (id, located) in merge_locations(per_segment) {
            if located.newest.version >= newer[&id] {
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
