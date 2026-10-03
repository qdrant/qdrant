//! Delete-only shard: retires the copies of points that a rebuild published
//! newer versions of, over the segments outside that rebuild.
//!
//! With deferred points the writer keeps a point's older copies visible while
//! its newer copy waits to be indexed. Once a rebuild publishes the newer copy
//! in a segment of its own, the older ones are duplicates. Only the id
//! trackers are opened: retiring a point never reads its payload or vectors.

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

/// Retires superseded copies of points across the segments it was opened on.
pub struct DeleteOnlyEdgeShard<Fs: UniversalAppendFs> {
    fs: Fs,
    segments: HashMap<Uuid, TrackerLookup<Fs>>,
    pool: Arc<ThreadPool>,
}

impl<Fs: UniversalAppendFs> DeleteOnlyEdgeShard<Fs> {
    /// Open the id tracker of every segment `enumerator` lists. Nothing is
    /// written: writers are resumed only in
    /// [`retire_superseded`](Self::retire_superseded), because resuming an
    /// appendable writer retires the slots an in-flight batch has claimed.
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
    /// retire, against the trackers as last read.
    pub fn superseded_count(
        &self,
        newer: &HashMap<PointIdType, SeqNumberType>,
    ) -> OperationResult<usize> {
        Ok(self.superseded(newer)?.values().map(Vec::len).sum())
    }

    /// Refresh every tracker, then retire each point's copies whose newest is
    /// older than the point's version in `newer`. A point with a copy here at
    /// or past that version keeps all of them: that newer copy is a deferred
    /// head, and the older ones serve until it is indexed. Returns how many
    /// copies were retired.
    ///
    /// Must run under the shard's write lock: the writers resume from the
    /// state this call reads. On error some segments may already be written;
    /// a retry against a freshly opened shard finds only what is left.
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
            // So that a later call resumes from what this one wrote.
            segment.live_reload()?;
            retired += points.len();
        }
        Ok(retired)
    }

    /// The copies to retire, grouped by segment.
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
