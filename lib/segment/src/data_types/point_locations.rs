//! Where a query's searches found their results in a segment, so that the
//! query reads their payloads and vectors by offset afterwards instead of
//! looking their ids up again.

use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::types::{DeferredBehavior, PointOffsetType, ScoredPointOffset};
use parking_lot::Mutex;

use crate::common::operation_error::OperationResult;
use crate::data_types::segment_record::SegmentRecord;
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::{ExtendedPointId, PointIdType, SeqNumberType, WithPayload, WithVector};

/// Where a segment holds a point a search returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PointLocation {
    pub id: PointIdType,
    pub offset: PointOffsetType,
    /// The version the search saw at `offset`.
    pub version: SeqNumberType,
}

/// The locations of the points one segment's searches returned during a query.
#[derive(Debug, Default)]
pub struct PointLocations {
    locations: Mutex<AHashMap<PointIdType, PointLocation>>,
}

impl PointLocations {
    /// Record search results: `point_ids[i]` found at `scored[i]`, at its
    /// version in `versions`. A result without a version is not recorded.
    pub(crate) fn record_results(
        &self,
        point_ids: &[PointIdType],
        scored: &[ScoredPointOffset],
        versions: &AHashMap<PointOffsetType, SeqNumberType>,
    ) {
        let mut recorded = self.locations.lock();
        for (&id, scored) in point_ids.iter().zip(scored) {
            if let Some(&version) = versions.get(&scored.idx) {
                let location = PointLocation {
                    id,
                    offset: scored.idx,
                    version,
                };
                recorded.insert(id, location);
            }
        }
    }

    pub fn get(&self, point_id: &PointIdType) -> Option<PointLocation> {
        self.locations.lock().get(point_id).copied()
    }
}

impl<TIdT, TPI, TPS, TVD> SegmentReadView<'_, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    /// Records of the points at `locations`, read by offset without looking
    /// their ids up. A location this segment no longer holds as recorded is
    /// left out: its point deleted, past the deferred cutoff, or at another
    /// version.
    pub fn retrieve_at(
        &self,
        locations: &[PointLocation],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        is_stopped: &AtomicBool,
    ) -> OperationResult<AHashMap<ExtendedPointId, SegmentRecord>> {
        let cutoff = DeferredBehavior::VisibleOnly.apply(self.id_tracker.deferred_internal_id());
        let mut versions = AHashMap::with_capacity(locations.len());
        self.id_tracker.internal_versions_batch(
            locations.iter().map(|location| location.offset),
            |offset, version| {
                versions.insert(offset, version);
            },
        )?;
        let (resolved_ids, resolved_offsets): (Vec<_>, Vec<_>) = locations
            .iter()
            .filter(|location| {
                let PointLocation {
                    id: _,
                    offset,
                    version,
                } = **location;
                !cutoff.is_some_and(|cutoff| offset >= cutoff)
                    && !self.id_tracker.is_deleted_point(offset)
                    && versions.get(&offset) == Some(&version)
            })
            .map(|location| (location.id, location.offset))
            .unzip();
        self.retrieve_resolved(
            &resolved_ids,
            &resolved_offsets,
            with_payload,
            with_vector,
            is_stopped,
        )
    }
}
