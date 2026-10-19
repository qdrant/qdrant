//! Where each segment holds the points a query's searches returned, so that the
//! query reads their payloads and vectors by offset instead of by id. See
//! [`PointLocations`].

use ahash::AHashMap;
use segment::common::operation_error::OperationResult;
use segment::data_types::point_locations::PointLocations;
use segment::data_types::query_context::SegmentQueryContext;
use segment::entry::ReadSegmentEntry;
use segment::types::{PointIdType, WithPayload, WithVector};
use shard::query::ShardQueryResponse;
use shard::retrieve::record_internal::RecordInternal;
use uuid::Uuid;

use crate::read_view::{EdgeReadView, ReadSegmentHandle};

/// The [`PointLocations`] of each segment of a read view, for one query.
pub(crate) struct QueryPointLocations {
    by_segment: AHashMap<Uuid, PointLocations>,
}

impl QueryPointLocations {
    pub(super) fn new<H: ReadSegmentHandle>(segments: &[H]) -> Self {
        let by_segment = segments
            .iter()
            .map(|segment| {
                let uuid = segment.read_segment().segment_uuid();
                (uuid, PointLocations::default())
            })
            .collect();
        Self { by_segment }
    }

    fn segment(&self, uuid: &Uuid) -> Option<&PointLocations> {
        self.by_segment.get(uuid)
    }
}

/// `context` recording into `segment`'s share of `point_locations`, if given.
pub(super) fn with_segment_locations<'a>(
    context: SegmentQueryContext<'a>,
    point_locations: Option<&'a QueryPointLocations>,
    segment: &(impl ReadSegmentEntry + ?Sized),
) -> SegmentQueryContext<'a> {
    match point_locations.and_then(|locations| locations.segment(&segment.segment_uuid())) {
        Some(locations) => context.with_point_locations(locations),
        None => context,
    }
}

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    /// The records of the points in `query_response` that a search of this
    /// query recorded at the version they were scored with, read by offset from
    /// the segment holding that version. A point left out is for the caller to
    /// read by id.
    pub(super) fn retrieve_recorded(
        &self,
        query_response: &ShardQueryResponse,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        point_locations: &QueryPointLocations,
    ) -> OperationResult<AHashMap<PointIdType, RecordInternal>> {
        let mut pending: AHashMap<_, _> = query_response
            .iter()
            .flatten()
            .map(|point| (point.id, point.version))
            .collect();
        let mut records = AHashMap::with_capacity(pending.len());
        for segment in &self.segments {
            if pending.is_empty() {
                break;
            }
            let segment = segment.read_segment();
            let Some(recorded) = point_locations.segment(&segment.segment_uuid()) else {
                continue;
            };
            let locations: Vec<_> = pending
                .iter()
                .filter_map(|(point_id, &version)| {
                    recorded
                        .get(point_id)
                        .filter(|location| location.version == version)
                })
                .collect();
            if locations.is_empty() {
                continue;
            }
            for (point_id, record) in
                segment.retrieve_at(&locations, with_payload, with_vector, &self.is_stopped)?
            {
                pending.remove(&point_id);
                records.insert(point_id, RecordInternal::from(record));
            }
        }
        self.check_stopped()?;
        Ok(records)
    }
}
