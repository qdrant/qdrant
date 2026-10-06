//! Finding a point's copies across segments. Used by the update-only and
//! delete-only shards.

use ahash::AHashMap;
use common::types::PointOffsetType;
use common::universal_io::UniversalReadFsAsync;
use segment::common::operation_error::OperationResult;
use segment::segment::update_only::{LookupSegment, TrackerLookup};
use segment::types::{PointIdType, SeqNumberType};
use uuid::Uuid;

/// What locating points needs from a segment.
pub(crate) trait LocateSegment {
    fn locate_points(
        &self,
        point_ids: impl IntoIterator<Item = PointIdType>,
        callback: impl FnMut(PointIdType, PointOffsetType),
    ) -> OperationResult<()>;

    fn point_versions(
        &self,
        internal_ids: &[PointOffsetType],
    ) -> OperationResult<AHashMap<PointOffsetType, SeqNumberType>>;

    fn appendable(&self) -> bool;
}

impl<Fs: UniversalReadFsAsync> LocateSegment for LookupSegment<Fs> {
    fn locate_points(
        &self,
        point_ids: impl IntoIterator<Item = PointIdType>,
        callback: impl FnMut(PointIdType, PointOffsetType),
    ) -> OperationResult<()> {
        LookupSegment::locate_points(self, point_ids, callback)
    }

    fn point_versions(
        &self,
        internal_ids: &[PointOffsetType],
    ) -> OperationResult<AHashMap<PointOffsetType, SeqNumberType>> {
        LookupSegment::point_versions(self, internal_ids)
    }

    fn appendable(&self) -> bool {
        self.appendable
    }
}

impl<Fs: UniversalReadFsAsync> LocateSegment for TrackerLookup<Fs> {
    fn locate_points(
        &self,
        point_ids: impl IntoIterator<Item = PointIdType>,
        callback: impl FnMut(PointIdType, PointOffsetType),
    ) -> OperationResult<()> {
        TrackerLookup::locate_points(self, point_ids, callback)
    }

    fn point_versions(
        &self,
        internal_ids: &[PointOffsetType],
    ) -> OperationResult<AHashMap<PointOffsetType, SeqNumberType>> {
        TrackerLookup::point_versions(self, internal_ids)
    }

    fn appendable(&self) -> bool {
        self.appendable
    }
}

/// One copy of a point: where it lives, and at what version.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PointLocation {
    pub(crate) segment: Uuid,
    pub(crate) internal_id: PointOffsetType,
    pub(crate) version: SeqNumberType,
    /// Whether the holding segment accepts appends; breaks a version tie.
    appendable: bool,
}

impl PointLocation {
    /// Whether this copy of the point supersedes `other`: the higher version
    /// wins, and on a tie the appendable copy is the live one (a point being
    /// moved between segments exists in both at the same version).
    fn supersedes(&self, other: &Self) -> bool {
        (self.version, self.appendable) > (other.version, other.appendable)
    }
}

/// Every copy of one point across the shard's segments.
pub(crate) struct PointLocations {
    /// The live copy: its version decides whether the batch is already
    /// applied, and its slot is the one a resolve reads from.
    pub(crate) newest: PointLocation,
    /// Every slot the point occupies, `newest`'s included. A rewrite or a
    /// delete retires them all — tombstoning only the newest slot would let
    /// an older duplicate (left by an interrupted move) outlive the point
    /// and, on a delete, resurrect it.
    pub(crate) slots: Vec<(Uuid, PointOffsetType)>,
}

/// Copies of `ids` in segment `uuid`.
pub(crate) fn locate_in(
    uuid: Uuid,
    segment: &impl LocateSegment,
    ids: &[PointIdType],
) -> OperationResult<Vec<(PointIdType, PointLocation)>> {
    let mut found_ids = Vec::new();
    let mut internal_ids = Vec::new();
    segment.locate_points(ids.iter().copied(), |id, internal_id| {
        found_ids.push(id);
        internal_ids.push(internal_id);
    })?;
    let versions = segment.point_versions(&internal_ids)?;
    let appendable = segment.appendable();

    Ok(found_ids
        .into_iter()
        .zip(internal_ids)
        .map(|(id, internal_id)| {
            let location = PointLocation {
                segment: uuid,
                internal_id,
                // A slot without a stored version is unwritten, which
                // compares as version 0.
                version: versions.get(&internal_id).copied().unwrap_or(0),
                appendable,
            };
            (id, location)
        })
        .collect())
}

/// Group copies by point and mark the newest.
pub(crate) fn merge_locations(
    per_segment: Vec<Vec<(PointIdType, PointLocation)>>,
) -> AHashMap<PointIdType, PointLocations> {
    let mut locations: AHashMap<PointIdType, PointLocations> = AHashMap::new();
    for (id, location) in per_segment.into_iter().flatten() {
        let slot = (location.segment, location.internal_id);
        locations
            .entry(id)
            .and_modify(|current| {
                current.slots.push(slot);
                if location.supersedes(&current.newest) {
                    current.newest = location;
                }
            })
            .or_insert_with(|| PointLocations {
                newest: location,
                slots: vec![slot],
            });
    }
    locations
}
