use ahash::AHashMap;
use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{UniversalRead, UniversalReadFsAsync};

use super::TrackerLookup;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerRead as _;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::types::{PointIdType, SeqNumberType};

impl<Fs: UniversalReadFsAsync> TrackerLookup<Fs> {
    /// See [`LookupSegment::locate_points`](crate::segment::update_only::LookupSegment::locate_points).
    pub fn locate_points(
        &self,
        point_ids: impl IntoIterator<Item = PointIdType>,
        callback: impl FnMut(PointIdType, PointOffsetType),
    ) -> OperationResult<()> {
        locate_points(&self.id_tracker, point_ids, callback)
    }

    /// See [`LookupSegment::point_versions`](crate::segment::update_only::LookupSegment::point_versions).
    pub fn point_versions(
        &self,
        internal_ids: &[PointOffsetType],
    ) -> OperationResult<AHashMap<PointOffsetType, SeqNumberType>> {
        point_versions(&self.id_tracker, internal_ids)
    }
}

/// Resolve external ids to internal ids, calling `callback` for each id the
/// tracker holds. Includes deferred points, so each id resolves to its latest
/// slot.
pub(in crate::segment::update_only) fn locate_points<S: UniversalRead>(
    id_tracker: &ReadOnlyIdTrackerEnum<S>,
    point_ids: impl IntoIterator<Item = PointIdType>,
    callback: impl FnMut(PointIdType, PointOffsetType),
) -> OperationResult<()> {
    id_tracker.resolve_external_ids(point_ids, DeferredBehavior::WithDeferred, callback)
}

/// Versions of the given internal ids. Ids without a stored version are
/// missing from the map and count as version 0.
pub(in crate::segment::update_only) fn point_versions<S: UniversalRead>(
    id_tracker: &ReadOnlyIdTrackerEnum<S>,
    internal_ids: &[PointOffsetType],
) -> OperationResult<AHashMap<PointOffsetType, SeqNumberType>> {
    let mut versions = AHashMap::with_capacity(internal_ids.len());
    id_tracker.internal_versions_batch(internal_ids.iter().copied(), |internal_id, version| {
        versions.insert(internal_id, version);
    })?;
    Ok(versions)
}
