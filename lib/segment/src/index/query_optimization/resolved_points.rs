//! A filter evaluated once in one segment, to be shared by several searches of a batch.
//!
//! A batch over several named vectors runs one search per vector, and each search
//! evaluates its filter on its own. When that is more expensive than evaluating the filter
//! once, the segment resolves it to the offsets of its matching points (see
//! [`share_filter`]), and the searches use [`Condition::ResolvedPoints`] instead. Checking a
//! point is then a bit test or a binary search, the cardinality is exact, and the points can
//! be iterated directly.
//!
//! Offsets are only meaningful in the segment that resolved them, so the condition carries
//! the segment's uuid, and a segment refuses to search with one resolved elsewhere (see
//! [`check_resolved_points`]).
//!
//! [`share_filter`]: crate::entry::entry_point::ReadSegmentEntry::share_filter
//! [`Condition::ResolvedPoints`]: crate::types::Condition::ResolvedPoints

use std::fmt;
use std::hash::{self, Hash};
use std::sync::Arc;

use uuid::Uuid;

use super::point_set::PointSet;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::{Condition, Filter};

/// Offsets of the points of one segment that match a filter.
#[derive(Clone)]
pub struct ResolvedPoints {
    segment_uuid: Uuid,
    points: Arc<PointSet>,
    /// Cached [`PointSet::len`], which counts the bits of a bitset
    len: usize,
}

impl ResolvedPoints {
    pub fn new(segment_uuid: Uuid, points: PointSet) -> Self {
        let len = points.len();
        Self {
            segment_uuid,
            points: Arc::new(points),
            len,
        }
    }

    pub fn segment_uuid(&self) -> Uuid {
        self.segment_uuid
    }

    pub fn points(&self) -> &Arc<PointSet> {
        &self.points
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl fmt::Debug for ResolvedPoints {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            segment_uuid,
            points: _,
            len,
        } = self;
        f.debug_struct("ResolvedPoints")
            .field("segment_uuid", segment_uuid)
            .field("len", len)
            .finish_non_exhaustive()
    }
}

impl PartialEq for ResolvedPoints {
    fn eq(&self, other: &Self) -> bool {
        // Two resolutions are the same condition only if they are the same set
        Arc::ptr_eq(&self.points, &other.points)
    }
}

impl Eq for ResolvedPoints {}

impl Hash for ResolvedPoints {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        std::ptr::hash(Arc::as_ptr(&self.points), state);
    }
}

/// Fails if `filter` holds a [`Condition::ResolvedPoints`] resolved in another segment than
/// `segment_uuid`: its offsets would select unrelated points.
pub fn check_resolved_points(filter: &Filter, segment_uuid: Uuid) -> OperationResult<()> {
    filter
        .iter_conditions()
        .try_for_each(|condition| match condition {
            Condition::ResolvedPoints(resolved) if resolved.segment_uuid != segment_uuid => {
                Err(OperationError::service_error(format!(
                    "filter resolved in segment {} used in segment {segment_uuid}",
                    resolved.segment_uuid,
                )))
            }
            Condition::ResolvedPoints(_) => Ok(()),
            Condition::Filter(filter) => check_resolved_points(filter, segment_uuid),
            // A nested filter checks array elements, which have no offsets: never resolved
            Condition::Nested(_)
            | Condition::Field(_)
            | Condition::IsEmpty(_)
            | Condition::IsNull(_)
            | Condition::HasId(_)
            | Condition::HasVector(_)
            | Condition::Slice(_)
            | Condition::CustomIdChecker(_) => Ok(()),
        })
}
