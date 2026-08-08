//! The writer over one segment, whichever of the two kinds it turned out to
//! be.

use std::path::Path;

use common::types::PointOffsetType;
use common::universal_io::{UniversalAppend, UniversalWrite};

use super::{AppendableIdTrackerState, AppendableSegment, DeleteOnlySegment};
use crate::common::operation_error::OperationResult;
use crate::types::PointIdType;

/// A segment opened for writing: appendable, or accepting deletes only.
pub enum UpdateOnlySegmentEnum<S: UniversalAppend + UniversalWrite + 'static> {
    DeleteOnly(DeleteOnlySegment<S>),
    Appendable(AppendableSegment<S>),
}

impl<S: UniversalAppend + UniversalWrite + 'static> UpdateOnlySegmentEnum<S> {
    /// Open a writer over the segment directory at `segment_path`: appendable
    /// when the read phase handed over a mappings-log state to resume from,
    /// delete-only when it had none to give.
    pub fn open(
        fs: &S::Fs,
        segment_path: &Path,
        id_tracker_state: Option<AppendableIdTrackerState>,
    ) -> OperationResult<Self> {
        Ok(match id_tracker_state {
            Some(state) => {
                Self::Appendable(AppendableSegment::open(fs.clone(), segment_path, state)?)
            }
            None => Self::DeleteOnly(DeleteOnlySegment::open(fs.clone(), segment_path)),
        })
    }

    /// The appendable writer, when this segment is one; `None` when it accepts
    /// deletes only. Storing points is the one operation the two do not share.
    pub fn as_appendable_mut(&mut self) -> Option<&mut AppendableSegment<S>> {
        match self {
            Self::Appendable(segment) => Some(segment),
            Self::DeleteOnly(_) => None,
        }
    }

    /// Retire the given points, each named by both its external id and the
    /// slot it occupies here — the two formats need different halves of that
    /// pair, see [`DeleteOnlySegment::tombstone_points`] and
    /// [`AppendableSegment::tombstone_points`].
    pub fn tombstone_points(
        &mut self,
        points: &[(PointIdType, PointOffsetType)],
    ) -> OperationResult<()> {
        match self {
            Self::DeleteOnly(segment) => segment.tombstone_points(points),
            Self::Appendable(segment) => segment.tombstone_points(points),
        }
    }
}
