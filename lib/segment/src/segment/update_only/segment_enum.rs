//! The writer over one segment, whichever of the two kinds it turned out to
//! be.

use std::path::Path;

use common::types::PointOffsetType;
use common::universal_io::{UniversalAppend, UniversalWrite};

use super::{AppendableSegment, DeleteOnlySegment, SegmentWriterState};
use crate::common::operation_error::OperationResult;
use crate::types::PointIdType;

/// A segment opened for writing: appendable, or accepting deletes only.
pub enum UpdateOnlySegmentEnum<S: UniversalAppend + UniversalWrite + 'static> {
    DeleteOnly(DeleteOnlySegment<S>),
    Appendable(AppendableSegment<S>),
}

impl<S: UniversalAppend + UniversalWrite + 'static> UpdateOnlySegmentEnum<S> {
    /// Open the writer `state` calls for, over the segment directory at
    /// `segment_path`.
    pub fn open(
        fs: &S::Fs,
        segment_path: &Path,
        state: SegmentWriterState,
    ) -> OperationResult<Self> {
        match state {
            SegmentWriterState::DeleteOnly => Ok(Self::DeleteOnly(DeleteOnlySegment::open(
                fs.clone(),
                segment_path,
            ))),
            SegmentWriterState::Appendable(id_tracker_state) => Ok(Self::Appendable(
                AppendableSegment::open(fs.clone(), segment_path, id_tracker_state)?,
            )),
        }
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

    /// Persist everything written since the last flush. There is no WAL:
    /// writes are durable only once this returns.
    pub fn flush(&self) -> OperationResult<()> {
        match self {
            Self::DeleteOnly(segment) => segment.flush(),
            Self::Appendable(segment) => segment.flush(),
        }
    }
}
