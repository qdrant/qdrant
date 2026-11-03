//! The writer over one segment, whichever of the two kinds it turned out to
//! be.

use std::path::Path;

use common::types::PointOffsetType;
use common::universal_io::UniversalAppendFs;

use super::{AppendableSegment, DeleteOnlySegment, WriterIdTrackerState};
use crate::common::operation_error::OperationResult;
use crate::id_tracker::delete_versions::DeleteVersioning;
use crate::types::{PointIdType, SegmentConfig, SeqNumberType};

/// A segment opened for writing: appendable, or accepting deletes only.
pub enum UpdateOnlySegmentEnum<Fs: UniversalAppendFs> {
    DeleteOnly(DeleteOnlySegment<Fs>),
    Appendable(Box<AppendableSegment<Fs>>),
}

impl<Fs: UniversalAppendFs> UpdateOnlySegmentEnum<Fs> {
    /// Open a writer over the segment directory at `segment_path`, of the
    /// kind the read phase's `id_tracker_state` dictates.
    pub fn open(
        fs: Fs,
        segment_path: &Path,
        config: &SegmentConfig,
        id_tracker_state: WriterIdTrackerState,
    ) -> OperationResult<Self> {
        Ok(match id_tracker_state {
            WriterIdTrackerState::Appendable(state) => Self::Appendable(Box::new(
                AppendableSegment::open(fs, segment_path, config, state)?,
            )),
            WriterIdTrackerState::DeleteOnly(state) => {
                Self::DeleteOnly(DeleteOnlySegment::open(fs, segment_path, state)?)
            }
        })
    }

    /// The appendable writer, when this segment is one; `None` when it accepts
    /// deletes only. Storing points is the one operation the two do not share.
    pub fn as_appendable_mut(&mut self) -> Option<&mut AppendableSegment<Fs>> {
        match self {
            Self::Appendable(segment) => Some(segment),
            Self::DeleteOnly(_) => None,
        }
    }

    /// Retire the given points, each named by both its external id and the
    /// slot it occupies here — the two formats need different halves of that
    /// pair, see [`DeleteOnlySegment::tombstone_points`] and
    /// [`AppendableSegment::tombstone_points`].
    ///
    /// `version` is the version of the batch that deletes the points, recorded
    /// as [`delete_versions`](crate::id_tracker::delete_versions) describes.
    pub fn tombstone_points(
        &mut self,
        points: &[(PointIdType, PointOffsetType)],
        version: SeqNumberType,
        versioning: DeleteVersioning,
    ) -> OperationResult<()> {
        match self {
            Self::DeleteOnly(segment) => segment.tombstone_points(points, version, versioning),
            Self::Appendable(segment) => segment.tombstone_points(points, version, versioning),
        }
    }
}
