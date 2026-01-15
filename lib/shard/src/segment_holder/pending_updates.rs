use crate::locked_segment::LockedSegment;
use crate::segment_holder::SegmentId;

#[derive(Debug, Default)]
pub enum PendingUpdatesSegment {
    /// There are no segment for pending updates
    /// therefore all updates should be routed to regular segments
    #[default]
    Empty,
    /// Segment, which is supposed to accumulate updates before optimization is available
    /// Temp segment is stored in the same directory as regular segments, but excluded from
    /// read operations.
    Used((SegmentId, LockedSegment)),
    /// Pending updates segment is currently under optimization.
    /// All incoming updates should be postponed.
    Optimizing((SegmentId, LockedSegment)),
}

impl PendingUpdatesSegment {
    /// Check if the segment is in optimizing state
    pub fn get_segment_id(&self) -> Option<SegmentId> {
        match self {
            PendingUpdatesSegment::Empty => None,
            PendingUpdatesSegment::Used((segment_id, _)) => Some(*segment_id),
            PendingUpdatesSegment::Optimizing((segment_id, _)) => Some(*segment_id),
        }
    }

    pub fn get_locked_segment(&self) -> Option<&LockedSegment> {
        match self {
            PendingUpdatesSegment::Empty => None,
            PendingUpdatesSegment::Used((_, segment)) => Some(segment),
            PendingUpdatesSegment::Optimizing((_, segment)) => Some(segment),
        }
    }

    pub fn get_locked_segment_by_id(&self, segment_id: SegmentId) -> Option<&LockedSegment> {
        match self {
            PendingUpdatesSegment::Empty => None,
            PendingUpdatesSegment::Used((id, segment))
            | PendingUpdatesSegment::Optimizing((id, segment))
                if *id == segment_id =>
            {
                Some(segment)
            }
            _ => None,
        }
    }

    pub fn iter_updatable(&self) -> impl Iterator<Item = (SegmentId, &LockedSegment)> {
        match self {
            PendingUpdatesSegment::Empty => vec![].into_iter(),
            PendingUpdatesSegment::Used((segment_id, segment)) => {
                vec![(*segment_id, segment)].into_iter()
            }
            PendingUpdatesSegment::Optimizing(_) => vec![].into_iter(),
        }
    }
}
