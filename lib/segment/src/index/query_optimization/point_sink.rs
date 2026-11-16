//! Receiving the points of a posting list as the index reads them.
//!
//! Some indexes can only read a posting list inside a callback, so they can't hand it out
//! as an iterator without collecting it first. They push the points into a [`PointSink`]
//! instead, which builds whatever the consumer needs (see
//! [`PointSetBuilder`](super::point_set::PointSetBuilder)).

use common::types::PointOffsetType;

/// Receives point offsets read from a posting list. Points may come in any order and may
/// repeat, e.g. when several posting lists are read one after another.
pub trait PointSink {
    fn push(&mut self, point: PointOffsetType);

    /// Push a run of points, e.g. one decompressed chunk of a posting list.
    fn extend_from_slice(&mut self, points: &[PointOffsetType]) {
        for &point in points {
            self.push(point);
        }
    }
}

impl PointSink for Vec<PointOffsetType> {
    fn push(&mut self, point: PointOffsetType) {
        Vec::push(self, point);
    }

    fn extend_from_slice(&mut self, points: &[PointOffsetType]) {
        Vec::extend_from_slice(self, points);
    }
}
