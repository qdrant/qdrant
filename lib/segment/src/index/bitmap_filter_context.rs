use common::types::PointOffsetType;
use roaring::RoaringBitmap;

/// A [`FilterContext`] backed by a pre-materialized set of matching points.
///
/// Built by evaluating a filter once (e.g. collecting
/// [`PayloadIndexRead::iter_filtered_points`]) so that every subsequent
/// [`check`] is a bitmap probe instead of a full per-condition evaluation
/// against the payload indexes.
///
/// Pays off whenever the same filter would otherwise be checked more than
/// once per point — e.g. facet counting, where every posting-list element of
/// every value is tested against the same filter, and points holding several
/// values are tested once per value.
///
/// Note that, unlike the lazy `StructFilterContext`, the set is fixed at
/// construction time: whatever visibility rules (deleted points, deferred
/// cutoff) the producing iterator applied are baked in.
///
/// [`PayloadIndexRead::iter_filtered_points`]: crate::index::PayloadIndexRead::iter_filtered_points
/// [`check`]: FilterContext::check
pub struct BitmapFilterContext(RoaringBitmap);

impl BitmapFilterContext {
    /// Whether `point_id` matched the filter.
    pub fn check(&self, point_id: PointOffsetType) -> bool {
        self.0.contains(point_id)
    }

    /// Number of points matching the materialized filter.
    pub fn len(&self) -> usize {
        self.0.len() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromIterator<PointOffsetType> for BitmapFilterContext {
    fn from_iter<I: IntoIterator<Item = PointOffsetType>>(iter: I) -> Self {
        Self(RoaringBitmap::from_iter(iter))
    }
}
