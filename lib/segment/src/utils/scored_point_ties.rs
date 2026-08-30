use std::cmp::Ordering;

use bytemuck::{TransparentWrapper, TransparentWrapperAlloc as _};

use crate::types::ScoredPoint;

// Newtype to provide alternative comparator for ScoredPoint which breaks ties by id
pub struct ScoredPointTies<'a>(pub &'a ScoredPoint);

impl<'a> From<&'a ScoredPoint> for ScoredPointTies<'a> {
    fn from(scored_point: &'a ScoredPoint) -> Self {
        ScoredPointTies(scored_point)
    }
}

impl Ord for ScoredPointTies<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .cmp(other.0)
            // for identical scores, we fallback to sorting by ids to have a stable output
            .then_with(|| self.0.id.cmp(&other.0.id))
    }
}

impl PartialOrd for ScoredPointTies<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for ScoredPointTies<'_> {}

impl PartialEq for ScoredPointTies<'_> {
    fn eq(&self, other: &Self) -> bool {
        // Must match Ord: ScoredPoint::eq ignores order_value, but cmp uses it.
        self.cmp(other) == Ordering::Equal
    }
}

/// Owned variant of [`ScoredPointTies`] for APIs that need `Ord` on owned values
/// (e.g. `peek_top_*`). Same layout as [`ScoredPoint`] — use [`Self::into_scored_points`]
/// to recover a `Vec<ScoredPoint>` without reallocating.
#[derive(TransparentWrapper)]
#[repr(transparent)]
pub struct ScoredPointTiesOwned(pub ScoredPoint);

impl From<ScoredPoint> for ScoredPointTiesOwned {
    fn from(scored_point: ScoredPoint) -> Self {
        ScoredPointTiesOwned(scored_point)
    }
}

impl ScoredPointTiesOwned {
    /// Transmute `Vec<Self>` to `Vec<ScoredPoint>` with no reallocation.
    #[inline]
    pub fn into_scored_points(points: Vec<Self>) -> Vec<ScoredPoint> {
        Self::peel_vec(points)
    }
}

impl Ord for ScoredPointTiesOwned {
    fn cmp(&self, other: &Self) -> Ordering {
        ScoredPointTies(&self.0).cmp(&ScoredPointTies(&other.0))
    }
}

impl PartialOrd for ScoredPointTiesOwned {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for ScoredPointTiesOwned {}

impl PartialEq for ScoredPointTiesOwned {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
