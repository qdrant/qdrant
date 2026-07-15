use ahash::AHashSet;

use crate::types::PointIdType;

/// A batch of external point ids handed to the id-tracker resolve path.
///
/// Unlike a bare iterator it is `Copy`, knows its `len`, and can be iterated
/// more than once without being collected. The disk trackers rely on both: they
/// walk the batch once to group the lookups by storage block and again to drive
/// the callback, so a re-iterable, length-known input lets them batch the reads
/// without first buffering the ids into a `Vec`.
///
/// Implemented for the two shapes callers already hold — `&[PointIdType]` and
/// `&AHashSet<PointIdType>`. Both are shared references, hence `Copy`, and both
/// expose an `ExactSizeIterator`.
///
/// The methods are deliberately named `num_ids` / `iter_ids` rather than
/// `len` / `iter`: the impls are on reference types whose inherent `len` /
/// `iter` we want to call, and identically named trait methods would shadow
/// them (resolving back into this trait and recursing).
pub trait PointIdBatch: Copy {
    /// Number of ids in the batch.
    fn num_ids(self) -> usize;

    /// A fresh iterator over the ids. May be called repeatedly — the batch is
    /// `Copy`, so each call starts a new pass over the same underlying data.
    fn iter_ids(self) -> impl Iterator<Item = PointIdType>;
}

impl PointIdBatch for &[PointIdType] {
    fn num_ids(self) -> usize {
        self.len()
    }

    fn iter_ids(self) -> impl Iterator<Item = PointIdType> {
        self.iter().copied()
    }
}

impl PointIdBatch for &AHashSet<PointIdType> {
    fn num_ids(self) -> usize {
        self.len()
    }

    fn iter_ids(self) -> impl Iterator<Item = PointIdType> {
        self.iter().copied()
    }
}
