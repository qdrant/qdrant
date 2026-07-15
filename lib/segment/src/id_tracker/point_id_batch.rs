use ahash::AHashSet;

use crate::types::PointIdType;

/// A batch of external point ids handed to the id-tracker resolve path.
///
/// It exists so callers can pass the shapes they already hold — `&[PointIdType]`
/// and `&AHashSet<PointIdType>` — and have the resolve path iterate them as
/// owned `PointIdType`s without an explicit `.iter().copied()` at every call
/// site. The batch is `Copy` (both impls are shared references), so it is cheap
/// to thread down the call chain by value.
///
/// The method is named `iter_ids` rather than `iter`: the impls are on reference
/// types whose inherent `iter` we want to call, and an identically named trait
/// method would shadow it (resolving back into this trait and recursing).
pub trait PointIdBatch: Copy {
    /// An iterator over the ids as owned `PointIdType`s.
    fn iter_ids(self) -> impl Iterator<Item = PointIdType>;
}

impl PointIdBatch for &[PointIdType] {
    fn iter_ids(self) -> impl Iterator<Item = PointIdType> {
        self.iter().copied()
    }
}

impl PointIdBatch for &AHashSet<PointIdType> {
    fn iter_ids(self) -> impl Iterator<Item = PointIdType> {
        self.iter().copied()
    }
}
