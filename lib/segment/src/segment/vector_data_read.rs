use std::ops::Deref;

use atomic_refcell::AtomicRef;

use crate::index::{VectorIndexEnum, VectorIndexRead};
use crate::segment::VectorData;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// Read-only view over a single named vector entry of a segment.
///
/// Abstracts the concrete `VectorData` so that `SegmentReadView` can be
/// constructed from any segment-like type (e.g. a future `ReadOnlySegment`)
/// that exposes its per-vector storages through this trait.
pub trait VectorDataRead {
    type IndexRef<'a>: Deref<Target: VectorIndexRead>
    where
        Self: 'a;

    type StorageRef<'a>: Deref<Target: VectorStorageRead>
    where
        Self: 'a;

    fn vector_index(&self) -> Self::IndexRef<'_>;

    fn vector_storage(&self) -> Self::StorageRef<'_>;
}

impl VectorDataRead for VectorData {
    type IndexRef<'a> = AtomicRef<'a, VectorIndexEnum>;
    type StorageRef<'a> = AtomicRef<'a, VectorStorageEnum>;

    fn vector_index(&self) -> Self::IndexRef<'_> {
        self.vector_index.borrow()
    }

    fn vector_storage(&self) -> Self::StorageRef<'_> {
        self.vector_storage.borrow()
    }
}
