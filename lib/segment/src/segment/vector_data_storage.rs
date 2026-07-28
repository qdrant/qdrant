use std::ops::Deref;

use atomic_refcell::AtomicRef;

use crate::segment::VectorData;
use crate::segment::vector_data_read::VectorDataRead;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// Access to a single named vector's *storage*, without its index.
///
/// Narrower counterpart of [`VectorDataRead`], which also hands out the vector
/// index. The update path never touches a vector index — an appendable segment
/// does not persist one, so the writer neither reads nor maintains it — and the
/// update-only segment therefore never opens one. Requiring only this trait is
/// what lets [`SegmentUpdateView`] be built over a segment that carries
/// storages alone.
///
/// [`SegmentUpdateView`]: crate::segment::update_view::SegmentUpdateView
pub trait VectorDataStorageRead {
    type StorageRef<'a>: Deref<Target: VectorStorageRead>
    where
        Self: 'a;

    fn vector_storage(&self) -> Self::StorageRef<'_>;
}

impl VectorDataStorageRead for VectorData {
    type StorageRef<'a> = AtomicRef<'a, VectorStorageEnum>;

    fn vector_storage(&self) -> Self::StorageRef<'_> {
        VectorDataRead::vector_storage(self)
    }
}
