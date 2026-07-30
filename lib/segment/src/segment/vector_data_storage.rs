use std::ops::Deref;

use atomic_refcell::AtomicRef;

use crate::segment::VectorData;
use crate::segment::vector_data_read::VectorDataRead;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// Access to a single named vector's *storage*, without its index.
///
/// Narrower counterpart of [`VectorDataRead`]: an implementer only has to
/// carry the vector storage — no vector index, no quantized vectors.
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
