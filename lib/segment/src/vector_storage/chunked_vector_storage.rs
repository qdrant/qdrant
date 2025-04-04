use std::mem::MaybeUninit;
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;

use crate::common::Flusher;
use crate::common::operation_error::OperationResult;

/// In case of simple vector storage, vector offset is the same as PointOffsetType.
/// But in case of multivectors, it requires an additional lookup.
pub type VectorOffsetType = usize;

#[allow(clippy::len_without_is_empty)]
pub trait ChunkedVectorStorage<T> {
    fn len(&self) -> usize;

    fn dim(&self) -> usize;

    fn get(&self, key: VectorOffsetType) -> Option<&[T]>;

    fn files(&self) -> Vec<PathBuf>;

    fn flusher(&self) -> Flusher;

    fn push(
        &mut self,
        vector: &[T],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<VectorOffsetType>;

    fn insert(
        &mut self,
        key: VectorOffsetType,
        vector: &[T],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    fn insert_many(
        &mut self,
        start_key: VectorOffsetType,
        vectors: &[T],
        count: usize,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    /// Returns `count` flattened vectors starting from key. if chunk boundary is crossed, returns None
    fn get_many(&self, key: VectorOffsetType, count: usize) -> Option<&[T]>;

    /// Returns batch of vectors by keys.
    /// Underlying storage might apply some optimizations to prefetch vectors.
    fn get_batch<'a>(
        &'a self,
        keys: &[VectorOffsetType],
        vectors: &'a mut [MaybeUninit<&'a [T]>],
    ) -> &'a [&'a [T]];

    fn get_remaining_chunk_keys(&self, start_key: VectorOffsetType) -> usize;

    fn max_vector_size_bytes(&self) -> usize;

    /// True, if this storage is on-disk by default.
    fn is_on_disk(&self) -> bool;

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    fn populate(&self) -> OperationResult<()>;

    /// Drop disk cache.
    fn clear_cache(&self) -> OperationResult<()>;
}
