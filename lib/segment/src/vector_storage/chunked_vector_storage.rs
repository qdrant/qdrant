use std::path::PathBuf;

use common::types::PointOffsetType;

use crate::common::operation_error::OperationResult;
use crate::common::Flusher;

pub trait ChunkedVectorStorage<T> {
    fn len(&self) -> usize;

    fn dim(&self) -> usize;

    fn get(&self, key: PointOffsetType) -> Option<&[T]>;

    fn files(&self) -> Vec<PathBuf>;

    fn flusher(&self) -> Flusher;

    fn push(&mut self, vector: &[T]) -> OperationResult<PointOffsetType>;

    fn insert(&mut self, key: PointOffsetType, vector: &[T]) -> OperationResult<()>;

    fn insert_many<TKey>(
        &mut self,
        start_key: TKey,
        vectors: &[T],
        count: usize,
    ) -> OperationResult<()>
    where
        TKey: num_traits::cast::AsPrimitive<usize>;

    fn get_many<TKey>(&self, key: TKey, count: usize) -> Option<&[T]>
    where
        TKey: num_traits::cast::AsPrimitive<usize>;

    fn get_remaining_chunk_keys<TKey>(&self, start_key: TKey) -> usize
    where
        TKey: num_traits::cast::AsPrimitive<usize>;

    fn max_vector_size_bytes(&self) -> usize;
}
