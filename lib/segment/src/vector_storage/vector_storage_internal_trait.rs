use std::path::PathBuf;

use common::types::PointOffsetType;

use crate::common::operation_error::OperationResult;
use crate::common::Flusher;

pub trait VectorStorageInternal<T> {
    fn len(&self) -> usize;

    fn dim(&self) -> usize;

    fn get(&self, key: PointOffsetType) -> Option<&[T]>;

    fn files(&self) -> Vec<PathBuf>;

    fn flusher(&self) -> Flusher;

    fn push(&mut self, vector: &[T]) -> OperationResult<PointOffsetType>;

    fn insert<TKey>(&mut self, key: TKey, vector: &[T]) -> OperationResult<()>
    where
        TKey: num_traits::cast::AsPrimitive<usize>;
}
