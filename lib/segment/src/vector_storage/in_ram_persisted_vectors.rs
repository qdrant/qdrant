use std::path::{Path, PathBuf};

use memory::madvise::{Advice, AdviceSetting};

use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::vector_storage::chunked_mmap_vectors::ChunkedMmapVectors;
use crate::vector_storage::chunked_vector_storage::{ChunkedVectorStorage, VectorOffsetType};

#[derive(Debug)]
pub struct InRamPersistedVectors<T: Sized + 'static> {
    mmap_storage: ChunkedMmapVectors<T>,
}

impl<T: Sized + Copy + Clone + Default + 'static> InRamPersistedVectors<T> {
    pub fn open(directory: &Path, dim: usize) -> OperationResult<Self> {
        let mmap_storage = ChunkedMmapVectors::open(
            directory,
            dim,
            Some(false),
            AdviceSetting::from(Advice::Normal),
            Some(true),
        )?;
        Ok(Self { mmap_storage })
    }
}

impl<T: Sized + Copy + Clone + Default + 'static> ChunkedVectorStorage<T>
    for InRamPersistedVectors<T>
{
    #[inline]
    fn len(&self) -> usize {
        self.mmap_storage.len()
    }

    #[inline]
    fn dim(&self) -> usize {
        self.mmap_storage.dim()
    }

    #[inline]
    fn get(&self, key: VectorOffsetType) -> Option<&[T]> {
        self.mmap_storage.get(key)
    }

    #[inline]
    fn files(&self) -> Vec<PathBuf> {
        self.mmap_storage.files()
    }

    #[inline]
    fn flusher(&self) -> Flusher {
        self.mmap_storage.flusher()
    }

    #[inline]
    fn push(&mut self, vector: &[T]) -> OperationResult<VectorOffsetType> {
        self.mmap_storage.push(vector)
    }

    #[inline]
    fn insert(&mut self, key: VectorOffsetType, vector: &[T]) -> OperationResult<()> {
        self.mmap_storage.insert(key, vector)
    }

    #[inline]
    fn insert_many(
        &mut self,
        start_key: VectorOffsetType,
        vectors: &[T],
        count: usize,
    ) -> OperationResult<()> {
        self.mmap_storage.insert_many(start_key, vectors, count)
    }

    #[inline]
    fn get_many(&self, key: VectorOffsetType, count: usize) -> Option<&[T]> {
        self.mmap_storage.get_many(key, count)
    }

    #[inline]
    fn get_batch<'a>(&'a self, keys: &[VectorOffsetType], vectors: &mut [&'a [T]]) {
        self.mmap_storage.get_batch(keys, vectors)
    }

    #[inline]
    fn get_remaining_chunk_keys(&self, start_key: VectorOffsetType) -> usize {
        self.mmap_storage.get_remaining_chunk_keys(start_key)
    }

    fn max_vector_size_bytes(&self) -> usize {
        self.mmap_storage.max_vector_size_bytes()
    }

    fn is_on_disk(&self) -> bool {
        false
    }
}
