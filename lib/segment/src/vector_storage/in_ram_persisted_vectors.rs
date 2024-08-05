use std::path::{Path, PathBuf};

use common::types::PointOffsetType;
use num_traits::AsPrimitive;

use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::vector_storage::chunked_mmap_vectors::ChunkedMmapVectors;
use crate::vector_storage::chunked_vector_storage::ChunkedVectorStorage;
use crate::vector_storage::chunked_vectors::ChunkedVectors;

#[derive(Debug)]
pub struct InRamPersistedVectors<T: Sized + 'static> {
    mmap_storage: ChunkedMmapVectors<T>,
    vectors: ChunkedVectors<T>,
}

impl<T: Sized + Copy + Clone + Default + 'static> InRamPersistedVectors<T> {
    pub fn open(directory: &Path, dim: usize) -> OperationResult<Self> {
        let mmap_storage = ChunkedMmapVectors::open(directory, dim, Some(false))?;

        let mut vectors = ChunkedVectors::new(dim);

        let total_vectors = mmap_storage.len();

        for key in 0..total_vectors {
            if let Some(vector) = mmap_storage.get(key) {
                vectors.push(vector)?;
            } else {
                debug_assert!(false, "Vector not found in mmap storage");
            }
        }

        Ok(Self {
            mmap_storage,
            vectors,
        })
    }
}

impl<T: Sized + Copy + Clone + Default + 'static> ChunkedVectorStorage<T>
    for InRamPersistedVectors<T>
{
    #[inline]
    fn len(&self) -> usize {
        self.vectors.len()
    }

    #[inline]
    fn dim(&self) -> usize {
        self.mmap_storage.dim()
    }

    #[inline]
    fn get(&self, key: PointOffsetType) -> Option<&[T]> {
        self.vectors.get_opt(key)
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
    fn push(&mut self, vector: &[T]) -> OperationResult<PointOffsetType> {
        self.mmap_storage.push(vector)?;
        let key = self.vectors.push(vector)?;
        Ok(key)
    }

    #[inline]
    fn insert(&mut self, key: PointOffsetType, vector: &[T]) -> OperationResult<()> {
        self.mmap_storage.insert(key, vector)?;
        self.vectors.insert(key, vector)?;
        Ok(())
    }

    #[inline]
    fn insert_many<TKey>(
        &mut self,
        start_key: TKey,
        vectors: &[T],
        count: usize,
    ) -> OperationResult<()>
    where
        TKey: num_traits::cast::AsPrimitive<usize>,
    {
        self.mmap_storage.insert_many(start_key, vectors, count)?;
        self.vectors.insert_many(start_key, vectors, count)?;
        Ok(())
    }

    #[inline]
    fn get_many<TKey>(&self, key: TKey, count: usize) -> Option<&[T]>
    where
        TKey: AsPrimitive<usize>,
    {
        self.vectors.get_many(key, count)
    }

    #[inline]
    fn get_remaining_chunk_keys<TKey>(&self, start_key: TKey) -> usize
    where
        TKey: AsPrimitive<usize>,
    {
        self.vectors.get_chunk_left_keys(start_key)
    }

    fn max_vector_size_bytes(&self) -> usize {
        self.mmap_storage.max_vector_size_bytes()
    }
}
