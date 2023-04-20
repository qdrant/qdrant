use std::cmp::max;
use std::fs::File;
use std::io::{Read, Write};
use std::mem;
use std::path::Path;

use super::div_ceil;
use crate::types::PointOffsetType;

// chunk size in bytes
const CHUNK_SIZE: usize = 32 * 1024 * 1024;

// if dimension is too high, use this capacity
const MIN_CHUNK_CAPACITY: usize = 16;

pub struct ChunkedVectors<T> {
    dim: usize,
    len: usize,            // amount of stored vectors
    chunk_capacity: usize, // max amount of vectors in each chunk
    chunks: Vec<Vec<T>>,
}

impl<T: Copy + Clone + Default> ChunkedVectors<T> {
    pub fn new(dim: usize) -> Self {
        assert_ne!(dim, 0, "The vector's dimension cannot be 0");
        let vector_size = dim * mem::size_of::<T>();
        let chunk_capacity = max(MIN_CHUNK_CAPACITY, CHUNK_SIZE / vector_size);
        Self {
            dim,
            len: 0,
            chunk_capacity,
            chunks: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get<TKey>(&self, key: TKey) -> &[T]
    where
        TKey: num_traits::cast::AsPrimitive<usize>,
    {
        let key: usize = key.as_();
        let chunk_data = &self.chunks[key / self.chunk_capacity];
        let idx = (key % self.chunk_capacity) * self.dim;
        &chunk_data[idx..idx + self.dim]
    }

    pub fn push(&mut self, vector: &[T]) -> PointOffsetType {
        let new_id = self.len as PointOffsetType;
        self.insert(new_id, vector);
        new_id
    }

    pub fn insert(&mut self, key: PointOffsetType, vector: &[T]) {
        let key = key as usize;
        self.len = max(self.len, key + 1);
        self.chunks
            .resize(div_ceil(self.len, self.chunk_capacity), vec![]);

        let chunk_data = &mut self.chunks[key / self.chunk_capacity];
        let idx = (key % self.chunk_capacity) * self.dim;
        if chunk_data.len() < idx + self.dim {
            chunk_data.resize(idx + self.dim, T::default());
        }
        let data = &mut chunk_data[idx..idx + self.dim];
        data.copy_from_slice(vector);
    }
}

impl quantization::EncodedStorage for ChunkedVectors<u8> {
    fn get_vector_data(&self, index: usize, _vector_size: usize) -> &[u8] {
        self.get(index)
    }

    fn from_file(
        path: &Path,
        quantized_vector_size: usize,
        vectors_count: usize,
    ) -> std::io::Result<Self> {
        let mut vectors = Self::new(quantized_vector_size);
        let mut file = File::open(path)?;
        let mut buffer = vec![0u8; quantized_vector_size];
        while file.read_exact(&mut buffer).is_ok() {
            vectors.push(&buffer);
        }
        if vectors.len() == vectors_count {
            Ok(vectors)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Loaded vectors count {} is not equal to expected count {vectors_count}",
                    vectors.len()
                ),
            ))
        }
    }

    fn save_to_file(&self, path: &Path) -> std::io::Result<()> {
        let mut buffer = File::create(path)?;
        for i in 0..self.len() {
            buffer.write_all(self.get(i))?;
        }
        buffer.flush()?;
        Ok(())
    }
}

impl quantization::EncodedStorageBuilder<ChunkedVectors<u8>> for ChunkedVectors<u8> {
    fn build(self) -> ChunkedVectors<u8> {
        self
    }

    fn push_vector_data(&mut self, other: &[u8]) {
        self.push(other);
    }
}
