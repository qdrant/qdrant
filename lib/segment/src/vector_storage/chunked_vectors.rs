use std::cmp::max;
use std::collections::TryReserveError;
use std::fs::File;
use std::io::{Read, Write};
use std::mem;
use std::path::Path;

use common::types::PointOffsetType;

use super::div_ceil;
use crate::common::vector_utils::TrySetCapacityExact;

// chunk size in bytes
const CHUNK_SIZE: usize = 32 * 1024 * 1024;

// if dimension is too high, use this capacity
const MIN_CHUNK_CAPACITY: usize = 16;

pub struct ChunkedVectors<T> {
    /// Vector's dimension.
    ///
    /// Each vector will consume `size_of::<T>() * dim` bytes.
    dim: usize,
    /// Number of stored vectors in all chunks.
    len: usize,
    /// Maximum number of vectors in each chunk.
    chunk_capacity: usize,
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

    pub fn push(&mut self, vector: &[T]) -> Result<PointOffsetType, TryReserveError> {
        let new_id = self.len as PointOffsetType;
        self.insert(new_id, vector)?;
        Ok(new_id)
    }

    pub fn insert(&mut self, key: PointOffsetType, vector: &[T]) -> Result<(), TryReserveError> {
        let key = key as usize;
        self.len = max(self.len, key + 1);
        self.chunks
            .resize_with(div_ceil(self.len, self.chunk_capacity), Vec::new);

        let chunk_idx = key / self.chunk_capacity;
        let chunk_data = &mut self.chunks[chunk_idx];
        let idx = (key % self.chunk_capacity) * self.dim;

        // Grow the current chunk if needed to fit the new vector.
        //
        // All chunks are dynamically resized to fit their vectors in it.
        // Chunks have a size of zero by default. It's grown with zeroes to fit new vectors.
        //
        // The capacity for the first chunk is allocated normally to keep the memory footprint as
        // small as possible, see
        // <https://doc.rust-lang.org/std/vec/struct.Vec.html#capacity-and-reallocation>).
        // All other chunks allocate their capacity in full on first use to prevent expensive
        // reallocations when their data grows.
        if chunk_data.len() < idx + self.dim {
            // If the chunk is not the first one, allocate it fully on first use
            if chunk_idx != 0 {
                let desired_capacity = self.chunk_capacity * self.dim;
                chunk_data.try_set_capacity_exact(desired_capacity)?;
            }
            chunk_data.resize_with(idx + self.dim, T::default);
        }

        let data = &mut chunk_data[idx..idx + self.dim];
        data.copy_from_slice(vector);

        Ok(())
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
        vectors
            .try_set_capacity_exact(vectors_count)
            .map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    format!("Failed to load quantized vectors from file: {err}"),
                )
            })?;
        let mut file = File::open(path)?;
        let mut buffer = vec![0u8; quantized_vector_size];
        while file.read_exact(&mut buffer).is_ok() {
            vectors.push(&buffer).map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    format!("Failed to load quantized vectors from file: {err}"),
                )
            })?;
        }
        if vectors.len() == vectors_count {
            Ok(vectors)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Loaded vectors count {} is not equal to expected count {vectors_count}",
                    vectors.len(),
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

impl<T: Clone> TrySetCapacityExact for ChunkedVectors<T> {
    fn try_set_capacity_exact(&mut self, capacity: usize) -> Result<(), TryReserveError> {
        let num_chunks = div_ceil(capacity, self.chunk_capacity);
        let last_chunk_idx = capacity / self.chunk_capacity;
        self.chunks.try_set_capacity_exact(num_chunks)?;
        self.chunks.resize_with(num_chunks, Vec::new);
        for chunk_idx in 0..num_chunks {
            if chunk_idx == last_chunk_idx {
                let desired_capacity = (capacity % self.chunk_capacity) * self.dim;
                self.chunks[chunk_idx].try_set_capacity_exact(desired_capacity)?;
            } else {
                let desired_capacity = self.chunk_capacity * self.dim;
                self.chunks[chunk_idx].try_set_capacity_exact(desired_capacity)?;
            }
        }
        Ok(())
    }
}

impl quantization::EncodedStorageBuilder<ChunkedVectors<u8>> for ChunkedVectors<u8> {
    fn build(self) -> ChunkedVectors<u8> {
        self
    }

    fn push_vector_data(&mut self, other: &[u8]) {
        // Memory for ChunkedVectors are already pre-allocated,
        // so we do not expect any errors here.
        self.push(other).unwrap();
    }
}
