use std::cmp::max;
use std::collections::TryReserveError;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::mem;
use std::path::Path;

use crate::common::vector_utils::{TrySetCapacity, TrySetCapacityExact};
use crate::vector_storage::chunked_vector_storage::VectorOffsetType;
use crate::vector_storage::common::CHUNK_SIZE;

#[derive(Debug)]
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
        let chunk_capacity = CHUNK_SIZE / vector_size;
        assert_ne!(chunk_capacity, 0, "The vector's size is too big");
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

    pub fn get(&self, key: VectorOffsetType) -> &[T] {
        self.get_opt(key).expect("vector not found")
    }

    pub fn get_opt(&self, key: VectorOffsetType) -> Option<&[T]> {
        if self.chunks.is_empty() {
            return None;
        }
        self.chunks
            .get(key / self.chunk_capacity)
            .and_then(|chunk_data| {
                let idx = (key % self.chunk_capacity) * self.dim;
                let range = idx..idx + self.dim;
                chunk_data.get(range)
            })
    }

    pub fn get_many(&self, key: VectorOffsetType, count: usize) -> Option<&[T]> {
        if self.chunks.is_empty() {
            return None;
        }
        self.chunks
            .get(key / self.chunk_capacity)
            .and_then(|chunk_data| {
                let idx = (key % self.chunk_capacity) * self.dim;
                let range = idx..idx + count * self.dim;
                chunk_data.get(range)
            })
    }

    pub fn push(&mut self, vector: &[T]) -> Result<VectorOffsetType, TryReserveError> {
        let new_id = self.len;
        self.insert(new_id, vector)?;
        Ok(new_id)
    }

    // returns how many flattened vectors can be inserted starting from key
    pub fn get_chunk_left_keys(&self, start_key: VectorOffsetType) -> usize {
        self.chunk_capacity - (start_key % self.chunk_capacity)
    }

    pub fn insert(&mut self, key: VectorOffsetType, vector: &[T]) -> Result<(), TryReserveError> {
        assert_eq!(vector.len(), self.dim, "Vector size mismatch");
        self.insert_many(key, vector, 1)
    }

    pub fn insert_many(
        &mut self,
        key: VectorOffsetType,
        vectors: &[T],
        vectors_count: usize,
    ) -> Result<(), TryReserveError> {
        assert_eq!(
            vectors.len(),
            vectors_count * self.dim,
            "Vector size mismatch"
        );
        assert!(
            self.get_chunk_left_keys(key) >= vectors_count,
            "Index out of bounds"
        );

        let desired_capacity = self.chunk_capacity * self.dim;
        let new_len = max(self.len, key + vectors_count);
        let chunks_len = new_len.div_ceil(self.chunk_capacity);

        if chunks_len > self.chunks.len() {
            // All chunks except the last one should be fully allocated.
            // If we are going to add new chunks, resize last one which may be partially allocated.
            if let Some(last_chunk) = self.chunks.last_mut() {
                last_chunk.try_set_capacity_exact(desired_capacity)?;
                last_chunk.resize_with(desired_capacity, T::default);
            }

            self.chunks.try_set_capacity(chunks_len)?;

            let new_chunks = chunks_len - self.chunks.len();
            let skipped_chunks = new_chunks - 1;

            // All skipped chunks should be fully allocated.
            for _ in 0..skipped_chunks {
                let mut chunk = Vec::new();
                chunk.try_set_capacity_exact(desired_capacity)?;
                chunk.resize_with(desired_capacity, T::default);
                self.chunks.push(chunk);
            }

            // Add new chunk with lower capacity.
            self.chunks.push(Default::default());
            assert_eq!(self.chunks.len(), chunks_len);
        }

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
        if chunk_data.len() < idx + vectors.len() {
            // If the chunk is not the first one, allocate it fully on first use
            if chunk_idx != 0 {
                chunk_data.try_set_capacity_exact(desired_capacity)?;
            }
            chunk_data.resize_with(idx + vectors.len(), T::default);
        }

        let data = &mut chunk_data[idx..idx + vectors.len()];
        data.copy_from_slice(vectors);

        // Update `self.len` only after the vector is successfully inserted.
        // In case of OOM, `self.len` will not be updated.
        self.len = new_len;

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
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut buffer = vec![0u8; quantized_vector_size];
        while reader.read_exact(&mut buffer).is_ok() {
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
        let num_chunks = capacity.div_ceil(self.chunk_capacity);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunked_vectors_with_skipped_chunks() {
        let mut vectors = ChunkedVectors::new(3);
        assert_eq!(vectors.get_opt(0), None);

        vectors.insert(0, &[1, 2, 3]).unwrap();
        vectors.insert(10_000_000, &[4, 5, 6]).unwrap();
        assert!(vectors.chunks.len() > 3);

        assert_eq!(vectors.get(0), &[1, 2, 3]);
        assert_eq!(vectors.get(10_000_000), &[4, 5, 6]);

        assert_eq!(vectors.get_opt(10_000_001), None);

        // check if first chunk is fully allocated
        assert_eq!(vectors.get(100), &[0, 0, 0]);

        // check if middle chunk is fully allocated
        assert_eq!(vectors.get(5_000_000), &[0, 0, 0]);
    }
}
