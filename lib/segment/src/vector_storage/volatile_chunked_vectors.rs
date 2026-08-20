use std::borrow::Cow;
use std::cmp::max;
use std::collections::TryReserveError;
use std::mem;

use crate::common::vector_utils::{TrySetCapacity, TrySetCapacityExact};
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::common::CHUNK_SIZE;

#[derive(Debug)]
pub struct VolatileChunkedVectors<T> {
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

impl<T: Copy + Clone + Default> VolatileChunkedVectors<T> {
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

    /// Shrink the last chunk's allocation to match its used length.
    /// Call after bulk loading is complete to avoid wasting up to CHUNK_SIZE
    /// bytes in the last partially-filled chunk.
    pub fn shrink_last_chunk(&mut self) {
        if let Some(last) = self.chunks.last_mut() {
            last.shrink_to_fit();
        }
    }

    /// Total heap bytes allocated by the chunks (capacity, not just used length).
    pub fn heap_size_bytes(&self) -> usize {
        let Self {
            dim: _,
            len: _,
            chunk_capacity: _,
            chunks,
        } = self;
        chunks.capacity() * mem::size_of::<Vec<T>>()
            + chunks
                .iter()
                .map(|chunk| chunk.capacity() * mem::size_of::<T>())
                .sum::<usize>()
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

    /// Borrows a single chunk, or copies when the run straddles a boundary.
    pub fn get_many(&self, key: VectorOffsetType, count: usize) -> Option<Cow<'_, [T]>> {
        let mut key = key;
        let mut left = count;
        let mut vectors: Option<Cow<'_, [T]>> = None;

        loop {
            let chunk_data = self.chunks.get(key / self.chunk_capacity)?;
            let idx = (key % self.chunk_capacity) * self.dim;
            let part_count = left.min(self.chunk_left_keys(key));
            let part = chunk_data.get(idx..idx + part_count * self.dim)?;

            vectors = Some(match vectors {
                None => Cow::Borrowed(part),
                Some(mut vectors) => {
                    vectors.to_mut().extend_from_slice(part);
                    vectors
                }
            });

            key += part_count;
            left -= part_count;

            if left == 0 {
                return vectors;
            }
        }
    }

    pub fn push(&mut self, vector: &[T]) -> Result<VectorOffsetType, TryReserveError> {
        let new_id = self.len;
        self.insert(new_id, vector)?;
        Ok(new_id)
    }

    /// How many vectors still fit in the chunk holding `start_key`.
    fn chunk_left_keys(&self, start_key: VectorOffsetType) -> usize {
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

        // A run longer than the chunk's tail continues in the next one
        let mut key = key;
        let mut rest = vectors;
        while !rest.is_empty() {
            let chunk_idx = key / self.chunk_capacity;
            let idx = (key % self.chunk_capacity) * self.dim;
            let fits = self.chunk_left_keys(key) * self.dim;
            let (part, tail) = rest.split_at(fits.min(rest.len()));

            let chunk_data = &mut self.chunks[chunk_idx];

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
            if chunk_data.len() < idx + part.len() {
                // If the chunk is not the first one, allocate it fully on first use
                if chunk_idx != 0 {
                    chunk_data.try_set_capacity_exact(desired_capacity)?;
                }
                chunk_data.resize_with(idx + part.len(), T::default);
            }

            chunk_data[idx..idx + part.len()].copy_from_slice(part);

            key += part.len() / self.dim;
            rest = tail;
        }

        // Update `self.len` only after the vector is successfully inserted.
        // In case of OOM, `self.len` will not be updated.
        self.len = new_len;

        Ok(())
    }

    /// Append all flattened vectors in `vectors` to the end of the storage.
    ///
    /// `vectors` holds `vectors.len() / dim` consecutive vectors.
    pub fn extend(&mut self, vectors: &[T]) -> Result<(), TryReserveError> {
        assert!(
            vectors.len().is_multiple_of(self.dim),
            "Vector data size mismatch"
        );

        self.insert_many(self.len, vectors, vectors.len() / self.dim)
    }
}

impl<T: Clone> TrySetCapacityExact for VolatileChunkedVectors<T> {
    fn try_set_capacity_exact(&mut self, capacity: usize) -> Result<(), TryReserveError> {
        let num_chunks = capacity.div_ceil(self.chunk_capacity);
        let last_chunk_idx = num_chunks.saturating_sub(1);
        self.chunks.try_set_capacity_exact(num_chunks)?;
        self.chunks.resize_with(num_chunks, Vec::new);
        for chunk_idx in 0..num_chunks {
            if chunk_idx == last_chunk_idx {
                let remainder = capacity % self.chunk_capacity;
                let desired_capacity = if remainder == 0 {
                    self.chunk_capacity * self.dim
                } else {
                    remainder * self.dim
                };
                self.chunks[chunk_idx].try_set_capacity_exact(desired_capacity)?;
            } else {
                let desired_capacity = self.chunk_capacity * self.dim;
                self.chunks[chunk_idx].try_set_capacity_exact(desired_capacity)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::*;
    use crate::vector_storage::common::CHUNK_SIZE;

    #[test]
    fn test_chunked_vectors_with_skipped_chunks() {
        let mut vectors = VolatileChunkedVectors::new(3);
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

    #[test]
    fn test_try_set_capacity_exact_zero_does_not_panic() {
        let mut vectors = VolatileChunkedVectors::<u8>::new(3);
        vectors.try_set_capacity_exact(0).unwrap();
        assert!(vectors.chunks.is_empty());
        assert_eq!(vectors.len(), 0);
    }

    /// Regression: when `capacity` is a multiple of `chunk_capacity`, the last chunk must still
    /// reserve `chunk_capacity * dim` flattened elements. Using only `(capacity % chunk_capacity) * dim`
    /// yields zero in that case and leaves the last chunk with no reserved capacity.
    #[test]
    fn try_set_capacity_exact_exact_multiple_of_chunk_reserves_full_last_chunk() {
        let dim = 3;
        let chunk_capacity = CHUNK_SIZE / (dim * mem::size_of::<u8>());
        let mut vectors = VolatileChunkedVectors::<u8>::new(dim);

        vectors
            .try_set_capacity_exact(chunk_capacity)
            .expect("single full chunk");
        assert_eq!(vectors.chunks.len(), 1);
        assert_eq!(vectors.chunks[0].capacity(), chunk_capacity * dim);

        vectors
            .try_set_capacity_exact(2 * chunk_capacity)
            .expect("two full chunks");
        assert_eq!(vectors.chunks.len(), 2);
        assert_eq!(vectors.chunks[0].capacity(), chunk_capacity * dim);
        assert_eq!(vectors.chunks[1].capacity(), chunk_capacity * dim);
    }
}
