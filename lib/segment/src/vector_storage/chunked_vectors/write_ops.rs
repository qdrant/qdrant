use std::cmp::max;

use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::UniversalWrite;
use num_traits::AsPrimitive;

use super::ChunkedVectors;
use super::chunks::create_chunk;
use crate::common::operation_error::OperationResult;
use crate::vector_storage::VectorOffsetType;

impl<T, S> ChunkedVectors<T, S>
where
    T: bytemuck::Pod + Send,
    S: UniversalWrite + Send + 'static,
{
    fn add_chunk(&mut self) -> OperationResult<()> {
        let chunk = create_chunk(
            &self.fs,
            &self.inner.directory,
            self.inner.chunks.len(),
            self.inner.config.chunk_size_bytes,
        )?;

        self.inner.chunks.push(chunk);
        Ok(())
    }

    pub fn insert(
        &mut self,
        key: VectorOffsetType,
        vector: &[T],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.insert_many(key, vector, 1, hw_counter)
    }

    #[inline]
    pub fn insert_many(
        &mut self,
        start_key: VectorOffsetType,
        vectors: &[T],
        count: usize,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        assert_eq!(
            vectors.len(),
            count * self.inner.config.dim,
            "Vector size mismatch"
        );

        let start_key = start_key.as_();

        // A run longer than the chunk's tail continues in the next one
        let mut key = start_key;
        let mut rest = vectors;
        while !rest.is_empty() {
            let chunk_idx = self.inner.config.get_chunk_index(key);
            let chunk_offset = self.inner.config.get_chunk_offset(key);

            // Ensure capacity
            while chunk_idx >= self.inner.chunks.len() {
                self.add_chunk()?;
            }

            let fits = self.inner.config.remaining_chunk_capacity(key) * self.inner.config.dim;
            let (part, tail) = rest.split_at(fits.min(rest.len()));

            let chunk = &mut self.inner.chunks[chunk_idx];
            chunk.write((chunk_offset * size_of::<T>()) as u64, part)?;

            key += part.len() / self.inner.config.dim;
            rest = tail;
        }

        hw_counter
            .vector_io_write_counter()
            .incr_delta(size_of_val(vectors));

        let new_len = max(self.status.len, start_key + count);

        if new_len > self.status.len {
            self.status.len = new_len;
            self.inner.len = new_len;
        }
        Ok(())
    }

    pub fn push(
        &mut self,
        vector: &[T],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<VectorOffsetType> {
        let new_id = self.status.len;
        self.insert(new_id, vector, hw_counter)?;
        Ok(new_id)
    }
}
