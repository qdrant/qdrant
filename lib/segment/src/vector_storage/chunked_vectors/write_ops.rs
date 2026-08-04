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
        let start_key = start_key.as_();
        let (chunk_idx, chunk_offset) =
            self.inner
                .config
                .chunk_slot(start_key, count, vectors.len())?;

        // Ensure capacity
        while chunk_idx >= self.inner.chunks.len() {
            self.add_chunk()?;
        }

        let chunk = &mut self.inner.chunks[chunk_idx];

        chunk.write((chunk_offset * size_of::<T>()) as u64, vectors)?;

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
