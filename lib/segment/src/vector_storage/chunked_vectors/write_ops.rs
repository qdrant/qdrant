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

        // Ensure capacity for the whole run up front, so the write loop below
        // does not need `&mut self`
        let last_key = start_key + count.saturating_sub(1);
        while self.inner.config.get_chunk_index(last_key) >= self.inner.chunks.len() {
            self.add_chunk()?;
        }

        let mut rest = vectors;
        for part in self.inner.config.split_run(start_key, count) {
            let (elements, tail) = rest.split_at(part.count * self.inner.config.dim);
            self.inner.chunks[part.chunk_idx]
                .write((part.element_offset * size_of::<T>()) as u64, elements)?;
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
