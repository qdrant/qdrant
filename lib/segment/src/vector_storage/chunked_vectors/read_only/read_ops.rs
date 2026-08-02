use std::borrow::Cow;
use std::mem::MaybeUninit;

use common::generic_consts::{AccessPattern, Random, Sequential};
use common::maybe_uninit::maybe_uninit_fill_from;
use common::types::PointOffsetType;
use common::universal_io::{ReadPipeline, ReadRange, TypedStorage, UniversalRead, UserData};
use num_traits::AsPrimitive;

use super::ChunkedVectorsRead;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::common::{PAGE_SIZE_BYTES, VECTOR_READ_BATCH_SIZE};
use crate::vector_storage::query_scorer::is_read_with_prefetch_efficient;
use crate::vector_storage::{VectorOffset, VectorOffsetType};

impl<T: bytemuck::Pod + Send, S: UniversalRead> ChunkedVectorsRead<T, S> {
    #[inline]
    pub(in crate::vector_storage::chunked_vectors) fn get_chunk_index(&self, key: usize) -> usize {
        key / self.config.chunk_size_vectors
    }

    /// Returns the byte offset of the vector in the chunk
    #[inline]
    pub(in crate::vector_storage::chunked_vectors) fn get_chunk_offset(&self, key: usize) -> usize {
        let chunk_vector_idx = key % self.config.chunk_size_vectors;
        chunk_vector_idx * self.config.dim
    }

    #[inline]
    pub fn max_vector_size_bytes(&self) -> usize {
        self.config.chunk_size_bytes
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn dim(&self) -> usize {
        self.config.dim
    }

    // returns how many vectors can be inserted starting from key
    pub fn get_remaining_chunk_keys(&self, start_key: VectorOffsetType) -> usize {
        let start_key = start_key.as_();
        let chunk_vector_idx = self.get_chunk_offset(start_key) / self.config.dim;
        self.config.chunk_size_vectors - chunk_vector_idx
    }

    #[inline]
    fn read_range(&self, offset: VectorOffsetType, count: usize) -> Option<(usize, ReadRange)> {
        if offset.checked_add(count)? > self.len {
            return None;
        }

        let chunk_idx = self.get_chunk_index(offset);
        if chunk_idx >= self.chunks.len() {
            return None;
        }

        let element_offset = self.get_chunk_offset(offset);
        let elements_length = count * self.config.dim;
        if element_offset + elements_length > self.config.chunk_size_vectors * self.config.dim {
            return None;
        }

        let range = ReadRange {
            byte_offset: (element_offset * size_of::<T>()) as u64,
            length: elements_length as u64,
        };

        Some((chunk_idx, range))
    }

    /// Returns `count` flattened vectors starting from `starting_key`.
    ///
    /// Returns `None` when:
    /// - chunk boundary is crossed
    /// - any section of `start_key..start_key + count` is out of bounds
    #[inline]
    fn get_many_impl(
        &self,
        start_key: VectorOffsetType,
        count: usize,
        force_sequential: bool,
    ) -> Option<Cow<'_, [T]>> {
        let (chunk_idx, range) = self.read_range(start_key, count)?;

        let chunk = &self.chunks[chunk_idx];

        let use_sequential =
            force_sequential || range.length as usize * size_of::<T>() > PAGE_SIZE_BYTES * 4;

        if use_sequential {
            chunk.read(range, Sequential).ok()
        } else {
            chunk.read(range, Random).ok()
        }
    }

    #[inline]
    pub fn get<P: AccessPattern>(&self, key: VectorOffsetType) -> Option<Cow<'_, [T]>> {
        self.get_many_impl(key, 1, P::IS_SEQUENTIAL)
    }

    #[inline]
    pub fn get_many<P: AccessPattern>(
        &self,
        key: VectorOffsetType,
        count: usize,
    ) -> Option<Cow<'_, [T]>> {
        self.get_many_impl(key, count, P::IS_SEQUENTIAL)
    }

    pub fn for_each_in_batch<F>(
        &self,
        keys: &[PointOffsetType],
        mut callback: F,
    ) -> OperationResult<()>
    where
        F: FnMut(usize, &[T]),
    {
        if TypedStorage::<S, T>::kind().can_be_async() {
            let point_offsets = keys
                .iter()
                .copied()
                .enumerate()
                .map(|(index, point_offset)| (index, point_offset, 1));

            return self.for_each_vector::<Random, _>(point_offsets, |idx, vectors| {
                callback(idx, vectors.as_ref());
                Ok(())
            });
        }

        // The `f` is most likely a scorer function. Fetching all vectors first, and then scoring
        // them is more cache friendly, than fetching and scoring in a single loop.

        let mut vectors_buffer = [const { MaybeUninit::uninit() }; VECTOR_READ_BATCH_SIZE];

        for (batch_idx, keys) in keys.chunks(VECTOR_READ_BATCH_SIZE).enumerate() {
            let force_sequential = is_read_with_prefetch_efficient(keys);

            let (vectors, _) = maybe_uninit_fill_from(
                &mut vectors_buffer,
                keys.iter().map(|&key| {
                    self.get_many_impl(key.offset(), 1, force_sequential)
                        .expect("vectors read")
                }),
            );

            let batch_offset = VECTOR_READ_BATCH_SIZE * batch_idx;

            for (vector_idx, vec) in vectors.iter().enumerate() {
                callback(batch_offset + vector_idx, vec.as_ref());
            }
        }

        Ok(())
    }

    /// Invoke `callback` for each flattened multi-vector at the given offsets.
    ///
    /// Drives the read pipeline directly across chunk files: refills it from the
    /// offsets, then drains completed reads.
    pub fn for_each_vector<P, U>(
        &self,
        mut offsets: impl Iterator<Item = (U, PointOffsetType, u32)>,
        mut callback: impl FnMut(U, Cow<'_, [T]>) -> OperationResult<()>,
    ) -> OperationResult<()>
    where
        P: AccessPattern,
        U: UserData,
    {
        // access pattern does not matter for io_uring
        let mut pipeline = S::ReadPipeline::<'_, U>::new()?;

        loop {
            while pipeline.can_schedule()
                && let Some((user_data, offset, count)) = offsets.next()
            {
                let (chunk_idx, range) = self
                    .read_range(offset as _, count as _)
                    .ok_or_else(|| OperationError::service_error("vector offset out of bounds"))?;
                let range = range.into_byte_range::<T>();
                pipeline.schedule::<P>(
                    user_data,
                    &self.chunks[chunk_idx].inner,
                    range,
                    align_of::<T>(),
                )?;
            }

            let Some((user_data, vector)) = pipeline.wait_bytemuck::<T>()? else {
                break;
            };
            callback(user_data, vector)?;
        }

        Ok(())
    }

    pub fn is_on_disk(&self) -> bool {
        !self.config.populate.unwrap_or(false)
    }

    pub fn heap_size_bytes(&self) -> usize {
        let Self {
            config: _,
            len: _,
            chunks: _,
            directory: _,
            advice: _,
            populate: _,
        } = self;

        0
    }
}
