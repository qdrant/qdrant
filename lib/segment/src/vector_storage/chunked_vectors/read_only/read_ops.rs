use std::borrow::Cow;
use std::collections::VecDeque;
use std::mem::MaybeUninit;

use ahash::AHashMap;
use common::generic_consts::{AccessPattern, Random, Sequential};
use common::maybe_uninit::maybe_uninit_fill_from;
use common::types::PointOffsetType;
use common::universal_io::{ReadPipeline, ReadRange, TypedStorage, UniversalRead, UserData};

use super::ReadOnlyChunkedVectors;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::chunked_vectors::config::RunPart;
use crate::vector_storage::common::{PAGE_SIZE_BYTES, VECTOR_READ_BATCH_SIZE};
use crate::vector_storage::query_scorer::is_read_with_prefetch_efficient;
use crate::vector_storage::{VectorOffset, VectorOffsetType};

/// A run whose parts are still arriving from the read pipeline.
struct SplitRun<'a, U, T: Clone> {
    user_data: U,
    landed: Vec<Option<Cow<'a, [T]>>>,
    missing: usize,
}

impl<'a, U, T: Clone> SplitRun<'a, U, T> {
    fn new(user_data: U, parts: usize) -> Self {
        Self {
            user_data,
            landed: (0..parts).map(|_| None).collect(),
            missing: parts,
        }
    }

    /// The whole run, in order. Every part must have landed.
    fn stitch(self) -> (U, Cow<'a, [T]>) {
        let mut stitched = Vec::new();
        for part in self.landed {
            stitched.extend_from_slice(&part.expect("every part landed"));
        }

        (self.user_data, Cow::Owned(stitched))
    }
}

impl<T: bytemuck::Pod + Send, S: UniversalRead> ReadOnlyChunkedVectors<T, S> {
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

    /// Per-chunk parts of `offset..offset + count`, in order.
    ///
    /// More than one part when the run straddles a chunk boundary.
    fn read_ranges(
        &self,
        offset: VectorOffsetType,
        count: usize,
    ) -> Option<impl ExactSizeIterator<Item = (usize, ReadRange)> + '_> {
        if offset.checked_add(count)? > self.len {
            return None;
        }

        Some(self.config.split_run(offset, count).map(|part| {
            let RunPart {
                chunk_idx,
                element_offset,
                count,
            } = part;

            let range = ReadRange {
                byte_offset: (element_offset * size_of::<T>()) as u64,
                length: (count * self.config.dim) as u64,
            };

            (chunk_idx, range)
        }))
    }

    fn read_part(
        &self,
        chunk_idx: usize,
        range: ReadRange,
        force_sequential: bool,
    ) -> Option<Cow<'_, [T]>> {
        let chunk = self.chunks.get(chunk_idx)?;

        let use_sequential =
            force_sequential || range.length as usize * size_of::<T>() > PAGE_SIZE_BYTES * 4;

        if use_sequential {
            chunk.read(range, Sequential).ok()
        } else {
            chunk.read(range, Random).ok()
        }
    }

    /// Returns `count` flattened vectors starting from `starting_key`.
    ///
    /// Borrows a single chunk, or copies when the run straddles a boundary.
    /// Returns `None` when any section of `start_key..start_key + count` is
    /// out of bounds.
    #[inline]
    fn get_many_impl(
        &self,
        start_key: VectorOffsetType,
        count: usize,
        force_sequential: bool,
    ) -> Option<Cow<'_, [T]>> {
        let mut parts = self.read_ranges(start_key, count)?;

        let (chunk_idx, range) = parts.next()?;
        let mut vectors = self.read_part(chunk_idx, range, force_sequential)?;

        for (chunk_idx, range) in parts {
            let part = self.read_part(chunk_idx, range, force_sequential)?;
            vectors.to_mut().extend_from_slice(&part);
        }

        Some(vectors)
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
    /// offsets, then drains completed reads. A run spanning several chunks is
    /// one scheduled read per chunk, stitched once the last one lands.
    pub fn for_each_vector<P, U>(
        &self,
        mut offsets: impl Iterator<Item = (U, PointOffsetType, u32)>,
        mut callback: impl FnMut(U, Cow<'_, [T]>) -> OperationResult<()>,
    ) -> OperationResult<()>
    where
        P: AccessPattern,
        U: UserData,
    {
        /// What a scheduled read carries back, so its completion knows what it is.
        #[derive(Debug)]
        enum ReadTag<U> {
            /// The run takes this one read, and the caller's data rides along
            /// with it
            Whole(U),
            /// One part of a run taking several reads, filed under `run` until
            /// the others land
            Part { run: u32, index: u32 },
        }

        let out_of_bounds = || OperationError::service_error("vector offset out of bounds");

        // access pattern does not matter for io_uring
        let mut pipeline = S::ReadPipeline::<'_, ReadTag<U>>::new()?;

        // A run resolves to as many reads as it covers chunks, which can be more
        // than the pipeline has room for, so they wait here
        let mut queued: VecDeque<(ReadTag<U>, usize, ReadRange)> = VecDeque::new();
        // Stays empty unless a run straddles a chunk boundary
        let mut split_runs: AHashMap<u32, SplitRun<'_, U, T>> = AHashMap::new();
        let mut next_run: u32 = 0;

        loop {
            while pipeline.can_schedule() {
                // Parts of a split run that had no room last time go first
                let (tag, chunk_idx, range) = match queued.pop_front() {
                    Some(read) => read,
                    None => {
                        let Some((user_data, offset, count)) = offsets.next() else {
                            break;
                        };

                        let mut ranges = self
                            .read_ranges(offset as _, count as _)
                            .ok_or_else(out_of_bounds)?;

                        // A run across chunks is queued whole and taken from the
                        // top on the next turns
                        if ranges.len() > 1 {
                            let run = next_run;
                            next_run = run.wrapping_add(1);
                            split_runs.insert(run, SplitRun::new(user_data, ranges.len()));

                            queued.extend(ranges.enumerate().map(|(index, (chunk_idx, range))| {
                                let index = index as u32;
                                (ReadTag::Part { run, index }, chunk_idx, range)
                            }));
                            continue;
                        }

                        let (chunk_idx, range) = ranges.next().ok_or_else(out_of_bounds)?;
                        (ReadTag::Whole(user_data), chunk_idx, range)
                    }
                };

                let chunk = self.chunks.get(chunk_idx).ok_or_else(out_of_bounds)?;
                pipeline.schedule::<P>(
                    tag,
                    &chunk.inner,
                    range.into_byte_range::<T>(),
                    align_of::<T>(),
                )?;
            }

            let Some((tag, vectors)) = pipeline.wait_bytemuck::<T>()? else {
                debug_assert!(queued.is_empty(), "scheduling left reads behind");
                debug_assert!(split_runs.is_empty(), "a run never got all its parts");
                break;
            };

            let (user_data, vectors) = match tag {
                ReadTag::Whole(user_data) => (user_data, vectors),
                ReadTag::Part { run, index } => {
                    let split = split_runs.get_mut(&run).expect("part of an in-flight run");
                    split.landed[index as usize] = Some(vectors);
                    split.missing -= 1;

                    if split.missing > 0 {
                        continue;
                    }

                    split_runs.remove(&run).expect("just filed").stitch()
                }
            };

            callback(user_data, vectors)?;
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
