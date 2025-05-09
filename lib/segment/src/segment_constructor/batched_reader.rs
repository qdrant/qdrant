use std::cmp::min;
use std::iter::Iterator;

use ahash::AHashMap;
use atomic_refcell::AtomicRef;
use common::small_uint::U24;
use common::types::PointOffsetType;

use crate::data_types::named_vectors::CowVector;
use crate::types::CompactExtendedPointId;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

const BATCH_SIZE: usize = 256;

/// Define location of the point source during segment construction.
pub struct PointData {
    pub external_id: CompactExtendedPointId,
    /// [`CompactExtendedPointId`] is 17 bytes, we reduce
    /// `segment_index` to 3 bytes to avoid paddings and align nicely.
    pub segment_index: U24,
    pub internal_id: PointOffsetType,
    pub version: u64,
    pub ordering: u64,
}

/// Batched iterator over points to insert.
/// This structure should read `BATCH_SIZE` points into a buffer,
/// and then iterate over them.
pub struct BatchedVectorReader<'a> {
    points_to_insert: &'a [PointData],
    source_vector_storages: &'a [AtomicRef<'a, VectorStorageEnum>],
    buffer: Vec<(CowVector<'a>, bool)>,
    seg_to_points_buffer: AHashMap<U24, Vec<(&'a PointData, usize)>>,
    /// Global position of the iterator.
    /// From 0 to `points_to_insert.len()`.
    position: usize,
}

impl<'a> BatchedVectorReader<'a> {
    pub fn new(
        points_to_insert: &'a [PointData],
        source_vector_storages: &'a [AtomicRef<'a, VectorStorageEnum>],
    ) -> BatchedVectorReader<'a> {
        // We need to allocate the buffer with the size of the batch,
        // but we don't know the size of the vectors.
        // So we use a placeholder vector with size 0.
        let buffer = vec![(CowVector::default(), false); BATCH_SIZE];

        BatchedVectorReader {
            points_to_insert,
            source_vector_storages,
            buffer,
            seg_to_points_buffer: AHashMap::default(),
            position: 0,
        }
    }

    /// Fills the buffer with the next batch of points.
    ///
    /// Reading of a single point looks like this:
    ///
    /// ```text
    ///  let source_vector_storage = &source_vector_storages[point_data.segment_index.get() as usize];
    ///  let vec = source_vector_storage.get_vector(point_data.internal_id);
    ///  let vector_deleted = source_vector_storage.is_deleted_vector(point_data.internal_id);
    ///  (vec, vector_deleted)
    /// ```
    fn refill_buffer(&mut self) {
        let start_pos = self.position;
        let end_pos = min(self.position + BATCH_SIZE, self.points_to_insert.len());

        // Read by segments, as we want to localize reads as much as possible.
        for pos in start_pos..end_pos {
            let point_data = &self.points_to_insert[pos];
            let offset_in_batch = pos - start_pos;

            self.seg_to_points_buffer
                .entry(point_data.segment_index)
                .or_default()
                .push((point_data, offset_in_batch))
        }

        for (segment_index, points) in self.seg_to_points_buffer.drain() {
            let source_vector_storage = &self.source_vector_storages[segment_index.get() as usize];
            for (point_data, offset_in_batch) in points {
                let vec = source_vector_storage.get_vector_sequential(point_data.internal_id);
                let vector_deleted =
                    source_vector_storage.is_deleted_vector(point_data.internal_id);
                self.buffer[offset_in_batch] = (vec, vector_deleted);
            }
        }
    }

    fn refill_buffer_if_needed(&mut self) {
        if self.position % BATCH_SIZE == 0 {
            self.refill_buffer();
        }
    }
}

impl<'a> Iterator for BatchedVectorReader<'a> {
    type Item = (CowVector<'a>, bool);

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.points_to_insert.len() {
            return None;
        }

        self.refill_buffer_if_needed();

        let item = self.buffer[self.position % BATCH_SIZE].clone();
        self.position += 1;

        Some(item)
    }
}
