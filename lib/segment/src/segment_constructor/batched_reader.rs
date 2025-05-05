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
    /// Offset in the buffer.
    /// From 0 to `BATCH_SIZE`.
    buffer_offset: usize,
    /// Global position of the iterator.
    /// From 0 to `points_to_insert.len()`.
    position: usize,
}

impl<'a> BatchedVectorReader<'a> {
    pub fn new(
        points_to_insert: &'a [PointData],
        source_vector_storages: &'a [AtomicRef<'a, VectorStorageEnum>],
    ) -> BatchedVectorReader<'a> {
        let buffer = (0..BATCH_SIZE)
            .map(|_| {
                // We need to allocate the buffer with the size of the batch,
                // but we don't know the size of the vectors.
                // So we use a placeholder vector with size 0.
                (CowVector::default(), false)
            })
            .collect();

        let mut res = BatchedVectorReader {
            points_to_insert,
            source_vector_storages,
            buffer,
            buffer_offset: 0,
            position: 0,
        };

        res.refill_buffer();

        res
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
        let from = self.position;
        let to = min(self.position + BATCH_SIZE, self.points_to_insert.len());
        // Read by segments, as we want to localize reads as much as possible.
        let mut segment_to_points: AHashMap<U24, Vec<(&PointData, usize)>> = Default::default();

        for i in from..to {
            let point_data = &self.points_to_insert[i];
            let offset_in_batch = i - from;

            let segment_index = point_data.segment_index;
            let points = segment_to_points.entry(segment_index).or_default();
            points.push((point_data, offset_in_batch));
        }

        for (segment_index, points) in segment_to_points {
            let source_vector_storage = &self.source_vector_storages[segment_index.get() as usize];
            // ToDo: Introduce batch operation for reading vectors
            for (point_data, offset_in_batch) in points {
                let vec = source_vector_storage.get_vector(point_data.internal_id);
                let vector_deleted =
                    source_vector_storage.is_deleted_vector(point_data.internal_id);
                self.buffer[offset_in_batch] = (vec, vector_deleted);
            }
        }
    }
}

impl<'a> Iterator for BatchedVectorReader<'a> {
    type Item = (CowVector<'a>, bool);

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.points_to_insert.len() {
            return None;
        }

        if self.buffer_offset == BATCH_SIZE {
            self.refill_buffer();
            self.buffer_offset = 0;
        }

        let item = self.buffer[self.buffer_offset].clone();
        self.buffer_offset += 1;
        self.position += 1;

        Some(item)
    }
}
