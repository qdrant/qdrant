use std::borrow::Cow;
use std::cmp::min;
use std::ops::Range;
use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::generic_consts::Sequential;
use common::small_uint::U24;
use common::types::PointOffsetType;
use sparse::common::sparse_vector::SparseVector;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::CowMultiVector;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::types::CompactExtendedPointId;
use crate::vector_storage::{
    DenseTQVectorStorage, DenseTQVectorStorageRead, DenseVectorStorage, DenseVectorStorageRead,
    MultiTQVectorStorage, MultiTQVectorStorageRead, MultiVectorStorage, MultiVectorStorageRead,
    SparseVectorStorage, SparseVectorStorageRead, VectorStorageEnum, VectorStorageRead,
};

/// Define location of the point source during segment construction.
pub(crate) struct PointData {
    pub external_id: CompactExtendedPointId,
    /// [`CompactExtendedPointId`] is 17 bytes, we reduce
    /// `segment_index` to 3 bytes to avoid paddings and align nicely.
    pub segment_index: U24,
    pub internal_id: PointOffsetType,
    pub version: u64,
    pub ordering: u64,
}

/// Append `points` (read from `sources`) into `target`, copying each vector in
/// its native representation — no `f32` round-trip, no dequantization.
///
/// Every source must be the same vector kind and element type as `target` (the
/// storage type is fixed by the collection config, so this holds within one
/// merge); a source of a different kind/element type yields a service error.
pub(crate) fn merge_from<'a>(
    target: &mut VectorStorageEnum,
    points: &'a [PointData],
    sources: &'a [&'a VectorStorageEnum],
    stopped: &AtomicBool,
) -> OperationResult<Range<PointOffsetType>> {
    match target {
        VectorStorageEnum::DenseVolatile(target) => {
            let mut reader = BatchedReader::new(points, sources, read_dense_f32);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileByte(target) => {
            let mut reader = BatchedReader::new(points, sources, read_dense_byte);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileHalf(target) => {
            let mut reader = BatchedReader::new(points, sources, read_dense_half);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        VectorStorageEnum::DenseMemmap(target) => {
            let mut reader = BatchedReader::new(points, sources, read_dense_f32);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        VectorStorageEnum::DenseMemmapByte(target) => {
            let mut reader = BatchedReader::new(points, sources, read_dense_byte);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        VectorStorageEnum::DenseMemmapHalf(target) => {
            let mut reader = BatchedReader::new(points, sources, read_dense_half);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUring(target) => {
            let mut reader = BatchedReader::new(points, sources, read_dense_f32);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUringByte(target) => {
            let mut reader = BatchedReader::new(points, sources, read_dense_byte);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUringHalf(target) => {
            let mut reader = BatchedReader::new(points, sources, read_dense_half);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        VectorStorageEnum::DenseAppendableMemmap(target) => {
            let mut reader = BatchedReader::new(points, sources, read_dense_f32);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        VectorStorageEnum::DenseAppendableMemmapByte(target) => {
            let mut reader = BatchedReader::new(points, sources, read_dense_byte);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        VectorStorageEnum::DenseAppendableMemmapHalf(target) => {
            let mut reader = BatchedReader::new(points, sources, read_dense_half);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        VectorStorageEnum::DenseTurboMemmap(target) => {
            let mut reader = BatchedReader::new(points, sources, read_dense_tq);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseTurboUring(target) => {
            let mut reader = BatchedReader::new(points, sources, read_dense_tq);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        VectorStorageEnum::DenseTurboAppendableMemmap(target) => {
            let mut reader = BatchedReader::new(points, sources, read_dense_tq);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        VectorStorageEnum::MultiDenseTurbo(target) => {
            let mut reader = BatchedReader::new(points, sources, read_multi_tq);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        VectorStorageEnum::SparseVolatile(target) => {
            let mut reader = BatchedReader::new(points, sources, read_sparse);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        VectorStorageEnum::SparseMmap(target) => {
            let mut reader = BatchedReader::new(points, sources, read_sparse);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        VectorStorageEnum::MultiDenseVolatile(target) => {
            let mut reader = BatchedReader::new(points, sources, read_multi_f32);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        #[cfg(test)]
        VectorStorageEnum::MultiDenseVolatileByte(target) => {
            let mut reader = BatchedReader::new(points, sources, read_multi_byte);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        #[cfg(test)]
        VectorStorageEnum::MultiDenseVolatileHalf(target) => {
            let mut reader = BatchedReader::new(points, sources, read_multi_half);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        VectorStorageEnum::MultiDenseAppendableMemmap(target) => {
            let mut reader = BatchedReader::new(points, sources, read_multi_f32);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        VectorStorageEnum::MultiDenseAppendableMemmapByte(target) => {
            let mut reader = BatchedReader::new(points, sources, read_multi_byte);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        VectorStorageEnum::MultiDenseAppendableMemmapHalf(target) => {
            let mut reader = BatchedReader::new(points, sources, read_multi_half);
            let range = target.update_from(&mut reader, stopped);
            reader.finish(range)
        }
        // Empty storages are read-only placeholders: their `update_from` always
        // errors, so there is no source vector to read and no reader to build.
        VectorStorageEnum::EmptyDense(target) => {
            target.update_from(&mut std::iter::empty(), stopped)
        }
        VectorStorageEnum::EmptySparse(target) => {
            target.update_from(&mut std::iter::empty(), stopped)
        }
    }
}

/// Test-only helper: merge `count` points (offsets `0..count`) from a single
/// `source` storage into `target` via [`merge_from`].
#[cfg(test)]
pub(crate) fn merge_from_single_source(
    target: &mut VectorStorageEnum,
    source: &VectorStorageEnum,
    count: PointOffsetType,
) -> OperationResult<Range<PointOffsetType>> {
    use crate::types::PointIdType;

    let points: Vec<PointData> = (0..count)
        .map(|internal_id| PointData {
            external_id: CompactExtendedPointId::from(PointIdType::NumId(u64::from(internal_id))),
            segment_index: U24::new_wrapped(0),
            internal_id,
            version: 0,
            ordering: 0,
        })
        .collect();
    let sources = [source];
    merge_from(target, &points, &sources, &AtomicBool::default())
}

// --- internal merge machinery -------------------------------------------------

const BATCH_SIZE: usize = 256;

/// Reads a point's vector (in the native representation `V`) and its deleted
/// flag from a source storage. One reader per kind/element-type — all source
/// variants of that kind are accepted, so e.g. a volatile source can feed an
/// mmap target without an f32 round-trip. A source of the wrong kind/element
/// type yields a service error.
type ReadFn<'a, V> = fn(&'a VectorStorageEnum, PointOffsetType) -> OperationResult<(V, bool)>;

/// Batched iterator over points to insert, reading each source vector in its
/// **native** representation `V` — no `f32` round-trip.
///
/// Reads `BATCH_SIZE` points into a buffer (grouped by source segment for read
/// locality) and then iterates over them. A read error is captured in `error`
/// and ends iteration; the caller retrieves it via [`BatchedReader::finish`].
struct BatchedReader<'a, V> {
    points: &'a [PointData],
    sources: &'a [&'a VectorStorageEnum],
    read: ReadFn<'a, V>,
    buffer: Vec<Option<(V, bool)>>,
    seg_to_points_buffer: AHashMap<U24, Vec<(&'a PointData, usize)>>,
    /// First read error encountered, if any. Set => iteration stops.
    error: Option<OperationError>,
    /// Global position of the iterator.
    /// From 0 to `points.len()`.
    position: usize,
}

impl<'a, V> BatchedReader<'a, V> {
    fn new(
        points: &'a [PointData],
        sources: &'a [&'a VectorStorageEnum],
        read: ReadFn<'a, V>,
    ) -> BatchedReader<'a, V> {
        // We don't know the vector type's size, so pre-size the buffer with
        // empty slots and fill them per batch.
        let buffer = (0..BATCH_SIZE).map(|_| None).collect();

        BatchedReader {
            points,
            sources,
            read,
            buffer,
            seg_to_points_buffer: AHashMap::default(),
            error: None,
            position: 0,
        }
    }

    /// Fills the buffer with the next batch of points, stopping at the first
    /// read error.
    fn refill_buffer(&mut self) -> OperationResult<()> {
        let start_pos = self.position;
        let end_pos = min(self.position + BATCH_SIZE, self.points.len());

        // Read by segments, as we want to localize reads as much as possible.
        for pos in start_pos..end_pos {
            let point_data = &self.points[pos];
            let offset_in_batch = pos - start_pos;

            self.seg_to_points_buffer
                .entry(point_data.segment_index)
                .or_default()
                .push((point_data, offset_in_batch))
        }

        for (segment_index, points) in self.seg_to_points_buffer.drain() {
            let source = self.sources[segment_index.get() as usize];
            for (point_data, offset_in_batch) in points {
                self.buffer[offset_in_batch] = Some((self.read)(source, point_data.internal_id)?);
            }
        }

        Ok(())
    }

    /// Combine `update_from`'s result with any error captured while reading.
    ///
    /// A read error makes the source iterator end early, so `update_from`
    /// returns `Ok` with a partial range — this surfaces the real error.
    fn finish(
        self,
        range: OperationResult<Range<PointOffsetType>>,
    ) -> OperationResult<Range<PointOffsetType>> {
        match self.error {
            Some(error) => Err(error),
            None => range,
        }
    }
}

impl<'a, V> Iterator for BatchedReader<'a, V> {
    type Item = (V, bool);

    fn next(&mut self) -> Option<Self::Item> {
        if self.error.is_some() || self.position >= self.points.len() {
            return None;
        }

        if self.position.is_multiple_of(BATCH_SIZE)
            && let Err(error) = self.refill_buffer()
        {
            self.error = Some(error);
            return None;
        }

        let item = self.buffer[self.position % BATCH_SIZE]
            .take()
            .expect("buffer slot must be filled for a valid position");
        self.position += 1;

        Some(item)
    }
}

// Native source readers, one per (kind, element type). All variants of the same
// kind/element type are accepted; a source of any other kind/element type is a
// merge between incompatible storages and yields a service error.

fn read_dense_f32(
    source: &VectorStorageEnum,
    key: PointOffsetType,
) -> OperationResult<(Cow<'_, [VectorElementType]>, bool)> {
    let deleted = source.is_deleted_vector(key);
    let vector = match source {
        VectorStorageEnum::DenseVolatile(v) => v.get_dense::<Sequential>(key),
        VectorStorageEnum::DenseMemmap(v) => v.get_dense::<Sequential>(key),
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUring(v) => v.get_dense::<Sequential>(key),
        VectorStorageEnum::DenseAppendableMemmap(v) => v.get_dense::<Sequential>(key),
        VectorStorageEnum::EmptyDense(v) => v.get_dense::<Sequential>(key),
        VectorStorageEnum::DenseMemmapByte(_)
        | VectorStorageEnum::DenseMemmapHalf(_)
        | VectorStorageEnum::DenseAppendableMemmapByte(_)
        | VectorStorageEnum::DenseAppendableMemmapHalf(_)
        | VectorStorageEnum::SparseVolatile(_)
        | VectorStorageEnum::SparseMmap(_)
        | VectorStorageEnum::MultiDenseVolatile(_)
        | VectorStorageEnum::MultiDenseAppendableMemmap(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
        | VectorStorageEnum::DenseTurboMemmap(_)
        | VectorStorageEnum::DenseTurboAppendableMemmap(_)
        | VectorStorageEnum::MultiDenseTurbo(_)
        | VectorStorageEnum::EmptySparse(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a f32 dense storage",
            ));
        }
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileByte(_)
        | VectorStorageEnum::DenseVolatileHalf(_)
        | VectorStorageEnum::MultiDenseVolatileByte(_)
        | VectorStorageEnum::MultiDenseVolatileHalf(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a f32 dense storage",
            ));
        }
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUringByte(_) | VectorStorageEnum::DenseUringHalf(_) | VectorStorageEnum::DenseTurboUring(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a f32 dense storage",
            ));
        }
    };
    Ok((vector, deleted))
}

fn read_dense_byte(
    source: &VectorStorageEnum,
    key: PointOffsetType,
) -> OperationResult<(Cow<'_, [VectorElementTypeByte]>, bool)> {
    let deleted = source.is_deleted_vector(key);
    let vector = match source {
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileByte(v) => v.get_dense::<Sequential>(key),
        VectorStorageEnum::DenseMemmapByte(v) => v.get_dense::<Sequential>(key),
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUringByte(v) => v.get_dense::<Sequential>(key),
        VectorStorageEnum::DenseAppendableMemmapByte(v) => v.get_dense::<Sequential>(key),
        VectorStorageEnum::DenseVolatile(_)
        | VectorStorageEnum::DenseMemmap(_)
        | VectorStorageEnum::DenseMemmapHalf(_)
        | VectorStorageEnum::DenseAppendableMemmap(_)
        | VectorStorageEnum::DenseAppendableMemmapHalf(_)
        | VectorStorageEnum::SparseVolatile(_)
        | VectorStorageEnum::SparseMmap(_)
        | VectorStorageEnum::MultiDenseVolatile(_)
        | VectorStorageEnum::MultiDenseAppendableMemmap(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
        | VectorStorageEnum::DenseTurboMemmap(_)
        | VectorStorageEnum::DenseTurboAppendableMemmap(_)
        | VectorStorageEnum::EmptyDense(_)
        | VectorStorageEnum::MultiDenseTurbo(_)
        | VectorStorageEnum::EmptySparse(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a byte dense storage",
            ));
        }
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileHalf(_)
        | VectorStorageEnum::MultiDenseVolatileByte(_)
        | VectorStorageEnum::MultiDenseVolatileHalf(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a byte dense storage",
            ));
        }
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUring(_) | VectorStorageEnum::DenseUringHalf(_) | VectorStorageEnum::DenseTurboUring(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a byte dense storage",
            ));
        }
    };
    Ok((vector, deleted))
}

fn read_dense_half(
    source: &VectorStorageEnum,
    key: PointOffsetType,
) -> OperationResult<(Cow<'_, [VectorElementTypeHalf]>, bool)> {
    let deleted = source.is_deleted_vector(key);
    let vector = match source {
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileHalf(v) => v.get_dense::<Sequential>(key),
        VectorStorageEnum::DenseMemmapHalf(v) => v.get_dense::<Sequential>(key),
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUringHalf(v) => v.get_dense::<Sequential>(key),
        VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.get_dense::<Sequential>(key),
        VectorStorageEnum::DenseVolatile(_)
        | VectorStorageEnum::DenseMemmap(_)
        | VectorStorageEnum::DenseMemmapByte(_)
        | VectorStorageEnum::DenseAppendableMemmap(_)
        | VectorStorageEnum::DenseAppendableMemmapByte(_)
        | VectorStorageEnum::SparseVolatile(_)
        | VectorStorageEnum::SparseMmap(_)
        | VectorStorageEnum::MultiDenseVolatile(_)
        | VectorStorageEnum::MultiDenseAppendableMemmap(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
        | VectorStorageEnum::DenseTurboMemmap(_)
        | VectorStorageEnum::DenseTurboAppendableMemmap(_)
        | VectorStorageEnum::EmptyDense(_)
        | VectorStorageEnum::MultiDenseTurbo(_)
        | VectorStorageEnum::EmptySparse(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a half dense storage",
            ));
        }
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileByte(_)
        | VectorStorageEnum::MultiDenseVolatileByte(_)
        | VectorStorageEnum::MultiDenseVolatileHalf(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a half dense storage",
            ));
        }
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUring(_) | VectorStorageEnum::DenseUringByte(_) | VectorStorageEnum::DenseTurboUring(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a half dense storage",
            ));
        }
    };
    Ok((vector, deleted))
}

fn read_dense_tq(
    source: &VectorStorageEnum,
    key: PointOffsetType,
) -> OperationResult<(Cow<'_, [u8]>, bool)> {
    let deleted = source.is_deleted_vector(key);
    let vector = match source {
        VectorStorageEnum::DenseTurboMemmap(v) => v.get_dense_tq::<Sequential>(key),
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseTurboUring(v) => v.get_dense_tq::<Sequential>(key),
        VectorStorageEnum::DenseTurboAppendableMemmap(v) => v.get_dense_tq::<Sequential>(key),
        VectorStorageEnum::DenseVolatile(_)
        | VectorStorageEnum::DenseMemmap(_)
        | VectorStorageEnum::DenseMemmapByte(_)
        | VectorStorageEnum::DenseMemmapHalf(_)
        | VectorStorageEnum::DenseAppendableMemmap(_)
        | VectorStorageEnum::DenseAppendableMemmapByte(_)
        | VectorStorageEnum::DenseAppendableMemmapHalf(_)
        | VectorStorageEnum::SparseVolatile(_)
        | VectorStorageEnum::SparseMmap(_)
        | VectorStorageEnum::MultiDenseVolatile(_)
        | VectorStorageEnum::MultiDenseAppendableMemmap(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
        | VectorStorageEnum::EmptyDense(_)
        | VectorStorageEnum::MultiDenseTurbo(_)
        | VectorStorageEnum::EmptySparse(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a turbo storage",
            ));
        }
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileByte(_)
        | VectorStorageEnum::DenseVolatileHalf(_)
        | VectorStorageEnum::MultiDenseVolatileByte(_)
        | VectorStorageEnum::MultiDenseVolatileHalf(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a turbo storage",
            ));
        }
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUring(_)
        | VectorStorageEnum::DenseUringByte(_)
        | VectorStorageEnum::DenseUringHalf(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a turbo storage",
            ));
        }
    };
    Ok((vector, deleted))
}

fn read_multi_tq(
    source: &VectorStorageEnum,
    key: PointOffsetType,
) -> OperationResult<(Cow<'_, [u8]>, bool)> {
    let deleted = source.is_deleted_vector(key);
    let vector = match source {
        VectorStorageEnum::MultiDenseTurbo(v) => v.get_multi_tq::<Sequential>(key),
        VectorStorageEnum::DenseVolatile(_)
        | VectorStorageEnum::DenseMemmap(_)
        | VectorStorageEnum::DenseMemmapByte(_)
        | VectorStorageEnum::DenseMemmapHalf(_)
        | VectorStorageEnum::DenseAppendableMemmap(_)
        | VectorStorageEnum::DenseAppendableMemmapByte(_)
        | VectorStorageEnum::DenseAppendableMemmapHalf(_)
        | VectorStorageEnum::DenseTurboMemmap(_)
        | VectorStorageEnum::DenseTurboAppendableMemmap(_)
        | VectorStorageEnum::SparseVolatile(_)
        | VectorStorageEnum::SparseMmap(_)
        | VectorStorageEnum::MultiDenseVolatile(_)
        | VectorStorageEnum::MultiDenseAppendableMemmap(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
        | VectorStorageEnum::EmptyDense(_)
        | VectorStorageEnum::EmptySparse(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a multi turbo storage",
            ));
        }
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileByte(_)
        | VectorStorageEnum::DenseVolatileHalf(_)
        | VectorStorageEnum::MultiDenseVolatileByte(_)
        | VectorStorageEnum::MultiDenseVolatileHalf(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a multi turbo storage",
            ));
        }
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUring(_)
        | VectorStorageEnum::DenseUringByte(_)
        | VectorStorageEnum::DenseUringHalf(_) | VectorStorageEnum::DenseTurboUring(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a multi turbo storage",
            ));
        }
    };
    Ok((vector, deleted))
}

fn read_multi_f32(
    source: &VectorStorageEnum,
    key: PointOffsetType,
) -> OperationResult<(CowMultiVector<'_, VectorElementType>, bool)> {
    let deleted = source.is_deleted_vector(key);
    let vector = match source {
        VectorStorageEnum::MultiDenseVolatile(v) => v.get_multi::<Sequential>(key),
        VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.get_multi::<Sequential>(key),
        VectorStorageEnum::DenseVolatile(_)
        | VectorStorageEnum::DenseMemmap(_)
        | VectorStorageEnum::DenseMemmapByte(_)
        | VectorStorageEnum::DenseMemmapHalf(_)
        | VectorStorageEnum::DenseAppendableMemmap(_)
        | VectorStorageEnum::DenseAppendableMemmapByte(_)
        | VectorStorageEnum::DenseAppendableMemmapHalf(_)
        | VectorStorageEnum::SparseVolatile(_)
        | VectorStorageEnum::SparseMmap(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
        | VectorStorageEnum::DenseTurboMemmap(_)
        | VectorStorageEnum::DenseTurboAppendableMemmap(_)
        | VectorStorageEnum::EmptyDense(_)
        | VectorStorageEnum::MultiDenseTurbo(_)
        | VectorStorageEnum::EmptySparse(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a f32 multi-dense storage",
            ));
        }
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileByte(_)
        | VectorStorageEnum::DenseVolatileHalf(_)
        | VectorStorageEnum::MultiDenseVolatileByte(_)
        | VectorStorageEnum::MultiDenseVolatileHalf(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a f32 multi-dense storage",
            ));
        }
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUring(_)
        | VectorStorageEnum::DenseUringByte(_)
        | VectorStorageEnum::DenseUringHalf(_) | VectorStorageEnum::DenseTurboUring(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a f32 multi-dense storage",
            ));
        }
    };
    Ok((vector, deleted))
}

fn read_multi_byte(
    source: &VectorStorageEnum,
    key: PointOffsetType,
) -> OperationResult<(CowMultiVector<'_, VectorElementTypeByte>, bool)> {
    let deleted = source.is_deleted_vector(key);
    let vector = match source {
        #[cfg(test)]
        VectorStorageEnum::MultiDenseVolatileByte(v) => v.get_multi::<Sequential>(key),
        VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.get_multi::<Sequential>(key),
        VectorStorageEnum::DenseVolatile(_)
        | VectorStorageEnum::DenseMemmap(_)
        | VectorStorageEnum::DenseMemmapByte(_)
        | VectorStorageEnum::DenseMemmapHalf(_)
        | VectorStorageEnum::DenseAppendableMemmap(_)
        | VectorStorageEnum::DenseAppendableMemmapByte(_)
        | VectorStorageEnum::DenseAppendableMemmapHalf(_)
        | VectorStorageEnum::SparseVolatile(_)
        | VectorStorageEnum::SparseMmap(_)
        | VectorStorageEnum::MultiDenseVolatile(_)
        | VectorStorageEnum::MultiDenseAppendableMemmap(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
        | VectorStorageEnum::DenseTurboMemmap(_)
        | VectorStorageEnum::DenseTurboAppendableMemmap(_)
        | VectorStorageEnum::EmptyDense(_)
        | VectorStorageEnum::MultiDenseTurbo(_)
        | VectorStorageEnum::EmptySparse(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a byte multi-dense storage",
            ));
        }
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileByte(_)
        | VectorStorageEnum::DenseVolatileHalf(_)
        | VectorStorageEnum::MultiDenseVolatileHalf(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a byte multi-dense storage",
            ));
        }
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUring(_)
        | VectorStorageEnum::DenseUringByte(_)
        | VectorStorageEnum::DenseUringHalf(_) | VectorStorageEnum::DenseTurboUring(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a byte multi-dense storage",
            ));
        }
    };
    Ok((vector, deleted))
}

fn read_multi_half(
    source: &VectorStorageEnum,
    key: PointOffsetType,
) -> OperationResult<(CowMultiVector<'_, VectorElementTypeHalf>, bool)> {
    let deleted = source.is_deleted_vector(key);
    let vector = match source {
        #[cfg(test)]
        VectorStorageEnum::MultiDenseVolatileHalf(v) => v.get_multi::<Sequential>(key),
        VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.get_multi::<Sequential>(key),
        VectorStorageEnum::DenseVolatile(_)
        | VectorStorageEnum::DenseMemmap(_)
        | VectorStorageEnum::DenseMemmapByte(_)
        | VectorStorageEnum::DenseMemmapHalf(_)
        | VectorStorageEnum::DenseAppendableMemmap(_)
        | VectorStorageEnum::DenseAppendableMemmapByte(_)
        | VectorStorageEnum::DenseAppendableMemmapHalf(_)
        | VectorStorageEnum::SparseVolatile(_)
        | VectorStorageEnum::SparseMmap(_)
        | VectorStorageEnum::MultiDenseVolatile(_)
        | VectorStorageEnum::MultiDenseAppendableMemmap(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
        | VectorStorageEnum::DenseTurboMemmap(_)
        | VectorStorageEnum::DenseTurboAppendableMemmap(_)
        | VectorStorageEnum::EmptyDense(_)
        | VectorStorageEnum::MultiDenseTurbo(_)
        | VectorStorageEnum::EmptySparse(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a half multi-dense storage",
            ));
        }
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileByte(_)
        | VectorStorageEnum::DenseVolatileHalf(_)
        | VectorStorageEnum::MultiDenseVolatileByte(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a half multi-dense storage",
            ));
        }
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUring(_)
        | VectorStorageEnum::DenseUringByte(_)
        | VectorStorageEnum::DenseUringHalf(_) | VectorStorageEnum::DenseTurboUring(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a half multi-dense storage",
            ));
        }
    };
    Ok((vector, deleted))
}

fn read_sparse(
    source: &VectorStorageEnum,
    key: PointOffsetType,
) -> OperationResult<(Cow<'_, SparseVector>, bool)> {
    let deleted = source.is_deleted_vector(key);
    let vector = match source {
        // A deleted/absent vector reads as `None`; the placeholder is fine
        // since the deleted flag is carried separately. Real read errors
        // propagate via `?`.
        VectorStorageEnum::SparseVolatile(v) => v
            .get_sparse_opt::<Sequential>(key)?
            .map(Cow::Owned)
            .unwrap_or_default(),
        VectorStorageEnum::SparseMmap(v) => v
            .get_sparse_opt::<Sequential>(key)?
            .map(Cow::Owned)
            .unwrap_or_default(),
        VectorStorageEnum::EmptySparse(v) => v
            .get_sparse_opt::<Sequential>(key)?
            .map(Cow::Owned)
            .unwrap_or_default(),
        VectorStorageEnum::DenseVolatile(_)
        | VectorStorageEnum::DenseMemmap(_)
        | VectorStorageEnum::DenseMemmapByte(_)
        | VectorStorageEnum::DenseMemmapHalf(_)
        | VectorStorageEnum::DenseAppendableMemmap(_)
        | VectorStorageEnum::DenseAppendableMemmapByte(_)
        | VectorStorageEnum::DenseAppendableMemmapHalf(_)
        | VectorStorageEnum::MultiDenseVolatile(_)
        | VectorStorageEnum::MultiDenseAppendableMemmap(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
        | VectorStorageEnum::DenseTurboMemmap(_)
        | VectorStorageEnum::DenseTurboAppendableMemmap(_)
        | VectorStorageEnum::MultiDenseTurbo(_)
        | VectorStorageEnum::EmptyDense(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a sparse storage",
            ));
        }
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileByte(_)
        | VectorStorageEnum::DenseVolatileHalf(_)
        | VectorStorageEnum::MultiDenseVolatileByte(_)
        | VectorStorageEnum::MultiDenseVolatileHalf(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a sparse storage",
            ));
        }
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUring(_)
        | VectorStorageEnum::DenseUringByte(_)
        | VectorStorageEnum::DenseUringHalf(_) | VectorStorageEnum::DenseTurboUring(_) => {
            return Err(OperationError::service_error(
                "Cannot merge vector storage: source is not a sparse storage",
            ));
        }
    };
    Ok((vector, deleted))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Distance;
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
    use crate::vector_storage::sparse::volatile_sparse_vector_storage::new_volatile_sparse_vector_storage;

    /// Merging storages of different kinds (here sparse into dense) is rejected
    /// with a service error rather than panicking.
    #[test]
    fn merge_rejects_incompatible_storage_kinds() {
        let mut dense_target = new_volatile_dense_vector_storage(4, Distance::Dot);
        let sparse_source = new_volatile_sparse_vector_storage();

        let result = merge_from_single_source(&mut dense_target, &sparse_source, 1);

        assert!(
            result.is_err(),
            "merging a sparse source into a dense target must error, got {result:?}"
        );
    }
}
