//! TurboQuant-backed multivector storage.
//!
//! [`TurboMultiVectorStorage`] is the multivector counterpart of
//! [`TurboVectorStorage`](super::TurboVectorStorage): a *primary* storage that
//! keeps only TQ-encoded inner vectors, per-point offsets and deletion state.
//!
//! It intentionally implements only [`VectorStorageRead`] + [`VectorStorage`] +
//! [`MultiTQVectorStorage`], **not** `MultiVectorStorageRead<T>`.

use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::{AccessPattern, Random};
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs};
use quantization::EncodedStorage;
use quantization::turboquant::quantization::TurboQuantizer;

use super::{DELETED_PATH, TQDT_BITS, TQDT_MODE, VECTORS_PATH};
use crate::common::Flusher;
use crate::common::flags::bitvec_flags::BitvecFlags;
use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::named_vectors::{CowMultiVector, CowVector};
use crate::data_types::vectors::{
    TypedMultiDenseVector, TypedMultiDenseVectorRef, VectorElementType, VectorRef,
};
use crate::types::{Distance, MultiVectorConfig, VectorStorageDatatype};
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::MultivectorMmapOffset;
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorage;
use crate::vector_storage::{
    MultiTQVectorStorage, VectorOffsetType, VectorStorage, VectorStorageRead,
};

const OFFSETS_PATH: &str = "tq_offsets.dat";

/// Multivector storage for TurboQuant encoded inner vectors.
pub struct TurboMultiVectorStorage {
    /// Flat inner-vector space: one fixed-size encoded record per inner vector.
    storage: QuantizedChunkedStorage<MmapFile>,

    /// Maps each point to its record range in the inner space.
    offsets: ChunkedVectors<MultivectorMmapOffset, MmapFile>,

    /// Quantizer used to de/quantize.
    quantizer: TurboQuantizer,

    /// Persisted flags marking which points are soft-deleted.
    deleted: BitvecFlags<MmapFile>,
    /// Number of points currently flagged as deleted.
    deleted_count: usize,

    /// Distance used for scoring / query preprocessing.
    distance: Distance,
    /// Original (un-padded) inner vector dimensionality.
    dim: usize,

    multi_vector_config: MultiVectorConfig,

    /// Reusable scratch buffer for `TurboQuantizer::quantize`, sized to the padded dimension.
    quantization_buffer: Vec<f64>,
}

impl std::fmt::Debug for TurboMultiVectorStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurboMultiVectorStorage")
            .field("dim", &self.dim)
            .field("distance", &self.distance)
            .field("total_vector_count", &self.offsets.len())
            .field("deleted_count", &self.deleted_count)
            .finish_non_exhaustive()
    }
}

/// Open (create-or-load) an appendable TurboQuant multivector storage backed by chunked mmap files.
pub fn open_appendable_turbo_multi_vector_storage(
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    in_ram: bool,
) -> OperationResult<TurboMultiVectorStorage> {
    fs_err::create_dir_all(path)?;

    let quantizer = TurboQuantizer::new(dim, TQDT_BITS, TQDT_MODE, distance.into(), None);

    let storage = QuantizedChunkedStorage::new(
        MmapFs,
        &path.join(VECTORS_PATH),
        quantizer.quantized_size(),
        in_ram,
    )?;

    let offsets = ChunkedVectors::open(
        MmapFs,
        &path.join(OFFSETS_PATH),
        1,
        AdviceSetting::Global,
        Some(in_ram),
    )?;

    let deleted = BitvecFlags::new(
        MmapFs,
        DynamicStoredFlags::open(&MmapFs, &path.join(DELETED_PATH), in_ram)?,
    )?;
    let deleted_count = deleted.count_trues();

    let quantization_buffer = vec![0.0; quantizer.get_padded_dim()];

    Ok(TurboMultiVectorStorage {
        storage,
        offsets,
        quantizer,
        deleted,
        deleted_count,
        distance,
        dim,
        multi_vector_config,
        quantization_buffer,
    })
}

impl TurboMultiVectorStorage {
    /// Offset record for `key`, if the point exists.
    fn get_offset<P: AccessPattern>(&self, key: PointOffsetType) -> Option<MultivectorMmapOffset> {
        let record = self.offsets.get::<P>(key as VectorOffsetType)?;
        let &[offset] = record.as_ref() else {
            debug_assert!(
                false,
                "multi-vector offsets are stored as vectors of length 1"
            );
            return None;
        };
        Some(offset)
    }

    /// Set the deleted flag for `key`, keeping `deleted_count` in sync, and return the previous state.
    /// Guards on the point count, not the inner record count.
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        if !deleted && self.offsets.len() <= key as usize {
            return false;
        }

        let previous = self.deleted.set(key, deleted);
        match (previous, deleted) {
            (false, true) => self.deleted_count += 1,
            (true, false) => self.deleted_count -= 1,
            _ => {}
        }
        previous
    }

    /// Decode one inner record into `out`: dequantize, rotate back, drop the padding tail.
    fn dequantize_inner_into(&self, encoded: &[u8], out: &mut Vec<VectorElementType>) {
        let mut dequantized = self.quantizer.dequantize::<f64>(encoded);
        self.quantizer.rotation.apply_inverse(&mut dequantized);
        out.extend(
            dequantized[..self.dim]
                .iter()
                .map(|&x| x as VectorElementType),
        );
    }

    /// Decode the full multivector behind an offset record.
    fn dequantize_multi(&self, offset: MultivectorMmapOffset) -> CowVector<'_> {
        let mut flattened = Vec::with_capacity(offset.count as usize * self.dim);
        for inner_id in offset.offset..offset.offset + offset.count {
            let encoded = self.storage.get_vector_data(inner_id);
            self.dequantize_inner_into(&encoded, &mut flattened);
        }
        CowVector::MultiDense(CowMultiVector::Owned(TypedMultiDenseVector {
            flattened_vectors: flattened,
            dim: self.dim,
        }))
    }

    /// Start of a fresh range for `count` records, never straddling a chunk
    /// boundary: skips the tail when the range wouldn't fit it, errors when
    /// even a whole chunk can't hold it.
    fn fresh_range_start(&self, count: PointOffsetType) -> OperationResult<PointOffsetType> {
        let start = self.storage.vectors_count() as PointOffsetType;
        let left = self.storage.get_remaining_chunk_keys(start);
        if count as usize <= left {
            return Ok(start);
        }
        let next_chunk = start + left as PointOffsetType;
        if count as usize > self.storage.get_remaining_chunk_keys(next_chunk) {
            return Err(OperationError::service_error(format!(
                "Multivector of {count} subvectors exceeds the chunk capacity",
            )));
        }
        Ok(next_chunk)
    }

    /// Encode and upsert one multivector at `key`: reuse the existing record range
    /// in place when the new count fits its capacity, else append a fresh range.
    fn insert_multi(
        &mut self,
        key: PointOffsetType,
        multi_vector: TypedMultiDenseVectorRef<'_, VectorElementType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        assert_eq!(multi_vector.dim, self.dim);

        let count = multi_vector.vectors_count() as PointOffsetType;
        let mut offset = self
            .offsets
            .get::<Random>(key as VectorOffsetType)
            .map(|x| x.first().copied().unwrap_or_default())
            .unwrap_or_default();

        if count > offset.capacity {
            offset = MultivectorMmapOffset {
                offset: self.fresh_range_start(count)?,
                count,
                capacity: count,
            };
        } else {
            offset.count = count;
        }

        for (i, inner) in multi_vector
            .flattened_vectors
            .chunks_exact(self.dim)
            .enumerate()
        {
            let quantized = self
                .quantizer
                .quantize(inner, &mut self.quantization_buffer);
            self.storage.upsert_vector(
                offset.offset + i as PointOffsetType,
                &quantized,
                hw_counter,
            )?;
        }
        self.offsets
            .insert(key as VectorOffsetType, &[offset], hw_counter)?;
        self.set_deleted(key, false);

        Ok(())
    }

    /// Populate all pages of the encoded vectors and offsets into the page cache.
    pub fn populate(&self) -> OperationResult<()> {
        // deleted bitvec is already loaded
        self.storage.populate()?;
        self.offsets.populate()?;
        Ok(())
    }

    /// Drop the disk cache for the encoded vectors, offsets and deleted flags.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache()?;
        self.offsets.clear_cache()?;
        self.deleted.clear_cache()?;
        Ok(())
    }
}

impl VectorStorageRead for TurboMultiVectorStorage {
    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        VectorStorageDatatype::Turbo4
    }

    fn is_on_disk(&self) -> bool {
        self.storage.is_on_disk()
    }

    fn total_vector_count(&self) -> usize {
        self.offsets.len()
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.get_vector_opt::<P>(key).expect("vector not found")
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        Some(self.dequantize_multi(self.get_offset::<P>(key)?))
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.deleted.get(key)
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.get_bitslice()
    }

    fn size_of_available_vectors_in_bytes(&self) -> usize {
        if self.total_vector_count() > 0 {
            let total_size = self.storage.vectors_count() * self.quantizer.quantized_size();
            (total_size as u128 * self.available_vector_count() as u128
                / self.total_vector_count() as u128) as usize
        } else {
            0
        }
    }
}

impl VectorStorage for TurboMultiVectorStorage {
    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let multi_vector: TypedMultiDenseVectorRef<VectorElementType> = vector.try_into()?;
        self.insert_multi(key, multi_vector, hw_counter)
    }

    fn flusher(&self) -> Flusher {
        let storage_flusher = self.storage.flusher();
        let offsets_flusher = self.offsets.flusher();
        let deleted_flusher = self.deleted.flusher();

        Box::new(move || {
            storage_flusher()?;
            offsets_flusher()?;
            deleted_flusher()?;
            Ok(())
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.storage.files();
        files.extend(self.offsets.files());
        files.extend(self.deleted.files());
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = self.storage.immutable_files();
        files.extend(self.offsets.immutable_files());
        files
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        Ok(!self.set_deleted(key, true))
    }
}

impl MultiTQVectorStorage for TurboMultiVectorStorage {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn quantized_vector_size(&self) -> usize {
        self.quantizer.quantized_size()
    }

    fn multi_vector_config(&self) -> &MultiVectorConfig {
        &self.multi_vector_config
    }

    fn get_multi_tq<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        self.get_offset::<P>(key)
            .and_then(|offset| {
                self.storage
                    .get_many::<P>(offset.offset, offset.count as usize)
            })
            // `get_many` is also `None` for a range across multiple chunks, but
            // `fresh_range_start` guarantees ranges never straddle a boundary.
            .expect("Multivector not found")
    }

    fn update_from<'a>(
        &mut self,
        other_vectors: &mut impl Iterator<Item = (Cow<'a, [u8]>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let record_size = self.quantizer.quantized_size();
        let disposed_hw = HardwareCounterCell::disposable();
        let start_index = self.offsets.len() as PointOffsetType;

        for (blob, deleted) in other_vectors {
            check_process_stopped(stopped)?;
            if blob.is_empty() || blob.len() % record_size != 0 {
                return Err(OperationError::service_error(format!(
                    "Malformed multi TQ blob of {} bytes, expected a positive multiple of {record_size}",
                    blob.len(),
                )));
            }

            let count = (blob.len() / record_size) as u32;
            let key = self.offsets.len() as PointOffsetType;
            let inner_start = self.fresh_range_start(count)?;
            for (i, record) in blob.chunks_exact(record_size).enumerate() {
                self.storage.upsert_vector(
                    inner_start + i as PointOffsetType,
                    record,
                    &disposed_hw,
                )?;
            }

            let offset = MultivectorMmapOffset {
                offset: inner_start,
                count,
                capacity: count,
            };
            self.offsets
                .insert(key as VectorOffsetType, &[offset], &disposed_hw)?;
            self.set_deleted(key, deleted);
        }

        Ok(start_index..self.offsets.len() as PointOffsetType)
    }
}

#[cfg(test)]
mod tests {
    use common::generic_consts::{Random, Sequential};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::{DenseVector, MultiDenseVectorInternal};
    use crate::vector_storage::common::CHUNK_SIZE;

    /// Deterministic multivectors of unit inner vectors; point `i` gets `(i % 4) + 1` inner vectors.
    fn make_multi_vectors(dim: usize, count: usize, seed: u64) -> Vec<MultiDenseVectorInternal> {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..count)
            .map(|i| {
                let inner_count = (i % 4) + 1;
                let flattened: Vec<f32> = (0..inner_count)
                    .flat_map(|_| {
                        let v: DenseVector =
                            (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
                        let norm = v
                            .iter()
                            .map(|&x| x * x)
                            .sum::<f32>()
                            .sqrt()
                            .max(f32::MIN_POSITIVE);
                        v.into_iter().map(move |x| x / norm).collect::<Vec<_>>()
                    })
                    .collect();
                MultiDenseVectorInternal::new(flattened, dim)
            })
            .collect()
    }

    /// Cosine similarity over the leading `a.len()` components of `b`.
    fn cosine(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b).map(|(&x, &y)| x * y).sum();
        let na: f32 = a.iter().map(|&x| x * x).sum::<f32>().sqrt();
        let nb: f32 = b[..a.len()].iter().map(|&x| x * x).sum::<f32>().sqrt();
        dot / (na * nb)
    }

    /// Extract the owned multivector; this storage always returns `Owned`.
    fn to_multi(vector: CowVector) -> MultiDenseVectorInternal {
        match vector {
            CowVector::MultiDense(CowMultiVector::Owned(multi)) => multi,
            CowVector::Dense(_) | CowVector::Sparse(_) | CowVector::MultiDense(_) => {
                panic!("expected owned multi-dense vector")
            }
        }
    }

    /// Insert `vectors` at contiguous keys starting from 0.
    fn insert_all(
        storage: &mut TurboMultiVectorStorage,
        vectors: &[MultiDenseVectorInternal],
        hw: &HardwareCounterCell,
    ) {
        for (i, multi) in vectors.iter().enumerate() {
            storage
                .insert_vector(
                    i as PointOffsetType,
                    TypedMultiDenseVectorRef::from(multi).into(),
                    hw,
                )
                .unwrap();
        }
    }

    /// Independent reference codec, configured exactly like the storage's quantizer.
    struct Oracle {
        quantizer: TurboQuantizer,
        dim: usize,
    }

    impl Oracle {
        fn new(dim: usize, distance: Distance) -> Self {
            // TQDT_BITS / TQDT_MODE come from turbo/mod.rs via the production
            // `use super::{…}` + the test module's `use super::*;` (same as the dense tests).
            let quantizer = TurboQuantizer::new(dim, TQDT_BITS, TQDT_MODE, distance.into(), None);
            Self { quantizer, dim }
        }

        /// Expected encoded record per inner vector.
        fn encode_multi(&self, multi: &MultiDenseVectorInternal) -> Vec<Vec<u8>> {
            let mut buf = vec![0.0f64; self.quantizer.get_padded_dim()];
            multi
                .multi_vectors()
                .map(|inner| self.quantizer.quantize(inner, &mut buf))
                .collect()
        }

        /// Mirror of the storage's per-record decode: dequantize, rotate back, drop padding.
        fn dequantize(&self, encoded: &[u8]) -> DenseVector {
            let mut d = self.quantizer.dequantize::<f64>(encoded);
            self.quantizer.rotation.apply_inverse(&mut d);
            d[..self.dim].iter().map(|&x| x as f32).collect()
        }

        fn quantized_size(&self) -> usize {
            self.quantizer.quantized_size()
        }
    }

    const SEEDS: [u64; 6] = [42, 0xC0FFEE, 0x0BAD_C0DE, 0x0DECAF, 0x5128E, 0xD15EA5E];

    #[test]
    fn upsert_flush_reload_matches_independent_oracle() {
        const COUNT: usize = 32;
        const TOL: f32 = 2e-2;

        for seed in SEEDS {
            for dim in [1, 127, 128, 1025] {
                let distance = Distance::Dot;
                let dir = Builder::new().prefix("turbo_multi").tempdir().unwrap();
                let hw_counter = HardwareCounterCell::new();

                let oracle = Oracle::new(dim, distance);
                let inputs = make_multi_vectors(dim, COUNT, seed);
                let expected: Vec<Vec<Vec<u8>>> =
                    inputs.iter().map(|m| oracle.encode_multi(m)).collect();

                // Write path: upsert on-disk, flush, drop, so reads must round-trip through disk.
                {
                    let mut storage = open_appendable_turbo_multi_vector_storage(
                        dir.path(),
                        dim,
                        distance,
                        MultiVectorConfig::default(),
                        false,
                    )
                    .unwrap();
                    insert_all(&mut storage, &inputs, &hw_counter);
                    assert_eq!(storage.total_vector_count(), COUNT);
                    storage.flusher()().unwrap();
                }

                // Load path: reopen in RAM and verify against the oracle.
                let storage = open_appendable_turbo_multi_vector_storage(
                    dir.path(),
                    dim,
                    distance,
                    MultiVectorConfig::default(),
                    true,
                )
                .unwrap();
                assert_eq!(storage.total_vector_count(), COUNT);
                assert_eq!(storage.distance(), distance);
                assert_eq!(storage.datatype(), VectorStorageDatatype::Turbo4);

                for (i, (input, expected_records)) in inputs.iter().zip(&expected).enumerate() {
                    let key = i as PointOffsetType;
                    assert!(!storage.is_deleted_vector(key));

                    // Lossy round-trip: count and dim are exact, directions within tolerance.
                    let retrieved = to_multi(storage.get_vector::<Random>(key));
                    assert_eq!(retrieved.dim, dim, "dim mismatch at {i} (seed {seed:#x})");
                    assert_eq!(
                        retrieved.multi_vectors().count(),
                        expected_records.len(),
                        "inner count mismatch at {i} (seed {seed:#x})",
                    );
                    if dim > 1 {
                        for (inner_in, inner_out) in
                            input.multi_vectors().zip(retrieved.multi_vectors())
                        {
                            let c = cosine(inner_in, inner_out);
                            assert!(
                                (1.0 - c).abs() < TOL,
                                "direction mismatch at {i} (seed {seed:#x}, dim {dim}): cosine {c}",
                            );
                        }
                    }

                    // Exact decode agreement with the oracle, record by record.
                    let expected_flattened: Vec<f32> = expected_records
                        .iter()
                        .flat_map(|r| oracle.dequantize(r))
                        .collect();
                    assert_eq!(
                        retrieved.flattened_vectors, expected_flattened,
                        "dequantized mismatch at {i} (seed {seed:#x})",
                    );

                    // `get_vector_opt` agrees for present keys.
                    let opt = to_multi(
                        storage
                            .get_vector_opt::<Random>(key)
                            .expect("present key must be Some"),
                    );
                    assert_eq!(opt.flattened_vectors, retrieved.flattened_vectors);
                }

                assert!(
                    storage
                        .get_vector_opt::<Random>(COUNT as PointOffsetType)
                        .is_none()
                );
            }
        }
    }

    /// Build via `update_from` from oracle-encoded blobs across two batches,
    /// then reopen and verify ranges, persisted bytes, deleted flags, and
    /// zero-record placeholders.
    #[test]
    fn update_from_builds_and_matches_independent_oracle() {
        const COUNT: usize = 24;
        const DELETED: PointOffsetType = 3;
        // Deletion landing in the second batch so `start + offset` arithmetic is exercised.
        const DELETED_LATE: PointOffsetType = 20;

        for seed in SEEDS {
            for dim in [1, 127, 128] {
                let distance = Distance::Dot;
                let dir = Builder::new()
                    .prefix("turbo_multi_build")
                    .tempdir()
                    .unwrap();
                let stopped = AtomicBool::new(false);

                let oracle = Oracle::new(dim, distance);
                let inputs = make_multi_vectors(dim, COUNT, seed);
                // Concatenated expected blob per point; the deleted placeholder at
                // DELETED is a single zero record, as prefill will produce in PR 2.
                let blobs: Vec<Vec<u8>> = inputs
                    .iter()
                    .enumerate()
                    .map(|(i, m)| {
                        if i as PointOffsetType == DELETED {
                            vec![0u8; oracle.quantized_size()]
                        } else {
                            oracle.encode_multi(m).concat()
                        }
                    })
                    .collect();

                {
                    let mut storage = open_appendable_turbo_multi_vector_storage(
                        dir.path(),
                        dim,
                        distance,
                        MultiVectorConfig::default(),
                        false,
                    )
                    .unwrap();

                    let split = COUNT / 2;
                    let mut first = blobs[..split].iter().enumerate().map(|(i, blob)| {
                        (Cow::from(blob.as_slice()), i as PointOffsetType == DELETED)
                    });
                    assert_eq!(
                        storage.update_from(&mut first, &stopped).unwrap(),
                        0..(split as PointOffsetType),
                    );

                    let mut second = blobs[split..].iter().enumerate().map(|(j, blob)| {
                        let offset = (split + j) as PointOffsetType;
                        (Cow::from(blob.as_slice()), offset == DELETED_LATE)
                    });
                    assert_eq!(
                        storage.update_from(&mut second, &stopped).unwrap(),
                        (split as PointOffsetType)..(COUNT as PointOffsetType),
                    );

                    assert_eq!(storage.total_vector_count(), COUNT);
                    storage.flusher()().unwrap();
                }

                let storage = open_appendable_turbo_multi_vector_storage(
                    dir.path(),
                    dim,
                    distance,
                    MultiVectorConfig::default(),
                    true,
                )
                .unwrap();
                assert_eq!(storage.total_vector_count(), COUNT);
                assert_eq!(storage.deleted_vector_count(), 2);
                assert!(storage.is_deleted_vector(DELETED));
                assert!(storage.is_deleted_vector(DELETED_LATE));

                for (i, blob) in blobs.iter().enumerate() {
                    let key = i as PointOffsetType;
                    // Bytes round-trip verbatim, soft-deleted data included.
                    assert_eq!(
                        storage.get_multi_tq::<Random>(key).as_ref(),
                        blob.as_slice(),
                        "blob mismatch at {i} (seed {seed:#x}, dim {dim})",
                    );
                    // The placeholder decodes without panicking and keeps the dim.
                    let retrieved = to_multi(storage.get_vector::<Random>(key));
                    assert_eq!(retrieved.dim, dim);
                    assert!(retrieved.flattened_vectors.iter().all(|x| x.is_finite()));
                }
            }
        }
    }

    /// `update_from` copies one storage into a fresh one verbatim — the
    /// storage-level optimizer move.
    #[test]
    fn update_from_copies_storage_verbatim() {
        const COUNT: usize = 16;
        const DIM: usize = 128;

        for seed in SEEDS {
            let distance = Distance::Dot;
            let src_dir = Builder::new().prefix("turbo_multi_src").tempdir().unwrap();
            let dst_dir = Builder::new().prefix("turbo_multi_dst").tempdir().unwrap();
            let hw_counter = HardwareCounterCell::new();
            let stopped = AtomicBool::new(false);

            let mut src = open_appendable_turbo_multi_vector_storage(
                src_dir.path(),
                DIM,
                distance,
                MultiVectorConfig::default(),
                true,
            )
            .unwrap();
            insert_all(&mut src, &make_multi_vectors(DIM, COUNT, seed), &hw_counter);
            src.delete_vector(2).unwrap();
            src.delete_vector(7).unwrap();

            let mut dst = open_appendable_turbo_multi_vector_storage(
                dst_dir.path(),
                DIM,
                distance,
                MultiVectorConfig::default(),
                true,
            )
            .unwrap();
            {
                let mut it = (0..COUNT as PointOffsetType)
                    .map(|k| (src.get_multi_tq::<Sequential>(k), src.is_deleted_vector(k)));
                let range = dst.update_from(&mut it, &stopped).unwrap();
                assert_eq!(range, 0..COUNT as PointOffsetType);
            }

            for k in 0..COUNT as PointOffsetType {
                assert_eq!(
                    dst.get_multi_tq::<Random>(k).as_ref(),
                    src.get_multi_tq::<Random>(k).as_ref(),
                    "copy: blob diverges at {k} (seed {seed:#x})",
                );
                assert_eq!(dst.is_deleted_vector(k), src.is_deleted_vector(k));
            }
            assert_eq!(dst.deleted_vector_count(), 2);
        }
    }

    /// Blobs that are empty or not a multiple of the record size are rejected.
    #[test]
    fn update_from_rejects_malformed_blobs() {
        const DIM: usize = 128;
        let distance = Distance::Dot;
        let stopped = AtomicBool::new(false);

        let dir = Builder::new().prefix("turbo_multi_bad").tempdir().unwrap();
        let mut storage = open_appendable_turbo_multi_vector_storage(
            dir.path(),
            DIM,
            distance,
            MultiVectorConfig::default(),
            true,
        )
        .unwrap();
        let record_size = storage.quantized_vector_size();

        for bad_len in [0, 1, record_size - 1, record_size + 1] {
            let blob = vec![0u8; bad_len];
            let mut it = std::iter::once((Cow::from(blob.as_slice()), false));
            assert!(
                storage.update_from(&mut it, &stopped).is_err(),
                "blob of {bad_len} bytes must be rejected",
            );
        }
    }

    /// Helper: one multivector of `count` distinct unit inner vectors.
    fn multi_of(dim: usize, count: usize, seed: u64) -> MultiDenseVectorInternal {
        let mut rng = StdRng::seed_from_u64(seed);
        let flattened: Vec<f32> = (0..count)
            .flat_map(|_| {
                let v: DenseVector = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
                let norm = v
                    .iter()
                    .map(|&x| x * x)
                    .sum::<f32>()
                    .sqrt()
                    .max(f32::MIN_POSITIVE);
                v.into_iter().map(move |x| x / norm).collect::<Vec<_>>()
            })
            .collect();
        MultiDenseVectorInternal::new(flattened, dim)
    }

    /// Overwrites within capacity reuse the record range; growth re-appends.
    #[test]
    fn insert_reuses_capacity_in_place_and_reappends_on_growth() {
        const DIM: usize = 128;
        let distance = Distance::Dot;
        let dir = Builder::new().prefix("turbo_multi_cap").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let mut storage = open_appendable_turbo_multi_vector_storage(
            dir.path(),
            DIM,
            distance,
            MultiVectorConfig::default(),
            true,
        )
        .unwrap();
        let record_size = storage.quantized_vector_size();
        // Inner record count probe; valid while nothing is deleted.
        let inner_records =
            |s: &TurboMultiVectorStorage| s.size_of_available_vectors_in_bytes() / record_size;
        let oracle = Oracle::new(DIM, distance);
        // Byte-exact stored blob vs the oracle encoding of `m`.
        let assert_blob = |s: &TurboMultiVectorStorage, m: &MultiDenseVectorInternal| {
            assert_eq!(
                s.get_multi_tq::<Random>(0).as_ref(),
                oracle.encode_multi(m).concat().as_slice(),
            );
        };

        // count=3 allocates 3 records.
        let initial = multi_of(DIM, 3, 1);
        storage
            .insert_vector(
                0,
                TypedMultiDenseVectorRef::from(&initial).into(),
                &hw_counter,
            )
            .unwrap();
        assert_eq!(storage.total_vector_count(), 1);
        assert_eq!(inner_records(&storage), 3);
        assert_eq!(storage.get_multi_tq::<Random>(0).len(), 3 * record_size);
        assert_blob(&storage, &initial);

        // Shrink to count=2: in place, no inner-space growth, capacity stays 3,
        // and the in-place overwrite stores the new bytes.
        let shrunk = multi_of(DIM, 2, 2);
        storage
            .insert_vector(
                0,
                TypedMultiDenseVectorRef::from(&shrunk).into(),
                &hw_counter,
            )
            .unwrap();
        assert_eq!(inner_records(&storage), 3);
        assert_eq!(storage.get_multi_tq::<Random>(0).len(), 2 * record_size);
        assert_eq!(
            to_multi(storage.get_vector::<Random>(0))
                .multi_vectors()
                .count(),
            2
        );
        assert_blob(&storage, &shrunk);

        // Regrow to count=3: still within capacity, no inner-space growth.
        let regrown = multi_of(DIM, 3, 3);
        storage
            .insert_vector(
                0,
                TypedMultiDenseVectorRef::from(&regrown).into(),
                &hw_counter,
            )
            .unwrap();
        assert_eq!(inner_records(&storage), 3);
        assert_eq!(storage.get_multi_tq::<Random>(0).len(), 3 * record_size);
        assert_blob(&storage, &regrown);

        // Grow to count=5: re-append, old range becomes garbage (3 + 5 records).
        let grown = multi_of(DIM, 5, 4);
        storage
            .insert_vector(
                0,
                TypedMultiDenseVectorRef::from(&grown).into(),
                &hw_counter,
            )
            .unwrap();
        assert_eq!(storage.total_vector_count(), 1);
        assert_eq!(inner_records(&storage), 8);
        assert_eq!(storage.get_multi_tq::<Random>(0).len(), 5 * record_size);

        // The re-appended content is the new multivector, byte-exact vs the oracle.
        assert_blob(&storage, &grown);
    }

    /// Both write paths skip the chunk tail when a fresh range would straddle
    /// the boundary (mirroring the multi-dense storage), so a multivector never
    /// spans two chunks and reads stay borrowable.
    #[test]
    fn fresh_ranges_skip_chunk_tail_so_reads_borrow() {
        const DIM: usize = 128;
        let distance = Distance::Dot;
        let dir = Builder::new()
            .prefix("turbo_multi_straddle")
            .tempdir()
            .unwrap();
        let hw_counter = HardwareCounterCell::new();
        let stopped = AtomicBool::new(false);

        let mut storage = open_appendable_turbo_multi_vector_storage(
            dir.path(),
            DIM,
            distance,
            MultiVectorConfig::default(),
            true,
        )
        .unwrap();
        let record_size = storage.quantized_vector_size();
        // Mirror of the backend's chunk geometry (`ChunkedVectors`, dim = record_size).
        let records_per_chunk = CHUNK_SIZE / record_size;
        let oracle = Oracle::new(DIM, distance);

        // Fill the first chunk up to one slot before the boundary with a single
        // bulk point of raw zero records (no quantization cost).
        let filler = vec![0u8; (records_per_chunk - 1) * record_size];
        let mut it = std::iter::once((Cow::from(filler.as_slice()), false));
        storage.update_from(&mut it, &stopped).unwrap();
        assert_eq!(storage.storage.vectors_count(), records_per_chunk - 1);

        // count=3 does not fit the chunk's single remaining slot: the tail is
        // skipped and the range starts at the next chunk boundary.
        let multi = multi_of(DIM, 3, 7);
        storage
            .insert_vector(
                1,
                TypedMultiDenseVectorRef::from(&multi).into(),
                &hw_counter,
            )
            .unwrap();
        let offset = storage.get_offset::<Random>(1).unwrap();
        assert_eq!(offset.offset as usize, records_per_chunk);
        assert_eq!(offset.count, 3);
        // One padding slot at the chunk tail, never referenced by any offset.
        assert_eq!(storage.storage.vectors_count(), records_per_chunk + 3);

        // Within one chunk the getter borrows, and the bytes are exact.
        let blob = storage.get_multi_tq::<Random>(1);
        assert!(matches!(blob, Cow::Borrowed(_)));
        assert_eq!(
            blob.as_ref(),
            oracle.encode_multi(&multi).concat().as_slice(),
        );

        // `update_from` skips too: pad to one slot before the second chunk
        // boundary, then append a 3-record point that lands at the boundary.
        let pad = records_per_chunk - 4;
        let tail_skipped = multi_of(DIM, 3, 8);
        let blobs = [
            vec![0u8; pad * record_size],
            oracle.encode_multi(&tail_skipped).concat(),
        ];
        let mut it = blobs.iter().map(|b| (Cow::from(b.as_slice()), false));
        storage.update_from(&mut it, &stopped).unwrap();
        let offset = storage.get_offset::<Random>(3).unwrap();
        assert_eq!(offset.offset as usize, 2 * records_per_chunk);
        // The skipped slot at the second boundary is never referenced.
        assert_eq!(storage.storage.vectors_count(), 2 * records_per_chunk + 3);

        // The range stays within one chunk: borrowed read, exact bytes.
        let blob = storage.get_multi_tq::<Random>(3);
        assert!(matches!(blob, Cow::Borrowed(_)));
        assert_eq!(
            blob.as_ref(),
            oracle.encode_multi(&tail_skipped).concat().as_slice(),
        );
    }

    /// The largest multivector that fits one chunk is accepted; one subvector
    /// more is rejected at write time on both write paths (`fresh_range_start`).
    /// Cheap thanks to the small 512 KiB test-build `CHUNK_SIZE`.
    #[test]
    fn chunk_sized_multivector_accepted_one_larger_rejected() {
        const DIM: usize = 128;
        let distance = Distance::Dot;
        let dir = Builder::new()
            .prefix("turbo_multi_oversized")
            .tempdir()
            .unwrap();
        let hw_counter = HardwareCounterCell::new();
        let stopped = AtomicBool::new(false);

        let mut storage = open_appendable_turbo_multi_vector_storage(
            dir.path(),
            DIM,
            distance,
            MultiVectorConfig::default(),
            true,
        )
        .unwrap();
        let record_size = storage.quantized_vector_size();
        let records_per_chunk = CHUNK_SIZE / record_size;

        // Exactly one whole chunk of records is the maximum allowed.
        let max_blob = vec![0u8; records_per_chunk * record_size];
        let mut it = std::iter::once((Cow::from(max_blob.as_slice()), false));
        assert_eq!(storage.update_from(&mut it, &stopped).unwrap(), 0..1);
        let blob = storage.get_multi_tq::<Random>(0);
        assert!(matches!(blob, Cow::Borrowed(_)));
        assert_eq!(blob.as_ref(), max_blob.as_slice());

        // One subvector more cannot fit any chunk: rejected via `update_from`...
        let oversized_blob = vec![0u8; (records_per_chunk + 1) * record_size];
        let mut it = std::iter::once((Cow::from(oversized_blob.as_slice()), false));
        assert!(storage.update_from(&mut it, &stopped).is_err());

        // ...and via `insert_vector`, before any record is written.
        let oversized = multi_of(DIM, records_per_chunk + 1, 11);
        assert!(
            storage
                .insert_vector(
                    1,
                    TypedMultiDenseVectorRef::from(&oversized).into(),
                    &hw_counter,
                )
                .is_err()
        );

        // The rejected points left no trace; the accepted one is intact.
        assert_eq!(storage.total_vector_count(), 1);
        assert_eq!(storage.storage.vectors_count(), records_per_chunk);
        assert_eq!(
            storage.get_multi_tq::<Random>(0).as_ref(),
            max_blob.as_slice()
        );
    }

    /// Re-inserting a soft-deleted point clears the flag and the counter.
    #[test]
    fn reinsert_clears_deleted_flag_and_count() {
        const DIM: usize = 128;

        for seed in SEEDS {
            let distance = Distance::Dot;
            let dir = Builder::new()
                .prefix("turbo_multi_reinsert")
                .tempdir()
                .unwrap();
            let hw_counter = HardwareCounterCell::new();

            let mut storage = open_appendable_turbo_multi_vector_storage(
                dir.path(),
                DIM,
                distance,
                MultiVectorConfig::default(),
                true,
            )
            .unwrap();
            let inputs = make_multi_vectors(DIM, 2, seed);
            insert_all(&mut storage, &inputs, &hw_counter);

            assert_eq!(storage.deleted_vector_count(), 0);

            // First deletion reports "newly deleted"; the second does not.
            assert!(storage.delete_vector(0).unwrap());
            assert!(!storage.delete_vector(0).unwrap());
            assert_eq!(storage.deleted_vector_count(), 1);
            assert!(storage.is_deleted_vector(0));

            storage
                .insert_vector(
                    0,
                    TypedMultiDenseVectorRef::from(&inputs[0]).into(),
                    &hw_counter,
                )
                .unwrap();
            assert!(!storage.is_deleted_vector(0));
            assert_eq!(storage.deleted_vector_count(), 0);
            assert!(!storage.is_deleted_vector(1));
        }
    }

    /// `available_vector_count` and the proportional size estimate track deletions.
    #[test]
    fn available_count_and_size_track_deletions() {
        const DIM: usize = 128;
        const COUNT: usize = 8;

        for seed in SEEDS {
            let distance = Distance::Dot;
            let dir = Builder::new()
                .prefix("turbo_multi_avail")
                .tempdir()
                .unwrap();
            let hw_counter = HardwareCounterCell::new();

            let mut storage = open_appendable_turbo_multi_vector_storage(
                dir.path(),
                DIM,
                distance,
                MultiVectorConfig::default(),
                true,
            )
            .unwrap();
            insert_all(
                &mut storage,
                &make_multi_vectors(DIM, COUNT, seed),
                &hw_counter,
            );

            let record_size = storage.quantized_vector_size();
            // make_multi_vectors gives point i (i % 4) + 1 inner vectors.
            let total_inner: usize = (0..COUNT).map(|i| (i % 4) + 1).sum();

            assert_eq!(storage.available_vector_count(), COUNT);
            assert_eq!(
                storage.size_of_available_vectors_in_bytes(),
                total_inner * record_size,
            );

            // Soft-delete one point: total unchanged, proportional estimate shrinks.
            assert!(storage.delete_vector(0).unwrap());
            assert_eq!(storage.total_vector_count(), COUNT);
            assert_eq!(storage.available_vector_count(), COUNT - 1);
            assert_eq!(
                storage.size_of_available_vectors_in_bytes(),
                (total_inner * record_size) * (COUNT - 1) / COUNT,
            );
        }
    }

    /// A freshly created storage holds nothing: all counts zero, reads miss.
    #[test]
    fn empty_storage_reports_zero_counts_and_no_vectors() {
        const DIM: usize = 128;
        let distance = Distance::Dot;
        let dir = Builder::new()
            .prefix("turbo_multi_empty")
            .tempdir()
            .unwrap();

        let storage = open_appendable_turbo_multi_vector_storage(
            dir.path(),
            DIM,
            distance,
            MultiVectorConfig::default(),
            true,
        )
        .unwrap();

        assert_eq!(storage.total_vector_count(), 0);
        assert_eq!(storage.deleted_vector_count(), 0);
        assert_eq!(storage.available_vector_count(), 0);
        assert_eq!(storage.size_of_available_vectors_in_bytes(), 0);
        assert!(storage.get_vector_opt::<Random>(0).is_none());
        assert_eq!(storage.multi_vector_config(), &MultiVectorConfig::default());
        assert_eq!(storage.vector_dim(), DIM);
    }

    /// `read_vectors` invokes the callback once per key, threading the caller's
    /// user data and offset through, and yields the same vectors as `get_vector`.
    #[test]
    fn read_vectors_threads_user_data_and_matches_get_vector() {
        const DIM: usize = 128;
        const COUNT: usize = 8;

        for seed in SEEDS {
            let distance = Distance::Dot;
            let dir = Builder::new()
                .prefix("turbo_multi_read_batch")
                .tempdir()
                .unwrap();
            let hw_counter = HardwareCounterCell::new();

            let mut storage = open_appendable_turbo_multi_vector_storage(
                dir.path(),
                DIM,
                distance,
                MultiVectorConfig::default(),
                true,
            )
            .unwrap();
            insert_all(
                &mut storage,
                &make_multi_vectors(DIM, COUNT, seed),
                &hw_counter,
            );

            // User data is an arbitrary tag we expect echoed back beside each offset.
            let keys: Vec<(usize, PointOffsetType)> =
                (0..COUNT).map(|i| (i * 10, i as PointOffsetType)).collect();

            let mut seen: Vec<(usize, PointOffsetType, MultiDenseVectorInternal)> = Vec::new();
            storage.read_vectors::<Random, usize>(keys.iter().copied(), |tag, offset, vector| {
                seen.push((tag, offset, to_multi(vector)));
            });

            // Order is not guaranteed (the trait permits parallel reads), so check
            // each callback against its own offset, not its arrival position.
            assert_eq!(seen.len(), COUNT);
            for (tag, offset, multi) in &seen {
                // The tag paired with this offset must travel back glued to it.
                assert_eq!(
                    *tag,
                    *offset as usize * 10,
                    "user data not threaded to its offset (seed {seed:#x})"
                );
                let direct = to_multi(storage.get_vector::<Random>(*offset));
                assert_eq!(
                    multi.flattened_vectors, direct.flattened_vectors,
                    "read_vectors disagrees with get_vector (seed {seed:#x})"
                );
                assert_eq!(multi.dim, direct.dim);
            }

            // Every requested offset was visited exactly once.
            let mut offsets: Vec<PointOffsetType> = seen.iter().map(|(_, o, _)| *o).collect();
            offsets.sort_unstable();
            assert_eq!(offsets, (0..COUNT as PointOffsetType).collect::<Vec<_>>());
        }
    }

    /// One point in the reference model.
    struct Slot {
        encoded: Vec<Vec<u8>>,
        deleted: bool,
    }

    /// Random multivector with 1..=4 unit inner vectors from the scenario RNG.
    fn random_multi(rng: &mut StdRng, dim: usize) -> MultiDenseVectorInternal {
        let count = rng.random_range(1..=4usize);
        let flattened: Vec<f32> = (0..count)
            .flat_map(|_| {
                let v: DenseVector = (0..dim).map(|_| rng.random_range(-1.0f32..1.0)).collect();
                let norm = v.iter().map(|&x| x * x).sum::<f32>().sqrt();
                if norm == 0.0 {
                    let mut u = vec![0.0f32; dim];
                    u[0] = 1.0;
                    u
                } else {
                    v.into_iter().map(|x| x / norm).collect()
                }
            })
            .collect();
        MultiDenseVectorInternal::new(flattened, dim)
    }

    /// Full comparison of a storage against the reference model.
    fn assert_matches_model(
        storage: &TurboMultiVectorStorage,
        model: &[Slot],
        oracle: &Oracle,
        ctx: &str,
    ) {
        let live = model.iter().filter(|s| !s.deleted).count();
        assert_eq!(storage.total_vector_count(), model.len(), "{ctx}: total");
        assert_eq!(
            storage.deleted_vector_count(),
            model.len() - live,
            "{ctx}: deleted",
        );
        assert_eq!(storage.available_vector_count(), live, "{ctx}: available");

        for (i, slot) in model.iter().enumerate() {
            let key = i as PointOffsetType;
            assert_eq!(
                storage.is_deleted_vector(key),
                slot.deleted,
                "{ctx}: flag at {i}"
            );

            let expected_blob = slot.encoded.concat();
            let blob = storage.get_multi_tq::<Random>(key);
            // Ranges never straddle a chunk (`fresh_range_start`), so blob
            // reads always borrow.
            assert!(
                matches!(blob, Cow::Borrowed(_)),
                "{ctx}: blob not borrowed at {i}"
            );
            assert_eq!(
                blob.as_ref(),
                expected_blob.as_slice(),
                "{ctx}: blob at {i}"
            );

            let retrieved = to_multi(storage.get_vector::<Random>(key));
            let expected_flattened: Vec<f32> = slot
                .encoded
                .iter()
                .flat_map(|r| oracle.dequantize(r))
                .collect();
            assert_eq!(
                retrieved.flattened_vectors, expected_flattened,
                "{ctx}: decode at {i}"
            );
            assert_eq!(retrieved.dim, oracle.dim, "{ctx}: dim at {i}");
        }

        assert!(
            storage
                .get_vector_opt::<Random>(model.len() as PointOffsetType)
                .is_none(),
            "{ctx}: opt past end",
        );
    }

    fn run_model_scenario(dim: usize, distance: Distance, seed: u64, ops: usize) {
        let mut rng = StdRng::seed_from_u64(seed);
        let oracle = Oracle::new(dim, distance);
        let dir = Builder::new()
            .prefix("turbo_multi_model_src")
            .tempdir()
            .unwrap();
        let dst_dir = Builder::new()
            .prefix("turbo_multi_model_dst")
            .tempdir()
            .unwrap();
        let hw = HardwareCounterCell::new();
        let stopped = AtomicBool::new(false);

        let mut model: Vec<Slot> = Vec::new();
        let mut in_ram = rng.random_range(0..2) == 0;
        let mut storage = open_appendable_turbo_multi_vector_storage(
            dir.path(),
            dim,
            distance,
            MultiVectorConfig::default(),
            in_ram,
        )
        .unwrap();

        for _ in 0..ops {
            let count = model.len() as PointOffsetType;
            let op = if count == 0 {
                0
            } else {
                rng.random_range(0..100)
            };

            match op {
                // Append a new point.
                0..=34 => {
                    let multi = random_multi(&mut rng, dim);
                    storage
                        .insert_vector(count, TypedMultiDenseVectorRef::from(&multi).into(), &hw)
                        .unwrap();
                    model.push(Slot {
                        encoded: oracle.encode_multi(&multi),
                        deleted: false,
                    });
                }
                // Overwrite an existing point with a possibly different inner count.
                35..=59 => {
                    let k = rng.random_range(0..model.len());
                    let multi = random_multi(&mut rng, dim);
                    storage
                        .insert_vector(
                            k as PointOffsetType,
                            TypedMultiDenseVectorRef::from(&multi).into(),
                            &hw,
                        )
                        .unwrap();
                    model[k] = Slot {
                        encoded: oracle.encode_multi(&multi),
                        deleted: false,
                    };
                }
                // Soft-delete an existing point.
                60..=84 => {
                    let k = rng.random_range(0..model.len());
                    let was_live = !model[k].deleted;
                    assert_eq!(
                        storage.delete_vector(k as PointOffsetType).unwrap(),
                        was_live
                    );
                    model[k].deleted = true;
                }
                // Flush + drop + reload (toggling in_ram), then a full check.
                _ => {
                    storage.flusher()().unwrap();
                    drop(storage);
                    in_ram = !in_ram;
                    storage = open_appendable_turbo_multi_vector_storage(
                        dir.path(),
                        dim,
                        distance,
                        MultiVectorConfig::default(),
                        in_ram,
                    )
                    .unwrap();
                    assert_matches_model(&storage, &model, &oracle, "after reload");
                }
            }

            let live = model.iter().filter(|s| !s.deleted).count();
            assert_eq!(storage.total_vector_count(), model.len());
            assert_eq!(storage.deleted_vector_count(), model.len() - live);
        }

        storage.flusher()().unwrap();
        drop(storage);
        let storage = open_appendable_turbo_multi_vector_storage(
            dir.path(),
            dim,
            distance,
            MultiVectorConfig::default(),
            in_ram,
        )
        .unwrap();
        assert_matches_model(&storage, &model, &oracle, "source final");

        // Optimizer-style copy into a fresh storage via update_from, checked
        // live and again after a flush/drop/reload.
        let mut dst = open_appendable_turbo_multi_vector_storage(
            dst_dir.path(),
            dim,
            distance,
            MultiVectorConfig::default(),
            false,
        )
        .unwrap();
        {
            let total = storage.total_vector_count() as PointOffsetType;
            let mut it = (0..total).map(|k| {
                (
                    storage.get_multi_tq::<Sequential>(k),
                    storage.is_deleted_vector(k),
                )
            });
            assert_eq!(dst.update_from(&mut it, &stopped).unwrap(), 0..total);
        }
        assert_matches_model(&dst, &model, &oracle, "dst after copy");
        dst.flusher()().unwrap();
        drop(dst);
        let dst = open_appendable_turbo_multi_vector_storage(
            dst_dir.path(),
            dim,
            distance,
            MultiVectorConfig::default(),
            true,
        )
        .unwrap();
        assert_matches_model(&dst, &model, &oracle, "dst after reload");
    }

    const SEEDS_PER_CELL: u64 = 16;
    const OPS: usize = 200;

    #[test]
    fn turbo_multi_model_test_random_ops_dot() {
        for dim in [1usize, 4, 127, 128] {
            for seed in 0..SEEDS_PER_CELL {
                let seed = seed
                    .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    .wrapping_add(dim as u64);
                run_model_scenario(dim, Distance::Dot, seed, OPS);
            }
        }
    }

    #[test]
    fn turbo_multi_model_test_random_ops_cosine() {
        for dim in [1usize, 4, 127, 128] {
            for seed in 0..SEEDS_PER_CELL {
                let seed = seed
                    .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    .wrapping_add(dim as u64 + 1);
                run_model_scenario(dim, Distance::Cosine, seed, OPS);
            }
        }
    }

    /// `files`/`immutable_files` report the exact expected on-disk layout. Leaf
    /// names are hardcoded so a layout rename fails this test loudly.
    #[test]
    fn files_and_immutable_files_match_expected_layout() {
        const DIM: usize = 128;
        const COUNT: usize = 4; // Small enough to live in single chunks.
        const SEED: u64 = 0xF11E5;

        let distance = Distance::Dot;
        let dir = Builder::new()
            .prefix("turbo_multi_files")
            .tempdir()
            .unwrap();
        let hw_counter = HardwareCounterCell::new();

        let mut storage = open_appendable_turbo_multi_vector_storage(
            dir.path(),
            DIM,
            distance,
            MultiVectorConfig::default(),
            true,
        )
        .unwrap();
        insert_all(
            &mut storage,
            &make_multi_vectors(DIM, COUNT, SEED),
            &hw_counter,
        );

        let vectors_dir = dir.path().join("tq_vectors.dat");
        let offsets_dir = dir.path().join("tq_offsets.dat");
        let deleted_dir = dir.path().join("deleted.dat");

        // Hardcoded, not derived from the constants `files()` itself uses.
        let mut expected_files = vec![
            vectors_dir.join("config.json"),
            vectors_dir.join("status.dat"),
            vectors_dir.join("chunk_0.mmap"),
            offsets_dir.join("config.json"),
            offsets_dir.join("status.dat"),
            offsets_dir.join("chunk_0.mmap"),
            deleted_dir.join("status.dat"),
            deleted_dir.join("flags_a.dat"),
        ];
        let mut expected_immutable = vec![
            vectors_dir.join("config.json"),
            offsets_dir.join("config.json"),
        ];

        let mut files = storage.files();
        files.sort();
        expected_files.sort();
        assert_eq!(files, expected_files);

        let mut immutable = storage.immutable_files();
        immutable.sort();
        expected_immutable.sort();
        assert_eq!(immutable, expected_immutable);
    }
}
