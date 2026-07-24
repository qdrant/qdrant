//! Single-file (non-appendable) TurboQuant dense vector storage.
//!
//! [`TurboVectorStorageImpl`] keeps the TQ-encoded vectors in one file read
//! through `S` (mmap or io_uring) plus its own mutable deletion flags. It is the
//! counterpart of the immutable dense
//! [`DenseVectorStorageImpl`](crate::vector_storage::dense::dense_vector_storage::DenseVectorStorageImpl):
//! the vector data is append-only (`insert_vector` is unsupported; bulk build
//! goes through [`DenseTQVectorStorage::update_from`]), while deletions stay
//! writable.

use std::borrow::Cow;
use std::io::{self, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::types::{PointOffsetType, ScoreType};
#[cfg(target_os = "linux")]
use common::universal_io::{IoUringFile, IoUringFs};
use common::universal_io::{MmapFile, MmapFs, Populate, UniversalRead, UserData};
use quantization::EncodedStorage;
use quantization::turboquant::EncodedQueryTQ;
use quantization::turboquant::quantization::TurboQuantizer;

use super::shared::{self, DELETED_PATH, VECTORS_PATH};
use crate::common::Flusher;
use crate::common::flags::bitvec_flags::BitvecFlags;
use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{DenseVector, VectorElementType, VectorRef};
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;
use crate::vector_storage::{
    DenseTQVectorStorage, DenseTQVectorStorageRead, TurboScoring, VectorStorage, VectorStorageEnum,
    VectorStorageRead,
};

/// Single-file TurboQuant dense vector storage over a [`UniversalRead`] backend
/// `S` (mmap by default, io_uring on Linux). Non-appendable data, mutable
/// deletions.
pub struct TurboVectorStorageImpl<S: UniversalRead = MmapFile> {
    /// Single-file encoded vectors, read through `S`.
    storage: QuantizedStorage<S>,

    /// The filesystem `storage` was opened through, needed to re-open it after a
    /// bulk append in [`DenseTQVectorStorage::update_from`].
    fs: S::Fs,

    /// Quantizer used to de/quantize.
    quantizer: TurboQuantizer,

    /// Persisted flags marking which vectors are soft-deleted.
    deleted: BitvecFlags<MmapFile>,
    /// Number of vectors currently flagged as deleted.
    deleted_count: usize,

    /// Distance used for scoring / query preprocessing.
    distance: Distance,
    /// Original (un-padded) vector dimensionality.
    dim: usize,

    /// Reusable scratch buffer for the padded, rotated working vector that
    /// `TurboQuantizer::quantize` writes into. Sized to the padded dimension.
    quantization_buffer: Vec<f64>,
}

/// Open (create-or-load) a single-file TurboQuant vector storage (non-appendable),
/// wrapped into the right [`VectorStorageEnum`] variant. Counterpart to
/// `open_dense_vector_storage`: reads go through io_uring when the async scorer is
/// enabled (Linux), plain mmap otherwise.
pub fn open_turbo_vector_storage(
    path: &Path,
    dim: usize,
    distance: Distance,
    populate: bool,
) -> OperationResult<VectorStorageEnum> {
    #[cfg(target_os = "linux")]
    let with_uring = crate::vector_storage::common::get_async_scorer();

    #[cfg(not(target_os = "linux"))]
    let with_uring = false;

    open_turbo_vector_storage_with_uring(path, dim, distance, populate, with_uring)
}

/// [`open_turbo_vector_storage`] with an explicit backend choice instead of the
/// global async-scorer flag. Falls back to mmap (with an error log) if the
/// io_uring backend cannot be opened.
pub fn open_turbo_vector_storage_with_uring(
    path: &Path,
    dim: usize,
    distance: Distance,
    populate: bool,
    with_uring: bool,
) -> OperationResult<VectorStorageEnum> {
    // prevent "unused variable" warning
    let _ = with_uring;

    #[cfg(target_os = "linux")]
    if with_uring {
        match TurboVectorStorageImpl::<IoUringFile>::open_uring(path, dim, distance, populate) {
            Ok(storage) => return Ok(VectorStorageEnum::DenseTurboUring(Box::new(storage))),
            Err(err) => {
                log::error!("Failed to open io_uring based TurboQuant storage: {err}");
            }
        }
    }

    let storage = TurboVectorStorageImpl::<MmapFile>::open_mmap(path, dim, distance, populate)?;
    Ok(VectorStorageEnum::DenseTurboMemmap(Box::new(storage)))
}

impl TurboVectorStorageImpl<MmapFile> {
    /// Open (create-or-load) the mem-mapped single-file backend.
    pub fn open_mmap(
        path: &Path,
        dim: usize,
        distance: Distance,
        populate: bool,
    ) -> OperationResult<Self> {
        let quantizer = shared::build_quantizer(dim, distance);
        let storage = QuantizedStorage::<MmapFile>::open(
            &MmapFs,
            &path.join(VECTORS_PATH),
            quantizer.quantized_size(),
            populate,
        )?;
        Self::finalize(storage, MmapFs, quantizer, path, dim, distance, populate)
    }
}

#[cfg(target_os = "linux")]
impl TurboVectorStorageImpl<IoUringFile> {
    /// Open (create-or-load) the single-file io_uring backend.
    pub fn open_uring(
        path: &Path,
        dim: usize,
        distance: Distance,
        populate: bool,
    ) -> OperationResult<Self> {
        let quantizer = shared::build_quantizer(dim, distance);
        let storage = QuantizedStorage::<IoUringFile>::open(
            &IoUringFs,
            &path.join(VECTORS_PATH),
            quantizer.quantized_size(),
            populate,
        )?;
        Self::finalize(storage, IoUringFs, quantizer, path, dim, distance, populate)
    }
}

impl<S: UniversalRead> TurboVectorStorageImpl<S> {
    /// Shared tail of the backend-specific `open_*` constructors: open the
    /// deletion flags and assemble the storage.
    fn finalize(
        storage: QuantizedStorage<S>,
        fs: S::Fs,
        quantizer: TurboQuantizer,
        path: &Path,
        dim: usize,
        distance: Distance,
        populate: bool,
    ) -> OperationResult<Self> {
        fs_err::create_dir_all(path)?;
        let deleted = BitvecFlags::new(
            MmapFs,
            DynamicStoredFlags::open(&MmapFs, &path.join(DELETED_PATH), Populate::from(populate))?,
        )?;
        let deleted_count = deleted.count_trues();
        let quantization_buffer = vec![0.0; quantizer.get_padded_dim()];

        Ok(Self {
            storage,
            fs,
            quantizer,
            deleted,
            deleted_count,
            distance,
            dim,
            quantization_buffer,
        })
    }

    /// Raw encoded vector blob for one vector (no dequantization).
    pub fn get_quantized_vector(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        self.storage.get_vector_data(key)
    }

    /// Populate all pages of the encoded vectors into the page cache.
    pub fn populate(&self) -> OperationResult<()> {
        // deleted bitvec is already loaded
        self.storage.populate();
        Ok(())
    }

    /// Drop the disk cache for the encoded vectors and deleted flags.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache();
        self.deleted.clear_cache()?;
        Ok(())
    }

    /// Upsert one vector from its already-encoded TurboQuant bytes, verbatim.
    /// The single-file backend is not appendable, so `upsert_vector` rejects the
    /// write with an unsupported error and nothing is mutated.
    pub(crate) fn insert_tq_bytes(
        &mut self,
        key: PointOffsetType,
        bytes: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let expected_size = self.quantizer.quantized_size();
        if bytes.len() != expected_size {
            return Err(OperationError::malformed_vector_blob(format!(
                "Malformed dense TQ blob of {} bytes, expected {expected_size}",
                bytes.len(),
            )));
        }
        self.storage.upsert_vector(key, bytes, hw_counter)?;
        self.set_deleted(key, false);
        Ok(())
    }

    /// Set the deleted flag for `key`, keeping `deleted_count` in sync, and
    /// return the previous state.
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        if !deleted && self.storage.vectors_count() <= key as usize {
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

    /// Preprocess a raw query for this storage's distance and precompute its
    /// asymmetric-scoring encoding.
    pub fn preprocess_query(&self, query: DenseVector) -> EncodedQueryTQ {
        shared::preprocess_query(&self.quantizer, self.distance, query)
    }

    /// Asymmetric score of a precomputed query against already-fetched encoded
    /// `bytes`, applying the metric sign convention.
    pub fn score_query_bytes(&self, query: &EncodedQueryTQ, bytes: &[u8]) -> ScoreType {
        shared::score_query_bytes(&self.quantizer, self.distance, query, bytes)
    }

    /// Symmetric score between two stored vectors, selected by their offsets.
    pub fn score_internal_encoded(
        &self,
        point_a: PointOffsetType,
        point_b: PointOffsetType,
    ) -> ScoreType {
        let v1 = self.storage.get_vector_data(point_a);
        let v2 = self.storage.get_vector_data(point_b);
        shared::score_symmetric_bytes(&self.quantizer, self.distance, &v1, &v2)
    }
}

impl<S: UniversalRead> std::fmt::Debug for TurboVectorStorageImpl<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurboVectorStorageImpl")
            .field("dim", &self.dim)
            .field("distance", &self.distance)
            .field("total_vector_count", &self.storage.vectors_count())
            .field("deleted_count", &self.deleted_count)
            .finish_non_exhaustive()
    }
}

impl<S: UniversalRead> VectorStorageRead for TurboVectorStorageImpl<S> {
    fn size_of_available_vectors_in_bytes(&self) -> usize {
        self.available_vector_count() * self.quantized_vector_size()
    }

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
        self.storage.vectors_count()
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        shared::dequantize_vector(
            &self.quantizer,
            self.dim,
            &self.storage.get_vector_data(key),
        )
    }

    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) {
        let (user_data, point_offsets): (Vec<U>, Vec<PointOffsetType>) = keys.into_iter().unzip();

        self.storage
            .for_each_in_batch(&point_offsets, |idx, bytes| {
                let vector = shared::dequantize_vector(&self.quantizer, self.dim, bytes);
                callback(user_data[idx], point_offsets[idx], vector);
            })
            .expect("read TQ vectors");
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        let bytes = self.storage.get_vector_data_opt(key)?;
        Some(shared::dequantize_vector(&self.quantizer, self.dim, &bytes))
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

    fn read_vector_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        self.read_dense_tq_bytes::<P, U>(keys, callback)
    }
}

impl<S: UniversalRead> VectorStorage for TurboVectorStorageImpl<S> {
    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let dense: &[VectorElementType] = vector.try_into()?;
        let quantized = self
            .quantizer
            .quantize(dense, &mut self.quantization_buffer);
        // The single-file backend is not appendable: `upsert_vector` returns the
        // "unsupported" error, matching the immutable dense storage.
        self.storage.upsert_vector(key, &quantized, hw_counter)?;
        self.set_deleted(key, false);
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let storage_flusher = self.storage.flusher();
        let deleted_flusher = self.deleted.flusher();

        Box::new(move || {
            storage_flusher()?;
            deleted_flusher()?;
            Ok(())
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.storage.files();
        files.extend(self.deleted.files());
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.storage.immutable_files()
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        Ok(!self.set_deleted(key, true))
    }
}

impl<S: UniversalRead> TurboScoring for TurboVectorStorageImpl<S> {
    fn preprocess_query(&self, query: DenseVector) -> EncodedQueryTQ {
        Self::preprocess_query(self, query)
    }

    fn score_query_bytes(&self, query: &EncodedQueryTQ, bytes: &[u8]) -> ScoreType {
        Self::score_query_bytes(self, query, bytes)
    }

    fn score_internal_encoded(
        &self,
        point_a: PointOffsetType,
        point_b: PointOffsetType,
    ) -> ScoreType {
        Self::score_internal_encoded(self, point_a, point_b)
    }

    fn get_quantized_vector(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        Self::get_quantized_vector(self, key)
    }
}

impl<S: UniversalRead> DenseTQVectorStorageRead for TurboVectorStorageImpl<S> {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn quantized_vector_size(&self) -> usize {
        self.quantizer.quantized_size()
    }

    fn get_dense_tq<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        self.storage.get_vector_data(key)
    }

    fn for_each_in_dense_tq_batch<F: FnMut(usize, &[u8])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
    ) -> OperationResult<()> {
        self.storage.for_each_in_batch(keys, f)
    }

    fn read_dense_tq_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        let (user_data, point_offsets): (Vec<U>, Vec<PointOffsetType>) = keys.into_iter().unzip();

        self.storage
            .for_each_in_batch(&point_offsets, |idx, bytes| {
                callback(user_data[idx], point_offsets[idx], bytes.to_vec());
            })
    }

    fn get_dense_for_requantization(
        &self,
        key: PointOffsetType,
        keep_rotated: bool,
    ) -> DenseVector {
        shared::dequantize_for_requantization(
            &self.quantizer,
            self.dim,
            &self.storage.get_vector_data(key),
            keep_rotated,
        )
    }
}

impl<S: UniversalRead> DenseTQVectorStorage for TurboVectorStorageImpl<S> {
    /// Bulk-append already-encoded vectors: write the encoded bytes to the file,
    /// then re-open the storage through `fs` so reads observe the appended
    /// vectors (mirrors `DenseVectorStorageImpl::update_from`).
    fn update_from<'a>(
        &mut self,
        other_vectors: &mut impl Iterator<Item = (Cow<'a, [u8]>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.storage.vectors_count() as PointOffsetType;
        let mut end_index = start_index;
        let mut deleted_offsets = Vec::new();

        let mut writer = self.storage.open_appender()?;
        for (vector, deleted) in other_vectors {
            check_process_stopped(stopped)?;
            writer.write_all(&vector)?;
            if deleted {
                deleted_offsets.push(end_index);
            }
            end_index += 1;
        }

        // Persist + reopen so reads observe the appended vectors.
        writer.flush()?;
        let file = writer
            .into_inner()
            .map_err(io::IntoInnerError::into_error)?;
        file.sync_data()?;
        self.storage.reload(&self.fs)?;

        for key in deleted_offsets {
            self.set_deleted(key, true);
        }

        Ok(start_index..end_index)
    }
}
