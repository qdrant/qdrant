//! Appendable (chunked mmap) TurboQuant dense vector storage.
//!
//! [`AppendableMmapTurboVectorStorage`] keeps the TQ-encoded vectors in growable
//! chunked mmap files plus its own deletion flags, supporting per-point
//! `insert_vector`. It is the counterpart of the appendable dense
//! [`AppendableMmapDenseVectorStorage`](crate::vector_storage::dense::appendable_dense_vector_storage::AppendableMmapDenseVectorStorage):
//! the IO backend is fixed to `MmapFile`, so the type is not generic over `S`.

use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::types::{PointOffsetType, ScoreType};
use common::universal_io::{MmapFile, MmapFs, Populate, UserData};
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
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorage;
use crate::vector_storage::{
    DenseTQVectorStorage, DenseTQVectorStorageRead, TurboScoring, VectorStorage, VectorStorageRead,
};

/// Appendable TurboQuant dense vector storage backed by chunked mmap files.
pub struct AppendableMmapTurboVectorStorage {
    /// Chunked, appendable encoded vectors.
    storage: QuantizedChunkedStorage<MmapFile>,

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

/// Open (create-or-load) an appendable TurboQuant vector storage backed by chunked
/// mmap files. Counterpart to `open_appendable_memmap_vector_storage`.
pub fn open_appendable_turbo_vector_storage(
    path: &Path,
    dim: usize,
    distance: Distance,
    in_ram: bool,
) -> OperationResult<AppendableMmapTurboVectorStorage> {
    AppendableMmapTurboVectorStorage::open(path, dim, distance, in_ram)
}

impl AppendableMmapTurboVectorStorage {
    /// Open (create-or-load) the appendable chunked mmap backend.
    pub fn open(
        path: &Path,
        dim: usize,
        distance: Distance,
        in_ram: bool,
    ) -> OperationResult<Self> {
        fs_err::create_dir_all(path)?;

        let quantizer = shared::build_quantizer(dim, distance);
        let storage = QuantizedChunkedStorage::new(
            MmapFs,
            &path.join(VECTORS_PATH),
            quantizer.quantized_size(),
            in_ram,
        )?;

        let deleted = BitvecFlags::new(
            MmapFs,
            DynamicStoredFlags::open(&MmapFs, &path.join(DELETED_PATH), Populate::from(in_ram))?,
        )?;
        let deleted_count = deleted.count_trues();
        let quantization_buffer = vec![0.0; quantizer.get_padded_dim()];

        Ok(Self {
            storage,
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
        self.storage.populate()
    }

    /// Drop the disk cache for the encoded vectors and deleted flags.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache()?;
        self.deleted.clear_cache()?;
        Ok(())
    }

    /// Upsert one vector from its already-encoded TurboQuant bytes, verbatim.
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

impl std::fmt::Debug for AppendableMmapTurboVectorStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppendableMmapTurboVectorStorage")
            .field("dim", &self.dim)
            .field("distance", &self.distance)
            .field("total_vector_count", &self.storage.vectors_count())
            .field("deleted_count", &self.deleted_count)
            .finish_non_exhaustive()
    }
}

impl VectorStorageRead for AppendableMmapTurboVectorStorage {
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
        for (user_data, key) in keys {
            let vector = shared::dequantize_vector(
                &self.quantizer,
                self.dim,
                &self.storage.get_vector_data(key),
            );
            callback(user_data, key, vector);
        }
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

impl VectorStorage for AppendableMmapTurboVectorStorage {
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

impl TurboScoring for AppendableMmapTurboVectorStorage {
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

impl DenseTQVectorStorageRead for AppendableMmapTurboVectorStorage {
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
        mut f: F,
    ) -> OperationResult<()> {
        for (idx, &key) in keys.iter().enumerate() {
            f(idx, &self.storage.get_vector_data(key));
        }
        Ok(())
    }

    fn read_dense_tq_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        for (user_data, key) in keys {
            callback(user_data, key, self.storage.get_vector_data(key).to_vec());
        }
        Ok(())
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

impl DenseTQVectorStorage for AppendableMmapTurboVectorStorage {
    /// Bulk-append already-encoded vectors one record at a time into the chunked
    /// backend, recording soft-deletions as they come.
    fn update_from<'a>(
        &mut self,
        other_vectors: &mut impl Iterator<Item = (Cow<'a, [u8]>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let disposed_hw = HardwareCounterCell::disposable();
        let start_index = self.storage.vectors_count() as PointOffsetType;
        let mut key = start_index;

        for (vector, deleted) in other_vectors {
            check_process_stopped(stopped)?;
            self.storage.upsert_vector(key, &vector, &disposed_hw)?;
            if deleted {
                self.set_deleted(key, true);
            }
            key += 1;
        }

        Ok(start_index..key)
    }
}
