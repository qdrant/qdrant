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
use quantization::turboquant::quantization::TurboQuantizer;
use quantization::turboquant::{EncodedQueryTQ, TQBits};

use super::shared::{self, DELETED_DIR_PATH, VECTORS_PATH};
use super::turbo_vectors::TurboVectorBlob;
use crate::common::Flusher;
use crate::common::flags::FlagsMode;
use crate::common::flags::bitvec_flags::BitvecFlags;
use crate::common::io_uring::{IoUringFallback, use_io_uring};
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{DenseVector, VectorRef};
use crate::index::hnsw_index::HnswGraph;
use crate::types::{Distance, IoBackend, Memory, VectorStorageDatatype};
use crate::vector_storage::common::error_immutable_insert;
use crate::vector_storage::graph_vectors::GraphVectors;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;
use crate::vector_storage::{
    DenseTQVectorStorage, DenseTQVectorStorageRead, TurboScoring, VectorStorage, VectorStorageEnum,
    VectorStorageRead,
};

/// Single-file TurboQuant dense vector storage over the encoded-vector blob `B`
/// — for now only its own file, read through mmap or io_uring. Non-appendable
/// data, mutable deletions.
pub struct TurboVectorStorageImpl<B: TurboVectorBlob> {
    /// Encoded vectors.
    storage: B,

    /// Quantizer used to de/quantize.
    quantizer: TurboQuantizer,

    /// Persisted flags marking which vectors are soft-deleted.
    deleted: BitvecFlags<MmapFile>,
    /// Number of vectors currently flagged as deleted.
    deleted_count: usize,

    on_disk: bool,

    /// Distance used for scoring / query preprocessing.
    distance: Distance,
    /// Original (un-padded) vector dimensionality.
    dim: usize,
}

/// Open (create-or-load) a single-file TurboQuant vector storage (non-appendable),
/// wrapped into the right [`VectorStorageEnum`] variant. Counterpart to
/// `open_dense_vector_storage`: reads go through io_uring when enabled and supported,
/// plain mmap otherwise.
pub fn open_turbo_vector_storage(
    path: &Path,
    dim: usize,
    distance: Distance,
    bits: TQBits,
    memory: Memory,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<VectorStorageEnum> {
    // Like the immutable dense storages: no feature flag of its own, and predates the setting.
    let with_uring = use_io_uring(IoUringFallback::AsyncScorer, memory, true);

    open_turbo_vector_storage_with_uring(
        path,
        dim,
        distance,
        bits,
        memory.populate_on_open(),
        with_uring,
        hw_counter,
    )
}

/// [`open_turbo_vector_storage`] with an explicit backend choice instead of the
/// node-wide io_uring setting. Falls back to mmap (with an error log) if the
/// io_uring backend cannot be opened.
pub fn open_turbo_vector_storage_with_uring(
    path: &Path,
    dim: usize,
    distance: Distance,
    bits: TQBits,
    populate: bool,
    with_uring: bool,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<VectorStorageEnum> {
    // prevent "unused variable" warning
    let _ = with_uring;

    #[cfg(target_os = "linux")]
    if with_uring {
        match TurboVectorStorageImpl::<QuantizedStorage<IoUringFile>>::open_uring(
            path, dim, distance, bits, populate, hw_counter,
        ) {
            Ok(storage) => return Ok(VectorStorageEnum::DenseTurboUring(Box::new(storage))),
            Err(err) => {
                log::error!("Failed to open io_uring based TurboQuant storage: {err}");
            }
        }
    }

    let storage = TurboVectorStorageImpl::<QuantizedStorage<MmapFile>>::open_mmap(
        path, dim, distance, bits, populate, hw_counter,
    )?;
    Ok(VectorStorageEnum::DenseTurboMemmap(Box::new(storage)))
}

impl TurboVectorStorageImpl<QuantizedStorage<MmapFile>> {
    /// Open (create-or-load) the mem-mapped single-file backend.
    pub fn open_mmap(
        path: &Path,
        dim: usize,
        distance: Distance,
        bits: TQBits,
        populate: bool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        let quantizer = shared::build_quantizer(dim, distance, bits);
        let storage = QuantizedStorage::<MmapFile>::open(
            &MmapFs,
            &path.join(VECTORS_PATH),
            quantizer.quantized_size(),
            populate,
            hw_counter,
        )?;
        Self::finalize(
            storage, quantizer, path, dim, distance, populate, true, hw_counter,
        )
    }
}

#[cfg(target_os = "linux")]
impl TurboVectorStorageImpl<QuantizedStorage<IoUringFile>> {
    /// Open (create-or-load) the single-file io_uring backend.
    pub fn open_uring(
        path: &Path,
        dim: usize,
        distance: Distance,
        bits: TQBits,
        populate: bool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        let quantizer = shared::build_quantizer(dim, distance, bits);
        let storage = QuantizedStorage::<IoUringFile>::open(
            &IoUringFs,
            &path.join(VECTORS_PATH),
            quantizer.quantized_size(),
            populate,
            hw_counter,
        )?;
        Self::finalize(
            storage, quantizer, path, dim, distance, populate, true, hw_counter,
        )
    }
}

impl<S: UniversalRead> TurboVectorStorageImpl<GraphVectors<u8, S>> {
    pub fn open_graph(
        graph: HnswGraph<S>,
        path: &Path,
        dim: usize,
        distance: Distance,
        bits: TQBits,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        let on_disk = graph.is_on_disk();
        let quantizer = shared::build_quantizer(dim, distance, bits);
        let storage = GraphVectors::new(graph, quantizer.quantized_size())?;
        Self::finalize(
            storage, quantizer, path, dim, distance, !on_disk, on_disk, hw_counter,
        )
    }

    pub fn io_backend(&self) -> Option<IoBackend> {
        self.storage.graph().io_backend()
    }

    pub fn hnsw_graph(&self) -> &HnswGraph<S> {
        self.storage.graph()
    }
}

impl<B: TurboVectorBlob> TurboVectorStorageImpl<B> {
    /// Shared tail of the backend-specific `open_*` constructors: open the
    /// deletion flags and assemble the storage.
    #[expect(clippy::too_many_arguments)]
    fn finalize(
        storage: B,
        quantizer: TurboQuantizer,
        path: &Path,
        dim: usize,
        distance: Distance,
        populate: bool,
        on_disk: bool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        fs_err::create_dir_all(path)?;
        let deleted = BitvecFlags::open_or_create(
            MmapFs,
            &path.join(DELETED_DIR_PATH),
            FlagsMode::from_feature_flags(),
            Populate::from(populate),
            hw_counter,
        )?;
        let deleted_count = deleted.count_trues();

        Ok(Self {
            storage,
            quantizer,
            deleted,
            deleted_count,
            on_disk,
            distance,
            dim,
        })
    }

    /// Raw encoded vector blob for one vector (no dequantization).
    pub fn get_quantized_vector(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Cow<'_, [u8]> {
        self.storage.get_vector_data(key, hw_counter)
    }

    /// Populate all pages of the encoded vectors into the page cache.
    pub fn populate(&self, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        // deleted bitvec is already loaded
        self.storage.populate(hw_counter);
        Ok(())
    }

    /// Drop the disk cache for the encoded vectors and deleted flags.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache()?;
        self.deleted.clear_cache()?;
        Ok(())
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn insert_tq_bytes(
        &self,
        _key: PointOffsetType,
        _bytes: &[u8],
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Err(error_immutable_insert())
    }

    /// Set the deleted flag for `key`, keeping `deleted_count` in sync, and
    /// return the previous state.
    fn set_deleted(
        &mut self,
        key: PointOffsetType,
        deleted: bool,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        if !deleted && self.storage.vectors_count(hw_counter) <= key as usize {
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
        hw_counter: &HardwareCounterCell,
    ) -> ScoreType {
        let v1 = self.storage.get_vector_data(point_a, hw_counter);
        let v2 = self.storage.get_vector_data(point_b, hw_counter);
        shared::score_symmetric_bytes(&self.quantizer, self.distance, &v1, &v2)
    }
}

impl<B: TurboVectorBlob> std::fmt::Debug for TurboVectorStorageImpl<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurboVectorStorageImpl")
            .field("dim", &self.dim)
            .field("distance", &self.distance)
            .field(
                "total_vector_count",
                &self
                    .storage
                    .vectors_count(&HardwareCounterCell::disposable()),
            )
            .field("deleted_count", &self.deleted_count)
            .finish_non_exhaustive()
    }
}

impl<B: TurboVectorBlob> VectorStorageRead for TurboVectorStorageImpl<B> {
    fn size_of_available_vectors_in_bytes(&self, hw_counter: &HardwareCounterCell) -> usize {
        self.available_vector_count(hw_counter) * self.quantized_vector_size()
    }

    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        shared::storage_datatype(&self.quantizer)
    }

    fn is_on_disk(&self) -> bool {
        self.on_disk
    }

    fn total_vector_count(&self, hw_counter: &HardwareCounterCell) -> usize {
        self.storage.vectors_count(hw_counter)
    }

    fn get_vector<P: AccessPattern>(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> CowVector<'_> {
        shared::dequantize_vector(
            &self.quantizer,
            self.dim,
            &self.storage.get_vector_data(key, hw_counter),
        )
    }

    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
        hw_counter: &HardwareCounterCell,
    ) {
        let (user_data, point_offsets): (Vec<U>, Vec<PointOffsetType>) = keys.into_iter().unzip();

        self.storage
            .for_each_in_batch(
                &point_offsets,
                |idx, bytes| {
                    let vector = shared::dequantize_vector(&self.quantizer, self.dim, bytes);
                    callback(user_data[idx], point_offsets[idx], vector);
                },
                hw_counter,
            )
            .expect("read TQ vectors");
    }

    fn get_vector_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<CowVector<'_>> {
        let bytes = self.storage.get_vector_data_opt(key, hw_counter)?;
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
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.read_dense_tq_bytes::<P, U>(keys, callback, hw_counter)
    }
}

impl<B: TurboVectorBlob> VectorStorage for TurboVectorStorageImpl<B> {
    fn insert_vector(
        &mut self,
        _key: PointOffsetType,
        _vector: VectorRef,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Err(error_immutable_insert())
    }

    fn flusher(&self, hw_counter: &HardwareCounterCell) -> Flusher {
        let storage_flusher = self.storage.flusher();
        let deleted_flusher = self.deleted.flusher(hw_counter);

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
        self.storage.files()
    }

    fn delete_vector(
        &mut self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        Ok(!self.set_deleted(key, true, hw_counter))
    }
}

impl<B: TurboVectorBlob> TurboScoring for TurboVectorStorageImpl<B> {
    fn preprocess_query(&self, query: DenseVector) -> EncodedQueryTQ {
        Self::preprocess_query(self, query)
    }

    fn score_query_bytes(&self, query: &EncodedQueryTQ, bytes: &[u8]) -> ScoreType {
        Self::score_query_bytes(self, query, bytes)
    }

    fn score_query_batch(
        &self,
        query: &EncodedQueryTQ,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
        hw_counter: &HardwareCounterCell,
    ) {
        self.storage.score_query_batch(
            &self.quantizer,
            self.distance,
            query,
            ids,
            scores,
            hw_counter,
        );
    }

    fn score_internal_encoded(
        &self,
        point_a: PointOffsetType,
        point_b: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> ScoreType {
        Self::score_internal_encoded(self, point_a, point_b, hw_counter)
    }

    fn get_quantized_vector(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Cow<'_, [u8]> {
        Self::get_quantized_vector(self, key, hw_counter)
    }
}

impl<B: TurboVectorBlob> DenseTQVectorStorageRead for TurboVectorStorageImpl<B> {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn quantized_vector_size(&self) -> usize {
        self.quantizer.quantized_size()
    }

    fn get_dense_tq<P: AccessPattern>(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Cow<'_, [u8]> {
        self.storage.get_vector_data(key, hw_counter)
    }

    fn for_each_in_dense_tq_batch<F: FnMut(usize, &[u8])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage.for_each_in_batch(keys, f, hw_counter)
    }

    fn read_dense_tq_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, Vec<u8>),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let (user_data, point_offsets): (Vec<U>, Vec<PointOffsetType>) = keys.into_iter().unzip();

        self.storage.for_each_in_batch(
            &point_offsets,
            |idx, bytes| {
                callback(user_data[idx], point_offsets[idx], bytes.to_vec());
            },
            hw_counter,
        )
    }

    fn get_dense_for_requantization(
        &self,
        key: PointOffsetType,
        keep_rotated: bool,
        hw_counter: &HardwareCounterCell,
    ) -> DenseVector {
        shared::dequantize_for_requantization(
            &self.quantizer,
            self.dim,
            &self.storage.get_vector_data(key, hw_counter),
            keep_rotated,
        )
    }
}

impl<S: UniversalRead> DenseTQVectorStorage for TurboVectorStorageImpl<GraphVectors<u8, S>> {
    fn update_from<'a>(
        &mut self,
        _other_vectors: &mut impl Iterator<Item = (Cow<'a, [u8]>, bool)>,
        _stopped: &AtomicBool,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Range<PointOffsetType>> {
        Err(OperationError::service_error(
            "Cannot merge into a graph-backed vector storage",
        ))
    }
}

impl<S: UniversalRead> DenseTQVectorStorage for TurboVectorStorageImpl<QuantizedStorage<S>> {
    fn update_from<'a>(
        &mut self,
        other_vectors: &mut impl Iterator<Item = (Cow<'a, [u8]>, bool)>,
        stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.storage.vectors_count(hw_counter) as PointOffsetType;
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
        self.storage.reload(hw_counter)?;

        for key in deleted_offsets {
            self.set_deleted(key, true, hw_counter);
        }

        Ok(start_index..end_index)
    }
}
