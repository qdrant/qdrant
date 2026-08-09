//! The write half of the appendable vector storages, for update-only segments.
//!
//! Each storage family decomposes into pieces that already append: the vectors
//! themselves into [`UpdateOnlyChunkedVectors`], the deleted flags into
//! [`UpdateOnlyStoredFlags`], the sparse values into [`UpdateOnlyBlobstore`].
//! What these writers add is the layout — which directory holds what — and the
//! rule that a point with no vector under a name still occupies its slot.
//!
//! [`UpdateOnlyChunkedVectors`]: crate::vector_storage::chunked_vectors::update_only::UpdateOnlyChunkedVectors
//! [`UpdateOnlyStoredFlags`]: crate::common::flags::update_only_stored_flags::UpdateOnlyStoredFlags
//! [`UpdateOnlyBlobstore`]: crate::common::update_only_blobstore::UpdateOnlyBlobstore

use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppend;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::{
    VectorElementType, VectorElementTypeByte, VectorElementTypeHalf, VectorRef,
};
use crate::types::{VectorDataConfig, VectorStorageDatatype, VectorStorageType};
use crate::vector_storage::dense::update_only::UpdateOnlyDenseVectorStorage;
use crate::vector_storage::multi_dense::update_only::UpdateOnlyMultiDenseVectorStorage;
use crate::vector_storage::sparse::update_only::UpdateOnlySparseVectorStorage;
use crate::vector_storage::turbo::multi_turbo::update_only::UpdateOnlyMultiTurboVectorStorage;
use crate::vector_storage::turbo::update_only::UpdateOnlyTurboVectorStorage;

/// One point's vector for one named storage, as a batch supplies it.
///
/// The two ways a vector arrives are the two halves of
/// [`FullyQualifiedPoint`][1]: `updated_vectors`, decoded by the batch, and
/// `stored_vectors`, carried over from the point's previous slot as
/// storage-native bytes that never need decoding.
///
/// [1]: crate::data_types::fully_qualified_point::FullyQualifiedPoint
pub enum VectorToStore<'a> {
    /// Decoded by the batch.
    Decoded(VectorRef<'a>),
    /// Storage-native bytes, in the form [`retrieve_raw`][1] returns.
    ///
    /// [1]: crate::entry::entry_point::ReadSegmentEntry::retrieve_raw
    Raw(&'a [u8]),
    /// The point has no vector under this name.
    ///
    /// Its slot is still written — with a placeholder value — and flagged
    /// deleted, because slots are shared across every named storage of the
    /// segment: skipping one here would shift every later vector of this
    /// storage against the id tracker. This is what the writable path does in
    /// [`PlainVectorIndex::update_vector`][1].
    ///
    /// [1]: crate::index::plain_vector_index::PlainVectorIndex
    Missing,
}

/// The write half of one named vector storage, over the backend `S`.
///
/// Covers the appendable half of [`VectorStorageEnum`][1] — the storages an
/// update-only segment can have. The immutable ones are built, not appended to,
/// so they have no writer here.
///
/// [1]: crate::vector_storage::VectorStorageEnum
pub enum UpdateOnlyVectorStorage<S: UniversalAppend + 'static> {
    Dense(Box<UpdateOnlyDenseVectorStorage<VectorElementType, S>>),
    DenseByte(Box<UpdateOnlyDenseVectorStorage<VectorElementTypeByte, S>>),
    DenseHalf(Box<UpdateOnlyDenseVectorStorage<VectorElementTypeHalf, S>>),
    MultiDense(Box<UpdateOnlyMultiDenseVectorStorage<VectorElementType, S>>),
    MultiDenseByte(Box<UpdateOnlyMultiDenseVectorStorage<VectorElementTypeByte, S>>),
    MultiDenseHalf(Box<UpdateOnlyMultiDenseVectorStorage<VectorElementTypeHalf, S>>),
    Turbo(Box<UpdateOnlyTurboVectorStorage<S>>),
    MultiTurbo(Box<UpdateOnlyMultiTurboVectorStorage<S>>),
    Sparse(Box<UpdateOnlySparseVectorStorage<S>>),
}

impl<S: UniversalAppend + 'static> UpdateOnlyVectorStorage<S> {
    /// Open the writer for the storage `config` describes at `path`, creating it
    /// if it is not there yet.
    ///
    /// Fails for a storage type an update-only segment cannot have: the mmap
    /// ones are immutable, built whole rather than appended to, and the empty
    /// placeholder has no files at all.
    pub fn open(fs: S::Fs, path: &Path, config: &VectorDataConfig) -> OperationResult<Self> {
        match config.storage_type {
            VectorStorageType::ChunkedMmap | VectorStorageType::InRamChunkedMmap => {}
            storage_type @ (VectorStorageType::Mmap
            | VectorStorageType::InRamMmap
            | VectorStorageType::Memory
            | VectorStorageType::Empty) => {
                return Err(OperationError::service_error(format!(
                    "Cannot open a {storage_type:?} vector storage for appending: it is not an \
                     appendable storage type",
                )));
            }
        }

        let dim = config.size;
        let datatype = config.datatype.unwrap_or_default();
        let storage = match (config.multivector_config.is_some(), datatype) {
            (false, VectorStorageDatatype::Float32) => {
                Self::Dense(Box::new(UpdateOnlyDenseVectorStorage::open(fs, path, dim)?))
            }
            (false, VectorStorageDatatype::Uint8) => {
                Self::DenseByte(Box::new(UpdateOnlyDenseVectorStorage::open(fs, path, dim)?))
            }
            (false, VectorStorageDatatype::Float16) => {
                Self::DenseHalf(Box::new(UpdateOnlyDenseVectorStorage::open(fs, path, dim)?))
            }
            (false, VectorStorageDatatype::Turbo4) => Self::Turbo(Box::new(
                UpdateOnlyTurboVectorStorage::open(fs, path, dim, config.distance)?,
            )),
            (true, VectorStorageDatatype::Float32) => Self::MultiDense(Box::new(
                UpdateOnlyMultiDenseVectorStorage::open(fs, path, dim)?,
            )),
            (true, VectorStorageDatatype::Uint8) => Self::MultiDenseByte(Box::new(
                UpdateOnlyMultiDenseVectorStorage::open(fs, path, dim)?,
            )),
            (true, VectorStorageDatatype::Float16) => Self::MultiDenseHalf(Box::new(
                UpdateOnlyMultiDenseVectorStorage::open(fs, path, dim)?,
            )),
            (true, VectorStorageDatatype::Turbo4) => Self::MultiTurbo(Box::new(
                UpdateOnlyMultiTurboVectorStorage::open(fs, path, dim, config.distance)?,
            )),
        };

        Ok(storage)
    }

    /// Open the writer for a sparse vector storage at `path`. Sparse vectors
    /// are configured separately from dense ones, so they do not go through
    /// [`open`](Self::open).
    pub fn open_sparse(fs: S::Fs, path: &Path) -> OperationResult<Self> {
        Ok(Self::Sparse(Box::new(UpdateOnlySparseVectorStorage::open(
            fs, path,
        )?)))
    }

    /// Append one vector per point of a batch, starting at `start_slot`, and
    /// persist them.
    pub fn append_many<'a>(
        &mut self,
        start_slot: PointOffsetType,
        vectors: impl IntoIterator<Item = VectorToStore<'a>>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            Self::Dense(s) => s.append_many(start_slot, vectors, hw_counter),
            Self::DenseByte(s) => s.append_many(start_slot, vectors, hw_counter),
            Self::DenseHalf(s) => s.append_many(start_slot, vectors, hw_counter),
            Self::MultiDense(s) => s.append_many(start_slot, vectors, hw_counter),
            Self::MultiDenseByte(s) => s.append_many(start_slot, vectors, hw_counter),
            Self::MultiDenseHalf(s) => s.append_many(start_slot, vectors, hw_counter),
            Self::Turbo(s) => s.append_many(start_slot, vectors, hw_counter),
            Self::MultiTurbo(s) => s.append_many(start_slot, vectors, hw_counter),
            Self::Sparse(s) => s.append_many(start_slot, vectors, hw_counter),
        }
    }
}
