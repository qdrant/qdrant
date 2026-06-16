use std::path::Path;

use common::mmap::{Advice, AdviceSetting};
use common::universal_io::UniversalRead;

use super::VectorStorageReadEnum;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::{VectorDataConfig, VectorStorageDatatype, VectorStorageType};
use crate::vector_storage::dense::read_only::{
    ReadOnlyChunkedDenseVectorStorage, ReadOnlyImmutableDenseVectorStorage,
};
use crate::vector_storage::multi_dense::read_only::ReadOnlyChunkedMultiDenseVectorStorage;

impl<S: UniversalRead> VectorStorageReadEnum<S> {
    /// Open the read-only counterpart of a dense vector storage from its
    /// `VectorDataConfig`, mirroring `open_vector_storage`. Sparse storages are
    /// opened separately via `ReadOnlySparseVectorStorage::open`.
    #[allow(dead_code)] // pending: read-only segment constructor will use this
    pub fn open(
        fs: &S::Fs,
        vector_config: &VectorDataConfig,
        path: &Path,
    ) -> OperationResult<Option<Self>> {
        let dim = vector_config.size;
        let distance = vector_config.distance;
        let datatype = vector_config.datatype.unwrap_or_default();

        let (advice, populate, chunked) = match vector_config.storage_type {
            VectorStorageType::Mmap => (AdviceSetting::Global, false, false),
            VectorStorageType::InRamMmap => (AdviceSetting::from(Advice::Normal), true, false),
            VectorStorageType::ChunkedMmap => (AdviceSetting::Global, false, true),
            VectorStorageType::InRamChunkedMmap => {
                (AdviceSetting::from(Advice::Normal), true, true)
            }
            // No on-disk data to open for these storage types: no-op.
            VectorStorageType::Memory | VectorStorageType::Empty => return Ok(None),
        };

        // Multivectors always use the appendable chunked layout.
        if let Some(multivector_config) = vector_config.multivector_config {
            return Ok(Some(match datatype {
                VectorStorageDatatype::Float32 => {
                    Self::MultiDenseChunked(Box::new(ReadOnlyChunkedMultiDenseVectorStorage::open(
                        fs,
                        path,
                        dim,
                        distance,
                        multivector_config,
                        advice,
                        populate,
                    )?))
                }
                VectorStorageDatatype::Uint8 => Self::MultiDenseChunkedByte(Box::new(
                    ReadOnlyChunkedMultiDenseVectorStorage::open(
                        fs,
                        path,
                        dim,
                        distance,
                        multivector_config,
                        advice,
                        populate,
                    )?,
                )),
                VectorStorageDatatype::Float16 => Self::MultiDenseChunkedHalf(Box::new(
                    ReadOnlyChunkedMultiDenseVectorStorage::open(
                        fs,
                        path,
                        dim,
                        distance,
                        multivector_config,
                        advice,
                        populate,
                    )?,
                )),
                VectorStorageDatatype::Turbo4 => {
                    return Err(OperationError::service_error(
                        "Turbo4 datatype storage is not yet supported",
                    ));
                }
            }));
        }

        // chunked-mmap is appendable; plain mmap is the immutable storage.
        Ok(Some(if chunked {
            match datatype {
                VectorStorageDatatype::Float32 => {
                    Self::DenseChunked(Box::new(ReadOnlyChunkedDenseVectorStorage::open(
                        fs, path, dim, distance, advice, populate,
                    )?))
                }
                VectorStorageDatatype::Uint8 => {
                    Self::DenseChunkedByte(Box::new(ReadOnlyChunkedDenseVectorStorage::open(
                        fs, path, dim, distance, advice, populate,
                    )?))
                }
                VectorStorageDatatype::Float16 => {
                    Self::DenseChunkedHalf(Box::new(ReadOnlyChunkedDenseVectorStorage::open(
                        fs, path, dim, distance, advice, populate,
                    )?))
                }
                VectorStorageDatatype::Turbo4 => {
                    return Err(OperationError::service_error(
                        "Turbo4 datatype storage is not yet supported",
                    ));
                }
            }
        } else {
            match datatype {
                VectorStorageDatatype::Float32 => Self::Dense(Box::new(
                    ReadOnlyImmutableDenseVectorStorage::open(fs, path, dim, distance, populate)?,
                )),
                VectorStorageDatatype::Uint8 => Self::DenseByte(Box::new(
                    ReadOnlyImmutableDenseVectorStorage::open(fs, path, dim, distance, populate)?,
                )),
                VectorStorageDatatype::Float16 => Self::DenseHalf(Box::new(
                    ReadOnlyImmutableDenseVectorStorage::open(fs, path, dim, distance, populate)?,
                )),
                VectorStorageDatatype::Turbo4 => {
                    return Err(OperationError::service_error(
                        "Turbo4 datatype storage is not yet supported",
                    ));
                }
            }
        }))
    }
}
