use std::path::Path;

use common::mmap::{Advice, AdviceSetting};
use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs};

use super::VectorStorageReadEnum;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::types::{VectorDataConfig, VectorStorageDatatype, VectorStorageType};
use crate::vector_storage::dense::read_only::{
    ReadOnlyChunkedDenseVectorStorage, ReadOnlyImmutableDenseVectorStorage,
};
use crate::vector_storage::multi_dense::read_only::ReadOnlyChunkedMultiDenseVectorStorage;

/// How the [`VectorStorageType`] maps onto the read-only open path: mmap
/// advice, whether the storage is populated on open, and whether it uses the
/// appendable chunked layout. `None` for the storage types with no on-disk
/// data to open.
fn storage_type_params(storage_type: VectorStorageType) -> Option<(AdviceSetting, Populate, bool)> {
    let (advice, populate, chunked) = match storage_type {
        VectorStorageType::Mmap => (AdviceSetting::Global, false, false),
        VectorStorageType::InRamMmap => (AdviceSetting::from(Advice::Normal), true, false),
        VectorStorageType::ChunkedMmap => (AdviceSetting::Global, false, true),
        VectorStorageType::InRamChunkedMmap => (AdviceSetting::from(Advice::Normal), true, true),
        VectorStorageType::Memory | VectorStorageType::Empty => return None,
    };

    let populate = match populate {
        true => Populate::PreferBackground,
        false => Populate::No,
    };

    Some((advice, populate, chunked))
}

impl<S: UniversalRead> VectorStorageReadEnum<S> {
    /// Schedule background prefetch of every file [`Self::open`] will read,
    /// dispatching on `vector_config` the same way.
    ///
    /// Absent files are skipped rather than reported: the subsequent open is
    /// the one to produce the error.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        vector_config: &VectorDataConfig,
        path: &Path,
    ) -> OperationResult<()> {
        let datatype = vector_config.datatype.unwrap_or_default();

        // No on-disk data to prefetch for these storage types: no-op.
        let Some((advice, populate, chunked)) = storage_type_params(vector_config.storage_type)
        else {
            return Ok(());
        };

        // Multivectors always use the appendable chunked layout.
        if vector_config.multivector_config.is_some() {
            return match datatype {
                VectorStorageDatatype::Float32 => {
                    ReadOnlyChunkedMultiDenseVectorStorage::<VectorElementType, S>::preopen(
                        fs, path, advice, populate,
                    )
                }
                VectorStorageDatatype::Uint8 => {
                    ReadOnlyChunkedMultiDenseVectorStorage::<VectorElementTypeByte, S>::preopen(
                        fs, path, advice, populate,
                    )
                }
                VectorStorageDatatype::Float16 => {
                    ReadOnlyChunkedMultiDenseVectorStorage::<VectorElementTypeHalf, S>::preopen(
                        fs, path, advice, populate,
                    )
                }
                VectorStorageDatatype::Turbo4 => Err(OperationError::service_error(
                    "Turbo4 datatype storage is not yet supported",
                )),
            };
        }

        // chunked-mmap is appendable; plain mmap is the immutable storage.
        if chunked {
            match datatype {
                VectorStorageDatatype::Float32 => {
                    ReadOnlyChunkedDenseVectorStorage::<VectorElementType, S>::preopen(
                        fs, path, advice, populate,
                    )
                }
                VectorStorageDatatype::Uint8 => {
                    ReadOnlyChunkedDenseVectorStorage::<VectorElementTypeByte, S>::preopen(
                        fs, path, advice, populate,
                    )
                }
                VectorStorageDatatype::Float16 => {
                    ReadOnlyChunkedDenseVectorStorage::<VectorElementTypeHalf, S>::preopen(
                        fs, path, advice, populate,
                    )
                }
                VectorStorageDatatype::Turbo4 => Err(OperationError::service_error(
                    "Turbo4 datatype storage is not yet supported",
                )),
            }
        } else {
            match datatype {
                VectorStorageDatatype::Float32 => ReadOnlyImmutableDenseVectorStorage::<
                    VectorElementType,
                    S,
                >::preopen(fs, path, populate),
                VectorStorageDatatype::Uint8 => ReadOnlyImmutableDenseVectorStorage::<
                    VectorElementTypeByte,
                    S,
                >::preopen(fs, path, populate),
                VectorStorageDatatype::Float16 => ReadOnlyImmutableDenseVectorStorage::<
                    VectorElementTypeHalf,
                    S,
                >::preopen(fs, path, populate),
                VectorStorageDatatype::Turbo4 => Err(OperationError::service_error(
                    "Turbo4 datatype storage is not yet supported",
                )),
            }
        }
    }

    /// Open the read-only counterpart of a dense vector storage from its
    /// `VectorDataConfig`, mirroring `open_vector_storage`. Sparse storages are
    /// opened separately via `ReadOnlySparseVectorStorage::open`.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        vector_config: &VectorDataConfig,
        path: &Path,
    ) -> OperationResult<Option<Self>> {
        let dim = vector_config.size;
        let distance = vector_config.distance;
        let datatype = vector_config.datatype.unwrap_or_default();

        // No on-disk data to open for these storage types: no-op.
        let Some((advice, populate, chunked)) = storage_type_params(vector_config.storage_type)
        else {
            return Ok(None);
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
