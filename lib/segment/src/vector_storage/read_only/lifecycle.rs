use std::path::Path;

use common::mmap::{Advice, AdviceSetting};
use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs};

use super::VectorStorageReadEnum;
use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::index::hnsw_index::HnswGraph;
use crate::index::hnsw_index::hnsw::graph_residency;
use crate::types::{VectorDataConfig, VectorStorageDatatype, VectorStorageType};
use crate::vector_storage::dense::appendable_dense_vector_storage::DELETED_DIR_PATH;
use crate::vector_storage::dense::immutable_dense_vectors::ImmutableDenseVectorData;
use crate::vector_storage::dense::read_only::{
    ReadOnlyChunkedDenseVectorStorage, ReadOnlyImmutableDenseVectorStorage,
};
use crate::vector_storage::multi_dense::read_only::ReadOnlyChunkedMultiDenseVectorStorage;
use crate::vector_storage::turbo::multi_turbo::read_only::ReadOnlyChunkedMultiTurboVectorStorage;
use crate::vector_storage::turbo::read_only::{
    ReadOnlyChunkedTurboVectorStorage, ReadOnlyImmutableTurboVectorStorage,
};

/// How the [`VectorStorageType`] maps onto the read-only open path.
enum ReadOnlyLayout {
    /// No on-disk data to open.
    None,
    /// Dedicated vector files.
    Files {
        advice: AdviceSetting,
        populate: Populate,
        chunked: bool,
    },
    /// Vectors inlined in the HNSW links file.
    GraphInline,
}

fn storage_type_layout(storage_type: VectorStorageType) -> ReadOnlyLayout {
    let (advice, populate, chunked) = match storage_type {
        VectorStorageType::Mmap => (AdviceSetting::Global, false, false),
        VectorStorageType::InRamMmap => (AdviceSetting::from(Advice::Normal), true, false),
        VectorStorageType::ChunkedMmap => (AdviceSetting::Global, false, true),
        VectorStorageType::InRamChunkedMmap => (AdviceSetting::from(Advice::Normal), true, true),
        VectorStorageType::Memory => return ReadOnlyLayout::None,
        VectorStorageType::GraphInline => return ReadOnlyLayout::GraphInline,
    };

    let populate = match populate {
        true => Populate::PreferBackground,
        false => Populate::No,
    };

    ReadOnlyLayout::Files {
        advice,
        populate,
        chunked,
    }
}

impl<S: UniversalRead> VectorStorageReadEnum<S> {
    /// Schedule background prefetch of every file [`Self::open`] will read,
    /// dispatching on `vector_config` the same way.
    ///
    /// A `populate_override` (from a request-specific
    /// [`LoadProfile`](crate::data_types::load_profile::LoadProfile)) replaces
    /// the storage-type-derived populate; the mmap advice stays derived from
    /// the storage type.
    ///
    /// Absent files are skipped rather than reported: the subsequent open is
    /// the one to produce the error.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        vector_config: &VectorDataConfig,
        path: &Path,
        vector_index_path: &Path,
        populate_override: Option<Populate>,
    ) -> OperationResult<()> {
        let datatype = vector_config.datatype.unwrap_or_default();

        let (advice, populate, chunked) = match storage_type_layout(vector_config.storage_type) {
            ReadOnlyLayout::None => return Ok(()),
            ReadOnlyLayout::GraphInline => {
                let (_memory, residency) =
                    graph_residency(vector_config.storage_memory(), populate_override);
                HnswGraph::preopen_universal(fs, vector_index_path, residency)?;
                return InMemoryBitvecFlags::preopen(fs, &path.join(DELETED_DIR_PATH));
            }
            ReadOnlyLayout::Files {
                advice,
                populate,
                chunked,
            } => (advice, populate, chunked),
        };
        let populate = populate_override.unwrap_or(populate);

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
                VectorStorageDatatype::Turbo4 => {
                    ReadOnlyChunkedMultiTurboVectorStorage::<S>::preopen(fs, path, advice, populate)
                }
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
                VectorStorageDatatype::Turbo4 => {
                    ReadOnlyChunkedTurboVectorStorage::<S>::preopen(fs, path, populate)
                }
            }
        } else {
            match datatype {
                VectorStorageDatatype::Float32 => ReadOnlyImmutableDenseVectorStorage::<
                    ImmutableDenseVectorData<VectorElementType, S>,
                >::preopen(fs, path, populate),
                VectorStorageDatatype::Uint8 => ReadOnlyImmutableDenseVectorStorage::<
                    ImmutableDenseVectorData<VectorElementTypeByte, S>,
                >::preopen(fs, path, populate),
                VectorStorageDatatype::Float16 => ReadOnlyImmutableDenseVectorStorage::<
                    ImmutableDenseVectorData<VectorElementTypeHalf, S>,
                >::preopen(fs, path, populate),
                VectorStorageDatatype::Turbo4 => {
                    ReadOnlyImmutableTurboVectorStorage::preopen(fs, path, populate)
                }
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
        vector_index_path: &Path,
        populate_override: Option<Populate>,
    ) -> OperationResult<Option<Self>>
    where
        S: 'static,
    {
        let dim = vector_config.size;
        let distance = vector_config.distance;
        let datatype = vector_config.datatype.unwrap_or_default();

        let (advice, populate, chunked) = match storage_type_layout(vector_config.storage_type) {
            ReadOnlyLayout::None => return Ok(None),
            ReadOnlyLayout::GraphInline => {
                let (_memory, residency) =
                    graph_residency(vector_config.storage_memory(), populate_override);
                let graph = HnswGraph::open_universal(fs, vector_index_path, residency)?;

                return Ok(Some(match datatype {
                    VectorStorageDatatype::Float32 => Self::DenseGraphInline(Box::new(
                        ReadOnlyImmutableDenseVectorStorage::open_graph(
                            fs, path, graph, dim, distance,
                        )?,
                    )),
                    VectorStorageDatatype::Uint8 => Self::DenseGraphInlineByte(Box::new(
                        ReadOnlyImmutableDenseVectorStorage::open_graph(
                            fs, path, graph, dim, distance,
                        )?,
                    )),
                    VectorStorageDatatype::Float16 => Self::DenseGraphInlineHalf(Box::new(
                        ReadOnlyImmutableDenseVectorStorage::open_graph(
                            fs, path, graph, dim, distance,
                        )?,
                    )),
                    VectorStorageDatatype::Turbo4 => Self::DenseTurboGraphInline(Box::new(
                        ReadOnlyImmutableTurboVectorStorage::open_graph(
                            fs, path, graph, dim, distance,
                        )?,
                    )),
                }));
            }
            ReadOnlyLayout::Files {
                advice,
                populate,
                chunked,
            } => (advice, populate, chunked),
        };
        let populate = populate_override.unwrap_or(populate);

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
                    Self::MultiDenseTurbo(Box::new(ReadOnlyChunkedMultiTurboVectorStorage::open(
                        fs,
                        path,
                        dim,
                        distance,
                        multivector_config,
                        advice,
                        populate,
                    )?))
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
                VectorStorageDatatype::Turbo4 => Self::DenseTurboChunked(Box::new(
                    ReadOnlyChunkedTurboVectorStorage::open(fs, path, dim, distance, populate)?,
                )),
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
                VectorStorageDatatype::Turbo4 => Self::DenseTurbo(Box::new(
                    ReadOnlyImmutableTurboVectorStorage::open(fs, path, dim, distance, populate)?,
                )),
            }
        }))
    }
}
