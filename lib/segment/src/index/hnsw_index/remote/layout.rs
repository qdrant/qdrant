//! Which vector storages can be built remotely, and how the indexer recreates them.

use std::path::Path;

use crate::common::operation_error::OperationResult;
use crate::segment_constructor::open_vector_storage_for_remote_build;
use crate::types::{
    Distance, Indexes, MultiVectorConfig, VectorDataConfig, VectorStorageDatatype,
    VectorStorageType,
};
use crate::vector_storage::{
    DenseVectorStorageRead, MultiVectorStorageRead, VectorStorageEnum, VectorStorageRead,
};

/// Shape of the vectors of a storage that can be built remotely: the indexer recreates a
/// storage with the same storage-native byte encoding from it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RemoteVectorLayout {
    pub dim: usize,
    pub datatype: VectorStorageDatatype,
    pub distance: Distance,
    pub multivector_config: Option<MultiVectorConfig>,
}

impl RemoteVectorLayout {
    /// `None` for storages that are not built remotely: sparse, empty, and TurboQuant
    /// storages, whose bytes are encoded with per-storage parameters.
    pub fn of(storage: &VectorStorageEnum) -> Option<Self> {
        let distance = storage.distance();
        let datatype = storage.datatype();
        let dense = |dim| {
            Some(Self {
                dim,
                datatype,
                distance,
                multivector_config: None,
            })
        };
        let multi = |dim, config: &MultiVectorConfig| {
            Some(Self {
                dim,
                datatype,
                distance,
                multivector_config: Some(*config),
            })
        };
        match storage {
            VectorStorageEnum::DenseVolatile(v) => dense(v.vector_dim()),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => dense(v.vector_dim()),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => dense(v.vector_dim()),
            VectorStorageEnum::DenseMemmap(v) => dense(v.vector_dim()),
            VectorStorageEnum::DenseMemmapByte(v) => dense(v.vector_dim()),
            VectorStorageEnum::DenseMemmapHalf(v) => dense(v.vector_dim()),
            VectorStorageEnum::DenseGraphInline(v) => dense(v.vector_dim()),
            VectorStorageEnum::DenseGraphInlineByte(v) => dense(v.vector_dim()),
            VectorStorageEnum::DenseGraphInlineHalf(v) => dense(v.vector_dim()),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => dense(v.vector_dim()),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => dense(v.vector_dim()),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => dense(v.vector_dim()),
            VectorStorageEnum::DenseAppendableMemmap(v) => dense(v.vector_dim()),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => dense(v.vector_dim()),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => dense(v.vector_dim()),
            VectorStorageEnum::MultiDenseVolatile(v) => {
                multi(v.vector_dim(), v.multi_vector_config())
            }
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => {
                multi(v.vector_dim(), v.multi_vector_config())
            }
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => {
                multi(v.vector_dim(), v.multi_vector_config())
            }
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => {
                multi(v.vector_dim(), v.multi_vector_config())
            }
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => {
                multi(v.vector_dim(), v.multi_vector_config())
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => {
                multi(v.vector_dim(), v.multi_vector_config())
            }
            VectorStorageEnum::DenseTurboMemmap(_)
            | VectorStorageEnum::DenseTurboGraphInline(_)
            | VectorStorageEnum::DenseTurboAppendableMemmap(_)
            | VectorStorageEnum::MultiDenseTurbo(_)
            | VectorStorageEnum::SparseVolatile(_)
            | VectorStorageEnum::SparseMmap(_)
            | VectorStorageEnum::EmptyDense(_)
            | VectorStorageEnum::EmptySparse(_) => None,
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseTurboUring(_) => None,
        }
    }

    /// An empty appendable storage in `dir` that takes the storage-native bytes of this
    /// layout through `VectorStorageEnum::insert_vector_bytes`.
    pub fn create_storage(&self, dir: &Path) -> OperationResult<VectorStorageEnum> {
        let Self {
            dim,
            datatype,
            distance,
            multivector_config,
        } = *self;
        let config = VectorDataConfig {
            size: dim,
            distance,
            storage_type: VectorStorageType::InRamChunkedMmap,
            index: Indexes::Plain {},
            quantization_config: None,
            multivector_config,
            datatype: Some(datatype),
        };
        open_vector_storage_for_remote_build(&config, dir)
    }
}
