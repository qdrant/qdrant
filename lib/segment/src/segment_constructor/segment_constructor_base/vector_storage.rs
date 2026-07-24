use std::path::Path;

use common::mmap::{Advice, AdviceSetting};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::{
    SparseVectorStorageType, VectorDataConfig, VectorStorageDatatype, VectorStorageType,
};
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::dense::dense_vector_storage::{
    open_dense_vector_storage, open_dense_vector_storage_byte, open_dense_vector_storage_half,
};
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::{
    open_appendable_memmap_multi_vector_storage, open_appendable_memmap_vector_storage,
};
use crate::vector_storage::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
use crate::vector_storage::turbo::open_turbo_vector_storage;

fn open_mmap_vector_storage(
    vector_storage_path: &Path,
    vector_config: &VectorDataConfig,
    madvise: AdviceSetting,
    populate: bool,
) -> OperationResult<VectorStorageEnum> {
    let storage_element_type = vector_config.datatype.unwrap_or_default();
    if let Some(multi_vec_config) = &vector_config.multivector_config {
        // there are no mmap multi vector storages, appendable only
        open_appendable_memmap_multi_vector_storage(
            storage_element_type,
            vector_storage_path,
            vector_config.size,
            vector_config.distance,
            *multi_vec_config,
            madvise,
            populate,
        )
    } else {
        match storage_element_type {
            VectorStorageDatatype::Float32 => open_dense_vector_storage(
                vector_storage_path,
                vector_config.size,
                vector_config.distance,
                populate,
            ),
            VectorStorageDatatype::Uint8 => open_dense_vector_storage_byte(
                vector_storage_path,
                vector_config.size,
                vector_config.distance,
                populate,
            ),
            VectorStorageDatatype::Float16 => open_dense_vector_storage_half(
                vector_storage_path,
                vector_config.size,
                vector_config.distance,
                populate,
            ),
            VectorStorageDatatype::Turbo4 => open_turbo_vector_storage(
                vector_storage_path,
                vector_config.size,
                vector_config.distance,
                populate,
            ),
        }
    }
}

fn open_chunked_mmap_vector_storage(
    vector_storage_path: &Path,
    vector_config: &VectorDataConfig,
    madvise: AdviceSetting,
    populate: bool,
) -> OperationResult<VectorStorageEnum> {
    let storage_element_type = vector_config.datatype.unwrap_or_default();
    if let Some(multi_vec_config) = &vector_config.multivector_config {
        open_appendable_memmap_multi_vector_storage(
            storage_element_type,
            vector_storage_path,
            vector_config.size,
            vector_config.distance,
            *multi_vec_config,
            madvise,
            populate,
        )
    } else {
        open_appendable_memmap_vector_storage(
            storage_element_type,
            vector_storage_path,
            vector_config.size,
            vector_config.distance,
            madvise,
            populate,
        )
    }
}

pub(crate) fn open_vector_storage(
    vector_config: &VectorDataConfig,
    vector_storage_path: &Path,
) -> OperationResult<VectorStorageEnum> {
    match vector_config.storage_type {
        VectorStorageType::Memory => Err(OperationError::service_error(
            "Failed to load 'Memory' storage type, RocksDB is not supported in this Qdrant version",
        )),

        // Mmap on disk, not appendable
        VectorStorageType::Mmap => open_mmap_vector_storage(
            vector_storage_path,
            vector_config,
            AdviceSetting::Global,
            false,
        ),
        VectorStorageType::InRamMmap => open_mmap_vector_storage(
            vector_storage_path,
            vector_config,
            AdviceSetting::from(Advice::Normal),
            true,
        ),

        // Chunked mmap on disk, appendable
        VectorStorageType::ChunkedMmap => open_chunked_mmap_vector_storage(
            vector_storage_path,
            vector_config,
            AdviceSetting::Global,
            false,
        ),
        VectorStorageType::InRamChunkedMmap => open_chunked_mmap_vector_storage(
            vector_storage_path,
            vector_config,
            AdviceSetting::from(Advice::Normal),
            true,
        ),

        // Empty placeholder storage, no files on disk
        VectorStorageType::Empty => {
            use crate::vector_storage::dense::empty_dense_vector_storage::new_empty_dense_vector_storage;
            Ok(new_empty_dense_vector_storage(
                vector_config.size,
                vector_config.distance,
                vector_config.datatype.unwrap_or_default(),
                vector_config.storage_type.is_on_disk(),
                vector_config.multivector_config,
                0, // num_points set after id_tracker is loaded
            ))
        }
    }
}

pub(crate) fn create_sparse_vector_storage(
    path: &Path,
    storage_type: &SparseVectorStorageType,
) -> OperationResult<VectorStorageEnum> {
    match storage_type {
        SparseVectorStorageType::Mmap => {
            let mmap_storage = MmapSparseVectorStorage::open_or_create(path)?;
            Ok(VectorStorageEnum::SparseMmap(mmap_storage))
        }
        SparseVectorStorageType::Empty => {
            use crate::vector_storage::sparse::empty_sparse_vector_storage::new_empty_sparse_vector_storage;
            Ok(new_empty_sparse_vector_storage(0))
        }
    }
}
