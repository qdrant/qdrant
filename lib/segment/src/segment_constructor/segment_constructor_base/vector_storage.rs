use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::{Advice, AdviceSetting};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::{
    Memory, SparseVectorStorageType, VectorDataConfig, VectorStorageDatatype, VectorStorageType,
};
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::dense::dense_vector_storage::{
    open_dense_vector_storage, open_dense_vector_storage_byte, open_dense_vector_storage_half,
};
use crate::vector_storage::graph_inline::open_graph_inline_vector_storage;
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::{
    open_appendable_memmap_multi_vector_storage, open_appendable_memmap_vector_storage,
};
use crate::vector_storage::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
use crate::vector_storage::turbo::{open_turbo_vector_storage, tq_bits};

fn open_mmap_vector_storage(
    vector_storage_path: &Path,
    vector_config: &VectorDataConfig,
    madvise: AdviceSetting,
    memory: Memory,
    hw_counter: &HardwareCounterCell,
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
            memory.populate_on_open(),
            hw_counter,
        )
    } else {
        // These pick their IO backend from the placement, so they take it whole rather than
        // the populate flag derived from it.
        match storage_element_type {
            VectorStorageDatatype::Float32 => open_dense_vector_storage(
                vector_storage_path,
                vector_config.size,
                vector_config.distance,
                memory,
                hw_counter,
            ),
            VectorStorageDatatype::Uint8 => open_dense_vector_storage_byte(
                vector_storage_path,
                vector_config.size,
                vector_config.distance,
                memory,
                hw_counter,
            ),
            VectorStorageDatatype::Float16 => open_dense_vector_storage_half(
                vector_storage_path,
                vector_config.size,
                vector_config.distance,
                memory,
                hw_counter,
            ),
            VectorStorageDatatype::Turbo4 | VectorStorageDatatype::Turbo8 => {
                open_turbo_vector_storage(
                    vector_storage_path,
                    vector_config.size,
                    vector_config.distance,
                    tq_bits(storage_element_type),
                    memory,
                    hw_counter,
                )
            }
        }
    }
}

fn open_chunked_mmap_vector_storage(
    vector_storage_path: &Path,
    vector_config: &VectorDataConfig,
    madvise: AdviceSetting,
    populate: bool,
    hw_counter: &HardwareCounterCell,
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
            hw_counter,
        )
    } else {
        open_appendable_memmap_vector_storage(
            storage_element_type,
            vector_storage_path,
            vector_config.size,
            vector_config.distance,
            madvise,
            populate,
            hw_counter,
        )
    }
}

pub(crate) fn open_vector_storage(
    vector_config: &VectorDataConfig,
    vector_storage_path: &Path,
    vector_index_path: &Path,
    hw_counter: &HardwareCounterCell,
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
            Memory::Cold,
            hw_counter,
        ),
        VectorStorageType::InRamMmap => open_mmap_vector_storage(
            vector_storage_path,
            vector_config,
            AdviceSetting::from(Advice::Normal),
            Memory::Cached,
            hw_counter,
        ),

        // Chunked mmap on disk, appendable
        VectorStorageType::ChunkedMmap => open_chunked_mmap_vector_storage(
            vector_storage_path,
            vector_config,
            AdviceSetting::Global,
            false,
            hw_counter,
        ),
        VectorStorageType::InRamChunkedMmap => open_chunked_mmap_vector_storage(
            vector_storage_path,
            vector_config,
            AdviceSetting::from(Advice::Normal),
            true,
            hw_counter,
        ),

        // Vectors inlined into the HNSW links file
        VectorStorageType::GraphInline => open_graph_inline_vector_storage(
            vector_storage_path,
            vector_index_path,
            vector_config,
            hw_counter,
        ),
    }
}

pub(crate) fn create_sparse_vector_storage(
    path: &Path,
    storage_type: &SparseVectorStorageType,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<VectorStorageEnum> {
    match storage_type {
        SparseVectorStorageType::Mmap => {
            let mmap_storage = MmapSparseVectorStorage::open_or_create(path, hw_counter)?;
            Ok(VectorStorageEnum::SparseMmap(mmap_storage))
        }
    }
}
