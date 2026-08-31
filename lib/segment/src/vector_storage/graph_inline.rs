use std::path::Path;

use common::universal_io::{MmapFile, MmapFs};

use crate::common::flags::FlagsMode;
use crate::common::flags::bitvec_flags::BitvecFlags;
use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::HnswGraph;
use crate::types::{VectorDataConfig, VectorStorageDatatype};
use crate::vector_storage::dense::graph_inline_dense_vector_storage::GraphInlineDenseVectorStorage;
use crate::vector_storage::turbo::TurboVectorStorageImpl;
use crate::vector_storage::turbo::shared::DELETED_DIR_PATH;
use crate::vector_storage::{VectorStorage, VectorStorageEnum, VectorStorageRead};

pub(crate) fn open_graph_inline_vector_storage(
    path: &Path,
    index_path: &Path,
    vector_config: &VectorDataConfig,
) -> OperationResult<VectorStorageEnum> {
    use VectorStorageDatatype::{Float16, Float32, Turbo4, Uint8};

    let graph = HnswGraph::open(index_path, vector_config.storage_memory())?;
    let dim = vector_config.size;
    let distance = vector_config.distance;
    Ok(match vector_config.datatype.unwrap_or_default() {
        Float32 => VectorStorageEnum::DenseGraphInline(
            GraphInlineDenseVectorStorage::open(graph, path, dim, distance).map(Box::new)?,
        ),
        Float16 => VectorStorageEnum::DenseGraphInlineHalf(
            GraphInlineDenseVectorStorage::open(graph, path, dim, distance).map(Box::new)?,
        ),
        Uint8 => VectorStorageEnum::DenseGraphInlineByte(
            GraphInlineDenseVectorStorage::open(graph, path, dim, distance).map(Box::new)?,
        ),
        Turbo4 => VectorStorageEnum::DenseTurboGraphInline(
            TurboVectorStorageImpl::open_graph(graph, path, dim, distance).map(Box::new)?,
        ),
    })
}

/// Replace the temporary build storage with the final inline-graph storage.
pub(crate) fn finalize(path: &Path, storage: VectorStorageEnum) -> OperationResult<()> {
    let files = storage.files();
    let deleted = storage.deleted_vector_bitslice().to_bitvec();

    // Close the storage (we are the only owner), so we can delete its files.
    drop(storage);

    // Delete old vector storage files: these were temporarely used during the
    // HNSW index building. The actual vector data is expected to be already
    // included in the graph-with-vectors.
    for file in files {
        fs_err::remove_file(file)?;
    }

    // Write deleted flags.
    // Why: we just deleted them along with other files. (an alternative is to
    // not delete them, but it's fragile and makes assumptions about the format)
    BitvecFlags::<MmapFile>::create_from_bitslice(
        MmapFs,
        &path.join(DELETED_DIR_PATH),
        FlagsMode::from_feature_flags(),
        &deleted,
    )?
    .flusher()()
}
