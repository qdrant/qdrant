use std::path::Path;

use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::HnswGraph;
use crate::types::{VectorDataConfig, VectorStorageDatatype};
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::dense::graph_inline_dense_vector_storage::GraphInlineDenseVectorStorage;
use crate::vector_storage::turbo::TurboVectorStorageImpl;

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
