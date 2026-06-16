use std::path::Path;

use common::mmap::AdviceSetting;
use common::universal_io::UniversalRead;

use super::ReadOnlyChunkedMultiDenseVectorStorage;
use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::types::{Distance, MultiVectorConfig};
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::{
    DELETED_DIR_PATH, OFFSETS_DIR_PATH, VECTORS_DIR_PATH,
};

impl<T: PrimitiveVectorElement, S: UniversalRead> ReadOnlyChunkedMultiDenseVectorStorage<T, S> {
    /// Open the read-only counterpart of the appendable multi-dense storage at
    /// `path`, threading every file open through `fs`; reads the existing layout
    /// but creates and writes nothing. `populate` warms the vector and offset
    /// chunks.
    #[allow(dead_code)] // pending: read-only vector storage enum will use this
    pub fn open(
        fs: &S::Fs,
        path: &Path,
        dim: usize,
        distance: Distance,
        multi_vector_config: MultiVectorConfig,
        advice: AdviceSetting,
        populate: bool,
    ) -> OperationResult<Self> {
        let vectors = ChunkedVectorsRead::open(
            fs,
            &path.join(VECTORS_DIR_PATH),
            dim,
            advice,
            Some(populate),
        )?;

        // Offsets store one `MultivectorMmapOffset` element per point, so the
        // chunked storage dimensionality is 1.
        let offsets =
            ChunkedVectorsRead::open(fs, &path.join(OFFSETS_DIR_PATH), 1, advice, Some(populate))?;

        let deleted = InMemoryBitvecFlags::open::<S>(fs, &path.join(DELETED_DIR_PATH))?;

        Ok(Self {
            vectors,
            offsets,
            deleted,
            distance,
            multi_vector_config,
        })
    }
}
