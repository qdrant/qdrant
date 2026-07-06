use std::path::Path;

use common::mmap::AdviceSetting;
use common::universal_io::{CachedReadFs, Populate, UniversalRead};

use super::ReadOnlyChunkedDenseVectorStorage;
use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::types::Distance;
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;
use crate::vector_storage::dense::appendable_dense_vector_storage::{
    DELETED_DIR_PATH, VECTORS_DIR_PATH,
};

impl<T: PrimitiveVectorElement, S: UniversalRead> ReadOnlyChunkedDenseVectorStorage<T, S> {
    /// Open the read-only counterpart of the appendable dense storage at `path`,
    /// threading every file open through `fs`; reads the existing layout but
    /// creates and writes nothing. `populate` warms the vector chunks.
    #[allow(dead_code)] // pending: read-only vector storage enum will use this
    pub fn open(
        fs: &CachedReadFs<S::Fs>,
        path: &Path,
        dim: usize,
        distance: Distance,
        advice: AdviceSetting,
        populate: Populate,
    ) -> OperationResult<Self> {
        let vectors =
            ChunkedVectorsRead::open(fs, &path.join(VECTORS_DIR_PATH), dim, advice, populate)?;

        let deleted = InMemoryBitvecFlags::open::<S>(fs, &path.join(DELETED_DIR_PATH))?;

        Ok(Self {
            vectors,
            deleted,
            distance,
        })
    }
}
