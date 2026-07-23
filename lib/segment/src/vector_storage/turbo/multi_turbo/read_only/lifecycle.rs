use std::path::Path;

use common::mmap::AdviceSetting;
use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs};

use super::ReadOnlyChunkedMultiTurboVectorStorage;
use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::common::operation_error::OperationResult;
use crate::types::{Distance, MultiVectorConfig};
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::MultivectorMmapOffset;
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorageRead;
use crate::vector_storage::turbo::multi_turbo::OFFSETS_PATH;
use crate::vector_storage::turbo::shared::{self, DELETED_PATH, VECTORS_PATH};

impl<S: UniversalRead> ReadOnlyChunkedMultiTurboVectorStorage<S> {
    /// Schedule background prefetch of the files [`Self::open`] will read.
    ///
    /// Absent files are skipped rather than reported: the subsequent open is
    /// the one to produce the error.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        advice: AdviceSetting,
        populate: Populate,
    ) -> OperationResult<()> {
        QuantizedChunkedStorageRead::<S>::preopen(fs, &path.join(VECTORS_PATH), populate)?;
        ChunkedVectorsRead::<MultivectorMmapOffset, S>::preopen(
            fs,
            &path.join(OFFSETS_PATH),
            advice,
            populate,
        )?;
        InMemoryBitvecFlags::preopen(fs, &path.join(DELETED_PATH))?;
        Ok(())
    }

    /// Open the read-only counterpart of a `Turbo4` multivector storage at
    /// `path`, threading every file open through `fs`; reads the existing layout
    /// but creates and writes nothing.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        dim: usize,
        distance: Distance,
        multi_vector_config: MultiVectorConfig,
        advice: AdviceSetting,
        populate: Populate,
    ) -> OperationResult<Self> {
        let quantizer = shared::build_quantizer(dim, distance);

        let storage = QuantizedChunkedStorageRead::open(
            fs,
            &path.join(VECTORS_PATH),
            quantizer.quantized_size(),
        )?;
        // The chunked read backend maps lazily; warm it when the profile asks.
        if !matches!(populate, Populate::No) {
            storage.populate()?;
        }

        let offsets = ChunkedVectorsRead::open(fs, &path.join(OFFSETS_PATH), 1, advice, populate)?;

        let deleted = InMemoryBitvecFlags::open::<S>(fs, &path.join(DELETED_PATH))?;

        Ok(Self {
            storage,
            offsets,
            quantizer,
            deleted,
            distance,
            dim,
            multi_vector_config,
        })
    }
}
