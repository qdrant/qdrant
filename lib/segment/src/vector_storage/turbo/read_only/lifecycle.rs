use std::path::Path;

use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs};

use super::super::shared::{self, DELETED_DIR_PATH, VECTORS_DIR_PATH};
use super::ReadOnlyChunkedTurboVectorStorage;
use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::common::operation_error::OperationResult;
use crate::types::Distance;
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorageRead;

impl<S: UniversalRead> ReadOnlyChunkedTurboVectorStorage<S> {
    /// Schedule background prefetch of the files [`Self::open`] will read.
    ///
    /// Absent files are skipped rather than reported: the subsequent open is
    /// the one to produce the error.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        populate: Populate,
    ) -> OperationResult<()> {
        QuantizedChunkedStorageRead::<S>::preopen(fs, &path.join(VECTORS_DIR_PATH), populate)?;
        InMemoryBitvecFlags::preopen(fs, &path.join(DELETED_DIR_PATH))?;
        Ok(())
    }

    /// Open the read-only counterpart of a chunked `Turbo4` dense storage at
    /// `path`, threading every file open through `fs`; reads the existing layout
    /// but creates and writes nothing.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        dim: usize,
        distance: Distance,
        populate: Populate,
    ) -> OperationResult<Self> {
        let quantizer = shared::build_quantizer(dim, distance);
        let storage = QuantizedChunkedStorageRead::open(
            fs,
            &path.join(VECTORS_DIR_PATH),
            quantizer.quantized_size(),
        )?;

        // The chunked read backend maps lazily; warm it when the profile asks.
        if !matches!(populate, Populate::No) {
            storage.populate()?;
        }

        let deleted = InMemoryBitvecFlags::open::<S>(fs, &path.join(DELETED_DIR_PATH))?;

        Ok(Self {
            storage,
            quantizer,
            deleted,
            distance,
            dim,
        })
    }
}
