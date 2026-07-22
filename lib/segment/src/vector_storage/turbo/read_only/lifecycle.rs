use std::path::Path;

use common::mmap::AdviceSetting;
use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs};
use quantization::turboquant::quantization::TurboQuantizer;

use super::super::multi::OFFSETS_PATH;
use super::super::{DELETED_PATH, TQDT_BITS, TQDT_MODE, TQDT_ROTATION, VECTORS_PATH};
use super::{ReadOnlyTurboEncoded, ReadOnlyTurboMultiVectorStorage, ReadOnlyTurboVectorStorage};
use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::common::operation_error::OperationResult;
use crate::types::{Distance, MultiVectorConfig};
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::MultivectorMmapOffset;
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorageRead;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;

impl<S: UniversalRead> ReadOnlyTurboVectorStorage<S> {
    /// Build the quantizer for a `Turbo4` storage; fully determined by
    /// `(dim, distance)` and the fixed TQDT constants (matches the writable
    /// `open_turbo_vector_storage_impl`).
    pub(super) fn build_quantizer(dim: usize, distance: Distance) -> TurboQuantizer {
        TurboQuantizer::new(
            dim,
            TQDT_BITS,
            TQDT_MODE,
            distance.into(),
            TQDT_ROTATION,
            None,
        )
    }

    /// Schedule background prefetch of the files [`Self::open`] will read.
    ///
    /// Absent files are skipped rather than reported: the subsequent open is
    /// the one to produce the error.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        chunked: bool,
        populate: Populate,
    ) -> OperationResult<()> {
        let vectors_path = path.join(VECTORS_PATH);
        if chunked {
            QuantizedChunkedStorageRead::<S>::preopen(fs, &vectors_path, populate)?;
        } else {
            QuantizedStorage::<S>::preopen(fs, &vectors_path, populate)?;
        }
        InMemoryBitvecFlags::preopen(fs, &path.join(DELETED_PATH))?;
        Ok(())
    }

    /// Open the read-only counterpart of a `Turbo4` dense storage at `path`,
    /// threading every file open through `fs`; reads the existing layout but
    /// creates and writes nothing. `chunked` selects the on-disk layout the
    /// writable storage produced for the segment's storage type.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        dim: usize,
        distance: Distance,
        chunked: bool,
        populate: Populate,
    ) -> OperationResult<Self> {
        let quantizer = Self::build_quantizer(dim, distance);
        let quantized_vector_size = quantizer.quantized_size();
        let vectors_path = path.join(VECTORS_PATH);

        let storage = if chunked {
            ReadOnlyTurboEncoded::Chunked(QuantizedChunkedStorageRead::open(
                fs,
                &vectors_path,
                quantized_vector_size,
            )?)
        } else {
            ReadOnlyTurboEncoded::Single(QuantizedStorage::from_file(
                fs,
                &vectors_path,
                quantized_vector_size,
            )?)
        };

        // The read-only backends map lazily; warm them when the load profile asks.
        if !matches!(populate, Populate::No) {
            storage.populate()?;
        }

        let deleted = InMemoryBitvecFlags::open::<S>(fs, &path.join(DELETED_PATH))?;

        Ok(Self {
            storage,
            quantizer,
            deleted,
            distance,
            dim,
        })
    }
}

impl<S: UniversalRead> ReadOnlyTurboMultiVectorStorage<S> {
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
        let quantizer = ReadOnlyTurboVectorStorage::<S>::build_quantizer(dim, distance);

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
