// Not wired into a segment yet — that lands in a later PR in this stack, so nothing outside
// this module's own tests constructs these types today.
#![expect(dead_code)]

use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::MmapFlusher;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppend;
use quantization::{EncodedStorageBuilder, EncodedStorageWrite};

use crate::common::operation_error::OperationResult;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::update_only::UpdateOnlyChunkedVectors;

/// Append-only counterpart of [`QuantizedChunkedStorage`](super::QuantizedChunkedStorage), for
/// the update-only write path.
///
/// A quantized vector persists through [`UpdateOnlyChunkedVectors`] — the same append-only,
/// fresh-slot-per-upsert storage [`UpdateOnlyDenseVectorStorage`] uses for raw vectors — instead
/// of the positional writes `ChunkedVectors::insert` needs, so this only requires
/// `S: UniversalAppend`, not `UniversalWrite`. It writes files in the exact layout
/// `QuantizedChunkedStorage` reads, so no new reading code is needed once a segment is promoted.
///
/// Implements [`EncodedStorageWrite`], not the full [`EncodedStorage`](quantization::EncodedStorage):
/// reads always go through the promoted read-only segment (opened as a
/// [`QuantizedChunkedStorageRead`](super::QuantizedChunkedStorageRead) over the same on-disk
/// files), never through this handle, so there is no read half to implement — write-only is
/// enforced by the trait this implements, not by panicking stand-ins for methods that don't
/// exist.
///
/// [`UpdateOnlyDenseVectorStorage`]: crate::vector_storage::dense::update_only::UpdateOnlyDenseVectorStorage
pub struct UpdateOnlyQuantizedChunkedStorage<S: UniversalAppend + 'static> {
    vectors: UpdateOnlyChunkedVectors<u8, S>,
}

impl<S: UniversalAppend + 'static> UpdateOnlyQuantizedChunkedStorage<S> {
    pub fn open(fs: S::Fs, path: &Path, quantized_vector_size: usize) -> OperationResult<Self> {
        Ok(Self {
            vectors: UpdateOnlyChunkedVectors::open(fs, path, quantized_vector_size)?,
        })
    }
}

impl<S: UniversalAppend + 'static> EncodedStorageWrite for UpdateOnlyQuantizedChunkedStorage<S> {
    fn is_in_ram_or_mmap() -> bool {
        false
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        self.upsert_many(id, std::iter::once(vector), hw_counter)
    }

    fn upsert_many<'a, I>(
        &mut self,
        start_id: PointOffsetType,
        vectors: I,
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()>
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: ExactSizeIterator,
    {
        // Update-only never rewrites a slot in place (every upsert clones to a fresh one), so
        // `id` is always the current end of the storage — a genuine append, matching what
        // `UpdateOnlyChunkedVectors::append_many` requires of `start_key`.
        self.vectors
            .append_many(start_id as VectorOffsetType, vectors, hw_counter)
            .map_err(std::io::Error::other)
    }

    fn vectors_count(&self) -> usize {
        self.vectors.stored_len().unwrap_or(0)
    }

    fn flusher(&self) -> MmapFlusher {
        // `append_many` persists synchronously before returning, so there is nothing left to
        // flush — the same contract `UpdateOnlyDenseVectorStorage` relies on for raw vectors.
        Box::new(|| Ok(()))
    }

    fn heap_size_bytes(&self) -> usize {
        0
    }
}

/// Builder counterpart, used once at segment creation to open an empty overlay — an update-only
/// segment always starts with zero quantized vectors, so [`build`](Self::build) never has
/// [`push_vector_data`](Self::push_vector_data) called on it.
pub struct UpdateOnlyQuantizedChunkedStorageBuilder<S: UniversalAppend + 'static> {
    storage: UpdateOnlyQuantizedChunkedStorage<S>,
}

impl<S: UniversalAppend + 'static> UpdateOnlyQuantizedChunkedStorageBuilder<S> {
    pub fn new(fs: S::Fs, path: &Path, quantized_vector_size: usize) -> OperationResult<Self> {
        Ok(Self {
            storage: UpdateOnlyQuantizedChunkedStorage::open(fs, path, quantized_vector_size)?,
        })
    }
}

impl<S: UniversalAppend + 'static> EncodedStorageBuilder
    for UpdateOnlyQuantizedChunkedStorageBuilder<S>
{
    type Storage = UpdateOnlyQuantizedChunkedStorage<S>;
    type Error = std::io::Error;

    fn build(self) -> std::io::Result<Self::Storage> {
        Ok(self.storage)
    }

    fn push_vector_data(&mut self, _other: &[u8]) -> std::io::Result<()> {
        Err(std::io::Error::other(
            "UpdateOnlyQuantizedChunkedStorageBuilder is only ever built empty: an update-only \
             segment starts with zero quantized vectors and grows through upsert_vector, not \
             through the batch-create path",
        ))
    }
}
