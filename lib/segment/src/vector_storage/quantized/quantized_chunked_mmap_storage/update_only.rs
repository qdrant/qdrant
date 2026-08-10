use std::borrow::Cow;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::MmapFlusher;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppend;
use quantization::{EncodedStorage, EncodedStorageBuilder};

use crate::common::operation_error::OperationResult;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::update_only::UpdateOnlyChunkedVectors;

/// Not the hot read path — that goes through the promoted read-only segment, opened as a
/// [`QuantizedChunkedStorage`](super::QuantizedChunkedStorage) over the same on-disk files (the
/// two share the chunk/status file layout). The only read this storage itself needs to serve is
/// [`EncodedVectorsBin::load`]/[`EncodedVectorsTQ::load`]'s one-vector size validation on reopen
/// — `for_each_batch`, `files`, and `immutable_files` are never called on the write path.
///
/// [`EncodedVectorsBin::load`]: quantization::encoded_vectors_binary::EncodedVectorsBin::load
/// [`EncodedVectorsTQ::load`]: quantization::encoded_vectors_tq::EncodedVectorsTQ::load
const NOT_READABLE: &str = "UpdateOnlyQuantizedChunkedStorage only supports reading back a \
                             single vector by index (needed to reopen); batch/file reads go \
                             through the promoted read-only segment, never through this handle";

/// Append-only counterpart of [`QuantizedChunkedStorage`](super::QuantizedChunkedStorage), for
/// the update-only write path.
///
/// A quantized vector persists through [`UpdateOnlyChunkedVectors`] — the same append-only,
/// fresh-slot-per-upsert storage [`UpdateOnlyDenseVectorStorage`] uses for raw vectors — instead
/// of the positional writes `ChunkedVectors::insert` needs, so this only requires
/// `S: UniversalAppend`, not `UniversalWrite`. It writes files in the exact layout
/// `QuantizedChunkedStorage` reads, so no new reading code is needed once a segment is promoted.
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

impl<S: UniversalAppend + 'static> EncodedStorage for UpdateOnlyQuantizedChunkedStorage<S> {
    fn get_vector_data(&self, index: PointOffsetType) -> Cow<'_, [u8]> {
        self.get_vector_data_opt(index).unwrap_or_default()
    }

    fn get_vector_data_opt(&self, index: PointOffsetType) -> Option<Cow<'_, [u8]>> {
        if index as usize >= self.vectors_count() {
            return None;
        }
        let vector = self
            .vectors
            .get(index as VectorOffsetType)
            .expect("index is within vectors_count(), so its chunk must exist and be readable");
        Some(Cow::Owned(vector))
    }

    fn for_each_batch(
        &self,
        _offsets: &[PointOffsetType],
        _callback: impl FnMut(usize, Cow<'_, [u8]>),
    ) {
        unreachable!("{NOT_READABLE}")
    }

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
        // Update-only never rewrites a slot in place (every upsert clones to a fresh one), so
        // `id` is always the current end of the storage — a genuine append, matching what
        // `UpdateOnlyChunkedVectors::append_many` requires of `start_key`.
        self.vectors
            .append_many(id as VectorOffsetType, std::iter::once(vector), hw_counter)
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

    fn files(&self) -> Vec<PathBuf> {
        unreachable!("{NOT_READABLE}")
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        unreachable!("{NOT_READABLE}")
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
