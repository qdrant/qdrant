use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::{ReadOnlyQuantizedVectorStorage, ReadOnlyQuantizedVectors};
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;

impl<S: UniversalRead> LiveReload for ReadOnlyQuantizedVectors<S> {
    type Fs = S::Fs;

    /// Reload appended quantized vectors from disk (chunked layouts only).
    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage_impl
            .live_reload(fs, deleted_points, new_points, hw_counter)
    }
}

impl<S: UniversalRead> LiveReload for ReadOnlyQuantizedVectorStorage<S> {
    type Fs = S::Fs;

    /// Pick up quantized vectors a writer appended. Only the chunked (appendable)
    /// layouts grow; Ram/Mmap are immutable, so they no-op. Deletions aren't
    /// tracked here — they live in the raw vector storage.
    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyQuantizedVectorStorage::ScalarRam(_)
            | ReadOnlyQuantizedVectorStorage::ScalarMmap(_)
            | ReadOnlyQuantizedVectorStorage::PQRam(_)
            | ReadOnlyQuantizedVectorStorage::PQMmap(_)
            | ReadOnlyQuantizedVectorStorage::BinaryRam(_)
            | ReadOnlyQuantizedVectorStorage::BinaryMmap(_)
            | ReadOnlyQuantizedVectorStorage::TQRam(_)
            | ReadOnlyQuantizedVectorStorage::TQMmap(_)
            | ReadOnlyQuantizedVectorStorage::ScalarRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::ScalarMmapMulti(_)
            | ReadOnlyQuantizedVectorStorage::PQRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::PQMmapMulti(_)
            | ReadOnlyQuantizedVectorStorage::BinaryRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::BinaryMmapMulti(_)
            | ReadOnlyQuantizedVectorStorage::TQRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::TQMmapMulti(_) => {}
            ReadOnlyQuantizedVectorStorage::BinaryChunked(q) => {
                q.storage_mut()
                    .live_reload(fs, deleted_points, new_points, hw_counter)?
            }
            ReadOnlyQuantizedVectorStorage::TQChunked(q) => {
                q.storage_mut()
                    .live_reload(fs, deleted_points, new_points, hw_counter)?
            }
            ReadOnlyQuantizedVectorStorage::BinaryChunkedMulti(q) => {
                q.storage_mut().storage_mut().live_reload(
                    fs,
                    deleted_points,
                    new_points,
                    hw_counter,
                )?;
                q.offsets_storage_mut()
                    .live_reload(fs, deleted_points, new_points, hw_counter)?;
            }
            ReadOnlyQuantizedVectorStorage::TQChunkedMulti(q) => {
                q.storage_mut().storage_mut().live_reload(
                    fs,
                    deleted_points,
                    new_points,
                    hw_counter,
                )?;
                q.offsets_storage_mut()
                    .live_reload(fs, deleted_points, new_points, hw_counter)?;
            }
        }
        Ok(())
    }
}
