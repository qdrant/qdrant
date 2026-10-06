use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs};
use futures::future::BoxFuture;

use super::storage::{BinaryChunkedMulti, TQChunkedMulti};
use super::{ReadOnlyQuantizedVectorStorage, ReadOnlyQuantizedVectors};
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::{OperationError, OperationResult};

impl<S: UniversalRead> LiveReload for ReadOnlyQuantizedVectors<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        self.storage_impl.live_preload(fs)
    }

    /// Reload appended quantized vectors from disk (chunked layouts only).
    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage_impl
            .live_reload(fs, deleted_points, new_points, hw_counter)
    }
}

impl<S: UniversalRead> LiveReload for ReadOnlyQuantizedVectorStorage<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        let mut futs = Vec::new();
        match self {
            // Ram/Mmap layouts are immutable: nothing to stage.
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
                futs.extend(q.storage().live_preload(fs)?);
            }
            ReadOnlyQuantizedVectorStorage::TQChunked(q) => {
                futs.extend(q.storage().live_preload(fs)?);
            }
            ReadOnlyQuantizedVectorStorage::BinaryChunkedMulti(q) => {
                futs.extend(q.storage().storage().live_preload(fs)?);
                futs.extend(q.offsets_storage().live_preload(fs)?);
            }
            ReadOnlyQuantizedVectorStorage::TQChunkedMulti(q) => {
                futs.extend(q.storage().storage().live_preload(fs)?);
                futs.extend(q.offsets_storage().live_preload(fs)?);
            }
        }
        Ok(futs)
    }

    /// Pick up quantized vectors a writer appended. Only the chunked (appendable)
    /// layouts grow; Ram/Mmap are immutable, so they no-op. Deletions aren't
    /// tracked here — they live in the raw vector storage.
    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
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
                live_reload_binary_multi(q, fs, deleted_points, new_points, hw_counter)?;
            }
            ReadOnlyQuantizedVectorStorage::TQChunkedMulti(q) => {
                live_reload_tq_multi(q, fs, deleted_points, new_points, hw_counter)?;
            }
        }
        Ok(())
    }
}

fn live_reload_binary_multi<S: UniversalRead, Fs: UniversalReadFs<File = S>>(
    q: &mut BinaryChunkedMulti<S>,
    fs: &Fs,
    deleted_points: &SortedSlice<'_, PointOffsetType>,
    new_points: &SortedSlice<'_, PointOffsetType>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<()> {
    q.offsets_storage_mut()
        .live_reload(fs, deleted_points, new_points, hw_counter)?;
    if let Some(&last_point) = new_points.last() {
        let offset = q
            .offsets_storage()
            .get_offset_opt(last_point)
            .ok_or_else(|| {
                OperationError::service_error(format!(
                    "Offset of published point {last_point} is missing",
                ))
            })?;
        q.storage_mut()
            .storage_mut()
            .live_reload_to(fs, (offset.start + offset.count) as usize)?;
    }
    Ok(())
}

fn live_reload_tq_multi<S: UniversalRead, Fs: UniversalReadFs<File = S>>(
    q: &mut TQChunkedMulti<S>,
    fs: &Fs,
    deleted_points: &SortedSlice<'_, PointOffsetType>,
    new_points: &SortedSlice<'_, PointOffsetType>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<()> {
    q.offsets_storage_mut()
        .live_reload(fs, deleted_points, new_points, hw_counter)?;
    if let Some(&last_point) = new_points.last() {
        let offset = q
            .offsets_storage()
            .get_offset_opt(last_point)
            .ok_or_else(|| {
                OperationError::service_error(format!(
                    "Offset of published point {last_point} is missing",
                ))
            })?;
        q.storage_mut()
            .storage_mut()
            .live_reload_to(fs, (offset.start + offset.count) as usize)?;
    }
    Ok(())
}
