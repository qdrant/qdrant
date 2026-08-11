use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UniversalReadFs};

use super::super::chunks::read_chunks_from;
use super::super::config::{read_status_len, status_file};
use super::ReadOnlyChunkedVectors;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;

impl<T: bytemuck::Pod + Send, S: UniversalRead> LiveReload for ReadOnlyChunkedVectors<T, S> {
    type File = S;

    /// Refresh the chunks that can have gained vectors since the last load; a
    /// no-op when the length is unchanged (the status file is read fresh, so
    /// it is a reliable change signal).
    ///
    /// Chunk files are preallocated to full size, so appended vectors are
    /// in-place writes *within* the existing file length — which a held
    /// handle's cached blocks never reflect on caching backends (a block
    /// fetched earlier extends past the old tail into then-unwritten space).
    /// So, mirroring `Pages::live_reload`, the last held chunk — the only one
    /// that can have gained vectors — is dropped and re-opened fresh,
    /// alongside adopting newly created chunk files. Deletions/new points are
    /// tracked by callers, so they are unused here.
    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        _deleted_points: &SortedSlice<'_, PointOffsetType>,
        _new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let new_len = read_status_len(fs, &status_file(&self.directory))?;
        if new_len == self.len {
            return Ok(());
        }

        let reload_from = self.chunks.len().saturating_sub(1);
        let new_chunks = read_chunks_from(
            fs,
            &self.directory,
            reload_from,
            self.advice,
            self.populate,
            false,
        )?;
        self.chunks.truncate(reload_from);
        self.chunks.extend(new_chunks);
        self.len = new_len;
        Ok(())
    }
}
