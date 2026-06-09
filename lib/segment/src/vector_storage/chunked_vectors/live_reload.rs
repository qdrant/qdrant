use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::chunks::read_chunks_from;
use super::read::{ChunkedVectorsRead, read_status_len};
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;

impl<T: bytemuck::Pod + Send, S: UniversalRead> LiveReload for ChunkedVectorsRead<T, S> {
    type Fs = S::Fs;

    /// Open only chunk files appended since the last load; a no-op when the
    /// length is unchanged. Chunk files are pre-allocated to full size, so the
    /// chunks already held keep reflecting appended vectors through their mmap.
    /// Deletions/new points are tracked by callers, so they are unused here.
    fn live_reload(
        &mut self,
        fs: &S::Fs,
        _deleted_points: &SortedSlice<'_, PointOffsetType>,
        _new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let new_len = read_status_len(fs, &Self::status_file(&self.directory))?;
        if new_len == self.len {
            return Ok(());
        }

        let new_chunks = read_chunks_from(
            fs,
            &self.directory,
            self.chunks.len(),
            self.advice,
            self.populate,
            false,
        )?;
        self.chunks.extend(new_chunks);
        self.len = new_len;
        Ok(())
    }
}
