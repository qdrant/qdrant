use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkUnchanged, TypedStorage, UniversalRead, UniversalReadFs,
};

use super::super::chunks::{chunk_name, chunk_open_options, list_chunk_files, read_chunks_from};
use super::super::config::{read_status_len, status_file};
use super::ReadOnlyChunkedVectors;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;

impl<T: bytemuck::Pod + Send, S: UniversalRead> LiveReload for ReadOnlyChunkedVectors<T, S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(&self, fs: &Fs) -> OperationResult<()> {
        // Status is the change signal, let reload skip reloading if this didn't change.
        fs.reschedule_prefetch(&status_file(&self.directory), None, None)?;

        if !self.chunks.is_empty() {
            // Re-schedule so an unchanged last chunk doesn't re-fetch.
            fs.reschedule_prefetch(
                &chunk_name(&self.directory, self.chunks.len() - 1),
                Some(chunk_open_options(self.advice, self.populate, false)),
                None,
            )?;
        }

        // Prefetch all new chunks
        for chunk_id in self.chunks.len()..list_chunk_files(fs, &self.directory)?.len() {
            fs.schedule_prefetch(
                &chunk_name(&self.directory, chunk_id),
                Some(chunk_open_options(self.advice, self.populate, false)),
                None,
            )?;
        }
        Ok(())
    }

    /// Refresh the chunks that can have gained vectors since the last load; a
    /// no-op when the length is unchanged (the status file is saved last, so
    /// it's a reliable change signal).
    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        _deleted_points: &SortedSlice<'_, PointOffsetType>,
        _new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let Some(new_len) = read_status_len(fs, &status_file(&self.directory)).ok_unchanged()?
        else {
            return Ok(());
        };

        // Same len is also no-op
        if new_len == self.len {
            return Ok(());
        }

        if let Some(last) = self.chunks.len().checked_sub(1) {
            // Fresh handle for the last held chunk, if the file actually changed.
            if let Some(fresh) = TypedStorage::open(
                fs,
                &chunk_name(&self.directory, last),
                chunk_open_options(self.advice, self.populate, false),
                Default::default(),
            )
            .ok_unchanged()?
            {
                self.chunks[last] = fresh;
            }
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
