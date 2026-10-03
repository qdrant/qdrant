use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkUnchanged, TypedStorage, UniversalRead, UniversalReadFs,
};
use futures::future::BoxFuture;

use super::super::chunks::{chunk_name, chunk_open_options, list_chunk_files, read_chunks_from};
use super::super::config::{read_status_len, status_file};
use super::ReadOnlyChunkedVectors;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;

impl<T: bytemuck::Pod + Send, S: UniversalRead> LiveReload for ReadOnlyChunkedVectors<T, S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        // Status is the change signal, let reload skip reloading if this didn't change.
        fs.reschedule_open(&status_file(&self.directory), None, None);

        let num_files = list_chunk_files(fs, &self.directory)?.len();

        // `len` marks max committed vector. First chunk that can have changed:
        // the one the next append lands in.
        let last_chunk = self.config.get_chunk_index(self.len);

        let fresh_from = if last_chunk < self.chunks.len().min(num_files) {
            fs.reschedule_open(
                &chunk_name(&self.directory, last_chunk),
                Some(chunk_open_options(self.advice, self.populate, false)),
                None,
            );
            last_chunk + 1
        } else {
            last_chunk
        };

        // Prefetch the rest of the chunks the reload may open.
        for chunk_id in fresh_from..num_files {
            fs.schedule_open(
                &chunk_name(&self.directory, chunk_id),
                Some(chunk_open_options(self.advice, self.populate, false)),
                None,
            );
        }
        Ok(Vec::new())
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
        let new_len = match read_status_len(fs, &status_file(&self.directory)).ok_unchanged()? {
            Some(new_len) => new_len,
            // An unchanged status still owes whatever a previous reload capped.
            None if self.status_len > self.len => self.status_len,
            None => return Ok(()),
        };
        self.status_len = new_len;

        // Same len is also no-op
        if new_len == self.len {
            return Ok(());
        }
        if new_len < self.len {
            return Err(
                crate::common::operation_error::OperationError::service_error(
                    "live_reload only supports appends",
                ),
            );
        }

        // First chunk that can have changed: the one committed by `len`
        let last_chunk = self.config.get_chunk_index(self.len);

        let fresh_from = if last_chunk < self.chunks.len() {
            if let Some(fresh_chunk) = TypedStorage::open(
                fs,
                &chunk_name(&self.directory, last_chunk),
                chunk_open_options(self.advice, self.populate, false),
                Default::default(),
            )
            .ok_unchanged()?
            {
                // Fresh handle for the watermark chunk
                self.chunks[last_chunk] = fresh_chunk;
            }
            last_chunk + 1
        } else {
            last_chunk
        };

        let new_chunks = read_chunks_from(
            fs,
            &self.directory,
            fresh_from,
            self.advice,
            self.populate,
            false,
        )?;

        self.chunks.truncate(fresh_from);
        self.chunks.extend(new_chunks);
        // The status can be visible before the bytes it counts; the next
        // reload sees it still ahead of `len` and picks up the rest.
        self.len = new_len.min(self.served_len()?);
        Ok(())
    }
}
