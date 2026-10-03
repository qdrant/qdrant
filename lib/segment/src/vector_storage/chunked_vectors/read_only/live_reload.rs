use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::{Advice, AdviceSetting};
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkUnchanged, OpenOptions, Populate, TypedStorage, UniversalRead, UniversalReadFs,
    UniversalReadFsAsync,
};
use futures::FutureExt;
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
        let status_path = status_file(&self.directory);

        // Status is the change signal.
        if fs.is_file_unchanged(&status_path) {
            // This will let `live_reload` know that nothing changed.
            fs.reschedule_open(&status_path, None, None);
            return Ok(Vec::new());
        }

        let (status_tx, status_rx) = futures::channel::oneshot::channel::<()>();
        let status_rx = status_rx.shared();

        let inner = fs.inner().clone();
        let path = status_path.clone();
        let status_fut = async move {
            let res = inner
                .open_async(
                    path,
                    OpenOptions {
                        writeable: false,
                        need_sequential: false,
                        populate: Populate::PreferBackground,
                        advice: AdviceSetting::Advice(Advice::Sequential),
                    },
                    Default::default(),
                )
                .await;
            let _ = status_tx.send(());
            res
        };
        fs.schedule(status_path, status_fut.boxed());

        let num_files = list_chunk_files(fs, &self.directory)?.len();

        // `len` marks max committed vector. First chunk that can have changed:
        // the one the next append lands in.
        let last_chunk = self.config.get_chunk_index(self.len);

        for chunk_id in last_chunk..num_files {
            let chunk_path = chunk_name(&self.directory, chunk_id);
            let options = chunk_open_options(self.advice, self.populate, false);
            let inner = fs.inner().clone();
            let signal = status_rx.clone();

            let chunk_fut = async move {
                // Schedule chunks AFTER status has been read to have a consistent view.
                let _ = signal.await;
                inner
                    .open_async(chunk_path, options, Default::default())
                    .await
            };

            fs.schedule(chunk_name(&self.directory, chunk_id), chunk_fut.boxed());
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
        let Some(new_len) = read_status_len(fs, &status_file(&self.directory)).ok_unchanged()?
        else {
            return Ok(());
        };

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
        self.len = new_len;
        Ok(())
    }
}
