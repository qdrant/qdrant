use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkUnchanged, TypedStorage, UniversalRead, UniversalReadFs,
};
use futures::future::BoxFuture;

use super::super::chunks::{chunk_name, chunk_open_options, list_chunk_files, read_chunks_from};
use super::ReadOnlyChunkedVectors;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::{OperationError, OperationResult};

/// Chunk handles opened by a reload, applied only once the new length is known
/// to be covered by them.
struct ReopenedChunks<S, T> {
    /// Chunks below this index are kept as they are.
    fresh_from: usize,
    /// Fresh handle for the chunk the previous length ends in, if it changed.
    watermark: Option<(usize, TypedStorage<S, T>)>,
    new_chunks: Vec<TypedStorage<S, T>>,
}

/// Reload sized from the id tracker delta rather than the status file.
///
/// The status file is replaced atomically at a fixed size, so a caching
/// filesystem can serve it newer than the chunk lengths of the listing snapshot
/// the reload is staged against. The id tracker is capped at that same
/// snapshot, and the writer publishes a point only after its data is durable,
/// so the snapshot chunks always cover what `new_points` refers to. Writers
/// that live reload supports never rewrite an existing offset, so the delta
/// covers all growth.
impl<T: bytemuck::Pod + Send, S: UniversalRead> LiveReload for ReadOnlyChunkedVectors<T, S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        let num_files = list_chunk_files(fs, &self.directory)?.len();
        let last_chunk = self.watermark_chunk();

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

    /// Grow the view to cover every offset in `new_points`, one vector per
    /// point offset.
    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        _deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        match new_points.last() {
            Some(&last_point) => self.live_reload_to(fs, last_point as usize + 1),
            None => Ok(()),
        }
    }
}

impl<T: bytemuck::Pod + Send, S: UniversalRead> ReadOnlyChunkedVectors<T, S> {
    /// Grow the view to `new_len` vectors; a no-op when it already holds them.
    ///
    /// For storages not indexed by point offset, such as the inner vectors of a
    /// multivector storage, where the caller derives `new_len` from the
    /// published points. Staged by [`LiveReload::live_preload`].
    pub fn live_reload_to<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        new_len: usize,
    ) -> OperationResult<()> {
        if new_len <= self.len {
            return Ok(());
        }

        let reopened = self.reopen_chunks(fs)?;
        let capacity = self.capacity_with(&reopened)?;
        if capacity < new_len {
            return Err(OperationError::service_error(format!(
                "Chunked vectors in {} hold {capacity} vectors, but {new_len} are published",
                self.directory.display(),
            )));
        }

        self.apply(reopened, new_len);
        Ok(())
    }

    /// First chunk that can have changed: the one the next append lands in.
    fn watermark_chunk(&self) -> usize {
        self.config.get_chunk_index(self.len)
    }

    /// Open the chunks that can have gained vectors, without touching `self`.
    fn reopen_chunks<Fs: UniversalReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<ReopenedChunks<S, T>> {
        let last_chunk = self.watermark_chunk();

        let (fresh_from, watermark) = if last_chunk < self.chunks.len() {
            let fresh_chunk = TypedStorage::open(
                fs,
                &chunk_name(&self.directory, last_chunk),
                chunk_open_options(self.advice, self.populate, false),
                Default::default(),
            )
            .ok_unchanged()?;
            (last_chunk + 1, fresh_chunk.map(|chunk| (last_chunk, chunk)))
        } else {
            (last_chunk, None)
        };

        let new_chunks = read_chunks_from(
            fs,
            &self.directory,
            fresh_from,
            self.advice,
            self.populate,
            false,
        )?;

        Ok(ReopenedChunks {
            fresh_from,
            watermark,
            new_chunks,
        })
    }

    /// Vectors the chunks would hold once `reopened` is applied.
    fn capacity_with(&self, reopened: &ReopenedChunks<S, T>) -> OperationResult<usize> {
        let ReopenedChunks {
            fresh_from,
            watermark,
            new_chunks,
        } = reopened;

        let last_chunk = self.watermark_chunk();
        let watermark = watermark.as_ref().map(|(_, chunk)| chunk);
        let kept = self
            .chunks
            .get(last_chunk..*fresh_from)
            .unwrap_or_default()
            .iter()
            .map(|chunk| watermark.unwrap_or(chunk));

        chunks_capacity(
            last_chunk,
            self.config.chunk_size_vectors,
            self.config.dim,
            kept.chain(new_chunks),
        )
    }

    fn apply(&mut self, reopened: ReopenedChunks<S, T>, new_len: usize) {
        let ReopenedChunks {
            fresh_from,
            watermark,
            new_chunks,
        } = reopened;

        if let Some((idx, chunk)) = watermark {
            self.chunks[idx] = chunk;
        }
        self.chunks.truncate(fresh_from);
        self.chunks.extend(new_chunks);
        self.len = new_len;
    }
}

/// Vectors held contiguously by `chunks`, the chunks starting at `first_chunk`;
/// every chunk before it is full. Counting stops at the first chunk that is not
/// full, since a later one cannot extend a contiguous run past it.
pub(super) fn chunks_capacity<'a, S: UniversalRead + 'a, T: bytemuck::Pod + Send + 'a>(
    first_chunk: usize,
    chunk_size_vectors: usize,
    dim: usize,
    chunks: impl IntoIterator<Item = &'a TypedStorage<S, T>>,
) -> OperationResult<usize> {
    let mut capacity = first_chunk * chunk_size_vectors;
    for chunk in chunks {
        let held = (chunk.len()? as usize / dim).min(chunk_size_vectors);
        capacity += held;
        if held < chunk_size_vectors {
            break;
        }
    }
    Ok(capacity)
}
