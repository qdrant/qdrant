#[cfg(test)]
mod tests;

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::AdviceSetting;
use common::universal_io::{
    OpenOptions, Populate, UniversalAppend, UniversalReadFileOps, UniversalReadFs,
    UniversalWriteFileOps,
};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::chunks::{chunk_name, list_chunk_files};
use crate::vector_storage::chunked_vectors::config::{
    ChunkedVectorsConfig, Status, ensure_config, read_status_len, status_file,
};

/// Short-lived append-only writer for chunked vectors storage.
///
/// Holds no chunk handles: each [`append_many`](Self::append_many) opens the
/// touched chunks, appends, and persists the vector count, so a batch is
/// durable when it returns and there is nothing to flush.
///
/// Bounded on [`UniversalAppend`] alone, not [`UniversalWrite`](common::universal_io::UniversalWrite):
/// the status file is small enough to go through `atomic_save`, like the
/// config file, so this writer stays usable over append-only backends
/// (object stores) that don't support random-offset writes.
#[derive(Debug)]
#[cfg_attr(not(test), expect(dead_code))]
pub struct UpdateOnlyChunkedVectors<T, S: UniversalAppend> {
    directory: PathBuf,
    config: ChunkedVectorsConfig,
    status: Status,
    fs: S::Fs,
    _t: PhantomData<T>,
}

/// Options for the short-lived append-only handles
fn append_options() -> OpenOptions {
    OpenOptions {
        writeable: true,
        need_sequential: false,
        populate: Populate::No,
        advice: AdviceSetting::Global,
    }
}

#[cfg_attr(not(test), expect(dead_code))]
impl<T, S> UpdateOnlyChunkedVectors<T, S>
where
    T: bytemuck::Pod + Send,
    S: UniversalAppend + 'static,
{
    /// Open a chunked-vectors directory for appending, creating it if missing.
    ///
    /// Only reads the persisted vector count; on-disk chunk files are
    /// reconciled against it lazily, in [`append_many`](Self::append_many),
    /// once the caller's next batch reveals the offset it actually wants to
    /// write at.
    pub fn open(fs: S::Fs, directory: &Path, dim: usize) -> OperationResult<Self> {
        let status_path = status_file(directory);
        // An absent status file marks the first open
        if !fs.exists(&status_path)? {
            fs.create_dir(directory)?;
            fs.atomic_save(&status_path, bytemuck::bytes_of(&Status { len: 0 }))?;
        }
        let status = Status {
            len: read_status_len(&fs, &status_path)?,
        };
        let config = ensure_config::<T, _>(&fs, directory, dim, false)?;

        Ok(Self {
            directory: directory.to_owned(),
            config,
            status,
            fs,
            _t: PhantomData,
        })
    }

    /// Make on-disk chunk files consistent with `target_len` before appending
    /// at it.
    ///
    /// `target_len` is the offset the caller is about to write its next
    /// vector at, which may disagree with the persisted watermark
    /// (`self.status.len`):
    /// - equal: the ordinary case, nothing to grow or shrink; only a
    ///   crashed writer's unacknowledged tail bytes need trimming.
    /// - greater: the caller skipped a range (e.g. deleted points that never
    ///   got a vector) — pad the gap with zeroes.
    /// - less: the caller is replaying a range that was already durably
    ///   appended — discard it, truncating chunks back down to `target_len`
    ///   so it can be overwritten.
    ///
    /// Either way, every chunk file is left at the length `target_len`
    /// implies, and anything entirely past it is removed.
    fn reconcile_chunk_lengths(&self, target_len: usize) -> OperationResult<()> {
        let total_bytes = target_len * self.config.dim * size_of::<T>();
        let num_chunks = target_len.div_ceil(self.config.chunk_size_vectors);

        let mut listed = list_chunk_files(&self.fs, &self.directory)?;

        for chunk_id in 0..num_chunks {
            let expected = self
                .config
                .chunk_size_bytes
                .min(total_bytes.saturating_sub(chunk_id * self.config.chunk_size_bytes))
                as u64;
            match listed.remove(&chunk_id) {
                Some(file_info) => {
                    match file_info.size.cmp(&expected) {
                        std::cmp::Ordering::Equal => {
                            // Ok
                        }
                        std::cmp::Ordering::Less => {
                            // fill with zeroes
                            log::warn!(
                                "Expected larger chunk, filling chunk {chunk_id} with zeroes"
                            );
                            let data = vec![0u8; (expected - file_info.size) as usize];
                            let mut file =
                                self.fs.open_append(&file_info.path, append_options())?;
                            file.append(file_info.size, &data)?;
                            file.flusher()()?;
                        }
                        std::cmp::Ordering::Greater => {
                            // truncate
                            log::warn!("Expected smaller chunk, truncating chunk {chunk_id}");
                            let file = self.fs.open_append(&file_info.path, append_options())?;
                            let content = file.read_whole::<u8>()?;
                            let Some(truncated) = content.get(..expected as usize) else {
                                return Err(OperationError::service_error(format!(
                                    "Chunk {chunk_id} is {} bytes, shorter than the expected \
                                     truncation length {expected}",
                                    content.len(),
                                )));
                            };
                            let truncated = truncated.to_vec();
                            drop(file);
                            self.fs.atomic_save(&file_info.path, &truncated)?;
                        }
                    }
                }
                None => {
                    // create and fill with zeroes
                    log::warn!(
                        "Expected non-existing chunk {chunk_id}, creating and filling with zeroes"
                    );
                    let mut file = self.open_chunk_for_append(chunk_id, true)?;
                    file.append(0, &vec![0u8; expected as usize])?;
                    file.flusher()()?;
                }
            }
        }

        // Files past the boundary hold no data this writer should serve —
        // either unacknowledged bytes from a crashed writer, an empty file
        // left behind by `open_chunk_for_append`'s `create()` before the
        // crash, or data being rolled back because `target_len` shrank.
        for (chunk_id, file) in listed {
            log::warn!(
                "Chunk {chunk_id} past the target vector count ({} bytes). Removing.",
                file.size,
            );
            self.fs.remove(&file.path)?;
        }

        Ok(())
    }

    /// Append a batch of vectors at the end of the storage, one file append
    /// per touched chunk, then persist the new vector count.
    ///
    /// `vectors` pairs each vector with the offset it belongs at. The stream
    /// must be ordered and contiguous: offsets increase by exactly one from
    /// entry to entry. The *first* offset is checked against the persisted
    /// watermark to decide whether on-disk chunks need to grow or shrink
    /// before this batch lands — see
    /// [`reconcile_chunk_lengths`](Self::reconcile_chunk_lengths).
    pub fn append_many<'a>(
        &mut self,
        vectors: impl IntoIterator<Item = (VectorOffsetType, &'a [T])>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let mut vectors = vectors.into_iter().peekable();
        let Some((first_offset, _)) = vectors.peek().copied() else {
            return Ok(());
        };

        self.reconcile_chunk_lengths(first_offset)?;
        let mut len = first_offset;

        while vectors.peek().is_some() {
            let chunk_idx = self.config.get_chunk_index(len);
            let chunk_offset = self.config.get_chunk_offset(len);
            let capacity = self.config.remaining_chunk_capacity(len);

            let batch: Vec<&[T]> = vectors
                .by_ref()
                .take(capacity)
                .enumerate()
                .map(|(i, (offset, vector))| {
                    assert_eq!(
                        offset,
                        len + i,
                        "vector offsets must be contiguous, starting at {first_offset}"
                    );
                    assert_eq!(vector.len(), self.config.dim, "Vector size mismatch");
                    vector
                })
                .collect();
            let batch_bytes = batch.len() * self.config.dim * size_of::<T>();

            let mut chunk = self.open_chunk_for_append(chunk_idx, chunk_offset == 0)?;
            chunk.append_batch(
                (chunk_offset * size_of::<T>()) as u64,
                batch.iter().copied(),
            )?;
            // Flush in case of local backends.
            chunk.flusher()()?;

            hw_counter.vector_io_write_counter().incr_delta(batch_bytes);
            len += batch.len();
        }

        // Persist the watermark only after the data landed
        self.status.len = len;
        self.fs.atomic_save(
            &status_file(&self.directory),
            bytemuck::bytes_of(&self.status),
        )?;

        Ok(())
    }

    /// Open the chunk for appending; a `new` chunk is past the watermark, so
    /// it is created, truncating any leftover from a crashed writer.
    fn open_chunk_for_append(&self, chunk_idx: usize, new: bool) -> OperationResult<S> {
        let path = chunk_name(&self.directory, chunk_idx);
        if new {
            self.fs.create(&path, 0)?;
        }
        Ok(self.fs.open(&path, append_options(), Default::default())?)
    }
}
