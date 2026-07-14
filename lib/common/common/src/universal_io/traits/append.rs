use super::{UniversalFlush, UniversalRead, UniversalWriteFileOps};
use crate::universal_io::{ByteOffset, Result};

/// A file handle that supports atomic appends at a caller-provided offset.
///
/// Deliberately does NOT extend [`UniversalWrite`]: append-only backends
/// (object stores) cannot offer random-offset writes, so the two mutation
/// capabilities stay independent — a backend implements either, both, or
/// neither.
///
/// # Contract
///
/// - [`append`](Self::append) grows the file by writing the data at exactly
///   `offset`, which must equal the current end of file, in a *single*
///   grow+write operation (one syscall on local backends, one RPC on remote
///   ones). It never overwrites existing bytes.
/// - The offset acts as a compare-and-swap token, making appends
///   idempotent: if the end of file is not at `offset`, the append is
///   rejected with [`AppendOffsetConflict`] and nothing is written — so a
///   duplicate of an append that already landed conflicts instead of
///   appending twice. To recover from a conflict, re-check the length
///   ([`len`], after [`reopen`] on a stale handle) to learn where the file
///   actually ends before appending anew.
/// - Single logical writer: exactly one handle appends to a given file at a
///   time. With concurrent appenders, object-store backends reject
///   atomically; local backends validate the offset and then append
///   non-atomically, so an out-of-contract concurrent grow can land data
///   past the validated offset (never overwriting existing bytes).
/// - After `Ok`, this handle's [`len`]/reads observe the appended bytes.
///   Other handles (including clones) must [`reopen`] first. For mmap-backed
///   handles this is a hard requirement rather than staleness: an append may
///   move the shared mapping, so any later read through a clone that has not
///   [`reopen`]ed is undefined behavior (see [`MmapFile`]).
/// - Durability: local backends require running [`UniversalFlush::flusher`];
///   object-store backends are durable when `append` returns `Ok` (their
///   flusher is a no-op).
/// - An `Err` does not guarantee nothing was appended: with remote backends
///   the operation may have durably completed (e.g. a lost
///   acknowledgement). Retrying at the *same* offset is safe — it conflicts
///   rather than duplicating; re-check the length before appending at a new
///   offset.
/// - Requires a handle opened with `writeable: true`. Not supported on
///   `prevent_caching` (`O_DIRECT`) handles.
/// - Appending no bytes trivially succeeds, without touching the file or
///   validating `offset`.
/// - Offsets are plain byte offsets; no `T`-alignment is guaranteed or
///   required — record framing is the caller's concern.
/// - [`UniversalWrite::write`] beyond the end-of-file still fails; append is
///   the only growth path.
///
/// [`AppendOffsetConflict`]: crate::universal_io::UniversalIoError::AppendOffsetConflict
/// [`MmapFile`]: crate::universal_io::MmapFile
/// [`UniversalWrite`]: super::UniversalWrite
/// [`UniversalWrite::write`]: super::UniversalWrite::write
/// [`len`]: UniversalRead::len
/// [`reopen`]: UniversalRead::reopen
pub trait UniversalAppend: UniversalRead<Fs: UniversalWriteFileOps> + UniversalFlush {
    /// Atomically grow the file by appending `data` at exactly `offset`,
    /// which must equal the current end of file; rejected with
    /// [`AppendOffsetConflict`] otherwise.
    ///
    /// [`AppendOffsetConflict`]: crate::universal_io::UniversalIoError::AppendOffsetConflict
    fn append<T: bytemuck::Pod>(&mut self, offset: ByteOffset, data: &[T]) -> Result<()>;

    /// Append several buffers contiguously, in order, starting at exactly
    /// `offset`, using as few operations as the backend allows (a single
    /// vectored syscall locally, a single RPC on object stores).
    fn append_batch<'a, T: bytemuck::Pod>(
        &mut self,
        offset: ByteOffset,
        items: impl IntoIterator<Item = &'a [T]>,
    ) -> Result<()> {
        let mut offset = offset;

        for item in items {
            self.append(offset, item)?;
            offset += std::mem::size_of_val(item) as u64;
        }

        Ok(())
    }

    // When adding provided methods, don't forget to update impls in
    // crate::universal_io::wrappers::*.
}
