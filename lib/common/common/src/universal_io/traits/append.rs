use super::{UniversalFlush, UniversalRead, UniversalWriteFileOps};
use crate::universal_io::{ByteOffset, Result};

/// A file handle that supports atomic appends.
///
/// Deliberately does NOT extend [`UniversalWrite`]: append-only backends
/// (object stores) cannot offer random-offset writes, so the two mutation
/// capabilities stay independent — a backend implements either, both, or
/// neither.
///
/// # Contract
///
/// - [`append`](Self::append) grows the file by writing the data at the
///   current end-of-file in a *single* grow+write operation (one syscall on
///   local backends, one RPC on remote ones). It never overwrites existing
///   bytes.
/// - Single logical writer: exactly one handle appends to a given file at a
///   time. With concurrent appenders, local backends stay uncorrupted
///   (appends are kernel-serialized) but the returned offsets become
///   unreliable; object-store backends fail with
///   [`AppendOffsetConflict`]. To recover, [`reopen`] the handle and retry.
/// - After `Ok`, this handle's [`len`]/reads observe the appended bytes.
///   Other handles (including clones) must [`reopen`] first; do not read
///   through clones of an mmap-backed handle concurrently with `append`
///   (same constraint as [`reopen`]).
/// - Durability: local backends require running [`UniversalFlush::flusher`];
///   object-store backends are durable when `append` returns `Ok` (their
///   flusher is a no-op).
/// - Requires a handle opened with `writeable: true` where the backend
///   enforces open modes. Not supported on `prevent_caching` (`O_DIRECT`)
///   handles.
/// - Appending no bytes is a no-op returning the current end-of-file offset.
/// - Returned offsets are plain byte offsets; no `T`-alignment of the
///   returned offset is guaranteed — record framing is the caller's concern.
/// - [`UniversalWrite::write`] beyond the end-of-file still fails; append is
///   the only growth path.
///
/// [`AppendOffsetConflict`]: crate::universal_io::UniversalIoError::AppendOffsetConflict
/// [`UniversalWrite`]: super::UniversalWrite
/// [`UniversalWrite::write`]: super::UniversalWrite::write
/// [`len`]: UniversalRead::len
/// [`reopen`]: UniversalRead::reopen
pub trait UniversalAppend: UniversalRead<Fs: UniversalWriteFileOps> + UniversalFlush {
    /// Atomically grow the file by appending `data` at its end.
    ///
    /// Returns the byte offset at which `data` begins.
    fn append<T: bytemuck::Pod>(&mut self, data: &[T]) -> Result<ByteOffset>;

    /// Append several buffers contiguously, in order, using as few
    /// operations as the backend allows (a single vectored syscall locally,
    /// a single RPC on object stores).
    ///
    /// Returns the byte offset of the first appended byte (the current
    /// end-of-file offset if `items` is empty).
    fn append_batch<'a, T: bytemuck::Pod>(
        &mut self,
        items: impl IntoIterator<Item = &'a [T]>,
    ) -> Result<ByteOffset> {
        let mut first = None;

        for item in items {
            let offset = self.append(item)?;
            first.get_or_insert(offset);
        }

        match first {
            Some(offset) => Ok(offset),
            None => self.len::<u8>(),
        }
    }

    // When adding provided methods, don't forget to update impls in
    // crate::universal_io::wrappers::*.
}
