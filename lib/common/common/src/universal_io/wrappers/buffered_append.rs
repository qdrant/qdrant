use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::is_alive_lock::IsAliveLock;
use crate::universal_io::{
    ByteOffset, Flusher, OpenOptions, UioResult, UniversalAppend, UniversalIoError, UniversalRead,
    UniversalReadFs, UniversalWriteFileOps,
};

/// A wrapper around [`UniversalAppend`] that buffers appends in memory and
/// lands them in the file as a *single* append per flush, so that many
/// small appends amortize into one syscall/RPC (object stores limit appends
/// per object).
///
/// The file only ever grows from its current end: there is no preallocation
/// and no trailing padding, and existing bytes are never rewritten. The
/// wrapper is write-only; readers open the file through a plain `S` handle.
///
/// [`flusher`](Self::flusher) captures the buffered length as a watermark:
/// running it appends the buffered bytes up to it and syncs the file, while
/// bytes appended in between stay buffered for the next flusher.
///
/// Unlike [`UniversalAppend::append`], an `Ok` append is only buffered — it
/// is durable, and visible to readers, once a flusher covering it ran.
#[derive(Debug)]
pub struct BufferedAppend<S> {
    /// Path of the file, used in error messages
    path: PathBuf,
    /// Shared with the flushers, which append the buffered bytes
    inner: Arc<Mutex<Inner<S>>>,
    is_alive_lock: IsAliveLock,
}

struct Inner<S> {
    /// Open handle to the file
    file: S,
    /// Length of the data persisted in the file, in bytes
    persisted_len: u64,
    /// Appended data that hasn't been written to the file yet
    ///
    /// Byte `i` corresponds to file offset `persisted_len + i`, so this is
    /// byte for byte the data of the next append.
    pending: Vec<u8>,
}

impl<S: fmt::Debug> fmt::Debug for Inner<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            file,
            persisted_len,
            pending,
        } = self;
        f.debug_struct("Inner")
            .field("file", file)
            .field("persisted_len", persisted_len)
            .field("pending_len", &pending.len())
            .finish()
    }
}

impl<S: UniversalRead> BufferedAppend<S> {
    /// Open an existing file at the given path to continue appending.
    pub fn open<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Fs::OpenExtra,
    ) -> UioResult<Self> {
        let file = fs.open(&path, options, extra)?;
        let persisted_len = file.len::<u8>()?;
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            inner: Arc::new(Mutex::new(Inner {
                file,
                persisted_len,
                pending: Vec::new(),
            })),
            is_alive_lock: IsAliveLock::new(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Logical length in bytes, including buffered bytes that haven't been
    /// flushed to the file yet.
    pub fn byte_len(&self) -> u64 {
        self.inner.lock().byte_len()
    }

    /// Length of the data persisted in the file, in bytes.
    pub fn persisted_len(&self) -> u64 {
        self.inner.lock().persisted_len
    }
}

impl<S: UniversalAppend> BufferedAppend<S> {
    /// Create a new empty file at the given path, truncating it if it already
    /// exists. The directory must exist already.
    pub fn create<Fs>(
        fs: &Fs,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Fs::OpenExtra,
    ) -> UioResult<Self>
    where
        Fs: UniversalReadFs<File = S> + UniversalWriteFileOps,
    {
        debug_assert!(options.writeable);
        fs.create(path.as_ref(), 0)?;
        Self::open(fs, path, options, extra)
    }

    /// Buffer `data` in memory; `offset` must equal the current logical end
    /// of file ([`byte_len`](Self::byte_len)), rejected with
    /// [`AppendOffsetConflict`] otherwise.
    ///
    /// [`AppendOffsetConflict`]: UniversalIoError::AppendOffsetConflict
    pub fn append<T: bytemuck::Pod>(&mut self, offset: ByteOffset, data: &[T]) -> UioResult<()> {
        let bytes: &[u8] = bytemuck::cast_slice(data);
        if bytes.is_empty() {
            return Ok(());
        }

        let mut inner = self.inner.lock();
        if offset != inner.byte_len() {
            return Err(UniversalIoError::AppendOffsetConflict {
                path: self.path.clone(),
                offset,
            });
        }

        inner.pending.extend_from_slice(bytes);
        Ok(())
    }
}

impl<S: UniversalAppend + 'static> BufferedAppend<S> {
    /// Flusher that appends the bytes buffered so far to the file and syncs
    /// it. Bytes appended after the flusher was created stay buffered for the
    /// next one; running flushers out of order is safe, a stale one is a
    /// no-op. A flusher outliving the storage is a no-op as well.
    pub fn flusher(&self) -> Flusher {
        let target_len = self.byte_len();
        let inner = Arc::downgrade(&self.inner);
        let is_alive_handle = self.is_alive_lock.handle();
        Box::new(move || {
            let (Some(_is_alive_guard), Some(inner)) =
                (is_alive_handle.lock_if_alive(), inner.upgrade())
            else {
                log::debug!("Aborted flushing on a dropped BufferedAppend instance");
                return Ok(());
            };

            // Keep the guard till the end to serialize with other flushers
            let mut inner = inner.lock();
            inner.write_pending(target_len)?;
            inner.file.flusher()()
        })
    }
}

impl<S> Inner<S> {
    fn byte_len(&self) -> u64 {
        self.persisted_len + self.pending.len() as u64
    }
}

impl<S: UniversalAppend> Inner<S> {
    /// Append buffered bytes for file offsets up to, but excluding,
    /// `target_len` to the file. A stale target, at or below what a more
    /// recent flush already persisted, is a no-op: bytes must never be
    /// written twice.
    fn write_pending(&mut self, target_len: u64) -> UioResult<()> {
        if target_len <= self.persisted_len {
            return Ok(());
        }

        let count = (target_len - self.persisted_len) as usize;
        debug_assert!(
            count <= self.pending.len(),
            "flush target exceeds buffered data",
        );
        let count = count.min(self.pending.len());
        let end = self.persisted_len + count as u64;

        self.file
            .append(self.persisted_len, &self.pending[..count])?;

        self.pending.drain(..count);
        self.persisted_len = end;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use fs_err as fs;
    use tempfile::TempDir;

    use super::*;
    use crate::mmap::AdviceSetting;
    use crate::universal_io::{MmapFile, MmapFs, Populate};

    fn options(writeable: bool) -> OpenOptions {
        OpenOptions {
            writeable,
            need_sequential: false,
            populate: Populate::No,
            advice: AdviceSetting::Global,
        }
    }

    /// Appends land in the file only up to the watermark captured at flusher
    /// creation, the rest stays buffered.
    #[test]
    fn test_buffered_append_watermark_flush() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("buffered.dat");
        let mut storage =
            BufferedAppend::<MmapFile>::create(&MmapFs, &path, options(true), ()).unwrap();

        storage.append(0, b"hello ".as_slice()).unwrap();
        storage.append(6, b"world".as_slice()).unwrap();
        assert_eq!(storage.byte_len(), 11);
        assert_eq!(fs::metadata(&path).unwrap().len(), 0, "still buffered");

        // Appending no bytes trivially succeeds, without validating the offset
        storage.append::<u8>(999, &[]).unwrap();
        assert_eq!(storage.byte_len(), 11);

        // Bytes appended after the flusher was created stay buffered
        let flush = storage.flusher();
        storage.append(11, b"!".as_slice()).unwrap();
        flush().unwrap();
        assert_eq!(storage.persisted_len(), 11);
        assert_eq!(storage.byte_len(), 12);
        assert_eq!(fs::read(&path).unwrap(), b"hello world");

        // The wrapped handle observes the flushed bytes
        assert_eq!(
            storage
                .inner
                .lock()
                .file
                .read_whole::<u8>()
                .unwrap()
                .as_ref(),
            b"hello world",
        );

        storage.flusher()().unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"hello world!");
    }

    /// Typed appends land byte for byte, in append order.
    #[test]
    fn test_typed_append() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("typed.dat");
        let mut storage =
            BufferedAppend::<MmapFile>::create(&MmapFs, &path, options(true), ()).unwrap();

        storage.append(0, &[1u32, 2]).unwrap();
        storage.append(8, &[3u32]).unwrap();
        assert_eq!(storage.byte_len(), 12);

        storage.flusher()().unwrap();
        assert_eq!(
            storage
                .inner
                .lock()
                .file
                .read_whole::<u32>()
                .unwrap()
                .as_ref(),
            [1, 2, 3],
        );
    }

    /// An append at a wrong offset is rejected before buffering anything.
    #[test]
    fn test_append_rejects_wrong_offset() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("conflict.dat");
        let mut storage =
            BufferedAppend::<MmapFile>::create(&MmapFs, &path, options(true), ()).unwrap();

        storage.append(0, b"data".as_slice()).unwrap();

        let err = storage.append(2, b"x".as_slice()).unwrap_err();
        assert!(matches!(
            err,
            UniversalIoError::AppendOffsetConflict { offset: 2, .. }
        ));
        assert_eq!(storage.byte_len(), 4);
    }

    /// A conflicting append — the file was grown outside this writer — is
    /// propagated, and the bytes stay buffered for a later flush.
    #[test]
    fn test_flush_keeps_pending_on_conflict() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("foreign.dat");
        let mut storage =
            BufferedAppend::<MmapFile>::create(&MmapFs, &path, options(true), ()).unwrap();

        storage.append(0, &[1u8, 2, 3, 4]).unwrap();

        // Something else grew the (previously empty) file
        fs::write(&path, [9; 7]).unwrap();

        let err = storage.flusher()().unwrap_err();
        assert!(matches!(err, UniversalIoError::AppendOffsetConflict { .. }));

        // Nothing was drained, the pending bytes are kept
        assert_eq!(storage.persisted_len(), 0);
        assert_eq!(storage.inner.lock().pending, [1, 2, 3, 4]);
    }

    /// Flushers run out of order: a stale flusher is a no-op instead of
    /// appending its bytes twice.
    #[test]
    fn test_stale_flusher_is_noop() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("stale.dat");
        let mut storage =
            BufferedAppend::<MmapFile>::create(&MmapFs, &path, options(true), ()).unwrap();

        storage.append(0, b"one".as_slice()).unwrap();
        let early = storage.flusher();
        storage.append(3, b" two".as_slice()).unwrap();

        storage.flusher()().unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"one two");

        early().unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"one two");
        assert_eq!(storage.persisted_len(), 7);
    }

    /// A flusher outliving its storage is a no-op instead of touching files.
    #[test]
    fn test_flusher_after_drop_is_noop() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("dropped.dat");
        let mut storage =
            BufferedAppend::<MmapFile>::create(&MmapFs, &path, options(true), ()).unwrap();

        storage.append(0, b"data".as_slice()).unwrap();
        let flush = storage.flusher();
        drop(storage);

        flush().unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
    }

    /// Flushed appends become visible to a plain `S` reader handle after
    /// `reopen`; buffered bytes never do.
    #[test]
    fn test_reader_handle_observes_flushed_appends() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("reader.dat");
        let mut writer =
            BufferedAppend::<MmapFile>::create(&MmapFs, &path, options(true), ()).unwrap();
        writer.append(0, b"one".as_slice()).unwrap();
        writer.flusher()().unwrap();

        let mut reader = MmapFs.open(&path, options(false), ()).unwrap();
        assert_eq!(reader.read_whole::<u8>().unwrap().as_ref(), b"one");

        writer.append(3, b" two".as_slice()).unwrap();
        reader.reopen().unwrap();
        assert_eq!(reader.len::<u8>().unwrap(), 3, "unflushed, not visible");

        writer.flusher()().unwrap();
        reader.reopen().unwrap();
        assert_eq!(reader.read_whole::<u8>().unwrap().as_ref(), b"one two");
    }
}
