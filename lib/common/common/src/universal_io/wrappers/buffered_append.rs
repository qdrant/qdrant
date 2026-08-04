use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::is_alive_lock::IsAliveLock;
use crate::universal_io::{
    ByteOffset, Flusher, OpenOptions, UioResult, UniversalAppend, UniversalIoError,
    UniversalReadFs, UniversalWriteFileOps, read_whole_via,
};

/// A write-only wrapper around [`UniversalAppend`] that buffers appends in
/// memory and lands them in the file as a *single* append per flush, so that
/// many small appends amortize into one operation.
///
/// [`flusher`](Self::flusher) captures the buffered length as a watermark:
/// running it appends the buffered bytes up to it and syncs the file, while
/// bytes appended in between stay buffered for the next flusher.
///
/// Unlike [`UniversalAppend::append`], an `Ok` append is only buffered. It is
/// durable and visible to readers once flushed.
#[derive(Debug)]
pub struct BufferedAppend<S> {
    /// Path of the file, used in error messages
    path: PathBuf,
    /// Shared with the flushers, which append the buffered bytes
    inner: Arc<Mutex<Inner<S>>>,
    is_alive_lock: IsAliveLock,
}

#[derive(Debug)]
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
    /// Whether bytes were appended to the file since the last successful
    /// sync, so flushers with nothing to do skip the sync
    dirty: bool,
}

impl<S: UniversalAppend + 'static> BufferedAppend<S> {
    /// Open an existing file at the given path to continue appending.
    /// `options.writeable` is forced on.
    ///
    /// To recover against a durable length watermark, use
    /// [`open_with_expected_len`](Self::open_with_expected_len) instead.
    pub fn open<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        path: impl AsRef<Path>,
        mut options: OpenOptions,
        extra: Fs::OpenExtra,
    ) -> UioResult<Self> {
        options.writeable = true;
        let file = fs.open(&path, options, extra)?;
        let persisted_len = file.len::<u8>()?;

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            inner: Arc::new(Mutex::new(Inner {
                file,
                persisted_len,
                pending: Vec::new(),
                dirty: false,
            })),
            is_alive_lock: IsAliveLock::new(),
        })
    }

    /// Open like [`open`](Self::open), reconciling the file length against
    /// `expected_len`, the length recorded by a durable watermark. A longer
    /// file on a backend without atomic appends can only be a torn tail
    /// append, so it is truncated back to `expected_len`.
    ///
    /// The returned [`persisted_len`](Self::persisted_len) can still differ
    /// from `expected_len`:
    /// - greater (only if [`APPEND_IS_ATOMIC`]): whole appends that missed the
    ///   last watermark update — the caller may adopt them
    /// - less: data covered by the watermark is missing
    ///
    /// [`APPEND_IS_ATOMIC`]: UniversalAppend::APPEND_IS_ATOMIC
    pub fn open_with_expected_len<Fs>(
        fs: &Fs,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Fs::OpenExtra,
        expected_len: u64,
    ) -> UioResult<Self>
    where
        Fs: UniversalReadFs<File = S> + UniversalWriteFileOps,
        Fs::OpenExtra: Clone,
    {
        let path = path.as_ref();
        let file = Self::open(fs, path, options, extra.clone())?;
        if S::APPEND_IS_ATOMIC || file.persisted_len() <= expected_len {
            return Ok(file);
        }

        // Truncate the torn tail via a full rewrite; universal_io has no
        // truncate operation (yet)
        drop(file);
        let prefix = read_whole_via(
            fs,
            path,
            |bytes| Ok(bytes[..expected_len as usize].to_vec()),
        )?;
        fs.atomic_save(path, &prefix)?;

        let file = Self::open(fs, path, options, extra)?;
        debug_assert_eq!(file.persisted_len(), expected_len);
        Ok(file)
    }

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
        fs.create(path.as_ref(), 0)?;
        Self::open(fs, path, options, extra)
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

    /// Buffer `data` in memory; `offset` must equal the current logical end
    /// of file ([`byte_len`](Self::byte_len))
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

    /// Flusher that appends the bytes buffered so far to the file and syncs it.
    pub fn flusher(&self) -> Flusher {
        let target_len = {
            let inner = self.inner.lock();
            if inner.pending.is_empty() && !inner.dirty {
                return Box::new(|| Ok(()));
            }
            inner.byte_len()
        };
        let inner = Arc::downgrade(&self.inner);
        let is_alive_handle = self.is_alive_lock.handle();
        Box::new(move || {
            // The alive guard doubles as the flush lock: flushers of the same
            // storage run mutually exclusive, start to finish
            let (Some(_is_alive_guard), Some(inner)) =
                (is_alive_handle.lock_if_alive(), inner.upgrade())
            else {
                log::debug!("Aborted flushing on a dropped BufferedAppend instance");
                return Ok(());
            };

            // Write & sync under the state lock.
            // perf: appends to this storage stall behind the sync; the
            // pending prefix could be staged and written outside the lock
            let mut inner = inner.lock();
            inner.write_pending(target_len)?;
            if inner.dirty {
                inner.file.flusher()()?;
                inner.dirty = false;
            }
            Ok(())
        })
    }
}

impl<S: UniversalAppend> Inner<S> {
    fn byte_len(&self) -> u64 {
        self.persisted_len + self.pending.len() as u64
    }

    /// Append buffered bytes for file offsets up to, but excluding,
    /// `target_len` to the file.
    fn write_pending(&mut self, target_len: u64) -> UioResult<()> {
        if target_len <= self.persisted_len {
            return Ok(());
        }

        // `target_len` came from `byte_len`, which never shrinks, so `count`
        // is within `pending`
        let count = (target_len - self.persisted_len) as usize;
        self.file
            .append(self.persisted_len, &self.pending[..count])?;

        self.pending.drain(..count);
        self.persisted_len = target_len;
        self.dirty = true;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use fs_err as fs;
    use tempfile::TempDir;

    use super::*;
    use crate::universal_io::conformance::open_options;
    use crate::universal_io::{MmapFile, MmapFs, UniversalRead};

    fn create_storage(dir: &TempDir, name: &str) -> (PathBuf, BufferedAppend<MmapFile>) {
        let path = dir.path().join(name);
        let storage =
            BufferedAppend::<MmapFile>::create(&MmapFs, &path, open_options(true), ()).unwrap();
        (path, storage)
    }

    /// Appends land in the file only up to the watermark captured at flusher
    /// creation, the rest stays buffered.
    #[test]
    fn test_buffered_append_watermark_flush() {
        let dir = TempDir::new().unwrap();
        let (path, mut storage) = create_storage(&dir, "buffered.dat");

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
        let (_path, mut storage) = create_storage(&dir, "typed.dat");

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
        let (_path, mut storage) = create_storage(&dir, "conflict.dat");

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
        let (path, mut storage) = create_storage(&dir, "foreign.dat");

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
        let (path, mut storage) = create_storage(&dir, "stale.dat");

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
        let (path, mut storage) = create_storage(&dir, "dropped.dat");

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
        let (path, mut writer) = create_storage(&dir, "reader.dat");
        writer.append(0, b"one".as_slice()).unwrap();
        writer.flusher()().unwrap();

        let mut reader = MmapFs.open(&path, open_options(false), ()).unwrap();
        assert_eq!(reader.read_whole::<u8>().unwrap().as_ref(), b"one");

        writer.append(3, b" two".as_slice()).unwrap();
        reader.reopen().unwrap();
        assert_eq!(reader.len::<u8>().unwrap(), 3, "unflushed, not visible");

        writer.flusher()().unwrap();
        reader.reopen().unwrap();
        assert_eq!(reader.read_whole::<u8>().unwrap().as_ref(), b"one two");
    }
}
