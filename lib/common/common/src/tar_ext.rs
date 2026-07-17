//! Extensions for the `tar` crate.

use std::io::{self, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tap::Tap;
use tokio::sync::Mutex;
use tokio::task::JoinError;

/// A wrapper around [`tar::Builder`] that:
/// 1. Usable both in sync and async contexts.
/// 2. Provides the [`BuilderExt::descend`] method.
/// 3. Supports both seekable (i.e. file) and streaming (i.e. sockets) outputs.
pub struct BuilderExt<W: Write + Seek = OwnedOutput> {
    tar: Arc<Mutex<BlowFuseOnDrop<W>>>,
    path: PathBuf,
}

type OwnedOutput = Box<dyn WriteSeek + Send + 'static>;
type BorrowedOutput<'a> = Box<dyn WriteSeek + 'a>;

pub trait WriteSeek: Write + Seek {}
impl<T: Write + Seek> WriteSeek for T {}

/// A wrapper around [`tar::Builder<FusedWriteSeek>`] that disables
/// [`FusedWriteSeek`] when it is dropped.
///
/// Disabling the [`FusedWriteSeek`] is a workaround for the inconvenient
/// [`tar::Builder`] behavior: dropping a [`tar::Builder`] might cause a final
/// write of archive footer.
/// This behavior is problematic for [`Write`] implementations that could panic
/// when used in an async context, such as [`SyncIoBridge`].
///
/// [`SyncIoBridge`]: https://docs.rs/tokio-util/0.7.12/tokio_util/io/struct.SyncIoBridge.html#method.new
struct BlowFuseOnDrop<W: Write + Seek> {
    tar: Option<tar::Builder<FusedWriteSeek<W>>>,
    enabled: Arc<AtomicBool>,
}

/// A wrapper around [`WriteSeek`] that could be disabled by [`BlowFuseOnDrop`].
struct FusedWriteSeek<W> {
    output: W,
    enabled: Arc<AtomicBool>,
    /// When true, all-zero writes are accumulated into `pending_hole` instead of
    /// being written out. Only enabled while appending file contents; headers and
    /// the archive footer are always written densely.
    skip_holes: bool,
    /// Bytes of an all-zero run not yet materialized in `output`.
    pending_hole: u64,
}

impl<W: Write + Seek> BlowFuseOnDrop<W> {
    fn tar(&mut self) -> &mut tar::Builder<FusedWriteSeek<W>> {
        self.tar.as_mut().unwrap()
    }
}

impl<W: Write + Seek> Drop for BlowFuseOnDrop<W> {
    fn drop(&mut self) {
        // Blow the fuse.
        self.enabled.store(false, Ordering::Release);
    }
}

impl<W: Write + Seek> FusedWriteSeek<W> {
    /// Materialize the pending all-zero run in `output`, by seeking over all but the
    /// last byte and writing that byte, so the output has the exact logical length
    /// even if nothing is written after the run. Falls back to writing the zeros for
    /// non-seekable aka streaming outputs.
    fn materialize_pending_hole(&mut self) -> io::Result<()> {
        if self.pending_hole == 0 {
            return Ok(());
        }
        let pending = self.pending_hole;
        self.pending_hole = 0;
        match self.output.seek(io::SeekFrom::Current(pending as i64 - 1)) {
            Ok(_) => self.output.write_all(&[0]),
            Err(err) if err.kind() == io::ErrorKind::NotSeekable => {
                self.skip_holes = false;
                write_zeros(&mut self.output, pending)
            }
            Err(err) => Err(err),
        }
    }
}

impl<W: Write + Seek> Write for FusedWriteSeek<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.enabled.load(Ordering::Acquire) {
            // This error shouldn't be observable. It might appear only in
            // `tar::Builder::drop`, and will be ignored there.
            return Err(io::Error::other("Using WriteBox after it is disabled"));
        }
        // Sparse storage files (e.g. preallocated mmap pages) are mostly holes, which
        // the dense copy in `tar::Builder` reads as long runs of zeros. Skip them with
        // a seek to keep the archive logically identical while only writing physical
        // data. `tar`'s own sparse-entry support does this better (the extracted file
        // is sparse again), but it only engages on Linux/Android/FreeBSD.
        if self.skip_holes && !buf.is_empty() && buf.iter().all(|&byte| byte == 0) {
            self.pending_hole += buf.len() as u64;
            return Ok(buf.len());
        }
        self.materialize_pending_hole()?;
        self.output.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        // This method is never called by `tar::Builder`.
        self.materialize_pending_hole()?;
        self.output.flush()
    }
}

impl<W: Write + Seek> Seek for FusedWriteSeek<W> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        // Relative seeks must observe the true position.
        self.materialize_pending_hole()?;
        self.output.seek(pos)
    }
}

fn write_zeros<W: Write>(output: &mut W, mut count: u64) -> io::Result<()> {
    let zeros = [0u8; 4096];
    while count > 0 {
        let n = count.min(zeros.len() as u64) as usize;
        output.write_all(&zeros[..n])?;
        count -= n as u64;
    }
    Ok(())
}

impl BuilderExt<OwnedOutput> {
    pub fn new_seekable_owned(output: impl Write + Seek + Send + 'static) -> Self {
        Self::new(Box::new(output))
    }

    pub fn new_streaming_owned(output: impl Write + Send + 'static) -> Self {
        Self::new(Box::new(SeekWrapper(output)))
    }
}

impl<'a> BuilderExt<BorrowedOutput<'a>> {
    pub fn new_seekable_borrowed(output: impl Write + Seek + 'a) -> Self {
        Self::new(Box::new(output))
    }

    pub fn new_streaming_borrowed(output: impl Write + 'a) -> Self {
        Self::new(Box::new(SeekWrapper(output)))
    }
}

impl<W: Write + Seek> Clone for BuilderExt<W> {
    fn clone(&self) -> Self {
        Self {
            tar: Arc::clone(&self.tar),
            path: self.path.clone(),
        }
    }
}

impl<W: Write + Seek> BuilderExt<W> {
    fn new(output: W) -> Self {
        let enabled = Arc::new(AtomicBool::new(true));
        Self {
            tar: Arc::new(Mutex::new(BlowFuseOnDrop {
                tar: Some(
                    tar::Builder::new(FusedWriteSeek {
                        output,
                        enabled: Arc::clone(&enabled),
                        skip_holes: false,
                        pending_hole: 0,
                    })
                    .tap_mut(|tar| tar.sparse(true)),
                ),
                enabled,
            })),
            path: PathBuf::new(),
        }
    }

    /// Create a new [`BuilderExt`] that writes to a subdirectory of the current
    /// path. I.e. the following two lines are equivalent:
    /// ```rust,ignore
    /// builder.append_data(data, Path::new("foo/bar/baz")).await?;
    /// builder.descend(Path::new("foo/bar"))?.append_data(data, Path::new("baz")).await?;
    /// ```
    pub fn descend(&self, subdir: &Path) -> io::Result<Self> {
        Ok(Self {
            tar: Arc::clone(&self.tar),
            path: join_relative(&self.path, subdir)?,
        })
    }

    /// Write an entry to the tar archive. Takes a closure that takes an
    /// `impl Write` and writes the entry contents into it.
    ///
    /// Require the underlying writer to be [`Seek`]. Returns an error for
    /// non-seekable aka streaming writers.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    /// There are no async counterpart.
    pub fn blocking_write_fn<T>(
        &self,
        dst: &Path,
        f: impl FnOnce(&mut tar::EntryWriter) -> T,
    ) -> io::Result<T> {
        let dst = join_relative(&self.path, dst)?;
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        let mut tar = self.tar.blocking_lock();
        let mut writer = tar.tar().append_writer(&mut header, dst)?;
        let result = f(&mut writer);
        writer.finish()?;
        Ok(result)
    }

    /// Append a file to the tar archive.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    /// Use [`BuilderExt::append_file`] instead.
    pub fn blocking_append_file(&self, src: &Path, dst: &Path) -> io::Result<()> {
        let dst = join_relative(&self.path, dst)?;
        let mut tar = self.tar.blocking_lock();
        let tar = tar.tar();
        tar.get_mut().skip_holes = true;
        let result = tar.append_path_with_name(src, dst);
        let output = tar.get_mut();
        output.skip_holes = false;
        result.and(output.materialize_pending_hole())
    }

    /// Append a directory to the tar archive.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    pub fn blocking_append_dir_all(&self, src: &Path, dst: &Path) -> io::Result<()> {
        let dst = join_relative(&self.path, dst)?;
        let mut tar = self.tar.blocking_lock();
        let tar = tar.tar();
        tar.get_mut().skip_holes = true;
        let result = tar.append_dir_all(dst, src);
        let output = tar.get_mut();
        output.skip_holes = false;
        result.and(output.materialize_pending_hole())
    }

    /// Append a new entry to the tar archive with the given file contents.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    /// Use [`BuilderExt::append_data`] instead.
    pub fn blocking_append_data(&self, src: &[u8], dst: &Path) -> io::Result<()> {
        let dst = join_relative(&self.path, dst)?;
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(src.len() as u64);
        self.tar
            .blocking_lock()
            .tar()
            .append_data(&mut header, dst, src)
    }

    /// Finish writing the tar archive. For async counterpart, see
    /// [`BuilderExt::finish`].
    pub fn blocking_finish(self) -> io::Result<()> {
        let mut bb: BlowFuseOnDrop<_> = Arc::try_unwrap(self.tar)
            .map_err(|_| {
                io::Error::other("finish called with multiple references to the tar builder")
            })?
            .into_inner();

        // Extract the builder out of bb.
        let tar: tar::Builder<FusedWriteSeek<_>> = bb.tar.take().unwrap();

        // Finish and flush before BuilderBox is dropped.
        let mut wb: FusedWriteSeek<_> = tar.into_inner()?; // calls finish()
        wb.flush()?;

        Ok(())
    }
}

impl<W: Send + Write + Seek + 'static> BuilderExt<W> {
    /// Append a new entry to the tar archive with the given file contents.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    pub async fn append_data(&self, src: Vec<u8>, dst: &Path) -> io::Result<()> {
        let dst = join_relative(&self.path, dst)?;
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(src.len() as u64);
        self.run_async(move |tar| tar.append_data(&mut header, dst, src.as_slice()))
            .await
    }

    /// Finish writing the tar archive.
    pub async fn finish(self) -> io::Result<()> {
        tokio::task::spawn_blocking(move || self.blocking_finish()).await?
    }

    async fn run_async<T, E>(
        &self,
        f: impl FnOnce(&mut tar::Builder<FusedWriteSeek<W>>) -> Result<T, E> + Send + 'static,
    ) -> Result<T, E>
    where
        T: Send + 'static,
        E: Send + 'static + From<JoinError>,
    {
        let tar = Arc::clone(&self.tar);
        tokio::task::spawn_blocking(move || f(tar.blocking_lock().tar())).await?
    }
}

fn join_relative(base: &Path, rel_path: &Path) -> io::Result<PathBuf> {
    if rel_path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path must be relative, but got {rel_path:?}"),
        ));
    }

    Ok(base.join(rel_path))
}

/// A wrapper that provides "dummy" [`io::Seek`] implementation to [`io::Write`] stream.
struct SeekWrapper<T>(T);

impl<T: Write> io::Write for SeekWrapper<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl<T: Write> io::Seek for SeekWrapper<T> {
    fn seek(&mut self, _: io::SeekFrom) -> io::Result<u64> {
        Err(io::ErrorKind::NotSeekable.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // ------------------------------ Dummy tests ------------------------------
    // -------------------------------------------------------------------------

    struct DummyBridgeWriter(bool, Arc<Mutex<Vec<u8>>>);

    impl Write for DummyBridgeWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.0 {
                return Err(io::Error::other("Forced error in write"));
            }
            self.1.blocking_lock().extend_from_slice(buf); // panics in async
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            if self.0 {
                return Err(io::Error::other("Forced error in flush"));
            }
            let _ = self.1.blocking_lock(); // panics in async
            Ok(())
        }
    }

    impl Seek for DummyBridgeWriter {
        fn seek(&mut self, _: io::SeekFrom) -> io::Result<u64> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_dummy_finish_ok() {
        let data = Arc::new(Mutex::new(Vec::new()));
        let tar = BuilderExt::new_seekable_owned(DummyBridgeWriter(false, Arc::clone(&data)));
        assert!(tar.finish().await.is_ok());
        assert_eq!(data.lock().await.len(), 1024);
    }

    #[tokio::test]
    async fn test_dummy_finish_fail() {
        let data = Arc::new(Mutex::new(Vec::new()));
        let tar = BuilderExt::new_seekable_owned(DummyBridgeWriter(true, Arc::clone(&data)));
        assert!(tar.finish().await.is_err());
        assert_eq!(data.lock().await.len(), 0);
    }

    #[tokio::test]
    async fn test_dummy_drop_fail() {
        let data = Arc::new(Mutex::new(Vec::new()));
        let tar = BuilderExt::new_seekable_owned(DummyBridgeWriter(true, Arc::clone(&data)));
        drop(tar);
        assert_eq!(data.lock().await.len(), 0);
    }

    #[tokio::test]
    async fn test_dummy_drop_ok() {
        let data = Arc::new(Mutex::new(Vec::new()));
        let tar = BuilderExt::new_seekable_owned(DummyBridgeWriter(false, Arc::clone(&data)));
        drop(tar);
        assert_eq!(data.lock().await.len(), 0);
    }

    // -------------------------------------------------------------------------
    // ------------------------- Write/WriteSeek tests -------------------------
    // -------------------------------------------------------------------------

    #[test]
    fn test_write_ok() {
        let tar = BuilderExt::new_streaming_borrowed(Vec::new());
        tar.blocking_append_data(b"foo", Path::new("foo")).unwrap();
        tar.blocking_finish().unwrap();
    }

    #[test]
    fn test_write_fail() {
        let tar = BuilderExt::new_streaming_borrowed(Vec::new());
        tar.blocking_append_data(b"foo", Path::new("foo")).unwrap();
        let result = tar.blocking_write_fn(Path::new("foo"), |writer| writer.write_all(b"bar"));
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotSeekable);
    }

    #[test]
    fn test_writeseek_ok() {
        let tar = BuilderExt::new_seekable_borrowed(io::Cursor::new(Vec::new()));
        tar.blocking_append_data(b"foo", Path::new("foo")).unwrap();
        tar.blocking_write_fn(Path::new("foo"), |writer| writer.write_all(b"bar"))
            .unwrap()
            .unwrap();
        tar.blocking_finish().unwrap();
    }

    // -------------------------------------------------------------------------
    // --------------------------- Hole skipping tests -------------------------
    // -------------------------------------------------------------------------

    /// Counts the bytes physically written, to observe hole skipping.
    struct CountingWriter {
        inner: io::Cursor<Vec<u8>>,
        written: u64,
    }

    impl Write for CountingWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.written += buf.len() as u64;
            self.inner.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()
        }
    }

    impl Seek for CountingWriter {
        fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
            self.inner.seek(pos)
        }
    }

    /// Contents mimicking a preallocated sparse storage page: data at the start,
    /// a long run of zeros, data in the middle, and a zero run at the end.
    fn zero_heavy_contents() -> Vec<u8> {
        let mut contents = vec![0_u8; 4 * 1024 * 1024];
        contents[..6].copy_from_slice(b"header");
        contents[2 * 1024 * 1024..2 * 1024 * 1024 + 4].copy_from_slice(b"data");
        contents
    }

    fn unpack_single_entry(archive: &[u8]) -> Vec<u8> {
        let mut ar = tar::Archive::new(io::Cursor::new(archive));
        let mut entries = ar.entries().unwrap();
        let mut entry = entries.next().unwrap().unwrap();
        let mut contents = Vec::new();
        io::Read::read_to_end(&mut entry, &mut contents).unwrap();
        assert!(entries.next().is_none());
        contents
    }

    #[test]
    fn test_append_file_skips_zero_runs() {
        let contents = zero_heavy_contents();
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("page_0.dat");
        std::fs::write(&src, &contents).unwrap();

        let mut output = CountingWriter {
            inner: io::Cursor::new(Vec::new()),
            written: 0,
        };
        let tar = BuilderExt::new_seekable_borrowed(&mut output);
        tar.blocking_append_file(&src, Path::new("page_0.dat"))
            .unwrap();
        tar.blocking_finish().unwrap();

        // The archive is logically complete but the zero runs were seeked over,
        // not written (headers + data + footer stay well under one zero run).
        let archive = output.inner.into_inner();
        assert!(archive.len() as u64 > contents.len() as u64);
        assert!(
            output.written < 64 * 1024,
            "expected zero runs to be skipped, but {} bytes were written",
            output.written
        );

        assert_eq!(unpack_single_entry(&archive), contents);
    }

    #[test]
    fn test_append_file_streaming_writes_zero_runs() {
        let contents = zero_heavy_contents();
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("page_0.dat");
        std::fs::write(&src, &contents).unwrap();

        // Streaming aka non-seekable output: holes must be written out as zeros.
        let mut archive = Vec::new();
        let tar = BuilderExt::new_streaming_borrowed(&mut archive);
        tar.blocking_append_file(&src, Path::new("page_0.dat"))
            .unwrap();
        tar.blocking_finish().unwrap();

        assert_eq!(unpack_single_entry(&archive), contents);
    }
}
