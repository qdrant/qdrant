//! Extensions for the `tar` crate.

use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tap::Tap;
use tokio::sync::Mutex;
use tokio::task::JoinError;

/// A wrapper around [`tar::Builder`] that:
/// 1. Usable both in sync and async contexts.
/// 2. Provides the [`BuilderExt::descend`] method.
/// 3. Supports both seekable (i.e. file) and streaming (i.e. sockets) outputs.
pub struct BuilderExt<W: Write + Seek = WriteSeekBoxOwned> {
    tar: Arc<Mutex<BlowFuseOnDrop<W>>>,
    path: PathBuf,
}

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

impl<W: Write> Write for FusedWriteSeek<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if !self.enabled.load(Ordering::Acquire) {
            // This error shouldn't be observable. It might appear only in
            // `tar::Builder::drop`, and will be ignored there.
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Using WriteBox after it is disabled",
            ));
        }
        self.output.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // This method is never called by `tar::Builder`.
        self.output.flush()
    }
}

impl<W: Seek> Seek for FusedWriteSeek<W> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.output.seek(pos)
    }
}

/// Either [`Write`] or [`Write`] + [`Seek`], configurable at runtime.
/// This is owned variant for use in async contexts.
#[allow(private_interfaces)]
pub enum WriteSeekBoxOwned {
    Streaming(Box<dyn Send + Write + 'static>),
    Seekable(Box<dyn Send + WriteSeek + 'static>),
}

/// Either [`Write`] or [`Write`] + [`Seek`], configurable at runtime.
/// This variant is for borrowed writers, e.g. [`tar::EntryWriter`].
#[allow(private_interfaces)]
pub enum WriteSeekBoxBorrowed<'a> {
    Streaming(Box<dyn Write + 'a>),
    Seekable(Box<dyn WriteSeek + 'a>),
}

trait WriteSeek: Write + Seek {}
impl<T: Write + Seek> WriteSeek for T {}

impl Write for WriteSeekBoxOwned {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            WriteSeekBoxOwned::Streaming(ref mut w) => w.write(buf),
            WriteSeekBoxOwned::Seekable(ref mut w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            WriteSeekBoxOwned::Streaming(ref mut w) => w.flush(),
            WriteSeekBoxOwned::Seekable(ref mut w) => w.flush(),
        }
    }
}

impl Seek for WriteSeekBoxOwned {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            WriteSeekBoxOwned::Streaming(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Seeking is not supported",
            )),
            WriteSeekBoxOwned::Seekable(ref mut w) => w.seek(pos),
        }
    }
}

impl Write for WriteSeekBoxBorrowed<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            WriteSeekBoxBorrowed::Streaming(ref mut w) => w.write(buf),
            WriteSeekBoxBorrowed::Seekable(ref mut w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            WriteSeekBoxBorrowed::Streaming(ref mut w) => w.flush(),
            WriteSeekBoxBorrowed::Seekable(ref mut w) => w.flush(),
        }
    }
}

impl Seek for WriteSeekBoxBorrowed<'_> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            WriteSeekBoxBorrowed::Streaming(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Seeking is not supported",
            )),
            WriteSeekBoxBorrowed::Seekable(ref mut w) => w.seek(pos),
        }
    }
}

impl BuilderExt<WriteSeekBoxOwned> {
    pub fn new_seekable_owned(output: impl Send + Write + Seek + 'static) -> Self {
        Self::new(WriteSeekBoxOwned::Seekable(Box::new(output)))
    }

    pub fn new_streaming_owned(output: impl Send + Write + 'static) -> Self {
        Self::new(WriteSeekBoxOwned::Streaming(Box::new(output)))
    }
}

impl<'a> BuilderExt<WriteSeekBoxBorrowed<'a>> {
    pub fn new_seekable_borrowed(output: impl Write + Seek + 'a) -> Self {
        Self::new(WriteSeekBoxBorrowed::Seekable(Box::new(output)))
    }

    pub fn new_streaming_borrowed(output: impl Write + 'a) -> Self {
        Self::new(WriteSeekBoxBorrowed::Streaming(Box::new(output)))
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
    pub fn descend(&self, subdir: &Path) -> std::io::Result<Self> {
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
    ) -> std::io::Result<T> {
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
    pub fn blocking_append_file(&self, src: &Path, dst: &Path) -> std::io::Result<()> {
        let dst = join_relative(&self.path, dst)?;
        self.tar
            .blocking_lock()
            .tar()
            .append_path_with_name(src, dst)
    }

    /// Append a directory to the tar archive.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    pub fn blocking_append_dir_all(&self, src: &Path, dst: &Path) -> std::io::Result<()> {
        let dst = join_relative(&self.path, dst)?;
        self.tar.blocking_lock().tar().append_dir_all(dst, src)
    }

    /// Append a new entry to the tar archive with the given file contents.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    /// Use [`BuilderExt::append_data`] instead.
    pub fn blocking_append_data(&self, src: &[u8], dst: &Path) -> std::io::Result<()> {
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
    pub fn blocking_finish(self) -> std::io::Result<()> {
        let mut bb: BlowFuseOnDrop<_> = Arc::try_unwrap(self.tar)
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "finish called with multiple references to the tar builder",
                )
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
    /// Append a file to the tar archive.
    pub async fn append_file(&self, src: &Path, dst: &Path) -> std::io::Result<()> {
        let src = src.to_path_buf();
        let dst = join_relative(&self.path, dst)?;
        self.run_async(move |tar| tar.append_path_with_name(src, dst))
            .await
    }

    /// Append a new entry to the tar archive with the given file contents.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    pub async fn append_data(&self, src: Vec<u8>, dst: &Path) -> std::io::Result<()> {
        let dst = join_relative(&self.path, dst)?;
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(src.len() as u64);
        self.run_async(move |tar| tar.append_data(&mut header, dst, src.as_slice()))
            .await
    }

    /// Finish writing the tar archive.
    pub async fn finish(self) -> std::io::Result<()> {
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

fn join_relative(base: &Path, rel_path: &Path) -> std::io::Result<PathBuf> {
    if rel_path.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("path must be relative, but got {rel_path:?}"),
        ));
    }

    Ok(base.join(rel_path))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    // -------------------------------------------------------------------------
    // ------------------------------ Dummy tests ------------------------------
    // -------------------------------------------------------------------------

    struct DummyBridgeWriter(bool, Arc<Mutex<Vec<u8>>>);

    impl Write for DummyBridgeWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Forced error in write",
                ));
            }
            self.1.blocking_lock().extend_from_slice(buf); // panics in async
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            if self.0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Forced error in write",
                ));
            }
            let _ = self.1.blocking_lock(); // panics in async
            Ok(())
        }
    }

    impl Seek for DummyBridgeWriter {
        fn seek(&mut self, _: std::io::SeekFrom) -> std::io::Result<u64> {
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
        let tar = BuilderExt::new_seekable_owned(DummyBridgeWriter(false, Arc::clone(&data)));
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
        assert_eq!(result.unwrap_err().to_string(), "Seeking is not supported");
    }

    #[test]
    fn test_writeseek_ok() {
        let tar = BuilderExt::new_seekable_borrowed(Cursor::new(Vec::new()));
        tar.blocking_append_data(b"foo", Path::new("foo")).unwrap();
        tar.blocking_write_fn(Path::new("foo"), |writer| writer.write_all(b"bar"))
            .unwrap()
            .unwrap();
        tar.blocking_finish().unwrap();
    }
}
