//! Extensions for the `tar` crate.

use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tap::Tap;
use tokio::sync::Mutex;
use tokio::task::JoinError;

/// A wrapper around [`tar::Builder`] that:
/// 1. Usable in async contexts.
/// 2. Provides the [`BuilderExt::descend`] method.
/// 3. Supports both seekable and streaming outputs.
#[derive(Clone)]
pub struct BuilderExt {
    tar: Arc<Mutex<BlowFuseOnDrop>>,
    path: PathBuf,
}

/// A wrapper around [`tar::Builder<FusedOutput>`] that disables [`FusedOutput`]
/// when it is dropped.
///
/// Disabling the [`FusedOutput`] is a workaround for the inconvenient
/// [`tar::Builder`] behavior: dropping a [`tar::Builder`] might cause a final
/// write of archive footer.
/// This behavior is problematic for [`Write`] implementations that could panic
/// when used in an async context, such as [`SyncIoBridge`].
///
/// [`SyncIoBridge`]: https://docs.rs/tokio-util/0.7.12/tokio_util/io/struct.SyncIoBridge.html#method.new
struct BlowFuseOnDrop {
    tar: Option<tar::Builder<FusedOutput>>,
    enabled: Arc<AtomicBool>,
}

/// A wrapper around [`Output`] that could be disabled by [`BlowFuseOnDrop`].
struct FusedOutput {
    output: Output,
    enabled: Arc<AtomicBool>,
}

enum Output {
    Streaming(Box<dyn Send + Write>),
    Seekable(Box<dyn Send + WriteSeek>),
}

trait WriteSeek: Write + Seek {}
impl<T: Write + Seek> WriteSeek for T {}

impl BlowFuseOnDrop {
    fn tar(&mut self) -> &mut tar::Builder<FusedOutput> {
        self.tar.as_mut().unwrap()
    }
}

impl Drop for BlowFuseOnDrop {
    fn drop(&mut self) {
        // Blow the fuse.
        self.enabled.store(false, Ordering::Release);
    }
}

impl Write for FusedOutput {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if !self.enabled.load(Ordering::Acquire) {
            // This error shouldn't be observable. It might appear only in
            // `tar::Builder::drop`, and will be ignored there.
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Using WriteBox after it is disabled",
            ));
        }
        match self.output {
            Output::Streaming(ref mut w) => w.write(buf),
            Output::Seekable(ref mut w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // This method is never called by `tar::Builder`.
        match self.output {
            Output::Streaming(ref mut w) => w.flush(),
            Output::Seekable(ref mut w) => w.flush(),
        }
    }
}

impl Seek for FusedOutput {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self.output {
            Output::Streaming(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Seeking is not supported",
            )),
            Output::Seekable(ref mut w) => w.seek(pos),
        }
    }
}

impl BuilderExt {
    #[inline]
    pub fn new_seekable(output: impl Send + Write + Seek + 'static) -> Self {
        Self::from_box(Output::Seekable(Box::new(output)))
    }

    #[inline]
    pub fn new_streaming(output: impl Send + Write + 'static) -> Self {
        Self::from_box(Output::Streaming(Box::new(output)))
    }

    fn from_box(output: Output) -> Self {
        let enabled = Arc::new(AtomicBool::new(true));
        Self {
            tar: Arc::new(Mutex::new(BlowFuseOnDrop {
                tar: Some(
                    tar::Builder::new(FusedOutput {
                        output,
                        enabled: Arc::clone(&enabled),
                    })
                    .tap_mut(|tar| tar.sparse(false)),
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
        Ok(BuilderExt {
            tar: Arc::clone(&self.tar),
            path: join_relative(&self.path, subdir)?,
        })
    }

    /// Write an entry to the tar archive. Takes a closure that takes an
    /// `impl Write` and writes the entry contents into it. Works only on
    /// builders created by [`BuilderExt::new_seekable()`]. Returns an error if
    /// the builder was created by [`BuilderExt::new_streaming()`].
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

    /// Append a file to the tar archive.
    pub async fn append_file(&self, src: &Path, dst: &Path) -> std::io::Result<()> {
        let src = src.to_path_buf();
        let dst = join_relative(&self.path, dst)?;
        self.run_async(move |tar| tar.append_path_with_name(src, dst))
            .await
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

    /// Finish writing the tar archive. For async counterpart, see
    /// [`BuilderExt::finish`].
    pub fn blocking_finish(self) -> std::io::Result<()> {
        let mut bb: BlowFuseOnDrop = Arc::try_unwrap(self.tar)
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "finish called with multiple references to the tar builder",
                )
            })?
            .into_inner();

        // Extract the builder out of bb.
        let tar: tar::Builder<FusedOutput> = bb.tar.take().unwrap();

        // Finish and flush before BuilderBox is dropped.
        let mut wb: FusedOutput = tar.into_inner()?; // calls finish()
        wb.flush()?;

        Ok(())
    }

    /// Finish writing the tar archive.
    pub async fn finish(self) -> std::io::Result<()> {
        tokio::task::spawn_blocking(move || self.blocking_finish()).await?
    }

    async fn run_async<T, E>(
        &self,
        f: impl FnOnce(&mut tar::Builder<FusedOutput>) -> Result<T, E> + Send + 'static,
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
        let tar = BuilderExt::new_seekable(DummyBridgeWriter(false, Arc::clone(&data)));
        assert!(tar.finish().await.is_ok());
        assert_eq!(data.lock().await.len(), 1024);
    }

    #[tokio::test]
    async fn test_dummy_finish_fail() {
        let data = Arc::new(Mutex::new(Vec::new()));
        let tar = BuilderExt::new_seekable(DummyBridgeWriter(true, Arc::clone(&data)));
        assert!(tar.finish().await.is_err());
        assert_eq!(data.lock().await.len(), 0);
    }

    #[tokio::test]
    async fn test_dummy_drop_fail() {
        let data = Arc::new(Mutex::new(Vec::new()));
        let tar = BuilderExt::new_seekable(DummyBridgeWriter(false, Arc::clone(&data)));
        drop(tar);
        assert_eq!(data.lock().await.len(), 0);
    }

    #[tokio::test]
    async fn test_dummy_drop_ok() {
        let data = Arc::new(Mutex::new(Vec::new()));
        let tar = BuilderExt::new_seekable(DummyBridgeWriter(false, Arc::clone(&data)));
        drop(tar);
        assert_eq!(data.lock().await.len(), 0);
    }

    // -------------------------------------------------------------------------
    // ------------------------- Write/WriteSeek tests -------------------------
    // -------------------------------------------------------------------------

    #[test]
    fn test_write_ok() {
        let tar = BuilderExt::new_streaming(Vec::new());
        tar.blocking_append_data(b"foo", Path::new("foo")).unwrap();
        tar.blocking_finish().unwrap();
    }

    #[test]
    fn test_write_fail() {
        let tar = BuilderExt::new_streaming(Vec::new());
        tar.blocking_append_data(b"foo", Path::new("foo")).unwrap();
        let result = tar.blocking_write_fn(Path::new("foo"), |writer| writer.write_all(b"bar"));
        assert_eq!(result.unwrap_err().to_string(), "Seeking is not supported");
    }

    #[test]
    fn test_writeseek_ok() {
        let tar = BuilderExt::new_seekable(Cursor::new(Vec::new()));
        tar.blocking_append_data(b"foo", Path::new("foo")).unwrap();
        tar.blocking_write_fn(Path::new("foo"), |writer| writer.write_all(b"bar"))
            .unwrap()
            .unwrap();
        tar.blocking_finish().unwrap();
    }
}
